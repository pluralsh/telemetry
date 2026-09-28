/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	productconfig "github.com/pluralsh/telemetry/go/operator/internal/config"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

type LineReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	DefaultProductVersion string
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=lines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=lines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=lines/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications,verbs=get;list;watch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=shardmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

func (r *LineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	line := &telemetryv1alpha1.Line{}
	if err := r.Get(ctx, req.NamespacedName, line); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	result, err := r.reconcile(ctx, line)
	if err != nil {
		var storageErr *StorageChangeError
		reason := reasonReconcileFailed
		if errors.As(err, &storageErr) {
			reason = reasonStorageResizeBlocked
			_ = r.setStatus(ctx, line, metav1.ConditionFalse, reason, err.Error(), line.Status.ConfigHash)
			return ctrl.Result{}, nil
		}
		_ = r.setStatus(ctx, line, metav1.ConditionFalse, reason, err.Error(), line.Status.ConfigHash)
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		return result, r.setStatus(ctx, line, metav1.ConditionFalse, reasonStorageResizing, "expanding persistent volumes", line.Status.ConfigHash)
	}
	ready, message, err := r.workloadsReady(ctx, line)
	if err != nil {
		return ctrl.Result{}, err
	}
	status, reason := metav1.ConditionFalse, reasonProgressing
	if ready {
		status, reason = metav1.ConditionTrue, reasonReady
	}
	return ctrl.Result{}, r.setStatus(ctx, line, status, reason, message, line.Status.ConfigHash)
}

func (r *LineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.Line{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.linesForPVC)).
		Watches(&telemetryv1alpha1.NamespaceAuthentication{}, handler.EnqueueRequestsFromMapFunc(r.linesForNamespaceAuth)).
		Watches(&telemetryv1alpha1.ShardMap{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			return requestsForShardMap(obj.(*telemetryv1alpha1.ShardMap), dataStoreLine)
		})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.linesForSecret)).
		Named("line").
		Complete(r)
}

func (r *LineReconciler) reconcile(ctx context.Context, line *telemetryv1alpha1.Line) (ctrl.Result, error) {
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(line.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list namespace authentications: %w", err)
	}
	auths.Items = lo.Filter(auths.Items, func(auth telemetryv1alpha1.NamespaceAuthentication, _ int) bool {
		return auth.Spec.DataStoreRef.Kind == dataStoreLine && auth.Spec.DataStoreRef.Name == line.Name
	})
	sort.Slice(auths.Items, func(i, j int) bool { return auths.Items[i].Name < auths.Items[j].Name })

	tokenName, tokenKey, token, err := r.reconcileInternalToken(ctx, line)
	if err != nil {
		return ctrl.Result{}, err
	}
	input, err := r.resolveConfigInput(ctx, line, auths.Items, token)
	if err != nil {
		return ctrl.Result{}, err
	}
	rendered, err := productconfig.Render(input)
	if err != nil {
		return ctrl.Result{}, err
	}
	configName := resources.Name(line.Name, suffixConfig)
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: line.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		generated.Labels, generated.Type, generated.Data = resources.Labels(line, resources.ComponentNone), corev1.SecretTypeOpaque, rendered.Data
		line.Status.ConfigHash = rendered.Hash
		return controllerutil.SetControllerReference(line, generated, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configuration secret: %w", err)
	}
	if err := r.reconcileServiceAccount(ctx, line); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.removeObsoleteResources(ctx, line); err != nil {
		return ctrl.Result{}, err
	}
	if resources.Mode(line) == telemetryv1alpha1.ProductModeSharded {
		if err := r.reconcileShardingRBAC(ctx, line); err != nil {
			return ctrl.Result{}, err
		}
	}
	for _, component := range resources.Components(line) {
		for _, headless := range []bool{false, true} {
			if err := r.reconcileService(ctx, line, component, headless); err != nil {
				return ctrl.Result{}, err
			}
		}
		result, err := r.reconcileStatefulSet(ctx, line, component, configName, tokenName, tokenKey)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}
	if err := r.reconcileIngress(ctx, line); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *LineReconciler) reconcileServiceAccount(ctx context.Context, line *telemetryv1alpha1.Line) error {
	desired := resources.ServiceAccount(line)
	current := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations = desired.Labels, desired.Annotations
		return controllerutil.SetControllerReference(line, current, r.Scheme)
	})
	return err
}

func (r *LineReconciler) reconcileShardingRBAC(ctx context.Context, line *telemetryv1alpha1.Line) error {
	desired := resources.Role(line)
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels, role.Rules = desired.Labels, desired.Rules
		return controllerutil.SetControllerReference(line, role, r.Scheme)
	}); err != nil {
		return err
	}
	desiredBinding := resources.RoleBinding(line)
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: desiredBinding.Name, Namespace: desiredBinding.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels, binding.Subjects, binding.RoleRef = desiredBinding.Labels, desiredBinding.Subjects, desiredBinding.RoleRef
		return controllerutil.SetControllerReference(line, binding, r.Scheme)
	})
	return err
}

func (r *LineReconciler) reconcileService(ctx context.Context, line *telemetryv1alpha1.Line, component resources.Component, headless bool) error {
	desired := resources.Service(line, component, headless)
	current := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations = desired.Labels, desired.Annotations
		current.Spec.Selector, current.Spec.Ports, current.Spec.Type = desired.Spec.Selector, desired.Spec.Ports, desired.Spec.Type
		if headless {
			current.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		return controllerutil.SetControllerReference(line, current, r.Scheme)
	})
	return err
}

func (r *LineReconciler) reconcileIngress(ctx context.Context, line *telemetryv1alpha1.Line) error {
	current := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: line.Name, Namespace: line.Namespace}}
	if !line.Spec.Ingress.Enabled {
		if err := r.Get(ctx, client.ObjectKeyFromObject(current), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		if metav1.IsControlledBy(current, line) {
			return client.IgnoreNotFound(r.Delete(ctx, current))
		}
		return nil
	}
	desired := resources.Ingress(line)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations, current.Spec = desired.Labels, desired.Annotations, desired.Spec
		return controllerutil.SetControllerReference(line, current, r.Scheme)
	})
	return err
}

func (r *LineReconciler) removeObsoleteResources(ctx context.Context, line *telemetryv1alpha1.Line) error {
	var objects []client.Object
	if resources.Mode(line) == telemetryv1alpha1.ProductModeSharded {
		objects = []client.Object{
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: line.Name, Namespace: line.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: line.Name, Namespace: line.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resources.Name(line.Name, suffixHeadless), Namespace: line.Namespace}},
		}
	} else {
		for _, component := range []resources.Component{resources.ComponentWriter, resources.ComponentReader} {
			name := resources.ComponentName(line, component)
			objects = append(objects,
				&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: line.Namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: line.Namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resources.Name(name, suffixHeadless), Namespace: line.Namespace}})
		}
		name := resources.Name(line.Name, suffixSharding)
		objects = append(objects, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: line.Namespace}}, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: line.Namespace}})
	}
	for _, object := range objects {
		if err := r.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if metav1.IsControlledBy(object, line) {
			if err := r.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (r *LineReconciler) reconcileInternalToken(ctx context.Context, line *telemetryv1alpha1.Line) (string, string, []byte, error) {
	helper := &MeterReconciler{Client: r.Client, Scheme: r.Scheme, DefaultProductVersion: r.DefaultProductVersion}
	generatedName := resources.Name(line.Name, suffixInternalToken)
	if ref := line.Spec.Config.Auth.InternalTokenSecretRef; ref != nil {
		token, err := helper.secretValue(ctx, line.Namespace, *ref)
		return ref.Name, ref.Key, token, err
	}
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: generatedName, Namespace: line.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		token := append([]byte(nil), generated.Data[resources.TokenKey]...)
		if len(token) == 0 {
			raw := make([]byte, 32)
			if _, err := rand.Read(raw); err != nil {
				return err
			}
			token = []byte(base64.RawURLEncoding.EncodeToString(raw))
		}
		generated.Labels, generated.Type, generated.Data = resources.Labels(line, resources.ComponentNone), corev1.SecretTypeOpaque, map[string][]byte{resources.TokenKey: token}
		return controllerutil.SetControllerReference(line, generated, r.Scheme)
	})
	if err != nil {
		return "", "", nil, err
	}
	return generatedName, resources.TokenKey, append([]byte(nil), generated.Data[resources.TokenKey]...), nil
}

func (r *LineReconciler) resolveConfigInput(ctx context.Context, line *telemetryv1alpha1.Line, auths []telemetryv1alpha1.NamespaceAuthentication, token []byte) (productconfig.Input, error) {
	helper := &MeterReconciler{Client: r.Client, Scheme: r.Scheme, DefaultProductVersion: r.DefaultProductVersion}
	global, err := helper.resolveAccess(ctx, line.Namespace, line.Spec.Config.Auth.Global)
	if err != nil {
		return productconfig.Input{}, err
	}
	input := productconfig.Input{Line: line, Global: global, InternalTokenPath: resources.LineDescriptor.InternalTokenPath, InternalToken: token}
	for _, auth := range auths {
		password, err := helper.secretValue(ctx, line.Namespace, auth.Spec.SecretKeyRef)
		if err != nil {
			return productconfig.Input{}, fmt.Errorf("resolve NamespaceAuthentication %s password: %w", auth.Name, err)
		}
		credential := productconfig.Credential{Username: auth.Spec.Username, Password: password, DataKey: "namespace-" + auth.Name + "-password"}
		access := productconfig.Access{}
		if auth.Spec.Permission == permissionRead {
			access.Read = []productconfig.Credential{credential}
		} else {
			access.Write = []productconfig.Credential{credential}
		}
		input.Namespaces = append(input.Namespaces, productconfig.NamespaceAccess{Name: auth.Spec.Namespace, KeyPrefix: auth.Name, Access: access})
	}
	if spec := line.Spec.Config.Auth.JWT; spec != nil {
		input.JWT = &productconfig.JWT{URL: spec.JWKS.URL, Issuer: spec.Issuer, Audience: spec.Audience, RefreshIntervalSeconds: spec.RefreshIntervalSeconds, RequestTimeoutSeconds: spec.RequestTimeoutSeconds}
		if spec.JWKS.SecretKeyRef != nil {
			input.JWT.JWKS, err = helper.secretValue(ctx, line.Namespace, *spec.JWKS.SecretKeyRef)
			if err != nil {
				return productconfig.Input{}, err
			}
		}
	}
	return input, nil
}

func lineSecretNames(line *telemetryv1alpha1.Line) []string {
	names := lo.FilterMap(append(append([]telemetryv1alpha1.BasicCredentialSpec{}, line.Spec.Config.Auth.Global.Read...), line.Spec.Config.Auth.Global.Write...), func(spec telemetryv1alpha1.BasicCredentialSpec, _ int) (string, bool) {
		return spec.Password.Name, spec.Password.Name != ""
	})
	refs := []*corev1.SecretKeySelector{line.Spec.Config.Auth.InternalTokenSecretRef}
	if line.Spec.Config.Auth.JWT != nil {
		refs = append(refs, line.Spec.Config.Auth.JWT.JWKS.SecretKeyRef)
	}
	for _, ref := range refs {
		if ref != nil && ref.Name != "" {
			names = append(names, ref.Name)
		}
	}
	return lo.Uniq(names)
}

func (r *LineReconciler) workloadsReady(ctx context.Context, line *telemetryv1alpha1.Line) (bool, string, error) {
	for _, component := range resources.Components(line) {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: line.Namespace, Name: resources.ComponentName(line, component)}, sts); err != nil {
			return false, "", err
		}
		desired := lo.FromPtrOr(sts.Spec.Replicas, int32(1))
		if sts.Status.ReadyReplicas < desired || sts.Status.ObservedGeneration < sts.Generation {
			return false, fmt.Sprintf("%s has %d/%d ready replicas", component, sts.Status.ReadyReplicas, desired), nil
		}
	}
	return true, messageWorkloadsReady, nil
}

func (r *LineReconciler) setStatus(ctx context.Context, line *telemetryv1alpha1.Line, status metav1.ConditionStatus, reason, message, hash string) error {
	current := &telemetryv1alpha1.Line{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(line), current); err != nil {
		return err
	}
	base := current.DeepCopy()
	current.Status.ObservedGeneration, current.Status.ConfigHash = current.Generation, hash
	if resources.Mode(current) == telemetryv1alpha1.ProductModeSharded {
		current.Status.WriterEndpoint = fmt.Sprintf("http://%s:%d", resources.ComponentName(current, resources.ComponentWriter), resources.HTTPPort(current))
		current.Status.ReaderEndpoint = fmt.Sprintf("http://%s:%d", resources.ComponentName(current, resources.ComponentReader), resources.HTTPPort(current))
		scaling, err := loadWriterScalingState(ctx, r.Client, current, dataStoreLine, resources.Mode(current), writerReplicaIntent(current))
		if err != nil {
			return err
		}
		current.Status.WriterScalingStatus = scaling.status
		setWriterScalingConditions(&current.Status.Conditions, current.Generation, scaling)
		if converged, scalingMessage := shardedReady(scaling); !converged && (reason == reasonReady || reason == reasonProgressing) {
			status, reason, message = metav1.ConditionFalse, reasonProgressing, scalingMessage
		}
	} else {
		current.Status.WriterEndpoint = fmt.Sprintf("http://%s:%d", current.Name, resources.HTTPPort(current))
		current.Status.ReaderEndpoint = current.Status.WriterEndpoint
		current.Status.WriterScalingStatus = telemetryv1alpha1.WriterScalingStatus{EffectiveWriterReplicas: 1}
	}
	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{Type: conditionReady, Status: status, Reason: reason, Message: message, ObservedGeneration: current.Generation, LastTransitionTime: metav1.NewTime(time.Now())})
	if equality.Semantic.DeepEqual(base.Status, current.Status) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(base))
}

func (r *LineReconciler) linesForNamespaceAuth(_ context.Context, obj client.Object) []reconcile.Request {
	auth := obj.(*telemetryv1alpha1.NamespaceAuthentication)
	if auth.Spec.DataStoreRef.Kind != dataStoreLine || auth.Spec.DataStoreRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: auth.Namespace, Name: auth.Spec.DataStoreRef.Name}}}
}

func (r *LineReconciler) linesForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	lines := &telemetryv1alpha1.LineList{}
	if err := r.List(ctx, lines, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := lo.FilterMap(lines.Items, func(line telemetryv1alpha1.Line, _ int) (reconcile.Request, bool) {
		return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&line)}, lo.Contains(lineSecretNames(&line), obj.GetName())
	})
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(obj.GetNamespace())); err == nil {
		for i := range auths.Items {
			if auths.Items[i].Spec.SecretKeyRef.Name == obj.GetName() {
				requests = append(requests, r.linesForNamespaceAuth(ctx, &auths.Items[i])...)
			}
		}
	}
	return lo.UniqBy(requests, func(request reconcile.Request) types.NamespacedName { return request.NamespacedName })
}

func (r *LineReconciler) linesForPVC(_ context.Context, obj client.Object) []reconcile.Request {
	pvc := obj.(*corev1.PersistentVolumeClaim)
	name := pvc.Annotations[resources.LineNameAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: pvc.Namespace, Name: name}}}
}
