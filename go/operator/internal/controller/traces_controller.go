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

type TracesReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	DefaultProductVersion string
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=traces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=traces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=traces/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications,verbs=get;list;watch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=shardmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

func (r *TracesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	traces := &telemetryv1alpha1.Traces{}
	if err := r.Get(ctx, req.NamespacedName, traces); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	result, err := r.reconcile(ctx, traces)
	if err != nil {
		var storageErr *StorageChangeError
		reason := reasonReconcileFailed
		if errors.As(err, &storageErr) {
			reason = reasonStorageResizeBlocked
			_ = r.setStatus(ctx, traces, metav1.ConditionFalse, reason, err.Error(), traces.Status.ConfigHash)
			return ctrl.Result{}, nil
		}
		_ = r.setStatus(ctx, traces, metav1.ConditionFalse, reason, err.Error(), traces.Status.ConfigHash)
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		return result, r.setStatus(ctx, traces, metav1.ConditionFalse, reasonStorageResizing, "expanding persistent volumes", traces.Status.ConfigHash)
	}
	ready, message, err := r.workloadsReady(ctx, traces)
	if err != nil {
		return ctrl.Result{}, err
	}
	status, reason := metav1.ConditionFalse, reasonProgressing
	if ready {
		status, reason = metav1.ConditionTrue, reasonReady
	}
	return ctrl.Result{}, r.setStatus(ctx, traces, status, reason, message, traces.Status.ConfigHash)
}

func (r *TracesReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.Traces{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.tracesForPVC)).
		Watches(&telemetryv1alpha1.NamespaceAuthentication{}, handler.EnqueueRequestsFromMapFunc(r.tracesForNamespaceAuth)).
		Watches(&telemetryv1alpha1.ShardMap{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			return requestsForShardMap(obj.(*telemetryv1alpha1.ShardMap), dataStoreTraces)
		})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.tracesForSecret)).
		Named("traces").
		Complete(r)
}

func (r *TracesReconciler) reconcile(ctx context.Context, traces *telemetryv1alpha1.Traces) (ctrl.Result, error) {
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(traces.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list namespace authentications: %w", err)
	}
	auths.Items = lo.Filter(auths.Items, func(auth telemetryv1alpha1.NamespaceAuthentication, _ int) bool {
		return auth.Spec.DataStoreRef.Kind == dataStoreTraces && auth.Spec.DataStoreRef.Name == traces.Name
	})
	sort.Slice(auths.Items, func(i, j int) bool { return auths.Items[i].Name < auths.Items[j].Name })

	tokenName, tokenKey, token, err := r.reconcileInternalToken(ctx, traces)
	if err != nil {
		return ctrl.Result{}, err
	}
	input, err := r.resolveConfigInput(ctx, traces, auths.Items, token)
	if err != nil {
		return ctrl.Result{}, err
	}
	rendered, err := productconfig.Render(input)
	if err != nil {
		return ctrl.Result{}, err
	}
	configName := resources.Name(traces.Name, suffixConfig)
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: traces.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		generated.Labels, generated.Type, generated.Data = resources.Labels(traces, resources.ComponentNone), corev1.SecretTypeOpaque, rendered.Data
		traces.Status.ConfigHash = rendered.Hash
		return controllerutil.SetControllerReference(traces, generated, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configuration secret: %w", err)
	}
	if err := r.reconcileServiceAccount(ctx, traces); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.removeObsoleteResources(ctx, traces); err != nil {
		return ctrl.Result{}, err
	}
	if resources.Mode(traces) == telemetryv1alpha1.ProductModeSharded {
		if err := r.reconcileShardingRBAC(ctx, traces); err != nil {
			return ctrl.Result{}, err
		}
	}
	for _, component := range resources.Components(traces) {
		for _, headless := range []bool{false, true} {
			if err := r.reconcileService(ctx, traces, component, headless); err != nil {
				return ctrl.Result{}, err
			}
		}
		result, err := r.reconcileStatefulSet(ctx, traces, component, configName, tokenName, tokenKey)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}
	if err := r.reconcileIngress(ctx, traces); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TracesReconciler) reconcileServiceAccount(ctx context.Context, traces *telemetryv1alpha1.Traces) error {
	desired := resources.ServiceAccount(traces)
	current := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations = desired.Labels, desired.Annotations
		return controllerutil.SetControllerReference(traces, current, r.Scheme)
	})
	return err
}

func (r *TracesReconciler) reconcileShardingRBAC(ctx context.Context, traces *telemetryv1alpha1.Traces) error {
	desired := resources.Role(traces)
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels, role.Rules = desired.Labels, desired.Rules
		return controllerutil.SetControllerReference(traces, role, r.Scheme)
	}); err != nil {
		return err
	}
	desiredBinding := resources.RoleBinding(traces)
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: desiredBinding.Name, Namespace: desiredBinding.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels, binding.Subjects, binding.RoleRef = desiredBinding.Labels, desiredBinding.Subjects, desiredBinding.RoleRef
		return controllerutil.SetControllerReference(traces, binding, r.Scheme)
	})
	return err
}

func (r *TracesReconciler) reconcileService(ctx context.Context, traces *telemetryv1alpha1.Traces, component resources.Component, headless bool) error {
	desired := resources.Service(traces, component, headless)
	current := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations = desired.Labels, desired.Annotations
		current.Spec.Selector, current.Spec.Ports, current.Spec.Type = desired.Spec.Selector, desired.Spec.Ports, desired.Spec.Type
		if headless {
			current.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		return controllerutil.SetControllerReference(traces, current, r.Scheme)
	})
	return err
}

func (r *TracesReconciler) reconcileIngress(ctx context.Context, traces *telemetryv1alpha1.Traces) error {
	current := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: traces.Name, Namespace: traces.Namespace}}
	if !traces.Spec.Ingress.Enabled {
		if err := r.Get(ctx, client.ObjectKeyFromObject(current), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		if metav1.IsControlledBy(current, traces) {
			return client.IgnoreNotFound(r.Delete(ctx, current))
		}
		return nil
	}
	desired := resources.Ingress(traces)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations, current.Spec = desired.Labels, desired.Annotations, desired.Spec
		return controllerutil.SetControllerReference(traces, current, r.Scheme)
	})
	return err
}

func (r *TracesReconciler) removeObsoleteResources(ctx context.Context, traces *telemetryv1alpha1.Traces) error {
	var objects []client.Object
	if resources.Mode(traces) == telemetryv1alpha1.ProductModeSharded {
		objects = []client.Object{
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: traces.Name, Namespace: traces.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: traces.Name, Namespace: traces.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resources.Name(traces.Name, suffixHeadless), Namespace: traces.Namespace}},
		}
	} else {
		for _, component := range []resources.Component{resources.ComponentWriter, resources.ComponentReader} {
			name := resources.ComponentName(traces, component)
			objects = append(objects,
				&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: traces.Namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: traces.Namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resources.Name(name, suffixHeadless), Namespace: traces.Namespace}})
		}
		name := resources.Name(traces.Name, suffixSharding)
		objects = append(objects, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: traces.Namespace}}, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: traces.Namespace}})
	}
	for _, object := range objects {
		if err := r.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if metav1.IsControlledBy(object, traces) {
			if err := r.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (r *TracesReconciler) reconcileInternalToken(ctx context.Context, traces *telemetryv1alpha1.Traces) (string, string, []byte, error) {
	helper := &MetricsReconciler{Client: r.Client, Scheme: r.Scheme, DefaultProductVersion: r.DefaultProductVersion}
	generatedName := resources.Name(traces.Name, suffixInternalToken)
	if ref := traces.Spec.Config.Auth.InternalTokenSecretRef; ref != nil {
		token, err := helper.secretValue(ctx, traces.Namespace, *ref)
		return ref.Name, ref.Key, token, err
	}
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: generatedName, Namespace: traces.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		token := append([]byte(nil), generated.Data[resources.TokenKey]...)
		if len(token) == 0 {
			raw := make([]byte, 32)
			if _, err := rand.Read(raw); err != nil {
				return err
			}
			token = []byte(base64.RawURLEncoding.EncodeToString(raw))
		}
		generated.Labels, generated.Type, generated.Data = resources.Labels(traces, resources.ComponentNone), corev1.SecretTypeOpaque, map[string][]byte{resources.TokenKey: token}
		return controllerutil.SetControllerReference(traces, generated, r.Scheme)
	})
	if err != nil {
		return "", "", nil, err
	}
	return generatedName, resources.TokenKey, append([]byte(nil), generated.Data[resources.TokenKey]...), nil
}

func (r *TracesReconciler) resolveConfigInput(ctx context.Context, traces *telemetryv1alpha1.Traces, auths []telemetryv1alpha1.NamespaceAuthentication, token []byte) (productconfig.Input, error) {
	helper := &MetricsReconciler{Client: r.Client, Scheme: r.Scheme, DefaultProductVersion: r.DefaultProductVersion}
	global, err := helper.resolveAccess(ctx, traces.Namespace, traces.Spec.Config.Auth.Global)
	if err != nil {
		return productconfig.Input{}, err
	}
	input := productconfig.Input{Traces: traces, Global: global, InternalTokenPath: resources.TracesDescriptor.InternalTokenPath, InternalToken: token}
	for _, auth := range auths {
		password, err := helper.secretValue(ctx, traces.Namespace, auth.Spec.SecretKeyRef)
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
	if spec := traces.Spec.Config.Auth.JWT; spec != nil {
		input.JWT = &productconfig.JWT{URL: spec.JWKS.URL, Issuer: spec.Issuer, Audience: spec.Audience, RefreshIntervalSeconds: spec.RefreshIntervalSeconds, RequestTimeoutSeconds: spec.RequestTimeoutSeconds}
		if spec.JWKS.SecretKeyRef != nil {
			input.JWT.JWKS, err = helper.secretValue(ctx, traces.Namespace, *spec.JWKS.SecretKeyRef)
			if err != nil {
				return productconfig.Input{}, err
			}
		}
	}
	return input, nil
}

func tracesSecretNames(traces *telemetryv1alpha1.Traces) []string {
	names := lo.FilterMap(append(append([]telemetryv1alpha1.BasicCredentialSpec{}, traces.Spec.Config.Auth.Global.Read...), traces.Spec.Config.Auth.Global.Write...), func(spec telemetryv1alpha1.BasicCredentialSpec, _ int) (string, bool) {
		return spec.Password.Name, spec.Password.Name != ""
	})
	refs := []*corev1.SecretKeySelector{traces.Spec.Config.Auth.InternalTokenSecretRef}
	if traces.Spec.Config.Auth.JWT != nil {
		refs = append(refs, traces.Spec.Config.Auth.JWT.JWKS.SecretKeyRef)
	}
	for _, ref := range refs {
		if ref != nil && ref.Name != "" {
			names = append(names, ref.Name)
		}
	}
	return lo.Uniq(names)
}

func (r *TracesReconciler) workloadsReady(ctx context.Context, traces *telemetryv1alpha1.Traces) (bool, string, error) {
	for _, component := range resources.Components(traces) {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: traces.Namespace, Name: resources.ComponentName(traces, component)}, sts); err != nil {
			return false, "", err
		}
		desired := lo.FromPtrOr(sts.Spec.Replicas, int32(1))
		if sts.Status.ReadyReplicas < desired || sts.Status.ObservedGeneration < sts.Generation {
			return false, fmt.Sprintf("%s has %d/%d ready replicas", component, sts.Status.ReadyReplicas, desired), nil
		}
	}
	return true, messageWorkloadsReady, nil
}

func (r *TracesReconciler) setStatus(ctx context.Context, traces *telemetryv1alpha1.Traces, status metav1.ConditionStatus, reason, message, hash string) error {
	current := &telemetryv1alpha1.Traces{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(traces), current); err != nil {
		return err
	}
	base := current.DeepCopy()
	current.Status.ObservedGeneration, current.Status.ConfigHash = current.Generation, hash
	if resources.Mode(current) == telemetryv1alpha1.ProductModeSharded {
		current.Status.WriterEndpoint = fmt.Sprintf("http://%s:%d", resources.ComponentName(current, resources.ComponentWriter), resources.HTTPPort(current))
		current.Status.ReaderEndpoint = fmt.Sprintf("http://%s:%d", resources.ComponentName(current, resources.ComponentReader), resources.HTTPPort(current))
		scaling, err := loadWriterScalingState(ctx, r.Client, current, dataStoreTraces, resources.Mode(current), writerReplicaIntent(current))
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

func (r *TracesReconciler) tracesForNamespaceAuth(_ context.Context, obj client.Object) []reconcile.Request {
	auth := obj.(*telemetryv1alpha1.NamespaceAuthentication)
	if auth.Spec.DataStoreRef.Kind != dataStoreTraces || auth.Spec.DataStoreRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: auth.Namespace, Name: auth.Spec.DataStoreRef.Name}}}
}

func (r *TracesReconciler) tracesForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	traces := &telemetryv1alpha1.TracesList{}
	if err := r.List(ctx, traces, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := lo.FilterMap(traces.Items, func(traces telemetryv1alpha1.Traces, _ int) (reconcile.Request, bool) {
		return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&traces)}, lo.Contains(tracesSecretNames(&traces), obj.GetName())
	})
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(obj.GetNamespace())); err == nil {
		for i := range auths.Items {
			if auths.Items[i].Spec.SecretKeyRef.Name == obj.GetName() {
				requests = append(requests, r.tracesForNamespaceAuth(ctx, &auths.Items[i])...)
			}
		}
	}
	return lo.UniqBy(requests, func(request reconcile.Request) types.NamespacedName { return request.NamespacedName })
}

func (r *TracesReconciler) tracesForPVC(_ context.Context, obj client.Object) []reconcile.Request {
	pvc := obj.(*corev1.PersistentVolumeClaim)
	name := pvc.Annotations[resources.TracesNameAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: pvc.Namespace, Name: name}}}
}
