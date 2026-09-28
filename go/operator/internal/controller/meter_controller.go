/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
	meterconfig "github.com/pluralsh/telemetry/go/operator/internal/config"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

// MeterReconciler reconciles a Meter object.
type MeterReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	DefaultProductVersion string
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=meters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=meters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=meters/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications,verbs=get;list;watch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=shardmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete

func (r *MeterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	meter := &telemetryv1alpha1.Meter{}
	if err := r.Get(ctx, req.NamespacedName, meter); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	result, err := r.reconcile(ctx, meter)
	if err != nil {
		var storageErr *StorageChangeError
		if errors.As(err, &storageErr) {
			_ = r.setStatus(ctx, meter, metav1.ConditionFalse, reasonStorageResizeBlocked, err.Error(), meter.Status.ConfigHash)
			return ctrl.Result{}, nil
		}
		_ = r.setStatus(ctx, meter, metav1.ConditionFalse, reasonReconcileFailed, err.Error(), meter.Status.ConfigHash)
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		if err := r.setStatus(ctx, meter, metav1.ConditionFalse, reasonStorageResizing, "expanding persistent volumes", meter.Status.ConfigHash); err != nil {
			return ctrl.Result{}, err
		}
		return result, nil
	}

	ready, message, err := r.workloadsReady(ctx, meter)
	if err != nil {
		return ctrl.Result{}, err
	}
	status, reason := metav1.ConditionFalse, reasonProgressing
	if ready {
		status, reason = metav1.ConditionTrue, reasonReady
	}
	return ctrl.Result{}, r.setStatus(ctx, meter, status, reason, message, meter.Status.ConfigHash)
}

func (r *MeterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.setupIndexes(mgr); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.Meter{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.metersForPVC)).
		Watches(&telemetryv1alpha1.NamespaceAuthentication{}, handler.EnqueueRequestsFromMapFunc(r.metersForNamespaceAuth)).
		Watches(&telemetryv1alpha1.ShardMap{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			return requestsForShardMap(obj.(*telemetryv1alpha1.ShardMap), dataStoreMeter)
		})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.metersForSecret)).
		Named("meter").
		Complete(r)
}

func (r *MeterReconciler) reconcile(ctx context.Context, meter *telemetryv1alpha1.Meter) (ctrl.Result, error) {
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(meter.Namespace), client.MatchingFields{namespaceAuthMeterIndex: meter.Name}); err != nil {
		if fallbackErr := r.List(ctx, auths, client.InNamespace(meter.Namespace)); fallbackErr != nil {
			return ctrl.Result{}, fmt.Errorf("list namespace authentications: %w", err)
		}
		auths.Items = lo.Filter(auths.Items, func(auth telemetryv1alpha1.NamespaceAuthentication, _ int) bool {
			return auth.Spec.DataStoreRef.Kind == dataStoreMeter && auth.Spec.DataStoreRef.Name == meter.Name
		})
	}
	sort.Slice(auths.Items, func(i, j int) bool { return auths.Items[i].Name < auths.Items[j].Name })

	internalSecretName, internalSecretKey, internalToken, err := r.reconcileInternalToken(ctx, meter)
	if err != nil {
		return ctrl.Result{}, err
	}
	renderInput, err := r.resolveConfigInput(ctx, meter, auths.Items, internalToken)
	if err != nil {
		return ctrl.Result{}, err
	}
	rendered, err := meterconfig.Render(renderInput)
	if err != nil {
		return ctrl.Result{}, err
	}
	secretName := resources.Name(meter.Name, suffixConfig)
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: meter.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		generated.Labels = resources.Labels(meter, resources.ComponentNone)
		generated.Type = corev1.SecretTypeOpaque
		generated.Data = rendered.Data
		meter.Status.ConfigHash = rendered.Hash
		return controllerutil.SetControllerReference(meter, generated, r.Scheme)
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configuration secret: %w", err)
	}
	if err := r.reconcileServiceAccount(ctx, meter); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.removeObsoleteResources(ctx, meter); err != nil {
		return ctrl.Result{}, err
	}
	if resources.Mode(meter) == telemetryv1alpha1.MeterModeSharded {
		if err := r.reconcileShardingRBAC(ctx, meter); err != nil {
			return ctrl.Result{}, err
		}
	}
	for _, component := range resources.Components(meter) {
		if err := r.reconcileService(ctx, meter, component, false); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileService(ctx, meter, component, true); err != nil {
			return ctrl.Result{}, err
		}
		result, err := r.reconcileStatefulSet(ctx, meter, component, secretName, internalSecretName, internalSecretKey)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}
	if err := r.reconcileIngress(ctx, meter); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *MeterReconciler) reconcileServiceAccount(ctx context.Context, meter *telemetryv1alpha1.Meter) error {
	desired := resources.ServiceAccount(meter)
	current := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels = desired.Labels
		current.Annotations = desired.Annotations
		return controllerutil.SetControllerReference(meter, current, r.Scheme)
	})
	return err
}

func (r *MeterReconciler) reconcileShardingRBAC(ctx context.Context, meter *telemetryv1alpha1.Meter) error {
	desiredRole := resources.Role(meter)
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: desiredRole.Name, Namespace: desiredRole.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels, role.Rules = desiredRole.Labels, desiredRole.Rules
		return controllerutil.SetControllerReference(meter, role, r.Scheme)
	}); err != nil {
		return err
	}
	desiredBinding := resources.RoleBinding(meter)
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: desiredBinding.Name, Namespace: desiredBinding.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels, binding.Subjects, binding.RoleRef = desiredBinding.Labels, desiredBinding.Subjects, desiredBinding.RoleRef
		return controllerutil.SetControllerReference(meter, binding, r.Scheme)
	})
	return err
}

func (r *MeterReconciler) reconcileService(ctx context.Context, meter *telemetryv1alpha1.Meter, component resources.Component, headless bool) error {
	desired := resources.Service(meter, component, headless)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels, service.Annotations = desired.Labels, desired.Annotations
		service.Spec.Selector, service.Spec.Ports, service.Spec.Type = desired.Spec.Selector, desired.Spec.Ports, desired.Spec.Type
		if headless {
			service.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		return controllerutil.SetControllerReference(meter, service, r.Scheme)
	})
	return err
}

func (r *MeterReconciler) reconcileIngress(ctx context.Context, meter *telemetryv1alpha1.Meter) error {
	current := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: meter.Namespace}}
	if !meter.Spec.Ingress.Enabled {
		if err := r.Get(ctx, client.ObjectKeyFromObject(current), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		if metav1.IsControlledBy(current, meter) {
			return client.IgnoreNotFound(r.Delete(ctx, current))
		}
		return nil
	}

	desired := resources.Ingress(meter)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels = desired.Labels
		current.Annotations = desired.Annotations
		current.Spec = desired.Spec
		return controllerutil.SetControllerReference(meter, current, r.Scheme)
	})
	return err
}

func (r *MeterReconciler) removeObsoleteResources(ctx context.Context, meter *telemetryv1alpha1.Meter) error {
	var objects []client.Object
	if resources.Mode(meter) == telemetryv1alpha1.MeterModeSharded {
		objects = []client.Object{
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: meter.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: meter.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resources.Name(meter.Name, suffixHeadless), Namespace: meter.Namespace}},
		}
	} else {
		for _, component := range []resources.Component{resources.ComponentWriter, resources.ComponentReader} {
			name := resources.ComponentName(meter, component)
			objects = append(objects,
				&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resources.Name(name, suffixHeadless), Namespace: meter.Namespace}})
		}
		name := resources.Name(meter.Name, suffixSharding)
		objects = append(objects,
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace}},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace}})
	}
	for _, object := range objects {
		if err := r.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if metav1.IsControlledBy(object, meter) {
			if err := r.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (r *MeterReconciler) reconcileInternalToken(ctx context.Context, meter *telemetryv1alpha1.Meter) (string, string, []byte, error) {
	generatedName := resources.Name(meter.Name, suffixInternalToken)
	if ref := meter.Spec.Config.Auth.InternalTokenSecretRef; ref != nil {
		generated := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: meter.Namespace, Name: generatedName}, generated); err == nil {
			if metav1.IsControlledBy(generated, meter) {
				if err := r.Delete(ctx, generated); err != nil && !apierrors.IsNotFound(err) {
					return "", "", nil, fmt.Errorf("remove generated internal token Secret: %w", err)
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return "", "", nil, fmt.Errorf("get generated internal token Secret: %w", err)
		}
		token, err := r.secretValue(ctx, meter.Namespace, *ref)
		if err != nil {
			return "", "", nil, fmt.Errorf("resolve internal token: %w", err)
		}
		return ref.Name, ref.Key, token, nil
	}
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: generatedName, Namespace: meter.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		token := append([]byte(nil), generated.Data[resources.TokenKey]...)
		if len(token) == 0 {
			raw := make([]byte, 32)
			if _, err := rand.Read(raw); err != nil {
				return fmt.Errorf("generate internal token: %w", err)
			}
			token = []byte(base64.RawURLEncoding.EncodeToString(raw))
		}
		generated.Labels = resources.Labels(meter, resources.ComponentNone)
		generated.Type = corev1.SecretTypeOpaque
		generated.Data = map[string][]byte{resources.TokenKey: token}
		return controllerutil.SetControllerReference(meter, generated, r.Scheme)
	})
	if err != nil {
		return "", "", nil, fmt.Errorf("reconcile internal token Secret: %w", err)
	}
	return generatedName, resources.TokenKey, append([]byte(nil), generated.Data[resources.TokenKey]...), nil
}

func (r *MeterReconciler) resolveConfigInput(ctx context.Context, meter *telemetryv1alpha1.Meter, auths []telemetryv1alpha1.NamespaceAuthentication, internalToken []byte) (meterconfig.Input, error) {
	global, err := r.resolveAccess(ctx, meter.Namespace, meter.Spec.Config.Auth.Global)
	if err != nil {
		return meterconfig.Input{}, err
	}
	input := meterconfig.Input{Meter: meter, Global: global, InternalTokenPath: resources.InternalTokenPath, InternalToken: internalToken}
	for _, auth := range auths {
		if auth.Spec.DataStoreRef.Kind != dataStoreMeter || auth.Spec.DataStoreRef.Name != meter.Name {
			continue
		}
		if auth.Spec.Namespace == "" || auth.Spec.Username == "" || !lo.Contains([]string{permissionRead, permissionWrite}, auth.Spec.Permission) {
			return meterconfig.Input{}, fmt.Errorf("NamespaceAuthentication %s is invalid", auth.Name)
		}
		password, err := r.secretValue(ctx, meter.Namespace, auth.Spec.SecretKeyRef)
		if err != nil {
			return meterconfig.Input{}, fmt.Errorf("resolve NamespaceAuthentication %s password: %w", auth.Name, err)
		}
		credential := meterconfig.Credential{Username: auth.Spec.Username, Password: password, DataKey: "namespace-" + auth.Name + "-password"}
		access := meterconfig.Access{}
		if auth.Spec.Permission == permissionRead {
			access.Read = []meterconfig.Credential{credential}
		} else {
			access.Write = []meterconfig.Credential{credential}
		}
		input.Namespaces = append(input.Namespaces, meterconfig.NamespaceAccess{Name: auth.Spec.Namespace, KeyPrefix: auth.Name, Access: access})
	}
	if spec := meter.Spec.Config.Auth.JWT; spec != nil {
		input.JWT = &meterconfig.JWT{URL: spec.JWKS.URL, Issuer: spec.Issuer, Audience: spec.Audience, RefreshIntervalSeconds: spec.RefreshIntervalSeconds, RequestTimeoutSeconds: spec.RequestTimeoutSeconds}
		if spec.JWKS.SecretKeyRef != nil {
			input.JWT.JWKS, err = r.secretValue(ctx, meter.Namespace, *spec.JWKS.SecretKeyRef)
			if err != nil {
				return meterconfig.Input{}, fmt.Errorf("resolve JWT JWKS: %w", err)
			}
		}
	}
	return input, nil
}

func (r *MeterReconciler) resolveAccess(ctx context.Context, namespace string, access telemetryv1alpha1.AccessSpec) (meterconfig.Access, error) {
	result := meterconfig.Access{}
	for permission, specs := range map[string][]telemetryv1alpha1.BasicCredentialSpec{permissionRead: access.Read, permissionWrite: access.Write} {
		for i, spec := range specs {
			value, err := r.secretValue(ctx, namespace, spec.Password)
			if err != nil {
				return result, fmt.Errorf("resolve global %s credential %d: %w", permission, i, err)
			}
			credential := meterconfig.Credential{Username: spec.Username, Password: value}
			if permission == permissionRead {
				result.Read = append(result.Read, credential)
			} else {
				result.Write = append(result.Write, credential)
			}
		}
	}
	return result, nil
}

func (r *MeterReconciler) secretValue(ctx context.Context, namespace string, ref corev1.SecretKeySelector) ([]byte, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		if lo.FromPtrOr(ref.Optional, false) && apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	value, found := secret.Data[ref.Key]
	if !found {
		if lo.FromPtrOr(ref.Optional, false) {
			return nil, nil
		}
		return nil, fmt.Errorf("key %q not found in Secret %s/%s", ref.Key, namespace, ref.Name)
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("key %q in Secret %s/%s is empty", ref.Key, namespace, ref.Name)
	}
	return append([]byte(nil), value...), nil
}

func meterSecretNames(meter *telemetryv1alpha1.Meter) []string {
	names := lo.FilterMap(append(append([]telemetryv1alpha1.BasicCredentialSpec{}, meter.Spec.Config.Auth.Global.Read...), meter.Spec.Config.Auth.Global.Write...), func(spec telemetryv1alpha1.BasicCredentialSpec, _ int) (string, bool) {
		return spec.Password.Name, spec.Password.Name != ""
	})
	refs := []*corev1.SecretKeySelector{meter.Spec.Config.Auth.InternalTokenSecretRef}
	if meter.Spec.Config.Auth.JWT != nil {
		refs = append(refs, meter.Spec.Config.Auth.JWT.JWKS.SecretKeyRef)
	}
	for _, ref := range refs {
		if ref != nil && ref.Name != "" {
			names = append(names, ref.Name)
		}
	}
	return lo.Uniq(names)
}

func (r *MeterReconciler) workloadsReady(ctx context.Context, meter *telemetryv1alpha1.Meter) (bool, string, error) {
	for _, component := range resources.Components(meter) {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: meter.Namespace, Name: resources.ComponentName(meter, component)}, sts); err != nil {
			return false, "", err
		}
		desired := lo.FromPtrOr(sts.Spec.Replicas, int32(1))
		if sts.Status.ReadyReplicas < desired || sts.Status.ObservedGeneration < sts.Generation {
			return false, fmt.Sprintf("%s has %d/%d ready replicas", component, sts.Status.ReadyReplicas, desired), nil
		}
	}
	return true, messageWorkloadsReady, nil
}

func (r *MeterReconciler) setStatus(ctx context.Context, meter *telemetryv1alpha1.Meter, status metav1.ConditionStatus, reason, message, hash string) error {
	current := &telemetryv1alpha1.Meter{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(meter), current); err != nil {
		return err
	}
	base := current.DeepCopy()
	current.Status.ObservedGeneration = current.Generation
	current.Status.ConfigHash = hash
	if resources.Mode(current) == telemetryv1alpha1.MeterModeSharded {
		current.Status.WriterEndpoint = fmt.Sprintf("http://%s:%d", resources.ComponentName(current, resources.ComponentWriter), resources.HTTPPort(current))
		current.Status.ReaderEndpoint = fmt.Sprintf("http://%s:%d", resources.ComponentName(current, resources.ComponentReader), resources.HTTPPort(current))
		scaling, err := loadWriterScalingState(ctx, r.Client, current, dataStoreMeter, resources.Mode(current), writerReplicaIntent(current))
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

func (r *MeterReconciler) setupIndexes(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &telemetryv1alpha1.NamespaceAuthentication{}, namespaceAuthMeterIndex, func(obj client.Object) []string {
		auth := obj.(*telemetryv1alpha1.NamespaceAuthentication)
		if auth.Spec.DataStoreRef.Kind != dataStoreMeter || auth.Spec.DataStoreRef.Name == "" {
			return nil
		}
		return []string{auth.Spec.DataStoreRef.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &telemetryv1alpha1.Meter{}, meterSecretIndex, func(obj client.Object) []string {
		return meterSecretNames(obj.(*telemetryv1alpha1.Meter))
	}); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(context.Background(), &telemetryv1alpha1.NamespaceAuthentication{}, namespaceAuthSecretIndex, func(obj client.Object) []string {
		name := obj.(*telemetryv1alpha1.NamespaceAuthentication).Spec.SecretKeyRef.Name
		if name == "" {
			return nil
		}
		return []string{name}
	})
}

func (r *MeterReconciler) metersForNamespaceAuth(_ context.Context, obj client.Object) []reconcile.Request {
	auth := obj.(*telemetryv1alpha1.NamespaceAuthentication)
	if auth.Spec.DataStoreRef.Kind != dataStoreMeter || auth.Spec.DataStoreRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: auth.Namespace, Name: auth.Spec.DataStoreRef.Name}}}
}

func (r *MeterReconciler) metersForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var requests []reconcile.Request
	meters := &telemetryv1alpha1.MeterList{}
	if err := r.List(ctx, meters, client.InNamespace(obj.GetNamespace()), client.MatchingFields{meterSecretIndex: obj.GetName()}); err == nil {
		requests = append(requests, lo.Map(meters.Items, func(meter telemetryv1alpha1.Meter, _ int) reconcile.Request {
			return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: meter.Namespace, Name: meter.Name}}
		})...)
	}
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(obj.GetNamespace()), client.MatchingFields{namespaceAuthSecretIndex: obj.GetName()}); err == nil {
		for i := range auths.Items {
			requests = append(requests, r.metersForNamespaceAuth(ctx, &auths.Items[i])...)
		}
	}
	return lo.UniqBy(requests, func(request reconcile.Request) types.NamespacedName { return request.NamespacedName })
}

func (r *MeterReconciler) metersForPVC(_ context.Context, obj client.Object) []reconcile.Request {
	pvc := obj.(*corev1.PersistentVolumeClaim)
	name := pvc.Annotations[resources.MeterNameAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: pvc.Namespace, Name: name}}}
}
