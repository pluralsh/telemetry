/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
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
	k8sutils "github.com/pluralsh/telemetry/go/operator/internal/utils"
)

// VectorTopologyLabel names the Vector a VectorTopology belongs to. The
// elected writer sets it when it creates the topology.
const VectorTopologyLabel = "telemetry.plural.sh/vector"

type VectorReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	DefaultProductVersion string
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=vectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=vectors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=vectors/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=vectortopologies,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *VectorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vector := &telemetryv1alpha1.Vector{}
	if err := r.Get(ctx, req.NamespacedName, vector); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	layout, err := r.shardLayout(ctx, vector)
	if err != nil {
		return ctrl.Result{}, err
	}
	shards := layout.shards
	vector.Status.ActiveShards, vector.Status.TopologyGeneration = shards, layout.generation
	result, err := r.reconcile(ctx, vector, layout)
	if err != nil {
		var storageErr *StorageChangeError
		if errors.As(err, &storageErr) {
			return ctrl.Result{}, r.setStatus(ctx, vector, metav1.ConditionFalse, reasonStorageResizeBlocked, err.Error())
		}
		_ = r.setStatus(ctx, vector, metav1.ConditionFalse, reasonReconcileFailed, err.Error())
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		return result, r.setStatus(ctx, vector, metav1.ConditionFalse, reasonStorageResizing, "expanding persistent volumes")
	}
	if requested := lo.FromPtrOr(resources.Replicas(vector, componentWriter), 1); layout.writers > requested {
		message := fmt.Sprintf("writer.replicas is %d, but %d writers each own a shard; shards don't merge, so writers can't be removed, and the writer StatefulSet keeps %d replicas", requested, layout.writers, layout.writers)
		return ctrl.Result{}, r.setStatus(ctx, vector, metav1.ConditionFalse, reasonWriterScaleDownRejected, message)
	}
	ready, message, err := r.workloadsReady(ctx, vector, shards)
	if err != nil {
		return ctrl.Result{}, err
	}
	status, reason := metav1.ConditionFalse, reasonProgressing
	if ready {
		status, reason = metav1.ConditionTrue, reasonReady
	}
	return ctrl.Result{}, r.setStatus(ctx, vector, status, reason, message)
}

func (r *VectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.Vector{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Watches(&telemetryv1alpha1.VectorTopology{}, handler.EnqueueRequestsFromMapFunc(vectorForTopology)).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(vectorForPVC)).
		Watches(&telemetryv1alpha1.NamespaceAuthentication{}, handler.EnqueueRequestsFromMapFunc(vectorForNamespaceAuth)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.vectorForSecret)).
		Named("vector").
		Complete(r)
}

// shardLayout is what a sharded cluster's VectorTopology says about it.
type shardLayout struct {
	// shards are the active shards, which reader groups serve.
	shards     []int32
	generation int64
	// writers is how many writers own a shard or are getting one, the
	// fewest writer replicas the cluster can run with.
	writers int32
}

// shardLayout reads the cluster's VectorTopology. Before the elected writer
// creates it, the shards are those it will create: one per writer replica.
func (r *VectorReconciler) shardLayout(ctx context.Context, vector *telemetryv1alpha1.Vector) (shardLayout, error) {
	if resources.Mode(vector) != telemetryv1alpha1.ProductModeSharded {
		return shardLayout{}, nil
	}
	initial := lo.Range(int(lo.FromPtrOr(resources.Replicas(vector, componentWriter), 1)))
	fallback := shardLayout{shards: lo.Map(initial, func(shard, _ int) int32 { return int32(shard) })}
	topology := &telemetryv1alpha1.VectorTopology{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: vector.Namespace, Name: VectorTopologyName(vector.Name)}, topology); err != nil {
		if apierrors.IsNotFound(err) {
			return fallback, nil
		}
		return shardLayout{}, fmt.Errorf("get VectorTopology: %w", err)
	}
	if topology.Labels[VectorTopologyLabel] != vector.Name {
		return fallback, nil
	}
	return shardLayout{shards: topology.Spec.ActiveShards(), generation: topology.Spec.Generation, writers: topology.Spec.Writers()}, nil
}

// VectorTopologyName is the VectorTopology of the Vector `name`.
func VectorTopologyName(name string) string {
	return name + "-topology"
}

func (r *VectorReconciler) reconcile(ctx context.Context, vector *telemetryv1alpha1.Vector, layout shardLayout) (ctrl.Result, error) {
	secrets := vectorSecrets{config: resources.Name(vector.Name, suffixConfig)}
	var token []byte
	var err error
	if secrets.token, secrets.tokenKey, token, err = r.reconcileInternalToken(ctx, vector); err != nil {
		return ctrl.Result{}, err
	}
	input, err := r.resolveConfigInput(ctx, vector, token)
	if err != nil {
		return ctrl.Result{}, err
	}
	rendered, err := productconfig.Render(input)
	if err != nil {
		return ctrl.Result{}, err
	}
	config := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secrets.config, Namespace: vector.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, config, func() error {
		config.Labels, config.Type, config.Data = resources.Labels(vector, componentNone), corev1.SecretTypeOpaque, rendered.Data
		vector.Status.ConfigHash = rendered.Hash
		return controllerutil.SetControllerReference(vector, config, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configuration secret: %w", err)
	}
	desiredAccount := resources.ServiceAccount(vector)
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: desiredAccount.Name, Namespace: desiredAccount.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, account, func() error {
		account.Labels, account.Annotations = desiredAccount.Labels, desiredAccount.Annotations
		return controllerutil.SetControllerReference(vector, account, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile service account: %w", err)
	}
	if err := r.reconcileIngress(ctx, vector); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile ingress: %w", err)
	}

	if resources.Mode(vector) != telemetryv1alpha1.ProductModeSharded {
		if err := r.reconcileService(ctx, vector, resources.Service(vector, componentStandalone, false)); err != nil {
			return ctrl.Result{}, err
		}
		if err := reconcileHeadlessService(ctx, r.Client, r.Scheme, vector, resources.Service(vector, componentStandalone, true)); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile headless service: %w", err)
		}
		return r.reconcileStatefulSet(ctx, vector, componentStandalone, nil, secrets, 0)
	}

	if err := r.reconcileRBAC(ctx, vector); err != nil {
		return ctrl.Result{}, err
	}
	// Every replica routes every request, so the entry Service selects
	// writers and readers alike.
	if err := r.reconcileService(ctx, vector, resources.Service(vector, componentNone, false)); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconcileHeadlessService(ctx, r.Client, r.Scheme, vector, resources.Service(vector, componentWriter, true)); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile writer headless service: %w", err)
	}
	if result, err := r.reconcileStatefulSet(ctx, vector, componentWriter, nil, secrets, layout.writers); err != nil || !result.IsZero() {
		return result, err
	}
	return r.reconcileReaders(ctx, vector, layout.shards, secrets)
}

// reconcileReaders runs one reader group per active shard, and once every
// group is ready, removes the groups of shards that split.
func (r *VectorReconciler) reconcileReaders(ctx context.Context, vector *telemetryv1alpha1.Vector, shards []int32, secrets vectorSecrets) (ctrl.Result, error) {
	targets := resources.ReaderTargetsForShards(vector, shards)
	keep := readerObjects{services: map[string]bool{}, workloads: map[string]bool{}}
	for _, target := range targets {
		headless := resources.Service(vector, componentReader, true)
		if target.Shard != nil {
			headless = resources.ReaderGroupService(vector, *target.Shard)
		}
		if err := reconcileHeadlessService(ctx, r.Client, r.Scheme, vector, headless); err != nil {
			return ctrl.Result{}, err
		}
		keep.workloads[target.Name], keep.services[headless.Name] = true, true
		if result, err := r.reconcileStatefulSet(ctx, vector, componentReader, target.Shard, secrets, 0); err != nil || !result.IsZero() {
			return result, err
		}
		if err := reconcilePodDisruptionBudget(ctx, r.Client, r.Scheme, vector, resources.PodDisruptionBudget(vector, target)); err != nil {
			return ctrl.Result{}, err
		}
		if err := reconcileAutoscaler(ctx, r.Client, r.Scheme, vector, target); err != nil {
			return ctrl.Result{}, err
		}
	}
	ready, err := readersReady(ctx, r.Client, vector, targets)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, pruneReaders(ctx, r.Client, vector, keep, ready)
}

func (r *VectorReconciler) reconcileIngress(ctx context.Context, vector *telemetryv1alpha1.Vector) error {
	current := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: vector.Name, Namespace: vector.Namespace}}
	if !vector.Spec.Ingress.Enabled {
		if err := r.Get(ctx, client.ObjectKeyFromObject(current), current); err != nil {
			return client.IgnoreNotFound(err)
		}
		if metav1.IsControlledBy(current, vector) {
			return client.IgnoreNotFound(r.Delete(ctx, current))
		}
		return nil
	}
	desired := resources.Ingress(vector)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, current, func() error {
		current.Labels, current.Annotations, current.Spec = desired.Labels, desired.Annotations, desired.Spec
		return controllerutil.SetControllerReference(vector, current, r.Scheme)
	})
	return err
}

func (r *VectorReconciler) reconcileService(ctx context.Context, vector *telemetryv1alpha1.Vector, desired *corev1.Service) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels, service.Annotations = desired.Labels, desired.Annotations
		service.Spec.Selector, service.Spec.Ports, service.Spec.Type = desired.Spec.Selector, desired.Spec.Ports, desired.Spec.Type
		return controllerutil.SetControllerReference(vector, service, r.Scheme)
	}); err != nil {
		return fmt.Errorf("reconcile service %s: %w", desired.Name, err)
	}
	return nil
}

// reconcileRBAC lets replicas hold shard and coordinator Leases, maintain
// the VectorTopology, and read the writer StatefulSet's replicas and pods.
func (r *VectorReconciler) reconcileRBAC(ctx context.Context, vector *telemetryv1alpha1.Vector) error {
	desiredRole := resources.Role(vector)
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: desiredRole.Name, Namespace: desiredRole.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels, role.Rules = desiredRole.Labels, desiredRole.Rules
		return controllerutil.SetControllerReference(vector, role, r.Scheme)
	}); err != nil {
		return fmt.Errorf("reconcile role: %w", err)
	}
	desiredBinding := resources.RoleBinding(vector)
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: desiredBinding.Name, Namespace: desiredBinding.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels, binding.Subjects, binding.RoleRef = desiredBinding.Labels, desiredBinding.Subjects, desiredBinding.RoleRef
		return controllerutil.SetControllerReference(vector, binding, r.Scheme)
	}); err != nil {
		return fmt.Errorf("reconcile role binding: %w", err)
	}
	return nil
}

// reconcileStatefulSet runs at least minReplicas replicas of component.
func (r *VectorReconciler) reconcileStatefulSet(ctx context.Context, vector *telemetryv1alpha1.Vector, component resources.Component, shard *int32, secrets vectorSecrets, minReplicas int32) (ctrl.Result, error) {
	desired, err := resources.StatefulSet(resources.StatefulSetInput{
		Vector: vector, Component: component, ConfigSecretName: secrets.config,
		InternalTokenSecretName: secrets.token, InternalTokenSecretKey: secrets.tokenKey,
		DefaultProductVersion: r.DefaultProductVersion, Shard: shard,
	})
	if err != nil {
		var volumeErr *resources.VolumeError
		if errors.As(err, &volumeErr) {
			return ctrl.Result{}, &StorageChangeError{Component: component, Volume: volumeErr.Volume, Reason: volumeErr.Reason}
		}
		return ctrl.Result{}, err
	}
	if err := controllerutil.SetControllerReference(vector, desired, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if minReplicas > lo.FromPtrOr(desired.Spec.Replicas, int32(1)) {
		desired.Spec.Replicas = lo.ToPtr(minReplicas)
	}
	replicas := lo.FromPtrOr(desired.Spec.Replicas, int32(1))
	current := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, err
		}
		return r.resizeResult(ctx, desired, replicas)
	}
	keepAutoscaledReplicas(vector, component, current, desired)
	expansions, err := k8sutils.CompareStatefulSetClaims(current.Spec.VolumeClaimTemplates, desired.Spec.VolumeClaimTemplates)
	if err != nil {
		var claimErr *k8sutils.ClaimTemplateError
		if errors.As(err, &claimErr) {
			return ctrl.Result{}, &StorageChangeError{Component: component, Volume: claimErr.Claim, Reason: claimErr.Reason}
		}
		return ctrl.Result{}, err
	}
	if len(expansions) > 0 {
		if err := k8sutils.PatchStatefulSetPVCs(ctx, r.Client, current.Namespace, current.Name, lo.FromPtrOr(current.Spec.Replicas, int32(1)), expansions); err != nil {
			return ctrl.Result{}, err
		}
		if err := k8sutils.OrphanDeleteStatefulSet(ctx, r.Client, current); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: storageResizeRequeue}, nil
	}
	base := current.DeepCopy()
	current.Labels, current.OwnerReferences, current.Spec = desired.Labels, desired.OwnerReferences, desired.Spec
	if err := r.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("update StatefulSet %s: %w", current.Name, err)
	}
	return r.resizeResult(ctx, desired, replicas)
}

func (r *VectorReconciler) resizeResult(ctx context.Context, desired *appsv1.StatefulSet, replicas int32) (ctrl.Result, error) {
	pending, err := k8sutils.StatefulSetResizePending(ctx, r.Client, desired.Namespace, desired.Name, desired.Spec.VolumeClaimTemplates, replicas)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pending {
		return ctrl.Result{RequeueAfter: storageResizeRequeue}, nil
	}
	return ctrl.Result{}, nil
}

func (r *VectorReconciler) workloadsReady(ctx context.Context, vector *telemetryv1alpha1.Vector, shards []int32) (bool, string, error) {
	names := []string{resources.ComponentName(vector, componentStandalone)}
	if resources.Mode(vector) == telemetryv1alpha1.ProductModeSharded {
		targets := resources.ReaderTargetsForShards(vector, shards)
		names = make([]string, 0, len(targets)+1)
		names = append(names, resources.ComponentName(vector, componentWriter))
		for _, target := range targets {
			names = append(names, target.Name)
		}
	}
	for _, name := range names {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: vector.Namespace, Name: name}, sts); err != nil {
			if apierrors.IsNotFound(err) {
				return false, fmt.Sprintf("StatefulSet %s does not exist yet", name), nil
			}
			return false, "", err
		}
		want := lo.FromPtrOr(sts.Spec.Replicas, int32(1))
		if sts.Status.ObservedGeneration < sts.Generation || sts.Status.ReadyReplicas < want {
			return false, fmt.Sprintf("%s has %d/%d ready replicas", name, sts.Status.ReadyReplicas, want), nil
		}
	}
	return true, messageWorkloadsReady, nil
}

func (r *VectorReconciler) setStatus(ctx context.Context, vector *telemetryv1alpha1.Vector, status metav1.ConditionStatus, reason, message string) error {
	current := &telemetryv1alpha1.Vector{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(vector), current); err != nil {
		return err
	}
	base := current.DeepCopy()
	current.Status.ObservedGeneration = current.Generation
	current.Status.ConfigHash = vector.Status.ConfigHash
	current.Status.ActiveShards = vector.Status.ActiveShards
	current.Status.TopologyGeneration = vector.Status.TopologyGeneration
	current.Status.Endpoint = fmt.Sprintf("http://%s.%s.svc:%d", current.Name, current.Namespace, resources.HTTPPort(current))
	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message,
		ObservedGeneration: current.Generation, LastTransitionTime: metav1.NewTime(time.Now()),
	})
	if equality.Semantic.DeepEqual(base.Status, current.Status) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(base))
}

func vectorForTopology(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetLabels()[VectorTopologyLabel]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}

func vectorForPVC(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetAnnotations()[resources.VectorNameAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}
