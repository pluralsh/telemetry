package controller

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

// readerGroupCount is the number of storage shards, which never shrinks: the
// larger of the authoritative ShardMap's count and the writer replicas the
// coordinator will next assign shards to.
func readerGroupCount(ctx context.Context, c client.Client, product client.Object, kind string) (int32, error) {
	count := writerReplicaIntent(product)
	writer := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: product.GetNamespace(), Name: writerStatefulSetName(product)}, writer); err == nil {
		count = max(count, lo.FromPtrOr(writer.Spec.Replicas, int32(1)))
	} else if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("get writer StatefulSet: %w", err)
	}
	shardMap := &telemetryv1alpha1.ShardMap{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: product.GetNamespace(), Name: shardMapName(product)}, shardMap); err == nil {
		if shardMapBelongsToProduct(shardMap, product, kind) {
			count = max(count, shardMap.Spec.ShardCount)
		}
	} else if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("get ShardMap: %w", err)
	}
	return max(count, 1), nil
}

// readerTargets are the reader StatefulSets a sharded product runs.
func readerTargets(ctx context.Context, c client.Client, product client.Object, kind string) ([]resources.ReaderTarget, error) {
	groups := int32(1)
	if resources.ReadGroupsEnabled(product) {
		var err error
		if groups, err = readerGroupCount(ctx, c, product, kind); err != nil {
			return nil, err
		}
	}
	return resources.ReaderTargets(product, groups), nil
}

// reconcileReaders reconciles every reader StatefulSet through `statefulSet`
// with the Services, disruption budgets, and autoscalers around them, then
// removes reader objects of groups or topologies no longer in use.
func reconcileReaders(
	ctx context.Context, c client.Client, scheme *runtime.Scheme, product client.Object, kind string,
	statefulSet func(shard *int32) (ctrl.Result, error),
) (ctrl.Result, error) {
	targets, err := readerTargets(ctx, c, product, kind)
	if err != nil {
		return ctrl.Result{}, err
	}
	keep := map[string]bool{resources.ComponentName(product, resources.ComponentReader): true}
	for _, target := range targets {
		headless := resources.Service(product, resources.ComponentReader, true)
		if target.Shard != nil {
			headless = resources.ReaderGroupService(product, *target.Shard)
		}
		if err := reconcileHeadlessService(ctx, c, scheme, product, headless); err != nil {
			return ctrl.Result{}, err
		}
		keep[target.Name], keep[headless.Name] = true, true

		result, err := statefulSet(target.Shard)
		if err != nil || !result.IsZero() {
			return result, err
		}
		if err := reconcilePodDisruptionBudget(ctx, c, scheme, product, resources.PodDisruptionBudget(product, target)); err != nil {
			return ctrl.Result{}, err
		}
		if err := reconcileAutoscaler(ctx, c, scheme, product, target); err != nil {
			return ctrl.Result{}, err
		}
	}
	ready, err := readersReady(ctx, c, product, targets)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Readers of a replaced topology keep serving until every new reader is
	// ready; the owned StatefulSets' status updates trigger the next pass.
	return ctrl.Result{}, pruneReaders(ctx, c, product, keep, ready)
}

func readersReady(ctx context.Context, c client.Client, product client.Object, targets []resources.ReaderTarget) (bool, error) {
	for _, target := range targets {
		sts := &appsv1.StatefulSet{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: product.GetNamespace(), Name: target.Name}, sts); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if sts.Status.ObservedGeneration < sts.Generation || sts.Status.ReadyReplicas < lo.FromPtrOr(sts.Spec.Replicas, int32(1)) {
			return false, nil
		}
	}
	return true, nil
}

func reconcileHeadlessService(ctx context.Context, c client.Client, scheme *runtime.Scheme, product client.Object, desired *corev1.Service) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, service, func() error {
		service.Labels, service.Annotations = desired.Labels, desired.Annotations
		service.Spec.Selector, service.Spec.Ports, service.Spec.Type = desired.Spec.Selector, desired.Spec.Ports, desired.Spec.Type
		service.Spec.ClusterIP = desired.Spec.ClusterIP
		return controllerutil.SetControllerReference(product, service, scheme)
	})
	return err
}

func reconcilePodDisruptionBudget(ctx context.Context, c client.Client, scheme *runtime.Scheme, product client.Object, desired *policyv1.PodDisruptionBudget) error {
	budget := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, budget, func() error {
		budget.Labels, budget.Spec = desired.Labels, desired.Spec
		return controllerutil.SetControllerReference(product, budget, scheme)
	})
	return err
}

func reconcileAutoscaler(ctx context.Context, c client.Client, scheme *runtime.Scheme, product client.Object, target resources.ReaderTarget) error {
	desired := resources.HorizontalPodAutoscaler(product, target)
	autoscaler := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: target.Name, Namespace: product.GetNamespace()}}
	if desired == nil {
		return deleteControlled(ctx, c, product, autoscaler)
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, autoscaler, func() error {
		autoscaler.Labels, autoscaler.Spec = desired.Labels, desired.Spec
		return controllerutil.SetControllerReference(product, autoscaler, scheme)
	})
	return err
}

func deleteControlled(ctx context.Context, c client.Client, product, object client.Object) error {
	if err := c.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(object, product) {
		return nil
	}
	return client.IgnoreNotFound(c.Delete(ctx, object))
}

// pruneReaders deletes the reader objects this product controls that are
// not in `keep`. Unless `workloads` is set, StatefulSets and the Services
// that resolve their pods are left serving.
func pruneReaders(ctx context.Context, c client.Client, product client.Object, keep map[string]bool, workloads bool) error {
	selector := client.MatchingLabels(resources.SelectorLabels(product, resources.ComponentReader))
	namespace := client.InNamespace(product.GetNamespace())
	lists := []client.ObjectList{&policyv1.PodDisruptionBudgetList{}, &autoscalingv2.HorizontalPodAutoscalerList{}}
	if workloads {
		lists = append(lists, &appsv1.StatefulSetList{}, &corev1.ServiceList{})
	}
	for _, list := range lists {
		if err := c.List(ctx, list, namespace, selector); err != nil {
			return err
		}
		items, err := metaItems(list)
		if err != nil {
			return err
		}
		for _, object := range items {
			if keep[object.GetName()] || !metav1.IsControlledBy(object, product) {
				continue
			}
			if err := c.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func metaItems(list client.ObjectList) ([]client.Object, error) {
	switch list := list.(type) {
	case *appsv1.StatefulSetList:
		return lo.Map(list.Items, func(item appsv1.StatefulSet, _ int) client.Object { return &item }), nil
	case *corev1.ServiceList:
		return lo.Map(list.Items, func(item corev1.Service, _ int) client.Object { return &item }), nil
	case *policyv1.PodDisruptionBudgetList:
		return lo.Map(list.Items, func(item policyv1.PodDisruptionBudget, _ int) client.Object { return &item }), nil
	case *autoscalingv2.HorizontalPodAutoscalerList:
		return lo.Map(list.Items, func(item autoscalingv2.HorizontalPodAutoscaler, _ int) client.Object { return &item }), nil
	default:
		return nil, fmt.Errorf("unsupported list %T", list)
	}
}

// workloadStatefulSets are the StatefulSet names whose readiness gates the
// product's readiness.
func workloadStatefulSets(ctx context.Context, c client.Client, product client.Object, kind string) ([]string, error) {
	if resources.Mode(product) != telemetryv1alpha1.ProductModeSharded {
		return []string{resources.ComponentName(product, resources.ComponentStandalone)}, nil
	}
	targets, err := readerTargets(ctx, c, product, kind)
	if err != nil {
		return nil, err
	}
	return append([]string{resources.ComponentName(product, resources.ComponentWriter)},
		lo.Map(targets, func(target resources.ReaderTarget, _ int) string { return target.Name })...), nil
}

// keepAutoscaledReplicas leaves an autoscaled StatefulSet's replicas to its
// HorizontalPodAutoscaler.
func keepAutoscaledReplicas(product any, component resources.Component, current, desired *appsv1.StatefulSet) {
	if resources.Autoscaling(product, component) != nil && current.Spec.Replicas != nil {
		desired.Spec.Replicas = lo.ToPtr(*current.Spec.Replicas)
	}
}
