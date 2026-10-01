package controller

import (
	"context"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
	k8sutils "github.com/pluralsh/telemetry/go/operator/internal/utils"
)

type StorageChangeError struct {
	Component resources.Component
	Volume    string
	Reason    string
}

func (e *StorageChangeError) Error() string {
	return fmt.Sprintf("%s storage %q cannot be changed: %s", e.Component, e.Volume, e.Reason)
}

func (r *MetricsReconciler) reconcileStatefulSet(ctx context.Context, metrics *telemetryv1alpha1.Metrics, component resources.Component, secretName, internalSecretName, internalSecretKey string) (ctrl.Result, error) {
	desired, err := resources.StatefulSet(resources.StatefulSetInput{
		Metrics: metrics, Component: component, ConfigSecretName: secretName,
		InternalTokenSecretName: internalSecretName, InternalTokenSecretKey: internalSecretKey,
		DefaultProductVersion: r.DefaultProductVersion,
	})
	if err != nil {
		var volumeErr *resources.VolumeError
		if errors.As(err, &volumeErr) {
			return ctrl.Result{}, &StorageChangeError{Component: component, Volume: volumeErr.Volume, Reason: volumeErr.Reason}
		}
		return ctrl.Result{}, err
	}
	if err := controllerutil.SetControllerReference(metrics, desired, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if component == resources.ComponentWriter {
		if err := synchronizeWriterReplicas(ctx, r.Client, metrics, dataStoreMetrics, resources.Mode(metrics), desired); err != nil {
			return ctrl.Result{}, err
		}
	}

	current := &appsv1.StatefulSet{}
	key := client.ObjectKeyFromObject(desired)
	if err := r.Get(ctx, key, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, err
		}
		return r.resizeResult(ctx, desired)
	}
	resizing, err := r.prepareStorageExpansion(ctx, component, current, desired)
	if err != nil {
		return ctrl.Result{}, err
	}
	if resizing {
		if err := k8sutils.OrphanDeleteStatefulSet(ctx, r.Client, current); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: storageResizeRequeue}, nil
	}

	base := current.DeepCopy()
	current.Labels = desired.Labels
	current.OwnerReferences = desired.OwnerReferences
	current.Spec = desired.Spec
	if err := r.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("update StatefulSet %s: %w", current.Name, err)
	}
	return r.resizeResult(ctx, desired)
}

func (r *MetricsReconciler) prepareStorageExpansion(ctx context.Context, component resources.Component, current, desired *appsv1.StatefulSet) (bool, error) {
	expansions, err := k8sutils.CompareStatefulSetClaims(current.Spec.VolumeClaimTemplates, desired.Spec.VolumeClaimTemplates)
	if err != nil {
		var claimErr *k8sutils.ClaimTemplateError
		if errors.As(err, &claimErr) {
			return false, &StorageChangeError{Component: component, Volume: claimErr.Claim, Reason: claimErr.Reason}
		}
		return false, err
	}
	if len(expansions) == 0 {
		return false, nil
	}
	replicas := int32(1)
	if current.Spec.Replicas != nil {
		replicas = *current.Spec.Replicas
	}
	if err := k8sutils.PatchStatefulSetPVCs(ctx, r.Client, current.Namespace, current.Name, replicas, expansions); err != nil {
		return false, err
	}
	return true, nil
}

func (r *MetricsReconciler) resizeResult(ctx context.Context, desired *appsv1.StatefulSet) (ctrl.Result, error) {
	replicas := int32(1)
	if desired.Spec.Replicas != nil {
		replicas = *desired.Spec.Replicas
	}
	pending, err := k8sutils.StatefulSetResizePending(ctx, r.Client, desired.Namespace, desired.Name, desired.Spec.VolumeClaimTemplates, replicas)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pending {
		return ctrl.Result{RequeueAfter: storageResizeRequeue}, nil
	}
	return ctrl.Result{}, nil
}
