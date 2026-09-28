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

func (r *PseudoFSReconciler) reconcileStatefulSet(ctx context.Context, pseudofs *telemetryv1alpha1.PseudoFS, configName string) (ctrl.Result, error) {
	desired, err := resources.StatefulSet(resources.StatefulSetInput{
		PseudoFS: pseudofs, Component: resources.ComponentStandalone, ConfigSecretName: configName,
		DefaultProductVersion: r.DefaultProductVersion,
	})
	if err != nil {
		var volumeErr *resources.VolumeError
		if errors.As(err, &volumeErr) {
			return ctrl.Result{}, &StorageChangeError{Component: componentStandalone, Volume: volumeErr.Volume, Reason: volumeErr.Reason}
		}
		return ctrl.Result{}, err
	}
	if err := controllerutil.SetControllerReference(pseudofs, desired, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	current := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, desired); err != nil {
			return ctrl.Result{}, err
		}
		return r.resizeResult(ctx, desired)
	}
	expansions, err := k8sutils.CompareStatefulSetClaims(current.Spec.VolumeClaimTemplates, desired.Spec.VolumeClaimTemplates)
	if err != nil {
		var claimErr *k8sutils.ClaimTemplateError
		if errors.As(err, &claimErr) {
			return ctrl.Result{}, &StorageChangeError{Component: componentStandalone, Volume: claimErr.Claim, Reason: claimErr.Reason}
		}
		return ctrl.Result{}, err
	}
	if len(expansions) > 0 {
		if err := k8sutils.PatchStatefulSetPVCs(ctx, r.Client, current.Namespace, current.Name, 1, expansions); err != nil {
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
	return r.resizeResult(ctx, desired)
}

func (r *PseudoFSReconciler) resizeResult(ctx context.Context, desired *appsv1.StatefulSet) (ctrl.Result, error) {
	pending, err := k8sutils.StatefulSetResizePending(ctx, r.Client, desired.Namespace, desired.Name, desired.Spec.VolumeClaimTemplates, 1)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pending {
		return ctrl.Result{RequeueAfter: storageResizeRequeue}, nil
	}
	return ctrl.Result{}, nil
}
