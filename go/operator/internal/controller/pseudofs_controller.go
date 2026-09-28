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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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

type PseudoFSReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	DefaultProductVersion string
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=pseudofs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=pseudofs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=pseudofs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

func (r *PseudoFSReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pseudofs := &telemetryv1alpha1.PseudoFS{}
	if err := r.Get(ctx, req.NamespacedName, pseudofs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	result, err := r.reconcile(ctx, pseudofs)
	if err != nil {
		var storageErr *StorageChangeError
		reason := reasonReconcileFailed
		if errors.As(err, &storageErr) {
			reason = reasonStorageResizeBlocked
			_ = r.setStatus(ctx, pseudofs, metav1.ConditionFalse, reason, err.Error(), pseudofs.Status.ConfigHash)
			return ctrl.Result{}, nil
		}
		_ = r.setStatus(ctx, pseudofs, metav1.ConditionFalse, reason, err.Error(), pseudofs.Status.ConfigHash)
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		return result, r.setStatus(ctx, pseudofs, metav1.ConditionFalse, reasonStorageResizing, "expanding persistent volumes", pseudofs.Status.ConfigHash)
	}
	ready, message, err := r.workloadReady(ctx, pseudofs)
	if err != nil {
		return ctrl.Result{}, err
	}
	status, reason := metav1.ConditionFalse, reasonProgressing
	if ready {
		status, reason = metav1.ConditionTrue, reasonReady
	}
	return ctrl.Result{}, r.setStatus(ctx, pseudofs, status, reason, message, pseudofs.Status.ConfigHash)
}

func (r *PseudoFSReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.PseudoFS{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&appsv1.StatefulSet{}).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.pseudoFSForPVC)).
		Named("pseudofs").
		Complete(r)
}

func (r *PseudoFSReconciler) reconcile(ctx context.Context, pseudofs *telemetryv1alpha1.PseudoFS) (ctrl.Result, error) {
	rendered, err := productconfig.Render(productconfig.Input{PseudoFS: pseudofs})
	if err != nil {
		return ctrl.Result{}, err
	}
	configName := resources.Name(pseudofs.Name, suffixConfig)
	config := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: pseudofs.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, config, func() error {
		config.Labels, config.Type, config.Data = resources.Labels(pseudofs, componentNone), corev1.SecretTypeOpaque, rendered.Data
		pseudofs.Status.ConfigHash = rendered.Hash
		return controllerutil.SetControllerReference(pseudofs, config, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configuration secret: %w", err)
	}
	desiredAccount := resources.ServiceAccount(pseudofs)
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: desiredAccount.Name, Namespace: desiredAccount.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, account, func() error {
		account.Labels, account.Annotations = desiredAccount.Labels, desiredAccount.Annotations
		return controllerutil.SetControllerReference(pseudofs, account, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile service account: %w", err)
	}
	desiredService := resources.Service(pseudofs, componentStandalone, false)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desiredService.Name, Namespace: desiredService.Namespace}}
	if _, err = controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels, service.Annotations = desiredService.Labels, desiredService.Annotations
		service.Spec.Selector, service.Spec.Ports, service.Spec.Type = desiredService.Spec.Selector, desiredService.Spec.Ports, desiredService.Spec.Type
		return controllerutil.SetControllerReference(pseudofs, service, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile service: %w", err)
	}
	return r.reconcileStatefulSet(ctx, pseudofs, configName)
}

func (r *PseudoFSReconciler) workloadReady(ctx context.Context, pseudofs *telemetryv1alpha1.PseudoFS) (bool, string, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pseudofs), sts); err != nil {
		return false, "", err
	}
	if sts.Status.ReadyReplicas < 1 || sts.Status.ObservedGeneration < sts.Generation {
		return false, fmt.Sprintf("standalone has %d/1 ready replicas", sts.Status.ReadyReplicas), nil
	}
	return true, messageWorkloadsReady, nil
}

func (r *PseudoFSReconciler) setStatus(ctx context.Context, pseudofs *telemetryv1alpha1.PseudoFS, status metav1.ConditionStatus, reason, message, hash string) error {
	current := &telemetryv1alpha1.PseudoFS{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pseudofs), current); err != nil {
		return err
	}
	base := current.DeepCopy()
	current.Status.ObservedGeneration = current.Generation
	current.Status.ConfigHash = hash
	current.Status.GRPCEndpoint = fmt.Sprintf("%s:%d", current.Name, resources.GRPCPort(current))
	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message,
		ObservedGeneration: current.Generation, LastTransitionTime: metav1.NewTime(time.Now()),
	})
	if equality.Semantic.DeepEqual(base.Status, current.Status) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(base))
}

func (r *PseudoFSReconciler) pseudoFSForPVC(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetAnnotations()[resources.PseudoFSNameAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}
