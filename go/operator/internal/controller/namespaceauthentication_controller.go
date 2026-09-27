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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

// NamespaceAuthenticationReconciler reconciles a NamespaceAuthentication object.
type NamespaceAuthenticationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceauthentications/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=meters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *NamespaceAuthenticationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	auth := &telemetryv1alpha1.NamespaceAuthentication{}
	if err := r.Get(ctx, req.NamespacedName, auth); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	status, reason, message := metav1.ConditionTrue, "Ready", "authentication reference is valid"
	if err := r.validate(ctx, auth); err != nil {
		status, reason, message = metav1.ConditionFalse, "Invalid", err.Error()
	}
	base := auth.DeepCopy()
	auth.Status.ObservedGeneration = auth.Generation
	meta.SetStatusCondition(&auth.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: status, Reason: reason, Message: message,
		ObservedGeneration: auth.Generation, LastTransitionTime: metav1.NewTime(time.Now()),
	})
	return ctrl.Result{}, r.Status().Patch(ctx, auth, client.MergeFrom(base))
}

func (r *NamespaceAuthenticationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.NamespaceAuthentication{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.authenticationsForSecret)).
		Watches(&telemetryv1alpha1.Meter{}, handler.EnqueueRequestsFromMapFunc(r.authenticationsForMeter)).
		Named("namespaceauthentication").
		Complete(r)
}

func (r *NamespaceAuthenticationReconciler) validate(ctx context.Context, auth *telemetryv1alpha1.NamespaceAuthentication) error {
	if auth.Spec.DataStoreRef.Kind != "Meter" {
		return fmt.Errorf("dataStoreRef.kind must be Meter")
	}
	if auth.Spec.DataStoreRef.Name == "" || auth.Spec.Namespace == "" || auth.Spec.Username == "" {
		return fmt.Errorf("dataStoreRef.name, namespace, and username are required")
	}
	if auth.Spec.Permission != "read" && auth.Spec.Permission != "write" {
		return fmt.Errorf("permission must be read or write")
	}
	if auth.Spec.SecretKeyRef.Name == "" || auth.Spec.SecretKeyRef.Key == "" {
		return fmt.Errorf("secretKeyRef.name and secretKeyRef.key are required")
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: auth.Namespace, Name: auth.Spec.DataStoreRef.Name}, &telemetryv1alpha1.Meter{}); err != nil {
		return fmt.Errorf("referenced Meter is unavailable: %w", err)
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: auth.Namespace, Name: auth.Spec.SecretKeyRef.Name}, secret); err != nil {
		return fmt.Errorf("referenced Secret is unavailable: %w", err)
	}
	if len(secret.Data[auth.Spec.SecretKeyRef.Key]) == 0 {
		return fmt.Errorf("secret key %q is missing or empty", auth.Spec.SecretKeyRef.Key)
	}
	return nil
}

func (r *NamespaceAuthenticationReconciler) authenticationsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	items := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, items, client.InNamespace(obj.GetNamespace()), client.MatchingFields{namespaceAuthSecretIndex: obj.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(items.Items))
	for i := range items.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&items.Items[i])})
	}
	return requests
}

func (r *NamespaceAuthenticationReconciler) authenticationsForMeter(ctx context.Context, obj client.Object) []reconcile.Request {
	items := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, items, client.InNamespace(obj.GetNamespace()), client.MatchingFields{namespaceAuthMeterIndex: obj.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(items.Items))
	for i := range items.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&items.Items[i])})
	}
	return requests
}
