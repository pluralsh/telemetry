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

	"k8s.io/apimachinery/pkg/api/equality"
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

const reasonSchemaConflict = "Conflict"

// NamespaceSchemaReconciler reports whether each NamespaceSchema is the
// one its datastore applies. The Vector controller renders the schemas into
// the servers' config.
type NamespaceSchemaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceschemas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceschemas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=namespaceschemas/finalizers,verbs=update
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=vectors,verbs=get;list;watch

func (r *NamespaceSchemaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	schema := &telemetryv1alpha1.NamespaceSchema{}
	if err := r.Get(ctx, req.NamespacedName, schema); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	status, reason, message := metav1.ConditionTrue, reasonReady, "the schema is declared in the datastore's config; its servers merge it into the namespace"
	if conflict, err := r.validate(ctx, schema); err != nil {
		status, reason, message = metav1.ConditionFalse, "Invalid", err.Error()
		if conflict {
			reason = reasonSchemaConflict
		}
	}
	base := schema.DeepCopy()
	schema.Status.ObservedGeneration = schema.Generation
	meta.SetStatusCondition(&schema.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message,
		ObservedGeneration: schema.Generation, LastTransitionTime: metav1.NewTime(time.Now()),
	})
	if equality.Semantic.DeepEqual(base.Status, schema.Status) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.Status().Patch(ctx, schema, client.MergeFrom(base))
}

func (r *NamespaceSchemaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&telemetryv1alpha1.NamespaceSchema{}).
		Watches(&telemetryv1alpha1.NamespaceSchema{}, handler.EnqueueRequestsFromMapFunc(r.schemasSharingDataStore)).
		Watches(&telemetryv1alpha1.Vector{}, handler.EnqueueRequestsFromMapFunc(r.schemasForVector)).
		Named("namespaceschema").
		Complete(r)
}

// validate reports why the schema doesn't apply, and whether that's because
// another NamespaceSchema declares the same namespace.
func (r *NamespaceSchemaReconciler) validate(ctx context.Context, schema *telemetryv1alpha1.NamespaceSchema) (bool, error) {
	if schema.Spec.DataStoreRef.Kind != dataStoreVector {
		return false, fmt.Errorf("dataStoreRef.kind must be Vector")
	}
	if len(schema.Spec.Schema) == 0 {
		return false, fmt.Errorf("schema must declare at least one attribute")
	}
	vector := &telemetryv1alpha1.Vector{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: schema.Namespace, Name: schema.Spec.DataStoreRef.Name}, vector); err != nil {
		return false, fmt.Errorf("referenced Vector is unavailable: %w", err)
	}
	schemas := &telemetryv1alpha1.NamespaceSchemaList{}
	if err := r.List(ctx, schemas, client.InNamespace(schema.Namespace)); err != nil {
		return false, fmt.Errorf("list namespace schemas: %w", err)
	}
	target := schema.TargetNamespace()
	if winner := effectiveSchemas(schemas.Items, vector.Name)[target]; winner != nil && winner.Name != schema.Name {
		return true, fmt.Errorf("NamespaceSchema %s also declares namespace %q and applies instead", winner.Name, target)
	}
	return false, nil
}

func (r *NamespaceSchemaReconciler) schemasSharingDataStore(ctx context.Context, obj client.Object) []reconcile.Request {
	schema := obj.(*telemetryv1alpha1.NamespaceSchema)
	return r.schemasForDataStore(ctx, schema.Namespace, schema.Spec.DataStoreRef.Name)
}

func (r *NamespaceSchemaReconciler) schemasForVector(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.schemasForDataStore(ctx, obj.GetNamespace(), obj.GetName())
}

func (r *NamespaceSchemaReconciler) schemasForDataStore(ctx context.Context, namespace, name string) []reconcile.Request {
	items := &telemetryv1alpha1.NamespaceSchemaList{}
	if err := r.List(ctx, items, client.InNamespace(namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range items.Items {
		if items.Items[i].Spec.DataStoreRef.Kind == dataStoreVector && items.Items[i].Spec.DataStoreRef.Name == name {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&items.Items[i])})
		}
	}
	return requests
}
