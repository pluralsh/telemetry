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

	"gocloud.dev/blob"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/objectstore"
)

const (
	bucketCleanerFinalizer   = "telemetry.plural.sh/bucket-cleaner"
	bucketCleanerSecretIndex = "telemetry.plural.sh/bucket-cleaner-secret"

	reasonBucketAccessible   = "BucketAccessible"
	reasonBucketInaccessible = "BucketInaccessible"
	reasonDeleting           = "Deleting"
	reasonDeletionFailed     = "DeletionFailed"

	defaultBucketCheckInterval = 5 * time.Minute
	bucketCheckRetry           = 30 * time.Second
	defaultDeleteBudget        = 2 * time.Minute
	defaultDeleteConcurrency   = 32
)

// BucketCleanerReconciler verifies access to a BucketCleaner's prefix while
// it exists and deletes everything under the prefix before releasing its
// finalizer.
type BucketCleanerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// OpenBucket defaults to objectstore.Open.
	OpenBucket objectstore.Opener
	// DeleteBudget bounds one deletion pass so a large prefix does not hold a
	// worker; remaining objects are deleted by the next pass.
	DeleteBudget      time.Duration
	DeleteConcurrency int
}

// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=bucketcleaners,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=bucketcleaners/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=telemetry.plural.sh,resources=bucketcleaners/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *BucketCleanerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cleaner := &telemetryv1alpha1.BucketCleaner{}
	if err := r.Get(ctx, req.NamespacedName, cleaner); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cleaner.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, cleaner)
	}
	if controllerutil.AddFinalizer(cleaner, bucketCleanerFinalizer) {
		if err := r.Update(ctx, cleaner); err != nil {
			return ctrl.Result{}, err
		}
	}
	prefix, err := objectstore.Prefix(cleaner.Spec.SubPath)
	if err == nil {
		err = r.checkAccess(ctx, cleaner, prefix)
	}
	if err != nil {
		retry := min(bucketCheckInterval(cleaner), bucketCheckRetry)
		return ctrl.Result{RequeueAfter: retry}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonBucketInaccessible, err.Error(), nil)
	}
	message := "operator can access " + objectstore.Location(cleaner.Spec.ObjectStore, prefix)
	now := metav1.Now()
	return ctrl.Result{RequeueAfter: bucketCheckInterval(cleaner)}, r.setStatus(ctx, cleaner, metav1.ConditionTrue, reasonBucketAccessible, message, func(status *telemetryv1alpha1.BucketCleanerStatus) {
		status.LastCheckTime = &now
	})
}

func (r *BucketCleanerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &telemetryv1alpha1.BucketCleaner{}, bucketCleanerSecretIndex, func(obj client.Object) []string {
		return objectStoreSecretNames(obj.(*telemetryv1alpha1.BucketCleaner).Spec.ObjectStore)
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		// Status writes record each periodic check, so only spec changes and
		// deletion trigger a reconcile; the check itself is timer driven.
		For(&telemetryv1alpha1.BucketCleaner{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, deletionStartedPredicate()))).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.cleanersForSecret)).
		Named("bucketcleaner").
		Complete(r)
}

func (r *BucketCleanerReconciler) finalize(ctx context.Context, cleaner *telemetryv1alpha1.BucketCleaner) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cleaner, bucketCleanerFinalizer) {
		return ctrl.Result{}, nil
	}
	prefix, err := objectstore.Prefix(cleaner.Spec.SubPath)
	if err != nil {
		return ctrl.Result{}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeletionFailed, err.Error(), nil)
	}
	bucket, err := r.open(ctx, cleaner)
	if err != nil {
		_ = r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeletionFailed, err.Error(), nil)
		return ctrl.Result{}, err
	}
	defer func() { _ = bucket.Close() }()
	location := objectstore.Location(cleaner.Spec.ObjectStore, prefix)
	passCtx, cancel := context.WithTimeout(ctx, r.deleteBudget())
	defer cancel()
	deleted, err := objectstore.DeletePrefix(passCtx, bucket, prefix, r.deleteConcurrency())
	total := cleaner.Status.DeletedObjects + deleted
	recordDeleted := func(status *telemetryv1alpha1.BucketCleanerStatus) { status.DeletedObjects = total }
	if err != nil {
		if passCtx.Err() != nil && ctx.Err() == nil {
			message := fmt.Sprintf("deleted %d objects under %s so far", total, location)
			return ctrl.Result{RequeueAfter: time.Second}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeleting, message, recordDeleted)
		}
		message := fmt.Sprintf("deleting objects under %s: %v", location, err)
		_ = r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeletionFailed, message, recordDeleted)
		return ctrl.Result{}, err
	}
	ctrl.LoggerFrom(ctx).Info("deleted bucket prefix", "location", location, "objects", total)
	base := cleaner.DeepCopy()
	controllerutil.RemoveFinalizer(cleaner, bucketCleanerFinalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, cleaner, client.MergeFrom(base)))
}

func (r *BucketCleanerReconciler) checkAccess(ctx context.Context, cleaner *telemetryv1alpha1.BucketCleaner, prefix string) error {
	bucket, err := r.open(ctx, cleaner)
	if err != nil {
		return err
	}
	defer func() { _ = bucket.Close() }()
	return objectstore.CheckAccess(ctx, bucket, prefix)
}

func (r *BucketCleanerReconciler) open(ctx context.Context, cleaner *telemetryv1alpha1.BucketCleaner) (*blob.Bucket, error) {
	open := r.OpenBucket
	if open == nil {
		open = objectstore.Open
	}
	bucket, err := open(ctx, cleaner.Spec.ObjectStore, r.secretFunc(cleaner.Namespace))
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", objectstore.Location(cleaner.Spec.ObjectStore, ""), err)
	}
	return bucket, nil
}

func (r *BucketCleanerReconciler) secretFunc(namespace string) objectstore.SecretFunc {
	return func(ctx context.Context, ref *corev1.SecretKeySelector) ([]byte, error) {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, secret); err != nil {
			return nil, fmt.Errorf("referenced Secret %q is unavailable: %w", ref.Name, err)
		}
		value := secret.Data[ref.Key]
		if len(value) == 0 {
			return nil, fmt.Errorf("secret %q key %q is missing or empty", ref.Name, ref.Key)
		}
		return value, nil
	}
}

func (r *BucketCleanerReconciler) setStatus(ctx context.Context, cleaner *telemetryv1alpha1.BucketCleaner, status metav1.ConditionStatus, reason, message string, mutate func(*telemetryv1alpha1.BucketCleanerStatus)) error {
	current := &telemetryv1alpha1.BucketCleaner{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cleaner), current); err != nil {
		return client.IgnoreNotFound(err)
	}
	base := current.DeepCopy()
	current.Status.ObservedGeneration = current.Generation
	if mutate != nil {
		mutate(&current.Status)
	}
	meta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message,
		ObservedGeneration: current.Generation, LastTransitionTime: metav1.NewTime(time.Now()),
	})
	if equality.Semantic.DeepEqual(base.Status, current.Status) {
		return nil
	}
	return r.Status().Patch(ctx, current, client.MergeFrom(base))
}

func (r *BucketCleanerReconciler) cleanersForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	items := &telemetryv1alpha1.BucketCleanerList{}
	if err := r.List(ctx, items, client.InNamespace(obj.GetNamespace()), client.MatchingFields{bucketCleanerSecretIndex: obj.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(items.Items))
	for i := range items.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&items.Items[i])})
	}
	return requests
}

func (r *BucketCleanerReconciler) deleteBudget() time.Duration {
	if r.DeleteBudget > 0 {
		return r.DeleteBudget
	}
	return defaultDeleteBudget
}

func (r *BucketCleanerReconciler) deleteConcurrency() int {
	if r.DeleteConcurrency > 0 {
		return r.DeleteConcurrency
	}
	return defaultDeleteConcurrency
}

func bucketCheckInterval(cleaner *telemetryv1alpha1.BucketCleaner) time.Duration {
	if cleaner.Spec.CheckIntervalSeconds == nil {
		return defaultBucketCheckInterval
	}
	return time.Duration(*cleaner.Spec.CheckIntervalSeconds) * time.Second
}

func deletionStartedPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
	}
}

func objectStoreSecretNames(spec telemetryv1alpha1.ObjectStoreSpec) []string {
	var refs []*corev1.SecretKeySelector
	if spec.AWS != nil {
		refs = append(refs, spec.AWS.AccessKeyIDSecretRef, spec.AWS.SecretAccessKeySecretRef, spec.AWS.SessionTokenSecretRef)
	}
	if spec.Azure != nil {
		refs = append(refs, spec.Azure.AccessKeySecretRef, spec.Azure.SASTokenSecretRef, spec.Azure.BearerTokenSecretRef)
		if spec.Azure.ClientSecret != nil {
			refs = append(refs, &spec.Azure.ClientSecret.ClientSecretKeyRef)
		}
	}
	if spec.GCP != nil {
		refs = append(refs, spec.GCP.ServiceAccountKeySecretRef, spec.GCP.BearerTokenSecretRef)
	}
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref != nil && ref.Name != "" {
			names = append(names, ref.Name)
		}
	}
	return names
}
