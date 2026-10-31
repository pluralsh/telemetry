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
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
	"gocloud.dev/blob"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const (
	bucketCleanerFinalizer   = "telemetry.plural.sh/bucket-cleaner"
	bucketCleanerSecretIndex = "telemetry.plural.sh/bucket-cleaner-secret"

	reasonBucketAccessible   = "BucketAccessible"
	reasonBucketInaccessible = "BucketInaccessible"
	reasonDeleting           = "Deleting"
	reasonDeletionFailed     = "DeletionFailed"
	reasonLocationUnresolved = "LocationUnresolved"

	defaultBucketCheckInterval = 5 * time.Minute
	bucketCheckRetry           = 30 * time.Second
	dataStoreGoneRetry         = 5 * time.Second
	defaultDeleteBudget        = 2 * time.Minute
	defaultDeleteConcurrency   = 32
)

// BucketCleanerReconciler verifies access to a BucketCleaner's prefix while
// it exists and deletes everything under the prefix before releasing its
// finalizer.
type BucketCleanerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader lists a datastore's pods without caching every pod; it
	// defaults to Client.
	APIReader client.Reader
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=list

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
	retry := min(bucketCheckInterval(cleaner), bucketCheckRetry)
	location, err := r.resolve(ctx, cleaner, false)
	if err != nil {
		return ctrl.Result{RequeueAfter: retry}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonLocationUnresolved, err.Error(), nil)
	}
	recordLocation := func(status *telemetryv1alpha1.BucketCleanerStatus) {
		status.ObjectStore, status.SubPath = location.store.DeepCopy(), location.subPath
	}
	if err := r.checkAccess(ctx, cleaner.Namespace, location); err != nil {
		return ctrl.Result{RequeueAfter: retry}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonBucketInaccessible, err.Error(), recordLocation)
	}
	message := "operator can access " + location.String()
	now := metav1.Now()
	return ctrl.Result{RequeueAfter: bucketCheckInterval(cleaner)}, r.setStatus(ctx, cleaner, metav1.ConditionTrue, reasonBucketAccessible, message, func(status *telemetryv1alpha1.BucketCleanerStatus) {
		recordLocation(status)
		status.LastCheckTime = &now
	})
}

func (r *BucketCleanerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &telemetryv1alpha1.BucketCleaner{}, bucketCleanerSecretIndex, func(obj client.Object) []string {
		cleaner := obj.(*telemetryv1alpha1.BucketCleaner)
		names := []string{}
		for _, store := range []*telemetryv1alpha1.ObjectStoreSpec{cleaner.Spec.ObjectStore, cleaner.Status.ObjectStore} {
			if store != nil {
				names = append(names, objectStoreSecretNames(*store)...)
			}
		}
		return lo.Uniq(names)
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
	if waiting, err := r.dataStoreRunning(ctx, cleaner); err != nil {
		_ = r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeletionFailed, err.Error(), nil)
		return ctrl.Result{}, err
	} else if waiting != "" {
		message := fmt.Sprintf("waiting for %s to be deleted before deleting its objects", waiting)
		return ctrl.Result{RequeueAfter: dataStoreGoneRetry}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeleting, message, nil)
	}
	location, err := r.resolve(ctx, cleaner, true)
	if errors.Is(err, errLocationUnknown) {
		// The datastore was gone before its location was ever resolved, so
		// there is nothing this cleaner is known to own.
		ctrl.LoggerFrom(ctx).Info("releasing bucket cleaner without deleting objects", "reason", err.Error())
		base := cleaner.DeepCopy()
		controllerutil.RemoveFinalizer(cleaner, bucketCleanerFinalizer)
		return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, cleaner, client.MergeFrom(base)))
	}
	if err != nil {
		return ctrl.Result{}, r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeletionFailed, err.Error(), nil)
	}
	prefix := location.prefix
	bucket, err := r.open(ctx, cleaner.Namespace, location)
	if err != nil {
		_ = r.setStatus(ctx, cleaner, metav1.ConditionFalse, reasonDeletionFailed, err.Error(), nil)
		return ctrl.Result{}, err
	}
	defer func() { _ = bucket.Close() }()
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
	ctrl.LoggerFrom(ctx).Info("deleted bucket prefix", "location", location.String(), "objects", total)
	base := cleaner.DeepCopy()
	controllerutil.RemoveFinalizer(cleaner, bucketCleanerFinalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, cleaner, client.MergeFrom(base)))
}

// dataStoreRunning describes what of the cleaner's datastore still exists:
// the resource itself, or pods that may still write to the prefix. It is
// empty once both are gone, or when the cleaner names no datastore.
func (r *BucketCleanerReconciler) dataStoreRunning(ctx context.Context, cleaner *telemetryv1alpha1.BucketCleaner) (string, error) {
	ref := cleaner.Spec.DataStoreRef
	if ref == nil {
		return "", nil
	}
	store, err := dataStoreObject(cleaner.Namespace, *ref)
	if err != nil {
		return "", err
	}
	described := fmt.Sprintf("%s %s", ref.Kind, ref.Name)
	labels := resources.SelectorLabels(store, componentNone)
	err = r.Get(ctx, client.ObjectKeyFromObject(store), store)
	if err == nil {
		return described, nil
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(cleaner.Namespace), client.MatchingLabels(labels)); err != nil {
		return "", err
	}
	if len(pods.Items) > 0 {
		return fmt.Sprintf("the %d pods of %s", len(pods.Items), described), nil
	}
	return "", nil
}

// errLocationUnknown means a cleaner relies on its datastore for settings it
// omits, and neither the datastore nor a previously resolved location exists.
var errLocationUnknown = errors.New("bucket location is unknown")

// bucketLocation is the bucket and prefix a cleaner owns.
type bucketLocation struct {
	store   telemetryv1alpha1.ObjectStoreSpec
	subPath string
	prefix  string
}

func (l bucketLocation) String() string {
	return objectstore.Location(l.store, l.prefix)
}

// resolve returns the cleaner's bucket and prefix: its own settings, with
// omitted ones taken from its datastore's storage. When the datastore is
// gone and deleting is set, the location last recorded in status is used.
func (r *BucketCleanerReconciler) resolve(ctx context.Context, cleaner *telemetryv1alpha1.BucketCleaner, deleting bool) (bucketLocation, error) {
	spec := cleaner.Spec
	var store *telemetryv1alpha1.ObjectStoreSpec
	if spec.ObjectStore != nil {
		store = spec.ObjectStore.DeepCopy()
	}
	subPath := spec.SubPath
	if (store == nil || subPath == "") && spec.DataStoreRef != nil {
		storage, found, err := r.dataStoreStorage(ctx, cleaner.Namespace, *spec.DataStoreRef)
		switch {
		case err != nil:
			return bucketLocation{}, err
		case found:
			if store == nil {
				store = storage.ObjectStore.DeepCopy()
			}
			if subPath == "" {
				subPath = lo.CoalesceOrEmpty(storage.Path, dataStoreDefaultPath(spec.DataStoreRef.Kind))
			}
		case deleting && cleaner.Status.ObjectStore != nil && cleaner.Status.SubPath != "":
			if store == nil {
				store = cleaner.Status.ObjectStore.DeepCopy()
			}
			if subPath == "" {
				subPath = cleaner.Status.SubPath
			}
		case deleting:
			return bucketLocation{}, fmt.Errorf("%w: %s %s is gone and its storage was never resolved", errLocationUnknown, spec.DataStoreRef.Kind, spec.DataStoreRef.Name)
		default:
			return bucketLocation{}, fmt.Errorf("%s %s not found; its storage settings are needed for the omitted objectStore or subPath", spec.DataStoreRef.Kind, spec.DataStoreRef.Name)
		}
	}
	if store == nil || subPath == "" {
		return bucketLocation{}, errors.New("objectStore and subPath are required unless dataStoreRef is set")
	}
	switch lo.CoalesceOrEmpty(store.Type, telemetryv1alpha1.ObjectStoreLocal) {
	case telemetryv1alpha1.ObjectStoreAWS, telemetryv1alpha1.ObjectStoreAzure, telemetryv1alpha1.ObjectStoreGCP:
	default:
		return bucketLocation{}, fmt.Errorf("objectStore type %q is not supported; BucketCleaner cleans Aws, Azure, or Gcp buckets", lo.CoalesceOrEmpty(store.Type, telemetryv1alpha1.ObjectStoreLocal))
	}
	store.Path = ""
	prefix, err := objectstore.Prefix(subPath)
	if err != nil {
		return bucketLocation{}, err
	}
	return bucketLocation{store: *store, subPath: subPath, prefix: prefix}, nil
}

// dataStoreStorage returns the storage settings of the datastore ref names,
// and whether it exists.
func (r *BucketCleanerReconciler) dataStoreStorage(ctx context.Context, namespace string, ref telemetryv1alpha1.BucketCleanerDataStoreReference) (telemetryv1alpha1.StorageSpec, bool, error) {
	store, err := dataStoreObject(namespace, ref)
	if err != nil {
		return telemetryv1alpha1.StorageSpec{}, false, err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(store), store); err != nil {
		return telemetryv1alpha1.StorageSpec{}, false, client.IgnoreNotFound(err)
	}
	switch store := store.(type) {
	case *telemetryv1alpha1.Metrics:
		return store.Spec.Config.Storage, true, nil
	case *telemetryv1alpha1.Logs:
		return store.Spec.Config.Storage, true, nil
	case *telemetryv1alpha1.Traces:
		return store.Spec.Config.Storage, true, nil
	case *telemetryv1alpha1.Vector:
		return store.Spec.Config.Storage, true, nil
	}
	return telemetryv1alpha1.StorageSpec{}, false, fmt.Errorf("unsupported dataStoreRef kind %q", ref.Kind)
}

func dataStoreObject(namespace string, ref telemetryv1alpha1.BucketCleanerDataStoreReference) (client.Object, error) {
	object := metav1.ObjectMeta{Name: ref.Name, Namespace: namespace}
	switch ref.Kind {
	case dataStoreMetrics:
		return &telemetryv1alpha1.Metrics{ObjectMeta: object}, nil
	case dataStoreLogs:
		return &telemetryv1alpha1.Logs{ObjectMeta: object}, nil
	case dataStoreTraces:
		return &telemetryv1alpha1.Traces{ObjectMeta: object}, nil
	case dataStoreVector:
		return &telemetryv1alpha1.Vector{ObjectMeta: object}, nil
	}
	return nil, fmt.Errorf("unsupported dataStoreRef kind %q", ref.Kind)
}

// dataStoreDefaultPath is the storage path a datastore of kind uses when its
// spec.config.storage.path is unset.
func dataStoreDefaultPath(kind string) string {
	switch kind {
	case dataStoreMetrics:
		return resources.MetricsDescriptor.Name
	case dataStoreLogs:
		return resources.LogsDescriptor.Name
	case dataStoreTraces:
		return resources.TracesDescriptor.Name
	case dataStoreVector:
		return resources.VectorDescriptor.Name
	}
	return ""
}

func (r *BucketCleanerReconciler) checkAccess(ctx context.Context, namespace string, location bucketLocation) error {
	bucket, err := r.open(ctx, namespace, location)
	if err != nil {
		return err
	}
	defer func() { _ = bucket.Close() }()
	return objectstore.CheckAccess(ctx, bucket, location.prefix)
}

func (r *BucketCleanerReconciler) open(ctx context.Context, namespace string, location bucketLocation) (*blob.Bucket, error) {
	open := r.OpenBucket
	if open == nil {
		open = objectstore.Open
	}
	bucket, err := open(ctx, location.store, r.secretFunc(namespace))
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", objectstore.Location(location.store, ""), err)
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
