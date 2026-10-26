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
	"io"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/objectstore"
)

const testBucketName = "telemetry"

var _ = Describe("BucketCleaner Controller", func() {
	ctx := context.Background()
	created := []client.Object{}

	AfterEach(func() {
		for i := len(created) - 1; i >= 0; i-- {
			if cleaner, ok := created[i].(*telemetryv1alpha1.BucketCleaner); ok {
				current := &telemetryv1alpha1.BucketCleaner{}
				if k8sClient.Get(ctx, client.ObjectKeyFromObject(cleaner), current) == nil {
					controllerutil.RemoveFinalizer(current, bucketCleanerFinalizer)
					Expect(k8sClient.Update(ctx, current)).To(Succeed())
				}
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, created[i]))).To(Succeed())
		}
		created = nil
	})

	// fileBucket stands in for a remote store and records the credentials
	// the reconciler resolved from the referenced Secrets.
	fileBucket := func(dir string, resolved *[]string) objectstore.Opener {
		return func(ctx context.Context, spec telemetryv1alpha1.ObjectStoreSpec, secret objectstore.SecretFunc) (*blob.Bucket, error) {
			for _, ref := range []*corev1.SecretKeySelector{spec.AWS.AccessKeyIDSecretRef, spec.AWS.SecretAccessKeySecretRef} {
				value, err := secret(ctx, ref)
				if err != nil {
					return nil, err
				}
				*resolved = append(*resolved, string(value))
			}
			return fileblob.OpenBucket(dir, nil)
		}
	}

	It("reports bucket access and deletes only the tracked subpath on deletion", func() {
		dir := GinkgoT().TempDir()
		seed, err := fileblob.OpenBucket(dir, nil)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = seed.Close() }()
		for _, key := range []string{"logs/manifest/00001", "logs/compacted/a.sst", "logs-archive/keep", "metrics/keep"} {
			Expect(seed.WriteAll(ctx, key, []byte("x"), nil)).To(Succeed())
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cleaner-s3", Namespace: testNamespace},
			Data:       map[string][]byte{"access-key-id": []byte("id"), "secret-access-key": []byte("key")},
		}
		cleaner := bucketCleaner("cleaner-logs", secret.Name, "logs")
		for _, object := range []client.Object{secret, cleaner} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			created = append(created, object)
		}
		var resolved []string
		reconciler := &BucketCleanerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), OpenBucket: fileBucket(dir, &resolved), DeleteConcurrency: 2}
		request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cleaner)}

		result, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(5 * time.Minute))
		Expect(resolved).To(Equal([]string{"id", "key"}))
		current := &telemetryv1alpha1.BucketCleaner{}
		Expect(k8sClient.Get(ctx, request.NamespacedName, current)).To(Succeed())
		Expect(current.Finalizers).To(ContainElement(bucketCleanerFinalizer))
		Expect(current.Status.LastCheckTime).NotTo(BeNil())
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady)).To(SatisfyAll(
			Not(BeNil()),
			HaveField("Status", metav1.ConditionTrue),
			HaveField("Reason", reasonBucketAccessible),
			HaveField("Message", "operator can access s3://telemetry/logs/"),
		))

		Expect(k8sClient.Delete(ctx, cleaner)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, request.NamespacedName, current))).To(BeTrue())
		Expect(bucketKeys(ctx, seed)).To(Equal([]string{"logs-archive/keep", "metrics/keep"}))
	})

	It("reports missing credentials and keeps the finalizer until objects can be deleted", func() {
		dir := GinkgoT().TempDir()
		seed, err := fileblob.OpenBucket(dir, nil)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = seed.Close() }()
		Expect(seed.WriteAll(ctx, "logs/keep-until-credentials", []byte("x"), nil)).To(Succeed())

		cleaner := bucketCleaner("cleaner-missing-secret", "cleaner-missing-s3", "logs")
		Expect(k8sClient.Create(ctx, cleaner)).To(Succeed())
		created = append(created, cleaner)
		var resolved []string
		reconciler := &BucketCleanerReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), OpenBucket: fileBucket(dir, &resolved)}
		request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cleaner)}

		result, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(bucketCheckRetry))
		current := &telemetryv1alpha1.BucketCleaner{}
		Expect(k8sClient.Get(ctx, request.NamespacedName, current)).To(Succeed())
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady)).To(SatisfyAll(
			Not(BeNil()),
			HaveField("Status", metav1.ConditionFalse),
			HaveField("Reason", reasonBucketInaccessible),
			HaveField("Message", ContainSubstring(`referenced Secret "cleaner-missing-s3" is unavailable`)),
		))

		Expect(k8sClient.Delete(ctx, cleaner)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).To(HaveOccurred())
		Expect(k8sClient.Get(ctx, request.NamespacedName, current)).To(Succeed())
		Expect(current.Finalizers).To(ContainElement(bucketCleanerFinalizer))
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady)).To(HaveField("Reason", reasonDeletionFailed))
		Expect(bucketKeys(ctx, seed)).To(Equal([]string{"logs/keep-until-credentials"}))
	})

	It("rejects local object stores and subPath changes", func() {
		local := bucketCleaner("cleaner-local", "unused", "logs")
		local.Spec.ObjectStore = telemetryv1alpha1.ObjectStoreSpec{Type: telemetryv1alpha1.ObjectStoreLocal}
		Expect(k8sClient.Create(ctx, local)).To(MatchError(ContainSubstring("objectStore.type must be Aws, Azure, or Gcp")))

		root := bucketCleaner("cleaner-root", "unused", "/")
		Expect(k8sClient.Create(ctx, root)).NotTo(Succeed())

		cleaner := bucketCleaner("cleaner-immutable", "unused", "logs")
		Expect(k8sClient.Create(ctx, cleaner)).To(Succeed())
		created = append(created, cleaner)
		cleaner.Spec.SubPath = "metrics"
		Expect(k8sClient.Update(ctx, cleaner)).To(MatchError(ContainSubstring("subPath is immutable")))
	})
})

func bucketCleaner(name, secretName, subPath string) *telemetryv1alpha1.BucketCleaner {
	ref := func(key string) *corev1.SecretKeySelector {
		return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secretName}, Key: key}
	}
	return &telemetryv1alpha1.BucketCleaner{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: telemetryv1alpha1.BucketCleanerSpec{
			ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreAWS,
				AWS: &telemetryv1alpha1.AWSObjectStoreSpec{
					Region: "us-east-1", Bucket: testBucketName,
					AccessKeyIDSecretRef:     ref("access-key-id"),
					SecretAccessKeySecretRef: ref("secret-access-key"),
				},
			},
			SubPath: subPath,
		},
	}
}

func bucketKeys(ctx context.Context, bucket *blob.Bucket) []string {
	var keys []string
	iter := bucket.List(nil)
	for {
		object, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		keys = append(keys, object.Key)
	}
	slices.Sort(keys)
	return keys
}
