/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

var _ = Describe("Vector Controller", func() {
	ctx := context.Background()

	cleanup := func(vector *telemetryv1alpha1.Vector) {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, vector))).To(Succeed())
		for _, list := range []client.ObjectList{&appsv1.StatefulSetList{}, &corev1.ServiceList{}, &corev1.SecretList{}} {
			Expect(k8sClient.List(ctx, list, client.InNamespace(testNamespace),
				client.MatchingLabels{"app.kubernetes.io/instance": vector.Name})).To(Succeed())
			items, err := metaItemsForCleanup(list)
			Expect(err).NotTo(HaveOccurred())
			for _, item := range items {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, item))).To(Succeed())
			}
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &telemetryv1alpha1.VectorTopology{
			ObjectMeta: metav1.ObjectMeta{Name: VectorTopologyName(vector.Name), Namespace: testNamespace},
		}))).To(Succeed())
	}

	reconcileVector := func(vector *telemetryv1alpha1.Vector) {
		reconciler := &VectorReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vector)})
		Expect(err).NotTo(HaveOccurred())
	}

	statefulSetExists := func(name string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, &appsv1.StatefulSet{})
		if apierrors.IsNotFound(err) {
			return false
		}
		Expect(err).NotTo(HaveOccurred())
		return true
	}

	markReady := func(name string) {
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, sts)).To(Succeed())
		sts.Status.Replicas = lo.FromPtrOr(sts.Spec.Replicas, 1)
		sts.Status.ReadyReplicas = sts.Status.Replicas
		sts.Status.ObservedGeneration = sts.Generation
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
	}

	It("reconciles a standalone server", func() {
		vector := &telemetryv1alpha1.Vector{ObjectMeta: metav1.ObjectMeta{Name: "vector-standalone", Namespace: testNamespace}}
		Expect(k8sClient.Create(ctx, vector)).To(Succeed())
		DeferCleanup(cleanup, vector)
		reconcileVector(vector)

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}, config)).To(Succeed())
		Expect(config.Data).To(HaveKey("vector.yaml"))
		Expect(config.Data).NotTo(HaveKey("reader.yaml"))
		Expect(string(config.Data["vector.yaml"])).To(SatisfyAll(
			ContainSubstring("role: standalone"),
			ContainSubstring("path: /var/lib/vector/data"),
			Not(ContainSubstring("kubernetes:")),
		))

		service := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), service)).To(Succeed())
		Expect(service.Spec.Ports).To(ConsistOf(SatisfyAll(HaveField("Name", "http"), HaveField("Port", int32(8080)))))

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		container := sts.Spec.Template.Spec.Containers[0]
		Expect(container.Name).To(Equal("vector"))
		Expect(container.Args).To(Equal([]string{"--port", "8080", "cluster", "--config", "/etc/vector/config/vector.yaml"}))
		Expect(container.Ports).To(ConsistOf(HaveField("Name", "http")))
		Expect(container.Resources.Limits.Memory().String()).To(Equal("2Gi"))
	})

	It("serves the API under the ingress path prefix and removes the ingress when disabled", func() {
		vector := &telemetryv1alpha1.Vector{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-ingress", Namespace: testNamespace},
			Spec: telemetryv1alpha1.VectorSpec{Ingress: telemetryv1alpha1.IngressSpec{
				Enabled: true, Hostname: "telemetry.example.com", PathPrefix: "/vector",
			}},
		}
		Expect(k8sClient.Create(ctx, vector)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &networkingv1.Ingress{
				ObjectMeta: metav1.ObjectMeta{Name: vector.Name, Namespace: testNamespace},
			}))).To(Succeed())
			cleanup(vector)
		})
		reconcileVector(vector)

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(ContainSubstring("path_prefix: /vector"))
		ingress := &networkingv1.Ingress{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), ingress)).To(Succeed())
		Expect(ingress.Spec.Rules).To(HaveLen(1))
		Expect(ingress.Spec.Rules[0].Host).To(Equal("telemetry.example.com"))
		Expect(ingress.Spec.Rules[0].HTTP.Paths).To(ConsistOf(SatisfyAll(
			HaveField("Path", "/vector/api"),
			HaveField("Backend.Service.Name", vector.Name),
		)))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), vector)).To(Succeed())
		vector.Spec.Ingress.Enabled = false
		Expect(k8sClient.Update(ctx, vector)).To(Succeed())
		reconcileVector(vector)
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), ingress))).To(BeTrue())
	})

	It("renders a bedrock embedding model and rejects provider-specific fields elsewhere", func() {
		vector := &telemetryv1alpha1.Vector{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-bedrock", Namespace: testNamespace},
			Spec: telemetryv1alpha1.VectorSpec{Config: telemetryv1alpha1.VectorConfigSpec{
				Embedding: &telemetryv1alpha1.VectorEmbeddingSpec{
					Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0", Region: "eu-west-1",
					Dimensions: lo.ToPtr(int32(512)),
				},
			}},
		}
		Expect(k8sClient.Create(ctx, vector)).To(Succeed())
		DeferCleanup(cleanup, vector)
		reconcileVector(vector)
		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(SatisfyAll(
			ContainSubstring("provider: bedrock"),
			ContainSubstring("region: eu-west-1"),
			Not(ContainSubstring("api_key:")),
		))

		invalid := map[string]telemetryv1alpha1.VectorEmbeddingSpec{
			"bedrock authenticates with the AWS credential chain": {
				Provider: "bedrock", Model: "amazon.titan-embed-text-v2:0",
				APIKeySecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "bedrock-key"}, Key: "token"},
			},
			"region only applies to the bedrock provider": {Provider: "openai", Model: "m", Region: "eu-central-1"},
			"cohere models embed at a fixed size":         {Provider: "cohere", Model: "embed-v4.0", Dimensions: lo.ToPtr(int32(512))},
		}
		for message, embedding := range invalid {
			rejected := &telemetryv1alpha1.Vector{
				ObjectMeta: metav1.ObjectMeta{Name: "vector-invalid-embedding", Namespace: testNamespace},
				Spec:       telemetryv1alpha1.VectorSpec{Config: telemetryv1alpha1.VectorConfigSpec{Embedding: &embedding}},
			}
			Expect(k8sClient.Create(ctx, rejected)).To(MatchError(ContainSubstring(message)))
		}
	})

	It("declares NamespaceAuthentications and reloads their credentials without rolling pods", func() {
		vector := &telemetryv1alpha1.Vector{ObjectMeta: metav1.ObjectMeta{Name: "vector-auth", Namespace: testNamespace}}
		password := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-auth-docs", Namespace: testNamespace},
			Data:       map[string][]byte{"password": []byte("first")},
		}
		auth := &telemetryv1alpha1.NamespaceAuthentication{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-auth-docs", Namespace: testNamespace},
			Spec: telemetryv1alpha1.NamespaceAuthenticationSpec{
				DataStoreRef: telemetryv1alpha1.DataStoreReference{Kind: dataStoreVector, Name: vector.Name},
				Namespace:    "docs", Username: "docs-writer", Permission: permissionWrite,
				SecretKeyRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: password.Name}, Key: "password"},
			},
		}
		for _, object := range []client.Object{vector, password, auth} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
		}
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, auth))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, password))).To(Succeed())
			cleanup(vector)
		})
		reconcileVector(vector)

		config := &corev1.Secret{}
		configKey := types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}
		Expect(k8sClient.Get(ctx, configKey, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(SatisfyAll(
			ContainSubstring("unauthenticated: false"),
			ContainSubstring("path: /var/run/secrets/vector/internal-token"),
			ContainSubstring("name: docs"),
			ContainSubstring("username: docs-writer"),
			ContainSubstring("path: /etc/vector/secrets/namespace-vector-auth-docs-password"),
			ContainSubstring("name: default"),
		))
		Expect(string(config.Data["namespace-vector-auth-docs-password"])).To(Equal("first"))
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		hash := sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]
		Expect(hash).NotTo(BeEmpty())
		Expect(sts.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElements(
			HaveField("MountPath", "/etc/vector/secrets"),
			HaveField("MountPath", "/var/run/secrets/vector/internal-token"),
		))

		password.Data["password"] = []byte("second")
		Expect(k8sClient.Update(ctx, password)).To(Succeed())
		reconcileVector(vector)
		Expect(k8sClient.Get(ctx, configKey, config)).To(Succeed())
		Expect(string(config.Data["namespace-vector-auth-docs-password"])).To(Equal("second"))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		Expect(sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]).To(Equal(hash))

		validator := &NamespaceAuthenticationReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		Expect(validator.validate(ctx, auth)).To(Succeed())
		reporting := auth.DeepCopy()
		reporting.ObjectMeta = metav1.ObjectMeta{Name: "vector-auth-usage", Namespace: testNamespace}
		reporting.Spec.UsageReportingEndpoint = "console:9090"
		Expect(k8sClient.Create(ctx, reporting)).To(MatchError(ContainSubstring("usageReportingEndpoint is not supported for Vector")))
	})

	It("renders the embedding model and rotates its API key without rolling pods", func() {
		key := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-embed-openai", Namespace: testNamespace},
			Data:       map[string][]byte{"api-key": []byte("sk-first")},
		}
		cacheSize := resource.MustParse("32Mi")
		vector := &telemetryv1alpha1.Vector{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-embed", Namespace: testNamespace},
			Spec: telemetryv1alpha1.VectorSpec{Config: telemetryv1alpha1.VectorConfigSpec{
				Embedding: &telemetryv1alpha1.VectorEmbeddingSpec{
					Provider:        "openai",
					Model:           "text-embedding-3-small",
					APIKeySecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: key.Name}, Key: "api-key"},
					Dimensions:      lo.ToPtr(int32(512)),
					CacheSize:       &cacheSize,
				},
			}},
		}
		for _, object := range []client.Object{key, vector} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
		}
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, key))).To(Succeed())
			cleanup(vector)
		})
		reconcileVector(vector)

		config := &corev1.Secret{}
		configKey := types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}
		Expect(k8sClient.Get(ctx, configKey, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(SatisfyAll(
			ContainSubstring("provider: openai"),
			ContainSubstring("model: text-embedding-3-small"),
			ContainSubstring("dimensions: 512"),
			ContainSubstring("capacity: 33554432"),
			ContainSubstring("path: /etc/vector/secrets/embedding-api-key"),
		))
		Expect(string(config.Data["embedding-api-key"])).To(Equal("sk-first"))
		Expect(vectorSecretNames(vector)).To(ContainElement(key.Name))
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		hash := sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]

		key.Data["api-key"] = []byte("sk-second")
		Expect(k8sClient.Update(ctx, key)).To(Succeed())
		reconcileVector(vector)
		Expect(k8sClient.Get(ctx, configKey, config)).To(Succeed())
		Expect(string(config.Data["embedding-api-key"])).To(Equal("sk-second"))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		Expect(sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]).To(Equal(hash))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), vector)).To(Succeed())
		vector.Spec.Config.Embedding.Model = "text-embedding-3-large"
		Expect(k8sClient.Update(ctx, vector)).To(Succeed())
		reconcileVector(vector)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		Expect(sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]).NotTo(Equal(hash))
	})

	It("runs a reader group per active shard, follows splits, and keeps a writer per shard", func() {
		vector := &telemetryv1alpha1.Vector{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-sharded", Namespace: testNamespace},
			Spec: telemetryv1alpha1.VectorSpec{
				Mode: telemetryv1alpha1.ProductModeSharded,
				Config: telemetryv1alpha1.VectorConfigSpec{
					Storage: telemetryv1alpha1.StorageSpec{ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
						Type: telemetryv1alpha1.ObjectStoreAWS,
						AWS:  &telemetryv1alpha1.AWSObjectStoreSpec{Region: "us-east-1", Bucket: "vectors"},
					}},
				},
				Writer: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(2))},
				Reader: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(1))},
			},
		}
		Expect(k8sClient.Create(ctx, vector)).To(Succeed())
		DeferCleanup(cleanup, vector)
		reconcileVector(vector)

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(SatisfyAll(
			ContainSubstring("role: writer"),
			Not(ContainSubstring("initial_shards")),
			ContainSubstring("name: vector-sharded"),
			ContainSubstring("reader_groups: true"),
		))
		Expect(string(config.Data["reader.yaml"])).To(ContainSubstring("role: reader"))

		writer := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-writer"}, writer)).To(Succeed())
		Expect(*writer.Spec.Replicas).To(Equal(int32(2)))
		Expect(writer.Spec.ServiceName).To(Equal(vector.Name + "-writer-headless"))
		Expect(writer.Spec.Template.Spec.AutomountServiceAccountToken).To(HaveValue(BeTrue()))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-writer-headless"}, &corev1.Service{})).To(Succeed())

		entry := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), entry)).To(Succeed())
		Expect(entry.Spec.Selector).NotTo(HaveKey("app.kubernetes.io/component"))

		role := &rbacv1.Role{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-sharding"}, role)).To(Succeed())
		Expect(role.Rules[0].Resources).To(Equal([]string{"vectortopologies"}))
		Expect(role.Rules).To(ContainElement(SatisfyAll(
			HaveField("Resources", []string{"pods"}),
			HaveField("Verbs", ContainElement("list")),
		)))

		// Before the topology exists, each writer replica will own a shard.
		for _, shard := range []string{"0", "1"} {
			reader := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-reader-" + shard}, reader)).To(Succeed())
			Expect(reader.Spec.Template.Spec.Containers[0].Args).To(ContainElement("/etc/vector/config/reader.yaml"))
			Expect(reader.Spec.Template.Spec.Containers[0].Env).To(ContainElement(corev1.EnvVar{Name: "READER_SHARD", Value: shard}))
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-reader-" + shard + "-headless"}, &corev1.Service{})).To(Succeed())
		}

		// A third writer joined: shard 0 split into shards 2 and 3 for it, and
		// shard 1 is splitting for a fourth.
		topology := &telemetryv1alpha1.VectorTopology{
			ObjectMeta: metav1.ObjectMeta{
				Name: VectorTopologyName(vector.Name), Namespace: testNamespace,
				Labels: map[string]string{VectorTopologyLabel: vector.Name},
			},
			Spec: telemetryv1alpha1.VectorTopologySpec{
				Generation: 4, NextShardID: 4,
				Shards: []telemetryv1alpha1.VectorShard{
					{ID: 0, State: telemetryv1alpha1.VectorShardRetired, Range: telemetryv1alpha1.VectorNamespaceRange{Start: "0000000000000000", End: "8000000000000000"}, Children: []int32{2, 3}},
					{ID: 1, State: telemetryv1alpha1.VectorShardActive, Range: telemetryv1alpha1.VectorNamespaceRange{Start: "8000000000000000"}, Owner: vector.Name + "-writer-1", SplitTo: vector.Name + "-writer-3"},
					{ID: 2, State: telemetryv1alpha1.VectorShardActive, Range: telemetryv1alpha1.VectorNamespaceRange{Start: "0000000000000000", End: "4000000000000000"}, Owner: vector.Name + "-writer-0", Parent: lo.ToPtr(int32(0))},
					{ID: 3, State: telemetryv1alpha1.VectorShardActive, Range: telemetryv1alpha1.VectorNamespaceRange{Start: "4000000000000000", End: "8000000000000000"}, Owner: vector.Name + "-writer-2", Parent: lo.ToPtr(int32(0))},
				},
			},
		}
		Expect(k8sClient.Create(ctx, topology)).To(Succeed())
		stored := &telemetryv1alpha1.VectorTopology{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(topology), stored)).To(Succeed())
		Expect(stored.Spec.Shards[1].SplitTo).To(Equal(vector.Name+"-writer-3"), "the CRD keeps split_to")
		reconcileVector(vector)
		for _, name := range []string{"reader-1", "reader-2", "reader-3"} {
			Expect(statefulSetExists(vector.Name+"-"+name)).To(BeTrue(), name)
		}
		// The split parent's readers keep serving until the children's are ready.
		Expect(statefulSetExists(vector.Name + "-reader-0")).To(BeTrue())

		for _, name := range []string{"writer", "reader-1", "reader-2", "reader-3"} {
			markReady(vector.Name + "-" + name)
		}
		reconcileVector(vector)
		Expect(statefulSetExists(vector.Name + "-reader-0")).To(BeFalse())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-reader-0-headless"}, &corev1.Service{}))).To(BeTrue())

		current := &telemetryv1alpha1.Vector{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), current)).To(Succeed())
		Expect(current.Status.ActiveShards).To(Equal([]int32{1, 2, 3}))
		Expect(current.Status.TopologyGeneration).To(Equal(int64(4)))
		Expect(current.Status.Endpoint).To(Equal("http://vector-sharded." + testNamespace + ".svc:8080"))

		// Four writers have or are getting a shard, so the writer StatefulSet
		// keeps four replicas while writer.replicas asks for two.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-writer"}, writer)).To(Succeed())
		Expect(*writer.Spec.Replicas).To(Equal(int32(4)))
		ready := meta.FindStatusCondition(current.Status.Conditions, conditionReady)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(reasonWriterScaleDownRejected))
		Expect(ready.Message).To(ContainSubstring("writer.replicas is 2, but 4 writers each own a shard"))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), current)).To(Succeed())
		current.Spec.Writer.Replicas = lo.ToPtr(int32(4))
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		reconcileVector(vector)
		markReady(vector.Name + "-writer")
		reconcileVector(vector)
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), current)).To(Succeed())
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady).Reason).To(Equal(reasonReady))
	})

	It("rejects a sharded cluster without a shared object store", func() {
		vector := &telemetryv1alpha1.Vector{
			ObjectMeta: metav1.ObjectMeta{Name: "vector-local", Namespace: testNamespace},
			Spec:       telemetryv1alpha1.VectorSpec{Mode: telemetryv1alpha1.ProductModeSharded},
		}
		err := k8sClient.Create(ctx, vector)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Sharded mode needs an Aws, Azure or Gcp object store"))
	})
})

func metaItemsForCleanup(list client.ObjectList) ([]client.Object, error) {
	if secrets, ok := list.(*corev1.SecretList); ok {
		return lo.Map(secrets.Items, func(item corev1.Secret, _ int) client.Object { return &item }), nil
	}
	return metaItems(list)
}
