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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

const (
	attributeBody = "body"
	attributeYear = "year"
	typeInt       = "int"
	typeString    = "string"
)

var _ = Describe("NamespaceSchema Controller", func() {
	ctx := context.Background()

	vectorRef := func(name string) telemetryv1alpha1.DataStoreReference {
		return telemetryv1alpha1.DataStoreReference{Kind: dataStoreVector, Name: name}
	}

	readyCondition := func(schema *telemetryv1alpha1.NamespaceSchema) *metav1.Condition {
		reconciler := &NamespaceSchemaReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(schema)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(schema), schema)).To(Succeed())
		return meta.FindStatusCondition(schema.Status.Conditions, conditionReady)
	}

	It("declares schemas in the Vector config without rolling pods and reports conflicts", func() {
		// given
		vector := &telemetryv1alpha1.Vector{ObjectMeta: metav1.ObjectMeta{Name: "vector-schemas", Namespace: testNamespace}}
		articles := &telemetryv1alpha1.NamespaceSchema{
			ObjectMeta: metav1.ObjectMeta{Name: "articles", Namespace: testNamespace},
			Spec: telemetryv1alpha1.NamespaceSchemaSpec{
				DataStoreRef: vectorRef(vector.Name),
				Schema: map[string]telemetryv1alpha1.AttributeSchema{
					attributeBody: {Type: typeString, FullTextSearch: true},
				},
			},
		}
		shadowed := &telemetryv1alpha1.NamespaceSchema{
			ObjectMeta: metav1.ObjectMeta{Name: "articles-v2", Namespace: testNamespace},
			Spec: telemetryv1alpha1.NamespaceSchemaSpec{
				DataStoreRef: vectorRef(vector.Name),
				Namespace:    "articles",
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{attributeBody: {Type: typeString}},
			},
		}
		for _, object := range []client.Object{vector, articles, shadowed} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
		}
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, articles))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, shadowed))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, vector))).To(Succeed())
		})
		reconciler := &VectorReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		reconcileVector := func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vector)})
			Expect(err).NotTo(HaveOccurred())
		}

		// when
		reconcileVector()

		// then
		config := &corev1.Secret{}
		configKey := types.NamespacedName{Namespace: testNamespace, Name: vector.Name + "-config"}
		Expect(k8sClient.Get(ctx, configKey, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(SatisfyAll(
			ContainSubstring("name: articles"),
			ContainSubstring("full_text_search: true"),
			ContainSubstring("name: default"),
		))
		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		hash := sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]
		Expect(readyCondition(articles).Status).To(Equal(metav1.ConditionTrue))
		conflict := readyCondition(shadowed)
		Expect(conflict.Status).To(Equal(metav1.ConditionFalse))
		Expect(conflict.Reason).To(Equal(reasonSchemaConflict))
		Expect(conflict.Message).To(ContainSubstring("NamespaceSchema articles also declares namespace \"articles\""))

		// when the schema changes
		articles.Spec.Schema[attributeYear] = telemetryv1alpha1.AttributeSchema{Type: typeInt}
		Expect(k8sClient.Update(ctx, articles)).To(Succeed())
		reconcileVector()

		// then the config follows in place
		Expect(k8sClient.Get(ctx, configKey, config)).To(Succeed())
		Expect(string(config.Data["vector.yaml"])).To(ContainSubstring("type: int"))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vector), sts)).To(Succeed())
		Expect(sts.Spec.Template.Annotations["telemetry.plural.sh/config-hash"]).To(Equal(hash))

		// when the winning schema goes away, then the other applies
		Expect(k8sClient.Delete(ctx, articles)).To(Succeed())
		Expect(readyCondition(shadowed).Status).To(Equal(metav1.ConditionTrue))
	})

	It("reports schemas whose Vector doesn't exist", func() {
		schema := &telemetryv1alpha1.NamespaceSchema{
			ObjectMeta: metav1.ObjectMeta{Name: "orphaned", Namespace: testNamespace},
			Spec: telemetryv1alpha1.NamespaceSchemaSpec{
				DataStoreRef: vectorRef("missing"),
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{attributeYear: {Type: typeInt}},
			},
		}
		Expect(k8sClient.Create(ctx, schema)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, schema))).To(Succeed()) })

		condition := readyCondition(schema)
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Message).To(ContainSubstring("referenced Vector is unavailable"))
	})

	It("rejects schemas the datastore can't take", func() {
		invalid := map[string]telemetryv1alpha1.NamespaceSchemaSpec{
			"only Vector namespaces have schemas": {
				DataStoreRef: telemetryv1alpha1.DataStoreReference{Kind: dataStoreLogs, Name: "logs"},
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{attributeYear: {Type: typeInt}},
			},
			"full_text_search and embed need type string": {
				DataStoreRef: vectorRef("vector"),
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{attributeYear: {Type: typeInt, FullTextSearch: true}},
			},
			"at most one attribute can embed": {
				DataStoreRef: vectorRef("vector"),
				Schema: map[string]telemetryv1alpha1.AttributeSchema{
					"title":       {Type: typeString, Embed: true},
					attributeBody: {Type: typeString, Embed: true},
				},
			},
			"can't be id, vector, or start with $": {
				DataStoreRef: vectorRef("vector"),
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{"$doc_id": {Type: typeString}},
			},
			"should have at least 1 properties": {
				DataStoreRef: vectorRef("vector"),
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{},
			},
			"Unsupported value": {
				DataStoreRef: vectorRef("vector"),
				Schema:       map[string]telemetryv1alpha1.AttributeSchema{attributeYear: {Type: "date"}},
			},
		}
		for message, spec := range invalid {
			schema := &telemetryv1alpha1.NamespaceSchema{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: testNamespace},
				Spec:       spec,
			}
			Expect(k8sClient.Create(ctx, schema)).To(MatchError(ContainSubstring(message)), message)
		}
	})
})
