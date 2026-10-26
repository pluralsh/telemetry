/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

// The API server prunes fields the CRD schema doesn't declare, so pod template
// labels and annotations only survive when the CRDs are generated with
// crd:generateEmbeddedObjectMeta=true. Azure workload identity depends on the
// azure.workload.identity/use pod label set here.
var _ = Describe("CRD schema", func() {
	ctx := context.Background()

	// The typed client always sends podTemplate.spec, whose containers field the
	// CRD requires, so each template names the product container.
	workload := func(container string) telemetryv1alpha1.WorkloadSpec {
		return telemetryv1alpha1.WorkloadSpec{PodTemplate: &corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"azure.workload.identity/use": "true"},
				Annotations: map[string]string{"k8s.grafana.com/scrape": "true"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: container}}},
		}}
	}

	for _, test := range []struct {
		kind     string
		object   func(name string) client.Object
		empty    func() client.Object
		readBack func(client.Object) (writer, reader *corev1.PodTemplateSpec)
	}{
		{
			kind: "Metrics",
			object: func(name string) client.Object {
				return &telemetryv1alpha1.Metrics{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
					Spec:       telemetryv1alpha1.MetricsSpec{Mode: telemetryv1alpha1.ProductModeSharded, Writer: workload("metrics"), Reader: workload("metrics")},
				}
			},
			empty: func() client.Object { return &telemetryv1alpha1.Metrics{} },
			readBack: func(object client.Object) (*corev1.PodTemplateSpec, *corev1.PodTemplateSpec) {
				spec := object.(*telemetryv1alpha1.Metrics).Spec
				return spec.Writer.PodTemplate, spec.Reader.PodTemplate
			},
		},
		{
			kind: "Logs",
			object: func(name string) client.Object {
				return &telemetryv1alpha1.Logs{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
					Spec:       telemetryv1alpha1.LogsSpec{Mode: telemetryv1alpha1.ProductModeSharded, Writer: workload("logs"), Reader: workload("logs")},
				}
			},
			empty: func() client.Object { return &telemetryv1alpha1.Logs{} },
			readBack: func(object client.Object) (*corev1.PodTemplateSpec, *corev1.PodTemplateSpec) {
				spec := object.(*telemetryv1alpha1.Logs).Spec
				return spec.Writer.PodTemplate, spec.Reader.PodTemplate
			},
		},
		{
			kind: "Traces",
			object: func(name string) client.Object {
				return &telemetryv1alpha1.Traces{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
					Spec:       telemetryv1alpha1.TracesSpec{Mode: telemetryv1alpha1.ProductModeSharded, Writer: workload("traces"), Reader: workload("traces")},
				}
			},
			empty: func() client.Object { return &telemetryv1alpha1.Traces{} },
			readBack: func(object client.Object) (*corev1.PodTemplateSpec, *corev1.PodTemplateSpec) {
				spec := object.(*telemetryv1alpha1.Traces).Spec
				return spec.Writer.PodTemplate, spec.Reader.PodTemplate
			},
		},
	} {
		It("keeps "+test.kind+" pod template labels and annotations", func() {
			object := test.object("crd-schema-" + strings.ToLower(test.kind))
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, object))).To(Succeed()) })

			current := test.empty()
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(object), current)).To(Succeed())
			writer, reader := test.readBack(current)
			for _, template := range []*corev1.PodTemplateSpec{writer, reader} {
				Expect(template).NotTo(BeNil())
				Expect(template.Labels).To(HaveKeyWithValue("azure.workload.identity/use", "true"))
				Expect(template.Annotations).To(HaveKeyWithValue("k8s.grafana.com/scrape", "true"))
			}
		})
	}
})
