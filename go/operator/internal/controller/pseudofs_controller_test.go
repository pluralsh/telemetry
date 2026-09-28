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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

var _ = Describe("PseudoFS Controller", func() {
	ctx := context.Background()
	created := []client.Object{}

	AfterEach(func() {
		for i := len(created) - 1; i >= 0; i-- {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, created[i]))).To(Succeed())
		}
		created = nil
	})

	It("reconciles one combined gRPC-only StatefulSet", func() {
		pseudofs := &telemetryv1alpha1.PseudoFS{
			ObjectMeta: metav1.ObjectMeta{Name: "pseudofs-envtest", Namespace: testNamespace},
		}
		Expect(k8sClient.Create(ctx, pseudofs)).To(Succeed())
		created = append(created, pseudofs)
		reconciler := &PseudoFSReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pseudofs)})
		Expect(err).NotTo(HaveOccurred())

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: pseudofs.Name + "-config"}, config)).To(Succeed())
		Expect(config.Data).To(HaveKey("pseudofs.yaml"))
		Expect(string(config.Data["pseudofs.yaml"])).To(SatisfyAll(
			ContainSubstring("listener: 0.0.0.0:9093"),
			ContainSubstring("filesystem:"),
			ContainSubstring("chunk_size_bytes: 1048576"),
		))

		service := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pseudofs), service)).To(Succeed())
		Expect(service.Spec.Ports).To(HaveLen(1))
		Expect(service.Spec.Ports[0]).To(SatisfyAll(HaveField("Name", "grpc"), HaveField("Port", int32(9093))))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: testNamespace, Name: pseudofs.Name + "-headless"},
			&corev1.Service{}))).To(BeTrue())

		statefulSet := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pseudofs), statefulSet)).To(Succeed())
		Expect(statefulSet.Spec.Replicas).NotTo(BeNil())
		Expect(*statefulSet.Spec.Replicas).To(Equal(int32(1)))
		Expect(statefulSet.Spec.VolumeClaimTemplates).To(HaveLen(2))
		Expect(statefulSet.Spec.Template.Spec.Containers[0]).To(SatisfyAll(
			HaveField("Name", "pseudofs"),
			HaveField("Image", "ghcr.io/pluralsh/pseudofs:0.1.0"),
		))

		created = append(created,
			config,
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: pseudofs.Name, Namespace: testNamespace}},
			service,
			statefulSet,
		)
	})

	It("rejects any replica count other than one", func() {
		pseudofs := &telemetryv1alpha1.PseudoFS{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-pseudofs-replicas", Namespace: testNamespace},
			Spec: telemetryv1alpha1.PseudoFSSpec{
				Workload: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(2))},
			},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, pseudofs))).To(BeTrue())
	})
})
