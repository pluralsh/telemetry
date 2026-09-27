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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

var _ = Describe("NamespaceAuthentication Controller", func() {
	const namespace = testNamespace

	ctx := context.Background()
	created := []client.Object{}

	AfterEach(func() {
		for i := len(created) - 1; i >= 0; i-- {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, created[i]))).To(Succeed())
		}
		created = nil
	})

	It("reports valid Meter and Secret references as ready", func() {
		meter := &telemetryv1alpha1.Meter{
			ObjectMeta: metav1.ObjectMeta{Name: "auth-valid-meter", Namespace: namespace},
			Spec:       telemetryv1alpha1.MeterSpec{Mode: telemetryv1alpha1.MeterModeStandalone},
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "auth-valid-password", Namespace: namespace},
			Data:       map[string][]byte{testPasswordKey: []byte("secret")},
		}
		auth := namespaceAuthentication("auth-valid", meter.Name, secret.Name)
		for _, object := range []client.Object{meter, secret, auth} {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			created = append(created, object)
		}

		reconciler := &NamespaceAuthenticationReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(auth)})
		Expect(err).NotTo(HaveOccurred())

		current := &telemetryv1alpha1.NamespaceAuthentication{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(auth), current)).To(Succeed())
		Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
		Expect(meta.FindStatusCondition(current.Status.Conditions, "Ready")).To(SatisfyAll(
			Not(BeNil()),
			HaveField("Status", metav1.ConditionTrue),
			HaveField("Reason", "Ready"),
			HaveField("Message", "authentication reference is valid"),
		))
	})

	It("records invalid references in status without returning a reconcile error", func() {
		auth := namespaceAuthentication("auth-invalid", "missing-meter", "missing-secret")
		Expect(k8sClient.Create(ctx, auth)).To(Succeed())
		created = append(created, auth)

		reconciler := &NamespaceAuthenticationReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(auth)})
		Expect(err).NotTo(HaveOccurred())

		current := &telemetryv1alpha1.NamespaceAuthentication{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: auth.Name}, current)).To(Succeed())
		condition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal("Invalid"))
		Expect(condition.Message).To(ContainSubstring("referenced Meter is unavailable"))
	})
})

func namespaceAuthentication(name, meterName, secretName string) *telemetryv1alpha1.NamespaceAuthentication {
	return &telemetryv1alpha1.NamespaceAuthentication{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: telemetryv1alpha1.NamespaceAuthenticationSpec{
			DataStoreRef: telemetryv1alpha1.DataStoreReference{Kind: "Meter", Name: meterName},
			Namespace:    testTenantNamespace,
			Username:     "reader",
			Permission:   "read",
			SecretKeyRef: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  testPasswordKey,
			},
		},
	}
}
