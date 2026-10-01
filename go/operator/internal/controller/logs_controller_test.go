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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

var _ = Describe("Logs Controller", func() {
	ctx := context.Background()
	created := []client.Object{}

	AfterEach(func() {
		for i := len(created) - 1; i >= 0; i-- {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, created[i]))).To(Succeed())
		}
		created = nil
	})

	for _, test := range []struct {
		name string
		mode telemetryv1alpha1.LogsMode
	}{
		{name: "standalone", mode: telemetryv1alpha1.LogsModeStandalone},
		{name: "sharded", mode: telemetryv1alpha1.LogsModeSharded},
	} {
		It("reconciles a "+test.name+" Logs with server-compatible routes and storage", func() {
			name := "logs-" + test.name + "-envtest"
			password := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-password", Namespace: testNamespace},
				Data:       map[string][]byte{testPasswordKey: []byte("secret")},
			}
			logs := &telemetryv1alpha1.Logs{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
				Spec: telemetryv1alpha1.LogsSpec{
					Mode:    test.mode,
					Version: "2.3.4",
					Image: telemetryv1alpha1.ImageSpec{
						Repository: "registry.example.com/telemetry/logs",
						PullPolicy: corev1.PullAlways,
					},
					Config:  telemetryv1alpha1.LogsConfigSpec{Namespaces: []string{testTenantNamespace}},
					Ingress: telemetryv1alpha1.IngressSpec{Enabled: true, Hostname: "logs.example.com"},
				},
			}
			writerReplicas := int32(1)
			if test.mode == telemetryv1alpha1.LogsModeSharded {
				writerReplicas = 4
			}
			logs.Spec.Writer.Replicas = &writerReplicas
			logs.Spec.Writer.NodeSelector = map[string]string{"node.example.com/pool": "logs"}
			logs.Spec.Writer.Tolerations = []corev1.Toleration{{
				Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "telemetry", Effect: corev1.TaintEffectNoSchedule,
			}}
			auth := &telemetryv1alpha1.NamespaceAuthentication{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-auth", Namespace: testNamespace},
				Spec: telemetryv1alpha1.NamespaceAuthenticationSpec{
					DataStoreRef: telemetryv1alpha1.DataStoreReference{Kind: dataStoreLogs, Name: name},
					Namespace:    testTenantNamespace, Username: "alice", Permission: permissionRead,
					SecretKeyRef: corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: password.Name}, Key: testPasswordKey,
					},
				},
			}
			for _, object := range []client.Object{password, logs, auth} {
				Expect(k8sClient.Create(ctx, object)).To(Succeed())
				created = append(created, object)
			}
			reconciler := &LogsReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(logs)})
			Expect(err).NotTo(HaveOccurred())

			config := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name + "-config"}, config)).To(Succeed())
			created = append(created, config)
			Expect(string(config.Data["logs.yaml"])).To(SatisfyAll(
				ContainSubstring("name: "+testTenantNamespace),
				ContainSubstring("/etc/logs/secrets/namespace-"+auth.Name+"-password"),
				ContainSubstring("path: /var/lib/logs/data"),
			))
			if test.mode == telemetryv1alpha1.LogsModeStandalone {
				Expect(string(config.Data["logs.yaml"])).To(ContainSubstring("mode: standalone"))
				Expect(config.Data).NotTo(HaveKey("reader.yaml"))
			} else {
				Expect(string(config.Data["logs.yaml"])).To(ContainSubstring("mode: writer"))
				Expect(string(config.Data["reader.yaml"])).To(ContainSubstring("mode: reader"))
			}

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, ingress)).To(Succeed())
			created = append(created, ingress)
			Expect(ingress.Spec.Rules[0].HTTP.Paths).To(ContainElements(
				HaveField("Path", "/write"),
				HaveField("Path", "/read"),
			))

			components := []string{name}
			if test.mode == telemetryv1alpha1.LogsModeSharded {
				components = []string{name + "-writer", name + "-reader"}
				role := &rbacv1.Role{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name + "-sharding"}, role)).To(Succeed())
				created = append(created, role, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name + "-sharding", Namespace: testNamespace}})
			}
			for _, component := range components {
				sts := &appsv1.StatefulSet{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: component}, sts)).To(Succeed())
				Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(2))
				Expect(sts.Spec.Template.Spec.Containers[0]).To(SatisfyAll(
					HaveField("Name", "logs"),
					HaveField("Image", "registry.example.com/telemetry/logs:2.3.4"),
					HaveField("ImagePullPolicy", corev1.PullAlways),
					HaveField("Args", []string{"--config", "/etc/logs/logs.yaml"}),
				))
				if component == name || component == name+"-writer" {
					Expect(sts.Spec.Replicas).NotTo(BeNil())
					Expect(*sts.Spec.Replicas).To(Equal(writerReplicas))
					Expect(sts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("node.example.com/pool", "logs"))
					Expect(sts.Spec.Template.Spec.Tolerations).To(ContainElement(HaveField("Key", "dedicated")))
				} else {
					Expect(sts.Spec.Replicas).NotTo(BeNil())
					Expect(*sts.Spec.Replicas).To(Equal(int32(2)))
				}
				created = append(created, sts,
					&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: component, Namespace: testNamespace}},
					&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: component + "-headless", Namespace: testNamespace}},
				)
			}
			created = append(created,
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-internal-token", Namespace: testNamespace}},
				&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace}},
			)
			current := &telemetryv1alpha1.Logs{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(logs), current)).To(Succeed())
			Expect(current.Status.WriterEndpoint).To(ContainSubstring(":3100"))
			Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady)).NotTo(BeNil())
		})
	}
})
