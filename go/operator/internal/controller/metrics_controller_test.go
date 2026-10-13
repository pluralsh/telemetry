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
	"math"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

const (
	testNamespace                = "default"
	testMetricsName              = "example"
	testPasswordKey              = "password"
	testTenantNamespace          = "tenant-a"
	testServiceAccountAnnotation = "eks.amazonaws.com/role-arn"
	testServiceAccountRole       = "arn:aws:iam::123456789012:role/metrics"
	testIngressHostname          = "metrics.example.com"
)

var _ = Describe("Metrics Controller", func() {
	const namespace = testNamespace

	ctx := context.Background()
	reconciler := &MetricsReconciler{}
	created := []client.Object{}

	BeforeEach(func() {
		reconciler = &MetricsReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		created = nil
	})

	AfterEach(func() {
		for i := len(created) - 1; i >= 0; i-- {
			err := k8sClient.Delete(ctx, created[i])
			Expect(client.IgnoreNotFound(err)).To(Succeed())
		}
	})

	It("reconciles a standalone Metrics with namespace authentication and readiness status", func() {
		metrics, auth, password := createMetricsFixture(ctx, "standalone-envtest", telemetryv1alpha1.MetricsModeStandalone)
		created = append(created, metrics, auth, password)
		key := client.ObjectKeyFromObject(metrics)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-config"}, config)).To(Succeed())
		created = append(created, config)
		rendered := string(config.Data["metrics.yaml"])
		Expect(rendered).To(ContainSubstring("mode: standalone"))
		Expect(rendered).To(ContainSubstring("name: tenant-a"))
		Expect(rendered).To(ContainSubstring("username: alice"))
		Expect(rendered).To(ContainSubstring("source: file"))
		Expect(rendered).To(ContainSubstring("path_prefix: /metrics"))
		Expect(rendered).To(ContainSubstring("/etc/metrics/secrets/namespace-" + auth.Name + "-password"))
		Expect(config.Data["namespace-"+auth.Name+"-password"]).To(Equal([]byte("initial-password")))

		internalToken := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-internal-token"}, internalToken)).To(Succeed())
		created = append(created, internalToken)
		Expect(internalToken.Data["internal-token"]).NotTo(BeEmpty())

		assertServiceAccount(ctx, metrics.Name)
		assertService(ctx, metrics.Name, false)
		assertService(ctx, metrics.Name+"-headless", true)
		ingress := assertIngress(ctx, metrics.Name)
		Expect(ingress.Spec.Rules[0].HTTP.Paths).To(ContainElements(
			SatisfyAll(
				HaveField("Path", "/metrics/read"),
				HaveField("Backend.Service.Name", metrics.Name),
			),
			SatisfyAll(
				HaveField("Path", "/metrics/write"),
				HaveField("Backend.Service.Name", metrics.Name),
			),
		))
		Expect(ingress.Spec.TLS[0].SecretName).To(Equal(metrics.Name + "-tls"))
		sts := assertStatefulSet(ctx, metrics.Name, componentStandalone, config)
		created = append(created,
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-headless", Namespace: namespace}},
			ingress,
			sts,
		)

		current := &telemetryv1alpha1.Metrics{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(current.Status.ConfigHash).NotTo(BeEmpty())
		Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
		Expect(current.Status.WriterEndpoint).To(Equal("http://" + metrics.Name + ":8080"))
		Expect(current.Status.ReaderEndpoint).To(Equal(current.Status.WriterEndpoint))
		Expect(meta.FindStatusCondition(current.Status.Conditions, "Ready")).To(SatisfyAll(
			Not(BeNil()),
			HaveField("Status", metav1.ConditionFalse),
			HaveField("Reason", "Progressing"),
		))

		sts.Status.Replicas = 1
		sts.Status.ReadyReplicas = 1
		sts.Status.ObservedGeneration = sts.Generation
		Expect(k8sClient.Status().Update(ctx, sts)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(meta.FindStatusCondition(current.Status.Conditions, "Ready")).To(SatisfyAll(
			Not(BeNil()),
			HaveField("Status", metav1.ConditionTrue),
			HaveField("Reason", "Ready"),
		))

		current.Spec.Ingress.Enabled = false
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name}, &networkingv1.Ingress{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("reconciles sharded resources and rolls the config hash after credential rotation", func() {
		metrics, auth, password := createMetricsFixture(ctx, "sharded-envtest", telemetryv1alpha1.MetricsModeSharded)
		created = append(created, metrics, auth, password)
		metrics.Spec.Version = "1.2.3"
		metrics.Spec.Image = telemetryv1alpha1.ImageSpec{
			Repository: "registry.example.com/telemetry/metrics",
			PullPolicy: corev1.PullAlways,
		}
		metrics.Spec.Writer.Replicas = lo.ToPtr(int32(4))
		metrics.Spec.Writer.NodeSelector = map[string]string{"kubernetes.io/arch": "arm64"}
		metrics.Spec.Writer.Tolerations = []corev1.Toleration{{
			Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "telemetry", Effect: corev1.TaintEffectNoSchedule,
		}}
		Expect(k8sClient.Update(ctx, metrics)).To(Succeed())
		key := client.ObjectKeyFromObject(metrics)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-config"}, config)).To(Succeed())
		created = append(created, config)
		Expect(string(config.Data["metrics.yaml"])).To(SatisfyAll(
			ContainSubstring("mode: writer"),
			ContainSubstring("backend: kubernetes"),
			ContainSubstring("path_prefix: /metrics"),
			ContainSubstring("source: file"),
			ContainSubstring("/etc/metrics/secrets/namespace-"+auth.Name+"-password"),
		))
		Expect(string(config.Data["reader.yaml"])).To(ContainSubstring("mode: reader"))
		Expect(config.Data["namespace-"+auth.Name+"-password"]).To(Equal([]byte("initial-password")))

		internalToken := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-internal-token"}, internalToken)).To(Succeed())
		created = append(created, internalToken)
		Expect(internalToken.Data["internal-token"]).NotTo(BeEmpty())
		assertServiceAccount(ctx, metrics.Name)
		created = append(created, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}})

		writerName := metrics.Name + "-writer"
		assertService(ctx, writerName, false)
		assertService(ctx, writerName+"-headless", true)
		writer := assertStatefulSet(ctx, writerName, componentWriter, config)
		Expect(writer.Spec.Template.Spec.Containers[0]).To(SatisfyAll(
			HaveField("Image", "registry.example.com/telemetry/metrics:1.2.3"),
			HaveField("ImagePullPolicy", corev1.PullAlways),
		))
		Expect(writer.Spec.Replicas).NotTo(BeNil())
		Expect(*writer.Spec.Replicas).To(Equal(int32(4)))
		Expect(writer.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("kubernetes.io/arch", "arm64"))
		Expect(writer.Spec.Template.Spec.Tolerations).To(ContainElement(HaveField("Key", "dedicated")))
		created = append(created,
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: writerName, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: writerName + "-headless", Namespace: namespace}},
			writer,
		)

		Expect(string(config.Data["reader.yaml"])).To(SatisfyAll(
			ContainSubstring("read_groups:"),
			ContainSubstring("endpoint: "+metrics.Name+"-reader-{shard}-headless."+namespace+".svc:9090"),
		))
		assertService(ctx, metrics.Name+"-reader", false)
		created = append(created, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader", Namespace: namespace}})
		err = k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-reader-headless"}, &corev1.Service{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		for shard := range 4 {
			name := fmt.Sprintf("%s-reader-%d", metrics.Name, shard)
			assertService(ctx, name+"-headless", true)
			sts := assertStatefulSet(ctx, name, componentReader, config)
			Expect(sts.Spec.Replicas).NotTo(BeNil())
			Expect(*sts.Spec.Replicas).To(Equal(int32(2)))
			Expect(sts.Spec.Selector.MatchLabels).To(HaveKeyWithValue("telemetry.plural.sh/shard", fmt.Sprint(shard)))
			Expect(sts.Spec.Template.Spec.Containers[0].Env).To(ContainElement(corev1.EnvVar{Name: "READER_SHARD", Value: fmt.Sprint(shard)}))
			budget := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, budget)).To(Succeed())
			Expect(budget.Spec.MaxUnavailable.IntValue()).To(Equal(1))
			Expect(budget.Spec.Selector.MatchLabels).To(Equal(sts.Spec.Selector.MatchLabels))
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			created = append(created,
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name + "-headless", Namespace: namespace}},
				sts, budget,
			)
		}
		ingress := assertIngress(ctx, metrics.Name)
		Expect(ingress.Spec.Rules[0].HTTP.Paths).To(ContainElements(
			SatisfyAll(HaveField("Path", "/metrics/write"), HaveField("Backend.Service.Name", metrics.Name+"-writer")),
			SatisfyAll(HaveField("Path", "/metrics/read"), HaveField("Backend.Service.Name", metrics.Name+"-reader")),
		))
		created = append(created, ingress)

		roleName := metrics.Name + "-sharding"
		role := &rbacv1.Role{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: roleName}, role)).To(Succeed())
		Expect(role.Rules).To(HaveLen(3))
		Expect(role.Rules[2].ResourceNames).To(Equal([]string{metrics.Name + "-writer"}))
		binding := &rbacv1.RoleBinding{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: roleName}, binding)).To(Succeed())
		Expect(binding.RoleRef.Name).To(Equal(roleName))
		Expect(binding.Subjects).To(ContainElement(HaveField("Name", metrics.Name)))
		created = append(created, role, binding)

		current := &telemetryv1alpha1.Metrics{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		originalHash := current.Status.ConfigHash
		Expect(originalHash).NotTo(BeEmpty())
		Expect(current.Status.WriterEndpoint).To(Equal("http://" + metrics.Name + "-writer:8080"))
		Expect(current.Status.ReaderEndpoint).To(Equal("http://" + metrics.Name + "-reader:8080"))
		Expect(meta.FindStatusCondition(current.Status.Conditions, "Ready").Status).To(Equal(metav1.ConditionFalse))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(password), password)).To(Succeed())
		password.Data[testPasswordKey] = []byte("rotated-password")
		Expect(k8sClient.Update(ctx, password)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(current.Status.ConfigHash).NotTo(Equal(originalHash))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(config), config)).To(Succeed())
		Expect(config.Data["namespace-"+auth.Name+"-password"]).To(Equal([]byte("rotated-password")))
		for _, name := range []string{metrics.Name + "-writer", metrics.Name + "-reader-0"} {
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Annotations).NotTo(HaveKey(configHashAnnotation))
		}
	})

	It("autoscales reader groups and folds them back into one reader when groups are disabled", func() {
		metrics, auth, password := createMetricsFixture(ctx, "reader-groups-envtest", telemetryv1alpha1.MetricsModeSharded)
		created = append(created, metrics, auth, password)
		metrics.Spec.Writer.Replicas = lo.ToPtr(int32(2))
		metrics.Spec.Reader.Autoscaling = &telemetryv1alpha1.AutoscalingSpec{Enabled: true, MinReplicas: lo.ToPtr(int32(2)), MaxReplicas: 6}
		Expect(k8sClient.Update(ctx, metrics)).To(Succeed())
		key := client.ObjectKeyFromObject(metrics)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		groups := []string{metrics.Name + "-reader-0", metrics.Name + "-reader-1"}
		for _, name := range groups {
			autoscaler := &autoscalingv2.HorizontalPodAutoscaler{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, autoscaler)).To(Succeed())
			Expect(autoscaler.Spec.ScaleTargetRef).To(Equal(autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: name}))
			Expect(*autoscaler.Spec.MinReplicas).To(Equal(int32(2)))
			Expect(autoscaler.Spec.MaxReplicas).To(Equal(int32(6)))
			Expect(autoscaler.Spec.Metrics).To(ConsistOf(SatisfyAll(
				HaveField("Resource.Name", corev1.ResourceCPU),
				HaveField("Resource.Target.AverageUtilization", HaveValue(Equal(int32(70)))),
			)))
		}

		scaled := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: groups[0]}, scaled)).To(Succeed())
		scaled.Spec.Replicas = lo.ToPtr(int32(5))
		Expect(k8sClient.Update(ctx, scaled)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(scaled), scaled)).To(Succeed())
		Expect(*scaled.Spec.Replicas).To(Equal(int32(5)))

		Expect(k8sClient.Get(ctx, key, metrics)).To(Succeed())
		metrics.Spec.Reader.ReadGroups = &telemetryv1alpha1.ReadGroupsSpec{Enabled: lo.ToPtr(false)}
		Expect(k8sClient.Update(ctx, metrics)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		reader := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-reader"}, reader)).To(Succeed())
		Expect(reader.Spec.Template.Spec.Containers[0].Env).NotTo(ContainElement(HaveField("Name", "READER_SHARD")))
		for _, name := range groups {
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &appsv1.StatefulSet{})).To(Succeed(),
				"reader groups keep serving until the reader opening every shard is ready")
		}
		reader.Status.Replicas = *reader.Spec.Replicas
		reader.Status.ReadyReplicas = *reader.Spec.Replicas
		reader.Status.ObservedGeneration = reader.Generation
		Expect(k8sClient.Status().Update(ctx, reader)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		assertService(ctx, metrics.Name+"-reader-headless", true)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-reader"}, &autoscalingv2.HorizontalPodAutoscaler{})).To(Succeed())
		for _, name := range groups {
			for _, object := range []client.Object{
				&appsv1.StatefulSet{}, &policyv1.PodDisruptionBudget{}, &autoscalingv2.HorizontalPodAutoscaler{},
			} {
				err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, object)
				Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%T %s", object, name)
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-headless"}, &corev1.Service{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-config"}, config)).To(Succeed())
		Expect(string(config.Data["reader.yaml"])).NotTo(ContainSubstring("read_groups"))

		created = append(created, config, reader,
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-internal-token", Namespace: namespace}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-writer", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-writer", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-writer-headless", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader-headless", Namespace: namespace}},
			&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader", Namespace: namespace}},
			&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader", Namespace: namespace}},
			&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-sharding", Namespace: namespace}},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-sharding", Namespace: namespace}},
		)
	})

	It("passes scale-up through and blocks unsafe writer scale-down from ShardMap state", func() {
		metrics, auth, password := createMetricsFixture(ctx, "shard-scaling-envtest", telemetryv1alpha1.MetricsModeSharded)
		created = append(created, metrics, auth, password)
		key := client.ObjectKeyFromObject(metrics)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		writer := &appsv1.StatefulSet{}
		writerKey := types.NamespacedName{Namespace: namespace, Name: metrics.Name + "-writer"}
		Expect(k8sClient.Get(ctx, writerKey, writer)).To(Succeed())
		created = append(created,
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-config", Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-internal-token", Namespace: namespace}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-writer", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-writer-headless", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader", Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader-0-headless", Namespace: namespace}},
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-reader-0", Namespace: namespace}},
			&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-sharding", Namespace: namespace}},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-sharding", Namespace: namespace}},
			writer,
		)

		shardMap := &telemetryv1alpha1.ShardMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: metrics.Name + "-writer-shard-map", Namespace: namespace,
				Labels: map[string]string{"telemetry.plural.sh/metrics": metrics.Name},
			},
			Spec: telemetryv1alpha1.ShardMapSpec{
				Generation: 1, ShardCount: 3,
				Epochs: []telemetryv1alpha1.RoutingEpoch{{
					EffectiveFromNs: math.MinInt64,
					Routing:         telemetryv1alpha1.HashRangeMap{Generation: 1, Assignments: []telemetryv1alpha1.HashRangeAssignment{}},
				}},
				Assignments: []telemetryv1alpha1.ShardAssignment{},
			},
		}
		Expect(k8sClient.Create(ctx, shardMap)).To(Succeed())
		created = append(created, shardMap)

		current := &telemetryv1alpha1.Metrics{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		current.Spec.Writer.Replicas = lo.ToPtr(int32(5))
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, writerKey, writer)).To(Succeed())
		Expect(*writer.Spec.Replicas).To(Equal(int32(5)))
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(current.Status.EffectiveWriterReplicas).To(Equal(int32(5)))
		Expect(current.Status.ShardCount).NotTo(BeNil())
		Expect(*current.Status.ShardCount).To(Equal(int32(3)))
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionWriterScaling).Status).To(Equal(metav1.ConditionTrue))
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady).Status).To(Equal(metav1.ConditionFalse))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(shardMap), shardMap)).To(Succeed())
		shardMap.Spec.Generation = 2
		shardMap.Spec.ShardCount = 5
		shardMap.Spec.Epochs = append(shardMap.Spec.Epochs, telemetryv1alpha1.RoutingEpoch{
			EffectiveFromNs: 3_600_000_000_000,
			Routing: telemetryv1alpha1.HashRangeMap{Generation: 5, Assignments: []telemetryv1alpha1.HashRangeAssignment{{
				Shard: 0,
				Range: telemetryv1alpha1.HashRange{Start: strings.Repeat("0", 32), End: strings.Repeat("f", 32)},
			}}},
		})
		Expect(k8sClient.Update(ctx, shardMap)).To(Succeed())
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		current.Spec.Writer.Replicas = lo.ToPtr(int32(2))
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, writerKey, writer)).To(Succeed())
		Expect(*writer.Spec.Replicas).To(Equal(int32(5)))
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(current.Status.RoutingEpochs).NotTo(BeNil())
		Expect(*current.Status.RoutingEpochs).To(Equal(int32(2)))
		Expect(current.Status.EffectiveWriterReplicas).To(Equal(int32(5)))
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionWriterScalingBlocked).Status).To(Equal(metav1.ConditionTrue))
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady).Status).To(Equal(metav1.ConditionFalse))
	})

	It("expands an existing PVC and recreates its StatefulSet deterministically", func() {
		className := "storage-envtest-expandable"
		allowExpansion := true
		storageClass := &storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: className},
			Provisioner:          "example.com/envtest",
			AllowVolumeExpansion: &allowExpansion,
		}
		Expect(k8sClient.Create(ctx, storageClass)).To(Succeed())
		created = append(created, storageClass)
		metrics := &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: "storage-envtest", Namespace: namespace},
			Spec: telemetryv1alpha1.MetricsSpec{Writer: telemetryv1alpha1.WorkloadSpec{
				DataVolume: persistentVolume("10Gi", &className),
			}},
		}
		Expect(k8sClient.Create(ctx, metrics)).To(Succeed())
		created = append(created, metrics)
		key := client.ObjectKeyFromObject(metrics)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "data-" + metrics.Name + "-0",
				Namespace:   namespace,
				Annotations: map[string]string{"volume.kubernetes.io/storage-provisioner": storageClass.Provisioner},
			},
			Spec: *sts.Spec.VolumeClaimTemplates[0].Spec.DeepCopy(),
		}
		pvc.Spec.VolumeName = "manual-envtest-volume"
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		pvc.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
		created = append(created,
			pvc,
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-config", Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-internal-token", Namespace: namespace}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: metrics.Name + "-headless", Namespace: namespace}},
			sts,
		)

		current := &telemetryv1alpha1.Metrics{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		current.Spec.Writer.DataVolume = persistentVolume("20Gi", &className)
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(storageResizeRequeue))
		terminating := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, terminating)).To(Succeed())
		Expect(terminating.DeletionTimestamp.IsZero()).To(BeFalse())
		// envtest has no garbage collector to complete orphan propagation.
		terminating.Finalizers = nil
		Expect(k8sClient.Update(ctx, terminating)).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, key, &appsv1.StatefulSet{}))
		}).Should(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)).To(Succeed())
		pvcSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(pvcSize.Cmp(resource.MustParse("20Gi"))).To(Equal(0))
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(meta.FindStatusCondition(current.Status.Conditions, conditionReady).Reason).To(Equal(reasonStorageResizing))

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		recreated := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, recreated)).To(Succeed())
		templateSize := recreated.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(templateSize.Cmp(resource.MustParse("20Gi"))).To(Equal(0))
	})

	It("validates canonical versions and conflicting legacy tags for Metrics and Logs", func() {
		invalidVersionMetrics := &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-version-metrics", Namespace: namespace},
			Spec:       telemetryv1alpha1.MetricsSpec{Version: "v1.2.3"},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, invalidVersionMetrics))).To(BeTrue())

		invalidVersionLogs := &telemetryv1alpha1.Logs{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-version-line", Namespace: namespace},
			Spec:       telemetryv1alpha1.LogsSpec{Version: "1.2"},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, invalidVersionLogs))).To(BeTrue())

		invalidLegacyTagMetrics := &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-legacy-tag-metrics", Namespace: namespace},
			Spec:       telemetryv1alpha1.MetricsSpec{Image: telemetryv1alpha1.ImageSpec{Tag: "latest"}},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, invalidLegacyTagMetrics))).To(BeTrue())

		invalidLegacyTagLogs := &telemetryv1alpha1.Logs{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-legacy-tag-line", Namespace: namespace},
			Spec:       telemetryv1alpha1.LogsSpec{Image: telemetryv1alpha1.ImageSpec{Tag: "v2.3.4"}},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, invalidLegacyTagLogs))).To(BeTrue())

		conflictingMetrics := &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: "conflicting-version-metrics", Namespace: namespace},
			Spec: telemetryv1alpha1.MetricsSpec{
				Version: "1.2.3",
				Image:   telemetryv1alpha1.ImageSpec{Tag: "1.2.4"},
			},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, conflictingMetrics))).To(BeTrue())

		conflictingLogs := &telemetryv1alpha1.Logs{
			ObjectMeta: metav1.ObjectMeta{Name: "conflicting-version-line", Namespace: namespace},
			Spec: telemetryv1alpha1.LogsSpec{
				Version: "2.3.4",
				Image:   telemetryv1alpha1.ImageSpec{Tag: "2.3.5"},
			},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, conflictingLogs))).To(BeTrue())

		valid := &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: "valid-version-metrics", Namespace: namespace},
			Spec: telemetryv1alpha1.MetricsSpec{
				Version: "1.2.3-rc.1+build.7",
				Image:   telemetryv1alpha1.ImageSpec{Tag: "1.2.3-rc.1+build.7"},
			},
		}
		Expect(k8sClient.Create(ctx, valid)).To(Succeed())
		created = append(created, valid)
	})

	It("rejects replica settings that violate standalone architecture", func() {
		metrics := &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-standalone-metrics", Namespace: namespace},
			Spec: telemetryv1alpha1.MetricsSpec{
				Mode:   telemetryv1alpha1.MetricsModeStandalone,
				Writer: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(2))},
			},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, metrics))).To(BeTrue())

		line := &telemetryv1alpha1.Logs{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-standalone-line", Namespace: namespace},
			Spec: telemetryv1alpha1.LogsSpec{
				Mode:   telemetryv1alpha1.LogsModeStandalone,
				Reader: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(1))},
			},
		}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, line))).To(BeTrue())
	})
})

func createMetricsFixture(ctx context.Context, name string, mode telemetryv1alpha1.MetricsMode) (*telemetryv1alpha1.Metrics, *telemetryv1alpha1.NamespaceAuthentication, *corev1.Secret) {
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-password", Namespace: testNamespace},
		Data:       map[string][]byte{testPasswordKey: []byte("initial-password")},
	}
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Mode: mode,
			Ingress: telemetryv1alpha1.IngressSpec{
				Enabled: true, Hostname: testIngressHostname, IngressClass: "nginx", PathPrefix: "/metrics",
				TLS: telemetryv1alpha1.IngressTLSSpec{Enabled: true},
			},
			ServiceAccount: telemetryv1alpha1.ServiceAccountSpec{
				Annotations: map[string]string{testServiceAccountAnnotation: testServiceAccountRole},
			},
			Config: telemetryv1alpha1.MetricsConfigSpec{Namespaces: []string{testTenantNamespace}},
		},
	}
	auth := &telemetryv1alpha1.NamespaceAuthentication{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-auth", Namespace: testNamespace},
		Spec: telemetryv1alpha1.NamespaceAuthenticationSpec{
			DataStoreRef: telemetryv1alpha1.DataStoreReference{Kind: "Metrics", Name: name},
			Namespace:    testTenantNamespace,
			Username:     "alice",
			Permission:   "read",
			SecretKeyRef: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: password.Name},
				Key:                  testPasswordKey,
			},
		},
	}
	Expect(k8sClient.Create(ctx, password)).To(Succeed())
	Expect(k8sClient.Create(ctx, metrics)).To(Succeed())
	Expect(k8sClient.Create(ctx, auth)).To(Succeed())
	return metrics, auth, password
}

func assertServiceAccount(ctx context.Context, name string) {
	serviceAccount := &corev1.ServiceAccount{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, serviceAccount)).To(Succeed())
	Expect(serviceAccount.OwnerReferences).To(ContainElement(HaveField("Name", name)))
	Expect(serviceAccount.Annotations).To(HaveKeyWithValue(testServiceAccountAnnotation, testServiceAccountRole))
}

func assertIngress(ctx context.Context, name string) *networkingv1.Ingress {
	ingress := &networkingv1.Ingress{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, ingress)).To(Succeed())
	Expect(ingress.OwnerReferences).To(ContainElement(HaveField("Name", name)))
	Expect(ingress.Spec.Rules).To(HaveLen(1))
	Expect(ingress.Spec.Rules[0].Host).To(Equal(testIngressHostname))
	return ingress
}

func assertService(ctx context.Context, name string, headless bool) {
	service := &corev1.Service{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, service)).To(Succeed())
	if headless {
		Expect(service.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		Expect(service.Spec.Ports).To(HaveLen(1))
		Expect(service.Spec.Ports[0].Name).To(Equal("grpc"))
	} else {
		Expect(service.Spec.Ports).To(HaveLen(2))
	}
}

var componentSuffix = regexp.MustCompile(`-(writer|reader(-\d+)?)$`)

func assertStatefulSet(ctx context.Context, name string, kind component, config *corev1.Secret) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, sts)).To(Succeed())
	Expect(sts.Spec.ServiceName).To(Equal(name + "-headless"))
	Expect(sts.Spec.Template.Spec.ServiceAccountName).To(Equal(componentSuffix.ReplaceAllString(name, "")))
	Expect(sts.Spec.Template.Annotations).NotTo(HaveKey(configHashAnnotation))
	Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
	Expect(sts.Spec.Template.Spec.Containers[0].Args).To(ContainElement("/etc/metrics/config/" + configKey(kind)))
	Expect(sts.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(SatisfyAll(
		HaveField("Name", "config"),
		HaveField("MountPath", "/etc/metrics/config"),
		HaveField("SubPath", ""),
	)))
	Expect(config.Data[configKey(kind)]).NotTo(BeEmpty())
	return sts
}
