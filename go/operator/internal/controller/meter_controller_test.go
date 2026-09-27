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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	testNamespace       = "default"
	testPasswordKey     = "password"
	testTenantNamespace = "tenant-a"
)

var _ = Describe("Meter Controller", func() {
	const namespace = testNamespace

	ctx := context.Background()
	reconciler := &MeterReconciler{}
	created := []client.Object{}

	BeforeEach(func() {
		reconciler = &MeterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		created = nil
	})

	AfterEach(func() {
		for i := len(created) - 1; i >= 0; i-- {
			err := k8sClient.Delete(ctx, created[i])
			Expect(client.IgnoreNotFound(err)).To(Succeed())
		}
	})

	It("reconciles a standalone Meter with namespace authentication and readiness status", func() {
		meter, auth, password := createMeterFixture(ctx, "standalone-envtest", telemetryv1alpha1.MeterModeStandalone)
		created = append(created, meter, auth, password)
		key := client.ObjectKeyFromObject(meter)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: meter.Name + "-config"}, config)).To(Succeed())
		created = append(created, config)
		rendered := string(config.Data["meter.yaml"])
		Expect(rendered).To(ContainSubstring("mode: standalone"))
		Expect(rendered).To(ContainSubstring("name: tenant-a"))
		Expect(rendered).To(ContainSubstring("username: alice"))
		Expect(rendered).To(ContainSubstring("source: file"))
		Expect(rendered).To(ContainSubstring("/etc/meter/secrets/namespace-" + auth.Name + "-password"))
		Expect(config.Data["namespace-"+auth.Name+"-password"]).To(Equal([]byte("initial-password")))

		internalToken := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: meter.Name + "-internal-token"}, internalToken)).To(Succeed())
		created = append(created, internalToken)
		Expect(internalToken.Data["internal-token"]).NotTo(BeEmpty())

		assertServiceAccount(ctx, meter.Name)
		assertService(ctx, meter.Name, false)
		assertService(ctx, meter.Name+"-headless", true)
		sts := assertStatefulSet(ctx, meter.Name, componentStandalone, config)
		created = append(created,
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: meter.Name + "-headless", Namespace: namespace}},
			sts,
		)

		current := &telemetryv1alpha1.Meter{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(current.Status.ConfigHash).NotTo(BeEmpty())
		Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
		Expect(current.Status.WriterEndpoint).To(Equal("http://" + meter.Name + ":8080"))
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
	})

	It("reconciles sharded resources and rolls the config hash after credential rotation", func() {
		meter, auth, password := createMeterFixture(ctx, "sharded-envtest", telemetryv1alpha1.MeterModeSharded)
		created = append(created, meter, auth, password)
		key := client.ObjectKeyFromObject(meter)

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		config := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: meter.Name + "-config"}, config)).To(Succeed())
		created = append(created, config)
		Expect(string(config.Data["meter.yaml"])).To(SatisfyAll(
			ContainSubstring("mode: writer"),
			ContainSubstring("backend: kubernetes"),
			ContainSubstring("source: file"),
			ContainSubstring("/etc/meter/secrets/namespace-"+auth.Name+"-password"),
		))
		Expect(string(config.Data["reader.yaml"])).To(ContainSubstring("mode: reader"))
		Expect(config.Data["namespace-"+auth.Name+"-password"]).To(Equal([]byte("initial-password")))

		internalToken := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: meter.Name + "-internal-token"}, internalToken)).To(Succeed())
		created = append(created, internalToken)
		Expect(internalToken.Data["internal-token"]).NotTo(BeEmpty())
		assertServiceAccount(ctx, meter.Name)
		created = append(created, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: namespace}})

		for _, kind := range []component{componentWriter, componentReader} {
			name := meter.Name + "-" + string(kind)
			assertService(ctx, name, false)
			assertService(ctx, name+"-headless", true)
			sts := assertStatefulSet(ctx, name, kind, config)
			created = append(created,
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}},
				&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name + "-headless", Namespace: namespace}},
				sts,
			)
		}

		roleName := meter.Name + "-sharding"
		role := &rbacv1.Role{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: roleName}, role)).To(Succeed())
		Expect(role.Rules).To(HaveLen(3))
		Expect(role.Rules[2].ResourceNames).To(Equal([]string{meter.Name + "-writer"}))
		binding := &rbacv1.RoleBinding{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: roleName}, binding)).To(Succeed())
		Expect(binding.RoleRef.Name).To(Equal(roleName))
		Expect(binding.Subjects).To(ContainElement(HaveField("Name", meter.Name)))
		created = append(created, role, binding)

		current := &telemetryv1alpha1.Meter{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		originalHash := current.Status.ConfigHash
		Expect(originalHash).NotTo(BeEmpty())
		Expect(current.Status.WriterEndpoint).To(Equal("http://" + meter.Name + "-writer:8080"))
		Expect(current.Status.ReaderEndpoint).To(Equal("http://" + meter.Name + "-reader:8080"))
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
		for _, name := range []string{meter.Name + "-writer", meter.Name + "-reader"} {
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Annotations[configHashAnnotation]).To(Equal(current.Status.ConfigHash))
		}
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
		meter := &telemetryv1alpha1.Meter{
			ObjectMeta: metav1.ObjectMeta{Name: "storage-envtest", Namespace: namespace},
			Spec: telemetryv1alpha1.MeterSpec{Writer: telemetryv1alpha1.WorkloadSpec{
				DataVolume: persistentVolume("10Gi", &className),
			}},
		}
		Expect(k8sClient.Create(ctx, meter)).To(Succeed())
		created = append(created, meter)
		key := client.ObjectKeyFromObject(meter)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		sts := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, key, sts)).To(Succeed())
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "data-" + meter.Name + "-0",
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
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: meter.Name + "-config", Namespace: namespace}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: meter.Name + "-internal-token", Namespace: namespace}},
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: meter.Name + "-headless", Namespace: namespace}},
			sts,
		)

		current := &telemetryv1alpha1.Meter{}
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
})

func createMeterFixture(ctx context.Context, name string, mode telemetryv1alpha1.MeterMode) (*telemetryv1alpha1.Meter, *telemetryv1alpha1.NamespaceAuthentication, *corev1.Secret) {
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-password", Namespace: testNamespace},
		Data:       map[string][]byte{testPasswordKey: []byte("initial-password")},
	}
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{
			Mode:   mode,
			Config: telemetryv1alpha1.MeterConfigSpec{Namespaces: []string{testTenantNamespace}},
		},
	}
	auth := &telemetryv1alpha1.NamespaceAuthentication{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-auth", Namespace: testNamespace},
		Spec: telemetryv1alpha1.NamespaceAuthenticationSpec{
			DataStoreRef: telemetryv1alpha1.DataStoreReference{Kind: "Meter", Name: name},
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
	Expect(k8sClient.Create(ctx, meter)).To(Succeed())
	Expect(k8sClient.Create(ctx, auth)).To(Succeed())
	return meter, auth, password
}

func assertServiceAccount(ctx context.Context, name string) {
	serviceAccount := &corev1.ServiceAccount{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, serviceAccount)).To(Succeed())
	Expect(serviceAccount.OwnerReferences).To(ContainElement(HaveField("Name", name)))
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

func assertStatefulSet(ctx context.Context, name string, kind component, config *corev1.Secret) *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, sts)).To(Succeed())
	Expect(sts.Spec.ServiceName).To(Equal(name + "-headless"))
	Expect(sts.Spec.Template.Spec.ServiceAccountName).To(Equal(strings.TrimSuffix(strings.TrimSuffix(name, "-writer"), "-reader")))
	Expect(sts.Spec.Template.Annotations[configHashAnnotation]).NotTo(BeEmpty())
	Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
	Expect(sts.Spec.Template.Spec.Containers[0].Args).To(ContainElement("/etc/meter/meter.yaml"))
	Expect(sts.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(SatisfyAll(
		HaveField("Name", "config"),
		HaveField("MountPath", "/etc/meter/meter.yaml"),
		HaveField("SubPath", configKey(kind)),
	)))
	Expect(config.Data[configKey(kind)]).NotTo(BeEmpty())
	return sts
}
