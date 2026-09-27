package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const storageTestNamespace = "test"

func TestStorageExpansionPatchesPVCAndOrphanDeletesStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, telemetryv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: storageTestNamespace, UID: types.UID("meter")},
		Spec: telemetryv1alpha1.MeterSpec{Writer: telemetryv1alpha1.WorkloadSpec{
			DataVolume: persistentVolume("20Gi", nil),
		}},
	}
	current := mustDesiredStatefulSet(t, meter)
	current.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("10Gi")
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-example-0", Namespace: storageTestNamespace},
		Spec:       *current.Spec.VolumeClaimTemplates[0].Spec.DeepCopy(),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current, pvc).Build()
	reconciler := &MeterReconciler{Client: c, Scheme: scheme}
	result, err := reconciler.reconcileStatefulSet(context.Background(), meter, resources.ComponentStandalone, "config", "token", resources.TokenKey)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != storageResizeRequeue {
		t.Fatalf("unexpected requeue: %#v", result)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(current), &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet was not orphan-deleted: %v", err)
	}
	updated := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pvc), updated); err != nil {
		t.Fatalf("PVC was deleted: %v", err)
	}
	if got := updated.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("20Gi")) != 0 {
		t.Fatalf("PVC size = %s", got.String())
	}
}

func TestStorageErrorsMapToMeterStatusErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, telemetryv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: storageTestNamespace},
		Spec: telemetryv1alpha1.MeterSpec{Writer: telemetryv1alpha1.WorkloadSpec{
			DataVolume: &telemetryv1alpha1.VolumeSpec{PersistentVolumeClaim: &corev1.PersistentVolumeClaimSpec{}},
		}},
	}
	reconciler := &MeterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme}
	_, err := reconciler.reconcileStatefulSet(context.Background(), meter, resources.ComponentStandalone, "config", "token", resources.TokenKey)
	var storageErr *StorageChangeError
	if !errors.As(err, &storageErr) || !strings.Contains(err.Error(), "must be greater than zero") {
		t.Fatalf("expected mapped storage error, got %v", err)
	}
}

func persistentVolume(size string, class *string) *telemetryv1alpha1.VolumeSpec {
	return &telemetryv1alpha1.VolumeSpec{PersistentVolumeClaim: &corev1.PersistentVolumeClaimSpec{
		AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		StorageClassName: class,
		Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse(size),
		}},
	}}
}

func mustDesiredStatefulSet(t *testing.T, meter *telemetryv1alpha1.Meter) *appsv1.StatefulSet {
	t.Helper()
	statefulSet, err := resources.StatefulSet(resources.StatefulSetInput{
		Meter: meter, Component: resources.ComponentStandalone, ConfigSecretName: "config",
		InternalTokenSecretName: "token", InternalTokenSecretKey: resources.TokenKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	return statefulSet
}
