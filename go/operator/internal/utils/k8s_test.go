package utils

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCompareStatefulSetClaims(t *testing.T) {
	oldClaim := claim("10Gi", nil)
	newClaim := claim("20Gi", nil)
	expansions, err := CompareStatefulSetClaims([]corev1.PersistentVolumeClaim{oldClaim}, []corev1.PersistentVolumeClaim{newClaim})
	if err != nil {
		t.Fatal(err)
	}
	if len(expansions) != 1 || expansions[0].Claim != "data" || expansions[0].NewSize.Cmp(resource.MustParse("20Gi")) != 0 {
		t.Fatalf("unexpected expansions: %#v", expansions)
	}

	_, err = CompareStatefulSetClaims([]corev1.PersistentVolumeClaim{newClaim}, []corev1.PersistentVolumeClaim{oldClaim})
	var claimErr *ClaimTemplateError
	if !errors.As(err, &claimErr) || !strings.Contains(err.Error(), "shrinking") {
		t.Fatalf("expected shrink error, got %v", err)
	}
	class := "other"
	changedClass := claim("10Gi", &class)
	_, err = CompareStatefulSetClaims([]corev1.PersistentVolumeClaim{oldClaim}, []corev1.PersistentVolumeClaim{changedClass})
	if !errors.As(err, &claimErr) || !strings.Contains(err.Error(), "storageClassName") {
		t.Fatalf("expected storage class error, got %v", err)
	}
}

func TestStatefulSetResizePending(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	desired := claim("20Gi", nil)
	tests := []struct {
		name    string
		status  corev1.PersistentVolumeClaimStatus
		pending bool
	}{
		{name: "resize condition", status: corev1.PersistentVolumeClaimStatus{Conditions: []corev1.PersistentVolumeClaimCondition{{Type: corev1.PersistentVolumeClaimResizing, Status: corev1.ConditionTrue}}}, pending: true},
		{name: "bound below desired", status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}}, pending: true},
		{name: "bound at desired", status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("20Gi")}}, pending: false},
		{name: "unbound request accepted", status: corev1.PersistentVolumeClaimStatus{}, pending: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "data-example-0", Namespace: "test"},
				Spec:       *desired.Spec.DeepCopy(),
				Status:     test.status,
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
			pending, err := StatefulSetResizePending(context.Background(), c, "test", "example", []corev1.PersistentVolumeClaim{desired}, 1)
			if err != nil {
				t.Fatal(err)
			}
			if pending != test.pending {
				t.Fatalf("pending = %t, want %t", pending, test.pending)
			}
		})
	}
}

func claim(size string, class *string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: class,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(size),
			}},
		},
	}
}
