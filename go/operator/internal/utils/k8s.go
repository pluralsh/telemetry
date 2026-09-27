package utils

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClaimTemplateError struct {
	Claim  string
	Reason string
}

func (e *ClaimTemplateError) Error() string {
	return fmt.Sprintf("claim template %q cannot be changed: %s", e.Claim, e.Reason)
}

type ClaimExpansion struct {
	Claim   string
	OldSize resource.Quantity
	NewSize resource.Quantity
}

func CompareStatefulSetClaims(current, desired []corev1.PersistentVolumeClaim) ([]ClaimExpansion, error) {
	oldClaims, newClaims := claimMap(current), claimMap(desired)
	names := make([]string, 0, len(oldClaims)+len(newClaims))
	for name := range oldClaims {
		names = append(names, name)
	}
	for name := range newClaims {
		if _, found := oldClaims[name]; !found {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	expansions := make([]ClaimExpansion, 0)
	for _, name := range names {
		oldClaim, hadClaim := oldClaims[name]
		newClaim, wantsClaim := newClaims[name]
		if hadClaim != wantsClaim {
			return nil, &ClaimTemplateError{Claim: name, Reason: "persistent volume claim kind transition is immutable"}
		}
		if reason := immutableClaimChange(oldClaim.Spec, newClaim.Spec); reason != "" {
			return nil, &ClaimTemplateError{Claim: name, Reason: reason}
		}
		oldSize := oldClaim.Spec.Resources.Requests[corev1.ResourceStorage]
		newSize := newClaim.Spec.Resources.Requests[corev1.ResourceStorage]
		if newSize.Cmp(oldSize) < 0 {
			return nil, &ClaimTemplateError{Claim: name, Reason: fmt.Sprintf("shrinking from %s to %s is not supported", oldSize.String(), newSize.String())}
		}
		if newSize.Cmp(oldSize) > 0 {
			expansions = append(expansions, ClaimExpansion{Claim: name, OldSize: oldSize, NewSize: newSize})
		}
	}
	return expansions, nil
}

func PatchStatefulSetPVCs(ctx context.Context, c client.Client, namespace, statefulSet string, replicas int32, expansions []ClaimExpansion) error {
	for _, expansion := range expansions {
		for ordinal := int32(0); ordinal < replicas; ordinal++ {
			name := fmt.Sprintf("%s-%s-%d", expansion.Claim, statefulSet, ordinal)
			if err := PatchPVCSize(ctx, c, types.NamespacedName{Namespace: namespace, Name: name}, expansion.NewSize); err != nil {
				return fmt.Errorf("expand PVC %s from %s to %s: %w", name, expansion.OldSize.String(), expansion.NewSize.String(), err)
			}
		}
	}
	return nil
}

func PatchPVCSize(ctx context.Context, c client.Client, key types.NamespacedName, desired resource.Quantity) error {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, key, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if current.Cmp(desired) >= 0 {
		return nil
	}
	base := pvc.DeepCopy()
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
	return c.Patch(ctx, pvc, client.MergeFrom(base))
}

func StatefulSetResizePending(ctx context.Context, c client.Client, namespace, statefulSet string, claims []corev1.PersistentVolumeClaim, replicas int32) (bool, error) {
	for _, claim := range claims {
		desired := claim.Spec.Resources.Requests[corev1.ResourceStorage]
		for ordinal := int32(0); ordinal < replicas; ordinal++ {
			key := types.NamespacedName{Namespace: namespace, Name: fmt.Sprintf("%s-%s-%d", claim.Name, statefulSet, ordinal)}
			pvc := &corev1.PersistentVolumeClaim{}
			if err := c.Get(ctx, key, pvc); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("get PVC %s: %w", key.Name, err)
			}
			requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			if requested.Cmp(desired) < 0 {
				if err := PatchPVCSize(ctx, c, key, desired); err != nil {
					return false, fmt.Errorf("expand PVC %s to %s: %w", key.Name, desired.String(), err)
				}
				return true, nil
			}
			for _, condition := range pvc.Status.Conditions {
				if condition.Status == corev1.ConditionTrue &&
					(condition.Type == corev1.PersistentVolumeClaimResizing || condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending) {
					return true, nil
				}
			}
			if pvc.Status.Phase == corev1.ClaimBound {
				capacity := pvc.Status.Capacity[corev1.ResourceStorage]
				if capacity.Cmp(desired) < 0 {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func OrphanDeleteStatefulSet(ctx context.Context, c client.Client, statefulSet *appsv1.StatefulSet) error {
	policy := metav1.DeletePropagationOrphan
	if err := c.Delete(ctx, statefulSet, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("orphan-delete StatefulSet %s: %w", statefulSet.Name, err)
	}
	return nil
}

func immutableClaimChange(oldSpec, newSpec corev1.PersistentVolumeClaimSpec) string {
	if !reflect.DeepEqual(oldSpec.StorageClassName, newSpec.StorageClassName) {
		return "storageClassName is immutable"
	}
	oldModes := append([]corev1.PersistentVolumeAccessMode(nil), oldSpec.AccessModes...)
	newModes := append([]corev1.PersistentVolumeAccessMode(nil), newSpec.AccessModes...)
	slices.Sort(oldModes)
	slices.Sort(newModes)
	if !slices.Equal(oldModes, newModes) {
		return "accessModes are immutable"
	}
	if !reflect.DeepEqual(oldSpec.Selector, newSpec.Selector) {
		return "selector is immutable"
	}
	oldMode, newMode := corev1.PersistentVolumeFilesystem, corev1.PersistentVolumeFilesystem
	if oldSpec.VolumeMode != nil {
		oldMode = *oldSpec.VolumeMode
	}
	if newSpec.VolumeMode != nil {
		newMode = *newSpec.VolumeMode
	}
	if oldMode != newMode {
		return "volumeMode is immutable"
	}
	return ""
}

func claimMap(claims []corev1.PersistentVolumeClaim) map[string]corev1.PersistentVolumeClaim {
	result := make(map[string]corev1.PersistentVolumeClaim, len(claims))
	for _, claim := range claims {
		result[claim.Name] = claim
	}
	return result
}
