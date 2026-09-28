package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const (
	conditionWriterScaling        = "WriterScaling"
	conditionWriterScalingBlocked = "WriterScalingBlocked"

	reasonScalingConverged    = "ScalingConverged"
	reasonScalingInProgress   = "ScalingInProgress"
	reasonScaleDownBlocked    = "ScaleDownBlocked"
	reasonShardMapUnavailable = "ShardMapUnavailable"
)

type writerScalingState struct {
	status    telemetryv1alpha1.WriterScalingStatus
	intent    int32
	effective int32
	blocked   bool
	blockedBy string
	active    bool
	trusted   bool
}

func writerReplicaIntent(product any) int32 {
	return lo.FromPtrOr(resources.Replicas(product, resources.ComponentWriter), int32(3))
}

func shardMapName(product client.Object) string {
	return resources.Name(product.GetName(), "writer-shard-map")
}

func writerStatefulSetName(product client.Object) string {
	return resources.Name(product.GetName(), string(resources.ComponentWriter))
}

// effectiveWriterReplicas preserves user intent in the product spec while
// preventing the operator from deleting an ordinal still represented by the
// authoritative ShardMap. A scale-up always passes through immediately so the
// Rust coordinator can observe and assign the new ordinal.
func effectiveWriterReplicas(intent, current int32, shardMap *telemetryv1alpha1.ShardMap, trusted bool) (int32, string) {
	if intent >= current {
		return intent, ""
	}
	if !trusted || shardMap == nil {
		return current, "the authoritative ShardMap is missing or cannot be trusted"
	}
	floor := shardMap.Spec.ShardCount
	if migration := shardMap.Spec.Migration; migration != nil {
		floor = max(floor, migration.DesiredShardCount)
		floor = max(floor, migration.Split.SourceOwner.Ordinal+1)
		floor = max(floor, migration.Split.TargetOwner.Ordinal+1)
	}
	for _, assignment := range shardMap.Spec.Assignments {
		if assignment.State != "released" {
			floor = max(floor, assignment.Owner.Ordinal+1)
		}
	}
	if intent < floor {
		return floor, fmt.Sprintf("authoritative shard state requires at least %d writer replicas", floor)
	}
	return intent, ""
}

func loadWriterScalingState(ctx context.Context, c client.Client, product client.Object, kind string, mode telemetryv1alpha1.ProductMode, intent int32) (writerScalingState, error) {
	state := writerScalingState{
		intent: intent, effective: intent,
		status: telemetryv1alpha1.WriterScalingStatus{EffectiveWriterReplicas: intent},
	}
	sts := &appsv1.StatefulSet{}
	stsKey := types.NamespacedName{Namespace: product.GetNamespace(), Name: writerStatefulSetName(product)}
	if err := c.Get(ctx, stsKey, sts); err == nil {
		state.status.ReadyWriterReplicas = sts.Status.ReadyReplicas
		state.status.EffectiveWriterReplicas = lo.FromPtrOr(sts.Spec.Replicas, int32(1))
		state.effective = state.status.EffectiveWriterReplicas
	} else if !apierrors.IsNotFound(err) {
		return state, fmt.Errorf("get writer StatefulSet: %w", err)
	}
	if mode != telemetryv1alpha1.ProductModeSharded {
		state.status.EffectiveWriterReplicas = 1
		state.effective = 1
		state.trusted = true
		return state, nil
	}

	shardMap := &telemetryv1alpha1.ShardMap{}
	key := types.NamespacedName{Namespace: product.GetNamespace(), Name: shardMapName(product)}
	err := c.Get(ctx, key, shardMap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if intent >= state.effective {
				state.effective = intent
				state.status.EffectiveWriterReplicas = intent
			}
			state.blocked = intent < state.effective
			state.blockedBy = "the authoritative ShardMap is missing"
			return state, nil
		}
		return state, fmt.Errorf("get ShardMap: %w", err)
	}
	state.trusted = shardMapBelongsToProduct(shardMap, product, kind)
	if !state.trusted {
		if intent >= state.effective {
			state.effective = intent
			state.status.EffectiveWriterReplicas = intent
		}
		state.blocked = intent < state.effective
		state.blockedBy = "the ShardMap ownership metadata is uncertain"
		return state, nil
	}
	state.status.ShardCount = lo.ToPtr(shardMap.Spec.ShardCount)
	state.status.ShardGeneration = lo.ToPtr(shardMap.Spec.Generation)
	if migration := shardMap.Spec.Migration; migration != nil {
		state.active = true
		state.status.MigrationPhase = migration.Phase
		state.status.MigrationError = migration.Error
	}
	effective, blockedBy := effectiveWriterReplicas(intent, state.effective, shardMap, true)
	state.effective = effective
	state.status.EffectiveWriterReplicas = effective
	state.blocked = effective > intent
	state.blockedBy = blockedBy
	return state, nil
}

func synchronizeWriterReplicas(ctx context.Context, c client.Client, product client.Object, kind string, mode telemetryv1alpha1.ProductMode, desired *appsv1.StatefulSet) error {
	if mode != telemetryv1alpha1.ProductModeSharded {
		return nil
	}
	state, err := loadWriterScalingState(ctx, c, product, kind, mode, lo.FromPtrOr(desired.Spec.Replicas, int32(3)))
	if err != nil {
		// An uncertain read must never result in a downscale. Returning the
		// error leaves the current StatefulSet untouched.
		return err
	}
	desired.Spec.Replicas = lo.ToPtr(state.effective)
	return nil
}

func shardMapBelongsToProduct(shardMap *telemetryv1alpha1.ShardMap, product client.Object, kind string) bool {
	if shardMap.Labels["telemetry.plural.sh/"+strings.ToLower(kind)] == product.GetName() {
		return true
	}
	writer := writerStatefulSetName(product)
	for _, owner := range shardMap.OwnerReferences {
		if owner.APIVersion == appsv1.SchemeGroupVersion.String() && owner.Kind == "StatefulSet" && owner.Name == writer {
			return true
		}
	}
	return false
}

func requestsForShardMap(shardMap *telemetryv1alpha1.ShardMap, kind string) []reconcile.Request {
	name := shardMap.Labels["telemetry.plural.sh/"+strings.ToLower(kind)]
	if name == "" {
		for _, owner := range shardMap.OwnerReferences {
			if owner.APIVersion == appsv1.SchemeGroupVersion.String() && owner.Kind == "StatefulSet" {
				name = strings.TrimSuffix(owner.Name, "-writer")
				break
			}
		}
	}
	if name == "" {
		name = strings.TrimSuffix(shardMap.Name, "-writer-shard-map")
	}
	if name == "" || name == shardMap.Name {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: shardMap.Namespace, Name: name}}}
}

func setWriterScalingConditions(conditions *[]metav1.Condition, generation int64, state writerScalingState) {
	scaling := state.active || state.status.ShardCount == nil || *state.status.ShardCount != state.intent
	scalingStatus, scalingReason, scalingMessage := metav1.ConditionFalse, reasonScalingConverged, "writer intent and authoritative shard count have converged"
	if scaling {
		scalingStatus, scalingReason = metav1.ConditionTrue, reasonScalingInProgress
		scalingMessage = "writer scaling is waiting for the Rust shard coordinator"
	}
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: conditionWriterScaling, Status: scalingStatus, Reason: scalingReason,
		Message: scalingMessage, ObservedGeneration: generation,
	})
	blockedStatus, blockedReason, blockedMessage := metav1.ConditionFalse, reasonScalingConverged, "writer scale-down is not blocked"
	if state.blocked {
		blockedStatus, blockedReason, blockedMessage = metav1.ConditionTrue, reasonScaleDownBlocked, state.blockedBy
	} else if !state.trusted {
		blockedReason, blockedMessage = reasonShardMapUnavailable, "ShardMap is unavailable; any future writer scale-down will be held conservatively"
	}
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type: conditionWriterScalingBlocked, Status: blockedStatus, Reason: blockedReason,
		Message: blockedMessage, ObservedGeneration: generation,
	})
}

func shardedReady(state writerScalingState) (bool, string) {
	if !state.trusted || state.status.ShardCount == nil {
		return false, "authoritative ShardMap is unavailable"
	}
	if state.active {
		return false, fmt.Sprintf("shard migration is %s", state.status.MigrationPhase)
	}
	if state.intent != *state.status.ShardCount {
		return false, fmt.Sprintf("writer intent %d has not converged to shard count %d", state.intent, *state.status.ShardCount)
	}
	return true, ""
}
