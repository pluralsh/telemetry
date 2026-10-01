package controller

import (
	"context"
	"testing"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

func TestEffectiveWriterReplicas(t *testing.T) {
	base := &telemetryv1alpha1.ShardMap{Spec: telemetryv1alpha1.ShardMapSpec{ShardCount: 3}}
	tests := []struct {
		name          string
		intent        int32
		current       int32
		shardMap      *telemetryv1alpha1.ShardMap
		trusted       bool
		want          int32
		wantBlockedBy bool
	}{
		{name: "scale up passes through", intent: 5, current: 3, shardMap: base, trusted: true, want: 5},
		{name: "downscale stops at shard count", intent: 2, current: 5, shardMap: base, trusted: true, want: 3, wantBlockedBy: true},
		{name: "missing ShardMap retains current replicas", intent: 2, current: 5, want: 5, wantBlockedBy: true},
		{
			name: "non-released assignment retains its ordinal", intent: 2, current: 5, trusted: true, want: 5, wantBlockedBy: true,
			shardMap: &telemetryv1alpha1.ShardMap{Spec: telemetryv1alpha1.ShardMapSpec{
				ShardCount: 3,
				Assignments: []telemetryv1alpha1.ShardAssignment{
					{Owner: telemetryv1alpha1.ShardOwner{Ordinal: 4}, State: "draining"},
				},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, blockedBy := effectiveWriterReplicas(test.intent, test.current, test.shardMap, test.trusted)
			if got != test.want {
				t.Fatalf("effective replicas = %d, want %d", got, test.want)
			}
			if (blockedBy != "") != test.wantBlockedBy {
				t.Fatalf("blocked reason = %q, want present=%v", blockedBy, test.wantBlockedBy)
			}
		})
	}
}

func TestMissingShardMapScaleUpAndDownscaleConservatism(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := telemetryv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Mode:   telemetryv1alpha1.ProductModeSharded,
			Writer: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(5))},
		},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName + "-writer", Namespace: testNamespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: lo.ToPtr(int32(3))},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(metrics, sts).Build()

	state, err := loadWriterScalingState(context.Background(), c, metrics, dataStoreMetrics, telemetryv1alpha1.ProductModeSharded, 5)
	if err != nil {
		t.Fatal(err)
	}
	if state.effective != 5 {
		t.Fatalf("scale-up effective replicas = %d, want 5", state.effective)
	}

	state, err = loadWriterScalingState(context.Background(), c, metrics, dataStoreMetrics, telemetryv1alpha1.ProductModeSharded, 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.effective != 3 || !state.blocked {
		t.Fatalf("missing-map downscale state = effective %d blocked %v, want 3/true", state.effective, state.blocked)
	}
}

func TestShardedReadyRequiresConvergedShardMap(t *testing.T) {
	count := int32(3)
	tests := []struct {
		name  string
		state writerScalingState
		ready bool
	}{
		{name: "converged", state: writerScalingState{trusted: true, intent: 3, status: telemetryv1alpha1.WriterScalingStatus{ShardCount: &count}}, ready: true},
		{name: "missing map", state: writerScalingState{intent: 3}, ready: false},
		{name: "intent differs", state: writerScalingState{trusted: true, intent: 4, status: telemetryv1alpha1.WriterScalingStatus{ShardCount: &count}}, ready: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := shardedReady(test.state)
			if got != test.ready {
				t.Fatalf("ready = %v, want %v", got, test.ready)
			}
		})
	}
}
