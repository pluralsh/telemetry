package config

import (
	"testing"

	"github.com/samber/lo"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

func TestRenderSplitConfig(t *testing.T) {
	if got := renderSplitConfig(nil); got != nil {
		t.Fatalf("no spec must render nothing, got %+v", got)
	}
	off := &telemetryv1alpha1.ReadGroupSplitSpec{Enabled: lo.ToPtr(false), MaxFragments: lo.ToPtr(int32(8))}
	if got := renderSplitConfig(off); got != nil {
		t.Fatalf("disabled splitting must render nothing, got %+v", got)
	}

	defaults := renderSplitConfig(&telemetryv1alpha1.ReadGroupSplitSpec{Enabled: lo.ToPtr(true)})
	if defaults == nil || !defaults.Enabled || defaults.MaxFragments != 4 || defaults.MaxLocalInflight != 32 {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
	if defaults.MinFragmentCost != nil {
		t.Fatalf("an unset cost must leave the server default, got %+v", defaults.MinFragmentCost)
	}

	forced := renderSplitConfig(&telemetryv1alpha1.ReadGroupSplitSpec{
		Enabled:         lo.ToPtr(true),
		MinFragmentCost: lo.ToPtr(int64(0)),
	})
	if forced.MinFragmentCost == nil || *forced.MinFragmentCost != (renderMinFragmentCost{}) {
		t.Fatalf("a zero cost must be rendered so splitting is forced on: %+v", forced.MinFragmentCost)
	}
}

func TestRenderMetricsPushdown(t *testing.T) {
	if renderMetricsPushdown(nil) != nil {
		t.Fatal("no spec must render nothing")
	}
	if renderMetricsPushdown(&telemetryv1alpha1.MetricsPushdownSpec{Enabled: lo.ToPtr(false)}) != nil {
		t.Fatal("disabled pushdown must render nothing")
	}
	if got := renderMetricsPushdown(&telemetryv1alpha1.MetricsPushdownSpec{Enabled: lo.ToPtr(true)}); got == nil || !got.Enabled {
		t.Fatalf("enabled pushdown must render enabled: %+v", got)
	}
}
