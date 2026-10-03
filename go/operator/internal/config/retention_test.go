package config

import (
	"strings"
	"testing"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

func TestParseRetention(t *testing.T) {
	for input, want := range map[string]int64{
		"30s":     30,
		"90m":     5400,
		"36h":     129600,
		"6d":      518400,
		"14d":     1209600,
		"2w":      1209600,
		"1w3d12h": 907200,
		"1d1s":    86401,
	} {
		got, err := parseRetention(input)
		if err != nil || got != want {
			t.Errorf("parseRetention(%q) = %d, %v; want %d", input, got, err, want)
		}
	}
	for _, input := range []string{"", "0d", "0w0s", "14", "d", "14x", "1.5d", "-1d", "1d ", "99999999999999999999w", "1000000000000000w"} {
		if got, err := parseRetention(input); err == nil {
			t.Errorf("parseRetention(%q) = %d; want error", input, got)
		}
	}
}

func TestRenderTranslatesRetentionForEveryProduct(t *testing.T) {
	legacy := int64(3600)
	for _, tc := range []struct {
		name  string
		input Input
		key   string
		want  string
	}{
		{name: "metrics", key: MetricsKey, want: "retention_seconds: 1209600", input: Input{Metrics: &telemetryv1alpha1.Metrics{
			Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{Retention: "2w"}},
		}}},
		{name: "logs", key: LogsKey, want: "retention_seconds: 518400", input: Input{Logs: &telemetryv1alpha1.Logs{
			Spec: telemetryv1alpha1.LogsSpec{Config: telemetryv1alpha1.LogsConfigSpec{Retention: "6d"}},
		}}},
		{name: "logs deprecated seconds", key: LogsKey, want: "retention_seconds: 3600", input: Input{Logs: &telemetryv1alpha1.Logs{
			Spec: telemetryv1alpha1.LogsSpec{Config: telemetryv1alpha1.LogsConfigSpec{RetentionSeconds: &legacy}},
		}}},
		{name: "traces", key: TracesKey, want: "retention_seconds: 1296000", input: Input{Traces: &telemetryv1alpha1.Traces{
			Spec: telemetryv1alpha1.TracesSpec{Config: telemetryv1alpha1.TracesConfigSpec{Retention: "2w1d"}},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.input.InternalToken = []byte("token")
			result, err := Render(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if rendered := string(result.Data[tc.key]); !strings.Contains(rendered, tc.want) {
				t.Fatalf("rendered config missing %q:\n%s", tc.want, rendered)
			}
		})
	}

	unset, err := Render(Input{Metrics: &telemetryv1alpha1.Metrics{}, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(unset.Data[MetricsKey]), "retention_seconds") {
		t.Fatal("unset retention must not render retention_seconds")
	}
	_, err = Render(Input{Metrics: &telemetryv1alpha1.Metrics{
		Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{Retention: "0d"}},
	}, InternalToken: []byte("token")})
	if err == nil {
		t.Fatal("zero retention should fail to render")
	}
}

func TestRenderRetentionPrefersDuration(t *testing.T) {
	legacy := int64(60)
	if got, err := renderRetention("", &legacy); err != nil || got != &legacy {
		t.Fatalf("empty retention should fall back to retentionSeconds, got %v, %v", got, err)
	}
	if got, err := renderRetention("", nil); err != nil || got != nil {
		t.Fatalf("unset retention should keep data forever, got %v, %v", got, err)
	}
	got, err := renderRetention("2w", nil)
	if err != nil || got == nil || *got != 1209600 {
		t.Fatalf("renderRetention(2w) = %v, %v", got, err)
	}
	if _, err := renderRetention("2y", nil); err == nil {
		t.Fatal("unknown unit should fail")
	}
}
