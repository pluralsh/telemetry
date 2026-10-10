package config

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const (
	testMetricsName         = "example"
	testMetricsNamespace    = "test"
	testBucket              = "metrics"
	testTenantNamespace     = "tenant-a"
	testFlushIntervalConfig = "flush_interval_seconds: 10"
	testCacheWarmerConfig   = "cache_warmer:"
	testWarmRangeConfig     = "warm_range_seconds: 7200"
	testWarmTimeoutConfig   = "timeout_seconds: 30"
	testWarmConcurrency     = "concurrency: 2"
	testWarmPayloadsConfig  = "include_payloads: false"
	testContinuousWarmer    = "continuous:"
	testContinuousInterval  = "interval_seconds: 10"
	testMetricsProduct      = "metrics"
	testLogsProduct         = "logs"
	testTracesProduct       = "traces"
)

func TestRenderDefaultsCredentialsAndHash(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testMetricsNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Config:  telemetryv1alpha1.MetricsConfigSpec{Namespaces: []string{"default", "default"}},
			Ingress: telemetryv1alpha1.IngressSpec{PathPrefix: "/metrics"},
		},
	}
	input := Input{
		Metrics: metrics,
		Global:  Access{Read: []Credential{{Username: "reader", Password: []byte("s3cret")}}},
		Namespaces: []NamespaceAccess{{
			Name: testTenantNamespace, KeyPrefix: "tenant-auth",
			Access: Access{Write: []Credential{{Username: "writer", Password: []byte("tenant-secret"), DataKey: "namespace-tenant-auth-password"}}},
		}},
		InternalTokenPath: "/var/run/secrets/metrics/internal-token",
		InternalToken:     []byte("token"),
	}
	result, err := Render(input)
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(result.Data[MetricsKey])
	for _, expected := range []string{
		"mode: standalone", "http: 0.0.0.0:8080", "grpc: 0.0.0.0:9090",
		"reader_cache_capacity: 268435456", "forward_index_cache_capacity_bytes: 67108864", testFlushIntervalConfig,
		testCacheWarmerConfig, "enabled: false", testWarmRangeConfig, testWarmTimeoutConfig, testWarmConcurrency, testWarmPayloadsConfig,
		testContinuousWarmer, testContinuousInterval,
		"shards: 1", "io_concurrency_limit: 128",
		"type: Local", "path: /var/lib/metrics/data",
		"path_prefix: /metrics",
		"path: /etc/metrics/secrets/global-read-0-password",
		"path: /etc/metrics/secrets/namespace-tenant-auth-password",
		"path: /var/run/secrets/metrics/internal-token",
		"unauthenticated: false",
	} {
		if !strings.Contains(rendered, expected) {
			t.Errorf("rendered config missing %q:\n%s", expected, rendered)
		}
	}
	if got := string(result.Data["global-read-0-password"]); got != "s3cret" {
		t.Fatalf("resolved password = %q", got)
	}
	if strings.Count(rendered, "name: default") != 1 {
		t.Fatalf("default namespace was not deduplicated:\n%s", rendered)
	}
	if result.Hash == "" {
		t.Fatal("hash was not generated")
	}
	input.InternalToken = []byte("different-token")
	changed, err := Render(input)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Hash == result.Hash {
		t.Fatal("internal token did not participate in rollout hash")
	}
}

func TestRenderExplicitUnauthenticatedAccess(t *testing.T) {
	for _, product := range []struct {
		name  string
		input Input
		key   string
	}{
		{
			name: testMetricsProduct,
			input: Input{Metrics: &telemetryv1alpha1.Metrics{
				Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{
					Auth: telemetryv1alpha1.AuthSpec{Unauthenticated: true},
				}},
			}},
			key: MetricsKey,
		},
		{
			name: testLogsProduct,
			input: Input{Logs: &telemetryv1alpha1.Logs{
				Spec: telemetryv1alpha1.LogsSpec{Config: telemetryv1alpha1.LogsConfigSpec{
					Auth: telemetryv1alpha1.AuthSpec{Unauthenticated: true},
				}},
			}},
			key: LogsKey,
		},
		{
			name: testTracesProduct,
			input: Input{Traces: &telemetryv1alpha1.Traces{
				Spec: telemetryv1alpha1.TracesSpec{Config: telemetryv1alpha1.TracesConfigSpec{
					Auth: telemetryv1alpha1.AuthSpec{Unauthenticated: true},
				}},
			}},
			key: TracesKey,
		},
	} {
		t.Run(product.name, func(t *testing.T) {
			result, err := Render(product.input)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(result.Data[product.key]), "unauthenticated: true") {
				t.Fatalf("rendered config did not enable anonymous access:\n%s", result.Data[product.key])
			}
		})
	}
}

func TestRenderUsageReportingEndpoints(t *testing.T) {
	namespaces := []NamespaceAccess{
		{Name: testTenantNamespace, KeyPrefix: "tenant-write", UsageReportingEndpoint: "z-console:50051"},
		{Name: testTenantNamespace, KeyPrefix: "tenant-read", UsageReportingEndpoint: "console:50051"},
		{Name: "unmetered", KeyPrefix: "unmetered"},
	}
	for _, product := range []struct {
		name  string
		input Input
		key   string
	}{
		{name: testMetricsProduct, input: Input{Metrics: &telemetryv1alpha1.Metrics{}, Namespaces: namespaces}, key: MetricsKey},
		{name: testLogsProduct, input: Input{Logs: &telemetryv1alpha1.Logs{}, Namespaces: namespaces}, key: LogsKey},
		{name: testTracesProduct, input: Input{Traces: &telemetryv1alpha1.Traces{}, Namespaces: namespaces}, key: TracesKey},
	} {
		t.Run(product.name, func(t *testing.T) {
			result, err := Render(product.input)
			if err != nil {
				t.Fatal(err)
			}
			rendered := string(result.Data[product.key])
			if strings.Count(rendered, "usage_reporting_endpoint:") != 1 || !strings.Contains(rendered, "usage_reporting_endpoint: console:50051") {
				t.Fatalf("expected only the smallest tenant endpoint to render:\n%s", rendered)
			}
		})
	}
}

func TestRenderTracesShardedConfig(t *testing.T) {
	traces := &telemetryv1alpha1.Traces{
		ObjectMeta: metav1.ObjectMeta{Name: testTracesProduct, Namespace: "observability"},
		Spec: telemetryv1alpha1.TracesSpec{
			Mode:    telemetryv1alpha1.TracesModeSharded,
			Ingress: telemetryv1alpha1.IngressSpec{PathPrefix: "/traces"},
		},
	}
	result, err := Render(Input{
		Traces: traces, InternalToken: []byte("token"),
		Namespaces: []NamespaceAccess{{
			Name: testTenantNamespace,
			Access: Access{Read: []Credential{{
				Username: "tempo", Password: []byte("tenant-secret"), DataKey: "namespace-traces-auth-password",
			}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := string(result.Data[TracesKey])
	reader := string(result.Data[ReaderKey])
	for _, expected := range []string{
		"mode: writer",
		"otlp_grpc: 0.0.0.0:4317",
		"jaeger_grpc: 0.0.0.0:14250",
		"path_prefix: /traces",
		"backend: kubernetes",
		"database: traces",
		"stateful_set: traces-writer",
		"headless_service: traces-writer-headless",
		testFlushIntervalConfig,
		"max_candidates: 10000",
		testCacheWarmerConfig,
		testWarmRangeConfig,
		testWarmTimeoutConfig,
		testWarmConcurrency,
		testWarmPayloadsConfig,
		testContinuousWarmer,
		testContinuousInterval,
		"name: tenant-a",
		"path: /etc/traces/secrets/namespace-traces-auth-password",
	} {
		if !strings.Contains(writer, expected) {
			t.Errorf("Traces writer config missing %q:\n%s", expected, writer)
		}
	}
	if !strings.Contains(reader, "mode: reader") {
		t.Fatalf("Traces reader config missing reader mode:\n%s", reader)
	}
	if strings.Contains(writer, "block_cache:") {
		t.Fatalf("Traces writer unexpectedly contains the default data cache:\n%s", writer)
	}
	if !strings.Contains(reader, "block_cache:") {
		t.Fatalf("Traces reader is missing the default data cache:\n%s", reader)
	}
	if got := string(result.Data["namespace-traces-auth-password"]); got != "tenant-secret" {
		t.Fatalf("resolved Traces namespace password = %q", got)
	}
}

func TestRenderShardedRoles(t *testing.T) {
	ioConcurrencyLimit := int32(96)
	forwardIndexCapacity := int64(42)
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testMetricsNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Mode: telemetryv1alpha1.MetricsModeSharded,
			Config: telemetryv1alpha1.MetricsConfigSpec{
				ForwardIndexCacheCapacityBytes: &forwardIndexCapacity,
				Sharding: telemetryv1alpha1.ShardingSpec{
					IOConcurrencyLimit: &ioConcurrencyLimit,
				},
			},
		},
	}
	result, err := Render(Input{Metrics: metrics, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.Data[MetricsKey]), "mode: writer") ||
		!strings.Contains(string(result.Data[MetricsKey]), "backend: kubernetes") ||
		!strings.Contains(string(result.Data[MetricsKey]), "shard_map: example-writer-shard-map") ||
		!strings.Contains(string(result.Data[MetricsKey]), "io_concurrency_limit: 96") ||
		!strings.Contains(string(result.Data[MetricsKey]), "forward_index_cache_capacity_bytes: 42") ||
		strings.Contains(string(result.Data[MetricsKey]), "shards:") ||
		!strings.Contains(string(result.Data[ReaderKey]), "mode: reader") {
		t.Fatalf("unexpected sharded configs:\n%s\n%s", result.Data[MetricsKey], result.Data[ReaderKey])
	}
	if strings.Contains(string(result.Data[MetricsKey]), "block_cache:") {
		t.Fatalf("Metrics writer unexpectedly contains the default data cache:\n%s", result.Data[MetricsKey])
	}
	if !strings.Contains(string(result.Data[ReaderKey]), "block_cache:") {
		t.Fatalf("Metrics reader is missing the default data cache:\n%s", result.Data[ReaderKey])
	}
}

func TestRenderInfersIOConcurrencyFromComponentMemoryRequests(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testMetricsNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Mode: telemetryv1alpha1.MetricsModeSharded,
			Writer: telemetryv1alpha1.WorkloadSpec{Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
			}},
			Reader: telemetryv1alpha1.WorkloadSpec{Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
			}},
		},
	}

	result, err := Render(Input{Metrics: metrics, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.Data[MetricsKey]), "io_concurrency_limit: 192") {
		t.Fatalf("writer config did not infer I/O concurrency from its memory request:\n%s", result.Data[MetricsKey])
	}
	if !strings.Contains(string(result.Data[MetricsKey]), "forward_index_cache_capacity_bytes: 134217728") {
		t.Fatalf("writer config did not infer forward-index capacity from its memory request:\n%s", result.Data[MetricsKey])
	}
	if !strings.Contains(string(result.Data[ReaderKey]), "io_concurrency_limit: 384") {
		t.Fatalf("reader config did not infer I/O concurrency from its memory request:\n%s", result.Data[ReaderKey])
	}
	if !strings.Contains(string(result.Data[ReaderKey]), "forward_index_cache_capacity_bytes: 268435456") {
		t.Fatalf("reader config did not infer forward-index capacity from its memory request:\n%s", result.Data[ReaderKey])
	}
}

func TestRenderLogsStandaloneAndShardedServerConfig(t *testing.T) {
	logs := &telemetryv1alpha1.Logs{
		ObjectMeta: metav1.ObjectMeta{Name: testLogsProduct, Namespace: testMetricsNamespace},
		Spec: telemetryv1alpha1.LogsSpec{
			Ingress: telemetryv1alpha1.IngressSpec{PathPrefix: "/logs"},
			Config:  telemetryv1alpha1.LogsConfigSpec{Namespaces: []string{testTenantNamespace}},
		},
	}
	result, err := Render(Input{Logs: logs, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(result.Data[LogsKey])
	for _, expected := range []string{
		"mode: standalone", "http: 0.0.0.0:3100", "grpc: 0.0.0.0:9091",
		"type: SlateDb", "path: logs", "path: /var/lib/logs/data", "disk_path: /var/cache/logs",
		"segment_duration_seconds: 3600", testFlushIntervalConfig,
		testCacheWarmerConfig, "enabled: false", testWarmRangeConfig, testWarmTimeoutConfig, testWarmConcurrency, testWarmPayloadsConfig,
		testContinuousWarmer, testContinuousInterval,
		"target_size_bytes: 1048576", "max_request_bytes: 33554432", "max_decoded_request_bytes: 134217728",
		"elasticsearch:\n  message_fields:\n  - message\n  - log\n  - msg\n  stream_fields: []\n  time_field: '@timestamp'",
		"path: /var/run/secrets/logs/internal-token", "name: tenant-a", "path_prefix: /logs",
	} {
		if !strings.Contains(rendered, expected) {
			t.Errorf("rendered Logs config missing %q:\n%s", expected, rendered)
		}
	}
	for _, invalid := range []string{"reader_cache_capacity:", "visibility_interval_seconds:"} {
		if strings.Contains(rendered, invalid) {
			t.Errorf("rendered Logs config contains unsupported field %q:\n%s", invalid, rendered)
		}
	}

	logs.Spec.Mode = telemetryv1alpha1.LogsModeSharded
	sharded, err := Render(Input{Logs: logs, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	writer, reader := string(sharded.Data[LogsKey]), string(sharded.Data[ReaderKey])
	for _, expected := range []string{"mode: writer", "backend: kubernetes", "owner_port: 9091", "stateful_set: logs-writer"} {
		if !strings.Contains(writer, expected) {
			t.Errorf("writer config missing %q:\n%s", expected, writer)
		}
	}
	if !strings.Contains(reader, "mode: reader") || !strings.Contains(reader, "backend: kubernetes") {
		t.Fatalf("unexpected Logs reader config:\n%s", reader)
	}
	if strings.Contains(writer, "block_cache:") {
		t.Fatalf("Logs writer unexpectedly contains the default data cache:\n%s", writer)
	}
	if !strings.Contains(reader, "block_cache:") {
		t.Fatalf("Logs reader is missing the default data cache:\n%s", reader)
	}
}

func TestWriterStoragePreservesExplicitDataCache(t *testing.T) {
	diskCapacity := resources.CacheDiskCapacity(telemetryv1alpha1.WorkloadSpec{})
	defaultWriter := renderStorageConfigForMode(telemetryv1alpha1.StorageSpec{}, resources.MetricsDescriptor, modeWriter, diskCapacity)
	if defaultWriter.BlockCache != nil {
		t.Fatal("writer storage contains the default data cache")
	}
	if defaultWriter.MetaCache == nil {
		t.Fatal("writer storage is missing the default metadata cache")
	}

	spec := telemetryv1alpha1.StorageSpec{
		BlockCache: &telemetryv1alpha1.CacheSpec{Type: telemetryv1alpha1.CacheFoyerHybrid},
	}
	writer := renderStorageConfigForMode(spec, resources.MetricsDescriptor, modeWriter, diskCapacity)
	if writer.BlockCache == nil {
		t.Fatal("writer discarded an explicitly configured data cache")
	}
	standalone := renderStorageConfigForMode(telemetryv1alpha1.StorageSpec{}, resources.MetricsDescriptor, modeStandalone, diskCapacity)
	if standalone.BlockCache == nil {
		t.Fatal("standalone storage is missing the default data cache")
	}
}

func TestRenderDerivesFoyerCapacityFromReaderPVC(t *testing.T) {
	size := resource.MustParse("100Gi")
	metrics := &telemetryv1alpha1.Metrics{Spec: telemetryv1alpha1.MetricsSpec{
		Mode: telemetryv1alpha1.MetricsModeSharded,
		Reader: telemetryv1alpha1.WorkloadSpec{CacheVolume: &telemetryv1alpha1.VolumeSpec{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: size},
				},
			},
		}},
	}}
	result, err := Render(Input{Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	if rendered := string(result.Data[ReaderKey]); !strings.Contains(rendered, "disk_capacity: 96636764160") {
		t.Fatalf("reader config did not use 90%% of its cache PVC:\n%s", rendered)
	}
}

func TestRenderRejectsFoyerCapacityLargerThanVolume(t *testing.T) {
	size := resource.MustParse("20Gi")
	diskCapacity := int64(21 * 1024 * 1024 * 1024)
	metrics := &telemetryv1alpha1.Metrics{Spec: telemetryv1alpha1.MetricsSpec{
		Config: telemetryv1alpha1.MetricsConfigSpec{Storage: telemetryv1alpha1.StorageSpec{
			BlockCache: &telemetryv1alpha1.CacheSpec{
				Type:         telemetryv1alpha1.CacheFoyerHybrid,
				DiskCapacity: &diskCapacity,
			},
		}},
		Writer: telemetryv1alpha1.WorkloadSpec{CacheVolume: &telemetryv1alpha1.VolumeSpec{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: size},
				},
			},
		}},
	}}
	_, err := Render(Input{Metrics: metrics})
	if err == nil || !strings.Contains(err.Error(), "exceeds cache volume capacity") {
		t.Fatalf("Render() error = %v, want cache volume capacity error", err)
	}
}

func TestRenderCacheWarmerOverrides(t *testing.T) {
	enabled, includePayloads := true, true
	warmRangeSeconds := int64(3600)
	timeoutSeconds := int64(15)
	concurrency := int32(4)
	continuousInterval, continuousRange := int64(20), int64(5400)
	continuousPayloads := false
	cacheWarmer := &telemetryv1alpha1.CacheWarmerSpec{
		Enabled:          &enabled,
		WarmRangeSeconds: &warmRangeSeconds,
		TimeoutSeconds:   &timeoutSeconds,
		Concurrency:      &concurrency,
		IncludePayloads:  &includePayloads,
		Continuous: &telemetryv1alpha1.ContinuousCacheWarmerSpec{
			Enabled:          &enabled,
			IntervalSeconds:  &continuousInterval,
			WarmRangeSeconds: &continuousRange,
			IncludePayloads:  &continuousPayloads,
		},
	}
	products := []struct {
		name  string
		input Input
		key   string
	}{
		{
			name: testMetricsProduct,
			input: Input{Metrics: &telemetryv1alpha1.Metrics{
				Spec: telemetryv1alpha1.MetricsSpec{
					Config: telemetryv1alpha1.MetricsConfigSpec{CacheWarmer: cacheWarmer},
				},
			}},
			key: MetricsKey,
		},
		{
			name: testLogsProduct,
			input: Input{Logs: &telemetryv1alpha1.Logs{
				Spec: telemetryv1alpha1.LogsSpec{
					Config: telemetryv1alpha1.LogsConfigSpec{CacheWarmer: cacheWarmer},
				},
			}},
			key: LogsKey,
		},
		{
			name: testTracesProduct,
			input: Input{Traces: &telemetryv1alpha1.Traces{
				Spec: telemetryv1alpha1.TracesSpec{
					Config: telemetryv1alpha1.TracesConfigSpec{CacheWarmer: cacheWarmer},
				},
			}},
			key: TracesKey,
		},
	}
	for _, product := range products {
		t.Run(product.name, func(t *testing.T) {
			result, err := Render(product.input)
			if err != nil {
				t.Fatal(err)
			}
			rendered := string(result.Data[product.key])
			for _, expected := range []string{
				testCacheWarmerConfig,
				"enabled: true",
				"warm_range_seconds: 3600",
				"timeout_seconds: 15",
				"concurrency: 4",
				"include_payloads: true",
				"continuous:",
				"interval_seconds: 20",
				"warm_range_seconds: 5400",
				"include_payloads: false",
			} {
				if !strings.Contains(rendered, expected) {
					t.Errorf("rendered config missing %q:\n%s", expected, rendered)
				}
			}
		})
	}
}

func TestRenderPseudoFSConfig(t *testing.T) {
	chunkSize, maxFileSize := int64(2097152), int64(2147483648)
	maxAppendGenerations, maxUnaryFileSize := int64(32), int64(4194304)
	pseudofs := &telemetryv1alpha1.PseudoFS{
		ObjectMeta: metav1.ObjectMeta{Name: "files", Namespace: testMetricsNamespace},
		Spec: telemetryv1alpha1.PseudoFSSpec{
			Config: telemetryv1alpha1.PseudoFSConfigSpec{
				ChunkSizeBytes: &chunkSize, MaxFileSizeBytes: &maxFileSize,
				MaxAppendGenerations: &maxAppendGenerations, MaxUnaryFileSizeBytes: &maxUnaryFileSize,
			},
		},
	}
	result, err := Render(Input{PseudoFS: pseudofs})
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(result.Data[PseudoFSKey])
	for _, expected := range []string{
		"listener: 0.0.0.0:9093",
		"filesystem:",
		"storage:",
		"type: SlateDb",
		"path: pseudofs",
		"path: /var/lib/pseudofs/data",
		"disk_path: /var/cache/pseudofs",
		"chunk_size_bytes: 2097152",
		"max_file_size_bytes: 2147483648",
		"max_append_generations: 32",
		"max_unary_file_size_bytes: 4194304",
		"max_decoding_message_bytes: 16777216",
		"max_encoding_message_bytes: 16777216",
	} {
		if !strings.Contains(rendered, expected) {
			t.Errorf("rendered PseudoFS config missing %q:\n%s", expected, rendered)
		}
	}
	for _, invalid := range []string{"mode:", "auth:", "namespaces:", "http:"} {
		if strings.Contains(rendered, invalid) {
			t.Errorf("rendered PseudoFS config contains unsupported field %q:\n%s", invalid, rendered)
		}
	}
	if result.Hash == "" {
		t.Fatal("hash was not generated")
	}
}

func TestRenderCloudObjectStoresWithoutCredentials(t *testing.T) {
	tests := []struct {
		name        string
		objectStore telemetryv1alpha1.ObjectStoreSpec
		expected    []string
	}{
		{
			name: "aws",
			objectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreAWS,
				AWS: &telemetryv1alpha1.AWSObjectStoreSpec{
					Region: "us-east-1", Bucket: testBucket, Endpoint: "http://minio:9000",
					AllowHTTP: true, VirtualHostedStyle: true,
				},
			},
			expected: []string{"type: Aws", "region: us-east-1", "bucket: metrics", "endpoint: http://minio:9000", "allow_http: true", "virtual_hosted_style: true"},
		},
		{
			name: "azure",
			objectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreAzure,
				Azure: &telemetryv1alpha1.AzureObjectStoreSpec{
					Account: "telemetry", Container: testBucket, Endpoint: "http://azurite:10000/telemetry", AllowHTTP: true,
				},
			},
			expected: []string{"type: Azure", "account: telemetry", "container: metrics", "endpoint: http://azurite:10000/telemetry", "allow_http: true"},
		},
		{
			name: "gcp",
			objectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreGCP,
				GCP:  &telemetryv1alpha1.GCPObjectStoreSpec{Bucket: testBucket, BaseURL: "http://gcs:4443"},
			},
			expected: []string{"type: Gcp", "bucket: metrics", "base_url: http://gcs:4443"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := &telemetryv1alpha1.Metrics{
				ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testMetricsNamespace},
				Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{
					Storage: telemetryv1alpha1.StorageSpec{ObjectStore: test.objectStore},
				}},
			}
			result, err := Render(Input{Metrics: metrics, InternalToken: []byte("token")})
			if err != nil {
				t.Fatal(err)
			}
			rendered := string(result.Data[MetricsKey])
			for _, expected := range test.expected {
				if !strings.Contains(rendered, expected) {
					t.Errorf("rendered config missing %q:\n%s", expected, rendered)
				}
			}
			if strings.Contains(rendered, "secretRef") {
				t.Fatalf("rendered config exposed a secret reference:\n%s", rendered)
			}
		})
	}
}
