package config

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const (
	testMeterName           = "example"
	testMeterNamespace      = "test"
	testBucket              = "meter"
	testTenantNamespace     = "tenant-a"
	testFlushIntervalConfig = "flush_interval_seconds: 10"
	testCacheWarmerConfig   = "cache_warmer:"
	testWarmRangeConfig     = "warm_range_seconds: 7200"
	testWarmTimeoutConfig   = "timeout_seconds: 30"
	testWarmPayloadsConfig  = "include_payloads: false"
)

func TestRenderDefaultsCredentialsAndHash(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testMeterNamespace},
		Spec: telemetryv1alpha1.MeterSpec{
			Config:  telemetryv1alpha1.MeterConfigSpec{Namespaces: []string{"default", "default"}},
			Ingress: telemetryv1alpha1.IngressSpec{PathPrefix: "/meter"},
		},
	}
	input := Input{
		Meter:  meter,
		Global: Access{Read: []Credential{{Username: "reader", Password: []byte("s3cret")}}},
		Namespaces: []NamespaceAccess{{
			Name: testTenantNamespace, KeyPrefix: "tenant-auth",
			Access: Access{Write: []Credential{{Username: "writer", Password: []byte("tenant-secret"), DataKey: "namespace-tenant-auth-password"}}},
		}},
		InternalTokenPath: "/var/run/secrets/meter/internal-token",
		InternalToken:     []byte("token"),
	}
	result, err := Render(input)
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(result.Data[MeterKey])
	for _, expected := range []string{
		"mode: standalone", "http: 0.0.0.0:8080", "grpc: 0.0.0.0:9090",
		"reader_cache_capacity: 268435456", testFlushIntervalConfig,
		testCacheWarmerConfig, testWarmRangeConfig, testWarmTimeoutConfig, testWarmPayloadsConfig,
		"virtual_shards: 1", "io_concurrency_limit: 128",
		"type: Local", "path: /var/lib/meter/data",
		"path_prefix: /meter",
		"path: /etc/meter/secrets/global-read-0-password",
		"path: /etc/meter/secrets/namespace-tenant-auth-password",
		"path: /var/run/secrets/meter/internal-token",
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
			name: "meter",
			input: Input{Meter: &telemetryv1alpha1.Meter{
				Spec: telemetryv1alpha1.MeterSpec{Config: telemetryv1alpha1.MeterConfigSpec{
					Auth: telemetryv1alpha1.AuthSpec{Unauthenticated: true},
				}},
			}},
			key: MeterKey,
		},
		{
			name: "line",
			input: Input{Line: &telemetryv1alpha1.Line{
				Spec: telemetryv1alpha1.LineSpec{Config: telemetryv1alpha1.LineConfigSpec{
					Auth: telemetryv1alpha1.AuthSpec{Unauthenticated: true},
				}},
			}},
			key: LineKey,
		},
		{
			name: "track",
			input: Input{Track: &telemetryv1alpha1.Track{
				Spec: telemetryv1alpha1.TrackSpec{Config: telemetryv1alpha1.TrackConfigSpec{
					Auth: telemetryv1alpha1.AuthSpec{Unauthenticated: true},
				}},
			}},
			key: TrackKey,
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

func TestRenderTrackShardedConfig(t *testing.T) {
	track := &telemetryv1alpha1.Track{
		ObjectMeta: metav1.ObjectMeta{Name: "traces", Namespace: "observability"},
		Spec: telemetryv1alpha1.TrackSpec{
			Mode:    telemetryv1alpha1.TrackModeSharded,
			Ingress: telemetryv1alpha1.IngressSpec{PathPrefix: "/traces"},
		},
	}
	result, err := Render(Input{
		Track: track, InternalToken: []byte("token"),
		Namespaces: []NamespaceAccess{{
			Name: testTenantNamespace,
			Access: Access{Read: []Credential{{
				Username: "tempo", Password: []byte("tenant-secret"), DataKey: "namespace-track-auth-password",
			}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := string(result.Data[TrackKey])
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
		testWarmPayloadsConfig,
		"name: tenant-a",
		"path: /etc/track/secrets/namespace-track-auth-password",
	} {
		if !strings.Contains(writer, expected) {
			t.Errorf("Track writer config missing %q:\n%s", expected, writer)
		}
	}
	if !strings.Contains(reader, "mode: reader") {
		t.Fatalf("Track reader config missing reader mode:\n%s", reader)
	}
	if strings.Contains(writer, "block_cache:") {
		t.Fatalf("Track writer unexpectedly contains the default data cache:\n%s", writer)
	}
	if !strings.Contains(reader, "block_cache:") {
		t.Fatalf("Track reader is missing the default data cache:\n%s", reader)
	}
	if got := string(result.Data["namespace-track-auth-password"]); got != "tenant-secret" {
		t.Fatalf("resolved Track namespace password = %q", got)
	}
}

func TestRenderShardedRoles(t *testing.T) {
	ioConcurrencyLimit := int32(96)
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testMeterNamespace},
		Spec: telemetryv1alpha1.MeterSpec{
			Mode: telemetryv1alpha1.MeterModeSharded,
			Config: telemetryv1alpha1.MeterConfigSpec{
				Sharding: telemetryv1alpha1.ShardingSpec{
					IOConcurrencyLimit: &ioConcurrencyLimit,
				},
			},
		},
	}
	result, err := Render(Input{Meter: meter, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.Data[MeterKey]), "mode: writer") ||
		!strings.Contains(string(result.Data[MeterKey]), "backend: kubernetes") ||
		!strings.Contains(string(result.Data[MeterKey]), "shard_map: example-writer-shard-map") ||
		!strings.Contains(string(result.Data[MeterKey]), "io_concurrency_limit: 96") ||
		strings.Contains(string(result.Data[MeterKey]), "virtual_shards:") ||
		!strings.Contains(string(result.Data[ReaderKey]), "mode: reader") {
		t.Fatalf("unexpected sharded configs:\n%s\n%s", result.Data[MeterKey], result.Data[ReaderKey])
	}
	if strings.Contains(string(result.Data[MeterKey]), "block_cache:") {
		t.Fatalf("Meter writer unexpectedly contains the default data cache:\n%s", result.Data[MeterKey])
	}
	if !strings.Contains(string(result.Data[ReaderKey]), "block_cache:") {
		t.Fatalf("Meter reader is missing the default data cache:\n%s", result.Data[ReaderKey])
	}
}

func TestRenderLineStandaloneAndShardedServerConfig(t *testing.T) {
	line := &telemetryv1alpha1.Line{
		ObjectMeta: metav1.ObjectMeta{Name: "logs", Namespace: testMeterNamespace},
		Spec: telemetryv1alpha1.LineSpec{
			Ingress: telemetryv1alpha1.IngressSpec{PathPrefix: "/logs"},
			Config:  telemetryv1alpha1.LineConfigSpec{Namespaces: []string{testTenantNamespace}},
		},
	}
	result, err := Render(Input{Line: line, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(result.Data[LineKey])
	for _, expected := range []string{
		"mode: standalone", "http: 0.0.0.0:3100", "grpc: 0.0.0.0:9091",
		"type: SlateDb", "path: line", "path: /var/lib/line/data", "disk_path: /var/cache/line",
		"segment_duration_seconds: 3600", testFlushIntervalConfig,
		testCacheWarmerConfig, testWarmRangeConfig, testWarmTimeoutConfig, testWarmPayloadsConfig,
		"target_size_bytes: 1048576", "max_request_bytes: 10485760",
		"path: /var/run/secrets/line/internal-token", "name: tenant-a", "path_prefix: /logs",
	} {
		if !strings.Contains(rendered, expected) {
			t.Errorf("rendered Line config missing %q:\n%s", expected, rendered)
		}
	}
	for _, invalid := range []string{"reader_cache_capacity:", "visibility_interval_seconds:"} {
		if strings.Contains(rendered, invalid) {
			t.Errorf("rendered Line config contains unsupported field %q:\n%s", invalid, rendered)
		}
	}

	line.Spec.Mode = telemetryv1alpha1.LineModeSharded
	sharded, err := Render(Input{Line: line, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	writer, reader := string(sharded.Data[LineKey]), string(sharded.Data[ReaderKey])
	for _, expected := range []string{"mode: writer", "backend: kubernetes", "owner_port: 9091", "stateful_set: logs-writer"} {
		if !strings.Contains(writer, expected) {
			t.Errorf("writer config missing %q:\n%s", expected, writer)
		}
	}
	if !strings.Contains(reader, "mode: reader") || !strings.Contains(reader, "backend: kubernetes") {
		t.Fatalf("unexpected Line reader config:\n%s", reader)
	}
	if strings.Contains(writer, "block_cache:") {
		t.Fatalf("Line writer unexpectedly contains the default data cache:\n%s", writer)
	}
	if !strings.Contains(reader, "block_cache:") {
		t.Fatalf("Line reader is missing the default data cache:\n%s", reader)
	}
}

func TestWriterStoragePreservesExplicitDataCache(t *testing.T) {
	defaultWriter := renderStorageConfigForMode(telemetryv1alpha1.StorageSpec{}, resources.MeterDescriptor, modeWriter)
	if defaultWriter.BlockCache != nil {
		t.Fatal("writer storage contains the default data cache")
	}
	if defaultWriter.MetaCache == nil {
		t.Fatal("writer storage is missing the default metadata cache")
	}

	spec := telemetryv1alpha1.StorageSpec{
		BlockCache: &telemetryv1alpha1.CacheSpec{Type: telemetryv1alpha1.CacheFoyerHybrid},
	}
	writer := renderStorageConfigForMode(spec, resources.MeterDescriptor, modeWriter)
	if writer.BlockCache == nil {
		t.Fatal("writer discarded an explicitly configured data cache")
	}
	standalone := renderStorageConfigForMode(telemetryv1alpha1.StorageSpec{}, resources.MeterDescriptor, modeStandalone)
	if standalone.BlockCache == nil {
		t.Fatal("standalone storage is missing the default data cache")
	}
}

func TestRenderCacheWarmerOverrides(t *testing.T) {
	enabled, includePayloads := false, true
	warmRangeSeconds := int64(3600)
	timeoutSeconds := int64(15)
	cacheWarmer := &telemetryv1alpha1.CacheWarmerSpec{
		Enabled:          &enabled,
		WarmRangeSeconds: &warmRangeSeconds,
		TimeoutSeconds:   &timeoutSeconds,
		IncludePayloads:  &includePayloads,
	}
	products := []struct {
		name  string
		input Input
		key   string
	}{
		{
			name: "meter",
			input: Input{Meter: &telemetryv1alpha1.Meter{
				Spec: telemetryv1alpha1.MeterSpec{
					Config: telemetryv1alpha1.MeterConfigSpec{CacheWarmer: cacheWarmer},
				},
			}},
			key: MeterKey,
		},
		{
			name: "line",
			input: Input{Line: &telemetryv1alpha1.Line{
				Spec: telemetryv1alpha1.LineSpec{
					Config: telemetryv1alpha1.LineConfigSpec{CacheWarmer: cacheWarmer},
				},
			}},
			key: LineKey,
		},
		{
			name: "track",
			input: Input{Track: &telemetryv1alpha1.Track{
				Spec: telemetryv1alpha1.TrackSpec{
					Config: telemetryv1alpha1.TrackConfigSpec{CacheWarmer: cacheWarmer},
				},
			}},
			key: TrackKey,
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
				"enabled: false",
				"warm_range_seconds: 3600",
				"timeout_seconds: 15",
				"include_payloads: true",
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
		ObjectMeta: metav1.ObjectMeta{Name: "files", Namespace: testMeterNamespace},
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
			expected: []string{"type: Aws", "region: us-east-1", "bucket: meter", "endpoint: http://minio:9000", "allow_http: true", "virtual_hosted_style: true"},
		},
		{
			name: "azure",
			objectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreAzure,
				Azure: &telemetryv1alpha1.AzureObjectStoreSpec{
					Account: "telemetry", Container: testBucket, Endpoint: "http://azurite:10000/telemetry", AllowHTTP: true,
				},
			},
			expected: []string{"type: Azure", "account: telemetry", "container: meter", "endpoint: http://azurite:10000/telemetry", "allow_http: true"},
		},
		{
			name: "gcp",
			objectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreGCP,
				GCP:  &telemetryv1alpha1.GCPObjectStoreSpec{Bucket: testBucket, BaseURL: "http://gcs:4443"},
			},
			expected: []string{"type: Gcp", "bucket: meter", "base_url: http://gcs:4443"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			meter := &telemetryv1alpha1.Meter{
				ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testMeterNamespace},
				Spec: telemetryv1alpha1.MeterSpec{Config: telemetryv1alpha1.MeterConfigSpec{
					Storage: telemetryv1alpha1.StorageSpec{ObjectStore: test.objectStore},
				}},
			}
			result, err := Render(Input{Meter: meter, InternalToken: []byte("token")})
			if err != nil {
				t.Fatal(err)
			}
			rendered := string(result.Data[MeterKey])
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
