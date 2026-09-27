package config

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

const (
	testMeterName      = "example"
	testMeterNamespace = "test"
	testBucket         = "meter"
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
			Name: "tenant-a", KeyPrefix: "tenant-auth",
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
		"reader_cache_capacity: 268435456", "flush_interval_seconds: 60",
		"virtual_shards: 64", "type: Local", "path: /var/lib/meter/data",
		"path_prefix: /meter",
		"path: /etc/meter/secrets/global-read-0-password",
		"path: /etc/meter/secrets/namespace-tenant-auth-password",
		"path: /var/run/secrets/meter/internal-token",
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

func TestRenderShardedRoles(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testMeterNamespace},
		Spec:       telemetryv1alpha1.MeterSpec{Mode: telemetryv1alpha1.MeterModeSharded},
	}
	result, err := Render(Input{Meter: meter, InternalToken: []byte("token")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.Data[MeterKey]), "mode: writer") ||
		!strings.Contains(string(result.Data[MeterKey]), "backend: kubernetes") ||
		!strings.Contains(string(result.Data[ReaderKey]), "mode: reader") {
		t.Fatalf("unexpected sharded configs:\n%s\n%s", result.Data[MeterKey], result.Data[ReaderKey])
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
