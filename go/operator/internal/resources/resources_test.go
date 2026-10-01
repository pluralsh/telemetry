package resources

import (
	"testing"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

const (
	testMetricsName     = "example"
	testLogsName        = "logs"
	testNamespace       = "test"
	testObjectStoreName = "metrics"
	testTokenSecretName = "token"
	testDedicatedKey    = "dedicated"
	testConfigHash      = "hash"
)

func TestStatefulSetUsesPersistentDefaults(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace}}
	statefulSet := mustStatefulSet(t, metrics)
	if image := statefulSet.Spec.Template.Spec.Containers[0].Image; image != "ghcr.io/pluralsh/metrics:0.1.0" {
		t.Fatalf("default image = %q, want GHCR Metrics image", image)
	}
	if policy := statefulSet.Spec.Template.Spec.Containers[0].ImagePullPolicy; policy != corev1.PullIfNotPresent {
		t.Fatalf("default image pull policy = %q, want IfNotPresent", policy)
	}
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 {
		t.Fatalf("default standalone replicas = %#v, want 1", statefulSet.Spec.Replicas)
	}
	if len(statefulSet.Spec.VolumeClaimTemplates) != 2 {
		t.Fatalf("default workload has %d claims, want 2", len(statefulSet.Spec.VolumeClaimTemplates))
	}
	assertClaim(t, statefulSet.Spec.VolumeClaimTemplates, "data", "10Gi")
	assertClaim(t, statefulSet.Spec.VolumeClaimTemplates, "cache", "20Gi")
	for _, claim := range statefulSet.Spec.VolumeClaimTemplates {
		if claim.Spec.StorageClassName != nil {
			t.Fatalf("%s storageClassName = %q, want nil", claim.Name, *claim.Spec.StorageClassName)
		}
		if !lo.Contains(claim.Spec.AccessModes, corev1.ReadWriteOnce) {
			t.Fatalf("%s access modes = %v", claim.Name, claim.Spec.AccessModes)
		}
	}
	if hasVolume(statefulSet.Spec.Template.Spec.Volumes, "data") || hasVolume(statefulSet.Spec.Template.Spec.Volumes, "cache") {
		t.Fatal("pod volumes shadow default claim templates")
	}
	resources := statefulSet.Spec.Template.Spec.Containers[0].Resources
	assertResourceQuantity(t, resources.Requests, corev1.ResourceCPU, "250m")
	assertResourceQuantity(t, resources.Requests, corev1.ResourceMemory, "512Mi")
	assertResourceQuantity(t, resources.Limits, corev1.ResourceMemory, "2Gi")
	if _, exists := resources.Limits[corev1.ResourceCPU]; exists {
		t.Fatal("default resources unexpectedly include a CPU limit")
	}
}

func TestStatefulSetPersistentVolumeClaimRetentionPolicy(t *testing.T) {
	t.Run("local object stores retain authoritative data", func(t *testing.T) {
		statefulSet := mustStatefulSet(t, &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		})
		assertPVCRetentionPolicy(t, statefulSet, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, appsv1.RetainPersistentVolumeClaimRetentionPolicyType)
	})

	t.Run("remote object stores delete cache volumes", func(t *testing.T) {
		statefulSet := mustStatefulSet(t, &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
			Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{
				Storage: telemetryv1alpha1.StorageSpec{ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
					Type: telemetryv1alpha1.ObjectStoreAWS,
				}},
			}},
		})
		assertPVCRetentionPolicy(t, statefulSet, appsv1.DeletePersistentVolumeClaimRetentionPolicyType, appsv1.DeletePersistentVolumeClaimRetentionPolicyType)
	})

	t.Run("explicit values override each computed default independently", func(t *testing.T) {
		statefulSet := mustStatefulSet(t, &telemetryv1alpha1.Metrics{
			ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
			Spec: telemetryv1alpha1.MetricsSpec{
				Config: telemetryv1alpha1.MetricsConfigSpec{
					Storage: telemetryv1alpha1.StorageSpec{ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
						Type: telemetryv1alpha1.ObjectStoreGCP,
					}},
				},
				Writer: telemetryv1alpha1.WorkloadSpec{
					PersistentVolumeClaimRetentionPolicy: &telemetryv1alpha1.PersistentVolumeClaimRetentionPolicySpec{
						WhenDeleted: telemetryv1alpha1.PersistentVolumeClaimRetentionPolicyRetain,
					},
				},
			},
		})
		assertPVCRetentionPolicy(t, statefulSet, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, appsv1.DeletePersistentVolumeClaimRetentionPolicyType)
	})
}

func TestStatefulSetMergesConfiguredContainerResources(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Mode: telemetryv1alpha1.MetricsModeSharded,
			Writer: telemetryv1alpha1.WorkloadSpec{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				},
				PodTemplate: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: containerMetrics,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("500m"),
							corev1.ResourceMemory:           resource.MustParse("768Mi"),
							corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
						},
						Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("3Gi")},
					},
				}}}},
			},
			Reader: telemetryv1alpha1.WorkloadSpec{Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("750m")},
			}},
		},
	}

	writer, err := StatefulSet(StatefulSetInput{
		Metrics: metrics, Component: ComponentWriter, ConfigSecretName: volumeConfig,
		InternalTokenSecretName: testTokenSecretName, InternalTokenSecretKey: TokenKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	writerResources := writer.Spec.Template.Spec.Containers[0].Resources
	assertResourceQuantity(t, writerResources.Requests, corev1.ResourceCPU, "500m")
	assertResourceQuantity(t, writerResources.Requests, corev1.ResourceMemory, "1Gi")
	assertResourceQuantity(t, writerResources.Requests, corev1.ResourceEphemeralStorage, "1Gi")
	assertResourceQuantity(t, writerResources.Limits, corev1.ResourceCPU, "2")
	assertResourceQuantity(t, writerResources.Limits, corev1.ResourceMemory, "3Gi")

	reader, err := StatefulSet(StatefulSetInput{
		Metrics: metrics, Component: ComponentReader, ConfigSecretName: volumeConfig,
		InternalTokenSecretName: testTokenSecretName, InternalTokenSecretKey: TokenKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	readerResources := reader.Spec.Template.Spec.Containers[0].Resources
	assertResourceQuantity(t, readerResources.Requests, corev1.ResourceCPU, "750m")
	assertResourceQuantity(t, readerResources.Requests, corev1.ResourceMemory, "512Mi")
	assertResourceQuantity(t, readerResources.Limits, corev1.ResourceMemory, "2Gi")
	if _, exists := readerResources.Limits[corev1.ResourceCPU]; exists {
		t.Fatal("reader resources unexpectedly include a CPU limit")
	}
}

func TestStatefulSetProductVersionPrecedence(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace}}
	input := StatefulSetInput{
		Metrics: metrics, Component: ComponentStandalone, ConfigSecretName: volumeConfig,
		InternalTokenSecretName: testTokenSecretName, InternalTokenSecretKey: TokenKey,
		DefaultProductVersion: "1.2.3",
	}
	statefulSet, err := StatefulSet(input)
	if err != nil {
		t.Fatal(err)
	}
	if image := statefulSet.Spec.Template.Spec.Containers[0].Image; image != "ghcr.io/pluralsh/metrics:1.2.3" {
		t.Fatalf("configured default image = %q, want release version", image)
	}

	metrics.Spec.Image.Tag = "2.0.0"
	statefulSet, err = StatefulSet(input)
	if err != nil {
		t.Fatal(err)
	}
	if image := statefulSet.Spec.Template.Spec.Containers[0].Image; image != "ghcr.io/pluralsh/metrics:2.0.0" {
		t.Fatalf("deprecated image tag image = %q, want explicit tag", image)
	}

	metrics.Spec.Image.Tag = ""
	metrics.Spec.Version = "3.0.0"
	statefulSet, err = StatefulSet(input)
	if err != nil {
		t.Fatal(err)
	}
	if image := statefulSet.Spec.Template.Spec.Containers[0].Image; image != "ghcr.io/pluralsh/metrics:3.0.0" {
		t.Fatalf("spec.version image = %q, want canonical explicit version", image)
	}
}

func TestServiceAccountIncludesConfiguredAnnotations(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{ServiceAccount: telemetryv1alpha1.ServiceAccountSpec{
			Annotations: map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/metrics"},
		}},
	}
	serviceAccount := ServiceAccount(metrics)
	if serviceAccount.Annotations["eks.amazonaws.com/role-arn"] != "arn:aws:iam::123456789012:role/metrics" {
		t.Fatalf("service account annotations = %#v", serviceAccount.Annotations)
	}
}

func TestIngressUsesStandaloneServiceAndTLSDefaults(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{Ingress: telemetryv1alpha1.IngressSpec{
			Enabled: true, Hostname: "metrics.example.com", IngressClass: "nginx", PathPrefix: "/metrics",
			Metadata: telemetryv1alpha1.IngressMetadataSpec{
				Annotations: map[string]string{"cert-manager.io/cluster-issuer": "letsencrypt"},
				Labels:      map[string]string{"example.com/exposure": "external"},
			},
			TLS: telemetryv1alpha1.IngressTLSSpec{Enabled: true},
		}},
	}
	ingress := Ingress(metrics)
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != "nginx" {
		t.Fatalf("ingress class = %#v", ingress.Spec.IngressClassName)
	}
	if ingress.Annotations["cert-manager.io/cluster-issuer"] != "letsencrypt" ||
		ingress.Labels["example.com/exposure"] != "external" {
		t.Fatalf("ingress metadata = %#v/%#v", ingress.Labels, ingress.Annotations)
	}
	if len(ingress.Spec.TLS) != 1 || ingress.Spec.TLS[0].SecretName != testMetricsName+"-tls" {
		t.Fatalf("ingress TLS = %#v", ingress.Spec.TLS)
	}
	paths := ingress.Spec.Rules[0].HTTP.Paths
	if len(paths) != 2 {
		t.Fatalf("standalone ingress paths = %#v", paths)
	}
	assertIngressPath(t, paths, "/metrics/write", testMetricsName)
	assertIngressPath(t, paths, "/metrics/read", testMetricsName)
}

func TestIngressRoutesShardedWritesAndReads(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{
			Mode:    telemetryv1alpha1.MetricsModeSharded,
			Ingress: telemetryv1alpha1.IngressSpec{Enabled: true, Hostname: "metrics.example.com"},
		},
	}
	paths := Ingress(metrics).Spec.Rules[0].HTTP.Paths
	if len(paths) != 2 {
		t.Fatalf("sharded ingress paths = %#v", paths)
	}
	assertIngressPath(t, paths, "/write", testMetricsName+"-writer")
	assertIngressPath(t, paths, "/read", testMetricsName+"-reader")
}

func TestLogsResourcesUseProductDefaultsAndNamespaceRoutes(t *testing.T) {
	logs := &telemetryv1alpha1.Logs{
		ObjectMeta: metav1.ObjectMeta{Name: testLogsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.LogsSpec{Ingress: telemetryv1alpha1.IngressSpec{
			Enabled: true, Hostname: "logs.example.com", PathPrefix: "/logs",
		}},
	}
	statefulSet, err := StatefulSet(StatefulSetInput{
		Logs: logs, Component: ComponentStandalone, ConfigSecretName: volumeConfig,
		InternalTokenSecretName: testTokenSecretName, InternalTokenSecretKey: TokenKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	container := statefulSet.Spec.Template.Spec.Containers[0]
	if container.Name != LogsDescriptor.Name || container.Image != "ghcr.io/pluralsh/logs:0.1.0" {
		t.Fatalf("unexpected Logs container: %#v", container)
	}
	if container.Ports[0].ContainerPort != 3100 || container.Ports[1].ContainerPort != 9091 {
		t.Fatalf("unexpected Logs ports: %#v", container.Ports)
	}
	if !lo.ContainsBy(container.VolumeMounts, func(mount corev1.VolumeMount) bool {
		return mount.Name == volumeConfig && mount.MountPath == "/etc/logs/logs.yaml" && mount.SubPath == "logs.yaml"
	}) {
		t.Fatalf("Logs config mount is missing: %#v", container.VolumeMounts)
	}
	paths := Ingress(logs).Spec.Rules[0].HTTP.Paths
	assertIngressPath(t, paths, "/logs/write", logs.Name)
	assertIngressPath(t, paths, "/logs/read", logs.Name)

	logs.Spec.Mode = telemetryv1alpha1.LogsModeSharded
	paths = Ingress(logs).Spec.Rules[0].HTTP.Paths
	assertIngressPath(t, paths, "/logs/write", logs.Name+"-writer")
	assertIngressPath(t, paths, "/logs/read", logs.Name+"-reader")
}

func TestTracesIngressUsesPathPrefix(t *testing.T) {
	traces := &telemetryv1alpha1.Traces{
		ObjectMeta: metav1.ObjectMeta{Name: "traces", Namespace: testNamespace},
		Spec: telemetryv1alpha1.TracesSpec{Ingress: telemetryv1alpha1.IngressSpec{
			Enabled: true, Hostname: "traces.example.com", PathPrefix: "/traces",
		}},
	}
	paths := Ingress(traces).Spec.Rules[0].HTTP.Paths
	assertIngressPath(t, paths, "/traces/write", traces.Name)
	assertIngressPath(t, paths, "/traces/read", traces.Name)
}

func TestPseudoFSResourcesAreGRPCOnlyAndPersistent(t *testing.T) {
	pseudofs := &telemetryv1alpha1.PseudoFS{
		ObjectMeta: metav1.ObjectMeta{Name: "files", Namespace: testNamespace},
		Status:     telemetryv1alpha1.PseudoFSStatus{ConfigHash: testConfigHash},
	}
	service := Service(pseudofs, ComponentStandalone, false)
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Name != portGRPC || service.Spec.Ports[0].Port != 9093 {
		t.Fatalf("unexpected PseudoFS service ports: %#v", service.Spec.Ports)
	}
	statefulSet, err := StatefulSet(StatefulSetInput{
		PseudoFS: pseudofs, Component: ComponentStandalone, ConfigSecretName: volumeConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 {
		t.Fatalf("PseudoFS replicas = %#v, want 1", statefulSet.Spec.Replicas)
	}
	if statefulSet.Spec.ServiceName != pseudofs.Name {
		t.Fatalf("PseudoFS governing service = %q, want %q", statefulSet.Spec.ServiceName, pseudofs.Name)
	}
	assertClaim(t, statefulSet.Spec.VolumeClaimTemplates, "data", "10Gi")
	assertClaim(t, statefulSet.Spec.VolumeClaimTemplates, "cache", "20Gi")
	container := statefulSet.Spec.Template.Spec.Containers[0]
	if container.Name != "pseudofs" || container.Image != "ghcr.io/pluralsh/pseudofs:0.1.0" {
		t.Fatalf("unexpected PseudoFS container: %#v", container)
	}
	if len(container.Ports) != 1 || container.Ports[0].Name != portGRPC || container.Ports[0].ContainerPort != 9093 {
		t.Fatalf("unexpected PseudoFS container ports: %#v", container.Ports)
	}
	if container.LivenessProbe == nil || container.LivenessProbe.GRPC == nil ||
		container.ReadinessProbe == nil || container.ReadinessProbe.GRPC == nil {
		t.Fatal("PseudoFS does not use gRPC health probes")
	}
	assertResourceQuantity(t, container.Resources.Requests, corev1.ResourceCPU, "250m")
	assertResourceQuantity(t, container.Resources.Requests, corev1.ResourceMemory, "512Mi")
	assertResourceQuantity(t, container.Resources.Limits, corev1.ResourceMemory, "2Gi")
	for _, mount := range container.VolumeMounts {
		if mount.Name == volumeSecrets || mount.Name == volumeInternalToken {
			t.Fatalf("PseudoFS contains unsupported secret mount: %#v", mount)
		}
	}
}

func TestStatefulSetsUseCanonicalImageSettings(t *testing.T) {
	tests := []struct {
		name      string
		input     StatefulSetInput
		container string
	}{
		{
			name: "Metrics",
			input: StatefulSetInput{Metrics: &telemetryv1alpha1.Metrics{
				ObjectMeta: metav1.ObjectMeta{Name: "metrics", Namespace: testNamespace},
				Spec: telemetryv1alpha1.MetricsSpec{
					Version: "1.2.3-rc.1",
					Image: telemetryv1alpha1.ImageSpec{
						Repository: "registry.example.com/observability/metrics",
						Tag:        "0.9.0",
						PullPolicy: corev1.PullAlways,
					},
					Writer: telemetryv1alpha1.WorkloadSpec{PodTemplate: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "metrics", Image: "ignored:latest", ImagePullPolicy: corev1.PullNever}},
					}}},
				},
			}},
			container: "metrics",
		},
		{
			name: "Logs",
			input: StatefulSetInput{Logs: &telemetryv1alpha1.Logs{
				ObjectMeta: metav1.ObjectMeta{Name: testLogsName, Namespace: testNamespace},
				Spec: telemetryv1alpha1.LogsSpec{
					Version: "2.3.4+build.5",
					Image: telemetryv1alpha1.ImageSpec{
						Repository: "registry.example.com/observability/logs",
						Tag:        "0.8.0",
						PullPolicy: corev1.PullAlways,
					},
					Writer: telemetryv1alpha1.WorkloadSpec{PodTemplate: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: LogsDescriptor.Name, Image: "ignored:latest", ImagePullPolicy: corev1.PullNever}},
					}}},
				},
			}},
			container: LogsDescriptor.Name,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.input.Component = ComponentStandalone
			test.input.ConfigSecretName = volumeConfig
			test.input.InternalTokenSecretName = testTokenSecretName
			test.input.InternalTokenSecretKey = TokenKey
			statefulSet, err := StatefulSet(test.input)
			if err != nil {
				t.Fatal(err)
			}
			container := statefulSet.Spec.Template.Spec.Containers[0]
			product := product(lo.Ternary(test.input.Metrics != nil, any(test.input.Metrics), any(test.input.Logs)))
			expected := product.Image.Repository + ":" + product.Version
			if container.Name != test.container || container.Image != expected || container.ImagePullPolicy != corev1.PullAlways {
				t.Fatalf("container image settings = %#v, want %s with Always", container, expected)
			}
		})
	}
}

func TestStatefulSetUsesDeprecatedImageTagAlias(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{Image: telemetryv1alpha1.ImageSpec{
			Repository: "registry.example.com/observability/metrics",
			Tag:        "3.2.1",
		}},
	}
	container := mustStatefulSet(t, metrics).Spec.Template.Spec.Containers[0]
	if container.Image != "registry.example.com/observability/metrics:3.2.1" {
		t.Fatalf("container image = %q, want deprecated tag alias", container.Image)
	}
}

func TestReplicaDefaultsOverridesAndStandaloneSafety(t *testing.T) {
	for _, value := range []any{
		&telemetryv1alpha1.Metrics{Spec: telemetryv1alpha1.MetricsSpec{Mode: telemetryv1alpha1.MetricsModeSharded}},
		&telemetryv1alpha1.Logs{Spec: telemetryv1alpha1.LogsSpec{Mode: telemetryv1alpha1.LogsModeSharded}},
	} {
		if got := *Replicas(value, ComponentWriter); got != 1 {
			t.Fatalf("%T default writer replicas = %d, want 1", value, got)
		}
		if got := *Replicas(value, ComponentReader); got != 2 {
			t.Fatalf("%T default reader replicas = %d, want 2", value, got)
		}
	}

	metrics := &telemetryv1alpha1.Metrics{Spec: telemetryv1alpha1.MetricsSpec{
		Mode:   telemetryv1alpha1.MetricsModeSharded,
		Writer: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(4))},
		Reader: telemetryv1alpha1.WorkloadSpec{Replicas: lo.ToPtr(int32(5))},
	}}
	if got := *Replicas(metrics, ComponentWriter); got != 4 {
		t.Fatalf("writer replica override = %d, want 4", got)
	}
	if got := *Replicas(metrics, ComponentReader); got != 5 {
		t.Fatalf("reader replica override = %d, want 5", got)
	}
	metrics.Spec.Mode = telemetryv1alpha1.MetricsModeStandalone
	metrics.Spec.Writer.Replicas = lo.ToPtr(int32(9))
	if got := *Replicas(metrics, ComponentStandalone); got != 1 {
		t.Fatalf("standalone replicas = %d, want safe fixed value 1", got)
	}
}

func TestStatefulSetMergesFirstClassScheduling(t *testing.T) {
	templateToleration := corev1.Toleration{Key: testDedicatedKey, Operator: corev1.TolerationOpEqual, Value: "old", Effect: corev1.TaintEffectNoSchedule}
	duplicateTemplateToleration := corev1.Toleration{Key: testDedicatedKey, Value: "newer-template", Effect: corev1.TaintEffectNoSchedule}
	firstClassToleration := corev1.Toleration{Key: testDedicatedKey, Value: "telemetry", Effect: corev1.TaintEffectNoSchedule}
	retainedToleration := corev1.Toleration{Key: "spot", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
	workload := telemetryv1alpha1.WorkloadSpec{
		NodeSelector: map[string]string{"topology.kubernetes.io/zone": "first-class", "kubernetes.io/arch": "arm64"},
		Tolerations:  []corev1.Toleration{firstClassToleration},
		PodTemplate: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"topology.kubernetes.io/zone": "template", "node.example.com/pool": "observability"},
			Tolerations:  []corev1.Toleration{templateToleration, retainedToleration, duplicateTemplateToleration},
		}},
	}
	inputs := []StatefulSetInput{
		{Metrics: &telemetryv1alpha1.Metrics{ObjectMeta: metav1.ObjectMeta{Name: "metrics"}, Spec: telemetryv1alpha1.MetricsSpec{Writer: workload}}},
		{Logs: &telemetryv1alpha1.Logs{ObjectMeta: metav1.ObjectMeta{Name: testLogsName}, Spec: telemetryv1alpha1.LogsSpec{Writer: workload}}},
	}
	for _, input := range inputs {
		input.Component = ComponentStandalone
		input.ConfigSecretName = volumeConfig
		input.InternalTokenSecretName = testTokenSecretName
		input.InternalTokenSecretKey = TokenKey
		statefulSet, err := StatefulSet(input)
		if err != nil {
			t.Fatal(err)
		}
		spec := statefulSet.Spec.Template.Spec
		if spec.NodeSelector["topology.kubernetes.io/zone"] != "first-class" ||
			spec.NodeSelector["node.example.com/pool"] != "observability" ||
			spec.NodeSelector["kubernetes.io/arch"] != "arm64" {
			t.Fatalf("merged node selector = %#v", spec.NodeSelector)
		}
		if len(spec.Tolerations) != 2 || !lo.Contains(spec.Tolerations, firstClassToleration) || !lo.Contains(spec.Tolerations, retainedToleration) {
			t.Fatalf("merged tolerations = %#v, want replacement without duplicates", spec.Tolerations)
		}
	}
}

func TestStatefulSetSupportsExplicitEmptyDir(t *testing.T) {
	dataSize, cacheSize := resource.MustParse("1Gi"), resource.MustParse("2Gi")
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{Writer: telemetryv1alpha1.WorkloadSpec{
			DataVolume:  &telemetryv1alpha1.VolumeSpec{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &dataSize}},
			CacheVolume: &telemetryv1alpha1.VolumeSpec{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &cacheSize}},
		}},
	}
	statefulSet := mustStatefulSet(t, metrics)
	if len(statefulSet.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("emptyDir workload unexpectedly has claims: %#v", statefulSet.Spec.VolumeClaimTemplates)
	}
	assertEmptyDir(t, statefulSet.Spec.Template.Spec.Volumes, "data", dataSize)
	assertEmptyDir(t, statefulSet.Spec.Template.Spec.Volumes, "cache", cacheSize)
}

func TestStatefulSetMergesPodSecurityDefaults(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Status:     telemetryv1alpha1.MetricsStatus{ConfigHash: testConfigHash},
		Spec: telemetryv1alpha1.MetricsSpec{Writer: telemetryv1alpha1.WorkloadSpec{PodTemplate: &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers:     []corev1.Container{{Name: containerMetrics, Env: []corev1.EnvVar{{Name: "CUSTOM", Value: "yes"}}}, {Name: "sidecar"}},
				InitContainers: []corev1.Container{{Name: "init"}},
			},
		}}},
	}
	statefulSet := mustStatefulSet(t, metrics)
	template := statefulSet.Spec.Template
	container := template.Spec.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("metrics container did not receive secure defaults")
	}
	if template.Spec.SecurityContext == nil || template.Spec.SecurityContext.RunAsUser == nil || *template.Spec.SecurityContext.RunAsUser != 10001 {
		t.Fatal("pod did not receive UID 10001")
	}
	if template.Spec.SecurityContext.SeccompProfile == nil || template.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("pod did not receive the RuntimeDefault seccomp profile")
	}
	if template.Spec.AutomountServiceAccountToken == nil || *template.Spec.AutomountServiceAccountToken {
		t.Fatal("standalone pod should not automount its service account token")
	}
	for _, secured := range append(template.Spec.Containers, template.Spec.InitContainers...) {
		context := secured.SecurityContext
		if context == nil ||
			context.AllowPrivilegeEscalation == nil || *context.AllowPrivilegeEscalation ||
			context.ReadOnlyRootFilesystem == nil || !*context.ReadOnlyRootFilesystem ||
			context.RunAsNonRoot == nil || !*context.RunAsNonRoot ||
			context.RunAsUser == nil || *context.RunAsUser != 10001 ||
			context.Capabilities == nil || !lo.Contains(context.Capabilities.Drop, corev1.Capability("ALL")) {
			t.Fatalf("container %q did not receive secure defaults: %#v", secured.Name, context)
		}
	}
	if template.Annotations[ConfigHashAnnotation] != testConfigHash ||
		!lo.ContainsBy(container.Env, func(value corev1.EnvVar) bool { return value.Name == "CUSTOM" }) ||
		!lo.ContainsBy(container.Env, func(value corev1.EnvVar) bool { return value.Name == "POD_NAME" }) {
		t.Fatal("pod template merge lost annotations or environment")
	}
}

func TestStatefulSetInjectsObjectStoreCredentialsFromSecrets(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{
			Storage: telemetryv1alpha1.StorageSpec{ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreAWS,
				AWS: &telemetryv1alpha1.AWSObjectStoreSpec{
					Region: "us-east-1", Bucket: testObjectStoreName,
					AccessKeyIDSecretRef:     &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s3"}, Key: "access-key"},
					SecretAccessKeySecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s3"}, Key: "secret-key"},
					SessionTokenSecretRef:    &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s3"}, Key: "session-token"},
				},
			}},
		}},
	}
	env := mustStatefulSet(t, metrics).Spec.Template.Spec.Containers[0].Env
	assertSecretEnv(t, env, envAWSAccessKeyID, "s3", "access-key")
	assertSecretEnv(t, env, envAWSSecretAccessKey, "s3", "secret-key")
	assertSecretEnv(t, env, envAWSSessionToken, "s3", "session-token")
}

func TestStatefulSetInjectsAzureClientSecretAuthentication(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{
			Storage: telemetryv1alpha1.StorageSpec{ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreAzure,
				Azure: &telemetryv1alpha1.AzureObjectStoreSpec{
					Account: "telemetry", Container: testObjectStoreName,
					ClientSecret: &telemetryv1alpha1.AzureClientSecretAuthSpec{
						ClientID: "client", TenantID: "tenant",
						ClientSecretKeyRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "azure"}, Key: "client-secret"},
					},
				},
			}},
		}},
	}
	env := mustStatefulSet(t, metrics).Spec.Template.Spec.Containers[0].Env
	assertLiteralEnv(t, env, envAzureCredentialType, "client_secret")
	assertLiteralEnv(t, env, envAzureClientID, "client")
	assertLiteralEnv(t, env, envAzureTenantID, "tenant")
	assertSecretEnv(t, env, envAzureClientSecret, "azure", "client-secret")
}

func TestStatefulSetInjectsGCPServiceAccount(t *testing.T) {
	metrics := &telemetryv1alpha1.Metrics{
		ObjectMeta: metav1.ObjectMeta{Name: testMetricsName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MetricsSpec{Config: telemetryv1alpha1.MetricsConfigSpec{
			Storage: telemetryv1alpha1.StorageSpec{ObjectStore: telemetryv1alpha1.ObjectStoreSpec{
				Type: telemetryv1alpha1.ObjectStoreGCP,
				GCP: &telemetryv1alpha1.GCPObjectStoreSpec{
					Bucket: testObjectStoreName,
					ServiceAccountKeySecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "gcp"}, Key: "service-account.json",
					},
				},
			}},
		}},
	}
	env := mustStatefulSet(t, metrics).Spec.Template.Spec.Containers[0].Env
	assertSecretEnv(t, env, envGoogleServiceAccountKey, "gcp", "service-account.json")
}

func mustStatefulSet(t *testing.T, metrics *telemetryv1alpha1.Metrics) *appsv1.StatefulSet {
	t.Helper()
	statefulSet, err := StatefulSet(StatefulSetInput{
		Metrics: metrics, Component: ComponentStandalone, ConfigSecretName: volumeConfig,
		InternalTokenSecretName: testTokenSecretName, InternalTokenSecretKey: TokenKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	return statefulSet
}

func assertPVCRetentionPolicy(
	t *testing.T,
	statefulSet *appsv1.StatefulSet,
	whenDeleted appsv1.PersistentVolumeClaimRetentionPolicyType,
	whenScaled appsv1.PersistentVolumeClaimRetentionPolicyType,
) {
	t.Helper()
	policy := statefulSet.Spec.PersistentVolumeClaimRetentionPolicy
	if policy == nil {
		t.Fatal("persistentVolumeClaimRetentionPolicy is missing")
	}
	if policy.WhenDeleted != whenDeleted || policy.WhenScaled != whenScaled {
		t.Fatalf(
			"persistentVolumeClaimRetentionPolicy = %#v, want whenDeleted=%q whenScaled=%q",
			policy,
			whenDeleted,
			whenScaled,
		)
	}
}

func assertClaim(t *testing.T, claims []corev1.PersistentVolumeClaim, name, size string) {
	t.Helper()
	for _, claim := range claims {
		if claim.Name == name {
			actual := claim.Spec.Resources.Requests[corev1.ResourceStorage]
			if actual.Cmp(resource.MustParse(size)) != 0 {
				t.Fatalf("%s size = %s, want %s", name, actual.String(), size)
			}
			return
		}
	}
	t.Fatalf("claim %s is missing", name)
}

func assertResourceQuantity(t *testing.T, resources corev1.ResourceList, name corev1.ResourceName, expected string) {
	t.Helper()
	actual, exists := resources[name]
	if !exists {
		t.Fatalf("resource %s is missing", name)
	}
	if actual.Cmp(resource.MustParse(expected)) != 0 {
		t.Fatalf("resource %s = %s, want %s", name, actual.String(), expected)
	}
}

func assertEmptyDir(t *testing.T, volumes []corev1.Volume, name string, size resource.Quantity) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name && volume.EmptyDir != nil && volume.EmptyDir.SizeLimit != nil {
			if volume.EmptyDir.SizeLimit.Cmp(size) != 0 {
				t.Fatalf("%s size = %s, want %s", name, volume.EmptyDir.SizeLimit.String(), size.String())
			}
			return
		}
	}
	t.Fatalf("%s emptyDir is missing", name)
}

func assertSecretEnv(t *testing.T, env []corev1.EnvVar, name, secret, key string) {
	t.Helper()
	value, found := lo.Find(env, func(item corev1.EnvVar) bool { return item.Name == name })
	if !found || value.ValueFrom == nil || value.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("secret environment variable %q is missing: %#v", name, env)
	}
	if value.ValueFrom.SecretKeyRef.Name != secret || value.ValueFrom.SecretKeyRef.Key != key {
		t.Fatalf("%s references %s/%s, want %s/%s", name, value.ValueFrom.SecretKeyRef.Name, value.ValueFrom.SecretKeyRef.Key, secret, key)
	}
}

func assertLiteralEnv(t *testing.T, env []corev1.EnvVar, name, expected string) {
	t.Helper()
	value, found := lo.Find(env, func(item corev1.EnvVar) bool { return item.Name == name })
	if !found || value.Value != expected {
		t.Fatalf("%s = %q, want %q", name, value.Value, expected)
	}
}

func assertIngressPath(t *testing.T, paths []networkingv1.HTTPIngressPath, path, service string) {
	t.Helper()
	value, found := lo.Find(paths, func(item networkingv1.HTTPIngressPath) bool { return item.Path == path })
	if !found || value.PathType == nil || *value.PathType != networkingv1.PathTypePrefix ||
		value.Backend.Service == nil || value.Backend.Service.Name != service ||
		value.Backend.Service.Port.Name != portHTTP {
		t.Fatalf("ingress path %q to %q is missing: %#v", path, service, paths)
	}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	return lo.ContainsBy(volumes, func(volume corev1.Volume) bool { return volume.Name == name })
}
