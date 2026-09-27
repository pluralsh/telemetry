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
	testMeterName       = "example"
	testNamespace       = "test"
	testObjectStoreName = "meter"
)

func TestStatefulSetUsesPersistentDefaults(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace}}
	statefulSet := mustStatefulSet(t, meter)
	if image := statefulSet.Spec.Template.Spec.Containers[0].Image; image != "ghcr.io/pluralsh/meter:0.1.0" {
		t.Fatalf("default image = %q, want GHCR Meter image", image)
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
}

func TestServiceAccountIncludesConfiguredAnnotations(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{ServiceAccount: telemetryv1alpha1.ServiceAccountSpec{
			Annotations: map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/meter"},
		}},
	}
	serviceAccount := ServiceAccount(meter)
	if serviceAccount.Annotations["eks.amazonaws.com/role-arn"] != "arn:aws:iam::123456789012:role/meter" {
		t.Fatalf("service account annotations = %#v", serviceAccount.Annotations)
	}
}

func TestIngressUsesStandaloneServiceAndTLSDefaults(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{Ingress: telemetryv1alpha1.IngressSpec{
			Enabled: true, Hostname: "meter.example.com", IngressClass: "nginx", PathPrefix: "/meter",
			Metadata: telemetryv1alpha1.IngressMetadataSpec{
				Annotations: map[string]string{"cert-manager.io/cluster-issuer": "letsencrypt"},
				Labels:      map[string]string{"example.com/exposure": "external"},
			},
			TLS: telemetryv1alpha1.IngressTLSSpec{Enabled: true},
		}},
	}
	ingress := Ingress(meter)
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != "nginx" {
		t.Fatalf("ingress class = %#v", ingress.Spec.IngressClassName)
	}
	if ingress.Annotations["cert-manager.io/cluster-issuer"] != "letsencrypt" ||
		ingress.Labels["example.com/exposure"] != "external" {
		t.Fatalf("ingress metadata = %#v/%#v", ingress.Labels, ingress.Annotations)
	}
	if len(ingress.Spec.TLS) != 1 || ingress.Spec.TLS[0].SecretName != testMeterName+"-tls" {
		t.Fatalf("ingress TLS = %#v", ingress.Spec.TLS)
	}
	paths := ingress.Spec.Rules[0].HTTP.Paths
	if len(paths) != 2 {
		t.Fatalf("standalone ingress paths = %#v", paths)
	}
	assertIngressPath(t, paths, "/meter/write", testMeterName)
	assertIngressPath(t, paths, "/meter/read", testMeterName)
}

func TestIngressRoutesShardedWritesAndReads(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{
			Mode:    telemetryv1alpha1.MeterModeSharded,
			Ingress: telemetryv1alpha1.IngressSpec{Enabled: true, Hostname: "meter.example.com"},
		},
	}
	paths := Ingress(meter).Spec.Rules[0].HTTP.Paths
	if len(paths) != 2 {
		t.Fatalf("sharded ingress paths = %#v", paths)
	}
	assertIngressPath(t, paths, "/write", testMeterName+"-writer")
	assertIngressPath(t, paths, "/read", testMeterName+"-reader")
}

func TestStatefulSetSupportsExplicitEmptyDir(t *testing.T) {
	dataSize, cacheSize := resource.MustParse("1Gi"), resource.MustParse("2Gi")
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{Writer: telemetryv1alpha1.WorkloadSpec{
			DataVolume:  &telemetryv1alpha1.VolumeSpec{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &dataSize}},
			CacheVolume: &telemetryv1alpha1.VolumeSpec{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &cacheSize}},
		}},
	}
	statefulSet := mustStatefulSet(t, meter)
	if len(statefulSet.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("emptyDir workload unexpectedly has claims: %#v", statefulSet.Spec.VolumeClaimTemplates)
	}
	assertEmptyDir(t, statefulSet.Spec.Template.Spec.Volumes, "data", dataSize)
	assertEmptyDir(t, statefulSet.Spec.Template.Spec.Volumes, "cache", cacheSize)
}

func TestStatefulSetMergesPodSecurityDefaults(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Status:     telemetryv1alpha1.MeterStatus{ConfigHash: "hash"},
		Spec: telemetryv1alpha1.MeterSpec{Writer: telemetryv1alpha1.WorkloadSpec{PodTemplate: &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers:     []corev1.Container{{Name: containerMeter, Env: []corev1.EnvVar{{Name: "CUSTOM", Value: "yes"}}}, {Name: "sidecar"}},
				InitContainers: []corev1.Container{{Name: "init"}},
			},
		}}},
	}
	statefulSet := mustStatefulSet(t, meter)
	template := statefulSet.Spec.Template
	container := template.Spec.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("meter container did not receive secure defaults")
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
	if template.Annotations[ConfigHashAnnotation] != "hash" ||
		!lo.ContainsBy(container.Env, func(value corev1.EnvVar) bool { return value.Name == "CUSTOM" }) ||
		!lo.ContainsBy(container.Env, func(value corev1.EnvVar) bool { return value.Name == "POD_NAME" }) {
		t.Fatal("pod template merge lost annotations or environment")
	}
}

func TestStatefulSetInjectsObjectStoreCredentialsFromSecrets(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{Config: telemetryv1alpha1.MeterConfigSpec{
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
	env := mustStatefulSet(t, meter).Spec.Template.Spec.Containers[0].Env
	assertSecretEnv(t, env, envAWSAccessKeyID, "s3", "access-key")
	assertSecretEnv(t, env, envAWSSecretAccessKey, "s3", "secret-key")
	assertSecretEnv(t, env, envAWSSessionToken, "s3", "session-token")
}

func TestStatefulSetInjectsAzureClientSecretAuthentication(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{Config: telemetryv1alpha1.MeterConfigSpec{
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
	env := mustStatefulSet(t, meter).Spec.Template.Spec.Containers[0].Env
	assertLiteralEnv(t, env, envAzureCredentialType, "client_secret")
	assertLiteralEnv(t, env, envAzureClientID, "client")
	assertLiteralEnv(t, env, envAzureTenantID, "tenant")
	assertSecretEnv(t, env, envAzureClientSecret, "azure", "client-secret")
}

func TestStatefulSetInjectsGCPServiceAccount(t *testing.T) {
	meter := &telemetryv1alpha1.Meter{
		ObjectMeta: metav1.ObjectMeta{Name: testMeterName, Namespace: testNamespace},
		Spec: telemetryv1alpha1.MeterSpec{Config: telemetryv1alpha1.MeterConfigSpec{
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
	env := mustStatefulSet(t, meter).Spec.Template.Spec.Containers[0].Env
	assertSecretEnv(t, env, envGoogleServiceAccountKey, "gcp", "service-account.json")
}

func mustStatefulSet(t *testing.T, meter *telemetryv1alpha1.Meter) *appsv1.StatefulSet {
	t.Helper()
	statefulSet, err := StatefulSet(StatefulSetInput{
		Meter: meter, Component: ComponentStandalone, ConfigSecretName: "config",
		InternalTokenSecretName: "token", InternalTokenSecretKey: TokenKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	return statefulSet
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
