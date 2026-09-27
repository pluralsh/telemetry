package resources

import (
	"fmt"
	"maps"
	"strings"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

type Component string

const (
	ComponentNone       Component = ""
	ComponentStandalone Component = "standalone"
	ComponentWriter     Component = "writer"
	ComponentReader     Component = "reader"

	ConfigHashAnnotation = "telemetry.plural.sh/config-hash"
	MeterNameAnnotation  = "telemetry.plural.sh/meter-name"
	TokenKey             = "internal-token"
	InternalTokenPath    = "/var/run/secrets/meter/internal-token"

	volumeConfig              = "config"
	volumeSecrets             = "secrets"
	volumeInternalToken       = "internal-token"
	volumeData                = "data"
	volumeCache               = "cache"
	containerMeter            = "meter"
	portHTTP                  = "http"
	portGRPC                  = "grpc"
	configPath                = "/etc/meter/meter.yaml"
	secretsPath               = "/etc/meter/secrets"
	dataPath                  = "/var/lib/meter"
	cachePath                 = "/var/cache/meter"
	verbGet                   = "get"
	defaultMeterImage         = "ghcr.io/pluralsh/meter"
	meterUserID         int64 = 10001

	envAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	envAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	envAWSSessionToken    = "AWS_SESSION_TOKEN"

	envAzureCredentialType = "AZURE_CREDENTIAL_TYPE"
	envAzureAccessKey      = "AZURE_STORAGE_ACCOUNT_KEY"
	envAzureSASToken       = "AZURE_STORAGE_SAS_TOKEN"
	envAzureBearerToken    = "AZURE_STORAGE_TOKEN"
	envAzureClientID       = "AZURE_STORAGE_CLIENT_ID"
	envAzureClientSecret   = "AZURE_STORAGE_CLIENT_SECRET"
	envAzureTenantID       = "AZURE_STORAGE_TENANT_ID"
	envAzureFederatedToken = "AZURE_FEDERATED_TOKEN_FILE"

	envGoogleServiceAccountKey = "GOOGLE_SERVICE_ACCOUNT_KEY"
	envGoogleBearerToken       = "GOOGLE_BEARER_TOKEN"
)

var (
	defaultDataSize  = resource.MustParse("10Gi")
	defaultCacheSize = resource.MustParse("20Gi")
)

type VolumeError struct {
	Component Component
	Volume    string
	Reason    string
}

func (e *VolumeError) Error() string {
	return fmt.Sprintf("%s storage %q is invalid: %s", e.Component, e.Volume, e.Reason)
}

type StatefulSetInput struct {
	Meter                   *telemetryv1alpha1.Meter
	Component               Component
	ConfigSecretName        string
	InternalTokenSecretName string
	InternalTokenSecretKey  string
}

func Mode(meter *telemetryv1alpha1.Meter) telemetryv1alpha1.MeterMode {
	return lo.Ternary(meter.Spec.Mode == "", telemetryv1alpha1.MeterModeStandalone, meter.Spec.Mode)
}

func Components(meter *telemetryv1alpha1.Meter) []Component {
	if Mode(meter) == telemetryv1alpha1.MeterModeSharded {
		return []Component{ComponentWriter, ComponentReader}
	}
	return []Component{ComponentStandalone}
}

func ComponentName(meter *telemetryv1alpha1.Meter, component Component) string {
	if component == ComponentStandalone {
		return meter.Name
	}
	return Name(meter.Name, string(component))
}

func ConfigKey(component Component) string {
	if component == ComponentReader {
		return "reader.yaml"
	}
	return "meter.yaml"
}

func HTTPPort(meter *telemetryv1alpha1.Meter) int32 {
	return lo.Ternary(meter.Spec.Service.HTTPPort == 0, int32(8080), meter.Spec.Service.HTTPPort)
}

func GRPCPort(meter *telemetryv1alpha1.Meter) int32 {
	return lo.Ternary(meter.Spec.Service.GRPCPort == 0, int32(9090), meter.Spec.Service.GRPCPort)
}

func Replicas(meter *telemetryv1alpha1.Meter, component Component) *int32 {
	if component == ComponentStandalone {
		return lo.ToPtr(int32(1))
	}
	if value := workloadFor(meter, component).Replicas; value != nil {
		return value
	}
	return lo.ToPtr(lo.Ternary(component == ComponentWriter, int32(3), int32(2)))
}

func ServiceAccount(meter *telemetryv1alpha1.Meter) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: meter.Name, Namespace: meter.Namespace, Labels: Labels(meter, ComponentNone)}}
}

func Role(meter *telemetryv1alpha1.Meter) *rbacv1.Role {
	name := Name(meter.Name, "sharding")
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace, Labels: Labels(meter, ComponentNone)},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{verbGet, "list", "watch", "create", "update", "patch"}},
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{verbGet, "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, ResourceNames: []string{ComponentName(meter, ComponentWriter)}, Verbs: []string{verbGet}},
		},
	}
}

func RoleBinding(meter *telemetryv1alpha1.Meter) *rbacv1.RoleBinding {
	name := Name(meter.Name, "sharding")
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace, Labels: Labels(meter, ComponentNone)},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: meter.Name, Namespace: meter.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
	}
}

func Service(meter *telemetryv1alpha1.Meter, component Component, headless bool) *corev1.Service {
	name := ComponentName(meter, component)
	if headless {
		name = Name(name, "headless")
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace, Labels: Labels(meter, component)},
		Spec: corev1.ServiceSpec{
			Selector: SelectorLabels(meter, component),
			Ports: []corev1.ServicePort{
				{Name: portHTTP, Port: HTTPPort(meter), TargetPort: intstr.FromString(portHTTP), Protocol: corev1.ProtocolTCP},
				{Name: portGRPC, Port: GRPCPort(meter), TargetPort: intstr.FromString(portGRPC), Protocol: corev1.ProtocolTCP},
			},
		},
	}
	if headless {
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.Ports = service.Spec.Ports[1:]
	} else {
		service.Spec.Type = lo.Ternary(meter.Spec.Service.Type == "", corev1.ServiceTypeClusterIP, meter.Spec.Service.Type)
		service.Annotations = copyMap(meter.Spec.Service.Annotations)
	}
	return service
}

func StatefulSet(input StatefulSetInput) (*appsv1.StatefulSet, error) {
	meter, component := input.Meter, input.Component
	name := ComponentName(meter, component)
	workload := workloadFor(meter, component)
	dataVolume := volumeSpec(workload.DataVolume, defaultDataSize)
	cacheVolume := volumeSpec(workload.CacheVolume, defaultCacheSize)
	claims := make([]corev1.PersistentVolumeClaim, 0, 2)
	for _, item := range []struct {
		name string
		spec telemetryv1alpha1.VolumeSpec
	}{{volumeData, dataVolume}, {volumeCache, cacheVolume}} {
		if err := validateVolume(component, item.name, item.spec); err != nil {
			return nil, err
		}
		if claim := claimTemplate(item.name, item.spec); claim != nil {
			claim.Labels = Labels(meter, component)
			claim.Annotations = map[string]string{MeterNameAnnotation: meter.Name}
			claims = append(claims, *claim)
		}
	}
	template := corev1.PodTemplateSpec{}
	if workload.PodTemplate != nil {
		template = *workload.PodTemplate
	}
	template = podTemplate(input, template, dataVolume, cacheVolume)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: meter.Namespace, Labels: Labels(meter, component)},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: Name(name, "headless"), Replicas: Replicas(meter, component),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: SelectorLabels(meter, component)},
			Template:            template, VolumeClaimTemplates: claims,
		},
	}, nil
}

func volumeSpec(configured *telemetryv1alpha1.VolumeSpec, size resource.Quantity) telemetryv1alpha1.VolumeSpec {
	if configured != nil {
		return *configured.DeepCopy()
	}
	quantity := size.DeepCopy()
	return telemetryv1alpha1.VolumeSpec{PersistentVolumeClaim: &corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: quantity}},
	}}
}

func validateVolume(component Component, volume string, spec telemetryv1alpha1.VolumeSpec) error {
	if spec.PersistentVolumeClaim == nil {
		return nil
	}
	size, found := spec.PersistentVolumeClaim.Resources.Requests[corev1.ResourceStorage]
	if !found || size.Sign() <= 0 {
		return &VolumeError{Component: component, Volume: volume, Reason: "persistentVolumeClaim resources.requests.storage must be greater than zero"}
	}
	return nil
}

func claimTemplate(name string, spec telemetryv1alpha1.VolumeSpec) *corev1.PersistentVolumeClaim {
	if spec.PersistentVolumeClaim == nil {
		return nil
	}
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: *spec.PersistentVolumeClaim.DeepCopy()}
}

func podTemplate(input StatefulSetInput, user corev1.PodTemplateSpec, dataVolume, cacheVolume telemetryv1alpha1.VolumeSpec) corev1.PodTemplateSpec {
	meter, component := input.Meter, input.Component
	template := *user.DeepCopy()
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	for key, value := range SelectorLabels(meter, component) {
		template.Labels[key] = value
	}
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	template.Annotations[ConfigHashAnnotation] = meter.Status.ConfigHash
	spec := &template.Spec
	spec.ServiceAccountName = meter.Name
	if spec.AutomountServiceAccountToken == nil {
		spec.AutomountServiceAccountToken = lo.ToPtr(Mode(meter) == telemetryv1alpha1.MeterModeSharded)
	}
	if spec.TerminationGracePeriodSeconds == nil {
		spec.TerminationGracePeriodSeconds = lo.ToPtr(int64(30))
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if spec.SecurityContext.RunAsNonRoot == nil {
		spec.SecurityContext.RunAsNonRoot = lo.ToPtr(true)
	}
	if spec.SecurityContext.RunAsUser == nil {
		spec.SecurityContext.RunAsUser = lo.ToPtr(meterUserID)
	}
	if spec.SecurityContext.RunAsGroup == nil {
		spec.SecurityContext.RunAsGroup = lo.ToPtr(meterUserID)
	}
	if spec.SecurityContext.FSGroup == nil {
		spec.SecurityContext.FSGroup = lo.ToPtr(meterUserID)
	}
	if spec.SecurityContext.FSGroupChangePolicy == nil {
		spec.SecurityContext.FSGroupChangePolicy = lo.ToPtr(corev1.FSGroupChangeOnRootMismatch)
	}
	if spec.SecurityContext.SeccompProfile == nil {
		spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}

	meterContainer := corev1.Container{Name: containerMeter}
	others := make([]corev1.Container, 0, len(spec.Containers))
	for _, container := range spec.Containers {
		if container.Name == containerMeter {
			meterContainer = container
		} else {
			others = append(others, container)
		}
	}
	if meterContainer.Image == "" {
		meterContainer.Image = lo.CoalesceOrEmpty(meter.Spec.Image.Repository, defaultMeterImage) + ":" + lo.CoalesceOrEmpty(meter.Spec.Image.Tag, "0.1.0")
	}
	if meter.Spec.Image.PullPolicy != "" {
		meterContainer.ImagePullPolicy = meter.Spec.Image.PullPolicy
	} else if meterContainer.ImagePullPolicy == "" {
		meterContainer.ImagePullPolicy = corev1.PullIfNotPresent
	}
	meterContainer.Args = []string{"--config", configPath}
	meterContainer.Ports = mergeNamed(meterContainer.Ports, func(item corev1.ContainerPort) string { return item.Name },
		corev1.ContainerPort{Name: portHTTP, ContainerPort: HTTPPort(meter), Protocol: corev1.ProtocolTCP},
		corev1.ContainerPort{Name: portGRPC, ContainerPort: GRPCPort(meter), Protocol: corev1.ProtocolTCP})
	meterContainer.Env = mergeNamed(meterContainer.Env, func(item corev1.EnvVar) string { return item.Name },
		corev1.EnvVar{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		corev1.EnvVar{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}})
	meterContainer.Env = mergeNamed(meterContainer.Env, func(item corev1.EnvVar) string { return item.Name }, objectStoreEnv(meter)...)
	meterContainer.VolumeMounts = mergeNamed(meterContainer.VolumeMounts, func(item corev1.VolumeMount) string { return item.Name },
		corev1.VolumeMount{Name: volumeConfig, MountPath: configPath, SubPath: ConfigKey(component), ReadOnly: true},
		corev1.VolumeMount{Name: volumeSecrets, MountPath: secretsPath, ReadOnly: true},
		corev1.VolumeMount{Name: volumeInternalToken, MountPath: InternalTokenPath, SubPath: TokenKey, ReadOnly: true},
		corev1.VolumeMount{Name: volumeData, MountPath: dataPath},
		corev1.VolumeMount{Name: volumeCache, MountPath: cachePath})
	if meterContainer.LivenessProbe == nil {
		meterContainer.LivenessProbe = httpProbe("/-/healthy", 10, 10, 2, 3)
	}
	if meterContainer.ReadinessProbe == nil {
		meterContainer.ReadinessProbe = httpProbe("/-/ready", 2, 5, 2, 6)
	}
	spec.Containers = append([]corev1.Container{meterContainer}, others...)
	for i := range spec.Containers {
		spec.Containers[i].SecurityContext = secureContainerContext(spec.Containers[i].SecurityContext)
	}
	for i := range spec.InitContainers {
		spec.InitContainers[i].SecurityContext = secureContainerContext(spec.InitContainers[i].SecurityContext)
	}
	for i := range spec.EphemeralContainers {
		spec.EphemeralContainers[i].SecurityContext = secureContainerContext(spec.EphemeralContainers[i].SecurityContext)
	}

	required := []corev1.Volume{
		{Name: volumeConfig, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.ConfigSecretName}}},
		{Name: volumeSecrets, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.ConfigSecretName}}},
		{Name: volumeInternalToken, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.InternalTokenSecretName, Items: []corev1.KeyToPath{{Key: input.InternalTokenSecretKey, Path: TokenKey}}}}},
	}
	if dataVolume.EmptyDir != nil {
		required = append(required, corev1.Volume{Name: volumeData, VolumeSource: corev1.VolumeSource{EmptyDir: dataVolume.EmptyDir.DeepCopy()}})
	}
	if cacheVolume.EmptyDir != nil {
		required = append(required, corev1.Volume{Name: volumeCache, VolumeSource: corev1.VolumeSource{EmptyDir: cacheVolume.EmptyDir.DeepCopy()}})
	}
	spec.Volumes = mergeNamed(spec.Volumes, func(item corev1.Volume) string { return item.Name }, required...)
	return template
}

func objectStoreEnv(meter *telemetryv1alpha1.Meter) []corev1.EnvVar {
	objectStore := meter.Spec.Config.Storage.ObjectStore
	switch {
	case objectStore.AWS != nil:
		return lo.Compact([]corev1.EnvVar{
			secretEnv(envAWSAccessKeyID, objectStore.AWS.AccessKeyIDSecretRef),
			secretEnv(envAWSSecretAccessKey, objectStore.AWS.SecretAccessKeySecretRef),
			secretEnv(envAWSSessionToken, objectStore.AWS.SessionTokenSecretRef),
		})
	case objectStore.Azure != nil:
		return azureObjectStoreEnv(objectStore.Azure)
	case objectStore.GCP != nil:
		return lo.Compact([]corev1.EnvVar{
			secretEnv(envGoogleServiceAccountKey, objectStore.GCP.ServiceAccountKeySecretRef),
			secretEnv(envGoogleBearerToken, objectStore.GCP.BearerTokenSecretRef),
		})
	default:
		return nil
	}
}

func azureObjectStoreEnv(spec *telemetryv1alpha1.AzureObjectStoreSpec) []corev1.EnvVar {
	switch {
	case spec.AccessKeySecretRef != nil:
		return []corev1.EnvVar{
			{Name: envAzureCredentialType, Value: "access_key"},
			secretEnv(envAzureAccessKey, spec.AccessKeySecretRef),
		}
	case spec.SASTokenSecretRef != nil:
		return []corev1.EnvVar{
			{Name: envAzureCredentialType, Value: "sas_token"},
			secretEnv(envAzureSASToken, spec.SASTokenSecretRef),
		}
	case spec.BearerTokenSecretRef != nil:
		return []corev1.EnvVar{
			{Name: envAzureCredentialType, Value: "bearer_token"},
			secretEnv(envAzureBearerToken, spec.BearerTokenSecretRef),
		}
	case spec.ClientSecret != nil:
		return []corev1.EnvVar{
			{Name: envAzureCredentialType, Value: "client_secret"},
			{Name: envAzureClientID, Value: spec.ClientSecret.ClientID},
			{Name: envAzureTenantID, Value: spec.ClientSecret.TenantID},
			secretEnv(envAzureClientSecret, &spec.ClientSecret.ClientSecretKeyRef),
		}
	case spec.WorkloadIdentity != nil:
		return []corev1.EnvVar{
			{Name: envAzureCredentialType, Value: "workload_identity"},
			{Name: envAzureClientID, Value: spec.WorkloadIdentity.ClientID},
			{Name: envAzureTenantID, Value: spec.WorkloadIdentity.TenantID},
			{Name: envAzureFederatedToken, Value: spec.WorkloadIdentity.TokenFile},
		}
	default:
		return nil
	}
}

func secretEnv(name string, selector *corev1.SecretKeySelector) corev1.EnvVar {
	if selector == nil {
		return corev1.EnvVar{}
	}
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: selector.DeepCopy()}}
}

func secureContainerContext(context *corev1.SecurityContext) *corev1.SecurityContext {
	if context == nil {
		context = &corev1.SecurityContext{}
	}
	if context.AllowPrivilegeEscalation == nil {
		context.AllowPrivilegeEscalation = lo.ToPtr(false)
	}
	if context.Privileged == nil {
		context.Privileged = lo.ToPtr(false)
	}
	if context.ReadOnlyRootFilesystem == nil {
		context.ReadOnlyRootFilesystem = lo.ToPtr(true)
	}
	if context.RunAsNonRoot == nil {
		context.RunAsNonRoot = lo.ToPtr(true)
	}
	if context.RunAsUser == nil {
		context.RunAsUser = lo.ToPtr(meterUserID)
	}
	if context.RunAsGroup == nil {
		context.RunAsGroup = lo.ToPtr(meterUserID)
	}
	if context.Capabilities == nil {
		context.Capabilities = &corev1.Capabilities{}
	}
	if !lo.Contains(context.Capabilities.Drop, corev1.Capability("ALL")) {
		context.Capabilities.Drop = append(context.Capabilities.Drop, corev1.Capability("ALL"))
	}
	if context.SeccompProfile == nil {
		context.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}
	return context
}

func httpProbe(path string, initial, period, timeout, failures int32) *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString(portHTTP)}}, InitialDelaySeconds: initial, PeriodSeconds: period, TimeoutSeconds: timeout, FailureThreshold: failures}
}

func mergeNamed[T any](existing []T, name func(T) string, required ...T) []T {
	for _, item := range required {
		if _, index, found := lo.FindIndexOf(existing, func(candidate T) bool { return name(candidate) == name(item) }); found {
			existing[index] = item
		} else {
			existing = append(existing, item)
		}
	}
	return existing
}

func workloadFor(meter *telemetryv1alpha1.Meter, component Component) telemetryv1alpha1.WorkloadSpec {
	if component == ComponentReader {
		return meter.Spec.Reader
	}
	return meter.Spec.Writer
}

func Labels(meter *telemetryv1alpha1.Meter, component Component) map[string]string {
	result := SelectorLabels(meter, component)
	result["app.kubernetes.io/managed-by"] = "meter-operator"
	return result
}

func SelectorLabels(meter *telemetryv1alpha1.Meter, component Component) map[string]string {
	result := map[string]string{"app.kubernetes.io/name": containerMeter, "app.kubernetes.io/instance": Name(meter.Name, "")}
	if component != ComponentNone {
		result["app.kubernetes.io/component"] = string(component)
	}
	return result
}

func Name(base, suffix string) string {
	base, suffix = strings.Trim(strings.ToLower(base), "-"), strings.Trim(strings.ToLower(suffix), "-")
	if suffix == "" {
		if len(base) <= 63 {
			return base
		}
		return strings.TrimRight(base[:63], "-")
	}
	maxBase := 63 - len(suffix) - 1
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + "-" + suffix
}

func copyMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	return maps.Clone(input)
}
