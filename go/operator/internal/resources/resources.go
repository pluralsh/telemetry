package resources

import (
	"fmt"
	"maps"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	operatorversion "github.com/pluralsh/telemetry/go/operator/internal/version"
)

type Component string

const (
	ComponentNone       Component = ""
	ComponentStandalone Component = "standalone"
	ComponentWriter     Component = "writer"
	ComponentReader     Component = "reader"

	ConfigHashAnnotation   = "telemetry.plural.sh/config-hash"
	MetricsNameAnnotation  = "telemetry.plural.sh/metrics-name"
	LogsNameAnnotation     = "telemetry.plural.sh/logs-name"
	TracesNameAnnotation   = "telemetry.plural.sh/traces-name"
	PseudoFSNameAnnotation = "telemetry.plural.sh/pseudofs-name"
	TokenKey               = "internal-token"
	// ShardLabel marks a reader group's workload with the storage shard it
	// serves.
	ShardLabel     = "telemetry.plural.sh/shard"
	envReaderShard = "READER_SHARD"
	// maxShardDigits bounds the shard ids reader group names must fit.
	maxShardDigits    = 5
	InternalTokenPath = "/var/run/secrets/metrics/internal-token"

	volumeConfig              = "config"
	volumeSecrets             = "secrets"
	volumeInternalToken       = "internal-token"
	volumeData                = "data"
	volumeCache               = "cache"
	containerMetrics          = "metrics"
	portHTTP                  = "http"
	portGRPC                  = "grpc"
	verbGet                   = "get"
	verbList                  = "list"
	verbWatch                 = "watch"
	routeRead                 = "/read"
	routeWrite                = "/write"
	productUserID       int64 = 10001

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
	defaultDataSize    = resource.MustParse("10Gi")
	defaultCacheSize   = resource.MustParse("20Gi")
	defaultCPURequest  = resource.MustParse("250m")
	defaultMemoryLimit = resource.MustParse("2Gi")
	// Writers roll closed days up into segments of up to 4 GiB of stored
	// data, and a rollup peaks at about one and a half times that.
	defaultRollupMemoryLimit = resource.MustParse("8Gi")
	MinRollupMemoryLimit     = resource.MustParse("6Gi")
)

// CacheDiskCapacity returns the Foyer disk budget for a workload's effective
// cache volume. Leave ten percent free for filesystem and cache-engine
// overhead instead of allowing Foyer to fill the mounted volume completely.
// An unbounded emptyDir has no capacity to derive, so it retains the historical
// 10 GiB cache budget.
func CacheDiskCapacity(workload telemetryv1alpha1.WorkloadSpec) int64 {
	const fallback = int64(10 * 1024 * 1024 * 1024)
	bytes, bounded := CacheVolumeCapacity(workload)
	if !bounded {
		return fallback
	}
	return bytes - bytes/10
}

// CacheVolumeCapacity returns the effective cache volume's configured byte
// capacity. An emptyDir without a size limit is intentionally unbounded.
func CacheVolumeCapacity(workload telemetryv1alpha1.WorkloadSpec) (int64, bool) {
	return volumeCapacity(volumeSpec(workload.CacheVolume, defaultCacheSize))
}

// DataVolumeCapacity returns the effective data volume's configured byte
// capacity. An emptyDir without a size limit is intentionally unbounded.
func DataVolumeCapacity(workload telemetryv1alpha1.WorkloadSpec) (int64, bool) {
	return volumeCapacity(volumeSpec(workload.DataVolume, defaultDataSize))
}

func volumeCapacity(spec telemetryv1alpha1.VolumeSpec) (int64, bool) {
	var size *resource.Quantity
	switch {
	case spec.PersistentVolumeClaim != nil:
		if quantity, ok := spec.PersistentVolumeClaim.Resources.Requests[corev1.ResourceStorage]; ok {
			size = &quantity
		}
	case spec.EmptyDir != nil:
		size = spec.EmptyDir.SizeLimit
	}
	if size == nil || size.Sign() <= 0 {
		return 0, false
	}
	return size.Value(), true
}

type VolumeError struct {
	Component Component
	Volume    string
	Reason    string
}

func (e *VolumeError) Error() string {
	return fmt.Sprintf("%s storage %q is invalid: %s", e.Component, e.Volume, e.Reason)
}

// MemoryError reports a component whose memory limit cannot hold a rollup.
type MemoryError struct {
	Component Component
	Limit     resource.Quantity
}

func (e *MemoryError) Error() string {
	return fmt.Sprintf("%s memory limit %s is below the %s a rollup needs", e.Component, e.Limit.String(), MinRollupMemoryLimit.String())
}

// rollsUp reports whether component runs the background rollup, which only
// writers and standalone servers of the time-segmented stores do.
func rollsUp(descriptor Descriptor, component Component) bool {
	return descriptor.Kind != PseudoFSDescriptor.Kind && component != ComponentReader
}

// ValidateMemory rejects writer and standalone memory limits too small for
// a rollup.
func ValidateMemory(value any) error {
	metrics := product(value)
	for _, component := range Components(value) {
		limit := MemoryLimit(workloadFor(metrics, component), metrics.Descriptor, component)
		if rollsUp(metrics.Descriptor, component) && limit < MinRollupMemoryLimit.Value() {
			return &MemoryError{Component: component, Limit: *resource.NewQuantity(limit, resource.BinarySI)}
		}
	}
	return nil
}

type StatefulSetInput struct {
	Metrics                 *telemetryv1alpha1.Metrics
	Logs                    *telemetryv1alpha1.Logs
	Traces                  *telemetryv1alpha1.Traces
	PseudoFS                *telemetryv1alpha1.PseudoFS
	Component               Component
	ConfigSecretName        string
	InternalTokenSecretName string
	InternalTokenSecretKey  string
	DefaultProductVersion   string
	// Shard makes a reader StatefulSet the group serving this storage shard.
	Shard *int32
}

type Descriptor struct {
	Kind, Name, Image, ConfigKey, ConfigDir, SecretsPath, DataPath, CachePath, InternalTokenPath string
	HTTPPort, GRPCPort                                                                           int32
	ReadRoute, WriteRoute                                                                        string
	NameAnnotation                                                                               string
	SupportsPathPrefix                                                                           bool
	GRPCOnly                                                                                     bool
	// ReloadsConfig servers apply config file changes in place, so config
	// changes don't roll their pods.
	ReloadsConfig bool
}

var (
	MetricsDescriptor = Descriptor{
		Kind: "Metrics", Name: "metrics", Image: "ghcr.io/pluralsh/metrics", ConfigKey: "metrics.yaml",
		ConfigDir: "/etc/metrics/config", SecretsPath: "/etc/metrics/secrets",
		DataPath: "/var/lib/metrics", CachePath: "/var/cache/metrics",
		InternalTokenPath: InternalTokenPath, HTTPPort: 8080, GRPCPort: 9090,
		ReadRoute: routeRead, WriteRoute: routeWrite, NameAnnotation: MetricsNameAnnotation, SupportsPathPrefix: true, ReloadsConfig: true,
	}
	LogsDescriptor = Descriptor{
		Kind: "Logs", Name: "logs", Image: "ghcr.io/pluralsh/logs", ConfigKey: "logs.yaml",
		ConfigDir: "/etc/logs/config", SecretsPath: "/etc/logs/secrets",
		DataPath: "/var/lib/logs", CachePath: "/var/cache/logs",
		InternalTokenPath: "/var/run/secrets/logs/internal-token", HTTPPort: 3100, GRPCPort: 9091,
		ReadRoute: routeRead, WriteRoute: routeWrite, NameAnnotation: LogsNameAnnotation, SupportsPathPrefix: true, ReloadsConfig: true,
	}
	TracesDescriptor = Descriptor{
		Kind: "Traces", Name: "traces", Image: "ghcr.io/pluralsh/traces", ConfigKey: "traces.yaml",
		ConfigDir: "/etc/traces/config", SecretsPath: "/etc/traces/secrets",
		DataPath: "/var/lib/traces", CachePath: "/var/cache/traces",
		InternalTokenPath: "/var/run/secrets/traces/internal-token", HTTPPort: 3200, GRPCPort: 9092,
		ReadRoute: routeRead, WriteRoute: routeWrite, NameAnnotation: TracesNameAnnotation, SupportsPathPrefix: true, ReloadsConfig: true,
	}
	PseudoFSDescriptor = Descriptor{
		Kind: "PseudoFS", Name: "pseudofs", Image: "ghcr.io/pluralsh/pseudofs", ConfigKey: "pseudofs.yaml",
		ConfigDir: "/etc/pseudofs/config", DataPath: "/var/lib/pseudofs", CachePath: "/var/cache/pseudofs",
		GRPCPort: 9093, NameAnnotation: PseudoFSNameAnnotation, GRPCOnly: true,
	}
)

type Product struct {
	metav1.ObjectMeta
	Descriptor         Descriptor
	Mode               telemetryv1alpha1.ProductMode
	Version            string
	Image              telemetryv1alpha1.ImageSpec
	Storage            telemetryv1alpha1.StorageSpec
	Writer             telemetryv1alpha1.WorkloadSpec
	Reader             telemetryv1alpha1.WorkloadSpec
	Service            telemetryv1alpha1.ServiceSpec
	Ingress            telemetryv1alpha1.IngressSpec
	ServiceAccountSpec telemetryv1alpha1.ServiceAccountSpec
	ConfigHash         string
}

func ForMetrics(metrics *telemetryv1alpha1.Metrics) *Product {
	return &Product{
		ObjectMeta: metrics.ObjectMeta, Descriptor: MetricsDescriptor, Mode: metrics.Spec.Mode,
		Version: metrics.Spec.Version, Image: metrics.Spec.Image, Storage: metrics.Spec.Config.Storage, Writer: metrics.Spec.Writer,
		Reader: metrics.Spec.Reader, Service: metrics.Spec.Service, Ingress: metrics.Spec.Ingress,
		ServiceAccountSpec: metrics.Spec.ServiceAccount, ConfigHash: metrics.Status.ConfigHash,
	}
}

func ForLogs(logs *telemetryv1alpha1.Logs) *Product {
	return &Product{
		ObjectMeta: logs.ObjectMeta, Descriptor: LogsDescriptor, Mode: logs.Spec.Mode,
		Version: logs.Spec.Version, Image: logs.Spec.Image, Storage: logs.Spec.Config.Storage, Writer: logs.Spec.Writer,
		Reader: logs.Spec.Reader, Service: logs.Spec.Service, Ingress: logs.Spec.Ingress,
		ServiceAccountSpec: logs.Spec.ServiceAccount, ConfigHash: logs.Status.ConfigHash,
	}
}

func ForTraces(traces *telemetryv1alpha1.Traces) *Product {
	return &Product{
		ObjectMeta: traces.ObjectMeta, Descriptor: TracesDescriptor, Mode: traces.Spec.Mode,
		Version: traces.Spec.Version, Image: traces.Spec.Image, Storage: traces.Spec.Config.Storage, Writer: traces.Spec.Writer,
		Reader: traces.Spec.Reader, Service: traces.Spec.Service, Ingress: traces.Spec.Ingress,
		ServiceAccountSpec: traces.Spec.ServiceAccount, ConfigHash: traces.Status.ConfigHash,
	}
}

func ForPseudoFS(pseudofs *telemetryv1alpha1.PseudoFS) *Product {
	return &Product{
		ObjectMeta: pseudofs.ObjectMeta, Descriptor: PseudoFSDescriptor, Mode: telemetryv1alpha1.ProductModeStandalone,
		Version: pseudofs.Spec.Version, Image: pseudofs.Spec.Image, Storage: pseudofs.Spec.Config.Storage,
		Writer: pseudofs.Spec.Workload,
		Service: telemetryv1alpha1.ServiceSpec{
			GRPCPort: pseudofs.Spec.Service.GRPCPort,
			Type:     pseudofs.Spec.Service.Type, Annotations: pseudofs.Spec.Service.Annotations,
		},
		ServiceAccountSpec: pseudofs.Spec.ServiceAccount, ConfigHash: pseudofs.Status.ConfigHash,
	}
}

func product(value any) *Product {
	switch value := value.(type) {
	case *Product:
		return value
	case *telemetryv1alpha1.Metrics:
		return ForMetrics(value)
	case *telemetryv1alpha1.Logs:
		return ForLogs(value)
	case *telemetryv1alpha1.Traces:
		return ForTraces(value)
	case *telemetryv1alpha1.PseudoFS:
		return ForPseudoFS(value)
	default:
		panic(fmt.Sprintf("unsupported telemetry product %T", value))
	}
}

func Mode(value any) telemetryv1alpha1.ProductMode {
	instance := product(value)
	return lo.Ternary(instance.Mode == "", telemetryv1alpha1.ProductModeStandalone, instance.Mode)
}

func Components(value any) []Component {
	if Mode(value) == telemetryv1alpha1.ProductModeSharded {
		return []Component{ComponentWriter, ComponentReader}
	}
	return []Component{ComponentStandalone}
}

func ComponentName(value any, component Component) string {
	metrics := product(value)
	if component == ComponentStandalone {
		return metrics.Name
	}
	return Name(metrics.Name, string(component))
}

func ConfigKeyFor(value any, component Component) string {
	instance := product(value)
	if component == ComponentReader {
		return "reader.yaml"
	}
	return instance.Descriptor.ConfigKey
}

func ConfigKey(component Component) string {
	return ConfigKeyFor(&Product{Descriptor: MetricsDescriptor}, component)
}

func HTTPPort(value any) int32 {
	instance := product(value)
	return lo.Ternary(instance.Service.HTTPPort == 0, instance.Descriptor.HTTPPort, instance.Service.HTTPPort)
}

func GRPCPort(value any) int32 {
	instance := product(value)
	return lo.Ternary(instance.Service.GRPCPort == 0, instance.Descriptor.GRPCPort, instance.Service.GRPCPort)
}

func Replicas(value any, component Component) *int32 {
	metrics := product(value)
	if component == ComponentStandalone {
		return lo.ToPtr(int32(1))
	}
	if autoscaling := Autoscaling(metrics, component); autoscaling != nil {
		return lo.ToPtr(minReplicas(metrics, autoscaling))
	}
	if value := workloadFor(metrics, component).Replicas; value != nil {
		return value
	}
	return lo.ToPtr(lo.Ternary(component == ComponentWriter, int32(1), int32(2)))
}

// ReadGroupsEnabled reports whether sharded readers run as one group per
// storage shard, the default.
func ReadGroupsEnabled(value any) bool {
	instance := product(value)
	if Mode(instance) != telemetryv1alpha1.ProductModeSharded {
		return false
	}
	groups := instance.Reader.ReadGroups
	return groups == nil || lo.FromPtrOr(groups.Enabled, true)
}

// ReaderGroupName is the StatefulSet of the reader group serving shard.
func ReaderGroupName(value any, shard int32) string {
	return Name(product(value).Name, fmt.Sprintf("%s-%d", ComponentReader, shard))
}

func ReaderGroupSelectorLabels(value any, shard int32) map[string]string {
	result := SelectorLabels(value, ComponentReader)
	result[ShardLabel] = fmt.Sprint(shard)
	return result
}

func ReaderGroupLabels(value any, shard int32) map[string]string {
	result := Labels(value, ComponentReader)
	result[ShardLabel] = fmt.Sprint(shard)
	return result
}

// ValidateReaderGroupNames rejects product names too long for every reader
// group's headless Service name to follow the endpoint template.
func ValidateReaderGroupNames(name string) error {
	base := Name(name, "")
	longest := len(base) + len("-"+string(ComponentReader)+"-") + maxShardDigits + len("-headless")
	if longest > 63 {
		return fmt.Errorf("name %q is too long for per-shard reader groups: it must be at most %d characters", name, len(base)-(longest-63))
	}
	return nil
}

// ReadGroupsEndpoint is the gRPC address of the reader group serving
// `{shard}`: its headless Service, which resolves to every ready replica.
func ReadGroupsEndpoint(name, namespace string, grpcPort int32) string {
	return fmt.Sprintf("%s-%s-{shard}-headless.%s.svc:%d", Name(name, ""), ComponentReader, namespace, grpcPort)
}

// ReaderGroupService is the headless Service a reader group's peers resolve
// to reach its replicas.
func ReaderGroupService(value any, shard int32) *corev1.Service {
	instance := product(value)
	service := Service(instance, ComponentReader, true)
	service.Name = Name(ReaderGroupName(instance, shard), "headless")
	service.Labels = ReaderGroupLabels(instance, shard)
	service.Spec.Selector = ReaderGroupSelectorLabels(instance, shard)
	return service
}

// Autoscaling is the component's enabled autoscaling, which only readers of
// a sharded product support.
func Autoscaling(value any, component Component) *telemetryv1alpha1.AutoscalingSpec {
	instance := product(value)
	if component != ComponentReader || Mode(instance) != telemetryv1alpha1.ProductModeSharded {
		return nil
	}
	if autoscaling := instance.Reader.Autoscaling; autoscaling != nil && autoscaling.Enabled {
		return autoscaling
	}
	return nil
}

func minReplicas(instance *Product, autoscaling *telemetryv1alpha1.AutoscalingSpec) int32 {
	if autoscaling.MinReplicas != nil {
		return *autoscaling.MinReplicas
	}
	return min(lo.FromPtrOr(instance.Reader.Replicas, int32(2)), autoscaling.MaxReplicas)
}

// PodDisruptionBudget keeps all but one replica of a reader StatefulSet up
// through voluntary disruptions, so a group always has a replica serving
// its shard while it has more than one.
func PodDisruptionBudget(value any, target ReaderTarget) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: target.Name, Namespace: product(value).Namespace, Labels: copyMap(target.Labels)},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: lo.ToPtr(intstr.FromInt32(1)),
			Selector:       &metav1.LabelSelector{MatchLabels: copyMap(target.Selector)},
		},
	}
}

// ReaderTarget is one reader StatefulSet: a reader group's, or the one
// whose readers open every shard.
type ReaderTarget struct {
	Name             string
	Shard            *int32
	Labels, Selector map[string]string
}

// ReaderTargets are the reader StatefulSets of a sharded product whose
// readers form `groups` shard groups when read groups are enabled.
func ReaderTargets(value any, groups int32) []ReaderTarget {
	instance := product(value)
	if !ReadGroupsEnabled(instance) {
		return []ReaderTarget{{
			Name:     ComponentName(instance, ComponentReader),
			Labels:   Labels(instance, ComponentReader),
			Selector: SelectorLabels(instance, ComponentReader),
		}}
	}
	targets := make([]ReaderTarget, 0, groups)
	for shard := range max(groups, 1) {
		targets = append(targets, ReaderTarget{
			Name:     ReaderGroupName(instance, shard),
			Shard:    lo.ToPtr(shard),
			Labels:   ReaderGroupLabels(instance, shard),
			Selector: ReaderGroupSelectorLabels(instance, shard),
		})
	}
	return targets
}

// HorizontalPodAutoscaler scales a reader StatefulSet on CPU utilization by
// default, or on memory, or both.
func HorizontalPodAutoscaler(value any, target ReaderTarget) *autoscalingv2.HorizontalPodAutoscaler {
	instance := product(value)
	autoscaling := Autoscaling(instance, ComponentReader)
	if autoscaling == nil {
		return nil
	}
	utilization := func(name corev1.ResourceName, target int32) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{
				Name:   name,
				Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: lo.ToPtr(target)},
			},
		}
	}
	var metrics []autoscalingv2.MetricSpec
	cpu := autoscaling.TargetCPUUtilizationPercentage
	if cpu == nil && autoscaling.TargetMemoryUtilizationPercentage == nil {
		cpu = lo.ToPtr(int32(70))
	}
	if cpu != nil {
		metrics = append(metrics, utilization(corev1.ResourceCPU, *cpu))
	}
	if memory := autoscaling.TargetMemoryUtilizationPercentage; memory != nil {
		metrics = append(metrics, utilization(corev1.ResourceMemory, *memory))
	}
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: target.Name, Namespace: instance.Namespace, Labels: copyMap(target.Labels)},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: target.Name},
			MinReplicas:    lo.ToPtr(minReplicas(instance, autoscaling)),
			MaxReplicas:    autoscaling.MaxReplicas,
			Metrics:        metrics,
			Behavior:       autoscaling.Behavior.DeepCopy(),
		},
	}
}

func ServiceAccount(value any) *corev1.ServiceAccount {
	metrics := product(value)
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name:        metrics.Name,
		Namespace:   metrics.Namespace,
		Labels:      Labels(metrics, ComponentNone),
		Annotations: copyMap(metrics.ServiceAccountSpec.Annotations),
	}}
}

func Role(value any) *rbacv1.Role {
	metrics := product(value)
	name := Name(metrics.Name, "sharding")
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metrics.Namespace, Labels: Labels(metrics, ComponentNone)},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"telemetry.plural.sh"}, Resources: []string{"shardmaps"}, Verbs: []string{verbGet, verbList, verbWatch, "create", "update", "patch"}},
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{verbGet, verbList, verbWatch, "create", "update", "patch", "delete"}},
			{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, ResourceNames: []string{ComponentName(metrics, ComponentWriter)}, Verbs: []string{verbGet, verbList, verbWatch}},
		},
	}
}

func RoleBinding(value any) *rbacv1.RoleBinding {
	metrics := product(value)
	name := Name(metrics.Name, "sharding")
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metrics.Namespace, Labels: Labels(metrics, ComponentNone)},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: metrics.Name, Namespace: metrics.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name},
	}
}

func Service(value any, component Component, headless bool) *corev1.Service {
	metrics := product(value)
	name := ComponentName(metrics, component)
	if headless {
		name = Name(name, "headless")
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metrics.Namespace, Labels: Labels(metrics, component)},
		Spec: corev1.ServiceSpec{
			Selector: SelectorLabels(metrics, component),
			Ports:    []corev1.ServicePort{{Name: portGRPC, Port: GRPCPort(metrics), TargetPort: intstr.FromString(portGRPC), Protocol: corev1.ProtocolTCP}},
		},
	}
	if !metrics.Descriptor.GRPCOnly {
		service.Spec.Ports = append([]corev1.ServicePort{{Name: portHTTP, Port: HTTPPort(metrics), TargetPort: intstr.FromString(portHTTP), Protocol: corev1.ProtocolTCP}}, service.Spec.Ports...)
	}
	if headless {
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.Ports = service.Spec.Ports[1:]
	} else {
		service.Spec.Type = lo.Ternary(metrics.Service.Type == "", corev1.ServiceTypeClusterIP, metrics.Service.Type)
		service.Annotations = copyMap(metrics.Service.Annotations)
	}
	return service
}

func Ingress(value any) *networkingv1.Ingress {
	metrics := product(value)
	labels := Labels(metrics, ComponentNone)
	maps.Copy(labels, metrics.Ingress.Metadata.Labels)
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        metrics.Name,
			Namespace:   metrics.Namespace,
			Labels:      labels,
			Annotations: copyMap(metrics.Ingress.Metadata.Annotations),
		},
		Spec: networkingv1.IngressSpec{Rules: []networkingv1.IngressRule{{
			Host: metrics.Ingress.Hostname,
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: ingressPaths(metrics),
			}},
		}}},
	}
	if metrics.Ingress.IngressClass != "" {
		ingress.Spec.IngressClassName = lo.ToPtr(metrics.Ingress.IngressClass)
	}
	if metrics.Ingress.TLS.Enabled {
		ingress.Spec.TLS = []networkingv1.IngressTLS{{
			Hosts:      []string{metrics.Ingress.Hostname},
			SecretName: lo.CoalesceOrEmpty(metrics.Ingress.TLS.SecretName, Name(metrics.Name, "tls")),
		}}
	}
	return ingress
}

func ingressPaths(metrics *Product) []networkingv1.HTTPIngressPath {
	prefix := metrics.Ingress.PathPrefix
	if !metrics.Descriptor.SupportsPathPrefix {
		prefix = ""
	}
	writerService := ComponentName(metrics, ComponentWriter)
	readerService := ComponentName(metrics, ComponentReader)
	if Mode(metrics) == telemetryv1alpha1.ProductModeStandalone {
		writerService, readerService = metrics.Name, metrics.Name
	}
	return []networkingv1.HTTPIngressPath{
		ingressPath(prefix+metrics.Descriptor.WriteRoute, networkingv1.PathTypePrefix, writerService),
		ingressPath(prefix+metrics.Descriptor.ReadRoute, networkingv1.PathTypePrefix, readerService),
	}
}

func ingressPath(path string, pathType networkingv1.PathType, serviceName string) networkingv1.HTTPIngressPath {
	return networkingv1.HTTPIngressPath{
		Path:     path,
		PathType: lo.ToPtr(pathType),
		Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
			Name: serviceName,
			Port: networkingv1.ServiceBackendPort{Name: portHTTP},
		}},
	}
}

func StatefulSet(input StatefulSetInput) (*appsv1.StatefulSet, error) {
	var metrics *Product
	if input.PseudoFS != nil {
		metrics = ForPseudoFS(input.PseudoFS)
	} else if input.Logs != nil {
		metrics = ForLogs(input.Logs)
	} else if input.Traces != nil {
		metrics = ForTraces(input.Traces)
	} else {
		metrics = ForMetrics(input.Metrics)
	}
	component := input.Component
	name := ComponentName(metrics, component)
	selector := SelectorLabels(metrics, component)
	labels := Labels(metrics, component)
	if input.Shard != nil {
		name = ReaderGroupName(metrics, *input.Shard)
		selector = ReaderGroupSelectorLabels(metrics, *input.Shard)
		labels = ReaderGroupLabels(metrics, *input.Shard)
	}
	workload := workloadFor(metrics, component)
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
			claim.Labels = copyMap(labels)
			claim.Annotations = map[string]string{metrics.Descriptor.NameAnnotation: metrics.Name}
			claims = append(claims, *claim)
		}
	}
	template := corev1.PodTemplateSpec{}
	if workload.PodTemplate != nil {
		template = *workload.PodTemplate
	}
	template = podTemplate(metrics, input, template, dataVolume, cacheVolume)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metrics.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: lo.Ternary(metrics.Descriptor.GRPCOnly, name, Name(name, "headless")), Replicas: Replicas(metrics, component),
			PodManagementPolicy:                  appsv1.ParallelPodManagement,
			UpdateStrategy:                       appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Selector:                             &metav1.LabelSelector{MatchLabels: selector},
			Template:                             template,
			VolumeClaimTemplates:                 claims,
			PersistentVolumeClaimRetentionPolicy: persistentVolumeClaimRetentionPolicy(metrics, workload),
		},
	}, nil
}

func persistentVolumeClaimRetentionPolicy(metrics *Product, workload telemetryv1alpha1.WorkloadSpec) *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
	defaultPolicy := appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	if metrics.Storage.ObjectStore.Type == "" || metrics.Storage.ObjectStore.Type == telemetryv1alpha1.ObjectStoreLocal {
		defaultPolicy = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}
	whenDeleted, whenScaled := defaultPolicy, defaultPolicy
	if configured := workload.PersistentVolumeClaimRetentionPolicy; configured != nil {
		if configured.WhenDeleted != "" {
			whenDeleted = appsv1.PersistentVolumeClaimRetentionPolicyType(configured.WhenDeleted)
		}
		if configured.WhenScaled != "" {
			whenScaled = appsv1.PersistentVolumeClaimRetentionPolicyType(configured.WhenScaled)
		}
	}
	return &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: whenDeleted,
		WhenScaled:  whenScaled,
	}
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

func podTemplate(metrics *Product, input StatefulSetInput, user corev1.PodTemplateSpec, dataVolume, cacheVolume telemetryv1alpha1.VolumeSpec) corev1.PodTemplateSpec {
	component := input.Component
	template := *user.DeepCopy()
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	for key, value := range SelectorLabels(metrics, component) {
		template.Labels[key] = value
	}
	if input.Shard != nil {
		template.Labels[ShardLabel] = fmt.Sprint(*input.Shard)
	}
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	if !metrics.Descriptor.ReloadsConfig {
		template.Annotations[ConfigHashAnnotation] = metrics.ConfigHash
	}
	spec := &template.Spec
	applyPodDefaults(spec, metrics, component)

	metricsContainer := corev1.Container{Name: metrics.Descriptor.Name}
	others := make([]corev1.Container, 0, len(spec.Containers))
	for _, container := range spec.Containers {
		if container.Name == metrics.Descriptor.Name {
			metricsContainer = container
		} else {
			others = append(others, container)
		}
	}
	metricsContainer.Resources = containerResources(metricsContainer.Resources, workloadFor(metrics, component).Resources, defaultMemory(metrics.Descriptor, component))
	productVersion := lo.CoalesceOrEmpty(metrics.Version, metrics.Image.Tag, input.DefaultProductVersion, operatorversion.ProductVersion)
	metricsContainer.Image = lo.CoalesceOrEmpty(metrics.Image.Repository, metrics.Descriptor.Image) + ":" + productVersion
	if metrics.Image.PullPolicy != "" {
		metricsContainer.ImagePullPolicy = metrics.Image.PullPolicy
	} else if metricsContainer.ImagePullPolicy == "" {
		metricsContainer.ImagePullPolicy = corev1.PullIfNotPresent
	}
	metricsContainer.Args = []string{"--config", filepath.Join(metrics.Descriptor.ConfigDir, ConfigKeyFor(metrics, component))}
	requiredPorts := []corev1.ContainerPort{{Name: portGRPC, ContainerPort: GRPCPort(metrics), Protocol: corev1.ProtocolTCP}}
	if !metrics.Descriptor.GRPCOnly {
		requiredPorts = append([]corev1.ContainerPort{{Name: portHTTP, ContainerPort: HTTPPort(metrics), Protocol: corev1.ProtocolTCP}}, requiredPorts...)
	}
	metricsContainer.Ports = mergeNamed(metricsContainer.Ports, func(item corev1.ContainerPort) string { return item.Name }, requiredPorts...)
	metricsContainer.Env = mergeNamed(metricsContainer.Env, func(item corev1.EnvVar) string { return item.Name },
		corev1.EnvVar{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		corev1.EnvVar{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}})
	metricsContainer.Env = mergeNamed(metricsContainer.Env, func(item corev1.EnvVar) string { return item.Name }, objectStoreEnv(metrics)...)
	if input.Shard != nil {
		metricsContainer.Env = mergeNamed(metricsContainer.Env, func(item corev1.EnvVar) string { return item.Name },
			corev1.EnvVar{Name: envReaderShard, Value: fmt.Sprint(*input.Shard)})
	}
	requiredMounts := []corev1.VolumeMount{
		{Name: volumeConfig, MountPath: metrics.Descriptor.ConfigDir, ReadOnly: true},
		{Name: volumeData, MountPath: metrics.Descriptor.DataPath},
		{Name: volumeCache, MountPath: metrics.Descriptor.CachePath},
	}
	if !metrics.Descriptor.GRPCOnly {
		requiredMounts = append(requiredMounts,
			corev1.VolumeMount{Name: volumeSecrets, MountPath: metrics.Descriptor.SecretsPath, ReadOnly: true},
			corev1.VolumeMount{Name: volumeInternalToken, MountPath: metrics.Descriptor.InternalTokenPath, SubPath: TokenKey, ReadOnly: true})
	}
	metricsContainer.VolumeMounts = mergeNamed(metricsContainer.VolumeMounts, func(item corev1.VolumeMount) string { return item.Name }, requiredMounts...)
	// Servers listen only once storage is open, which includes WAL replay, so
	// the startup probe holds liveness off for up to startupProbeFailures periods.
	if metrics.Descriptor.GRPCOnly && metricsContainer.StartupProbe == nil {
		metricsContainer.StartupProbe = grpcProbe(GRPCPort(metrics), 0, 10, 2, startupProbeFailures)
	} else if metricsContainer.StartupProbe == nil {
		metricsContainer.StartupProbe = httpProbe("/-/healthy", 0, 10, 2, startupProbeFailures)
	}
	if metrics.Descriptor.GRPCOnly && metricsContainer.LivenessProbe == nil {
		metricsContainer.LivenessProbe = grpcProbe(GRPCPort(metrics), 10, 10, 2, 3)
	} else if metricsContainer.LivenessProbe == nil {
		metricsContainer.LivenessProbe = httpProbe("/-/healthy", 10, 10, 2, 3)
	}
	if metrics.Descriptor.GRPCOnly && metricsContainer.ReadinessProbe == nil {
		metricsContainer.ReadinessProbe = grpcProbe(GRPCPort(metrics), 2, 5, 2, 6)
	} else if metricsContainer.ReadinessProbe == nil {
		metricsContainer.ReadinessProbe = httpProbe("/-/ready", 2, 5, 2, 6)
	}
	spec.Containers = append([]corev1.Container{metricsContainer}, others...)
	for i := range spec.Containers {
		spec.Containers[i].SecurityContext = secureContainerContext(spec.Containers[i].SecurityContext)
	}
	for i := range spec.InitContainers {
		spec.InitContainers[i].SecurityContext = secureContainerContext(spec.InitContainers[i].SecurityContext)
	}
	for i := range spec.EphemeralContainers {
		spec.EphemeralContainers[i].SecurityContext = secureContainerContext(spec.EphemeralContainers[i].SecurityContext)
	}

	required := []corev1.Volume{{Name: volumeConfig, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.ConfigSecretName}}}}
	if !metrics.Descriptor.GRPCOnly {
		required = append(required,
			corev1.Volume{Name: volumeSecrets, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.ConfigSecretName}}},
			corev1.Volume{
				Name: volumeInternalToken,
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName: input.InternalTokenSecretName,
					Items:      []corev1.KeyToPath{{Key: input.InternalTokenSecretKey, Path: TokenKey}},
				}},
			})
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

func applyPodDefaults(spec *corev1.PodSpec, metrics *Product, component Component) {
	if spec.NodeSelector == nil {
		spec.NodeSelector = map[string]string{}
	}
	maps.Copy(spec.NodeSelector, workloadFor(metrics, component).NodeSelector)
	spec.Tolerations = mergeTolerations(spec.Tolerations, workloadFor(metrics, component).Tolerations)
	spec.ServiceAccountName = metrics.Name
	if spec.AutomountServiceAccountToken == nil {
		spec.AutomountServiceAccountToken = lo.ToPtr(Mode(metrics) == telemetryv1alpha1.ProductModeSharded)
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
		spec.SecurityContext.RunAsUser = lo.ToPtr(productUserID)
	}
	if spec.SecurityContext.RunAsGroup == nil {
		spec.SecurityContext.RunAsGroup = lo.ToPtr(productUserID)
	}
	if spec.SecurityContext.FSGroup == nil {
		spec.SecurityContext.FSGroup = lo.ToPtr(productUserID)
	}
	if spec.SecurityContext.FSGroupChangePolicy == nil {
		spec.SecurityContext.FSGroupChangePolicy = lo.ToPtr(corev1.FSGroupChangeOnRootMismatch)
	}
	if spec.SecurityContext.SeccompProfile == nil {
		spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}
}

// containerResources merges the pod template's and the workload's resources
// over the defaults. Memory is requested at its limit unless a request is
// set: the scheduler then reserves what each pod may use, so pods that all
// reach their limit at once cannot run their node out of memory. A request
// set without a limit gets the larger of it and the default limit.
func containerResources(template, configured corev1.ResourceRequirements, defaultLimit resource.Quantity) corev1.ResourceRequirements {
	result := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: defaultCPURequest.DeepCopy()},
		Limits:   corev1.ResourceList{},
	}
	mergeResources := func(resources corev1.ResourceRequirements) {
		maps.Copy(result.Requests, resources.Requests)
		maps.Copy(result.Limits, resources.Limits)
		if resources.Claims != nil {
			result.Claims = append([]corev1.ResourceClaim(nil), resources.Claims...)
		}
	}
	mergeResources(template)
	mergeResources(configured)
	request, requested := result.Requests[corev1.ResourceMemory]
	limit, limited := result.Limits[corev1.ResourceMemory]
	switch {
	case !requested && !limited:
		result.Requests[corev1.ResourceMemory] = defaultLimit.DeepCopy()
		result.Limits[corev1.ResourceMemory] = defaultLimit.DeepCopy()
	case !requested:
		result.Requests[corev1.ResourceMemory] = limit.DeepCopy()
	case !limited:
		limit = defaultLimit.DeepCopy()
		if request.Cmp(limit) > 0 {
			limit = request.DeepCopy()
		}
		result.Limits[corev1.ResourceMemory] = limit
	}
	return result
}

func defaultMemory(descriptor Descriptor, component Component) resource.Quantity {
	return lo.Ternary(rollsUp(descriptor, component), defaultRollupMemoryLimit, defaultMemoryLimit)
}

// MemoryLimit is the byte memory limit the operator renders for the
// workload's descriptor container in component.
func MemoryLimit(workload telemetryv1alpha1.WorkloadSpec, descriptor Descriptor, component Component) int64 {
	var template corev1.ResourceRequirements
	if workload.PodTemplate != nil {
		if found, ok := lo.Find(workload.PodTemplate.Spec.Containers, func(candidate corev1.Container) bool {
			return candidate.Name == descriptor.Name
		}); ok {
			template = found.Resources
		}
	}
	limit := containerResources(template, workload.Resources, defaultMemory(descriptor, component)).Limits[corev1.ResourceMemory]
	return limit.Value()
}

func objectStoreEnv(metrics *Product) []corev1.EnvVar {
	objectStore := metrics.Storage.ObjectStore
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
		context.RunAsUser = lo.ToPtr(productUserID)
	}
	if context.RunAsGroup == nil {
		context.RunAsGroup = lo.ToPtr(productUserID)
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

// startupProbeFailures allows 15 minutes for storage to open.
const startupProbeFailures = 90

func httpProbe(path string, initial, period, timeout, failures int32) *corev1.Probe {
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString(portHTTP)}}, InitialDelaySeconds: initial, PeriodSeconds: period, TimeoutSeconds: timeout, FailureThreshold: failures}
}

func grpcProbe(port, initial, period, timeout, failures int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: port}},
		InitialDelaySeconds: initial, PeriodSeconds: period, TimeoutSeconds: timeout, FailureThreshold: failures,
	}
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

func mergeTolerations(existing, firstClass []corev1.Toleration) []corev1.Toleration {
	result := make([]corev1.Toleration, 0, len(existing)+len(firstClass))
	for _, toleration := range append(append([]corev1.Toleration(nil), existing...), firstClass...) {
		if _, index, found := lo.FindIndexOf(result, func(candidate corev1.Toleration) bool {
			return tolerationIdentity(candidate) == tolerationIdentity(toleration)
		}); found {
			result[index] = toleration
		} else {
			result = append(result, toleration)
		}
	}
	return result
}

func tolerationIdentity(toleration corev1.Toleration) string {
	operator := toleration.Operator
	if operator == "" {
		operator = corev1.TolerationOpEqual
	}
	return toleration.Key + "\x00" + string(operator) + "\x00" + string(toleration.Effect)
}

func workloadFor(metrics *Product, component Component) telemetryv1alpha1.WorkloadSpec {
	if component == ComponentReader {
		return metrics.Reader
	}
	return metrics.Writer
}

func Labels(value any, component Component) map[string]string {
	metrics := product(value)
	result := SelectorLabels(metrics, component)
	result["app.kubernetes.io/managed-by"] = metrics.Descriptor.Name + "-operator"
	return result
}

func SelectorLabels(value any, component Component) map[string]string {
	metrics := product(value)
	result := map[string]string{"app.kubernetes.io/name": metrics.Descriptor.Name, "app.kubernetes.io/instance": Name(metrics.Name, "")}
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
