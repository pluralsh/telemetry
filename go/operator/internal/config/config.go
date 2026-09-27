package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/samber/lo"
	"sigs.k8s.io/yaml"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const (
	MeterKey  = "meter.yaml"
	LineKey   = "line.yaml"
	ReaderKey = "reader.yaml"

	sourceFile     = "file"
	modeStandalone = "standalone"
)

type Credential struct {
	Username string
	Password []byte
	DataKey  string
}

type Access struct {
	Read  []Credential
	Write []Credential
}

type NamespaceAccess struct {
	Name      string
	KeyPrefix string
	Access    Access
}

type JWT struct {
	JWKS                   []byte
	URL                    string
	Issuer                 string
	Audience               string
	RefreshIntervalSeconds *int64
	RequestTimeoutSeconds  *int64
}

type Input struct {
	Meter             *telemetryv1alpha1.Meter
	Line              *telemetryv1alpha1.Line
	Global            Access
	Namespaces        []NamespaceAccess
	JWT               *JWT
	InternalTokenPath string
	InternalToken     []byte
	SecretsPath       string
}

type Result struct {
	Data map[string][]byte
	Hash string
}

func Render(input Input) (Result, error) {
	if input.Line != nil {
		return renderLine(input)
	}
	if input.Meter == nil {
		return Result{}, fmt.Errorf("Meter or Line is required")
	}
	descriptor := resources.MeterDescriptor
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, descriptor.SecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, descriptor.InternalTokenPath)
	data := map[string][]byte{}
	global := renderResolvedAccess(data, "global", input.Global, secretsPath)
	namespaceAccess := map[string]renderAccess{}
	for _, namespace := range input.Namespaces {
		prefix := lo.CoalesceOrEmpty(namespace.KeyPrefix, namespace.Name)
		rendered := renderResolvedAccess(data, "namespace-"+prefix, namespace.Access, secretsPath)
		current := lo.ValueOr(namespaceAccess, namespace.Name, emptyAccess())
		current.Read = append(current.Read, rendered.Read...)
		current.Write = append(current.Write, rendered.Write...)
		namespaceAccess[namespace.Name] = current
	}

	var jwt *renderJWT
	if input.JWT != nil {
		jwt = &renderJWT{
			Issuer:                 input.JWT.Issuer,
			Audience:               input.JWT.Audience,
			RefreshIntervalSeconds: int64Value(input.JWT.RefreshIntervalSeconds, 300),
			RequestTimeoutSeconds:  int64Value(input.JWT.RequestTimeoutSeconds, 5),
		}
		switch {
		case len(input.JWT.JWKS) > 0:
			data["jwks.json"] = append([]byte(nil), input.JWT.JWKS...)
			jwt.JWKS = renderJWKS{Source: sourceFile, Path: secretsPath + "/jwks.json"}
		case input.JWT.URL != "":
			jwt.JWKS = renderJWKS{Source: "url", URL: input.JWT.URL}
		default:
			return Result{}, fmt.Errorf("auth.jwt.jwks requires url or resolved secret data")
		}
	}

	names := lo.Uniq(input.Meter.Spec.Config.Namespaces)
	if len(names) == 0 {
		names = []string{"default"}
	}
	names = lo.Uniq(append(names, lo.Keys(namespaceAccess)...))
	sort.Strings(names)
	namespaces := lo.Map(names, func(name string, _ int) renderNamespace {
		return renderNamespace{Name: name, Auth: lo.ValueOr(namespaceAccess, name, emptyAccess())}
	})

	makeConfig := func(mode string) ([]byte, error) {
		return yaml.Marshal(renderConfig{
			Mode:                mode,
			Listeners:           renderListeners{HTTP: fmt.Sprintf("0.0.0.0:%d", httpPort(input.Meter)), GRPC: fmt.Sprintf("0.0.0.0:%d", grpcPort(input.Meter))},
			PathPrefix:          input.Meter.Spec.Ingress.PathPrefix,
			Storage:             renderStorageConfig(input.Meter.Spec.Config.Storage, descriptor),
			ReaderCacheCapacity: int64Value(input.Meter.Spec.Config.ReaderCacheCapacity, 268435456),
			Write:               renderWriteConfig(input.Meter.Spec.Config.Write),
			Sharding:            renderShardingConfig(input.Meter.Name, input.Meter.Namespace, input.Meter.Spec.Config.Sharding, grpcPort(input.Meter), mode),
			Auth:                renderAuth{Unauthenticated: input.Meter.Spec.Config.Auth.Unauthenticated, JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}},
			Namespaces:          namespaces,
		})
	}
	writerMode := modeStandalone
	if mode(input.Meter) == telemetryv1alpha1.MeterModeSharded {
		writerMode = "writer"
	}
	var err error
	data[MeterKey], err = makeConfig(writerMode)
	if err != nil {
		return Result{}, fmt.Errorf("render meter config: %w", err)
	}
	if writerMode == "writer" {
		data[ReaderKey], err = makeConfig("reader")
		if err != nil {
			return Result{}, fmt.Errorf("render reader config: %w", err)
		}
	}
	sum := sha256.New()
	keys := lo.Keys(data)
	slices.Sort(keys)
	for _, key := range keys {
		_, _ = sum.Write([]byte(key))
		_, _ = sum.Write(data[key])
	}
	_, _ = sum.Write(input.InternalToken)
	return Result{Data: data, Hash: hex.EncodeToString(sum.Sum(nil))}, nil
}

func renderLine(input Input) (Result, error) {
	line := input.Line
	descriptor := resources.LineDescriptor
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, descriptor.SecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, descriptor.InternalTokenPath)
	data := map[string][]byte{}
	global := renderResolvedAccess(data, "global", input.Global, secretsPath)
	namespaceAccess := map[string]renderAccess{}
	for _, namespace := range input.Namespaces {
		prefix := lo.CoalesceOrEmpty(namespace.KeyPrefix, namespace.Name)
		rendered := renderResolvedAccess(data, "namespace-"+prefix, namespace.Access, secretsPath)
		current := lo.ValueOr(namespaceAccess, namespace.Name, emptyAccess())
		current.Read = append(current.Read, rendered.Read...)
		current.Write = append(current.Write, rendered.Write...)
		namespaceAccess[namespace.Name] = current
	}
	var jwt *renderJWT
	if input.JWT != nil {
		jwt = &renderJWT{
			Issuer: input.JWT.Issuer, Audience: input.JWT.Audience,
			RefreshIntervalSeconds: int64Value(input.JWT.RefreshIntervalSeconds, 300),
			RequestTimeoutSeconds:  int64Value(input.JWT.RequestTimeoutSeconds, 5),
		}
		switch {
		case len(input.JWT.JWKS) > 0:
			data["jwks.json"] = append([]byte(nil), input.JWT.JWKS...)
			jwt.JWKS = renderJWKS{Source: sourceFile, Path: secretsPath + "/jwks.json"}
		case input.JWT.URL != "":
			jwt.JWKS = renderJWKS{Source: "url", URL: input.JWT.URL}
		default:
			return Result{}, fmt.Errorf("auth.jwt.jwks requires url or resolved secret data")
		}
	}
	names := lo.Uniq(line.Spec.Config.Namespaces)
	if len(names) == 0 {
		names = []string{"default"}
	}
	names = lo.Uniq(append(names, lo.Keys(namespaceAccess)...))
	sort.Strings(names)
	namespaces := lo.Map(names, func(name string, _ int) renderNamespace {
		return renderNamespace{Name: name, Auth: lo.ValueOr(namespaceAccess, name, emptyAccess())}
	})
	makeConfig := func(component string) ([]byte, error) {
		spec := line.Spec.Config
		return yaml.Marshal(renderLineConfig{
			Mode: component,
			Listeners: renderListeners{
				HTTP: fmt.Sprintf("0.0.0.0:%d", resources.HTTPPort(line)),
				GRPC: fmt.Sprintf("0.0.0.0:%d", resources.GRPCPort(line)),
			},
			Storage:                renderStorageConfig(spec.Storage, descriptor),
			SegmentDurationSeconds: int64Value(spec.SegmentDurationSeconds, 3600),
			RetentionSeconds:       spec.RetentionSeconds,
			Page: renderLinePage{
				TargetSizeBytes: int64Value(spec.Page.TargetSizeBytes, 1048576),
				MaxRows:         int64Value(spec.Page.MaxRows, 8192),
				MaxAgeSeconds:   int64Value(spec.Page.MaxAgeSeconds, 5),
				RowsPerBlock:    int64Value(spec.Page.RowsPerBlock, 256),
			},
			VisibilityIntervalSeconds: int64Value(spec.VisibilityIntervalSeconds, 1),
			Write: renderLineWrite{
				Durability:        lo.CoalesceOrEmpty(string(spec.Write.Durability), string(telemetryv1alpha1.DurabilityWritten)),
				RemoteConcurrency: int32Value(spec.Write.RemoteConcurrency, 16),
				RemoteRetries:     int32Value(spec.Write.RemoteRetries, 2),
			},
			Sharding: renderShardingConfig(line.Name, line.Namespace, spec.Sharding, resources.GRPCPort(line), component),
			Request: renderLineRequest{
				MaxRequestBytes:             int64Value(spec.Request.MaxRequestBytes, 10485760),
				MaxQueryEntries:             int64Value(spec.Request.MaxQueryEntries, 5000),
				MaxQueryPages:               int64Value(spec.Request.MaxQueryPages, 10000),
				MaxStructuredMetadataFields: int64Value(spec.Request.MaxStructuredMetadataFields, 128),
				QueryConcurrency:            int32Value(spec.Request.QueryConcurrency, 8),
				MaxInFlightQueryBytes:       int64Value(spec.Request.MaxInFlightQueryBytes, 67108864),
			},
			Cache:      renderLineQueryCache{QueryEntries: int64Value(spec.Cache.QueryEntries, 256)},
			Auth:       renderAuth{Unauthenticated: line.Spec.Config.Auth.Unauthenticated, JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}},
			Namespaces: namespaces,
		})
	}
	writerMode := modeStandalone
	if resources.Mode(line) == telemetryv1alpha1.ProductModeSharded {
		writerMode = "writer"
	}
	var err error
	data[LineKey], err = makeConfig(writerMode)
	if err != nil {
		return Result{}, fmt.Errorf("render Line config: %w", err)
	}
	if writerMode == "writer" {
		data[ReaderKey], err = makeConfig("reader")
		if err != nil {
			return Result{}, fmt.Errorf("render Line reader config: %w", err)
		}
	}
	sum := sha256.New()
	keys := lo.Keys(data)
	slices.Sort(keys)
	for _, key := range keys {
		_, _ = sum.Write([]byte(key))
		_, _ = sum.Write(data[key])
	}
	_, _ = sum.Write(input.InternalToken)
	return Result{Data: data, Hash: hex.EncodeToString(sum.Sum(nil))}, nil
}

func renderResolvedAccess(data map[string][]byte, prefix string, access Access, secretsPath string) renderAccess {
	result := emptyAccess()
	for permission, credentials := range map[string][]Credential{"read": access.Read, "write": access.Write} {
		for i, credential := range credentials {
			key := credential.DataKey
			if key == "" {
				key = fmt.Sprintf("%s-%s-%d-password", prefix, permission, i)
			}
			key = uniqueDataKey(data, key)
			data[key] = append([]byte(nil), credential.Password...)
			rendered := renderCredential{Type: "basic", Username: credential.Username, Password: renderFileSecret{Source: sourceFile, Path: secretsPath + "/" + key}}
			if permission == "read" {
				result.Read = append(result.Read, rendered)
			} else {
				result.Write = append(result.Write, rendered)
			}
		}
	}
	return result
}

func emptyAccess() renderAccess {
	return renderAccess{Read: []renderCredential{}, Write: []renderCredential{}}
}

func renderStorageConfig(spec telemetryv1alpha1.StorageSpec, descriptor resources.Descriptor) renderStorage {
	objectType := lo.CoalesceOrEmpty(string(spec.ObjectStore.Type), string(telemetryv1alpha1.ObjectStoreLocal))
	objectPath := spec.ObjectStore.Path
	if objectType == string(telemetryv1alpha1.ObjectStoreLocal) {
		objectPath = lo.CoalesceOrEmpty(objectPath, descriptor.DataPath+"/data")
	}
	result := renderStorage{
		Type:         "SlateDb",
		Path:         lo.CoalesceOrEmpty(spec.Path, descriptor.Name),
		SettingsPath: spec.SettingsPath,
		ObjectStore:  renderObjectStoreConfig(spec.ObjectStore, objectType, objectPath),
	}
	if spec.BlockCache == nil {
		result.BlockCache = &renderCache{Type: string(telemetryv1alpha1.CacheFoyerHybrid), MemoryCapacity: lo.ToPtr(int64(536870912)), DiskCapacity: lo.ToPtr(int64(10737418240)), DiskPath: descriptor.CachePath}
	} else {
		result.BlockCache = renderCacheConfig(spec.BlockCache, descriptor.CachePath)
	}
	if spec.MetaCache == nil {
		result.MetaCache = &renderCache{Type: string(telemetryv1alpha1.CacheFoyerMemory), Capacity: lo.ToPtr(int64(134217728))}
	} else {
		result.MetaCache = renderCacheConfig(spec.MetaCache, descriptor.CachePath)
	}
	return result
}

func renderObjectStoreConfig(spec telemetryv1alpha1.ObjectStoreSpec, objectType, objectPath string) renderObjectStore {
	result := renderObjectStore{Type: objectType, Path: objectPath}
	switch {
	case spec.AWS != nil:
		result.Region = spec.AWS.Region
		result.Bucket = spec.AWS.Bucket
		result.Endpoint = spec.AWS.Endpoint
		result.AllowHTTP = spec.AWS.AllowHTTP
		result.VirtualHostedStyle = spec.AWS.VirtualHostedStyle
	case spec.Azure != nil:
		result.Account = spec.Azure.Account
		result.Container = spec.Azure.Container
		result.Endpoint = spec.Azure.Endpoint
		result.AllowHTTP = spec.Azure.AllowHTTP
	case spec.GCP != nil:
		result.Bucket = spec.GCP.Bucket
		result.BaseURL = spec.GCP.BaseURL
	}
	return result
}

func renderCacheConfig(spec *telemetryv1alpha1.CacheSpec, defaultCachePath string) *renderCache {
	result := &renderCache{Type: string(spec.Type), MemoryCapacity: spec.MemoryCapacity, DiskCapacity: spec.DiskCapacity, DiskPath: spec.DiskPath, Capacity: spec.Capacity, Shards: spec.Shards, WritePolicy: spec.WritePolicy, Flushers: spec.Flushers, BufferPoolSize: spec.BufferPoolSize, SubmitQueueSizeThreshold: spec.SubmitQueueSizeThreshold}
	if spec.Type == telemetryv1alpha1.CacheFoyerHybrid {
		result.MemoryCapacity = lo.CoalesceOrEmpty(result.MemoryCapacity, lo.ToPtr(int64(536870912)))
		result.DiskCapacity = lo.CoalesceOrEmpty(result.DiskCapacity, lo.ToPtr(int64(10737418240)))
		result.DiskPath = lo.CoalesceOrEmpty(result.DiskPath, defaultCachePath)
	}
	if spec.Type == telemetryv1alpha1.CacheFoyerMemory {
		result.Capacity = lo.CoalesceOrEmpty(result.Capacity, lo.ToPtr(int64(134217728)))
	}
	return result
}

func renderWriteConfig(spec telemetryv1alpha1.WriteSpec) renderWrite {
	return renderWrite{
		Durability:           lo.CoalesceOrEmpty(string(spec.Durability), string(telemetryv1alpha1.DurabilityWritten)),
		FlushIntervalSeconds: int64Value(spec.FlushIntervalSeconds, 60),
		RemoteConcurrency:    int32Value(spec.RemoteConcurrency, 16),
		RemoteRetries:        int32Value(spec.RemoteRetries, 2),
	}
}

func renderShardingConfig(name, namespace string, spec telemetryv1alpha1.ShardingSpec, grpcPort int32, component string) renderSharding {
	virtual := int32Value(spec.VirtualShards, 8)
	ioConcurrencyMultiplier := int32Value(spec.IOConcurrencyMultiplier, 4)
	if component == modeStandalone {
		return renderSharding{
			VirtualShards: virtual, IOConcurrencyMultiplier: ioConcurrencyMultiplier,
			Backend: modeStandalone,
		}
	}
	writer := resourceName(name, "writer")
	return renderSharding{
		VirtualShards: virtual, IOConcurrencyMultiplier: ioConcurrencyMultiplier,
		Backend: "kubernetes", Namespace: namespace,
		StatefulSet: writer, HeadlessService: resourceName(writer, "headless"),
		OwnerPort: grpcPort, AssignmentConfigMap: resourceName(name, "writer-shard-assignments"),
		CoordinatorLease: resourceName(name, "writer-shard-coordinator"), ShardLeasePrefix: resourceName(name, "writer-shard"),
		LeaseDurationSeconds: int64Value(spec.LeaseDurationSeconds, 15),
		RenewIntervalSeconds: int64Value(spec.RenewIntervalSeconds, 5),
	}
}

func mode(meter *telemetryv1alpha1.Meter) telemetryv1alpha1.MeterMode {
	return lo.Ternary(meter.Spec.Mode == "", telemetryv1alpha1.MeterModeStandalone, meter.Spec.Mode)
}

func httpPort(meter *telemetryv1alpha1.Meter) int32 {
	return lo.Ternary(meter.Spec.Service.HTTPPort == 0, int32(8080), meter.Spec.Service.HTTPPort)
}

func grpcPort(meter *telemetryv1alpha1.Meter) int32 {
	return lo.Ternary(meter.Spec.Service.GRPCPort == 0, int32(9090), meter.Spec.Service.GRPCPort)
}

func resourceName(base, suffix string) string {
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

func uniqueDataKey(data map[string][]byte, base string) string {
	key := resourceName(base, "")
	for i := 2; ; i++ {
		if _, found := data[key]; !found {
			return key
		}
		key = resourceName(base, fmt.Sprintf("%d", i))
	}
}

func int64Value(value *int64, fallback int64) int64 { return lo.FromPtrOr(value, fallback) }
func int32Value(value *int32, fallback int32) int32 { return lo.FromPtrOr(value, fallback) }

type renderConfig struct {
	Mode                string            `json:"mode"`
	Listeners           renderListeners   `json:"listeners"`
	PathPrefix          string            `json:"path_prefix,omitempty"`
	Storage             renderStorage     `json:"storage"`
	ReaderCacheCapacity int64             `json:"reader_cache_capacity"`
	Write               renderWrite       `json:"write"`
	Sharding            renderSharding    `json:"sharding"`
	Auth                renderAuth        `json:"auth"`
	Namespaces          []renderNamespace `json:"namespaces"`
}
type renderLineConfig struct {
	Mode                      string               `json:"mode"`
	Listeners                 renderListeners      `json:"listeners"`
	Storage                   renderStorage        `json:"storage"`
	SegmentDurationSeconds    int64                `json:"segment_duration_seconds"`
	RetentionSeconds          *int64               `json:"retention_seconds,omitempty"`
	Page                      renderLinePage       `json:"page"`
	VisibilityIntervalSeconds int64                `json:"visibility_interval_seconds"`
	Write                     renderLineWrite      `json:"write"`
	Sharding                  renderSharding       `json:"sharding"`
	Request                   renderLineRequest    `json:"request"`
	Cache                     renderLineQueryCache `json:"cache"`
	Auth                      renderAuth           `json:"auth"`
	Namespaces                []renderNamespace    `json:"namespaces"`
}
type renderLinePage struct {
	TargetSizeBytes int64 `json:"target_size_bytes"`
	MaxRows         int64 `json:"max_rows"`
	MaxAgeSeconds   int64 `json:"max_age_seconds"`
	RowsPerBlock    int64 `json:"rows_per_block"`
}
type renderLineWrite struct {
	Durability        string `json:"durability"`
	RemoteConcurrency int32  `json:"remote_concurrency"`
	RemoteRetries     int32  `json:"remote_retries"`
}
type renderLineRequest struct {
	MaxRequestBytes             int64 `json:"max_request_bytes"`
	MaxQueryEntries             int64 `json:"max_query_entries"`
	MaxQueryPages               int64 `json:"max_query_pages"`
	MaxStructuredMetadataFields int64 `json:"max_structured_metadata_fields"`
	QueryConcurrency            int32 `json:"query_concurrency"`
	MaxInFlightQueryBytes       int64 `json:"max_in_flight_query_bytes"`
}
type renderLineQueryCache struct {
	QueryEntries int64 `json:"query_entries"`
}
type renderListeners struct {
	HTTP string `json:"http"`
	GRPC string `json:"grpc"`
}
type renderStorage struct {
	Type         string            `json:"type"`
	Path         string            `json:"path"`
	ObjectStore  renderObjectStore `json:"object_store"`
	SettingsPath string            `json:"settings_path,omitempty"`
	BlockCache   *renderCache      `json:"block_cache,omitempty"`
	MetaCache    *renderCache      `json:"meta_cache,omitempty"`
}
type renderObjectStore struct {
	Type               string `json:"type"`
	Path               string `json:"path,omitempty"`
	Region             string `json:"region,omitempty"`
	Bucket             string `json:"bucket,omitempty"`
	Endpoint           string `json:"endpoint,omitempty"`
	AllowHTTP          bool   `json:"allow_http,omitempty"`
	VirtualHostedStyle bool   `json:"virtual_hosted_style,omitempty"`
	Account            string `json:"account,omitempty"`
	Container          string `json:"container,omitempty"`
	BaseURL            string `json:"base_url,omitempty"`
}
type renderCache struct {
	Type                     string `json:"type"`
	MemoryCapacity           *int64 `json:"memory_capacity,omitempty"`
	DiskCapacity             *int64 `json:"disk_capacity,omitempty"`
	DiskPath                 string `json:"disk_path,omitempty"`
	Capacity                 *int64 `json:"capacity,omitempty"`
	Shards                   *int32 `json:"shards,omitempty"`
	WritePolicy              string `json:"write_policy,omitempty"`
	Flushers                 *int32 `json:"flushers,omitempty"`
	BufferPoolSize           *int64 `json:"buffer_pool_size,omitempty"`
	SubmitQueueSizeThreshold *int64 `json:"submit_queue_size_threshold,omitempty"`
}
type renderWrite struct {
	Durability           string `json:"durability"`
	FlushIntervalSeconds int64  `json:"flush_interval_seconds"`
	RemoteConcurrency    int32  `json:"remote_concurrency"`
	RemoteRetries        int32  `json:"remote_retries"`
}
type renderSharding struct {
	VirtualShards           int32  `json:"virtual_shards"`
	IOConcurrencyMultiplier int32  `json:"io_concurrency_multiplier"`
	Backend                 string `json:"backend"`
	Namespace               string `json:"namespace,omitempty"`
	StatefulSet             string `json:"stateful_set,omitempty"`
	HeadlessService         string `json:"headless_service,omitempty"`
	OwnerPort               int32  `json:"owner_port,omitempty"`
	AssignmentConfigMap     string `json:"assignment_config_map,omitempty"`
	CoordinatorLease        string `json:"coordinator_lease,omitempty"`
	ShardLeasePrefix        string `json:"shard_lease_prefix,omitempty"`
	LeaseDurationSeconds    int64  `json:"lease_duration_seconds,omitempty"`
	RenewIntervalSeconds    int64  `json:"renew_interval_seconds,omitempty"`
}
type renderFileSecret struct {
	Source string `json:"source"`
	Path   string `json:"path"`
}
type renderJWKS struct {
	Source string `json:"source"`
	Path   string `json:"path,omitempty"`
	URL    string `json:"url,omitempty"`
}
type renderJWT struct {
	JWKS                   renderJWKS `json:"jwks"`
	Issuer                 string     `json:"issuer,omitempty"`
	Audience               string     `json:"audience,omitempty"`
	RefreshIntervalSeconds int64      `json:"refresh_interval_seconds"`
	RequestTimeoutSeconds  int64      `json:"request_timeout_seconds"`
}
type renderCredential struct {
	Type     string           `json:"type"`
	Username string           `json:"username"`
	Password renderFileSecret `json:"password"`
}
type renderAccess struct {
	Read  []renderCredential `json:"read"`
	Write []renderCredential `json:"write"`
}
type renderAuth struct {
	Unauthenticated bool              `json:"unauthenticated"`
	JWT             *renderJWT        `json:"jwt,omitempty"`
	Global          renderAccess      `json:"global"`
	Internal        *renderFileSecret `json:"internal,omitempty"`
}
type renderNamespace struct {
	Name string       `json:"name"`
	Auth renderAccess `json:"auth"`
}
