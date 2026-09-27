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
)

const (
	MeterKey  = "meter.yaml"
	ReaderKey = "reader.yaml"

	defaultSecretsPath = "/etc/meter/secrets"
	defaultDataPath    = "/var/lib/meter"
	defaultCachePath   = "/var/cache/meter"
	sourceFile         = "file"
	modeStandalone     = "standalone"
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
	if input.Meter == nil {
		return Result{}, fmt.Errorf("meter is required")
	}
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, defaultSecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, "/var/run/secrets/meter/internal-token")
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
			Storage:             renderStorageConfig(input.Meter.Spec.Config.Storage),
			ReaderCacheCapacity: int64Value(input.Meter.Spec.Config.ReaderCacheCapacity, 268435456),
			Write:               renderWriteConfig(input.Meter.Spec.Config.Write),
			Sharding:            renderShardingConfig(input.Meter, mode),
			Auth:                renderAuth{JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}},
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

func renderStorageConfig(spec telemetryv1alpha1.StorageSpec) renderStorage {
	objectType := lo.CoalesceOrEmpty(string(spec.ObjectStore.Type), string(telemetryv1alpha1.ObjectStoreLocal))
	objectPath := spec.ObjectStore.Path
	if objectType == string(telemetryv1alpha1.ObjectStoreLocal) {
		objectPath = lo.CoalesceOrEmpty(objectPath, defaultDataPath+"/data")
	}
	result := renderStorage{
		Path:         lo.CoalesceOrEmpty(spec.Path, "meter"),
		SettingsPath: spec.SettingsPath,
		ObjectStore:  renderObjectStore{Type: objectType, Path: objectPath, Region: spec.ObjectStore.Region, Bucket: spec.ObjectStore.Bucket},
	}
	if spec.BlockCache == nil {
		result.BlockCache = &renderCache{Type: string(telemetryv1alpha1.CacheFoyerHybrid), MemoryCapacity: lo.ToPtr(int64(536870912)), DiskCapacity: lo.ToPtr(int64(10737418240)), DiskPath: defaultCachePath}
	} else {
		result.BlockCache = renderCacheConfig(spec.BlockCache)
	}
	if spec.MetaCache == nil {
		result.MetaCache = &renderCache{Type: string(telemetryv1alpha1.CacheFoyerMemory), Capacity: lo.ToPtr(int64(134217728))}
	} else {
		result.MetaCache = renderCacheConfig(spec.MetaCache)
	}
	return result
}

func renderCacheConfig(spec *telemetryv1alpha1.CacheSpec) *renderCache {
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

func renderShardingConfig(meter *telemetryv1alpha1.Meter, component string) renderSharding {
	virtual := int32Value(meter.Spec.Config.Sharding.VirtualShards, 64)
	if component == modeStandalone {
		return renderSharding{VirtualShards: virtual, Backend: modeStandalone}
	}
	writer := resourceName(meter.Name, "writer")
	return renderSharding{
		VirtualShards: virtual, Backend: "kubernetes", Namespace: meter.Namespace,
		StatefulSet: writer, HeadlessService: resourceName(writer, "headless"),
		OwnerPort: grpcPort(meter), AssignmentConfigMap: resourceName(meter.Name, "writer-shard-assignments"),
		CoordinatorLease: resourceName(meter.Name, "writer-shard-coordinator"), ShardLeasePrefix: resourceName(meter.Name, "writer-shard"),
		LeaseDurationSeconds:     int64Value(meter.Spec.Config.Sharding.LeaseDurationSeconds, 15),
		RenewIntervalSeconds:     int64Value(meter.Spec.Config.Sharding.RenewIntervalSeconds, 5),
		WatchPollIntervalSeconds: int64Value(meter.Spec.Config.Sharding.WatchPollIntervalSeconds, 2),
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
	Storage             renderStorage     `json:"storage"`
	ReaderCacheCapacity int64             `json:"reader_cache_capacity"`
	Write               renderWrite       `json:"write"`
	Sharding            renderSharding    `json:"sharding"`
	Auth                renderAuth        `json:"auth"`
	Namespaces          []renderNamespace `json:"namespaces"`
}
type renderListeners struct {
	HTTP string `json:"http"`
	GRPC string `json:"grpc"`
}
type renderStorage struct {
	Path         string            `json:"path"`
	ObjectStore  renderObjectStore `json:"object_store"`
	SettingsPath string            `json:"settings_path,omitempty"`
	BlockCache   *renderCache      `json:"block_cache,omitempty"`
	MetaCache    *renderCache      `json:"meta_cache,omitempty"`
}
type renderObjectStore struct {
	Type   string `json:"type"`
	Path   string `json:"path,omitempty"`
	Region string `json:"region,omitempty"`
	Bucket string `json:"bucket,omitempty"`
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
	VirtualShards            int32  `json:"virtual_shards"`
	Backend                  string `json:"backend"`
	Namespace                string `json:"namespace,omitempty"`
	StatefulSet              string `json:"stateful_set,omitempty"`
	HeadlessService          string `json:"headless_service,omitempty"`
	OwnerPort                int32  `json:"owner_port,omitempty"`
	AssignmentConfigMap      string `json:"assignment_config_map,omitempty"`
	CoordinatorLease         string `json:"coordinator_lease,omitempty"`
	ShardLeasePrefix         string `json:"shard_lease_prefix,omitempty"`
	LeaseDurationSeconds     int64  `json:"lease_duration_seconds,omitempty"`
	RenewIntervalSeconds     int64  `json:"renew_interval_seconds,omitempty"`
	WatchPollIntervalSeconds int64  `json:"watch_poll_interval_seconds,omitempty"`
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
	JWT      *renderJWT        `json:"jwt,omitempty"`
	Global   renderAccess      `json:"global"`
	Internal *renderFileSecret `json:"internal,omitempty"`
}
type renderNamespace struct {
	Name string       `json:"name"`
	Auth renderAccess `json:"auth"`
}
