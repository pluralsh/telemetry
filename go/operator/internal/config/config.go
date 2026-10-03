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
	MetricsKey  = "metrics.yaml"
	LogsKey     = "logs.yaml"
	TracesKey   = "traces.yaml"
	PseudoFSKey = "pseudofs.yaml"
	ReaderKey   = "reader.yaml"

	sourceFile       = "file"
	sourceURL        = "url"
	defaultNamespace = "default"
	modeStandalone   = "standalone"
	modeWriter       = "writer"
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
	Name                   string
	KeyPrefix              string
	Access                 Access
	UsageReportingEndpoint string
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
	Metrics           *telemetryv1alpha1.Metrics
	Logs              *telemetryv1alpha1.Logs
	Traces            *telemetryv1alpha1.Traces
	PseudoFS          *telemetryv1alpha1.PseudoFS
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
	if input.PseudoFS != nil {
		return renderPseudoFS(input.PseudoFS)
	}
	if input.Traces != nil {
		return renderTraces(input)
	}
	if input.Logs != nil {
		return renderLogs(input)
	}
	if input.Metrics == nil {
		return Result{}, fmt.Errorf("metrics, logs, traces, or pseudofs is required")
	}
	descriptor := resources.MetricsDescriptor
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, descriptor.SecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, descriptor.InternalTokenPath)
	data := map[string][]byte{}
	global := renderResolvedAccess(data, "global", input.Global, secretsPath)
	namespaces := renderNamespaces(data, input.Namespaces, input.Metrics.Spec.Config.Namespaces, secretsPath)

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
			jwt.JWKS = renderJWKS{Source: sourceURL, URL: input.JWT.URL}
		default:
			return Result{}, fmt.Errorf("auth.jwt.jwks requires url or resolved secret data")
		}
	}

	retention, err := renderRetention(input.Metrics.Spec.Config.Retention, nil)
	if err != nil {
		return Result{}, err
	}
	makeConfig := func(mode string) ([]byte, error) {
		return yaml.Marshal(renderConfig{
			Mode:                mode,
			Listeners:           renderListeners{HTTP: fmt.Sprintf("0.0.0.0:%d", httpPort(input.Metrics)), GRPC: fmt.Sprintf("0.0.0.0:%d", grpcPort(input.Metrics))},
			PathPrefix:          input.Metrics.Spec.Ingress.PathPrefix,
			Storage:             renderStorageConfigForMode(input.Metrics.Spec.Config.Storage, descriptor, mode),
			RetentionSeconds:    retention,
			ReaderCacheCapacity: int64Value(input.Metrics.Spec.Config.ReaderCacheCapacity, 268435456),
			CacheWarmer:         renderCacheWarmerConfig(input.Metrics.Spec.Config.CacheWarmer),
			Write:               renderWriteConfig(input.Metrics.Spec.Config.Write),
			Request: renderMetricsRequest{
				MaxRequestBytes:        int64Value(input.Metrics.Spec.Config.Request.MaxRequestBytes, defaultMaxRequestBytes),
				MaxDecodedRequestBytes: int64Value(input.Metrics.Spec.Config.Request.MaxDecodedRequestBytes, defaultMaxDecodedRequestBytes),
			},
			Sharding:   renderShardingConfig(input.Metrics.Name, input.Metrics.Namespace, input.Metrics.Spec.Config.Sharding, grpcPort(input.Metrics), mode),
			Auth:       renderAuth{Unauthenticated: input.Metrics.Spec.Config.Auth.Unauthenticated, JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}},
			Namespaces: namespaces,
		})
	}
	writerMode := modeStandalone
	if mode(input.Metrics) == telemetryv1alpha1.MetricsModeSharded {
		writerMode = modeWriter
	}
	data[MetricsKey], err = makeConfig(writerMode)
	if err != nil {
		return Result{}, fmt.Errorf("render metrics config: %w", err)
	}
	if writerMode == modeWriter {
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

func renderPseudoFS(pseudofs *telemetryv1alpha1.PseudoFS) (Result, error) {
	spec := pseudofs.Spec.Config
	rendered, err := yaml.Marshal(renderPseudoFSConfig{
		Listener: fmt.Sprintf("0.0.0.0:%d", resources.GRPCPort(pseudofs)),
		Filesystem: renderPseudoFSFilesystem{
			Storage:              renderStorageConfig(spec.Storage, resources.PseudoFSDescriptor),
			ChunkSizeBytes:       int64Value(spec.ChunkSizeBytes, 1048576),
			MaxFileSizeBytes:     int64Value(spec.MaxFileSizeBytes, 1073741824),
			MaxAppendGenerations: int64Value(spec.MaxAppendGenerations, 64),
		},
		MaxUnaryFileSizeBytes:   int64Value(spec.MaxUnaryFileSizeBytes, 8388608),
		MaxDecodingMessageBytes: int64Value(spec.MaxDecodingMessageBytes, 16777216),
		MaxEncodingMessageBytes: int64Value(spec.MaxEncodingMessageBytes, 16777216),
	})
	if err != nil {
		return Result{}, fmt.Errorf("render PseudoFS config: %w", err)
	}
	sum := sha256.Sum256(rendered)
	return Result{Data: map[string][]byte{PseudoFSKey: rendered}, Hash: hex.EncodeToString(sum[:])}, nil
}

func renderLogs(input Input) (Result, error) {
	logs := input.Logs
	descriptor := resources.LogsDescriptor
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, descriptor.SecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, descriptor.InternalTokenPath)
	data := map[string][]byte{}
	global := renderResolvedAccess(data, "global", input.Global, secretsPath)
	namespaces := renderNamespaces(data, input.Namespaces, logs.Spec.Config.Namespaces, secretsPath)
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
			jwt.JWKS = renderJWKS{Source: sourceURL, URL: input.JWT.URL}
		default:
			return Result{}, fmt.Errorf("auth.jwt.jwks requires url or resolved secret data")
		}
	}
	retention, err := renderRetention(logs.Spec.Config.Retention, logs.Spec.Config.RetentionSeconds)
	if err != nil {
		return Result{}, err
	}
	makeConfig := func(component string) ([]byte, error) {
		spec := logs.Spec.Config
		return yaml.Marshal(renderLogsConfig{
			Mode:       component,
			PathPrefix: logs.Spec.Ingress.PathPrefix,
			Listeners: renderListeners{
				HTTP: fmt.Sprintf("0.0.0.0:%d", resources.HTTPPort(logs)),
				GRPC: fmt.Sprintf("0.0.0.0:%d", resources.GRPCPort(logs)),
			},
			Storage:                renderStorageConfigForMode(spec.Storage, descriptor, component),
			SegmentDurationSeconds: int64Value(spec.SegmentDurationSeconds, 3600),
			RetentionSeconds:       retention,
			Page: renderLogsPage{
				TargetSizeBytes: int64Value(spec.Page.TargetSizeBytes, 1048576),
				MaxRows:         int64Value(spec.Page.MaxRows, 8192),
				RowsPerBlock:    int64Value(spec.Page.RowsPerBlock, 256),
			},
			Write: renderWrite{
				Durability:                      lo.CoalesceOrEmpty(string(spec.Write.Durability), string(telemetryv1alpha1.DurabilityApplied)),
				FlushIntervalSeconds:            int64Value(spec.Write.FlushIntervalSeconds, 10),
				BufferQueueCapacity:             int32Value(spec.Write.BufferQueueCapacity, 10000),
				BufferFlushIntervalMilliseconds: int64Value(spec.Write.BufferFlushIntervalMilliseconds, 10000),
				BufferSizeThresholdBytes:        int64Value(spec.Write.BufferSizeThresholdBytes, 67108864),
				RemoteConcurrency:               int32Value(spec.Write.RemoteConcurrency, 16),
				RemoteRetries:                   int32Value(spec.Write.RemoteRetries, 2),
			},
			Sharding: renderShardingConfig(logs.Name, logs.Namespace, spec.Sharding, resources.GRPCPort(logs), component),
			Request: renderLogsRequest{
				MaxRequestBytes:             int64Value(spec.Request.MaxRequestBytes, defaultMaxRequestBytes),
				MaxDecodedRequestBytes:      int64Value(spec.Request.MaxDecodedRequestBytes, defaultMaxDecodedRequestBytes),
				MaxQueryEntries:             int64Value(spec.Request.MaxQueryEntries, 5000),
				MaxQueryPages:               int64Value(spec.Request.MaxQueryPages, 10000),
				MaxStructuredMetadataFields: int64Value(spec.Request.MaxStructuredMetadataFields, 128),
				QueryConcurrency:            int32Value(spec.Request.QueryConcurrency, 16),
				MaxInFlightQueryBytes:       int64Value(spec.Request.MaxInFlightQueryBytes, 134217728),
			},
			Elasticsearch: renderElasticsearch{
				MessageFields: lo.Ternary(spec.Elasticsearch.MessageFields == nil, []string{"message", "log", "msg"}, spec.Elasticsearch.MessageFields),
				TimeField:     lo.CoalesceOrEmpty(spec.Elasticsearch.TimeField, "@timestamp"),
				StreamFields:  lo.Ternary(spec.Elasticsearch.StreamFields == nil, []string{}, spec.Elasticsearch.StreamFields),
			},
			CacheWarmer: renderCacheWarmerConfig(spec.CacheWarmer),
			Auth:        renderAuth{Unauthenticated: logs.Spec.Config.Auth.Unauthenticated, JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}},
			Namespaces:  namespaces,
		})
	}
	writerMode := modeStandalone
	if resources.Mode(logs) == telemetryv1alpha1.ProductModeSharded {
		writerMode = modeWriter
	}
	data[LogsKey], err = makeConfig(writerMode)
	if err != nil {
		return Result{}, fmt.Errorf("render Logs config: %w", err)
	}
	if writerMode == modeWriter {
		data[ReaderKey], err = makeConfig("reader")
		if err != nil {
			return Result{}, fmt.Errorf("render Logs reader config: %w", err)
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

func renderTraces(input Input) (Result, error) {
	traces := input.Traces
	descriptor := resources.TracesDescriptor
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, descriptor.SecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, descriptor.InternalTokenPath)
	data := map[string][]byte{}
	global := renderResolvedAccess(data, "global", input.Global, secretsPath)
	namespaces := renderNamespaces(data, input.Namespaces, traces.Spec.Config.Namespaces, secretsPath)
	var jwt *renderJWT
	if input.JWT != nil {
		jwt = &renderJWT{Issuer: input.JWT.Issuer, Audience: input.JWT.Audience, RefreshIntervalSeconds: int64Value(input.JWT.RefreshIntervalSeconds, 300), RequestTimeoutSeconds: int64Value(input.JWT.RequestTimeoutSeconds, 5)}
		switch {
		case len(input.JWT.JWKS) > 0:
			data["jwks.json"] = append([]byte(nil), input.JWT.JWKS...)
			jwt.JWKS = renderJWKS{Source: sourceFile, Path: secretsPath + "/jwks.json"}
		case input.JWT.URL != "":
			jwt.JWKS = renderJWKS{Source: sourceURL, URL: input.JWT.URL}
		default:
			return Result{}, fmt.Errorf("auth.jwt.jwks requires url or resolved secret data")
		}
	}
	retention, err := renderRetention(traces.Spec.Config.Retention, traces.Spec.Config.RetentionSeconds)
	if err != nil {
		return Result{}, err
	}
	makeConfig := func(component string) ([]byte, error) {
		spec := traces.Spec.Config
		return yaml.Marshal(renderTracesConfig{
			Mode:       component,
			PathPrefix: traces.Spec.Ingress.PathPrefix,
			Listeners: renderListeners{
				HTTP:     fmt.Sprintf("0.0.0.0:%d", resources.HTTPPort(traces)),
				GRPC:     fmt.Sprintf("0.0.0.0:%d", resources.GRPCPort(traces)),
				OTLPGRPC: "0.0.0.0:4317", JaegerGRPC: "0.0.0.0:14250",
			},
			Storage:                renderStorageConfigForMode(spec.Storage, descriptor, component),
			SegmentDurationSeconds: int64Value(spec.SegmentDurationSeconds, 3600),
			RetentionSeconds:       retention,
			Page:                   renderTracesPage{TargetSizeBytes: int64Value(spec.Page.TargetSizeBytes, 1048576), MaxSizeBytes: int64Value(spec.Page.MaxSizeBytes, 4194304), MaxTraces: int64Value(spec.Page.MaxTraces, 1024)},
			Write: renderWrite{
				Durability:                      lo.CoalesceOrEmpty(string(spec.Write.Durability), string(telemetryv1alpha1.DurabilityApplied)),
				FlushIntervalSeconds:            int64Value(spec.Write.FlushIntervalSeconds, 10),
				BufferQueueCapacity:             int32Value(spec.Write.BufferQueueCapacity, 10000),
				BufferFlushIntervalMilliseconds: int64Value(spec.Write.BufferFlushIntervalMilliseconds, 10000),
				BufferSizeThresholdBytes:        int64Value(spec.Write.BufferSizeThresholdBytes, 67108864),
				RemoteConcurrency:               int32Value(spec.Write.RemoteConcurrency, 16),
				RemoteRetries:                   int32Value(spec.Write.RemoteRetries, 2),
			},
			Sharding:    renderShardingConfig(traces.Name, traces.Namespace, spec.Sharding, resources.GRPCPort(traces), component),
			Request:     renderTracesRequest{MaxRequestBytes: int64Value(spec.Request.MaxRequestBytes, defaultMaxRequestBytes), MaxDecodedRequestBytes: int64Value(spec.Request.MaxDecodedRequestBytes, defaultMaxDecodedRequestBytes), RequestConcurrency: int32Value(spec.Request.RequestConcurrency, 64), QueryConcurrency: int32Value(spec.Request.QueryConcurrency, 8), MaxCandidates: int64Value(spec.Request.MaxCandidates, 10000), MaxSpansPerTrace: int64Value(spec.Request.MaxSpansPerTrace, 100000), MaxQueryLimit: int64Value(spec.Request.MaxQueryLimit, 1000)},
			CacheWarmer: renderCacheWarmerConfig(spec.CacheWarmer),
			Auth:        renderAuth{Unauthenticated: spec.Auth.Unauthenticated, JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}},
			Namespaces:  namespaces,
		})
	}
	writerMode := modeStandalone
	if resources.Mode(traces) == telemetryv1alpha1.ProductModeSharded {
		writerMode = modeWriter
	}
	data[TracesKey], err = makeConfig(writerMode)
	if err != nil {
		return Result{}, fmt.Errorf("render Traces config: %w", err)
	}
	if writerMode == modeWriter {
		data[ReaderKey], err = makeConfig("reader")
		if err != nil {
			return Result{}, fmt.Errorf("render Traces reader config: %w", err)
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

func renderStorageConfigForMode(spec telemetryv1alpha1.StorageSpec, descriptor resources.Descriptor, mode string) renderStorage {
	result := renderStorageConfig(spec, descriptor)
	if mode == modeWriter && spec.BlockCache == nil {
		result.BlockCache = nil
	}
	return result
}

func renderCacheWarmerConfig(spec *telemetryv1alpha1.CacheWarmerSpec) renderCacheWarmer {
	if spec == nil {
		return renderCacheWarmer{Enabled: false, WarmRangeSeconds: 7200, TimeoutSeconds: 30, Concurrency: 2, IncludePayloads: false}
	}
	return renderCacheWarmer{
		Enabled:          boolValue(spec.Enabled, false),
		WarmRangeSeconds: int64Value(spec.WarmRangeSeconds, 7200),
		TimeoutSeconds:   int64Value(spec.TimeoutSeconds, 30),
		Concurrency:      int32Value(spec.Concurrency, 2),
		IncludePayloads:  boolValue(spec.IncludePayloads, false),
	}
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
		Durability:                      lo.CoalesceOrEmpty(string(spec.Durability), string(telemetryv1alpha1.DurabilityApplied)),
		FlushIntervalSeconds:            int64Value(spec.FlushIntervalSeconds, 10),
		BufferQueueCapacity:             int32Value(spec.BufferQueueCapacity, 10000),
		BufferFlushIntervalMilliseconds: int64Value(spec.BufferFlushIntervalMilliseconds, 10000),
		BufferSizeThresholdBytes:        int64Value(spec.BufferSizeThresholdBytes, 67108864),
		RemoteConcurrency:               int32Value(spec.RemoteConcurrency, 16),
		RemoteRetries:                   int32Value(spec.RemoteRetries, 2),
	}
}

func renderShardingConfig(name, namespace string, spec telemetryv1alpha1.ShardingSpec, grpcPort int32, component string) renderSharding {
	ioConcurrencyLimit := int32Value(spec.IOConcurrencyLimit, 128)
	if component == modeStandalone {
		shards := int32(1)
		return renderSharding{
			Shards: &shards, IOConcurrencyLimit: ioConcurrencyLimit,
			Backend: modeStandalone,
		}
	}
	writer := resourceName(name, modeWriter)
	return renderSharding{
		IOConcurrencyLimit: ioConcurrencyLimit,
		Backend:            "kubernetes", Database: resourceName(name, ""), Namespace: namespace,
		StatefulSet: writer, HeadlessService: resourceName(writer, "headless"),
		OwnerPort: grpcPort, ShardMap: resourceName(name, "writer-shard-map"),
		CoordinatorLease: resourceName(name, "writer-shard-coordinator"), ShardLeasePrefix: resourceName(name, "writer-shard"),
		LeaseDurationSeconds: int64Value(spec.LeaseDurationSeconds, 15),
		RenewIntervalSeconds: int64Value(spec.RenewIntervalSeconds, 5),
	}
}

func mode(metrics *telemetryv1alpha1.Metrics) telemetryv1alpha1.MetricsMode {
	return lo.Ternary(metrics.Spec.Mode == "", telemetryv1alpha1.MetricsModeStandalone, metrics.Spec.Mode)
}

func httpPort(metrics *telemetryv1alpha1.Metrics) int32 {
	return lo.Ternary(metrics.Spec.Service.HTTPPort == 0, int32(8080), metrics.Spec.Service.HTTPPort)
}

func grpcPort(metrics *telemetryv1alpha1.Metrics) int32 {
	return lo.Ternary(metrics.Spec.Service.GRPCPort == 0, int32(9090), metrics.Spec.Service.GRPCPort)
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

const (
	defaultMaxRequestBytes        = 33554432
	defaultMaxDecodedRequestBytes = 134217728
)

func int64Value(value *int64, fallback int64) int64 { return lo.FromPtrOr(value, fallback) }
func int32Value(value *int32, fallback int32) int32 { return lo.FromPtrOr(value, fallback) }
func boolValue(value *bool, fallback bool) bool     { return lo.FromPtrOr(value, fallback) }

type renderConfig struct {
	Mode                string               `json:"mode"`
	Listeners           renderListeners      `json:"listeners"`
	PathPrefix          string               `json:"path_prefix,omitempty"`
	Storage             renderStorage        `json:"storage"`
	RetentionSeconds    *int64               `json:"retention_seconds,omitempty"`
	ReaderCacheCapacity int64                `json:"reader_cache_capacity"`
	CacheWarmer         renderCacheWarmer    `json:"cache_warmer"`
	Write               renderWrite          `json:"write"`
	Request             renderMetricsRequest `json:"request"`
	Sharding            renderSharding       `json:"sharding"`
	Auth                renderAuth           `json:"auth"`
	Namespaces          []renderNamespace    `json:"namespaces"`
}
type renderMetricsRequest struct {
	MaxRequestBytes        int64 `json:"max_request_bytes"`
	MaxDecodedRequestBytes int64 `json:"max_decoded_request_bytes"`
}
type renderLogsConfig struct {
	Mode                   string              `json:"mode"`
	Listeners              renderListeners     `json:"listeners"`
	PathPrefix             string              `json:"path_prefix,omitempty"`
	Storage                renderStorage       `json:"storage"`
	SegmentDurationSeconds int64               `json:"segment_duration_seconds"`
	RetentionSeconds       *int64              `json:"retention_seconds,omitempty"`
	Page                   renderLogsPage      `json:"page"`
	Write                  renderWrite         `json:"write"`
	Sharding               renderSharding      `json:"sharding"`
	Request                renderLogsRequest   `json:"request"`
	Elasticsearch          renderElasticsearch `json:"elasticsearch"`
	CacheWarmer            renderCacheWarmer   `json:"cache_warmer"`
	Auth                   renderAuth          `json:"auth"`
	Namespaces             []renderNamespace   `json:"namespaces"`
}
type renderElasticsearch struct {
	MessageFields []string `json:"message_fields"`
	TimeField     string   `json:"time_field"`
	StreamFields  []string `json:"stream_fields"`
}
type renderTracesConfig struct {
	Mode                   string              `json:"mode"`
	Listeners              renderListeners     `json:"listeners"`
	PathPrefix             string              `json:"path_prefix,omitempty"`
	Storage                renderStorage       `json:"storage"`
	SegmentDurationSeconds int64               `json:"segment_duration_seconds"`
	RetentionSeconds       *int64              `json:"retention_seconds,omitempty"`
	Page                   renderTracesPage    `json:"page"`
	Write                  renderWrite         `json:"write"`
	Sharding               renderSharding      `json:"sharding"`
	Request                renderTracesRequest `json:"request"`
	CacheWarmer            renderCacheWarmer   `json:"cache_warmer"`
	Auth                   renderAuth          `json:"auth"`
	Namespaces             []renderNamespace   `json:"namespaces"`
}
type renderPseudoFSConfig struct {
	Listener                string                   `json:"listener"`
	Filesystem              renderPseudoFSFilesystem `json:"filesystem"`
	MaxUnaryFileSizeBytes   int64                    `json:"max_unary_file_size_bytes"`
	MaxDecodingMessageBytes int64                    `json:"max_decoding_message_bytes"`
	MaxEncodingMessageBytes int64                    `json:"max_encoding_message_bytes"`
}
type renderPseudoFSFilesystem struct {
	Storage              renderStorage `json:"storage"`
	ChunkSizeBytes       int64         `json:"chunk_size_bytes"`
	MaxFileSizeBytes     int64         `json:"max_file_size_bytes"`
	MaxAppendGenerations int64         `json:"max_append_generations"`
}
type renderTracesPage struct {
	TargetSizeBytes int64 `json:"target_size_bytes"`
	MaxSizeBytes    int64 `json:"max_size_bytes"`
	MaxTraces       int64 `json:"max_traces"`
}
type renderTracesRequest struct {
	MaxRequestBytes        int64 `json:"max_request_bytes"`
	MaxDecodedRequestBytes int64 `json:"max_decoded_request_bytes"`
	RequestConcurrency     int32 `json:"request_concurrency"`
	QueryConcurrency       int32 `json:"query_concurrency"`
	MaxCandidates          int64 `json:"max_candidates"`
	MaxSpansPerTrace       int64 `json:"max_spans_per_trace"`
	MaxQueryLimit          int64 `json:"max_query_limit"`
}
type renderLogsPage struct {
	TargetSizeBytes int64 `json:"target_size_bytes"`
	MaxRows         int64 `json:"max_rows"`
	RowsPerBlock    int64 `json:"rows_per_block"`
}
type renderLogsRequest struct {
	MaxRequestBytes             int64 `json:"max_request_bytes"`
	MaxDecodedRequestBytes      int64 `json:"max_decoded_request_bytes"`
	MaxQueryEntries             int64 `json:"max_query_entries"`
	MaxQueryPages               int64 `json:"max_query_pages"`
	MaxStructuredMetadataFields int64 `json:"max_structured_metadata_fields"`
	QueryConcurrency            int32 `json:"query_concurrency"`
	MaxInFlightQueryBytes       int64 `json:"max_in_flight_query_bytes"`
}
type renderCacheWarmer struct {
	Enabled          bool  `json:"enabled"`
	WarmRangeSeconds int64 `json:"warm_range_seconds"`
	TimeoutSeconds   int64 `json:"timeout_seconds"`
	Concurrency      int32 `json:"concurrency"`
	IncludePayloads  bool  `json:"include_payloads"`
}
type renderListeners struct {
	HTTP       string `json:"http"`
	GRPC       string `json:"grpc"`
	OTLPGRPC   string `json:"otlp_grpc,omitempty"`
	JaegerGRPC string `json:"jaeger_grpc,omitempty"`
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
	Durability                      string `json:"durability"`
	FlushIntervalSeconds            int64  `json:"flush_interval_seconds"`
	BufferQueueCapacity             int32  `json:"buffer_queue_capacity"`
	BufferFlushIntervalMilliseconds int64  `json:"buffer_flush_interval_milliseconds"`
	BufferSizeThresholdBytes        int64  `json:"buffer_size_threshold_bytes"`
	RemoteConcurrency               int32  `json:"remote_concurrency"`
	RemoteRetries                   int32  `json:"remote_retries"`
}
type renderSharding struct {
	Shards               *int32 `json:"shards,omitempty"`
	IOConcurrencyLimit   int32  `json:"io_concurrency_limit"`
	Backend              string `json:"backend"`
	Database             string `json:"database,omitempty"`
	Namespace            string `json:"namespace,omitempty"`
	StatefulSet          string `json:"stateful_set,omitempty"`
	HeadlessService      string `json:"headless_service,omitempty"`
	OwnerPort            int32  `json:"owner_port,omitempty"`
	ShardMap             string `json:"shard_map,omitempty"`
	CoordinatorLease     string `json:"coordinator_lease,omitempty"`
	ShardLeasePrefix     string `json:"shard_lease_prefix,omitempty"`
	LeaseDurationSeconds int64  `json:"lease_duration_seconds,omitempty"`
	RenewIntervalSeconds int64  `json:"renew_interval_seconds,omitempty"`
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
	Name                   string       `json:"name"`
	Auth                   renderAccess `json:"auth"`
	UsageReportingEndpoint string       `json:"usage_reporting_endpoint,omitempty"`
}

// renderNamespaces merges NamespaceAuthentication-derived access with the
// datastore's configured namespace names. Conflicting usage reporting
// endpoints resolve to the lexicographically smallest so output is stable.
func renderNamespaces(data map[string][]byte, access []NamespaceAccess, configured []string, secretsPath string) []renderNamespace {
	namespaceAccess := map[string]renderAccess{}
	endpoints := map[string]string{}
	for _, namespace := range access {
		prefix := lo.CoalesceOrEmpty(namespace.KeyPrefix, namespace.Name)
		rendered := renderResolvedAccess(data, "namespace-"+prefix, namespace.Access, secretsPath)
		current := lo.ValueOr(namespaceAccess, namespace.Name, emptyAccess())
		current.Read = append(current.Read, rendered.Read...)
		current.Write = append(current.Write, rendered.Write...)
		namespaceAccess[namespace.Name] = current
		if endpoint := namespace.UsageReportingEndpoint; endpoint != "" {
			if existing, found := endpoints[namespace.Name]; !found || endpoint < existing {
				endpoints[namespace.Name] = endpoint
			}
		}
	}
	names := lo.Uniq(configured)
	if len(names) == 0 {
		names = []string{defaultNamespace}
	}
	names = lo.Uniq(append(names, lo.Keys(namespaceAccess)...))
	sort.Strings(names)
	return lo.Map(names, func(name string, _ int) renderNamespace {
		return renderNamespace{Name: name, Auth: lo.ValueOr(namespaceAccess, name, emptyAccess()), UsageReportingEndpoint: endpoints[name]}
	})
}
