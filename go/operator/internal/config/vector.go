package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/samber/lo"
	"sigs.k8s.io/yaml"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

const VectorKey = "vector.yaml"

type renderVectorConfig struct {
	Storage               renderStorage           `json:"storage"`
	Role                  string                  `json:"role"`
	PathPrefix            string                  `json:"path_prefix,omitempty"`
	FlushInterval         int64                   `json:"flush_interval"`
	ReaderRefreshInterval int64                   `json:"reader_refresh_interval"`
	Index                 renderVectorIndex       `json:"index,omitempty"`
	Kubernetes            *renderVectorKubernetes `json:"kubernetes,omitempty"`
	Auth                  renderAuth              `json:"auth"`
	Namespaces            []renderNamespace       `json:"namespaces"`
	Embedding             *renderVectorEmbedding  `json:"embedding,omitempty"`
	Memory                renderVectorMemory      `json:"memory"`
}

type renderVectorMemory struct {
	QueryBytes      int64 `json:"query_bytes"`
	WriteBytes      int64 `json:"write_bytes"`
	CompactionBytes int64 `json:"compaction_bytes"`
}

// Vector memory outside its caches and budgets that the server's fixed
// defaults bound: a writer's SlateDB memtables (128 MiB unflushed at about
// 2.5 bytes per byte), and a reader's replay of the writer's WAL not yet in
// L0, bounded by the same 128 MiB. Each also holds its request bodies, at
// most 2 MiB each and a few times that decoded, and the centroids its
// namespaces keep in memory.
const (
	vectorWriterReserved = 512 * mebibyte
	vectorReaderReserved = 448 * mebibyte
)

// vectorMemory splits the plan between the budgets of what queries, writes
// and compactions hold. Readers neither take writes nor compact, so their
// share goes to queries.
func (plan memoryPlan) vectorMemory(role string) renderVectorMemory {
	if role == modeReader {
		return renderVectorMemory{
			QueryBytes:      plan.share(40, 128*mebibyte),
			WriteBytes:      16 * mebibyte,
			CompactionBytes: 64 * mebibyte,
		}
	}
	return renderVectorMemory{
		QueryBytes:      plan.share(24, 128*mebibyte),
		WriteBytes:      plan.share(12, 32*mebibyte),
		CompactionBytes: plan.share(12, 128*mebibyte),
	}
}

func (plan memoryPlan) embeddingCache() int64 { return plan.share(4, 16*mebibyte) }

type renderVectorEmbedding struct {
	Provider   string                `json:"provider"`
	Model      string                `json:"model"`
	APIKey     *renderFileSecret     `json:"api_key,omitempty"`
	Endpoint   string                `json:"endpoint,omitempty"`
	Region     string                `json:"region,omitempty"`
	Dimensions *int32                `json:"dimensions,omitempty"`
	BatchSize  *int32                `json:"batch_size,omitempty"`
	Timeout    *int64                `json:"timeout,omitempty"`
	Cache      *renderEmbeddingCache `json:"cache,omitempty"`
}

type renderEmbeddingCache struct {
	Capacity int64 `json:"capacity"`
}

type renderVectorIndex struct {
	SplitThresholdVectors *int64 `json:"split_threshold_vectors,omitempty"`
	MergeThresholdVectors *int64 `json:"merge_threshold_vectors,omitempty"`
}

type renderVectorKubernetes struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	Port          int32  `json:"port"`
	LeaseDuration int64  `json:"lease_duration"`
	ReaderGroups  bool   `json:"reader_groups"`
}

// renderVector renders vector.yaml, for standalone servers and writers, and
// for a sharded cluster reader.yaml. Readers learn the shard they serve from
// READER_SHARD, so every reader group shares one config.
//
// Servers reload namespaces and their credentials in place, so the hash, which
// rolls pods when it changes, leaves them out.
func renderVector(input Input) (Result, error) {
	vector := input.Vector
	spec := vector.Spec.Config
	descriptor := resources.VectorDescriptor
	sharded := resources.Mode(vector) == telemetryv1alpha1.ProductModeSharded
	secretsPath := lo.CoalesceOrEmpty(input.SecretsPath, descriptor.SecretsPath)
	internalTokenPath := lo.CoalesceOrEmpty(input.InternalTokenPath, descriptor.InternalTokenPath)
	data := map[string][]byte{}
	global := renderResolvedAccess(data, "global", input.Global, secretsPath)
	namespaces := renderNamespaces(data, lo.Map(input.Namespaces, func(namespace NamespaceAccess, _ int) NamespaceAccess {
		namespace.UsageReportingEndpoint = ""
		return namespace
	}), spec.Namespaces, secretsPath)
	jwt, err := renderVectorJWT(data, input.JWT, secretsPath)
	if err != nil {
		return Result{}, err
	}
	auth := renderAuth{Unauthenticated: spec.Auth.Unauthenticated, JWT: jwt, Global: global, Internal: &renderFileSecret{Source: sourceFile, Path: internalTokenPath}}
	embedding, err := renderEmbedding(data, spec.Embedding, input.EmbeddingAPIKey, secretsPath)
	if err != nil {
		return Result{}, err
	}
	if err := validateCacheVolume(spec.Storage, vector.Spec.Writer, modeWriter); err != nil {
		return Result{}, err
	}
	if sharded {
		if err := validateCacheVolume(spec.Storage, vector.Spec.Reader, modeReader); err != nil {
			return Result{}, err
		}
		if resources.ReadGroupsEnabled(vector) {
			if err := resources.ValidateReaderGroupNames(vector.Name); err != nil {
				return Result{}, err
			}
		}
	}
	makeConfig := func(role string, live bool) ([]byte, error) {
		component := resources.Component(role)
		workload := vector.Spec.Writer
		if component == resources.ComponentReader {
			workload = vector.Spec.Reader
		}
		plan := memoryPlan{
			limit:    resources.MemoryLimit(workload, descriptor, component),
			reserved: lo.Ternary(role == modeReader, vectorReaderReserved, vectorWriterReserved),
		}
		rendered := renderVectorConfig{
			Storage:               renderStorageConfig(spec.Storage, descriptor, resources.CacheDiskCapacity(workload), plan),
			Role:                  role,
			PathPrefix:            vector.Spec.Ingress.PathPrefix,
			FlushInterval:         int64Value(spec.FlushIntervalSeconds, 1),
			ReaderRefreshInterval: int64Value(spec.ReaderRefreshIntervalSeconds, 1),
			Index: renderVectorIndex{
				SplitThresholdVectors: spec.Index.SplitThresholdVectors,
				MergeThresholdVectors: spec.Index.MergeThresholdVectors,
			},
			Embedding: embedding,
			Memory:    plan.vectorMemory(role),
		}
		if embedding != nil && embedding.Cache == nil {
			sized := *embedding
			sized.Cache = &renderEmbeddingCache{Capacity: plan.embeddingCache()}
			rendered.Embedding = &sized
		}
		if live {
			rendered.Auth, rendered.Namespaces = auth, namespaces
		} else {
			rendered.Auth = renderAuth{JWT: auth.JWT, Internal: auth.Internal}
		}
		if sharded {
			rendered.Kubernetes = &renderVectorKubernetes{
				Name:          vector.Name,
				Namespace:     lo.CoalesceOrEmpty(vector.Namespace, defaultNamespace),
				Port:          resources.HTTPPort(vector),
				LeaseDuration: int64Value(spec.LeaseDurationSeconds, 15),
				ReaderGroups:  resources.ReadGroupsEnabled(vector),
			}
		}
		return yaml.Marshal(rendered)
	}
	roles := map[string]string{VectorKey: modeStandalone}
	if sharded {
		roles = map[string]string{VectorKey: modeWriter, ReaderKey: modeReader}
	}
	sum := sha256.New()
	keys := lo.Keys(roles)
	slices.Sort(keys)
	for _, key := range keys {
		if data[key], err = makeConfig(roles[key], true); err != nil {
			return Result{}, fmt.Errorf("render vector %s config: %w", roles[key], err)
		}
		static, err := makeConfig(roles[key], false)
		if err != nil {
			return Result{}, fmt.Errorf("render vector %s config: %w", roles[key], err)
		}
		_, _ = sum.Write([]byte(key))
		_, _ = sum.Write(static)
	}
	_, _ = sum.Write(data[jwksKey])
	_, _ = sum.Write(input.InternalToken)
	return Result{Data: data, Hash: hex.EncodeToString(sum.Sum(nil))}, nil
}

const embeddingAPIKeyKey = "embedding-api-key"

// renderEmbedding renders the embedding model, with its API key read from
// the config Secret so key rotations apply in place.
func renderEmbedding(data map[string][]byte, spec *telemetryv1alpha1.VectorEmbeddingSpec, apiKey []byte, secretsPath string) (*renderVectorEmbedding, error) {
	if spec == nil {
		return nil, nil
	}
	embedding := &renderVectorEmbedding{Provider: spec.Provider, Model: spec.Model, Endpoint: spec.Endpoint, Region: spec.Region, Dimensions: spec.Dimensions, BatchSize: spec.BatchSize, Timeout: spec.TimeoutSeconds}
	if spec.CacheSize != nil {
		capacity, ok := spec.CacheSize.AsInt64()
		if !ok || capacity < 0 {
			return nil, fmt.Errorf("embedding.cacheSize %s must be a non-negative byte count", spec.CacheSize.String())
		}
		embedding.Cache = &renderEmbeddingCache{Capacity: capacity}
	}
	if spec.APIKeySecretRef != nil {
		if len(apiKey) == 0 {
			return nil, fmt.Errorf("embedding.apiKeySecretRef %s/%s is empty", spec.APIKeySecretRef.Name, spec.APIKeySecretRef.Key)
		}
		data[embeddingAPIKeyKey] = append([]byte(nil), apiKey...)
		embedding.APIKey = &renderFileSecret{Source: sourceFile, Path: secretsPath + "/" + embeddingAPIKeyKey}
	}
	return embedding, nil
}

const jwksKey = "jwks.json"

func renderVectorJWT(data map[string][]byte, input *JWT, secretsPath string) (*renderJWT, error) {
	if input == nil {
		return nil, nil
	}
	jwt := &renderJWT{Issuer: input.Issuer, Audience: input.Audience, RefreshIntervalSeconds: int64Value(input.RefreshIntervalSeconds, 300), RequestTimeoutSeconds: int64Value(input.RequestTimeoutSeconds, 5)}
	switch {
	case len(input.JWKS) > 0:
		data[jwksKey] = append([]byte(nil), input.JWKS...)
		jwt.JWKS = renderJWKS{Source: sourceFile, Path: secretsPath + "/" + jwksKey}
	case input.URL != "":
		jwt.JWKS = renderJWKS{Source: sourceURL, URL: input.URL}
	default:
		return nil, fmt.Errorf("auth.jwt.jwks requires url or resolved secret data")
	}
	return jwt, nil
}
