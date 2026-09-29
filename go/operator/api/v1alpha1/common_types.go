/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import corev1 "k8s.io/api/core/v1"

type ProductMode string

const (
	ProductModeStandalone ProductMode = "Standalone"
	ProductModeSharded    ProductMode = "Sharded"
)

// WriterScalingStatus reports the operator-observed writer membership and the
// authoritative Rust-managed ShardMap state. Nil shard fields mean the
// ShardMap was absent or could not be trusted.
type WriterScalingStatus struct {
	EffectiveWriterReplicas int32  `json:"effectiveWriterReplicas,omitempty"`
	ReadyWriterReplicas     int32  `json:"readyWriterReplicas,omitempty"`
	ShardCount              *int32 `json:"shardCount,omitempty"`
	ShardGeneration         *int64 `json:"shardGeneration,omitempty"`
	MigrationPhase          string `json:"migrationPhase,omitempty"`
	MigrationError          string `json:"migrationError,omitempty"`
}

type ObjectStoreType string

const (
	ObjectStoreInMemory ObjectStoreType = "InMemory"
	ObjectStoreLocal    ObjectStoreType = "Local"
	ObjectStoreAWS      ObjectStoreType = "Aws"
	ObjectStoreAzure    ObjectStoreType = "Azure"
	ObjectStoreGCP      ObjectStoreType = "Gcp"
)

type CacheType string

const (
	CacheFoyerHybrid CacheType = "FoyerHybrid"
	CacheFoyerMemory CacheType = "FoyerMemory"
)

type Durability string

const (
	DurabilityApplied Durability = "applied"
	DurabilityWritten Durability = "written"
	DurabilityDurable Durability = "durable"
)

type ImageSpec struct {
	// Repository is the container image repository. It defaults to the product's
	// official GHCR repository.
	Repository string `json:"repository,omitempty"`
	// Tag is a deprecated alias for the enclosing resource's spec.version.
	// When both are set they must match, and spec.version takes precedence.
	// Deprecated: use spec.version.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Tag string `json:"tag,omitempty"`
	// PullPolicy is the Kubernetes image pull policy.
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// AWSObjectStoreSpec configures an Amazon S3 or S3-compatible object store.
// +kubebuilder:validation:XValidation:rule="has(self.accessKeyIDSecretRef) == has(self.secretAccessKeySecretRef)",message="accessKeyIDSecretRef and secretAccessKeySecretRef must be configured together"
type AWSObjectStoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`
	// +kubebuilder:validation:MinLength=1
	Bucket                   string                    `json:"bucket"`
	Endpoint                 string                    `json:"endpoint,omitempty"`
	AllowHTTP                bool                      `json:"allowHTTP,omitempty"`
	VirtualHostedStyle       bool                      `json:"virtualHostedStyle,omitempty"`
	AccessKeyIDSecretRef     *corev1.SecretKeySelector `json:"accessKeyIDSecretRef,omitempty"`
	SecretAccessKeySecretRef *corev1.SecretKeySelector `json:"secretAccessKeySecretRef,omitempty"`
	SessionTokenSecretRef    *corev1.SecretKeySelector `json:"sessionTokenSecretRef,omitempty"`
}

type AzureClientSecretAuthSpec struct {
	// +kubebuilder:validation:MinLength=1
	ClientID string `json:"clientID"`
	// +kubebuilder:validation:MinLength=1
	TenantID           string                   `json:"tenantID"`
	ClientSecretKeyRef corev1.SecretKeySelector `json:"clientSecretKeyRef"`
}

type AzureWorkloadIdentityAuthSpec struct {
	// +kubebuilder:validation:MinLength=1
	ClientID string `json:"clientID"`
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantID"`
	// +kubebuilder:validation:MinLength=1
	TokenFile string `json:"tokenFile"`
}

// +kubebuilder:validation:XValidation:rule="[has(self.accessKeySecretRef), has(self.sasTokenSecretRef), has(self.bearerTokenSecretRef), has(self.clientSecret), has(self.workloadIdentity)].filter(x, x).size() <= 1",message="at most one Azure authentication method may be configured"
type AzureObjectStoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	Account string `json:"account"`
	// +kubebuilder:validation:MinLength=1
	Container            string                         `json:"container"`
	Endpoint             string                         `json:"endpoint,omitempty"`
	AllowHTTP            bool                           `json:"allowHTTP,omitempty"`
	AccessKeySecretRef   *corev1.SecretKeySelector      `json:"accessKeySecretRef,omitempty"`
	SASTokenSecretRef    *corev1.SecretKeySelector      `json:"sasTokenSecretRef,omitempty"`
	BearerTokenSecretRef *corev1.SecretKeySelector      `json:"bearerTokenSecretRef,omitempty"`
	ClientSecret         *AzureClientSecretAuthSpec     `json:"clientSecret,omitempty"`
	WorkloadIdentity     *AzureWorkloadIdentityAuthSpec `json:"workloadIdentity,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="![has(self.serviceAccountKeySecretRef), has(self.bearerTokenSecretRef)].all(x, x)",message="serviceAccountKeySecretRef and bearerTokenSecretRef are mutually exclusive"
type GCPObjectStoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	Bucket                     string                    `json:"bucket"`
	BaseURL                    string                    `json:"baseURL,omitempty"`
	ServiceAccountKeySecretRef *corev1.SecretKeySelector `json:"serviceAccountKeySecretRef,omitempty"`
	BearerTokenSecretRef       *corev1.SecretKeySelector `json:"bearerTokenSecretRef,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.type != 'Aws' || has(self.aws)",message="Aws object stores require aws configuration"
// +kubebuilder:validation:XValidation:rule="self.type == 'Aws' || !has(self.aws)",message="aws configuration is only valid for Aws object stores"
// +kubebuilder:validation:XValidation:rule="self.type != 'Azure' || has(self.azure)",message="Azure object stores require azure configuration"
// +kubebuilder:validation:XValidation:rule="self.type == 'Azure' || !has(self.azure)",message="azure configuration is only valid for Azure object stores"
// +kubebuilder:validation:XValidation:rule="self.type != 'Gcp' || has(self.gcp)",message="Gcp object stores require gcp configuration"
// +kubebuilder:validation:XValidation:rule="self.type == 'Gcp' || !has(self.gcp)",message="gcp configuration is only valid for Gcp object stores"
type ObjectStoreSpec struct {
	// +kubebuilder:validation:Enum=InMemory;Local;Aws;Azure;Gcp
	// +kubebuilder:default=Local
	Type  ObjectStoreType       `json:"type,omitempty"`
	Path  string                `json:"path,omitempty"`
	AWS   *AWSObjectStoreSpec   `json:"aws,omitempty"`
	Azure *AzureObjectStoreSpec `json:"azure,omitempty"`
	GCP   *GCPObjectStoreSpec   `json:"gcp,omitempty"`
}

type CacheSpec struct {
	// +kubebuilder:validation:Enum=FoyerHybrid;FoyerMemory
	Type                     CacheType `json:"type"`
	MemoryCapacity           *int64    `json:"memoryCapacity,omitempty"`
	DiskCapacity             *int64    `json:"diskCapacity,omitempty"`
	DiskPath                 string    `json:"diskPath,omitempty"`
	Capacity                 *int64    `json:"capacity,omitempty"`
	Shards                   *int32    `json:"shards,omitempty"`
	WritePolicy              string    `json:"writePolicy,omitempty"`
	Flushers                 *int32    `json:"flushers,omitempty"`
	BufferPoolSize           *int64    `json:"bufferPoolSize,omitempty"`
	SubmitQueueSizeThreshold *int64    `json:"submitQueueSizeThreshold,omitempty"`
}

type CacheWarmerSpec struct {
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=7200
	WarmRangeSeconds *int64 `json:"warmRangeSeconds,omitempty"`
	// +kubebuilder:default=true
	IncludePayloads *bool `json:"includePayloads,omitempty"`
}

type StorageSpec struct {
	Path         string          `json:"path,omitempty"`
	SettingsPath string          `json:"settingsPath,omitempty"`
	ObjectStore  ObjectStoreSpec `json:"objectStore,omitempty"`
	BlockCache   *CacheSpec      `json:"blockCache,omitempty"`
	MetaCache    *CacheSpec      `json:"metaCache,omitempty"`
}

type WriteSpec struct {
	// +kubebuilder:validation:Enum=applied;written;durable
	// +kubebuilder:default=applied
	Durability Durability `json:"durability,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=10
	FlushIntervalSeconds *int64 `json:"flushIntervalSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	BufferQueueCapacity *int32 `json:"bufferQueueCapacity,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	BufferFlushIntervalMilliseconds *int64 `json:"bufferFlushIntervalMilliseconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=67108864
	BufferSizeThresholdBytes *int64 `json:"bufferSizeThresholdBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16
	RemoteConcurrency *int32 `json:"remoteConcurrency,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=2
	RemoteRetries *int32 `json:"remoteRetries,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.renewIntervalSeconds) || !has(self.leaseDurationSeconds) || self.renewIntervalSeconds < self.leaseDurationSeconds",message="renewIntervalSeconds must be less than leaseDurationSeconds"
type ShardingSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=128
	// IOConcurrencyLimit bounds concurrent storage I/O operations per pod.
	IOConcurrencyLimit *int32 `json:"ioConcurrencyLimit,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=15
	LeaseDurationSeconds *int64 `json:"leaseDurationSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	RenewIntervalSeconds *int64 `json:"renewIntervalSeconds,omitempty"`
}

type BasicCredentialSpec struct {
	// +kubebuilder:validation:MinLength=1
	Username string                   `json:"username"`
	Password corev1.SecretKeySelector `json:"passwordSecretKeyRef"`
}

type AccessSpec struct {
	Read  []BasicCredentialSpec `json:"read,omitempty"`
	Write []BasicCredentialSpec `json:"write,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="(has(self.url) && self.url.size() > 0) != has(self.secretKeyRef)",message="exactly one of url or secretKeyRef is required"
type JWKSSpec struct {
	URL          string                    `json:"url,omitempty"`
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty"`
}

type JWTSpec struct {
	JWKS     JWKSSpec `json:"jwks"`
	Issuer   string   `json:"issuer,omitempty"`
	Audience string   `json:"audience,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=300
	RefreshIntervalSeconds *int64 `json:"refreshIntervalSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	RequestTimeoutSeconds *int64 `json:"requestTimeoutSeconds,omitempty"`
}

type AuthSpec struct {
	// Unauthenticated permits anonymous access to namespace HTTP APIs.
	// +kubebuilder:default=false
	Unauthenticated        bool                      `json:"unauthenticated,omitempty"`
	Global                 AccessSpec                `json:"global,omitempty"`
	JWT                    *JWTSpec                  `json:"jwt,omitempty"`
	InternalTokenSecretRef *corev1.SecretKeySelector `json:"internalTokenSecretRef,omitempty"`
}

type WorkloadSpec struct {
	// Replicas defaults to three for sharded writers and two for sharded
	// readers. Standalone mode uses exactly one writer and no reader.
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`
	// NodeSelector is merged with podTemplate.spec.nodeSelector. Values here
	// take precedence when the same key is configured in both places.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations are merged with podTemplate.spec.tolerations. A first-class
	// toleration replaces a podTemplate toleration with the same key, operator,
	// and effect; otherwise it is appended.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// Resources configures requests and limits for the datastore container.
	// Values here take precedence over podTemplate container resources. Missing
	// values default to 250m CPU and 512Mi memory requests and a 2Gi memory limit.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// PodTemplate is the full Kubernetes pod template escape hatch. Operator
	// required fields and the first-class scheduling fields are merged into it.
	PodTemplate *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
	DataVolume  *VolumeSpec             `json:"dataVolume,omitempty"`
	CacheVolume *VolumeSpec             `json:"cacheVolume,omitempty"`
	// PersistentVolumeClaimRetentionPolicy controls whether StatefulSet PVCs
	// are retained or deleted when the workload is deleted or scaled down.
	// When omitted, remote and in-memory object stores default to Delete while
	// Local object stores default to Retain because the data PVC is authoritative.
	PersistentVolumeClaimRetentionPolicy *PersistentVolumeClaimRetentionPolicySpec `json:"persistentVolumeClaimRetentionPolicy,omitempty"`
}

type PersistentVolumeClaimRetentionPolicyType string

const (
	PersistentVolumeClaimRetentionPolicyRetain PersistentVolumeClaimRetentionPolicyType = "Retain"
	PersistentVolumeClaimRetentionPolicyDelete PersistentVolumeClaimRetentionPolicyType = "Delete"
)

type PersistentVolumeClaimRetentionPolicySpec struct {
	// WhenDeleted controls PVC retention when the StatefulSet is deleted.
	// When omitted, the object-store-dependent workload default is used.
	// +kubebuilder:validation:Enum=Retain;Delete
	WhenDeleted PersistentVolumeClaimRetentionPolicyType `json:"whenDeleted,omitempty"`
	// WhenScaled controls PVC retention when StatefulSet replicas are reduced.
	// When omitted, the object-store-dependent workload default is used.
	// +kubebuilder:validation:Enum=Retain;Delete
	WhenScaled PersistentVolumeClaimRetentionPolicyType `json:"whenScaled,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.emptyDir) != has(self.persistentVolumeClaim)",message="exactly one of emptyDir or persistentVolumeClaim is required"
type VolumeSpec struct {
	EmptyDir              *corev1.EmptyDirVolumeSource      `json:"emptyDir,omitempty"`
	PersistentVolumeClaim *corev1.PersistentVolumeClaimSpec `json:"persistentVolumeClaim,omitempty"`
}

type ServiceSpec struct {
	HTTPPort int32 `json:"httpPort,omitempty"`
	GRPCPort int32 `json:"grpcPort,omitempty"`
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

type IngressMetadataSpec struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type IngressTLSSpec struct {
	Enabled    bool   `json:"enabled,omitempty"`
	SecretName string `json:"secretName,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.enabled) || !self.enabled || (has(self.hostname) && self.hostname.size() > 0)",message="hostname is required when ingress is enabled"
// +kubebuilder:validation:XValidation:rule="!has(self.pathPrefix) || self.pathPrefix.size() == 0 || (self.pathPrefix.size() > 1 && self.pathPrefix.startsWith('/') && !self.pathPrefix.endsWith('/'))",message="pathPrefix must be empty or start with '/' and must not end with '/'"
type IngressSpec struct {
	Enabled      bool                `json:"enabled,omitempty"`
	Hostname     string              `json:"hostname,omitempty"`
	IngressClass string              `json:"ingressClass,omitempty"`
	PathPrefix   string              `json:"pathPrefix,omitempty"`
	Metadata     IngressMetadataSpec `json:"metadata,omitempty"`
	TLS          IngressTLSSpec      `json:"tls,omitempty"`
}

type ServiceAccountSpec struct {
	Annotations map[string]string `json:"annotations,omitempty"`
}
