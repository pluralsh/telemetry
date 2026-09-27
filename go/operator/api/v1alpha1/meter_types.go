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

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MeterMode string

const (
	MeterModeStandalone MeterMode = "Standalone"
	MeterModeSharded    MeterMode = "Sharded"
)

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
	// +kubebuilder:default="ghcr.io/pluralsh/meter"
	Repository string `json:"repository,omitempty"`
	Tag        string `json:"tag,omitempty"`
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// AWSObjectStoreSpec configures an Amazon S3 or S3-compatible object store.
//
// Credentials are optional so workloads can use ambient credentials such as
// IAM roles for service accounts. When static credentials are used, both the
// access key ID and secret access key must be supplied.
// +kubebuilder:validation:XValidation:rule="has(self.accessKeyIDSecretRef) == has(self.secretAccessKeySecretRef)",message="accessKeyIDSecretRef and secretAccessKeySecretRef must be configured together"
type AWSObjectStoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region"`
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`
	// Endpoint overrides the AWS endpoint for S3-compatible stores.
	Endpoint string `json:"endpoint,omitempty"`
	// AllowHTTP permits unencrypted HTTP connections to the custom endpoint.
	AllowHTTP bool `json:"allowHTTP,omitempty"`
	// VirtualHostedStyle sends requests to bucket.endpoint instead of endpoint/bucket.
	VirtualHostedStyle       bool                      `json:"virtualHostedStyle,omitempty"`
	AccessKeyIDSecretRef     *corev1.SecretKeySelector `json:"accessKeyIDSecretRef,omitempty"`
	SecretAccessKeySecretRef *corev1.SecretKeySelector `json:"secretAccessKeySecretRef,omitempty"`
	SessionTokenSecretRef    *corev1.SecretKeySelector `json:"sessionTokenSecretRef,omitempty"`
}

// AzureClientSecretAuthSpec configures Azure service-principal authentication.
type AzureClientSecretAuthSpec struct {
	// +kubebuilder:validation:MinLength=1
	ClientID string `json:"clientID"`
	// +kubebuilder:validation:MinLength=1
	TenantID           string                   `json:"tenantID"`
	ClientSecretKeyRef corev1.SecretKeySelector `json:"clientSecretKeyRef"`
}

// AzureWorkloadIdentityAuthSpec configures Azure workload identity federation.
// The pod template must mount the projected service-account token at TokenFile.
type AzureWorkloadIdentityAuthSpec struct {
	// +kubebuilder:validation:MinLength=1
	ClientID string `json:"clientID"`
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantID"`
	// +kubebuilder:validation:MinLength=1
	TokenFile string `json:"tokenFile"`
}

// AzureObjectStoreSpec configures Azure Blob Storage.
//
// Authentication is optional to support managed identity. At most one explicit
// authentication method may be configured.
// +kubebuilder:validation:XValidation:rule="[has(self.accessKeySecretRef), has(self.sasTokenSecretRef), has(self.bearerTokenSecretRef), has(self.clientSecret), has(self.workloadIdentity)].filter(x, x).size() <= 1",message="at most one Azure authentication method may be configured"
type AzureObjectStoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	Account string `json:"account"`
	// +kubebuilder:validation:MinLength=1
	Container string `json:"container"`
	// Endpoint overrides the Azure Blob Storage endpoint.
	Endpoint string `json:"endpoint,omitempty"`
	// AllowHTTP permits unencrypted HTTP connections to the custom endpoint.
	AllowHTTP            bool                           `json:"allowHTTP,omitempty"`
	AccessKeySecretRef   *corev1.SecretKeySelector      `json:"accessKeySecretRef,omitempty"`
	SASTokenSecretRef    *corev1.SecretKeySelector      `json:"sasTokenSecretRef,omitempty"`
	BearerTokenSecretRef *corev1.SecretKeySelector      `json:"bearerTokenSecretRef,omitempty"`
	ClientSecret         *AzureClientSecretAuthSpec     `json:"clientSecret,omitempty"`
	WorkloadIdentity     *AzureWorkloadIdentityAuthSpec `json:"workloadIdentity,omitempty"`
}

// GCPObjectStoreSpec configures Google Cloud Storage.
//
// Authentication is optional to support application default credentials.
// +kubebuilder:validation:XValidation:rule="![has(self.serviceAccountKeySecretRef), has(self.bearerTokenSecretRef)].all(x, x)",message="serviceAccountKeySecretRef and bearerTokenSecretRef are mutually exclusive"
type GCPObjectStoreSpec struct {
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`
	// BaseURL overrides the Google Cloud Storage API URL.
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

type StorageSpec struct {
	// +kubebuilder:default=meter
	Path         string          `json:"path,omitempty"`
	SettingsPath string          `json:"settingsPath,omitempty"`
	ObjectStore  ObjectStoreSpec `json:"objectStore,omitempty"`
	BlockCache   *CacheSpec      `json:"blockCache,omitempty"`
	MetaCache    *CacheSpec      `json:"metaCache,omitempty"`
}

type WriteSpec struct {
	// +kubebuilder:validation:Enum=applied;written;durable
	// +kubebuilder:default=written
	Durability Durability `json:"durability,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=60
	FlushIntervalSeconds *int64 `json:"flushIntervalSeconds,omitempty"`
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
	// +kubebuilder:default=64
	VirtualShards *int32 `json:"virtualShards,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=15
	LeaseDurationSeconds *int64 `json:"leaseDurationSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	RenewIntervalSeconds *int64 `json:"renewIntervalSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	WatchPollIntervalSeconds *int64 `json:"watchPollIntervalSeconds,omitempty"`
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
	Global                 AccessSpec                `json:"global,omitempty"`
	JWT                    *JWTSpec                  `json:"jwt,omitempty"`
	InternalTokenSecretRef *corev1.SecretKeySelector `json:"internalTokenSecretRef,omitempty"`
}

type MeterConfigSpec struct {
	Storage StorageSpec `json:"storage,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=268435456
	ReaderCacheCapacity *int64       `json:"readerCacheCapacity,omitempty"`
	Write               WriteSpec    `json:"write,omitempty"`
	Sharding            ShardingSpec `json:"sharding,omitempty"`
	Auth                AuthSpec     `json:"auth,omitempty"`
	// +kubebuilder:default={"default"}
	Namespaces []string `json:"namespaces,omitempty"`
}

type WorkloadSpec struct {
	// +kubebuilder:validation:Minimum=0
	Replicas    *int32                  `json:"replicas,omitempty"`
	PodTemplate *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
	DataVolume  *VolumeSpec             `json:"dataVolume,omitempty"`
	CacheVolume *VolumeSpec             `json:"cacheVolume,omitempty"`
}

// VolumeSpec configures one of the fixed data or cache workload volumes.
// +kubebuilder:validation:XValidation:rule="has(self.emptyDir) != has(self.persistentVolumeClaim)",message="exactly one of emptyDir or persistentVolumeClaim is required"
type VolumeSpec struct {
	EmptyDir              *corev1.EmptyDirVolumeSource      `json:"emptyDir,omitempty"`
	PersistentVolumeClaim *corev1.PersistentVolumeClaimSpec `json:"persistentVolumeClaim,omitempty"`
}

type ServiceSpec struct {
	// +kubebuilder:default=8080
	HTTPPort int32 `json:"httpPort,omitempty"`
	// +kubebuilder:default=9090
	GRPCPort int32 `json:"grpcPort,omitempty"`
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

// MeterSpec defines the desired state of Meter.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.reader) || !has(self.reader.replicas) || self.reader.replicas == 0",message="reader replicas must be zero in Standalone mode"
type MeterSpec struct {
	// +kubebuilder:validation:Enum=Standalone;Sharded
	// +kubebuilder:default=Standalone
	Mode    MeterMode       `json:"mode,omitempty"`
	Image   ImageSpec       `json:"image,omitempty"`
	Config  MeterConfigSpec `json:"config,omitempty"`
	Writer  WorkloadSpec    `json:"writer,omitempty"`
	Reader  WorkloadSpec    `json:"reader,omitempty"`
	Service ServiceSpec     `json:"service,omitempty"`
}

// MeterStatus defines the observed state of Meter.
type MeterStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	WriterEndpoint     string             `json:"writerEndpoint,omitempty"`
	ReaderEndpoint     string             `json:"readerEndpoint,omitempty"`
	ConfigHash         string             `json:"configHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mtr
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Meter is the Schema for the meters API.
type Meter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MeterSpec   `json:"spec,omitempty"`
	Status MeterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MeterList contains a list of Meter.
type MeterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Meter `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Meter{}, &MeterList{})
}
