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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type VectorIndexSpec struct {
	// SplitThresholdVectors is the posting-list size at which a centroid
	// splits. The server default is 150.
	// +kubebuilder:validation:Minimum=2
	SplitThresholdVectors *int64 `json:"splitThresholdVectors,omitempty"`
	// MergeThresholdVectors is the posting-list size below which a centroid
	// merges into its neighbours. The server default is 50.
	// +kubebuilder:validation:Minimum=1
	MergeThresholdVectors *int64 `json:"mergeThresholdVectors,omitempty"`
}

type VectorConfigSpec struct {
	// +kubebuilder:default={"path":"vector"}
	Storage StorageSpec `json:"storage,omitempty"`
	// FlushIntervalSeconds is how often namespaces flush buffered writes.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	FlushIntervalSeconds *int64 `json:"flushIntervalSeconds,omitempty"`
	// ReaderRefreshIntervalSeconds is how often readers pick up their
	// writer's changes, which bounds the staleness of eventual reads.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	ReaderRefreshIntervalSeconds *int64 `json:"readerRefreshIntervalSeconds,omitempty"`
	// LeaseDurationSeconds is the lifetime of shard and coordinator leases:
	// how long a shard stays unserved after its writer dies.
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=15
	LeaseDurationSeconds *int64          `json:"leaseDurationSeconds,omitempty"`
	Index                VectorIndexSpec `json:"index,omitempty"`
	// Auth configures credentials, as for the other datastores. Unless
	// unauthenticated is set, only declared namespaces exist: those of
	// namespaces and of NamespaceAuthentications referencing this Vector.
	// Namespace read access covers queries, rows and metadata; write access
	// also covers creating, configuring, deleting and writing to the
	// namespace. Shard listing needs global credentials.
	// Credential changes apply without restarting pods.
	Auth AuthSpec `json:"auth,omitempty"`
	// Namespaces are declared without namespace credentials, for global
	// credentials or JWTs to use.
	Namespaces []string `json:"namespaces,omitempty"`
	// Embedding configures the model that embeds namespaces' embed
	// attributes and query texts. Changing it restarts the pods; namespaces
	// keep the model they first embedded with.
	Embedding *VectorEmbeddingSpec `json:"embedding,omitempty"`
}

// VectorEmbeddingSpec is an embedding model served by a provider API.
// +kubebuilder:validation:XValidation:rule="self.provider != 'bedrock' || (!has(self.apiKeySecretRef) && !has(self.endpoint))",message="bedrock authenticates with the AWS credential chain; apiKeySecretRef and endpoint don't apply"
// +kubebuilder:validation:XValidation:rule="self.provider == 'bedrock' || !has(self.region)",message="region only applies to the bedrock provider"
// +kubebuilder:validation:XValidation:rule="self.provider != 'cohere' || !has(self.dimensions)",message="cohere models embed at a fixed size; dimensions doesn't apply"
type VectorEmbeddingSpec struct {
	// Provider is the API: openai (or an OpenAI-compatible endpoint, such
	// as Azure OpenAI, vLLM or Ollama), cohere, gemini, or bedrock (Amazon
	// Bedrock's Titan Text Embeddings v2).
	// +kubebuilder:validation:Enum=openai;cohere;gemini;bedrock
	Provider string `json:"provider"`
	// Model is the provider's model name, e.g. text-embedding-3-small or
	// amazon.titan-embed-text-v2:0.
	// +kubebuilder:validation:MinLength=1
	Model string `json:"model"`
	// APIKeySecretRef holds the provider API key. Rotating the Secret
	// applies without restarting pods. Bedrock instead uses the pods' AWS
	// credentials: an IAM role through the service account, or a Bedrock
	// API key in AWS_BEARER_TOKEN_BEDROCK.
	APIKeySecretRef *corev1.SecretKeySelector `json:"apiKeySecretRef,omitempty"`
	// Endpoint replaces the provider's API root, e.g.
	// http://ollama.ai.svc:11434/v1.
	Endpoint string `json:"endpoint,omitempty"`
	// Region is the AWS region of the Bedrock runtime. Without it, the
	// region of the pods' AWS config (AWS_REGION).
	// +kubebuilder:validation:MinLength=1
	Region string `json:"region,omitempty"`
	// Dimensions asks models that can shorten their vectors for this many
	// dimensions: OpenAI's text-embedding-3 models, Gemini, and Titan v2
	// (256, 512 or 1024).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Dimensions *int32 `json:"dimensions,omitempty"`
	// BatchSize is the most texts sent in one provider request. The server
	// default is 96.
	// +kubebuilder:validation:Minimum=1
	BatchSize *int32 `json:"batchSize,omitempty"`
	// TimeoutSeconds bounds each provider request. The server default is
	// 30.
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`
	// CacheSize is the memory each replica spends caching embeddings, so
	// repeated texts and queries skip the provider. 0 disables the cache.
	// Without it, 4% of what each component's memory limit leaves after
	// headroom and reservations, and at least 16Mi.
	CacheSize *resource.Quantity `json:"cacheSize,omitempty"`
}

// VectorSpec defines the desired state of Vector.
//
// In Sharded mode each writer replica owns one shard, held with a Lease, and
// reader groups serve eventually consistent queries, one group per active
// shard by default. Adding writers splits shards for them: the elected
// writer picks the widest shard for each new writer once its pod is ready,
// and the shard's owner splits it with SlateDB clones, handing half its
// namespaces to the new writer without copying data. Writers can't be
// removed, since shards don't merge; the operator keeps the writer
// StatefulSet at one replica per shard. Any replica accepts any request and
// forwards it to the replica that can serve it.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.reader) || !has(self.reader.replicas) || self.reader.replicas == 0",message="reader replicas must be zero in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.writer) || !has(self.writer.replicas) || self.writer.replicas == 1",message="writer replicas must be one in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Sharded' || (has(self.config) && has(self.config.storage) && has(self.config.storage.objectStore) && self.config.storage.objectStore.type in ['Aws', 'Azure', 'Gcp'])",message="Sharded mode needs an Aws, Azure or Gcp object store that every replica shares"
type VectorSpec struct {
	// +kubebuilder:validation:Enum=Standalone;Sharded
	// +kubebuilder:default=Standalone
	Mode ProductMode `json:"mode,omitempty"`
	// Version is the Vector container image tag.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version,omitempty"`
	// +kubebuilder:default={"repository":"ghcr.io/pluralsh/vector","pullPolicy":"IfNotPresent"}
	Image  ImageSpec        `json:"image,omitempty"`
	Config VectorConfigSpec `json:"config,omitempty"`
	Writer WorkloadSpec     `json:"writer,omitempty"`
	Reader WorkloadSpec     `json:"reader,omitempty"`
	// +kubebuilder:default={"httpPort":8080,"type":"ClusterIP"}
	Service ServiceSpec `json:"service,omitempty"`
	// Ingress exposes the API, which any replica serves, at
	// {pathPrefix}/api on the hostname. With a pathPrefix the servers serve
	// the API under it too, so the Ingress needs no rewrite and the
	// hostname can be shared with other services. Changing pathPrefix
	// restarts the pods.
	Ingress        IngressSpec        `json:"ingress,omitempty"`
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`
}

type VectorStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	// Endpoint is the Service any request can be sent to.
	Endpoint   string `json:"endpoint,omitempty"`
	ConfigHash string `json:"configHash,omitempty"`
	// TopologyGeneration is the generation of the VectorTopology the reader
	// groups were last reconciled against.
	TopologyGeneration int64 `json:"topologyGeneration,omitempty"`
	// ActiveShards are the ids of the shards serving namespaces.
	ActiveShards []int32 `json:"activeShards,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=vectors,singular=vector
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Shards",type=string,JSONPath=`.status.activeShards`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Vector runs a vector and full-text search database on object storage, with
// its namespaces spread over shards that split as they grow.
type Vector struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VectorSpec   `json:"spec,omitempty"`
	Status            VectorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type VectorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Vector `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Vector{}, &VectorList{})
}
