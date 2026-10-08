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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type LogsMode = ProductMode

const (
	LogsModeStandalone = ProductModeStandalone
	LogsModeSharded    = ProductModeSharded
)

type LogsPageSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1048576
	TargetSizeBytes *int64 `json:"targetSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8192
	MaxRows *int64 `json:"maxRows,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=256
	RowsPerBlock *int64 `json:"rowsPerBlock,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.maxRequestBytes) || !has(self.maxDecodedRequestBytes) || self.maxDecodedRequestBytes >= self.maxRequestBytes",message="maxDecodedRequestBytes must be at least maxRequestBytes"
type LogsRequestSpec struct {
	// MaxRequestBytes caps a request body as received, before content decoding.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=33554432
	MaxRequestBytes *int64 `json:"maxRequestBytes,omitempty"`
	// MaxDecodedRequestBytes caps a write body after gzip or snappy decoding.
	// Must be at least maxRequestBytes.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=134217728
	MaxDecodedRequestBytes *int64 `json:"maxDecodedRequestBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5000
	MaxQueryEntries *int64 `json:"maxQueryEntries,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	MaxQueryPages *int64 `json:"maxQueryPages,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=128
	MaxStructuredMetadataFields *int64 `json:"maxStructuredMetadataFields,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16
	QueryConcurrency *int32 `json:"queryConcurrency,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=134217728
	MaxInFlightQueryBytes *int64 `json:"maxInFlightQueryBytes,omitempty"`
}

// LogsElasticsearchSpec maps Elasticsearch _bulk documents to log entries.
type LogsElasticsearchSpec struct {
	// MessageFields are tried in order; the first present becomes the log line.
	// +kubebuilder:default={"message","log","msg"}
	// +kubebuilder:validation:items:MinLength=1
	MessageFields []string `json:"messageFields,omitempty"`
	// TimeField holds the entry timestamp as RFC3339 or epoch milliseconds.
	// +kubebuilder:default="@timestamp"
	// +kubebuilder:validation:MinLength=1
	TimeField string `json:"timeField,omitempty"`
	// StreamFields are document fields promoted to stream labels. Keep them
	// low-cardinality; all other fields become structured metadata.
	// +kubebuilder:validation:items:MinLength=1
	StreamFields []string `json:"streamFields,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.retention) || !has(self.retentionSeconds)",message="set retention or the deprecated retentionSeconds, not both"
type LogsConfigSpec struct {
	// +kubebuilder:default={"path":"logs"}
	Storage StorageSpec `json:"storage,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3600
	SegmentDurationSeconds *int64 `json:"segmentDurationSeconds,omitempty"`
	// Retention is how long data is kept, counted from ingestion, e.g. 14d or 2w.
	// When unset, the server default of 14 days applies.
	// +kubebuilder:validation:Pattern=`^([0-9]+[wdhms])*0*[1-9][0-9]*[wdhms]([0-9]+[wdhms])*$`
	Retention string `json:"retention,omitempty"`
	// RetentionSeconds is deprecated in favor of retention.
	// +kubebuilder:validation:Minimum=1
	RetentionSeconds *int64          `json:"retentionSeconds,omitempty"`
	Page             LogsPageSpec    `json:"page,omitempty"`
	Write            WriteSpec       `json:"write,omitempty"`
	Sharding         ShardingSpec    `json:"sharding,omitempty"`
	Request          LogsRequestSpec `json:"request,omitempty"`
	// +kubebuilder:default={}
	Elasticsearch LogsElasticsearchSpec `json:"elasticsearch,omitempty"`
	CacheWarmer   *CacheWarmerSpec      `json:"cacheWarmer,omitempty"`
	Auth          AuthSpec              `json:"auth,omitempty"`
	// +kubebuilder:default={"default"}
	Namespaces []string `json:"namespaces,omitempty"`
}

// LogsSpec defines the desired state of Logs.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.reader) || !has(self.reader.replicas) || self.reader.replicas == 0",message="reader replicas must be zero in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.writer) || !has(self.writer.replicas) || self.writer.replicas == 1",message="writer replicas must be one in Standalone mode"
// +kubebuilder:validation:XValidation:rule="!has(self.version) || !has(self.image) || !has(self.image.tag) || self.version == self.image.tag",message="spec.version and deprecated spec.image.tag must match when both are set"
type LogsSpec struct {
	// +kubebuilder:validation:Enum=Standalone;Sharded
	// +kubebuilder:default=Standalone
	Mode LogsMode `json:"mode,omitempty"`
	// Version is the canonical Logs container image tag. It must be SemVer
	// without a leading "v". When omitted, deprecated image.tag is used, then
	// the operator's default version.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version,omitempty"`
	// +kubebuilder:default={"repository":"ghcr.io/pluralsh/logs","pullPolicy":"IfNotPresent"}
	Image  ImageSpec      `json:"image,omitempty"`
	Config LogsConfigSpec `json:"config,omitempty"`
	Writer WorkloadSpec   `json:"writer,omitempty"`
	Reader WorkloadSpec   `json:"reader,omitempty"`
	// +kubebuilder:default={"httpPort":3100,"grpcPort":9091,"type":"ClusterIP"}
	Service        ServiceSpec        `json:"service,omitempty"`
	Ingress        IngressSpec        `json:"ingress,omitempty"`
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`
}

type LogsStatus struct {
	ObservedGeneration  int64              `json:"observedGeneration,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
	WriterEndpoint      string             `json:"writerEndpoint,omitempty"`
	ReaderEndpoint      string             `json:"readerEndpoint,omitempty"`
	ConfigHash          string             `json:"configHash,omitempty"`
	WriterScalingStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=logs,singular=logs
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Logs struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LogsSpec   `json:"spec,omitempty"`
	Status            LogsStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type LogsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Logs `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Logs{}, &LogsList{})
}
