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

type TracesMode = ProductMode

const (
	TracesModeStandalone = ProductModeStandalone
	TracesModeSharded    = ProductModeSharded
)

type TracesPageSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1048576
	TargetSizeBytes *int64 `json:"targetSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=4194304
	MaxSizeBytes *int64 `json:"maxSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1024
	MaxTraces *int64 `json:"maxTraces,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.maxRequestBytes) || !has(self.maxDecodedRequestBytes) || self.maxDecodedRequestBytes >= self.maxRequestBytes",message="maxDecodedRequestBytes must be at least maxRequestBytes"
type TracesRequestSpec struct {
	// MaxRequestBytes caps a request body as received, before content decoding.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=33554432
	MaxRequestBytes *int64 `json:"maxRequestBytes,omitempty"`
	// MaxDecodedRequestBytes caps a write body after gzip decoding and a
	// decoded OTLP or Jaeger gRPC message. Must be at least maxRequestBytes.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=134217728
	MaxDecodedRequestBytes *int64 `json:"maxDecodedRequestBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=64
	RequestConcurrency *int32 `json:"requestConcurrency,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	MaxCandidates *int64 `json:"maxCandidates,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=100000
	MaxSpansPerTrace *int64 `json:"maxSpansPerTrace,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8
	QueryConcurrency *int32 `json:"queryConcurrency,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1000
	MaxQueryLimit *int64 `json:"maxQueryLimit,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.retention) || !has(self.retentionSeconds)",message="set retention or the deprecated retentionSeconds, not both"
type TracesConfigSpec struct {
	// +kubebuilder:default={"path":"traces"}
	Storage StorageSpec `json:"storage,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3600
	SegmentDurationSeconds *int64 `json:"segmentDurationSeconds,omitempty"`
	// Retention is how long data is kept, counted from ingestion, e.g. 14d or 2w.
	// When unset, data is kept forever.
	// +kubebuilder:validation:Pattern=`^([0-9]+[wdhms])*0*[1-9][0-9]*[wdhms]([0-9]+[wdhms])*$`
	Retention string `json:"retention,omitempty"`
	// RetentionSeconds is deprecated in favor of retention.
	// +kubebuilder:validation:Minimum=1
	RetentionSeconds *int64            `json:"retentionSeconds,omitempty"`
	Page             TracesPageSpec    `json:"page,omitempty"`
	Write            WriteSpec         `json:"write,omitempty"`
	Sharding         ShardingSpec      `json:"sharding,omitempty"`
	Request          TracesRequestSpec `json:"request,omitempty"`
	CacheWarmer      *CacheWarmerSpec  `json:"cacheWarmer,omitempty"`
	Auth             AuthSpec          `json:"auth,omitempty"`
	// +kubebuilder:default={"default"}
	Namespaces []string `json:"namespaces,omitempty"`
}

// TracesSpec defines the desired state of Traces.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.reader) || !has(self.reader.replicas) || self.reader.replicas == 0",message="reader replicas must be zero in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.writer) || !has(self.writer.replicas) || self.writer.replicas == 1",message="writer replicas must be one in Standalone mode"
// +kubebuilder:validation:XValidation:rule="!has(self.version) || !has(self.image) || !has(self.image.tag) || self.version == self.image.tag",message="spec.version and deprecated spec.image.tag must match when both are set"
type TracesSpec struct {
	// +kubebuilder:validation:Enum=Standalone;Sharded
	// +kubebuilder:default=Standalone
	Mode TracesMode `json:"mode,omitempty"`
	// Version is the canonical Traces container image tag. It must be SemVer
	// without a leading "v". When omitted, deprecated image.tag is used, then
	// the operator's default version.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version,omitempty"`
	// +kubebuilder:default={"repository":"ghcr.io/pluralsh/traces","pullPolicy":"IfNotPresent"}
	Image  ImageSpec        `json:"image,omitempty"`
	Config TracesConfigSpec `json:"config,omitempty"`
	Writer WorkloadSpec     `json:"writer,omitempty"`
	Reader WorkloadSpec     `json:"reader,omitempty"`
	// +kubebuilder:default={"httpPort":3200,"grpcPort":9092,"type":"ClusterIP"}
	Service        ServiceSpec        `json:"service,omitempty"`
	Ingress        IngressSpec        `json:"ingress,omitempty"`
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`
}

type TracesStatus struct {
	ObservedGeneration  int64              `json:"observedGeneration,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
	WriterEndpoint      string             `json:"writerEndpoint,omitempty"`
	ReaderEndpoint      string             `json:"readerEndpoint,omitempty"`
	ConfigHash          string             `json:"configHash,omitempty"`
	WriterScalingStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=traces,singular=traces
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Traces struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TracesSpec   `json:"spec,omitempty"`
	Status            TracesStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type TracesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Traces `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Traces{}, &TracesList{})
}
