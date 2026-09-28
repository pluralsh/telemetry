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

type LineMode = ProductMode

const (
	LineModeStandalone = ProductModeStandalone
	LineModeSharded    = ProductModeSharded
)

type LinePageSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1048576
	TargetSizeBytes *int64 `json:"targetSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8192
	MaxRows *int64 `json:"maxRows,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=5
	MaxAgeSeconds *int64 `json:"maxAgeSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=256
	RowsPerBlock *int64 `json:"rowsPerBlock,omitempty"`
}

type LineRequestSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10485760
	MaxRequestBytes *int64 `json:"maxRequestBytes,omitempty"`
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

type LineCacheSpec struct {
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=256
	QueryEntries *int64 `json:"queryEntries,omitempty"`
}

type LineConfigSpec struct {
	// +kubebuilder:default={"path":"line"}
	Storage StorageSpec `json:"storage,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3600
	SegmentDurationSeconds *int64 `json:"segmentDurationSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	RetentionSeconds *int64       `json:"retentionSeconds,omitempty"`
	Page             LinePageSpec `json:"page,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	VisibilityIntervalSeconds *int64          `json:"visibilityIntervalSeconds,omitempty"`
	Write                     WriteSpec       `json:"write,omitempty"`
	Sharding                  ShardingSpec    `json:"sharding,omitempty"`
	Request                   LineRequestSpec `json:"request,omitempty"`
	Cache                     LineCacheSpec   `json:"cache,omitempty"`
	Auth                      AuthSpec        `json:"auth,omitempty"`
	// +kubebuilder:default={"default"}
	Namespaces []string `json:"namespaces,omitempty"`
}

// LineSpec defines the desired state of Line.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.reader) || !has(self.reader.replicas) || self.reader.replicas == 0",message="reader replicas must be zero in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.writer) || !has(self.writer.replicas) || self.writer.replicas == 1",message="writer replicas must be one in Standalone mode"
// +kubebuilder:validation:XValidation:rule="!has(self.version) || !has(self.image) || !has(self.image.tag) || self.version == self.image.tag",message="spec.version and deprecated spec.image.tag must match when both are set"
// +kubebuilder:validation:XValidation:rule="!has(self.ingress.pathPrefix) || self.ingress.pathPrefix.size() == 0",message="Line does not support ingress.pathPrefix because namespace API routes are fixed"
type LineSpec struct {
	// +kubebuilder:validation:Enum=Standalone;Sharded
	// +kubebuilder:default=Standalone
	Mode LineMode `json:"mode,omitempty"`
	// Version is the canonical Line container image tag. It must be SemVer
	// without a leading "v". When omitted, deprecated image.tag is used, then
	// the operator's default version.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version,omitempty"`
	// +kubebuilder:default={"repository":"ghcr.io/pluralsh/line","pullPolicy":"IfNotPresent"}
	Image  ImageSpec      `json:"image,omitempty"`
	Config LineConfigSpec `json:"config,omitempty"`
	Writer WorkloadSpec   `json:"writer,omitempty"`
	Reader WorkloadSpec   `json:"reader,omitempty"`
	// +kubebuilder:default={"httpPort":3100,"grpcPort":9091,"type":"ClusterIP"}
	Service        ServiceSpec        `json:"service,omitempty"`
	Ingress        IngressSpec        `json:"ingress,omitempty"`
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`
}

type LineStatus struct {
	ObservedGeneration  int64              `json:"observedGeneration,omitempty"`
	Conditions          []metav1.Condition `json:"conditions,omitempty"`
	WriterEndpoint      string             `json:"writerEndpoint,omitempty"`
	ReaderEndpoint      string             `json:"readerEndpoint,omitempty"`
	ConfigHash          string             `json:"configHash,omitempty"`
	WriterScalingStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=line
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Line struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LineSpec   `json:"spec,omitempty"`
	Status            LineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type LineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Line `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Line{}, &LineList{})
}
