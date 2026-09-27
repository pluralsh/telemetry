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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MeterMode = ProductMode

const (
	MeterModeStandalone = ProductModeStandalone
	MeterModeSharded    = ProductModeSharded
)

type MeterConfigSpec struct {
	// +kubebuilder:default={"path":"meter"}
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

// MeterSpec defines the desired state of Meter.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.reader) || !has(self.reader.replicas) || self.reader.replicas == 0",message="reader replicas must be zero in Standalone mode"
// +kubebuilder:validation:XValidation:rule="self.mode != 'Standalone' || !has(self.writer) || !has(self.writer.replicas) || self.writer.replicas == 1",message="writer replicas must be one in Standalone mode"
// +kubebuilder:validation:XValidation:rule="!has(self.version) || !has(self.image) || !has(self.image.tag) || self.version == self.image.tag",message="spec.version and deprecated spec.image.tag must match when both are set"
type MeterSpec struct {
	// +kubebuilder:validation:Enum=Standalone;Sharded
	// +kubebuilder:default=Standalone
	Mode MeterMode `json:"mode,omitempty"`
	// Version is the canonical Meter container image tag. It must be SemVer
	// without a leading "v". When omitted, deprecated image.tag is used, then
	// the operator's default version.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version,omitempty"`
	// +kubebuilder:default={"repository":"ghcr.io/pluralsh/meter","pullPolicy":"IfNotPresent"}
	Image  ImageSpec       `json:"image,omitempty"`
	Config MeterConfigSpec `json:"config,omitempty"`
	Writer WorkloadSpec    `json:"writer,omitempty"`
	Reader WorkloadSpec    `json:"reader,omitempty"`
	// +kubebuilder:default={"httpPort":8080,"grpcPort":9090,"type":"ClusterIP"}
	Service        ServiceSpec        `json:"service,omitempty"`
	Ingress        IngressSpec        `json:"ingress,omitempty"`
	ServiceAccount ServiceAccountSpec `json:"serviceAccount,omitempty"`
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
