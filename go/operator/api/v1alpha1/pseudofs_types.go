/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PseudoFSConfigSpec struct {
	// +kubebuilder:default={"path":"pseudofs"}
	Storage StorageSpec `json:"storage,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1048576
	ChunkSizeBytes *int64 `json:"chunkSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1073741824
	MaxFileSizeBytes *int64 `json:"maxFileSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:default=64
	MaxAppendGenerations *int64 `json:"maxAppendGenerations,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8388608
	MaxUnaryFileSizeBytes *int64 `json:"maxUnaryFileSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16777216
	MaxDecodingMessageBytes *int64 `json:"maxDecodingMessageBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16777216
	MaxEncodingMessageBytes *int64 `json:"maxEncodingMessageBytes,omitempty"`
}

type PseudoFSServiceSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=9093
	GRPCPort int32 `json:"grpcPort,omitempty"`
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

// PseudoFSSpec defines the desired state of PseudoFS.
// +kubebuilder:validation:XValidation:rule="!has(self.version) || !has(self.image) || !has(self.image.tag) || self.version == self.image.tag",message="spec.version and deprecated spec.image.tag must match when both are set"
// +kubebuilder:validation:XValidation:rule="!has(self.workload.replicas) || self.workload.replicas == 1",message="PseudoFS always runs exactly one replica"
// +kubebuilder:validation:XValidation:rule="!has(self.config.chunkSizeBytes) || !has(self.config.maxFileSizeBytes) || self.config.chunkSizeBytes <= self.config.maxFileSizeBytes",message="chunkSizeBytes must not exceed maxFileSizeBytes"
// +kubebuilder:validation:XValidation:rule="!has(self.config.maxUnaryFileSizeBytes) || !has(self.config.maxFileSizeBytes) || self.config.maxUnaryFileSizeBytes <= self.config.maxFileSizeBytes",message="maxUnaryFileSizeBytes must not exceed maxFileSizeBytes"
// +kubebuilder:validation:XValidation:rule="!has(self.config.maxUnaryFileSizeBytes) || !has(self.config.maxDecodingMessageBytes) || self.config.maxUnaryFileSizeBytes <= self.config.maxDecodingMessageBytes",message="maxUnaryFileSizeBytes must not exceed maxDecodingMessageBytes"
// +kubebuilder:validation:XValidation:rule="!has(self.config.maxUnaryFileSizeBytes) || !has(self.config.maxEncodingMessageBytes) || self.config.maxUnaryFileSizeBytes <= self.config.maxEncodingMessageBytes",message="maxUnaryFileSizeBytes must not exceed maxEncodingMessageBytes"
type PseudoFSSpec struct {
	// Version is the canonical PseudoFS container image tag.
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*))(\.((0|[1-9][0-9]*)|([0-9]*[A-Za-z-][0-9A-Za-z-]*)))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version,omitempty"`
	// +kubebuilder:default={"repository":"ghcr.io/pluralsh/pseudofs","pullPolicy":"IfNotPresent"}
	Image          ImageSpec           `json:"image,omitempty"`
	Config         PseudoFSConfigSpec  `json:"config,omitempty"`
	Workload       WorkloadSpec        `json:"workload,omitempty"`
	Service        PseudoFSServiceSpec `json:"service,omitempty"`
	ServiceAccount ServiceAccountSpec  `json:"serviceAccount,omitempty"`
}

type PseudoFSStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	GRPCEndpoint       string             `json:"grpcEndpoint,omitempty"`
	ConfigHash         string             `json:"configHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=pseudofs,shortName=pseudofs
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.grpcEndpoint`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PseudoFS struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PseudoFSSpec   `json:"spec,omitempty"`
	Status            PseudoFSStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PseudoFSList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PseudoFS `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PseudoFS{}, &PseudoFSList{})
}
