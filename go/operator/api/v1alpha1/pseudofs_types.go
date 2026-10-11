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

// +kubebuilder:validation:XValidation:rule="!has(self.storage) || !has(self.storage.path) || self.storage.path.matches('^[^/]+(/[^/]+)*$')",message="storage.path must be an object-store prefix without leading, trailing, or repeated '/'"
type PseudoFSConfigSpec struct {
	// Storage.Path is the filesystem's root prefix in the object store: the
	// metastore is kept under "<path>/metastore" and blobs under
	// "<path>/blobs".
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
	// InlineThresholdBytes is the largest write stored in the metastore
	// rather than as a blob. Inline bytes pass through the metastore's WAL and
	// compactions, so raising this trades metastore write load for fewer blobs.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=16777216
	// +kubebuilder:default=16384
	InlineThresholdBytes *int64 `json:"inlineThresholdBytes,omitempty"`
	// +kubebuilder:default={}
	Blobs PseudoFSBlobsSpec `json:"blobs,omitempty"`
	// +kubebuilder:default={}
	Search PseudoFSSearchSpec `json:"search,omitempty"`
}

// PseudoFSBlobsSpec configures file content stored outside the metastore.
// Blobs are kept on the data volume until uploaded and then cached there, and
// are uploaded to the storage object store with the same credentials, under
// "<storage.path>/blobs".
type PseudoFSBlobsSpec struct {
	// CacheCapacityBytes bounds blob usage of the data volume, including
	// blobs awaiting upload. Keep it well below the volume size.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=4294967296
	CacheCapacityBytes *int64 `json:"cacheCapacityBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=4
	UploadConcurrency *int32 `json:"uploadConcurrency,omitempty"`
	// GCGraceSeconds is how long unreferenced blobs are kept so in-flight
	// reads of replaced files can finish.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=300
	GCGraceSeconds *int64 `json:"gcGraceSeconds,omitempty"`
}

// PseudoFSSearchSpec bounds Glob and Grep requests. Requests that leave a
// limit unset use its default; larger requested values are capped.
// +kubebuilder:validation:XValidation:rule="!has(self.defaultGlobResults) || !has(self.maxGlobResults) || self.defaultGlobResults <= self.maxGlobResults",message="defaultGlobResults must not exceed maxGlobResults"
// +kubebuilder:validation:XValidation:rule="!has(self.defaultGrepMatches) || !has(self.maxGrepMatches) || self.defaultGrepMatches <= self.maxGrepMatches",message="defaultGrepMatches must not exceed maxGrepMatches"
// +kubebuilder:validation:XValidation:rule="!has(self.defaultGrepFileSizeBytes) || !has(self.maxGrepFileSizeBytes) || self.defaultGrepFileSizeBytes <= self.maxGrepFileSizeBytes",message="defaultGrepFileSizeBytes must not exceed maxGrepFileSizeBytes"
type PseudoFSSearchSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1000
	DefaultGlobResults *int64 `json:"defaultGlobResults,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10000
	MaxGlobResults *int64 `json:"maxGlobResults,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=100
	DefaultGrepMatches *int64 `json:"defaultGrepMatches,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1000
	MaxGrepMatches *int64 `json:"maxGrepMatches,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=100
	MaxGrepContextLines *int64 `json:"maxGrepContextLines,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16777216
	DefaultGrepFileSizeBytes *int64 `json:"defaultGrepFileSizeBytes,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=67108864
	MaxGrepFileSizeBytes *int64 `json:"maxGrepFileSizeBytes,omitempty"`
	// GrepConcurrency is how many files one Grep request searches at once.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8
	GrepConcurrency *int32 `json:"grepConcurrency,omitempty"`
	// MaxScannedEntries is how many directory entries one Glob or Grep walk
	// may read before failing with RESOURCE_EXHAUSTED.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1000000
	MaxScannedEntries *int64 `json:"maxScannedEntries,omitempty"`
	// MaxGrepSeconds is how long one Grep request may search before failing
	// with RESOURCE_EXHAUSTED after the matches already sent.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=30
	MaxGrepSeconds *int32 `json:"maxGrepSeconds,omitempty"`
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
