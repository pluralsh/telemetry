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

// BucketCleanerSpec defines the desired state of BucketCleaner.
// +kubebuilder:validation:XValidation:rule="has(self.dataStoreRef) || (has(self.objectStore) && has(self.subPath))",message="objectStore and subPath are required unless dataStoreRef is set"
// +kubebuilder:validation:XValidation:rule="!has(self.objectStore) || self.objectStore.type in ['Aws', 'Azure', 'Gcp']",message="objectStore.type must be Aws, Azure, or Gcp"
// +kubebuilder:validation:XValidation:rule="!has(self.objectStore) || !has(self.objectStore.path) || self.objectStore.path.size() == 0",message="objectStore.path is not supported; use subPath"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.subPath) || has(self.subPath)",message="subPath is immutable"
type BucketCleanerSpec struct {
	// ObjectStore is the bucket holding the tracked data, in the same form as
	// a datastore's spec.config.storage.objectStore. Credential Secrets are
	// read from the BucketCleaner's namespace by the operator itself; when
	// they are omitted the operator's own cloud identity is used. Defaults to
	// the dataStoreRef's spec.config.storage.objectStore.
	// +optional
	ObjectStore *ObjectStoreSpec `json:"objectStore,omitempty"`
	// SubPath is the key prefix within the bucket, typically a datastore's
	// spec.config.storage.path. It is treated as a directory: "logs" covers
	// "logs/..." but not "logs-archive/...". Every object under it is deleted
	// when the BucketCleaner is deleted. Defaults to the dataStoreRef's
	// spec.config.storage.path, or the product name when that is unset.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`[^/]`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="subPath is immutable"
	SubPath string `json:"subPath,omitempty"`
	// CheckIntervalSeconds is how often bucket access is re-verified.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:default=300
	CheckIntervalSeconds *int64 `json:"checkIntervalSeconds,omitempty"`
	// DataStoreRef is the datastore, in the BucketCleaner's namespace, that
	// writes under subPath. Omitted objectStore and subPath settings are
	// taken from its storage while it exists. Deletion waits until the
	// datastore and its pods are gone, so no replica writes objects after the
	// prefix is emptied, and data is never deleted while the datastore exists.
	// +optional
	DataStoreRef *BucketCleanerDataStoreReference `json:"dataStoreRef,omitempty"`
}

type BucketCleanerDataStoreReference struct {
	// +kubebuilder:validation:Enum=Metrics;Logs;Traces;Vector
	Kind string `json:"kind"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// BucketCleanerStatus defines the observed state of BucketCleaner.
type BucketCleanerStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	// LastCheckTime is when bucket access was last verified successfully.
	LastCheckTime *metav1.Time `json:"lastCheckTime,omitempty"`
	// DeletedObjects counts objects deleted under subPath since deletion began.
	DeletedObjects int64 `json:"deletedObjects,omitempty"`
	// ObjectStore is the bucket last resolved from the spec and dataStoreRef.
	// Deletion uses it once the datastore is gone.
	ObjectStore *ObjectStoreSpec `json:"objectStore,omitempty"`
	// SubPath is the prefix last resolved from the spec and dataStoreRef.
	SubPath string `json:"subPath,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.status.objectStore.type`
// +kubebuilder:printcolumn:name="SubPath",type=string,JSONPath=`.status.subPath`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BucketCleaner verifies the operator can reach an object-store prefix and
// deletes every object under it when the BucketCleaner is deleted.
type BucketCleaner struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BucketCleanerSpec   `json:"spec,omitempty"`
	Status BucketCleanerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BucketCleanerList contains a list of BucketCleaner.
type BucketCleanerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketCleaner `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BucketCleaner{}, &BucketCleanerList{})
}
