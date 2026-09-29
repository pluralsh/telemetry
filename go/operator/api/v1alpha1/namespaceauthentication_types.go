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

type DataStoreReference struct {
	// +kubebuilder:validation:Enum=Meter;Line;Track
	Kind string `json:"kind"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// NamespaceAuthenticationSpec defines the desired state of NamespaceAuthentication.
type NamespaceAuthenticationSpec struct {
	DataStoreRef DataStoreReference `json:"dataStoreRef"`
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Username string `json:"username"`
	// +kubebuilder:validation:Enum=read;write
	Permission   string                   `json:"permission"`
	SecretKeyRef corev1.SecretKeySelector `json:"secretKeyRef"`
}

// NamespaceAuthenticationStatus defines the observed state of NamespaceAuthentication.
type NamespaceAuthenticationStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nsauth
// +kubebuilder:printcolumn:name="Datastore",type=string,JSONPath=`.spec.dataStoreRef.name`
// +kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.spec.namespace`
// +kubebuilder:printcolumn:name="Permission",type=string,JSONPath=`.spec.permission`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NamespaceAuthentication is the Schema for the namespaceauthentications API.
type NamespaceAuthentication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NamespaceAuthenticationSpec   `json:"spec,omitempty"`
	Status NamespaceAuthenticationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NamespaceAuthenticationList contains a list of NamespaceAuthentication.
type NamespaceAuthenticationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NamespaceAuthentication `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NamespaceAuthentication{}, &NamespaceAuthenticationList{})
}
