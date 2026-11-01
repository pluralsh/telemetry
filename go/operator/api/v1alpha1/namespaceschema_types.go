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

// AttributeSchema is the schema of one namespace attribute, as in the
// datastore's schema API.
// +kubebuilder:validation:XValidation:rule="self.type == 'string' || !(has(self.full_text_search) && self.full_text_search) && !(has(self.embed) && self.embed)",message="full_text_search and embed need type string"
type AttributeSchema struct {
	// Type is fixed once the attribute exists.
	// +kubebuilder:validation:Enum=string;int;float;bool;[]string;[]int
	Type string `json:"type"`
	// FullTextSearch indexes a string attribute for BM25 ranking.
	// +optional
	FullTextSearch bool `json:"full_text_search,omitempty"`
	// Embed embeds a string attribute into the vector of rows written
	// without one. At most one attribute per namespace.
	// +optional
	Embed bool `json:"embed,omitempty"`
}

// NamespaceSchemaSpec defines the desired state of NamespaceSchema.
// +kubebuilder:validation:XValidation:rule="self.dataStoreRef.kind == 'Vector'",message="only Vector namespaces have schemas"
type NamespaceSchemaSpec struct {
	DataStoreRef DataStoreReference `json:"dataStoreRef"`
	// Namespace is the datastore namespace the schema is for. Defaults to
	// the resource's name.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace,omitempty"`
	// Schema maps attribute names to their schemas. Servers merge it into
	// the namespace whenever it changes and when the namespace is created:
	// attributes it names are replaced whole, others are left alone, and an
	// attribute's type can't change once set. Attributes removed from it
	// stay in the namespace.
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:XValidation:rule="self.all(name, size(name) <= 128 && name != 'id' && name != 'vector' && !name.startsWith('$'))",message="attribute names are at most 128 bytes and can't be id, vector, or start with $"
	// +kubebuilder:validation:XValidation:rule="self.filter(name, has(self[name].embed) && self[name].embed).size() <= 1",message="at most one attribute can embed"
	Schema map[string]AttributeSchema `json:"schema"`
}

// NamespaceSchemaStatus defines the observed state of NamespaceSchema.
type NamespaceSchemaStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nsschema
// +kubebuilder:printcolumn:name="Datastore",type=string,JSONPath=`.spec.dataStoreRef.name`
// +kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.spec.namespace`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NamespaceSchema declares the attribute schema of a datastore namespace.
type NamespaceSchema struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NamespaceSchemaSpec   `json:"spec,omitempty"`
	Status NamespaceSchemaStatus `json:"status,omitempty"`
}

// TargetNamespace is the datastore namespace the schema is for.
func (schema *NamespaceSchema) TargetNamespace() string {
	if schema.Spec.Namespace != "" {
		return schema.Spec.Namespace
	}
	return schema.Name
}

// +kubebuilder:object:root=true

// NamespaceSchemaList contains a list of NamespaceSchema.
type NamespaceSchemaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NamespaceSchema `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NamespaceSchema{}, &NamespaceSchemaList{})
}
