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

const (
	VectorShardActive  = "active"
	VectorShardRetired = "retired"
)

// VectorNamespaceRange is the namespace ids [start, end) a shard holds, as
// 16 hex digits. An absent end extends to the end of the id space.
type VectorNamespaceRange struct {
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{16}$`
	Start string `json:"start"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{16}$`
	End string `json:"end,omitempty"`
}

type VectorShard struct {
	// +kubebuilder:validation:Minimum=0
	ID int32 `json:"id"`
	// +kubebuilder:validation:Enum=active;retired
	State string               `json:"state"`
	Range VectorNamespaceRange `json:"range"`
	// Owner is the writer pod that owns the shard. Each writer owns one
	// active shard.
	Owner string `json:"owner,omitempty"`
	// SplitTo is a writer pod without a shard that the owner is splitting
	// this shard for: the writer takes the upper half of the range.
	SplitTo string `json:"split_to,omitempty"`
	// Parent is the shard this one was split from.
	Parent *int32 `json:"parent,omitempty"`
	// Children are the shards this one was split into.
	Children    []int32 `json:"children,omitempty"`
	CreatedAtMs int64   `json:"created_at_ms"`
}

// VectorTopologySpec is the authoritative shard layout of a Vector cluster.
// The elected writer and the writer splitting a shard change it with
// resourceVersion-based compare-and-swap; every replica follows it by watch.
type VectorTopologySpec struct {
	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation"`
	// +kubebuilder:validation:Minimum=1
	NextShardID int32         `json:"next_shard_id"`
	Shards      []VectorShard `json:"shards"`
}

// Writers is how many writers hold a shard or are getting one by a split.
func (s VectorTopologySpec) Writers() int32 {
	var writers int32
	for _, shard := range s.Shards {
		if shard.State != VectorShardActive {
			continue
		}
		if shard.Owner != "" {
			writers++
		}
		if shard.SplitTo != "" {
			writers++
		}
	}
	return writers
}

// ActiveShards are the ids of the active shards, in order.
func (s VectorTopologySpec) ActiveShards() []int32 {
	var active []int32
	for _, shard := range s.Shards {
		if shard.State == VectorShardActive {
			active = append(active, shard.ID)
		}
	}
	return active
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=vtop
// +kubebuilder:printcolumn:name="Generation",type=integer,JSONPath=`.spec.generation`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VectorTopology is the shard layout of a Vector database. The operator and
// the database's writers maintain it, so it is not meant to be edited.
type VectorTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec VectorTopologySpec `json:"spec"`
}

// +kubebuilder:object:root=true

// VectorTopologyList contains a list of VectorTopology.
type VectorTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VectorTopology `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VectorTopology{}, &VectorTopologyList{})
}
