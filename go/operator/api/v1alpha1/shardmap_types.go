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

type ShardOwner struct {
	ID string `json:"id"`
	// +kubebuilder:validation:Minimum=0
	Ordinal int32 `json:"ordinal"`
}

type ShardRange struct {
	// +kubebuilder:validation:Minimum=0
	Start int32 `json:"start"`
	// +kubebuilder:validation:Minimum=1
	End int32 `json:"end"`
}

type ShardAssignment struct {
	Owner ShardOwner `json:"owner"`
	Range ShardRange `json:"range"`
	// +kubebuilder:validation:Enum=pending;active;draining;released
	State string `json:"state"`
}

type HashRange struct {
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{32}$`
	Start string `json:"start"`
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{32}$`
	End string `json:"end"`
}

type HashRangeAssignment struct {
	// +kubebuilder:validation:Minimum=0
	Shard int32     `json:"shard"`
	Range HashRange `json:"range"`
}

type HashRangeMap struct {
	// +kubebuilder:validation:Minimum=1
	Generation  int64                 `json:"generation"`
	Assignments []HashRangeAssignment `json:"assignments"`
}

type ShardSplit struct {
	// +kubebuilder:validation:Minimum=0
	SourceShard int32 `json:"source_shard"`
	// +kubebuilder:validation:Minimum=0
	TargetShard int32      `json:"target_shard"`
	MovedRange  HashRange  `json:"moved_range"`
	SourceOwner ShardOwner `json:"source_owner"`
	TargetOwner ShardOwner `json:"target_owner"`
}

type ShardMigration struct {
	// +kubebuilder:validation:Enum=preparing;prepared;draining;cloning;ready;completing;failed
	Phase string `json:"phase"`
	// +kubebuilder:validation:Minimum=1
	DesiredShardCount int32        `json:"desired_shard_count"`
	Split             ShardSplit   `json:"split"`
	TargetRouting     HashRangeMap `json:"target_routing"`
	Error             string       `json:"error,omitempty"`
}

// ShardMapSpec is the authoritative routing and writer-ownership snapshot.
// The elected Rust shard coordinator writes it with resourceVersion-based
// compare-and-swap updates; all writer and reader replicas consume it by watch.
type ShardMapSpec struct {
	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation"`
	// ShardCount is the current number of physical SlateDB storage shards.
	// Desired count comes from writer StatefulSet replicas.
	// +kubebuilder:validation:Minimum=1
	ShardCount  int32             `json:"shard_count"`
	Routing     HashRangeMap      `json:"routing"`
	Assignments []ShardAssignment `json:"assignments"`
	Migration   *ShardMigration   `json:"migration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=sm
// +kubebuilder:printcolumn:name="Generation",type=integer,JSONPath=`.spec.generation`
// +kubebuilder:printcolumn:name="Shards",type=integer,JSONPath=`.spec.shard_count`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ShardMap is the Schema for authoritative telemetry shard routing.
type ShardMap struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ShardMapSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// ShardMapList contains a list of ShardMap.
type ShardMapList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShardMap `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ShardMap{}, &ShardMapList{})
}
