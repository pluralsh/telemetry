use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::{HashRange, HashRangeMap, RoutingError, hash_routing_key};

pub const DEFAULT_VIRTUAL_SHARDS: u32 = 8;
pub const DEFAULT_IO_CONCURRENCY_MULTIPLIER: u32 = 8;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct ShardId(u32);

impl ShardId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ShardRange {
    start: ShardId,
    end: ShardId,
}

impl<'de> Deserialize<'de> for ShardRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireRange {
            start: ShardId,
            end: ShardId,
        }

        let range = WireRange::deserialize(deserializer)?;
        Self::new(range.start, range.end).map_err(de::Error::custom)
    }
}

impl ShardRange {
    pub fn new(start: ShardId, end: ShardId) -> Result<Self, ModelError> {
        if start >= end {
            return Err(ModelError::EmptyOrReversedRange { start, end });
        }
        Ok(Self { start, end })
    }

    pub fn within(start: u32, end: u32, virtual_shards: u32) -> Result<Self, ModelError> {
        let range = Self::new(ShardId::new(start), ShardId::new(end))?;
        if end > virtual_shards {
            return Err(ModelError::RangeOutOfBounds {
                range,
                virtual_shards,
            });
        }
        Ok(range)
    }

    pub const fn start(self) -> ShardId {
        self.start
    }

    pub const fn end(self) -> ShardId {
        self.end
    }

    pub const fn len(self) -> u32 {
        self.end.0 - self.start.0
    }

    pub const fn is_empty(self) -> bool {
        false
    }

    pub const fn contains(self, shard: ShardId) -> bool {
        shard.0 >= self.start.0 && shard.0 < self.end.0
    }

    pub const fn is_adjacent(self, other: Self) -> bool {
        self.end.0 == other.start.0 || other.end.0 == self.start.0
    }
}

impl fmt::Display for ShardRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.start, self.end)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct AssignmentGeneration(u64);

impl AssignmentGeneration {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for AssignmentGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Owner {
    pub id: String,
    pub ordinal: u32,
}

impl Owner {
    pub fn new(id: impl Into<String>, ordinal: u32) -> Self {
        Self {
            id: id.into(),
            ordinal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentState {
    Pending,
    Active,
    Draining,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub owner: Owner,
    pub range: ShardRange,
    pub state: AssignmentState,
}

impl Assignment {
    pub const fn new(owner: Owner, range: ShardRange, state: AssignmentState) -> Self {
        Self {
            owner,
            range,
            state,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    Preparing,
    Prepared,
    Draining,
    Cloning,
    Ready,
    Completing,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSplit {
    pub source_shard: ShardId,
    pub target_shard: ShardId,
    pub moved_range: HashRange,
    pub source_owner: Owner,
    pub target_owner: Owner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMigration {
    pub phase: MigrationPhase,
    pub desired_shard_count: u32,
    pub split: ShardSplit,
    pub target_routing: HashRangeMap,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShardMap {
    pub generation: AssignmentGeneration,
    #[serde(rename = "shard_count")]
    pub virtual_shards: u32,
    pub routing: HashRangeMap,
    pub assignments: Vec<Assignment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<ShardMigration>,
}

impl<'de> Deserialize<'de> for ShardMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireMap {
            generation: AssignmentGeneration,
            #[serde(rename = "shard_count")]
            virtual_shards: u32,
            routing: HashRangeMap,
            assignments: Vec<Assignment>,
            #[serde(default)]
            migration: Option<ShardMigration>,
        }

        let map = WireMap::deserialize(deserializer)?;
        Self::with_routing_and_migration(
            map.generation,
            map.virtual_shards,
            map.routing,
            map.assignments,
            map.migration,
        )
        .map_err(de::Error::custom)
    }
}

impl ShardMap {
    pub fn new(
        generation: AssignmentGeneration,
        virtual_shards: u32,
        assignments: Vec<Assignment>,
    ) -> Result<Self, ModelError> {
        let routing = HashRangeMap::bootstrap(virtual_shards)?;
        Self::with_routing(generation, virtual_shards, routing, assignments)
    }

    pub fn with_routing(
        generation: AssignmentGeneration,
        virtual_shards: u32,
        routing: HashRangeMap,
        assignments: Vec<Assignment>,
    ) -> Result<Self, ModelError> {
        Self::with_routing_and_migration(generation, virtual_shards, routing, assignments, None)
    }

    pub fn with_routing_and_migration(
        generation: AssignmentGeneration,
        virtual_shards: u32,
        routing: HashRangeMap,
        mut assignments: Vec<Assignment>,
        migration: Option<ShardMigration>,
    ) -> Result<Self, ModelError> {
        if virtual_shards == 0 {
            return Err(ModelError::ZeroVirtualShards);
        }
        let mut routing_shards = routing
            .assignments
            .iter()
            .map(|assignment| assignment.shard)
            .collect::<Vec<_>>();
        routing_shards.sort_unstable();
        if routing_shards.len() != virtual_shards as usize
            || routing_shards
                .iter()
                .enumerate()
                .any(|(index, shard)| shard.get() != index as u32)
        {
            return Err(ModelError::RoutingShardSetMismatch { virtual_shards });
        }
        assignments.sort_by_key(|assignment| assignment.range.start());
        let mut expected = 0;
        for assignment in &assignments {
            let start = assignment.range.start().get();
            if assignment.range.end().get() > virtual_shards {
                return Err(ModelError::RangeOutOfBounds {
                    range: assignment.range,
                    virtual_shards,
                });
            }
            if start < expected {
                return Err(ModelError::Overlap {
                    previous_end: ShardId::new(expected),
                    next_start: assignment.range.start(),
                });
            }
            if start > expected {
                return Err(ModelError::Gap {
                    expected: ShardId::new(expected),
                    actual: assignment.range.start(),
                });
            }
            expected = assignment.range.end().get();
        }
        if expected != virtual_shards {
            return Err(ModelError::IncompleteCoverage {
                covered_until: ShardId::new(expected),
                virtual_shards,
            });
        }
        Ok(Self {
            generation,
            virtual_shards,
            routing,
            assignments,
            migration,
        })
    }

    pub fn route_key(&self, key: &[u8]) -> ShardId {
        self.routing.route(hash_routing_key(key))
    }

    pub fn owner_of(&self, shard: ShardId) -> Option<&Owner> {
        self.assignments
            .iter()
            .find(|assignment| assignment.range.contains(shard))
            .map(|assignment| &assignment.owner)
    }

    pub fn assignments_for<'a>(
        &'a self,
        owner_id: &'a str,
    ) -> impl Iterator<Item = &'a Assignment> + 'a {
        self.assignments
            .iter()
            .filter(move |assignment| assignment.owner.id == owner_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    #[error("virtual shard count must be greater than zero")]
    ZeroVirtualShards,
    #[error("shard range must be non-empty and ordered, got {start}..{end}")]
    EmptyOrReversedRange { start: ShardId, end: ShardId },
    #[error("range {range} exceeds virtual shard count {virtual_shards}")]
    RangeOutOfBounds {
        range: ShardRange,
        virtual_shards: u32,
    },
    #[error("assignment overlap: previous end {previous_end}, next start {next_start}")]
    Overlap {
        previous_end: ShardId,
        next_start: ShardId,
    },
    #[error("assignment gap: expected {expected}, got {actual}")]
    Gap { expected: ShardId, actual: ShardId },
    #[error(
        "assignment coverage ends at {covered_until}, expected virtual shard count {virtual_shards}"
    )]
    IncompleteCoverage {
        covered_until: ShardId,
        virtual_shards: u32,
    },
    #[error("routing map does not contain exactly shards [0, {virtual_shards})")]
    RoutingShardSetMismatch { virtual_shards: u32 },
    #[error(transparent)]
    InvalidRouting(#[from] RoutingError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_ranges() {
        assert!(ShardRange::within(0, 1, 64).is_ok());
        assert!(matches!(
            ShardRange::within(2, 2, 64),
            Err(ModelError::EmptyOrReversedRange { .. })
        ));
        assert!(matches!(
            ShardRange::within(0, 65, 64),
            Err(ModelError::RangeOutOfBounds { .. })
        ));
    }

    #[test]
    fn requires_exact_assignment_coverage() {
        let owner = Owner::new("meter-0", 0);
        let assignment = |start, end| {
            Assignment::new(
                owner.clone(),
                ShardRange::within(start, end, 64).unwrap(),
                AssignmentState::Active,
            )
        };
        assert!(ShardMap::new(AssignmentGeneration::new(1), 64, vec![assignment(0, 64)]).is_ok());
        assert!(matches!(
            ShardMap::new(AssignmentGeneration::new(1), 64, vec![assignment(1, 64)]),
            Err(ModelError::Gap { .. })
        ));
        assert!(matches!(
            ShardMap::new(
                AssignmentGeneration::new(1),
                64,
                vec![assignment(0, 40), assignment(39, 64)]
            ),
            Err(ModelError::Overlap { .. })
        ));
    }

    #[test]
    fn deserialization_cannot_bypass_validation() {
        let invalid_range = r#"{"start":5,"end":4}"#;
        assert!(serde_json::from_str::<ShardRange>(invalid_range).is_err());
        let incomplete_map = r#"{
            "generation": 1,
            "shard_count": 64,
            "routing": {
                "generation": 1,
                "assignments": [{
                    "shard": 0,
                    "range": {
                        "start": "00000000000000000000000000000000",
                        "end": "ffffffffffffffffffffffffffffffff"
                    }
                }]
            },
            "assignments": [{
                "owner": {"id": "meter-0", "ordinal": 0},
                "range": {"start": 1, "end": 64},
                "state": "active"
            }]
        }"#;
        assert!(serde_json::from_str::<ShardMap>(incomplete_map).is_err());
    }

    #[test]
    fn assignment_snapshot_round_trips_with_routing_generation() {
        let map = ShardMap::new(
            AssignmentGeneration::new(7),
            2,
            vec![Assignment::new(
                Owner::new("meter-0", 0),
                ShardRange::within(0, 2, 2).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap();
        let encoded = serde_json::to_string(&map).unwrap();
        assert!(encoded.contains(r#""shard_count":2"#));
        assert!(!encoded.contains("virtual_shards"));
        let decoded = serde_json::from_str::<ShardMap>(&encoded).unwrap();
        assert_eq!(decoded, map);
        assert_eq!(decoded.routing.generation.get(), 1);
    }
}
