use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};

pub const DEFAULT_VIRTUAL_SHARDS: u32 = 8;
pub const DEFAULT_IO_CONCURRENCY_MULTIPLIER: u32 = 4;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShardMap {
    pub generation: AssignmentGeneration,
    pub virtual_shards: u32,
    pub assignments: Vec<Assignment>,
}

impl<'de> Deserialize<'de> for ShardMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireMap {
            generation: AssignmentGeneration,
            virtual_shards: u32,
            assignments: Vec<Assignment>,
        }

        let map = WireMap::deserialize(deserializer)?;
        Self::new(map.generation, map.virtual_shards, map.assignments).map_err(de::Error::custom)
    }
}

impl ShardMap {
    pub fn new(
        generation: AssignmentGeneration,
        virtual_shards: u32,
        mut assignments: Vec<Assignment>,
    ) -> Result<Self, ModelError> {
        if virtual_shards == 0 {
            return Err(ModelError::ZeroVirtualShards);
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
            assignments,
        })
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardingConfig {
    pub virtual_shards: u32,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            virtual_shards: DEFAULT_VIRTUAL_SHARDS,
        }
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
    fn config_defaults_to_eight_virtual_shards() {
        assert_eq!(ShardingConfig::default().virtual_shards, 8);
        assert_eq!(
            serde_json::from_str::<ShardingConfig>("{}")
                .unwrap()
                .virtual_shards,
            8
        );
    }

    #[test]
    fn deserialization_cannot_bypass_validation() {
        let invalid_range = r#"{"start":5,"end":4}"#;
        assert!(serde_json::from_str::<ShardRange>(invalid_range).is_err());
        let incomplete_map = r#"{
            "generation": 1,
            "virtual_shards": 64,
            "assignments": [{
                "owner": {"id": "meter-0", "ordinal": 0},
                "range": {"start": 1, "end": 64},
                "state": "active"
            }]
        }"#;
        assert!(serde_json::from_str::<ShardMap>(incomplete_map).is_err());
    }
}
