use std::{fmt, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::{HashRangeMap, RoutingError, hash_routing_key};

pub const DEFAULT_SHARDS: u32 = 1;
pub const DEFAULT_IO_CONCURRENCY_LIMIT: u32 = 128;

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

    pub fn within(start: u32, end: u32, shard_count: u32) -> Result<Self, ModelError> {
        let range = Self::new(ShardId::new(start), ShardId::new(end))?;
        if end > shard_count {
            return Err(ModelError::RangeOutOfBounds { range, shard_count });
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

/// Hash routing that applies to records timestamped at or after
/// `effective_from_ns`, until the next epoch begins.
///
/// Epochs only record where data written during a period lives; nothing moves
/// when a new epoch is added. Each epoch routes over shards `[0, shard_count)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingEpoch {
    pub effective_from_ns: i64,
    pub routing: HashRangeMap,
}

impl RoutingEpoch {
    pub fn initial(shards: u32) -> Result<Self, RoutingError> {
        Ok(Self {
            effective_from_ns: i64::MIN,
            routing: HashRangeMap::bootstrap(shards)?,
        })
    }

    pub fn shard_count(&self) -> u32 {
        u32::try_from(self.routing.assignments.len()).expect("shard count fits in u32")
    }
}

/// When a newly requested routing epoch takes effect.
///
/// Cutovers land on `alignment` boundaries so a product time partition (Meter
/// bucket, Line/Track segment) is always written under a single epoch, and at
/// least `lead_time` in the future so every writer observes the epoch first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochPolicy {
    pub alignment: Duration,
    pub lead_time: Duration,
}

impl Default for EpochPolicy {
    fn default() -> Self {
        Self {
            alignment: Duration::from_secs(3600),
            lead_time: Duration::from_secs(120),
        }
    }
}

impl EpochPolicy {
    pub fn cutover_after(&self, now_ns: i64) -> i64 {
        let lead = i64::try_from(self.lead_time.as_nanos()).unwrap_or(i64::MAX);
        let alignment = i64::try_from(self.alignment.as_nanos())
            .unwrap_or(i64::MAX)
            .max(1);
        let earliest = now_ns.saturating_add(lead);
        let remainder = earliest.rem_euclid(alignment);
        if remainder == 0 {
            earliest
        } else {
            earliest.saturating_add(alignment - remainder)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShardMap {
    pub generation: AssignmentGeneration,
    /// Storage shards `[0, shard_count)` referenced by any epoch. Equal to the
    /// latest epoch's shard count because epochs only grow.
    pub shard_count: u32,
    pub epochs: Vec<RoutingEpoch>,
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
            shard_count: u32,
            epochs: Vec<RoutingEpoch>,
            assignments: Vec<Assignment>,
        }

        let map = WireMap::deserialize(deserializer)?;
        let decoded = Self::with_epochs(map.generation, map.epochs, map.assignments)
            .map_err(de::Error::custom)?;
        if decoded.shard_count != map.shard_count {
            return Err(de::Error::custom(ModelError::EpochShardCountMismatch {
                shard_count: map.shard_count,
            }));
        }
        Ok(decoded)
    }
}

impl ShardMap {
    pub fn new(
        generation: AssignmentGeneration,
        shard_count: u32,
        assignments: Vec<Assignment>,
    ) -> Result<Self, ModelError> {
        Self::with_epochs(generation, Self::initial_epochs(shard_count)?, assignments)
    }

    pub fn initial_epochs(shard_count: u32) -> Result<Vec<RoutingEpoch>, ModelError> {
        if shard_count == 0 {
            return Err(ModelError::ZeroShards);
        }
        Ok(vec![RoutingEpoch::initial(shard_count)?])
    }

    pub fn with_epochs(
        generation: AssignmentGeneration,
        epochs: Vec<RoutingEpoch>,
        mut assignments: Vec<Assignment>,
    ) -> Result<Self, ModelError> {
        validate_epochs(&epochs)?;
        let shard_count = epochs.last().expect("validated epochs").shard_count();
        assignments.sort_by_key(|assignment| assignment.range.start());
        let mut expected = 0;
        for assignment in &assignments {
            let start = assignment.range.start().get();
            if assignment.range.end().get() > shard_count {
                return Err(ModelError::RangeOutOfBounds {
                    range: assignment.range,
                    shard_count,
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
        if expected != shard_count {
            return Err(ModelError::IncompleteCoverage {
                covered_until: ShardId::new(expected),
                shard_count,
            });
        }
        Ok(Self {
            generation,
            shard_count,
            epochs,
            assignments,
        })
    }

    /// Epoch whose routing applies to a record timestamped `timestamp_ns`.
    pub fn epoch_at(&self, timestamp_ns: i64) -> &RoutingEpoch {
        let index = self
            .epochs
            .partition_point(|epoch| epoch.effective_from_ns <= timestamp_ns);
        &self.epochs[index.saturating_sub(1)]
    }

    pub fn latest_epoch(&self) -> &RoutingEpoch {
        self.epochs.last().expect("validated epochs")
    }

    pub fn route_key(&self, key: &[u8], timestamp_ns: i64) -> ShardId {
        self.epoch_at(timestamp_ns)
            .routing
            .route(hash_routing_key(key))
    }

    /// Every shard that may hold data for `key`, across all epochs.
    pub fn shards_for_key(&self, key: &[u8]) -> Vec<ShardId> {
        let hash = hash_routing_key(key);
        let mut shards = self
            .epochs
            .iter()
            .map(|epoch| epoch.routing.route(hash))
            .collect::<Vec<_>>();
        shards.sort_unstable();
        shards.dedup();
        shards
    }

    /// Epochs after scaling to `shards`. A pending epoch (not yet effective at
    /// `now_ns`) is replaced rather than stacked, so repeated scale requests
    /// before a cutover collapse into one epoch.
    pub fn epochs_scaled_to(
        &self,
        shards: u32,
        policy: EpochPolicy,
        now_ns: i64,
    ) -> Result<Vec<RoutingEpoch>, ModelError> {
        if shards < self.shard_count {
            return Err(ModelError::ScaleDownUnsupported {
                current: self.shard_count,
                desired: shards,
            });
        }
        let mut epochs = self.epochs.clone();
        if shards == self.shard_count {
            return Ok(epochs);
        }
        let routing = HashRangeMap::bootstrap(shards)?;
        let pending = epochs
            .last()
            .is_some_and(|epoch| epoch.effective_from_ns > now_ns && epochs.len() > 1);
        if pending {
            epochs.last_mut().expect("pending epoch").routing = routing;
        } else {
            let previous = epochs.last().expect("validated epochs").effective_from_ns;
            epochs.push(RoutingEpoch {
                effective_from_ns: policy.cutover_after(now_ns).max(previous.saturating_add(1)),
                routing,
            });
        }
        Ok(epochs)
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

fn validate_epochs(epochs: &[RoutingEpoch]) -> Result<(), ModelError> {
    let Some(first) = epochs.first() else {
        return Err(ModelError::NoEpochs);
    };
    if first.effective_from_ns != i64::MIN {
        return Err(ModelError::FirstEpochNotUnbounded);
    }
    let mut previous: Option<&RoutingEpoch> = None;
    for epoch in epochs {
        let count = epoch.shard_count();
        let mut shards = epoch
            .routing
            .assignments
            .iter()
            .map(|assignment| assignment.shard.get())
            .collect::<Vec<_>>();
        shards.sort_unstable();
        if shards
            .iter()
            .enumerate()
            .any(|(index, shard)| *shard != index as u32)
        {
            return Err(ModelError::RoutingShardSetMismatch { shard_count: count });
        }
        if let Some(previous) = previous {
            if epoch.effective_from_ns <= previous.effective_from_ns {
                return Err(ModelError::UnorderedEpochs);
            }
            if count < previous.shard_count() {
                return Err(ModelError::ScaleDownUnsupported {
                    current: previous.shard_count(),
                    desired: count,
                });
            }
        }
        previous = Some(epoch);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    #[error("shard count must be greater than zero")]
    ZeroShards,
    #[error("shard range must be non-empty and ordered, got {start}..{end}")]
    EmptyOrReversedRange { start: ShardId, end: ShardId },
    #[error("range {range} exceeds shard count {shard_count}")]
    RangeOutOfBounds { range: ShardRange, shard_count: u32 },
    #[error("assignment overlap: previous end {previous_end}, next start {next_start}")]
    Overlap {
        previous_end: ShardId,
        next_start: ShardId,
    },
    #[error("assignment gap: expected {expected}, got {actual}")]
    Gap { expected: ShardId, actual: ShardId },
    #[error("assignment coverage ends at {covered_until}, expected shard count {shard_count}")]
    IncompleteCoverage {
        covered_until: ShardId,
        shard_count: u32,
    },
    #[error("routing map does not contain exactly shards [0, {shard_count})")]
    RoutingShardSetMismatch { shard_count: u32 },
    #[error("at least one routing epoch is required")]
    NoEpochs,
    #[error("the first routing epoch must start at the beginning of time")]
    FirstEpochNotUnbounded,
    #[error("routing epochs must have strictly increasing start times")]
    UnorderedEpochs,
    #[error("shard_count {shard_count} does not match the latest routing epoch")]
    EpochShardCountMismatch { shard_count: u32 },
    #[error("decreasing storage shard count from {current} to {desired} is unsupported")]
    ScaleDownUnsupported { current: u32, desired: u32 },
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
            "shard_count": 1,
            "epochs": [{
                "effective_from_ns": -9223372036854775808,
                "routing": {
                    "generation": 1,
                    "assignments": [{
                        "shard": 0,
                        "range": {
                            "start": "00000000000000000000000000000000",
                            "end": "ffffffffffffffffffffffffffffffff"
                        }
                    }]
                }
            }],
            "assignments": [{
                "owner": {"id": "meter-0", "ordinal": 0},
                "range": {"start": 1, "end": 1},
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
        let decoded = serde_json::from_str::<ShardMap>(&encoded).unwrap();
        assert_eq!(decoded, map);
        assert_eq!(decoded.latest_epoch().routing.generation.get(), 1);
    }

    fn single_owner(shards: u32, epochs: Vec<RoutingEpoch>) -> ShardMap {
        ShardMap::with_epochs(
            AssignmentGeneration::new(1),
            epochs,
            vec![Assignment::new(
                Owner::new("meter-0", 0),
                ShardRange::within(0, shards, shards).unwrap(),
                AssignmentState::Active,
            )],
        )
        .unwrap()
    }

    #[test]
    fn cutovers_are_aligned_and_leave_lead_time() {
        let policy = EpochPolicy {
            alignment: Duration::from_secs(3600),
            lead_time: Duration::from_secs(120),
        };
        let hour = 3_600_000_000_000_i64;
        assert_eq!(policy.cutover_after(10 * hour + 1), 11 * hour);
        assert_eq!(policy.cutover_after(11 * hour - 60_000_000_000), 12 * hour);
        assert_eq!(policy.cutover_after(11 * hour - 120_000_000_000), 11 * hour);
    }

    #[test]
    fn scale_up_adds_an_epoch_and_routes_by_record_time() {
        let hour = 3_600_000_000_000_i64;
        let policy = EpochPolicy::default();
        let two = single_owner(2, ShardMap::initial_epochs(2).unwrap());
        let epochs = two.epochs_scaled_to(3, policy, 10 * hour + 5).unwrap();
        assert_eq!(epochs.len(), 2);
        assert_eq!(epochs[1].effective_from_ns, 11 * hour);
        let three = single_owner(3, epochs);
        assert_eq!(three.shard_count, 3);

        let mut saw_new_shard = false;
        for key in 0_u32..1_000 {
            let key = key.to_be_bytes();
            let before = three.route_key(&key, 11 * hour - 1);
            let after = three.route_key(&key, 11 * hour);
            assert_eq!(before, two.route_key(&key, 0));
            assert!(before.get() < 2);
            saw_new_shard |= after == ShardId::new(2);
            let mut expected = vec![before, after];
            expected.sort_unstable();
            expected.dedup();
            assert_eq!(three.shards_for_key(&key), expected);
        }
        assert!(saw_new_shard);
    }

    #[test]
    fn pending_epoch_is_replaced_and_scale_down_is_rejected() {
        let hour = 3_600_000_000_000_i64;
        let policy = EpochPolicy::default();
        let two = single_owner(2, ShardMap::initial_epochs(2).unwrap());
        let three = single_owner(3, two.epochs_scaled_to(3, policy, hour).unwrap());
        let four = three.epochs_scaled_to(4, policy, hour + 1).unwrap();
        assert_eq!(four.len(), 2);
        assert_eq!(four[1].shard_count(), 4);
        assert_eq!(
            three.epochs_scaled_to(3, policy, hour).unwrap(),
            three.epochs
        );
        assert!(matches!(
            three.epochs_scaled_to(2, policy, hour),
            Err(ModelError::ScaleDownUnsupported { .. })
        ));

        let effective = three.epochs_scaled_to(4, policy, 3 * hour).unwrap();
        assert_eq!(effective.len(), 3);
        assert_eq!(effective[2].effective_from_ns, 4 * hour);
    }

    #[test]
    fn epochs_must_be_ordered_and_non_decreasing() {
        let mut epochs = ShardMap::initial_epochs(2).unwrap();
        epochs.push(RoutingEpoch {
            effective_from_ns: 10,
            routing: HashRangeMap::bootstrap(1).unwrap(),
        });
        assert!(matches!(
            ShardMap::with_epochs(AssignmentGeneration::new(1), epochs, Vec::new()),
            Err(ModelError::ScaleDownUnsupported { .. })
        ));
        let unbounded = vec![RoutingEpoch {
            effective_from_ns: 0,
            routing: HashRangeMap::bootstrap(1).unwrap(),
        }];
        assert!(matches!(
            ShardMap::with_epochs(AssignmentGeneration::new(1), unbounded, Vec::new()),
            Err(ModelError::FirstEpochNotUnbounded)
        ));
    }
}
