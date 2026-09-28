use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::ShardId;

pub const ROUTING_SLOT_BITS: u32 = 12;
pub const ROUTING_SLOT_COUNT: u16 = 1 << ROUTING_SLOT_BITS;
const ROUTING_SLOT_SHIFT: u32 = 128 - ROUTING_SLOT_BITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingSlot(u16);

impl RoutingSlot {
    pub const fn get(self) -> u16 {
        self.0
    }

    pub fn from_key(key: &[u8]) -> Self {
        hash_routing_key(key).routing_slot()
    }
}

/// A deterministic position in the 128-bit routing space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HashValue(u128);

impl HashValue {
    pub const MIN: Self = Self(u128::MIN);
    pub const MAX: Self = Self(u128::MAX);

    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u128 {
        self.0
    }

    pub const fn routing_slot(self) -> RoutingSlot {
        RoutingSlot((self.0 >> ROUTING_SLOT_SHIFT) as u16)
    }
}

impl fmt::Display for HashValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:032x}", self.0)
    }
}

impl Serialize for HashValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{:032x}", self.0))
    }
}

impl<'de> Deserialize<'de> for HashValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        u128::from_str_radix(&value, 16)
            .map(Self)
            .map_err(de::Error::custom)
    }
}

/// An inclusive range in the complete 128-bit hash space.
///
/// Inclusive endpoints let the final range end at `u128::MAX` without a
/// sentinel for the otherwise-unrepresentable exclusive endpoint `2^128`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct HashRange {
    start: HashValue,
    end: HashValue,
}

impl<'de> Deserialize<'de> for HashRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireRange {
            start: HashValue,
            end: HashValue,
        }

        let range = WireRange::deserialize(deserializer)?;
        Self::new(range.start, range.end).map_err(de::Error::custom)
    }
}

impl HashRange {
    pub fn new(start: HashValue, end: HashValue) -> Result<Self, RoutingError> {
        if start > end {
            return Err(RoutingError::ReversedRange { start, end });
        }
        Ok(Self { start, end })
    }

    pub const fn start(self) -> HashValue {
        self.start
    }

    pub const fn end(self) -> HashValue {
        self.end
    }

    pub const fn contains(self, value: HashValue) -> bool {
        value.0 >= self.start.0 && value.0 <= self.end.0
    }

    pub fn from_slots(start: u16, end: u16) -> Result<Self, RoutingError> {
        if start >= end || end > ROUTING_SLOT_COUNT {
            return Err(RoutingError::InvalidSlotRange { start, end });
        }
        let start_hash = u128::from(start) << ROUTING_SLOT_SHIFT;
        let end_hash = if end == ROUTING_SLOT_COUNT {
            u128::MAX
        } else {
            (u128::from(end) << ROUTING_SLOT_SHIFT) - 1
        };
        Ok(Self {
            start: HashValue(start_hash),
            end: HashValue(end_hash),
        })
    }

    pub fn slots(self) -> Result<std::ops::Range<u16>, RoutingError> {
        let start = self.start.routing_slot().get();
        if self.start.get() != u128::from(start) << ROUTING_SLOT_SHIFT {
            return Err(RoutingError::UnalignedRange { range: self });
        }
        let end = if self.end == HashValue::MAX {
            ROUTING_SLOT_COUNT
        } else {
            let next = self.end.get() + 1;
            let slot = (next >> ROUTING_SLOT_SHIFT) as u16;
            if next != u128::from(slot) << ROUTING_SLOT_SHIFT {
                return Err(RoutingError::UnalignedRange { range: self });
            }
            slot
        };
        Ok(start..end)
    }

    fn split(self) -> Result<(Self, Self), RoutingError> {
        let slots = self.slots()?;
        if slots.end - slots.start < 2 {
            return Err(RoutingError::UnsplittableRange { range: self });
        }
        let midpoint = slots.start + (slots.end - slots.start) / 2;
        Ok((
            Self::from_slots(slots.start, midpoint)?,
            Self::from_slots(midpoint, slots.end)?,
        ))
    }

    pub const fn size_minus_one(self) -> u128 {
        self.end.0 - self.start.0
    }
}

impl fmt::Display for HashRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..={}", self.start, self.end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct RoutingGeneration(u64);

impl RoutingGeneration {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HashRangeAssignment {
    pub shard: ShardId,
    pub range: HashRange,
}

impl HashRangeAssignment {
    pub const fn new(shard: ShardId, range: HashRange) -> Self {
        Self { shard, range }
    }
}

/// Versioned routing metadata mapping stable hash ranges to storage shards.
///
/// Writer ownership remains a separate concern in [`crate::ShardMap`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HashRangeMap {
    pub generation: RoutingGeneration,
    pub assignments: Vec<HashRangeAssignment>,
}

impl<'de> Deserialize<'de> for HashRangeMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireMap {
            generation: RoutingGeneration,
            assignments: Vec<HashRangeAssignment>,
        }

        let map = WireMap::deserialize(deserializer)?;
        Self::new(map.generation, map.assignments).map_err(de::Error::custom)
    }
}

impl HashRangeMap {
    pub fn new(
        generation: RoutingGeneration,
        mut assignments: Vec<HashRangeAssignment>,
    ) -> Result<Self, RoutingError> {
        assignments.sort_by_key(|assignment| assignment.range.start());
        if assignments.is_empty() {
            return Err(RoutingError::NoRanges);
        }
        for assignment in &assignments {
            assignment.range.slots()?;
        }
        if assignments[0].range.start() != HashValue::MIN {
            return Err(RoutingError::IncompleteCoverage);
        }

        for pair in assignments.windows(2) {
            let previous = pair[0].range;
            let next = pair[1].range;
            let expected = previous
                .end()
                .get()
                .checked_add(1)
                .ok_or(RoutingError::Overlap { previous, next })?;
            if next.start().get() < expected {
                return Err(RoutingError::Overlap { previous, next });
            }
            if next.start().get() > expected {
                return Err(RoutingError::Gap { previous, next });
            }
        }

        if assignments
            .last()
            .is_none_or(|assignment| assignment.range.end() != HashValue::MAX)
        {
            return Err(RoutingError::IncompleteCoverage);
        }

        let mut shard_ids = assignments
            .iter()
            .map(|assignment| assignment.shard)
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids.dedup();
        if shard_ids.len() != assignments.len() {
            return Err(RoutingError::DuplicateShard);
        }

        Ok(Self {
            generation,
            assignments,
        })
    }

    /// Builds deterministic, contiguous ranges over fixed routing slots.
    pub fn bootstrap(shards: u32) -> Result<Self, RoutingError> {
        if shards == 0 {
            return Err(RoutingError::NoRanges);
        }
        if shards > u32::from(ROUTING_SLOT_COUNT) {
            return Err(RoutingError::TooManyShards {
                requested: shards,
                maximum: ROUTING_SLOT_COUNT,
            });
        }
        let slots = u32::from(ROUTING_SLOT_COUNT);
        let base_size = slots / shards;
        let extra_ranges = slots % shards;
        let mut start = 0_u16;
        let mut assignments = Vec::with_capacity(shards as usize);
        for shard in 0..shards {
            let size = base_size + u32::from(shard < extra_ranges);
            let end = start + size as u16;
            assignments.push(HashRangeAssignment::new(
                ShardId::new(shard),
                HashRange::from_slots(start, end)?,
            ));
            start = end;
        }
        Self::new(RoutingGeneration::new(1), assignments)
    }

    pub fn route(&self, value: HashValue) -> ShardId {
        let index = self
            .assignments
            .partition_point(|assignment| assignment.range.end() < value);
        self.assignments[index].shard
    }

    /// Splits exactly one storage shard at its hash-range midpoint.
    ///
    /// The existing shard retains the lower half and `new_shard` receives the
    /// upper half. Every other assignment is preserved byte-for-byte.
    pub fn split(&self, shard: ShardId, new_shard: ShardId) -> Result<Self, RoutingError> {
        if self
            .assignments
            .iter()
            .any(|assignment| assignment.shard == new_shard)
        {
            return Err(RoutingError::DuplicateShard);
        }

        let mut assignments = Vec::with_capacity(self.assignments.len() + 1);
        let mut found = false;
        for assignment in &self.assignments {
            if assignment.shard != shard {
                assignments.push(*assignment);
                continue;
            }
            let (left, right) = assignment.range.split()?;
            assignments.push(HashRangeAssignment::new(shard, left));
            assignments.push(HashRangeAssignment::new(new_shard, right));
            found = true;
        }
        if !found {
            return Err(RoutingError::UnknownShard(shard));
        }
        Self::new(self.generation.next(), assignments)
    }

    /// Grows this routing map to `desired_shards` by repeatedly splitting the
    /// largest range. Equal-size ranges favor the lowest stable shard id.
    pub fn grow_to(&self, desired_shards: u32) -> Result<Self, RoutingError> {
        let current = u32::try_from(self.assignments.len()).expect("shard count fits in u32");
        if desired_shards < current {
            return Err(RoutingError::ScaleDownUnsupported {
                current,
                desired: desired_shards,
            });
        }

        let mut map = self.clone();
        let mut next_shard = map
            .assignments
            .iter()
            .map(|assignment| assignment.shard.get())
            .max()
            .expect("validated routing map is non-empty")
            .checked_add(1)
            .ok_or(RoutingError::ShardIdExhausted)?;
        while map.assignments.len() < desired_shards as usize {
            let source = map
                .assignments
                .iter()
                .max_by(|left, right| {
                    left.range
                        .size_minus_one()
                        .cmp(&right.range.size_minus_one())
                        .then_with(|| right.shard.cmp(&left.shard))
                })
                .expect("validated routing map is non-empty")
                .shard;
            map = map.split(source, ShardId::new(next_shard))?;
            next_shard = next_shard
                .checked_add(1)
                .ok_or(RoutingError::ShardIdExhausted)?;
        }
        Ok(map)
    }
}

/// Hashes a canonical product routing key into the shared 128-bit space.
pub fn hash_routing_key(key: &[u8]) -> HashValue {
    let digest = blake3::hash(key);
    HashValue::new(u128::from_be_bytes(
        digest.as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest has at least 16 bytes"),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoutingError {
    #[error("at least one hash range is required")]
    NoRanges,
    #[error("hash range is reversed: {start}..={end}")]
    ReversedRange { start: HashValue, end: HashValue },
    #[error("hash range {range} contains only one value and cannot be split")]
    UnsplittableRange { range: HashRange },
    #[error("routing slot range must satisfy 0 <= start < end <= 4096, got {start}..{end}")]
    InvalidSlotRange { start: u16, end: u16 },
    #[error("hash range {range} is not aligned to routing slot boundaries")]
    UnalignedRange { range: HashRange },
    #[error("requested {requested} shards, but only {maximum} routing slots exist")]
    TooManyShards { requested: u32, maximum: u16 },
    #[error("hash ranges overlap: {previous} and {next}")]
    Overlap {
        previous: HashRange,
        next: HashRange,
    },
    #[error("gap between hash ranges {previous} and {next}")]
    Gap {
        previous: HashRange,
        next: HashRange,
    },
    #[error("hash ranges do not cover the full 128-bit space")]
    IncompleteCoverage,
    #[error("storage shard {0} does not exist")]
    UnknownShard(ShardId),
    #[error("a storage shard may own only one hash range")]
    DuplicateShard,
    #[error(
        "decreasing storage shard count from {current} to {desired} is unsupported; ranges may only grow by splitting"
    )]
    ScaleDownUnsupported { current: u32, desired: u32 },
    #[error("cannot allocate another storage shard id")]
    ShardIdExhausted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_covers_the_complete_hash_space() {
        for count in [1, 2, 3, 8, 64, 255] {
            let map = HashRangeMap::bootstrap(count).unwrap();
            assert_eq!(map.assignments.len(), count as usize);
            assert_eq!(map.assignments[0].range.start(), HashValue::MIN);
            assert_eq!(map.assignments.last().unwrap().range.end(), HashValue::MAX);
            for pair in map.assignments.windows(2) {
                assert_eq!(pair[0].range.end().get() + 1, pair[1].range.start().get());
            }
            assert!(
                map.assignments
                    .iter()
                    .all(|assignment| assignment.range.slots().is_ok())
            );
        }
        assert!(matches!(
            HashRangeMap::bootstrap(u32::from(ROUTING_SLOT_COUNT) + 1),
            Err(RoutingError::TooManyShards { .. })
        ));
    }

    #[test]
    fn split_moves_only_the_selected_ranges_upper_half() {
        let original = HashRangeMap::bootstrap(4).unwrap();
        let split = original.split(ShardId::new(1), ShardId::new(4)).unwrap();

        assert_eq!(split.generation, original.generation.next());
        assert_eq!(split.assignments.len(), 5);
        for assignment in original
            .assignments
            .iter()
            .filter(|assignment| assignment.shard != ShardId::new(1))
        {
            assert!(split.assignments.contains(assignment));
            assert_eq!(split.route(assignment.range.start()), assignment.shard);
            assert_eq!(split.route(assignment.range.end()), assignment.shard);
        }

        let old = original.assignments[1].range;
        let retained = split
            .assignments
            .iter()
            .find(|assignment| assignment.shard == ShardId::new(1))
            .unwrap()
            .range;
        let created = split
            .assignments
            .iter()
            .find(|assignment| assignment.shard == ShardId::new(4))
            .unwrap()
            .range;
        assert_eq!(retained.start(), old.start());
        assert_eq!(retained.end().get() + 1, created.start().get());
        assert_eq!(created.end(), old.end());
    }

    #[test]
    fn serialized_boundaries_are_fixed_width_hex() {
        let map = HashRangeMap::bootstrap(2).unwrap();
        let encoded = serde_json::to_string(&map).unwrap();
        assert!(encoded.contains(r#""start":"00000000000000000000000000000000""#));
        assert!(encoded.contains(r#""end":"ffffffffffffffffffffffffffffffff""#));
        assert_eq!(serde_json::from_str::<HashRangeMap>(&encoded).unwrap(), map);
    }

    #[test]
    fn routing_slots_use_the_high_twelve_hash_bits() {
        assert_eq!(HashValue::MIN.routing_slot().get(), 0);
        assert_eq!(HashValue::MAX.routing_slot().get(), ROUTING_SLOT_COUNT - 1);
        let range = HashRange::from_slots(17, 29).unwrap();
        assert_eq!(range.slots().unwrap(), 17..29);
    }

    #[test]
    fn growth_is_deterministic_and_only_splits_one_source_per_step() {
        let original = HashRangeMap::bootstrap(3).unwrap();
        let first = original.grow_to(4).unwrap();
        let second = original.grow_to(4).unwrap();
        assert_eq!(first, second);

        let changed = original
            .assignments
            .iter()
            .filter(|assignment| !first.assignments.contains(assignment))
            .collect::<Vec<_>>();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].shard, ShardId::new(0));
        assert!(matches!(
            first.grow_to(2),
            Err(RoutingError::ScaleDownUnsupported {
                current: 4,
                desired: 2
            })
        ));
    }

    #[test]
    fn adding_one_shard_only_remaps_keys_from_the_split_source() {
        let original = HashRangeMap::bootstrap(4).unwrap();
        let grown = original.grow_to(5).unwrap();
        let source = ShardId::new(0);
        let mut moved = 0;
        for key in 0_u32..100_000 {
            let hash = hash_routing_key(&key.to_be_bytes());
            let before = original.route(hash);
            let after = grown.route(hash);
            if before != after {
                moved += 1;
                assert_eq!(before, source);
                assert_eq!(after, ShardId::new(4));
            }
        }
        assert!(moved > 0);
    }

    #[test]
    fn rejects_gaps_overlaps_and_duplicate_shards() {
        let owner = |shard, start, end| {
            HashRangeAssignment::new(
                ShardId::new(shard),
                HashRange::from_slots(start, end).unwrap(),
            )
        };
        assert!(matches!(
            HashRangeMap::new(
                RoutingGeneration::new(1),
                vec![owner(0, 0, 1), owner(1, 2, ROUTING_SLOT_COUNT)]
            ),
            Err(RoutingError::Gap { .. })
        ));
        assert!(matches!(
            HashRangeMap::new(
                RoutingGeneration::new(1),
                vec![owner(0, 0, 2), owner(1, 1, ROUTING_SLOT_COUNT)]
            ),
            Err(RoutingError::Overlap { .. })
        ));
        assert!(matches!(
            HashRangeMap::new(
                RoutingGeneration::new(1),
                vec![owner(0, 0, 1), owner(0, 1, ROUTING_SLOT_COUNT)]
            ),
            Err(RoutingError::DuplicateShard)
        ));
    }
}
