//! Cluster topology: the shards, the namespace-id range each one holds, and
//! which writer owns it.
//!
//! Every namespace has a fixed 64-bit id, a hash of its name. Each active
//! shard holds one contiguous range of that id space, and the active ranges
//! partition it, so a namespace's shard follows from its name alone.
//!
//! Each writer owns exactly one active shard, and each active shard one
//! writer. A writer added to the cluster gets its shard by a split: the
//! coordinator marks an active shard [`ShardMeta::split_to`] the new writer,
//! and the shard's owner splits it, keeping the lower child and handing the
//! upper one over.
//!
//! The topology is a single versioned document. Standalone clusters keep it
//! in their catalog database; on Kubernetes it is a `VectorTopology` custom
//! resource. Either way it changes only by compare-and-swap on
//! [`Topology::generation`].

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};
use crate::storage::namespaced::NamespaceId;

/// Lifecycle of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardState {
    /// Serving its namespace range.
    Active,
    /// Split into children; kept because the children's clones reference
    /// its SSTs.
    Retired,
}

/// A namespace id, written as 16 hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HexId(pub u64);

impl fmt::Display for HexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl Serialize for HexId {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HexId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.len() != 16 {
            return Err(D::Error::custom(format!(
                "namespace id '{text}' must be 16 hex digits"
            )));
        }
        u64::from_str_radix(&text, 16)
            .map(HexId)
            .map_err(|e| D::Error::custom(format!("namespace id '{text}': {e}")))
    }
}

/// The namespace ids `[start, end)`; no `end` means the rest of the space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceRange {
    pub start: HexId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<HexId>,
}

impl NamespaceRange {
    pub const FULL: Self = Self {
        start: HexId(0),
        end: None,
    };

    pub fn contains(&self, id: NamespaceId) -> bool {
        let id = id.as_u64();
        id >= self.start.0 && self.end.is_none_or(|end| id < end.0)
    }

    /// The range's bounds as namespace ids, for key-range projection.
    pub(crate) fn bounds(&self) -> (Option<NamespaceId>, Option<NamespaceId>) {
        let start = (self.start.0 > 0).then(|| NamespaceId::from_u64(self.start.0));
        (start, self.end.map(|end| NamespaceId::from_u64(end.0)))
    }

    /// The middle of the range, where an empty shard splits.
    pub(crate) fn midpoint(&self) -> u64 {
        let end = self.end.map_or(u64::MAX, |end| end.0);
        self.start.0 + (end - self.start.0) / 2
    }

    fn width(&self) -> u64 {
        self.end.map_or(u64::MAX, |end| end.0) - self.start.0
    }

    fn split_at(&self, at: u64) -> Option<(Self, Self)> {
        let inside = at > self.start.0 && self.end.is_none_or(|end| at < end.0);
        inside.then_some((
            Self {
                start: self.start,
                end: Some(HexId(at)),
            },
            Self {
                start: HexId(at),
                end: self.end,
            },
        ))
    }
}

/// Persistent description of a shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMeta {
    pub id: u32,
    pub state: ShardState,
    /// The namespace ids this shard holds while active.
    pub range: NamespaceRange,
    /// Writer replica that owns the shard; on standalone clusters, the one
    /// process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// A writer without a shard that this one is to be split for: the
    /// owner splits it and hands the upper child to this writer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_to: Option<String>,
    /// The shard this one was split from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<u32>,
    /// Shards this one was split into.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<u32>,
    pub created_at_ms: u64,
}

/// Every shard and the shard id allocator, at one generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    pub generation: u64,
    pub next_shard_id: u32,
    pub shards: Vec<ShardMeta>,
}

impl Topology {
    /// One shard per writer of `owners`, evenly dividing the namespace-id
    /// space; one unowned shard when there are none.
    pub fn initial(owners: &[String], now_ms: u64) -> Self {
        let count = owners.len().max(1) as u32;
        let width = u64::MAX / u64::from(count);
        let shards = (0..count)
            .map(|id| ShardMeta {
                id,
                state: ShardState::Active,
                range: NamespaceRange {
                    start: HexId(u64::from(id) * width),
                    end: (id + 1 < count).then(|| HexId(u64::from(id + 1) * width)),
                },
                owner: owners.get(id as usize).cloned(),
                split_to: None,
                parent: None,
                children: Vec::new(),
                created_at_ms: now_ms,
            })
            .collect();
        Self {
            generation: 1,
            next_shard_id: count,
            shards,
        }
    }

    pub fn shard(&self, id: u32) -> Option<&ShardMeta> {
        self.shards.iter().find(|s| s.id == id)
    }

    fn shard_mut(&mut self, id: u32) -> Option<&mut ShardMeta> {
        self.shards.iter_mut().find(|s| s.id == id)
    }

    pub fn active(&self) -> impl Iterator<Item = &ShardMeta> {
        self.shards.iter().filter(|s| s.state == ShardState::Active)
    }

    /// The active shard holding namespace `id`.
    pub fn shard_for(&self, id: NamespaceId) -> Option<&ShardMeta> {
        self.active().find(|s| s.range.contains(id))
    }

    /// The active shard owned by `writer`.
    pub fn owned_by(&self, writer: &str) -> Option<&ShardMeta> {
        self.active().find(|s| s.owner.as_deref() == Some(writer))
    }

    /// Checks that shard ids are unique and below `next_shard_id`, that the
    /// active ranges partition the namespace-id space, and that no writer
    /// owns or is to receive more than one active shard.
    pub fn validate(&self) -> Result<()> {
        let invalid = |why: String| Err(Error::Internal(format!("invalid topology: {why}")));
        let mut ids = HashSet::new();
        for shard in &self.shards {
            if !ids.insert(shard.id) || shard.id >= self.next_shard_id {
                return invalid(format!(
                    "shard id {} is duplicated or unallocated",
                    shard.id
                ));
            }
        }
        let mut writers = HashSet::new();
        for shard in self.active() {
            for writer in shard.owner.iter().chain(&shard.split_to) {
                if !writers.insert(writer.as_str()) {
                    return invalid(format!("writer {writer} has more than one shard"));
                }
            }
        }
        let mut active: Vec<&ShardMeta> = self.active().collect();
        active.sort_by_key(|s| s.range.start);
        let mut expected = Some(0u64);
        for shard in active {
            if expected != Some(shard.range.start.0) {
                return invalid(format!(
                    "shard {} does not start where the last ended",
                    shard.id
                ));
            }
            expected = shard.range.end.map(|end| end.0);
            if expected.is_some_and(|end| end <= shard.range.start.0) {
                return invalid(format!("shard {} has an empty range", shard.id));
            }
        }
        if expected.is_some() {
            return invalid("the active ranges stop short of the end".into());
        }
        Ok(())
    }

    /// Reserves `n` shard ids, returning the first.
    pub fn reserve_ids(&self, n: u32) -> (Self, u32) {
        let mut next = self.clone();
        next.generation += 1;
        next.next_shard_id += n;
        (next, self.next_shard_id)
    }

    /// Retires shard `id` in favour of `left` and `right`, which take its
    /// range below and from `at`. The parent's owner keeps `left`; `right`
    /// goes to the writer the parent was split for.
    pub fn split(&self, id: u32, left: u32, right: u32, at: u64, now_ms: u64) -> Result<Self> {
        let parent = self
            .shard(id)
            .filter(|s| s.state == ShardState::Active)
            .ok_or_else(|| Error::Conflict(format!("shard {id} is not active")))?
            .clone();
        let receiver = parent
            .split_to
            .clone()
            .ok_or_else(|| Error::Conflict(format!("shard {id} is not to be split")))?;
        let (low, high) = parent.range.split_at(at).ok_or_else(|| {
            Error::Internal(format!("split point {at:016x} is outside shard {id}"))
        })?;
        if left.max(right) >= self.next_shard_id || self.shard(left).or(self.shard(right)).is_some()
        {
            return Err(Error::Internal(format!(
                "split children {left} and {right} were not reserved"
            )));
        }
        let mut next = self.clone();
        next.generation += 1;
        let retired = next.shard_mut(id).expect("checked above");
        retired.state = ShardState::Retired;
        retired.split_to = None;
        retired.children = vec![left, right];
        for (child, range, owner) in [(left, low, parent.owner), (right, high, Some(receiver))] {
            next.shards.push(ShardMeta {
                id: child,
                state: ShardState::Active,
                range,
                owner,
                split_to: None,
                parent: Some(id),
                children: Vec::new(),
                created_at_ms: now_ms,
            });
        }
        next.shards.sort_by_key(|s| s.id);
        next.validate()?;
        Ok(next)
    }

    /// Plans a split for every writer of `ready` without a shard: the
    /// widest active shards whose owners are ready are marked
    /// [`ShardMeta::split_to`] them, in order. Splits planned for writers no
    /// longer ready are withdrawn. `None` when nothing changes.
    pub fn plan(&self, ready: &[String]) -> Option<Self> {
        let mut next = self.clone();
        let mut changed = false;
        for shard in next.shards.iter_mut() {
            if shard
                .split_to
                .as_ref()
                .is_some_and(|to| !ready.contains(to))
            {
                shard.split_to = None;
                changed = true;
            }
        }
        let placed: HashSet<&str> = next
            .active()
            .flat_map(|s| s.owner.iter().chain(&s.split_to))
            .map(String::as_str)
            .collect();
        let idle: Vec<String> = ready
            .iter()
            .filter(|w| !placed.contains(w.as_str()))
            .cloned()
            .collect();
        let mut candidates: Vec<(u64, u32)> = next
            .active()
            .filter(|s| s.split_to.is_none())
            .filter(|s| s.owner.as_ref().is_some_and(|o| ready.contains(o)))
            .map(|s| (s.range.width(), s.id))
            .collect();
        candidates.sort_by_key(|&(width, id)| (std::cmp::Reverse(width), id));
        let planned: BTreeMap<u32, String> =
            candidates.into_iter().map(|(_, id)| id).zip(idle).collect();
        for (id, writer) in planned {
            next.shard_mut(id).expect("active shard").split_to = Some(writer);
            changed = true;
        }
        changed.then(|| {
            next.generation += 1;
            next
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writers(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn owners(topology: &Topology) -> Vec<Option<&str>> {
        topology.active().map(|s| s.owner.as_deref()).collect()
    }

    fn split_to(topology: &Topology) -> Vec<Option<&str>> {
        topology.active().map(|s| s.split_to.as_deref()).collect()
    }

    #[test]
    fn should_give_each_writer_a_shard_initially() {
        let topology = Topology::initial(&writers(&["a", "b", "c"]), 0);
        topology.validate().unwrap();
        assert_eq!(topology.next_shard_id, 3);
        assert_eq!(owners(&topology), vec![Some("a"), Some("b"), Some("c")]);
        let ids = [0u64, u64::MAX / 3, u64::MAX / 3 * 2, u64::MAX - 1];
        let shards: Vec<u32> = ids
            .iter()
            .map(|id| topology.shard_for(NamespaceId::from_u64(*id)).unwrap().id)
            .collect();
        assert_eq!(shards, vec![0, 1, 2, 2]);
        assert_eq!(owners(&Topology::initial(&[], 0)), vec![None]);
    }

    #[test]
    fn should_split_a_shard_for_the_writer_it_was_planned_for() {
        let topology = Topology::initial(&writers(&["w-0"]), 0);
        assert!(
            topology.split(0, 1, 2, 1 << 63, 5).is_err(),
            "no split planned"
        );
        let planned = topology.plan(&writers(&["w-0", "w-1"])).unwrap();
        let (reserved, left) = planned.reserve_ids(2);
        assert_eq!((left, reserved.next_shard_id), (1, 3));
        let split = reserved.split(0, 1, 2, 1 << 63, 5).unwrap();
        assert_eq!(split.generation, 4);
        let parent = split.shard(0).unwrap();
        assert_eq!(parent.state, ShardState::Retired);
        assert_eq!(parent.children, vec![1, 2]);
        assert_eq!(parent.split_to, None);
        assert_eq!(split.shard_for(NamespaceId::from_u64(5)).unwrap().id, 1);
        assert_eq!(
            split.shard_for(NamespaceId::from_u64(1 << 63)).unwrap().id,
            2
        );
        assert_eq!(owners(&split), vec![Some("w-0"), Some("w-1")]);
        assert!(split.plan(&writers(&["w-0", "w-1"])).is_none());

        assert!(
            reserved.split(0, 1, 2, 0, 5).is_err(),
            "split point at start"
        );
        assert!(
            planned.split(0, 1, 2, 1 << 63, 5).is_err(),
            "unreserved ids"
        );
        assert!(split.split(0, 3, 4, 1, 5).is_err(), "retired shard");
    }

    #[test]
    fn should_plan_splits_of_the_widest_shards_for_ready_writers() {
        let topology = Topology::initial(&writers(&["a", "b"]), 0);
        assert!(topology.plan(&writers(&["a", "b"])).is_none());

        // two new writers each get a split of a different shard, the wider
        // (upper) one first
        let planned = topology.plan(&writers(&["a", "b", "c", "d"])).unwrap();
        assert_eq!(split_to(&planned), vec![Some("d"), Some("c")]);
        assert!(planned.plan(&writers(&["a", "b", "c", "d"])).is_none());

        // more new writers than shards to split: the rest wait
        let crowded = topology.plan(&writers(&["a", "b", "c", "d", "e"])).unwrap();
        assert_eq!(split_to(&crowded), vec![Some("d"), Some("c")]);

        // a shard whose owner isn't ready can't split
        let partial = topology.plan(&writers(&["b", "c"])).unwrap();
        assert_eq!(split_to(&partial), vec![None, Some("c")]);

        // a split for a writer no longer ready is withdrawn, and goes to
        // another idle writer
        let moved = planned.plan(&writers(&["a", "b", "d", "e"])).unwrap();
        assert_eq!(split_to(&moved), vec![Some("d"), Some("e")]);
        moved.validate().unwrap();
    }

    #[test]
    fn should_reject_writers_with_two_shards() {
        let mut topology = Topology::initial(&writers(&["a", "b"]), 0);
        topology.shards[1].owner = Some("a".into());
        assert!(topology.validate().is_err());
        let mut topology = Topology::initial(&writers(&["a", "b"]), 0);
        topology.shards[1].split_to = Some("a".into());
        assert!(topology.validate().is_err());
    }

    #[test]
    fn should_round_trip_through_json() {
        let topology = Topology::initial(&writers(&["w-0", "w-1"]), 7)
            .plan(&writers(&["w-0", "w-1", "w-2"]))
            .unwrap();
        let json = serde_json::to_value(&topology).unwrap();
        assert_eq!(json["shards"][1]["range"]["start"], "7fffffffffffffff");
        assert!(json["shards"][1]["range"].get("end").is_none());
        assert!(json["shards"][0].get("split_to").is_none());
        assert_eq!(json["shards"][1]["split_to"], "w-2");
        let back: Topology = serde_json::from_value(json).unwrap();
        assert_eq!(back, topology);
    }
}
