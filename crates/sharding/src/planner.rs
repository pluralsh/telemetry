use std::collections::HashSet;

use crate::{
    Assignment, AssignmentGeneration, AssignmentState, ModelError, Owner, RoutingEpoch, ShardMap,
    ShardRange,
};

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("at least one owner is required")]
    NoOwners,
    #[error("{owners} owners cannot receive non-empty ranges from {shard_count} shards")]
    TooManyOwners { owners: usize, shard_count: u32 },
    #[error("owner ids and ordinals must both be unique")]
    DuplicateOwner,
    #[error("at least one routing epoch is required")]
    NoEpochs,
    #[error(transparent)]
    InvalidMap(#[from] ModelError),
}

#[derive(Clone)]
struct Candidate {
    retained: u32,
    extras: Vec<bool>,
}

impl Candidate {
    fn extend(&self, extra: bool, retained: u32) -> Self {
        let mut extras = self.extras.clone();
        extras.push(extra);
        Self {
            retained: self.retained + retained,
            extras,
        }
    }
}

/// Plans balanced, contiguous ownership of every shard referenced by `epochs`
/// in stable ordinal order.
///
/// Every owner receives either `floor(shards / owners)` or one additional
/// shard. When there is a choice about which owners receive the remainder,
/// the planner maximizes shards retained by the same stable owner from the
/// previous map. Ties favor lower StatefulSet ordinals.
pub fn balanced_contiguous(
    generation: AssignmentGeneration,
    epochs: Vec<RoutingEpoch>,
    owners: &[Owner],
    previous: Option<&ShardMap>,
) -> Result<ShardMap, PlanError> {
    let shard_count = epochs
        .last()
        .map(RoutingEpoch::shard_count)
        .ok_or(PlanError::NoEpochs)?;
    if owners.is_empty() {
        return Err(PlanError::NoOwners);
    }
    if owners.len() > shard_count as usize {
        return Err(PlanError::TooManyOwners {
            owners: owners.len(),
            shard_count,
        });
    }
    let mut owners = owners.to_vec();
    owners.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
    });
    let ids = owners
        .iter()
        .map(|owner| owner.id.as_str())
        .collect::<HashSet<_>>();
    let ordinals = owners
        .iter()
        .map(|owner| owner.ordinal)
        .collect::<HashSet<_>>();
    if ids.len() != owners.len() || ordinals.len() != owners.len() {
        return Err(PlanError::DuplicateOwner);
    }

    let count = owners.len() as u32;
    let base = shard_count / count;
    let remainder = (shard_count % count) as usize;
    let extras = choose_extras(&owners, base, remainder, previous);
    let mut start = 0;
    let assignments = owners
        .into_iter()
        .zip(extras)
        .map(|(owner, extra)| {
            let end = start + base + u32::from(extra);
            let range =
                ShardRange::within(start, end, shard_count).expect("planner creates valid ranges");
            start = end;
            Assignment::new(owner, range, AssignmentState::Active)
        })
        .collect();
    ShardMap::with_epochs(generation, epochs, assignments).map_err(PlanError::from)
}

/// Chooses which owners receive one of the `remainder` extra shards,
/// maximizing shards retained from `previous`. Dynamic programming over
/// owners in order, indexed by the number of extras handed out so far.
fn choose_extras(
    owners: &[Owner],
    base: u32,
    remainder: usize,
    previous: Option<&ShardMap>,
) -> Vec<bool> {
    let mut choices: Vec<Option<Candidate>> = vec![None; remainder + 1];
    choices[0] = Some(Candidate {
        retained: 0,
        extras: Vec::new(),
    });
    for (index, owner) in owners.iter().enumerate() {
        let mut next = vec![None; remainder + 1];
        let options = choices
            .iter()
            .enumerate()
            .filter_map(|(used, candidate)| Some((used, candidate.as_ref()?)))
            .flat_map(|(used, candidate)| [false, true].map(|extra| (used, candidate, extra)))
            .filter(|(used, _, extra)| used + usize::from(*extra) <= remainder);
        for (used, candidate, extra) in options {
            let start = index as u32 * base + used as u32;
            let end = start + base + u32::from(extra);
            let proposed =
                candidate.extend(extra, retained_shards(previous, &owner.id, start, end));
            let slot = &mut next[used + usize::from(extra)];
            if is_better(&proposed, slot.as_ref()) {
                *slot = Some(proposed);
            }
        }
        choices = next;
    }
    choices
        .swap_remove(remainder)
        .expect("balanced partition always has a solution")
        .extras
}

fn retained_shards(previous: Option<&ShardMap>, owner_id: &str, start: u32, end: u32) -> u32 {
    let Some(previous) = previous else {
        return 0;
    };
    previous
        .assignments_for(owner_id)
        .map(|assignment| {
            let overlap_start = start.max(assignment.range.start().get());
            let overlap_end = end.min(assignment.range.end().get());
            overlap_end.saturating_sub(overlap_start)
        })
        .sum()
}

fn is_better(proposed: &Candidate, current: Option<&Candidate>) -> bool {
    let Some(current) = current else {
        return true;
    };
    proposed.retained > current.retained
        || (proposed.retained == current.retained && proposed.extras > current.extras)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EpochPolicy, ShardId};

    fn owners(count: u32) -> Vec<Owner> {
        (0..count)
            .map(|ordinal| Owner::new(format!("metrics-{ordinal}"), ordinal))
            .collect()
    }

    fn epochs(shards: u32) -> Vec<RoutingEpoch> {
        ShardMap::initial_epochs(shards).unwrap()
    }

    fn movement(previous: &ShardMap, next: &ShardMap) -> u32 {
        (0..previous.shard_count)
            .filter(|shard| {
                previous.owner_of(ShardId::new(*shard)) != next.owner_of(ShardId::new(*shard))
            })
            .count() as u32
    }

    #[test]
    fn scales_one_to_many_to_one_with_exact_balanced_coverage() {
        let one = balanced_contiguous(AssignmentGeneration::new(1), epochs(64), &owners(1), None)
            .unwrap();
        let four = balanced_contiguous(
            AssignmentGeneration::new(2),
            epochs(64),
            &owners(4),
            Some(&one),
        )
        .unwrap();
        assert_eq!(
            four.assignments
                .iter()
                .map(|assignment| assignment.range.len())
                .collect::<Vec<_>>(),
            vec![16, 16, 16, 16]
        );
        assert_eq!(movement(&one, &four), 48);

        let one_again = balanced_contiguous(
            AssignmentGeneration::new(3),
            epochs(64),
            &owners(1),
            Some(&four),
        )
        .unwrap();
        assert_eq!(one_again.assignments[0].range.len(), 64);
        assert_eq!(movement(&four, &one_again), 48);
    }

    #[test]
    fn minimizes_movement_when_placing_remainder() {
        let previous = ShardMap::new(
            AssignmentGeneration::new(1),
            8,
            vec![
                Assignment::new(
                    Owner::new("metrics-0", 0),
                    ShardRange::within(0, 2, 8).unwrap(),
                    AssignmentState::Active,
                ),
                Assignment::new(
                    Owner::new("metrics-1", 1),
                    ShardRange::within(2, 5, 8).unwrap(),
                    AssignmentState::Active,
                ),
                Assignment::new(
                    Owner::new("metrics-2", 2),
                    ShardRange::within(5, 8, 8).unwrap(),
                    AssignmentState::Active,
                ),
            ],
        )
        .unwrap();
        let next = balanced_contiguous(
            AssignmentGeneration::new(2),
            epochs(8),
            &owners(3),
            Some(&previous),
        )
        .unwrap();
        assert_eq!(
            next.assignments
                .iter()
                .map(|assignment| assignment.range.len())
                .collect::<Vec<_>>(),
            vec![2, 3, 3]
        );
        assert_eq!(movement(&previous, &next), 0);
    }

    #[test]
    fn output_is_deterministic_and_ordinal_ordered() {
        let unordered = vec![
            Owner::new("metrics-2", 2),
            Owner::new("metrics-0", 0),
            Owner::new("metrics-1", 1),
        ];
        let first = balanced_contiguous(AssignmentGeneration::new(1), epochs(64), &unordered, None)
            .unwrap();
        let second =
            balanced_contiguous(AssignmentGeneration::new(1), epochs(64), &unordered, None)
                .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.assignments[0].owner.id, "metrics-0");
    }

    #[test]
    fn scale_up_keeps_existing_owners_and_assigns_new_shard_to_new_owner() {
        let two =
            balanced_contiguous(AssignmentGeneration::new(1), epochs(2), &owners(2), None).unwrap();
        let scaled = two.epochs_scaled_to(3, EpochPolicy::default(), 0).unwrap();
        let three =
            balanced_contiguous(AssignmentGeneration::new(2), scaled, &owners(3), Some(&two))
                .unwrap();
        assert_eq!(three.epochs.len(), 2);
        assert_eq!(three.epochs[0], two.epochs[0]);
        assert_eq!(movement(&two, &three), 0);
        assert_eq!(three.owner_of(ShardId::new(2)).unwrap().id, "metrics-2");
    }
}
