use std::collections::HashSet;

use crate::{
    Assignment, AssignmentGeneration, AssignmentState, HashRangeMap, ModelError, Owner,
    RoutingError, ShardMap, ShardRange,
};

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("at least one owner is required")]
    NoOwners,
    #[error("{owners} owners cannot receive non-empty ranges from {virtual_shards} shards")]
    TooManyOwners { owners: usize, virtual_shards: u32 },
    #[error("owner ids and ordinals must both be unique")]
    DuplicateOwner,
    #[error(
        "changing storage shard count from {current} to {desired} requires a split/backfill/cutover migration"
    )]
    ShardCountChangeRequiresMigration { current: u32, desired: u32 },
    #[error(transparent)]
    InvalidMap(#[from] ModelError),
    #[error(transparent)]
    InvalidRouting(#[from] RoutingError),
}

#[derive(Clone)]
struct Candidate {
    retained: u32,
    extras: Vec<bool>,
}

/// Plans balanced, contiguous ranges in stable ordinal order.
///
/// Every owner receives either `floor(shards / owners)` or one additional
/// shard. When there is a choice about which owners receive the remainder,
/// the planner maximizes shards retained by the same stable owner from the
/// previous map. Ties favor lower StatefulSet ordinals.
pub fn balanced_contiguous(
    generation: AssignmentGeneration,
    virtual_shards: u32,
    owners: &[Owner],
    previous: Option<&ShardMap>,
) -> Result<ShardMap, PlanError> {
    if owners.is_empty() {
        return Err(PlanError::NoOwners);
    }
    if owners.len() > virtual_shards as usize {
        return Err(PlanError::TooManyOwners {
            owners: owners.len(),
            virtual_shards,
        });
    }
    let routing = match previous {
        Some(previous) if previous.virtual_shards != virtual_shards => {
            return Err(PlanError::ShardCountChangeRequiresMigration {
                current: previous.virtual_shards,
                desired: virtual_shards,
            });
        }
        Some(previous) => previous.routing.clone(),
        None => HashRangeMap::bootstrap(virtual_shards)?,
    };
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
    let base = virtual_shards / count;
    let remainder = (virtual_shards % count) as usize;
    let mut choices: Vec<Option<Candidate>> = vec![None; remainder + 1];
    choices[0] = Some(Candidate {
        retained: 0,
        extras: Vec::new(),
    });

    for (index, owner) in owners.iter().enumerate() {
        let mut next = vec![None; remainder + 1];
        for (used, candidate) in choices.iter().enumerate() {
            let Some(candidate) = candidate else {
                continue;
            };
            for extra in [false, true] {
                let next_used = used + usize::from(extra);
                if next_used > remainder {
                    continue;
                }
                let start = index as u32 * base + used as u32;
                let end = start + base + u32::from(extra);
                let retained = candidate.retained
                    + retained_shards(previous, virtual_shards, &owner.id, start, end);
                let mut extras = candidate.extras.clone();
                extras.push(extra);
                let proposed = Candidate { retained, extras };
                if is_better(&proposed, next[next_used].as_ref()) {
                    next[next_used] = Some(proposed);
                }
            }
        }
        choices = next;
    }

    let extras = choices[remainder]
        .take()
        .expect("balanced partition always has a solution")
        .extras;
    let mut start = 0;
    let assignments = owners
        .into_iter()
        .zip(extras)
        .map(|(owner, extra)| {
            let end = start + base + u32::from(extra);
            let range = ShardRange::within(start, end, virtual_shards)
                .expect("planner creates valid ranges");
            start = end;
            Assignment::new(owner, range, AssignmentState::Active)
        })
        .collect();
    ShardMap::with_routing(generation, virtual_shards, routing, assignments)
        .map_err(PlanError::from)
}

fn retained_shards(
    previous: Option<&ShardMap>,
    virtual_shards: u32,
    owner_id: &str,
    start: u32,
    end: u32,
) -> u32 {
    let Some(previous) = previous.filter(|map| map.virtual_shards == virtual_shards) else {
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
    use crate::ShardId;

    fn owners(count: u32) -> Vec<Owner> {
        (0..count)
            .map(|ordinal| Owner::new(format!("meter-{ordinal}"), ordinal))
            .collect()
    }

    fn movement(previous: &ShardMap, next: &ShardMap) -> u32 {
        (0..previous.virtual_shards)
            .filter(|shard| {
                previous.owner_of(ShardId::new(*shard)) != next.owner_of(ShardId::new(*shard))
            })
            .count() as u32
    }

    #[test]
    fn scales_one_to_many_to_one_with_exact_balanced_coverage() {
        let one = balanced_contiguous(AssignmentGeneration::new(1), 64, &owners(1), None).unwrap();
        let four =
            balanced_contiguous(AssignmentGeneration::new(2), 64, &owners(4), Some(&one)).unwrap();
        assert_eq!(
            four.assignments
                .iter()
                .map(|assignment| assignment.range.len())
                .collect::<Vec<_>>(),
            vec![16, 16, 16, 16]
        );
        assert_eq!(movement(&one, &four), 48);

        let one_again =
            balanced_contiguous(AssignmentGeneration::new(3), 64, &owners(1), Some(&four)).unwrap();
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
                    Owner::new("meter-0", 0),
                    ShardRange::within(0, 2, 8).unwrap(),
                    AssignmentState::Active,
                ),
                Assignment::new(
                    Owner::new("meter-1", 1),
                    ShardRange::within(2, 5, 8).unwrap(),
                    AssignmentState::Active,
                ),
                Assignment::new(
                    Owner::new("meter-2", 2),
                    ShardRange::within(5, 8, 8).unwrap(),
                    AssignmentState::Active,
                ),
            ],
        )
        .unwrap();
        let next =
            balanced_contiguous(AssignmentGeneration::new(2), 8, &owners(3), Some(&previous))
                .unwrap();
        assert_eq!(
            next.assignments
                .iter()
                .map(|assignment| assignment.range.len())
                .collect::<Vec<_>>(),
            vec![2, 3, 3]
        );
        assert_eq!(movement(&previous, &next), 0);
        assert_eq!(next.assignments.len(), 3);
    }

    #[test]
    fn output_is_deterministic_and_ordinal_ordered() {
        let unordered = vec![
            Owner::new("meter-2", 2),
            Owner::new("meter-0", 0),
            Owner::new("meter-1", 1),
        ];
        let first =
            balanced_contiguous(AssignmentGeneration::new(1), 64, &unordered, None).unwrap();
        let second =
            balanced_contiguous(AssignmentGeneration::new(1), 64, &unordered, None).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.assignments[0].owner.id, "meter-0");
    }

    #[test]
    fn membership_only_rebalance_preserves_routing_and_shard_count_changes_are_rejected() {
        let initial =
            balanced_contiguous(AssignmentGeneration::new(1), 4, &owners(1), None).unwrap();
        let rebalanced =
            balanced_contiguous(AssignmentGeneration::new(2), 4, &owners(2), Some(&initial))
                .unwrap();
        assert_eq!(rebalanced.routing, initial.routing);

        assert!(matches!(
            balanced_contiguous(
                AssignmentGeneration::new(3),
                5,
                &owners(2),
                Some(&rebalanced)
            ),
            Err(PlanError::ShardCountChangeRequiresMigration {
                current: 4,
                desired: 5
            })
        ));
        assert!(matches!(
            balanced_contiguous(
                AssignmentGeneration::new(3),
                3,
                &owners(2),
                Some(&rebalanced)
            ),
            Err(PlanError::ShardCountChangeRequiresMigration {
                current: 4,
                desired: 3
            })
        ));
    }
}
