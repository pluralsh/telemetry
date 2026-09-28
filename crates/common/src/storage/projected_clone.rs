//! Offline, idempotent SlateDB projected-clone support for shard migration.

use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use bytes::Bytes;
use slatedb::{
    admin::{Admin, CloneSourceSpec},
    config::CheckpointOptions,
    object_store::ObjectStore,
};

/// Everything needed to materialize one shard split without opening either DB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCloneSpec {
    pub source_path: String,
    pub target_path: String,
    pub checkpoint_name: String,
    pub slot_start: u16,
    pub slot_end: u16,
    pub segment_extractor_name: String,
}

impl ProjectedCloneSpec {
    pub fn validate(&self) -> Result<(), ProjectedCloneError> {
        if self.source_path == self.target_path {
            return Err(ProjectedCloneError::Invalid(
                "source and target paths must differ".to_owned(),
            ));
        }
        if self.slot_start >= self.slot_end || self.slot_end > 4096 {
            return Err(ProjectedCloneError::Invalid(format!(
                "invalid projected slot range {}..{}",
                self.slot_start, self.slot_end
            )));
        }
        if self.checkpoint_name.is_empty() || self.segment_extractor_name.is_empty() {
            return Err(ProjectedCloneError::Invalid(
                "checkpoint and segment extractor names must be non-empty".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectedCloneError {
    #[error("invalid projected clone: {0}")]
    Invalid(String),
    #[error("source database is not initialized: {0}")]
    MissingSource(String),
    #[error("source database uses segment extractor {actual:?}, expected {expected:?}")]
    SegmentExtractorMismatch {
        actual: Option<String>,
        expected: String,
    },
    #[error("target database is already initialized with conflicting contents: {0}")]
    ConflictingTarget(String),
    #[error("SlateDB projected clone failed: {0}")]
    Slate(#[source] slatedb::Error),
}

impl ProjectedCloneError {
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Invalid(_)
                | Self::MissingSource(_)
                | Self::SegmentExtractorMismatch { .. }
                | Self::ConflictingTarget(_)
        )
    }
}

/// Performs the non-mutating checks used by the `Preparing` phase.
pub async fn preflight_projected_clone(
    spec: &ProjectedCloneSpec,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), ProjectedCloneError> {
    spec.validate()?;
    let source = Admin::builder(spec.source_path.clone(), Arc::clone(&object_store)).build();
    let source_manifest = source
        .read_manifest(None)
        .await
        .map_err(ProjectedCloneError::Slate)?
        .filter(|manifest| manifest.initialized())
        .ok_or_else(|| ProjectedCloneError::MissingSource(spec.source_path.clone()))?;
    verify_extractor(spec, source_manifest.segment_extractor_name())?;

    let target = Admin::builder(spec.target_path.clone(), object_store).build();
    if let Some(manifest) = target
        .read_manifest(None)
        .await
        .map_err(ProjectedCloneError::Slate)?
        .filter(|manifest| manifest.initialized())
    {
        verify_existing_target(spec, &manifest, None)?;
    }
    Ok(())
}

/// Creates/reuses a named detached source checkpoint, creates the projected
/// clone if needed, and verifies its manifest. Both success paths are safe to
/// retry after process or publication failure.
pub async fn execute_projected_clone(
    spec: &ProjectedCloneSpec,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), ProjectedCloneError> {
    spec.validate()?;
    let source = Admin::builder(spec.source_path.clone(), Arc::clone(&object_store)).build();
    let source_manifest = source
        .read_manifest(None)
        .await
        .map_err(ProjectedCloneError::Slate)?
        .filter(|manifest| manifest.initialized())
        .ok_or_else(|| ProjectedCloneError::MissingSource(spec.source_path.clone()))?;
    verify_extractor(spec, source_manifest.segment_extractor_name())?;

    let checkpoint = match source
        .list_checkpoints(Some(&spec.checkpoint_name))
        .await
        .map_err(ProjectedCloneError::Slate)?
        .into_iter()
        .max_by_key(|checkpoint| checkpoint.create_time)
    {
        Some(checkpoint) => checkpoint,
        None => {
            let created = source
                .create_detached_checkpoint(&CheckpointOptions {
                    name: Some(spec.checkpoint_name.clone()),
                    ..CheckpointOptions::default()
                })
                .await
                .map_err(ProjectedCloneError::Slate)?;
            source
                .list_checkpoints(Some(&spec.checkpoint_name))
                .await
                .map_err(ProjectedCloneError::Slate)?
                .into_iter()
                .find(|checkpoint| checkpoint.id == created.id)
                .ok_or_else(|| {
                    ProjectedCloneError::Invalid(
                        "created checkpoint was not visible in the source manifest".to_owned(),
                    )
                })?
        }
    };

    let target = Admin::builder(spec.target_path.clone(), Arc::clone(&object_store)).build();
    if let Some(manifest) = target
        .read_manifest(None)
        .await
        .map_err(ProjectedCloneError::Slate)?
        .filter(|manifest| manifest.initialized())
    {
        return verify_existing_target(spec, &manifest, Some(checkpoint.id));
    }

    let start_slot = spec.slot_start.to_be_bytes();
    let end_slot = spec.slot_end.to_be_bytes();
    target
        .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
            spec.source_path.clone(),
            checkpoint.id,
        ))
        // Product databases put all records in named `(namespace, time)`
        // segments. Excluding the empty logical segment prevents interpreting
        // its first two key bytes as a routing slot.
        .with_segment_filter(|prefix| !prefix.is_empty())
        .with_segment_projection(move |prefix| {
            let mut start = Vec::with_capacity(prefix.len() + 2);
            start.extend_from_slice(prefix);
            start.extend_from_slice(&start_slot);
            let mut end = Vec::with_capacity(prefix.len() + 2);
            end.extend_from_slice(prefix);
            end.extend_from_slice(&end_slot);
            (
                Bound::Included(Bytes::from(start)),
                Bound::Excluded(Bytes::from(end)),
            )
        })
        .build()
        .await
        .map_err(ProjectedCloneError::Slate)?;

    let manifest = target
        .read_manifest(None)
        .await
        .map_err(ProjectedCloneError::Slate)?
        .filter(|manifest| manifest.initialized())
        .ok_or_else(|| {
            ProjectedCloneError::Invalid("clone completed without an initialized manifest".into())
        })?;
    verify_existing_target(spec, &manifest, Some(checkpoint.id))
}

fn verify_extractor(
    spec: &ProjectedCloneSpec,
    actual: Option<&str>,
) -> Result<(), ProjectedCloneError> {
    if actual == Some(spec.segment_extractor_name.as_str()) {
        Ok(())
    } else {
        Err(ProjectedCloneError::SegmentExtractorMismatch {
            actual: actual.map(str::to_owned),
            expected: spec.segment_extractor_name.clone(),
        })
    }
}

fn verify_existing_target(
    spec: &ProjectedCloneSpec,
    manifest: &slatedb::manifest::VersionedManifest,
    checkpoint_id: Option<uuid::Uuid>,
) -> Result<(), ProjectedCloneError> {
    verify_extractor(spec, manifest.segment_extractor_name())?;
    let matching_source = manifest.external_dbs().iter().any(|external| {
        external.path == spec.source_path
            && checkpoint_id.is_none_or(|checkpoint| external.source_checkpoint_id == checkpoint)
            && external.final_checkpoint_id.is_some()
    });
    if !matching_source {
        return Err(ProjectedCloneError::ConflictingTarget(
            spec.target_path.clone(),
        ));
    }

    // CloneBuilder validates every returned segment range against its segment
    // prefix. Requiring all retained views that cross a projection boundary to
    // carry a visible range guards against accepting an unprojected clone.
    let projected = manifest.segments().iter().all(|segment| {
        segment
            .l0()
            .iter()
            .chain(
                segment
                    .compacted()
                    .iter()
                    .flat_map(|run| run.sst_views.iter()),
            )
            .all(|view| {
                let mut start = segment.prefix().to_vec();
                start.extend_from_slice(&spec.slot_start.to_be_bytes());
                let mut end = segment.prefix().to_vec();
                end.extend_from_slice(&spec.slot_end.to_be_bytes());
                match (&view.sst.info.first_entry, &view.sst.info.last_entry) {
                    (Some(first), Some(last))
                        if first.as_ref() >= start.as_slice() && last.as_ref() < end.as_slice() =>
                    {
                        true
                    }
                    _ => view.visible_range().is_some_and(|range| {
                        let start_ok = match range.start_bound() {
                            Bound::Included(key) | Bound::Excluded(key) => {
                                key.as_ref() >= start.as_slice()
                            }
                            Bound::Unbounded => false,
                        };
                        let end_ok = match range.end_bound() {
                            Bound::Excluded(key) => key.as_ref() <= end.as_slice(),
                            Bound::Included(key) => key.as_ref() < end.as_slice(),
                            Bound::Unbounded => false,
                        };
                        start_ok && end_ok
                    }),
                }
            })
    });
    if projected {
        Ok(())
    } else {
        Err(ProjectedCloneError::ConflictingTarget(format!(
            "{} has data outside the requested slot projection",
            spec.target_path
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::{
        DbBuilder, DbReader, PrefixExtractor, PrefixTarget,
        config::{FlushOptions, FlushType},
        object_store::memory::InMemory,
    };

    #[derive(Debug)]
    struct TestExtractor;

    impl PrefixExtractor for TestExtractor {
        fn name(&self) -> &str {
            "projected-clone-test/v1"
        }

        fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
            let bytes = match target {
                PrefixTarget::Point(bytes) | PrefixTarget::Prefix(bytes) => bytes,
            };
            (!bytes.is_empty()).then_some(1)
        }
    }

    fn key(segment: u8, slot: u16, suffix: u8) -> Bytes {
        let mut key = Vec::with_capacity(4);
        key.push(segment);
        key.extend_from_slice(&slot.to_be_bytes());
        key.push(suffix);
        Bytes::from(key)
    }

    fn spec(source: &str, target: &str) -> ProjectedCloneSpec {
        ProjectedCloneSpec {
            source_path: source.to_owned(),
            target_path: target.to_owned(),
            checkpoint_name: "migration-source-0-target-1-slots-2-4".to_owned(),
            slot_start: 2,
            slot_end: 4,
            segment_extractor_name: "projected-clone-test/v1".to_owned(),
        }
    }

    async fn build_source(path: &str, object_store: Arc<dyn ObjectStore>) {
        let db = DbBuilder::new(path, object_store)
            .with_segment_extractor(Arc::new(TestExtractor))
            .build()
            .await
            .unwrap();
        for segment in [b'a', b'b'] {
            for slot in 0..6 {
                db.put(key(segment, slot, 0), Bytes::from_static(b"value"))
                    .await
                    .unwrap();
            }
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn projected_clone_keeps_only_moved_slots_and_is_idempotent() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        build_source("source", Arc::clone(&object_store)).await;
        let spec = spec("source", "target");

        preflight_projected_clone(&spec, Arc::clone(&object_store))
            .await
            .unwrap();
        execute_projected_clone(&spec, Arc::clone(&object_store))
            .await
            .unwrap();
        execute_projected_clone(&spec, Arc::clone(&object_store))
            .await
            .unwrap();

        let reader = DbReader::builder("target", object_store)
            .with_segment_extractor(Arc::new(TestExtractor))
            .build()
            .await
            .unwrap();
        for segment in [b'a', b'b'] {
            for slot in 0..6 {
                assert_eq!(
                    reader.get(key(segment, slot, 0)).await.unwrap().is_some(),
                    (2..4).contains(&slot),
                    "unexpected visibility for segment {segment} slot {slot}"
                );
            }
        }
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn initialized_non_clone_target_is_a_fatal_conflict() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        build_source("source-conflict", Arc::clone(&object_store)).await;
        let target = DbBuilder::new("target-conflict", Arc::clone(&object_store))
            .with_segment_extractor(Arc::new(TestExtractor))
            .build()
            .await
            .unwrap();
        target.close().await.unwrap();

        let error =
            preflight_projected_clone(&spec("source-conflict", "target-conflict"), object_store)
                .await
                .unwrap_err();
        assert!(matches!(error, ProjectedCloneError::ConflictingTarget(_)));
        assert!(error.is_fatal());
    }
}
