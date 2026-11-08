//! SlateDB settings shared by every database.
//!
//! [`base_settings`] is SlateDB's defaults with the overrides every store
//! needs; [`load_settings_over`] layers a settings file or `SLATEDB_`
//! environment variables on top of a base, so a file only has to name the
//! keys it changes.

use std::time::Duration;

use slatedb::config::Settings;

use super::config::SlateDbStorageConfig;
use super::{StorageError, StorageResult};

/// How long garbage collection keeps an SST no manifest references. SlateDB
/// defaults to a day, which with an L0 flush every few seconds and
/// constant compaction keeps a day of dead SSTs in `compacted/`. Live SSTs
/// are protected regardless: GC never deletes anything a manifest or
/// checkpoint references, or anything newer than the oldest running
/// compaction or the newest L0.
pub const DEFAULT_COMPACTED_GC_MIN_AGE: Duration = Duration::from_secs(60 * 60);

/// L0 SSTs one database uploads at once. Each streams through an upload
/// buffer that holds up to about 90 MiB of parts, so SlateDB's 4 held about
/// 350 MiB more at the end of an ingest burst than one, for the same ingest
/// rate.
pub const DEFAULT_L0_FLUSH_PARALLELISM: usize = 1;

/// How often the compactor looks for work and its worker for compactions to
/// claim, 1 s rather than SlateDB's 5 s each. A segment runs one L0
/// compaction at a time, so both waits fall between every two of them: at
/// 5 s, a writer flushing an L0 SST every few seconds gains several L0 SSTs
/// per compaction while its cores sit idle. Each poll reads the manifest,
/// a request a second.
pub const DEFAULT_COMPACTOR_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// SlateDB's defaults with the overrides every database shares:
///
/// - `compacted_options.min_age` of [`DEFAULT_COMPACTED_GC_MIN_AGE`].
/// - `enable_trivial_move`, so a compaction whose inputs don't overlap
///   relinks them instead of rewriting them.
/// - `l0_flush_parallelism` of [`DEFAULT_L0_FLUSH_PARALLELISM`].
/// - the compactor's and its worker's poll intervals of
///   [`DEFAULT_COMPACTOR_POLL_INTERVAL`].
pub fn base_settings() -> Settings {
    let mut settings = Settings {
        l0_flush_parallelism: DEFAULT_L0_FLUSH_PARALLELISM,
        ..Settings::default()
    };
    if let Some(compacted) = settings
        .garbage_collector_options
        .as_mut()
        .and_then(|gc| gc.compacted_options.as_mut())
    {
        compacted.min_age = DEFAULT_COMPACTED_GC_MIN_AGE;
    }
    if let Some(compactor) = settings.compactor_options.as_mut() {
        compactor.enable_trivial_move = true;
        compactor.poll_interval = DEFAULT_COMPACTOR_POLL_INTERVAL;
        if let Some(worker) = compactor.worker.as_mut() {
            worker.compactions_poll_interval = DEFAULT_COMPACTOR_POLL_INTERVAL;
        }
    }
    settings
}

/// L0 SSTs a writer uploads at once. With one, a memtable that fills while
/// the previous one still uploads holds every write: at about 15 MB/s of L0
/// a 64 MiB upload competing with compaction for the object store did so
/// every few seconds, and the write budget then turned ingest away. Each
/// upload buffers up to about 90 MiB of parts.
pub const DEFAULT_WRITER_L0_FLUSH_PARALLELISM: usize = 2;

/// A writer's SlateDB `max_unflushed_bytes`, 192 MiB rather than SlateDB's
/// 1 GiB: the estimated encoded size of the memtables and WAL buffer past
/// which SlateDB holds new writes until L0 flushes catch up. That is one
/// memtable filling beside [`DEFAULT_WRITER_L0_FLUSH_PARALLELISM`] flushing,
/// of SlateDB's 64 MiB `l0_sst_size_bytes`. Memtables hold about 2.5 times
/// their estimate, and readers replay the same unflushed WAL, so this
/// bounds both.
pub const DEFAULT_MAX_UNFLUSHED_BYTES: usize = 192 * 1024 * 1024;

/// A writer's SlateDB `l0_max_ssts` and `l0_max_ssts_per_key`, 24 rather
/// than SlateDB's 8: the L0 SSTs a segment may hold before flushes stall.
/// Ingest admission stops taking writes [`L0Limit::backlog`] short of it,
/// 5 SSTs here, and starts shedding at five eighths, so it tapers over 4.
/// See [`l0_max_ssts_for`].
///
/// [`L0Limit::backlog`]: super::admission::L0Limit::backlog
pub const DEFAULT_WRITER_L0_MAX_SSTS: usize = 24;

/// The least `l0_max_ssts` over which admission tapers ingest across 4 L0
/// SSTs before it stops taking writes, for a writer whose unflushed writes
/// and uploads add `backlog` SSTs: shedding starts at five eighths of the
/// limit and stops `backlog` short of it, so the limit is at least
/// `8 (backlog + 3) / 3`, rounded up to a multiple of 8.
pub const fn l0_max_ssts_for(backlog: usize) -> usize {
    (8 * (backlog + 3)).div_ceil(3).next_multiple_of(8)
}

/// [`base_settings`] with a writer's unflushed bytes, L0 uploads and L0
/// limit: [`DEFAULT_MAX_UNFLUSHED_BYTES`],
/// [`DEFAULT_WRITER_L0_FLUSH_PARALLELISM`] and
/// [`DEFAULT_WRITER_L0_MAX_SSTS`].
pub fn writer_settings() -> Settings {
    Settings {
        max_unflushed_bytes: DEFAULT_MAX_UNFLUSHED_BYTES,
        l0_flush_parallelism: DEFAULT_WRITER_L0_FLUSH_PARALLELISM,
        l0_max_ssts: DEFAULT_WRITER_L0_MAX_SSTS,
        l0_max_ssts_per_key: DEFAULT_WRITER_L0_MAX_SSTS,
        ..base_settings()
    }
}

/// Loads SlateDB settings the way `Settings::from_file` / `Settings::load`
/// do, but over `base` instead of SlateDB's defaults, so `base` applies
/// wherever the file or environment leaves a key unset.
pub fn load_settings_over(
    base: Settings,
    slate_config: &SlateDbStorageConfig,
) -> StorageResult<Settings> {
    use figment::Figment;
    use figment::providers::{Env, Format, Json, Toml, Yaml};

    let base = Figment::from(base);
    let (figment, source) = match &slate_config.settings_path {
        Some(path) => {
            let extension = std::path::Path::new(path)
                .extension()
                .and_then(|ext| ext.to_str());
            let figment = match extension {
                Some("json") => base.merge(Json::file(path)),
                Some("toml") => base.merge(Toml::file(path)),
                Some("yaml" | "yml") => base.merge(Yaml::file(path)),
                _ => {
                    return Err(StorageError::Storage(format!(
                        "Failed to load SlateDB settings from {path}: \
                         unknown format (expected .json, .toml, .yaml or .yml)"
                    )));
                }
            };
            (figment, path.as_str())
        }
        None => (
            base.merge(Json::file("SlateDb.json"))
                .merge(Toml::file("SlateDb.toml"))
                .merge(Yaml::file("SlateDb.yaml"))
                .merge(Yaml::file("SlateDb.yml"))
                .admerge(Env::prefixed("SLATEDB_")),
            "SlateDb.* / SLATEDB_*",
        ),
    };
    figment.extract().map_err(|e| {
        StorageError::Storage(format!(
            "Failed to load SlateDB settings from {source}: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(path: Option<String>) -> SlateDbStorageConfig {
        SlateDbStorageConfig {
            settings_path: path,
            ..SlateDbStorageConfig::default()
        }
    }

    fn compacted_min_age(settings: &Settings) -> Option<Duration> {
        Some(
            settings
                .garbage_collector_options
                .as_ref()?
                .compacted_options
                .as_ref()?
                .min_age,
        )
    }

    #[test]
    fn base_settings_shorten_gc_enable_trivial_moves_and_upload_one_l0() {
        let settings = load_settings_over(base_settings(), &config(None)).unwrap();
        assert_eq!(
            compacted_min_age(&settings),
            Some(DEFAULT_COMPACTED_GC_MIN_AGE)
        );
        assert_eq!(settings.l0_flush_parallelism, DEFAULT_L0_FLUSH_PARALLELISM);
        let compactor = settings.compactor_options.unwrap();
        assert!(compactor.enable_trivial_move);
        assert_eq!(compactor.poll_interval, DEFAULT_COMPACTOR_POLL_INTERVAL);
        assert_eq!(
            compactor.worker.unwrap().compactions_poll_interval,
            DEFAULT_COMPACTOR_POLL_INTERVAL
        );
    }

    #[test]
    fn writer_settings_cap_unflushed_bytes_above_the_l0_sst_size() {
        let settings = load_settings_over(writer_settings(), &config(None)).unwrap();
        assert_eq!(settings.max_unflushed_bytes, DEFAULT_MAX_UNFLUSHED_BYTES);
        assert!(settings.max_unflushed_bytes > settings.l0_sst_size_bytes);
        assert_eq!(
            settings.l0_flush_parallelism,
            DEFAULT_WRITER_L0_FLUSH_PARALLELISM
        );
        assert_eq!(settings.l0_max_ssts, DEFAULT_WRITER_L0_MAX_SSTS);
        assert_eq!(settings.l0_max_ssts_per_key, DEFAULT_WRITER_L0_MAX_SSTS);
    }

    #[test]
    fn writers_shed_ingest_over_several_l0_ssts_before_stalling() {
        let limit = super::super::admission::L0Limit::of(&writer_settings());
        assert_eq!(limit.backlog, 5);
        assert_eq!(limit.max_ssts, l0_max_ssts_for(limit.backlog));
        assert!(limit.max_ssts.div_ceil(8) * 5 + 3 <= limit.max_ssts - limit.backlog);
    }

    #[test]
    fn a_settings_file_overrides_only_the_keys_it_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slatedb.toml");
        std::fs::write(
            &path,
            "l0_max_ssts = 16\n[garbage_collector_options.compacted_options]\nmin_age = \"7200s\"\n",
        )
        .unwrap();
        let settings = load_settings_over(
            base_settings(),
            &config(Some(path.to_string_lossy().into_owned())),
        )
        .unwrap();
        assert_eq!(settings.l0_max_ssts, 16);
        assert_eq!(
            compacted_min_age(&settings),
            Some(Duration::from_secs(7200))
        );
        assert!(settings.compactor_options.unwrap().enable_trivial_move);
    }

    #[test]
    fn a_settings_file_can_serialize_compactions_and_disable_trivial_moves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slatedb.yaml");
        std::fs::write(
            &path,
            "compactor_options:\n  max_concurrent_compactions: 1\n  enable_trivial_move: false\n",
        )
        .unwrap();
        let settings = load_settings_over(
            base_settings(),
            &config(Some(path.to_string_lossy().into_owned())),
        )
        .unwrap();
        assert_eq!(
            compacted_min_age(&settings),
            Some(DEFAULT_COMPACTED_GC_MIN_AGE)
        );
        let compactor = settings.compactor_options.unwrap();
        let defaults = base_settings().compactor_options.unwrap();
        assert_eq!(compactor.max_concurrent_compactions, 1);
        assert!(!compactor.enable_trivial_move);
        assert_eq!(compactor.poll_interval, defaults.poll_interval);
    }
}
