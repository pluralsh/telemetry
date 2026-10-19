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

/// SlateDB's defaults with the overrides every database shares:
///
/// - `compacted_options.min_age` of [`DEFAULT_COMPACTED_GC_MIN_AGE`].
/// - `enable_trivial_move`, so a compaction whose inputs don't overlap
///   relinks them instead of rewriting them.
pub fn base_settings() -> Settings {
    let mut settings = Settings::default();
    if let Some(compacted) = settings
        .garbage_collector_options
        .as_mut()
        .and_then(|gc| gc.compacted_options.as_mut())
    {
        compacted.min_age = DEFAULT_COMPACTED_GC_MIN_AGE;
    }
    if let Some(compactor) = settings.compactor_options.as_mut() {
        compactor.enable_trivial_move = true;
    }
    settings
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
    fn base_settings_shorten_gc_and_enable_trivial_moves() {
        let settings = load_settings_over(base_settings(), &config(None)).unwrap();
        assert_eq!(
            compacted_min_age(&settings),
            Some(DEFAULT_COMPACTED_GC_MIN_AGE)
        );
        assert!(settings.compactor_options.unwrap().enable_trivial_move);
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
