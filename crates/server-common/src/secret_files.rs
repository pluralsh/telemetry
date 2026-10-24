//! Secrets read from files, kept in memory and re-read when their files
//! change, so authorizing a request never touches the filesystem.
//!
//! Each file's directory is watched rather than the file: Kubernetes rotates
//! a mounted Secret by swapping a symlink in that directory, which a watch on
//! the old file would not see.

use std::{
    collections::{HashMap, HashSet},
    io,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex, PoisonError, RwLock},
};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::auth::SecretError;

static SECRET_FILES: LazyLock<SecretFiles> = LazyLock::new(SecretFiles::default);

/// The current value of the secret in the file at `path`.
pub(crate) fn read(path: &Path) -> Result<String, SecretError> {
    SECRET_FILES.get(path)
}

#[derive(Default)]
struct SecretFiles {
    /// Values of files whose directory is watched. Others are read on every
    /// use, since nothing would tell the cache they changed.
    values: RwLock<HashMap<PathBuf, String>>,
    watching: Mutex<Watching>,
}

#[derive(Default)]
struct Watching {
    /// Created with the first watch; delivers events on its own thread.
    watcher: Option<RecommendedWatcher>,
    directories: HashSet<PathBuf>,
}

impl SecretFiles {
    fn get(&self, path: &Path) -> Result<String, SecretError> {
        if let Some(value) = self.values().get(path) {
            return Ok(value.clone());
        }
        // Watched before reading, so a change in between is not missed.
        let watched = self.watch(path);
        let value = read_file(path).map_err(|error| {
            SecretError::new(format!("cannot read {}: {error}", path.display()))
        })??;
        if watched {
            self.values_mut().insert(path.to_owned(), value.clone());
        }
        Ok(value)
    }

    /// Watches the directory of `path`, returning whether it is watched.
    fn watch(&self, path: &Path) -> bool {
        let directory = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        let mut watching = self.watching.lock().unwrap_or_else(PoisonError::into_inner);
        if watching.directories.contains(directory) {
            return true;
        }
        let watcher = match &mut watching.watcher {
            Some(watcher) => watcher,
            empty => match notify::recommended_watcher(|event| SECRET_FILES.refresh(event)) {
                Ok(watcher) => empty.insert(watcher),
                Err(error) => {
                    tracing::warn!(%error, "cannot watch secret files; reading them on every use");
                    return false;
                }
            },
        };
        if let Err(error) = watcher.watch(directory, RecursiveMode::NonRecursive) {
            tracing::warn!(
                directory = %directory.display(),
                %error,
                "cannot watch secret directory; reading its secrets on every use"
            );
            return false;
        }
        watching.directories.insert(directory.to_owned());
        true
    }

    /// Re-reads every cached file after a change in a watched directory.
    /// Events do not say which secret changed when a symlink swap replaced
    /// it, and there are only a few secret files.
    fn refresh(&self, event: notify::Result<Event>) {
        match event {
            // Reading a secret can itself be reported as an access.
            Ok(event) if matches!(event.kind, EventKind::Access(_)) => return,
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "secret file watch error; re-reading secrets"),
        }
        let paths = self.values().keys().cloned().collect::<Vec<_>>();
        for path in paths {
            match read_file(&path) {
                Ok(Ok(value)) => {
                    self.values_mut().insert(path, value);
                }
                // A removed or emptied secret no longer authorizes anything.
                Ok(Err(_)) => {
                    tracing::warn!(path = %path.display(), "secret file is now empty");
                    self.values_mut().remove(&path);
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    tracing::warn!(path = %path.display(), "secret file was removed");
                    self.values_mut().remove(&path);
                }
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    %error,
                    "cannot re-read secret file; keeping its last value"
                ),
            }
        }
    }

    fn values(&self) -> std::sync::RwLockReadGuard<'_, HashMap<PathBuf, String>> {
        self.values.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn values_mut(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<PathBuf, String>> {
        self.values.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Reads a secret file: an I/O error, or the secret's validation.
fn read_file(path: &Path) -> io::Result<Result<String, SecretError>> {
    std::fs::read_to_string(path).map(crate::auth::normalize_secret)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    /// Polls until `read(path)` satisfies `done`; watch events are delivered
    /// asynchronously.
    fn eventually(path: &Path, done: impl Fn(&Result<String, SecretError>) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let value = read(path);
            if done(&value) {
                return;
            }
            assert!(Instant::now() < deadline, "secret never updated: {value:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn caches_and_follows_in_place_changes_and_removal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("password");
        std::fs::write(&path, "first\n").unwrap();
        assert_eq!(read(&path).unwrap(), "first");
        assert!(SECRET_FILES.values().contains_key(&path));

        std::fs::write(&path, "second\n").unwrap();
        eventually(&path, |value| value.as_deref() == Ok("second"));

        std::fs::remove_file(&path).unwrap();
        eventually(&path, Result::is_err);
    }

    #[cfg(unix)]
    #[test]
    fn follows_a_kubernetes_style_symlink_swap() {
        let dir = tempfile::tempdir().unwrap();
        let write_revision = |name: &str, value: &str| {
            let revision = dir.path().join(name);
            std::fs::create_dir(&revision).unwrap();
            std::fs::write(revision.join("password"), value).unwrap();
            let staged = dir.path().join("..data_tmp");
            std::os::unix::fs::symlink(&revision, &staged).unwrap();
            std::fs::rename(&staged, dir.path().join("..data")).unwrap();
        };
        write_revision("..rev1", "first");
        let path = dir.path().join("password");
        std::os::unix::fs::symlink("..data/password", &path).unwrap();
        assert_eq!(read(&path).unwrap(), "first");

        write_revision("..rev2", "second");
        eventually(&path, |value| value.as_deref() == Ok("second"));
    }
}
