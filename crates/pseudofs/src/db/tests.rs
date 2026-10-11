// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::*;
use crate::config::BlobConfig;
use crate::search::{GlobOptions, GrepOptions};
use common::storage::config::{LocalObjectStoreConfig, SlateDbStorageConfig};
use tempfile::TempDir;

const TENANT: &str = "tenant-a";

struct Harness {
    fs: PseudoFs,
    config: Config,
    _dir: TempDir,
    remote: std::path::PathBuf,
}

fn config(dir: &TempDir, storage: StorageConfig) -> Config {
    Config {
        storage,
        metastore_wal_path: None,
        blobs: BlobConfig {
            object_store: Some(ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().join("remote").display().to_string(),
            })),
            path: Some("blobs".to_owned()),
            local_path: dir.path().join("local").display().to_string(),
            cache_capacity_bytes: 1024 * 1024,
            upload_concurrency: 2,
            gc_grace_seconds: 0,
        },
        chunk_size_bytes: 3,
        inline_threshold_bytes: 8,
        max_file_size_bytes: 1024 * 1024,
        max_append_generations: 4,
    }
}

async fn harness_with(edit: impl FnOnce(&mut Config)) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(&dir, StorageConfig::InMemory);
    edit(&mut config);
    let fs = PseudoFs::open(config.clone()).await.unwrap();
    let remote = dir.path().join("remote").join("blobs");
    Harness {
        fs,
        config,
        _dir: dir,
        remote,
    }
}

async fn harness() -> Harness {
    harness_with(|_| {}).await
}

fn remote_blobs(harness: &Harness) -> usize {
    std::fs::read_dir(&harness.remote).map_or(0, |entries| entries.count())
}

async fn pending_blobs(fs: &PseudoFs) -> usize {
    scan_blob_keys(fs.inner.storage.as_ref(), pending_blob_prefix())
        .await
        .unwrap()
        .len()
}

async fn collect<T>(mut receiver: mpsc::Receiver<Result<T>>) -> Vec<T> {
    let mut items = Vec::new();
    while let Some(item) = receiver.recv().await {
        items.push(item.unwrap());
    }
    items
}

fn results(max_results: usize) -> GlobOptions {
    GlobOptions {
        max_results,
        ..GlobOptions::default()
    }
}

async fn range(fs: &PseudoFs, path: &str, offset: u64, length: Option<u64>) -> Vec<u8> {
    collect(fs.stream_range(TENANT, path, offset, length).await.unwrap())
        .await
        .concat()
}

#[test]
fn normalizes_paths_lexically() {
    assert_eq!(normalize_path("/a//b/../c/").unwrap(), "/a/c");
    assert!(normalize_path("../../a").is_err());
    assert!(normalize_path("/../a").is_err());
    assert_eq!(normalize_path(".").unwrap(), "/");
}

#[tokio::test]
async fn implements_file_and_directory_operations() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.write_text(
        TENANT,
        "/data/note.txt",
        "hello".to_owned(),
        Durability::Applied,
    )
    .await
    .unwrap();
    assert!(fs.exists(TENANT, "/data").await.unwrap());
    assert!(fs.is_dir(TENANT, "/data").await.unwrap());
    assert!(fs.is_file(TENANT, "/data/note.txt").await.unwrap());
    assert!(!fs.is_symlink(TENANT, "/data/note.txt").await.unwrap());
    assert_eq!(
        fs.read_text(TENANT, "/data/note.txt").await.unwrap(),
        "hello"
    );

    fs.append_text(
        TENANT,
        "/data/note.txt",
        " world".to_owned(),
        Durability::Applied,
    )
    .await
    .unwrap();
    assert_eq!(
        fs.read_text(TENANT, "/data/note.txt").await.unwrap(),
        "hello world"
    );
    assert_eq!(fs.stat(TENANT, "/data/note.txt").await.unwrap().size, 11);
    assert_eq!(fs.iterdir(TENANT, "/data").await.unwrap().len(), 1);

    fs.rename(TENANT, "/data", "/archive", Durability::Applied)
        .await
        .unwrap();
    assert_eq!(
        fs.read_text(TENANT, "/archive/note.txt").await.unwrap(),
        "hello world"
    );
    assert!(!fs.exists(TENANT, "/data").await.unwrap());

    fs.unlink(TENANT, "/archive/note.txt", Durability::Applied)
        .await
        .unwrap();
    fs.rmdir(TENANT, "/archive", Durability::Applied)
        .await
        .unwrap();
    assert!(!fs.exists(TENANT, "/archive").await.unwrap());
}

#[tokio::test]
async fn streamed_upload_is_invisible_until_finished() {
    let Harness { fs, _dir, .. } = harness().await;
    for (path, content) in [
        ("/small.bin", &b"abcdef"[..]),
        ("/large.bin", b"abcdefghijklmnop"),
    ] {
        let mut upload = fs.start_upload(TENANT, path, false).await.unwrap();
        upload
            .push(Bytes::copy_from_slice(&content[..4]))
            .await
            .unwrap();
        upload
            .push(Bytes::copy_from_slice(&content[4..]))
            .await
            .unwrap();
        assert!(!fs.exists(TENANT, path).await.unwrap());
        upload.finish(Durability::Applied).await.unwrap();
        assert_eq!(fs.read_bytes(TENANT, path).await.unwrap(), content);
    }
}

#[tokio::test]
async fn reads_stream_in_configured_chunks() {
    let Harness { fs, _dir, .. } = harness().await;
    for (path, content) in [("/inline", "abcdefg"), ("/blob", "abcdefghijk")] {
        let mut upload = fs.start_upload(TENANT, path, false).await.unwrap();
        for byte in content.bytes() {
            upload.push(Bytes::copy_from_slice(&[byte])).await.unwrap();
        }
        upload.finish(Durability::Applied).await.unwrap();
        let lengths: Vec<usize> = fs
            .read_chunks(TENANT, path)
            .await
            .unwrap()
            .iter()
            .map(Bytes::len)
            .collect();
        let mut expected = vec![3; content.len() / 3];
        if content.len() % 3 != 0 {
            expected.push(content.len() % 3);
        }
        assert_eq!(lengths, expected);
    }
}

#[tokio::test]
async fn active_stream_keeps_its_snapshot_across_overwrite() {
    let Harness { fs, _dir, .. } = harness_with(|config| config.blobs.gc_grace_seconds = 300).await;
    let original =
        Bytes::from_static(b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
    fs.write_bytes(
        TENANT,
        "/snapshot.bin",
        original.clone(),
        Durability::Applied,
    )
    .await
    .unwrap();

    let mut chunks = fs.stream_chunks(TENANT, "/snapshot.bin").await.unwrap();
    let first = chunks.recv().await.unwrap().unwrap();
    fs.write_bytes(
        TENANT,
        "/snapshot.bin",
        Bytes::from_static(b"replacement"),
        Durability::Applied,
    )
    .await
    .unwrap();
    fs.run_blob_maintenance().await.unwrap();

    let mut streamed = first.to_vec();
    while let Some(chunk) = chunks.recv().await {
        streamed.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(streamed, original);
    assert_eq!(
        fs.read_bytes(TENANT, "/snapshot.bin").await.unwrap(),
        Bytes::from_static(b"replacement")
    );
}

#[tokio::test]
async fn append_chain_is_compacted_without_changing_content() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.write_text(TENANT, "/append.txt", "0".to_owned(), Durability::Applied)
        .await
        .unwrap();
    for value in ["1", "2", "3456789abc"] {
        fs.append_text(TENANT, "/append.txt", value.to_owned(), Durability::Applied)
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let inode = fs.resolve(TENANT, "/append.txt").await.unwrap();
            if inode.generation_count == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("append compaction did not finish");
    assert_eq!(
        fs.read_text(TENANT, "/append.txt").await.unwrap(),
        "0123456789abc"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_racing_compaction_are_never_lost() {
    let Harness { fs, _dir, .. } = harness_with(|config| {
        config.max_append_generations = 2;
        config.inline_threshold_bytes = 16;
    })
    .await;
    let appends = 300;
    for index in 0..appends {
        fs.append_text(TENANT, "/log", format!("<{index}>"), Durability::Applied)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let text = fs.read_text(TENANT, "/log").await.unwrap();
    let expected: String = (0..appends).map(|index| format!("<{index}>")).collect();
    assert_eq!(text, expected);
}

#[tokio::test]
async fn abort_discards_staged_upload() {
    let harness = harness().await;
    let fs = &harness.fs;
    let mut upload = fs.start_upload(TENANT, "/partial", false).await.unwrap();
    upload
        .push(Bytes::from_static(b"more than the inline threshold"))
        .await
        .unwrap();
    upload.abort().await.unwrap();
    assert!(!fs.exists(TENANT, "/partial").await.unwrap());
    let staging = std::path::Path::new(&harness.config.blobs.local_path).join("staging");
    assert_eq!(std::fs::read_dir(staging).unwrap().count(), 0);
}

#[tokio::test]
async fn blobs_upload_in_background_and_are_collected_when_unreferenced() {
    let harness = harness().await;
    let fs = &harness.fs;
    let content = Bytes::from(vec![7_u8; 100]);
    fs.write_bytes(TENANT, "/image.png", content.clone(), Durability::Written)
        .await
        .unwrap();
    fs.run_blob_maintenance().await.unwrap();
    assert_eq!(pending_blobs(fs).await, 0);
    assert_eq!(remote_blobs(&harness), 1);

    fs.write_bytes(
        TENANT,
        "/image.png",
        Bytes::from(vec![8_u8; 50]),
        Durability::Durable,
    )
    .await
    .unwrap();
    assert_eq!(remote_blobs(&harness), 2);
    fs.run_blob_maintenance().await.unwrap();
    assert_eq!(remote_blobs(&harness), 1);

    fs.unlink(TENANT, "/image.png", Durability::Applied)
        .await
        .unwrap();
    fs.run_blob_maintenance().await.unwrap();
    assert_eq!(remote_blobs(&harness), 0);
    let local = std::path::Path::new(&harness.config.blobs.local_path).join("blobs");
    assert_eq!(std::fs::read_dir(local).unwrap().count(), 0);
}

#[tokio::test]
async fn evicted_blobs_are_read_back_from_the_remote_store() {
    let harness = harness_with(|config| config.blobs.cache_capacity_bytes = 64).await;
    let fs = &harness.fs;
    let first: Vec<u8> = (0..100_u8).collect();
    let second: Vec<u8> = (100..200_u8).collect();
    fs.write_bytes(
        TENANT,
        "/first",
        Bytes::from(first.clone()),
        Durability::Durable,
    )
    .await
    .unwrap();
    fs.write_bytes(
        TENANT,
        "/second",
        Bytes::from(second.clone()),
        Durability::Durable,
    )
    .await
    .unwrap();
    let local = std::path::Path::new(&harness.config.blobs.local_path).join("blobs");
    assert_eq!(std::fs::read_dir(&local).unwrap().count(), 0);

    assert_eq!(range(fs, "/first", 10, Some(20)).await, &first[10..30]);
    assert_eq!(std::fs::read_dir(&local).unwrap().count(), 0);
    assert_eq!(fs.read_bytes(TENANT, "/second").await.unwrap(), second);
    assert_eq!(fs.read_bytes(TENANT, "/first").await.unwrap(), first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staging_waits_for_uploads_when_local_storage_is_full() {
    let Harness { fs, _dir, .. } =
        harness_with(|config| config.blobs.cache_capacity_bytes = 20).await;
    // Each blob alone exceeds the capacity, so every write after the first
    // must wait for the background uploader to unpin the previous one.
    let writes = (0..4).map(|index| {
        let fs = fs.clone();
        tokio::spawn(async move {
            fs.write_bytes(
                TENANT,
                &format!("/blob-{index}"),
                Bytes::from(vec![index as u8; 30]),
                Durability::Applied,
            )
            .await
        })
    });
    for write in futures::future::join_all(writes).await {
        write.unwrap().unwrap();
    }
    for index in 0..4_u8 {
        let content = fs
            .read_bytes(TENANT, &format!("/blob-{index}"))
            .await
            .unwrap();
        assert_eq!(content, vec![index; 30]);
    }
}

#[tokio::test]
async fn existence_checks_treat_file_parents_as_absent_and_reject_invalid_paths() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.write_text(TENANT, "/file", "x".to_owned(), Durability::Applied)
        .await
        .unwrap();
    assert!(!fs.exists(TENANT, "/file/child").await.unwrap());
    assert!(!fs.is_file(TENANT, "/file/child").await.unwrap());
    assert!(!fs.is_dir(TENANT, "/file/child").await.unwrap());
    for result in [
        fs.exists(TENANT, "/bad\0").await,
        fs.is_file(TENANT, "/bad\0").await,
        fs.is_dir(TENANT, "/bad\0").await,
    ] {
        assert!(matches!(result, Err(Error::InvalidPath(_))));
    }
    assert!(matches!(
        fs.is_file("bad tenant", "/file").await,
        Err(Error::InvalidPath(_))
    ));
}

#[tokio::test]
async fn renaming_a_missing_path_onto_itself_is_not_found() {
    let Harness { fs, _dir, .. } = harness().await;
    let error = fs
        .rename(TENANT, "/missing", "/missing", Durability::Applied)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotFound(_)));
    fs.write_text(TENANT, "/present", "x".to_owned(), Durability::Applied)
        .await
        .unwrap();
    assert_eq!(
        fs.rename(TENANT, "/present", "/present", Durability::Applied)
            .await
            .unwrap(),
        "/present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookups_racing_unlink_and_replacing_rename_never_report_corruption() {
    let Harness { fs, _dir, .. } = harness().await;
    let churn = {
        let fs = fs.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                fs.write_text(TENANT, "/d/f", "content".to_owned(), Durability::Applied)
                    .await
                    .unwrap();
                fs.unlink(TENANT, "/d/f", Durability::Applied)
                    .await
                    .unwrap();
                fs.write_text(TENANT, "/d/g", "next".to_owned(), Durability::Applied)
                    .await
                    .unwrap();
                fs.write_text(TENANT, "/d/f", "old".to_owned(), Durability::Applied)
                    .await
                    .unwrap();
                fs.rename(TENANT, "/d/g", "/d/f", Durability::Applied)
                    .await
                    .unwrap();
            }
        })
    };
    let readers = (0..4).map(|_| {
        let fs = fs.clone();
        tokio::spawn(async move {
            for _ in 0..400 {
                for result in [
                    fs.stat(TENANT, "/d/f").await.map(|_| ()),
                    fs.exists(TENANT, "/d/f").await.map(|_| ()),
                    fs.read_bytes(TENANT, "/d/f").await.map(|_| ()),
                ] {
                    assert!(
                        matches!(result, Ok(()) | Err(Error::NotFound(_))),
                        "{result:?}"
                    );
                }
            }
        })
    });
    for reader in futures::future::join_all(readers).await {
        reader.unwrap();
    }
    churn.await.unwrap();
}

#[tokio::test]
async fn bounded_reads_check_size_in_the_read_snapshot() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.write_bytes(
        TENANT,
        "/f",
        Bytes::from(vec![7_u8; 30]),
        Durability::Applied,
    )
    .await
    .unwrap();
    assert!(matches!(
        fs.read_bytes_within(TENANT, "/f", 29).await,
        Err(Error::ResourceExhausted(_))
    ));
    assert_eq!(
        fs.read_bytes_within(TENANT, "/f", 30).await.unwrap(),
        vec![7_u8; 30]
    );
}

#[tokio::test]
async fn resurrected_pending_key_for_an_uploaded_evicted_blob_is_cleared() {
    let harness = harness().await;
    let fs = &harness.fs;
    fs.write_bytes(
        TENANT,
        "/f",
        Bytes::from(vec![3_u8; 30]),
        Durability::Applied,
    )
    .await
    .unwrap();
    fs.run_blob_maintenance().await.unwrap();
    assert_eq!(pending_blobs(fs).await, 0);
    assert_eq!(remote_blobs(&harness), 1);
    // A crash can lose the pending key's deletion after the blob was unpinned
    // and evicted.
    let scope = fs.ensure_tenant(TENANT).await.unwrap();
    let local = std::path::Path::new(&harness.config.blobs.local_path).join("blobs");
    let entry = std::fs::read_dir(&local).unwrap().next().unwrap().unwrap();
    let id = Uuid::parse_str(entry.file_name().to_str().unwrap()).unwrap();
    std::fs::remove_file(entry.path()).unwrap();
    fs.inner
        .storage
        .apply(vec![put_op(
            pending_blob_key(scope, id),
            encode(&PendingBlob { size: 30 }).unwrap(),
        )])
        .await
        .unwrap();
    fs.run_blob_maintenance().await.unwrap();
    assert_eq!(pending_blobs(fs).await, 0);
    assert_eq!(fs.read_bytes(TENANT, "/f").await.unwrap(), vec![3_u8; 30]);
}

#[tokio::test]
async fn range_reads_span_inline_and_blob_generations() {
    let Harness { fs, _dir, .. } = harness().await;
    let blob = b"0123456789abcdefghij";
    fs.write_bytes(
        TENANT,
        "/mixed",
        Bytes::from_static(blob),
        Durability::Applied,
    )
    .await
    .unwrap();
    fs.append_bytes(
        TENANT,
        "/mixed",
        Bytes::from_static(b"KLM"),
        Durability::Applied,
    )
    .await
    .unwrap();
    let whole = [&blob[..], b"KLM"].concat();
    assert_eq!(range(&fs, "/mixed", 0, None).await, whole);
    assert_eq!(range(&fs, "/mixed", 18, Some(4)).await, &whole[18..22]);
    assert_eq!(range(&fs, "/mixed", 21, None).await, &whole[21..]);
    assert_eq!(range(&fs, "/mixed", 5, Some(0)).await, b"");
    assert_eq!(range(&fs, "/mixed", 100, Some(5)).await, b"");
}

#[tokio::test]
async fn enforces_directory_rules_and_open_modes() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.mkdir(TENANT, "/empty", false, false).await.unwrap();
    assert!(matches!(
        fs.mkdir(TENANT, "/empty", false, false).await,
        Err(Error::AlreadyExists(_))
    ));
    fs.write_bytes(
        TENANT,
        "/empty/file",
        Bytes::from_static(b"x"),
        Durability::Applied,
    )
    .await
    .unwrap();
    assert!(matches!(
        fs.rmdir(TENANT, "/empty", Durability::Applied).await,
        Err(Error::DirectoryNotEmpty(_))
    ));
    fs.open_file(TENANT, "/created", "a", Durability::Applied)
        .await
        .unwrap();
    assert!(fs.exists(TENANT, "/created").await.unwrap());
    assert!(matches!(
        fs.open_file(TENANT, "/created", "x", Durability::Applied)
            .await,
        Err(Error::InvalidMode(_))
    ));
    assert!(matches!(
        fs.write_bytes(
            TENANT,
            "/empty",
            Bytes::from_static(b"x"),
            Durability::Applied
        )
        .await,
        Err(Error::IsDirectory(_))
    ));
}

#[tokio::test]
async fn isolates_tenant_roots_and_rejects_boundary_traversal() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.write_text(
        "tenant-a",
        "/shared.txt",
        "alpha".to_owned(),
        Durability::Applied,
    )
    .await
    .unwrap();
    fs.write_text(
        "tenant-b",
        "/shared.txt",
        "beta".to_owned(),
        Durability::Applied,
    )
    .await
    .unwrap();

    assert_eq!(
        fs.read_text("tenant-a", "/shared.txt").await.unwrap(),
        "alpha"
    );
    assert_eq!(
        fs.read_text("tenant-b", "/shared.txt").await.unwrap(),
        "beta"
    );
    assert!(matches!(
        fs.read_text("tenant-a", "/../shared.txt").await,
        Err(Error::InvalidPath(_))
    ));
    assert!(matches!(
        fs.exists("../tenant-b/shared.txt", "/shared.txt").await,
        Err(Error::InvalidPath(_))
    ));
    assert!(fs.exists("tenant-c", "/shared.txt").await.is_ok_and(|v| !v));
    let globbed = collect(fs.glob("tenant-b", "/**", results(100)).await.unwrap()).await;
    assert_eq!(globbed.len(), 1);
}

#[tokio::test]
async fn persists_metadata_and_pending_blobs_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(
        &dir,
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "pseudofs".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().join("metastore").display().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        }),
    );
    config.metastore_wal_path = Some(dir.path().join("wal").display().to_string());
    let large = Bytes::from(vec![3_u8; 64]);
    let fs = PseudoFs::open(config.clone()).await.unwrap();
    fs.inner.closed.store(true, Ordering::Release);
    fs.write_text(
        TENANT,
        "/small.txt",
        "stored".to_owned(),
        Durability::Written,
    )
    .await
    .unwrap();
    fs.write_bytes(TENANT, "/large.bin", large.clone(), Durability::Written)
        .await
        .unwrap();
    assert_eq!(pending_blobs(&fs).await, 1);
    fs.inner.storage.close().await.unwrap();
    drop(fs);
    assert!(std::fs::read_dir(dir.path().join("wal")).unwrap().count() > 0);

    let reopened = PseudoFs::open(config).await.unwrap();
    assert_eq!(
        reopened.read_text(TENANT, "/small.txt").await.unwrap(),
        "stored"
    );
    assert_eq!(
        reopened.read_bytes(TENANT, "/large.bin").await.unwrap(),
        large
    );
    reopened.run_blob_maintenance().await.unwrap();
    assert_eq!(pending_blobs(&reopened).await, 0);
    assert_eq!(
        std::fs::read_dir(dir.path().join("remote").join("blobs"))
            .unwrap()
            .count(),
        1
    );
    reopened.close().await.unwrap();
}

async fn tree(fs: &PseudoFs) {
    for (path, content) in [
        ("/dir/a.txt", "alpha\nbeta\ngamma\n"),
        (
            "/dir/sub/subfolder/b.txt",
            "one\ntwo\nBeta three\nfour\nfive",
        ),
        ("/dir/x/y/subfolder/c.md", "beta in a blob-sized file\n"),
        ("/dir/x/notsub/d.txt", "nothing here"),
        ("/other/subfolder/e.txt", "beta"),
        ("/dir/bin.dat", "beta\0binary"),
        ("/media/photo.PNG", "beta, but an image by name"),
        ("/media/report", "%PDF-1.7\nbeta\n"),
        (
            "/media/blob.bin.txt",
            "beta: only the last extension counts",
        ),
        ("/media/notes.txt", "beta"),
    ] {
        fs.write_text(TENANT, path, content.to_owned(), Durability::Applied)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn glob_walks_recursive_and_component_patterns() {
    let Harness { fs, _dir, .. } = harness().await;
    tree(&fs).await;
    let paths = |entries: Vec<DirectoryEntry>| {
        let mut paths: Vec<String> = entries.into_iter().map(|entry| entry.path).collect();
        paths.sort();
        paths
    };

    let found = collect(
        fs.glob(TENANT, "/dir/**/subfolder", results(100))
            .await
            .unwrap(),
    )
    .await;
    assert!(found.iter().all(|entry| entry.kind == FileKind::Directory));
    assert_eq!(
        paths(found),
        vec!["/dir/sub/subfolder", "/dir/x/y/subfolder"]
    );
    let found = collect(fs.glob(TENANT, "/**/*.txt", results(100)).await.unwrap()).await;
    assert_eq!(
        paths(found),
        vec![
            "/dir/a.txt",
            "/dir/sub/subfolder/b.txt",
            "/dir/x/notsub/d.txt",
            "/media/blob.bin.txt",
            "/media/notes.txt",
            "/other/subfolder/e.txt"
        ]
    );
    let found = collect(fs.glob(TENANT, "/dir/*", results(100)).await.unwrap()).await;
    assert_eq!(
        paths(found),
        vec!["/dir/a.txt", "/dir/bin.dat", "/dir/sub", "/dir/x"]
    );
    let found = collect(fs.glob(TENANT, "/dir/a.txt", results(100)).await.unwrap()).await;
    assert_eq!(paths(found), vec!["/dir/a.txt"]);
    let found = collect(
        fs.glob(TENANT, "/*/*/subfolder", results(100))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(paths(found), vec!["/dir/sub/subfolder"]);
    let found = collect(
        fs.glob(TENANT, "/dir/{sub,x}/subfolder/b.txt", results(100))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(paths(found), vec!["/dir/sub/subfolder/b.txt"]);

    let mut limited = fs
        .glob(
            TENANT,
            "/**/nothing-matches-this",
            GlobOptions {
                max_results: 100,
                max_scanned_entries: 3,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        limited.recv().await,
        Some(Err(Error::ResourceExhausted(_)))
    ));
    assert!(
        collect(fs.glob(TENANT, "/missing/**", results(100)).await.unwrap())
            .await
            .is_empty()
    );
    assert_eq!(
        collect(fs.glob(TENANT, "/**", results(3)).await.unwrap())
            .await
            .len(),
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_grep_keeps_walk_order_and_match_budget() {
    let Harness { fs, _dir, .. } = harness().await;
    let files = 40;
    for index in 0..files {
        let content = format!("header\nneedle {index}\n{}\n", "padding ".repeat(index % 5));
        fs.write_text(
            TENANT,
            &format!("/logs/{index:03}.log"),
            content,
            Durability::Applied,
        )
        .await
        .unwrap();
    }
    let options = GrepOptions {
        concurrency: 8,
        ..grep_options("needle")
    };
    let found = collect(fs.grep(TENANT, "/logs", options.clone()).await.unwrap()).await;
    let paths: Vec<String> = found.iter().map(|found| found.path.clone()).collect();
    let expected: Vec<String> = (0..files)
        .map(|index| format!("/logs/{index:03}.log"))
        .collect();
    assert_eq!(paths, expected);

    let limited = collect(
        fs.grep(
            TENANT,
            "/logs",
            GrepOptions {
                max_matches: 7,
                ..options
            },
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(limited.len(), 7);
}

fn grep_options(pattern: &str) -> GrepOptions {
    GrepOptions {
        pattern: pattern.to_owned(),
        max_file_size_bytes: 1024 * 1024,
        concurrency: 4,
        ..GrepOptions::default()
    }
}

#[tokio::test]
async fn grep_treats_a_directory_path_literally() {
    let Harness { fs, _dir, .. } = harness().await;
    fs.write_text(
        TENANT,
        "/a\\b/f.txt",
        "hit\n".to_owned(),
        Durability::Applied,
    )
    .await
    .unwrap();
    let found = collect(fs.grep(TENANT, "/a\\b", grep_options("hit")).await.unwrap()).await;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].path, "/a\\b/f.txt");
}

#[tokio::test]
async fn grep_finds_lines_with_context_across_chunk_boundaries() {
    let Harness { fs, _dir, .. } = harness().await;
    tree(&fs).await;

    let found = collect(
        fs.grep(
            TENANT,
            "/dir",
            GrepOptions {
                case_insensitive: true,
                before_context: 1,
                after_context: 2,
                ..grep_options("beta")
            },
        )
        .await
        .unwrap(),
    )
    .await;
    let summary: Vec<(String, u64)> = found
        .iter()
        .map(|found| (found.path.clone(), found.line_number))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("/dir/a.txt".to_owned(), 2),
            ("/dir/sub/subfolder/b.txt".to_owned(), 3),
            ("/dir/x/y/subfolder/c.md".to_owned(), 1),
        ]
    );
    let b = found
        .iter()
        .find(|found| found.path.ends_with("b.txt"))
        .unwrap();
    assert_eq!(b.line, "Beta three");
    assert_eq!(
        b.before
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>(),
        ["two"]
    );
    assert_eq!(
        b.after
            .iter()
            .map(|line| (line.line_number, line.text.as_str()))
            .collect::<Vec<_>>(),
        [(4, "four"), (5, "five")]
    );

    let found = collect(
        fs.grep(TENANT, "/dir/**/*.txt", grep_options("beta"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].path, "/dir/a.txt");

    let found = collect(
        fs.grep(
            TENANT,
            "/",
            GrepOptions {
                max_matches: 2,
                case_insensitive: true,
                ..grep_options("e")
            },
        )
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(found.len(), 2);

    let found = collect(
        fs.grep(
            TENANT,
            "/dir/a.txt",
            GrepOptions {
                fixed_string: true,
                ..grep_options("a.p")
            },
        )
        .await
        .unwrap(),
    )
    .await;
    assert!(found.is_empty());

    let found = collect(
        fs.grep(TENANT, "/media", grep_options("beta"))
            .await
            .unwrap(),
    )
    .await;
    let paths: Vec<&str> = found.iter().map(|found| found.path.as_str()).collect();
    assert_eq!(paths, ["/media/blob.bin.txt", "/media/notes.txt"]);

    assert!(matches!(
        fs.grep(TENANT, "/", grep_options("(")).await,
        Err(Error::InvalidPattern(_))
    ));
    assert!(matches!(
        fs.grep(TENANT, "/nope", grep_options("x")).await,
        Err(Error::NotFound(_))
    ));
}
