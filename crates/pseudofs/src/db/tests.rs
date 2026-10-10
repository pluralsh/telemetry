// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::*;
use common::storage::config::{
    LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig, StorageConfig,
};

const TENANT: &str = "tenant-a";

async fn fs() -> PseudoFs {
    PseudoFs::open(Config {
        storage: StorageConfig::InMemory,
        chunk_size_bytes: 3,
        max_file_size_bytes: 1024,
        max_append_generations: 4,
    })
    .await
    .unwrap()
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
    let fs = fs().await;
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
    let fs = fs().await;
    let mut upload = fs.start_upload(TENANT, "/large.bin", false).await.unwrap();
    upload.push(Bytes::from_static(b"abc")).await.unwrap();
    upload.push(Bytes::from_static(b"def")).await.unwrap();
    assert!(!fs.exists(TENANT, "/large.bin").await.unwrap());
    upload.finish(Durability::Applied).await.unwrap();
    assert_eq!(
        fs.read_bytes(TENANT, "/large.bin").await.unwrap(),
        Bytes::from_static(b"abcdef")
    );
}

#[tokio::test]
async fn streamed_upload_coalesces_tiny_messages_into_configured_chunks() {
    let fs = fs().await;
    let mut upload = fs
        .start_upload(TENANT, "/coalesced.bin", false)
        .await
        .unwrap();
    for byte in b"abcdefg" {
        upload.push(Bytes::copy_from_slice(&[*byte])).await.unwrap();
    }
    upload.finish(Durability::Applied).await.unwrap();

    let mut chunks = fs.stream_chunks(TENANT, "/coalesced.bin").await.unwrap();
    let mut lengths = Vec::new();
    while let Some(chunk) = chunks.recv().await {
        lengths.push(chunk.unwrap().len());
    }
    assert_eq!(lengths, vec![3, 3, 1]);
}

#[tokio::test]
async fn active_stream_keeps_its_snapshot_across_overwrite() {
    let fs = fs().await;
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
    let fs = fs().await;
    fs.write_text(TENANT, "/append.txt", "0".to_owned(), Durability::Applied)
        .await
        .unwrap();
    for value in ["1", "2", "3"] {
        fs.append_text(TENANT, "/append.txt", value.to_owned(), Durability::Applied)
            .await
            .unwrap();
    }

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
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
    assert_eq!(fs.read_text(TENANT, "/append.txt").await.unwrap(), "0123");
}

#[tokio::test]
async fn abort_removes_staged_upload() {
    let fs = fs().await;
    let mut upload = fs.start_upload(TENANT, "/partial", false).await.unwrap();
    upload.push(Bytes::from_static(b"abc")).await.unwrap();
    upload.abort().await.unwrap();
    assert!(!fs.exists(TENANT, "/partial").await.unwrap());
}

#[tokio::test]
async fn enforces_directory_rules_and_open_modes() {
    let fs = fs().await;
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
}

#[tokio::test]
async fn isolates_tenant_roots_and_rejects_boundary_traversal() {
    let fs = fs().await;
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
}

#[tokio::test]
async fn persists_across_local_slatedb_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "pseudofs".to_owned(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: directory.path().display().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
            disk: Default::default(),
        }),
        chunk_size_bytes: 3,
        max_file_size_bytes: 1024,
        max_append_generations: 4,
    };
    let fs = PseudoFs::open(config.clone()).await.unwrap();
    fs.write_text(
        TENANT,
        "/persistent.txt",
        "stored".to_owned(),
        Durability::Durable,
    )
    .await
    .unwrap();
    fs.close().await.unwrap();
    drop(fs);

    let reopened = PseudoFs::open(config).await.unwrap();
    assert_eq!(
        reopened.read_text(TENANT, "/persistent.txt").await.unwrap(),
        "stored"
    );
    reopened.close().await.unwrap();
}
