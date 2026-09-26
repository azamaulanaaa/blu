//! Deduplication: repeated content must be stored once, both within a single
//! run and across runs that share a base snapshot.

// `.clippy.toml` exempts code inside a `#[test]` function; the `snapshot`
// helper below is a plain async fn called from one, so it needs the exemption
// spelled out.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;

use lepatch::{
    command::Options,
    storage::{BlobFileStorage, StorageGet},
};

/// Read a snapshot back out of a blob store.
async fn snapshot(blob: &std::path::Path, key: &str) -> lepatch::metadata::Snapshot {
    use tokio::io::AsyncReadExt;

    let storage = BlobFileStorage::<false>::new(blob).await.unwrap();
    let mut reader = storage.get(key).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    lepatch::metadata::Snapshot::from_bytes(&buffer).unwrap()
}

#[tokio::test]
async fn repeated_content_within_one_run_is_stored_once() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // The same 64 KiB block in three files. Identical content at an identical
    // offset, so the chunker is guaranteed to cut it the same way each time.
    let block = vec![0x5au8; 64 * 1024];
    for name in ["a.bin", "b.bin", "c.bin"] {
        fs::write(source.join(name), &block).unwrap();
    }

    let blob = dir.path().join("store.bin");
    let key = common::run_backup(&source, &blob, None).await;
    let snapshot = snapshot(&blob, &key).await;

    assert_eq!(
        snapshot.chunks.len(),
        1,
        "three identical files should collapse to a single stored chunk, got {}",
        snapshot.chunks.len()
    );

    // 192 KiB of input must not have grown the store by 192 KiB.
    let stored = fs::metadata(&blob).unwrap().len();
    assert!(
        stored < 128 * 1024,
        "blob store grew to {stored} bytes, deduplication did not happen"
    );
}

#[tokio::test]
async fn a_second_run_reuses_chunks_from_its_base_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("stable.bin"), vec![0x11u8; 100_000]).unwrap();

    let blob = dir.path().join("store.bin");

    let first = common::run_backup(&source, &blob, None).await;
    let after_first = fs::metadata(&blob).unwrap().len();

    // An unchanged tree backed up against its own previous snapshot must not
    // upload a single byte.
    let second = common::run_backup(&source, &blob, Some(first.clone())).await;
    let after_second = fs::metadata(&blob).unwrap().len();

    assert_ne!(
        first, second,
        "each run should produce its own snapshot key"
    );

    // The second run still writes its own snapshot blob, so the store grows a
    // little. It must not grow by anything like the size of the data.
    let growth = after_second - after_first;
    assert!(
        growth < 8 * 1024,
        "re-backing up an unchanged tree grew the store by {growth} bytes, \
         which suggests chunk data was uploaded again"
    );

    // Every chunk the second snapshot references must already be present in
    // the store, i.e. no new location was minted.
    let first_snapshot = snapshot(&blob, &first).await;
    let second_snapshot = snapshot(&blob, &second).await;

    let locations = |s: &lepatch::metadata::Snapshot| -> std::collections::BTreeSet<_> {
        s.chunks.iter().map(|c| c.location.clone()).collect()
    };

    assert_eq!(
        locations(&first_snapshot),
        locations(&second_snapshot),
        "the second run minted new chunk locations instead of reusing the base snapshot"
    );

    // Both snapshots must still describe the same content.
    assert_eq!(first_snapshot.file_chunks, second_snapshot.file_chunks);
}

#[tokio::test]
async fn deduplication_does_not_change_the_restored_tree() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    for name in ["a.bin", "b.bin", "c.bin"] {
        fs::write(source.join(name), vec![0x5au8; 64 * 1024]).unwrap();
    }
    fs::write(source.join("unique.txt"), b"not shared").unwrap();

    let blob = dir.path().join("store.bin");
    let key = common::run_backup(&source, &blob, None).await;
    let destination = dir.path().join("restored");
    common::run_restore(&blob, &key, &destination).await;

    assert_eq!(common::describe(&source), common::describe(&destination));
}

#[tokio::test]
async fn chunks_are_deduplicated_however_many_are_in_flight() {
    // Every one of these files is one 64 KiB run of identical bytes, which the
    // chunker cuts at its maximum size — so each file is exactly one chunk and
    // all of them hash the same. That is the case worth racing on: the same
    // content reaching several stores at once, where a deduplication check that
    // only works in sequence would store a copy per worker.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let shared = vec![0x5au8; 64 * 1024];
    for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        fs::write(source.join(format!("{name}.bin")), &shared).unwrap();
    }

    // Plus content that must *not* be deduplicated, so a run cannot pass simply
    // by collapsing everything into one entry.
    fs::write(source.join("one.bin"), common::noisy(70 * 1024)).unwrap();
    fs::write(source.join("two.bin"), common::noisy(70 * 1024)).unwrap();

    let serial = count_at(&dir.path().join("serial.bin"), &source, 1).await;

    // The eight identical files must have become one chunk, not eight.
    assert!(
        serial < 8,
        "eight identical files produced {serial} stored chunks, so they were not \
         deduplicated at all"
    );

    for concurrency in [2, 4, 8, 16, 32] {
        let blob = dir.path().join(format!("parallel{concurrency}.bin"));
        assert_eq!(
            count_at(&blob, &source, concurrency).await,
            serial,
            "with {concurrency} stores in flight a different number of chunks was stored"
        );
    }
}

/// Back up `source` with `concurrency` stores in flight, and report how many
/// distinct chunks ended up stored.
async fn count_at(blob: &std::path::Path, source: &std::path::Path, concurrency: usize) -> usize {
    let key = common::run_backup_with(
        source,
        blob,
        None,
        Options {
            concurrency,
            ..Options::default()
        },
    )
    .await;

    snapshot(blob, &key).await.chunks.len()
}

#[tokio::test]
async fn concurrency_does_not_change_the_snapshot() {
    // Uploads finish in whatever order the store lets them, but the snapshot
    // must depend only on the order chunks were read in. If this drifts, two
    // backups of the same tree stop being comparable and the concurrency knob
    // stops being safe to touch.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // Several distinct files plus shared content between them, so a run has
    // chunks to deduplicate as well as chunks to keep in order.
    for i in 0..6 {
        let mut bytes = common::noisy(40 * 1024 + i * 997);
        bytes.extend_from_slice(&common::noisy(20 * 1024));
        fs::write(source.join(format!("file{i}.bin")), &bytes).unwrap();
    }

    let serial = {
        let blob = dir.path().join("serial.bin");
        let key = common::run_backup_with(
            &source,
            &blob,
            None,
            Options {
                concurrency: 1,
                ..Options::default()
            },
        )
        .await;
        snapshot(&blob, &key).await
    };

    for concurrency in [2, 8, 32] {
        let blob = dir.path().join(format!("parallel{concurrency}.bin"));
        let key = common::run_backup_with(
            &source,
            &blob,
            None,
            Options {
                concurrency,
                ..Options::default()
            },
        )
        .await;
        let parallel = snapshot(&blob, &key).await;

        assert_eq!(
            serial.chunks, parallel.chunks,
            "concurrency {concurrency} stored a different set of chunks"
        );
        assert_eq!(
            serial.file_chunks, parallel.file_chunks,
            "concurrency {concurrency} laid the chunks out differently"
        );
        assert_eq!(
            serial.files, parallel.files,
            "concurrency {concurrency} recorded the files differently"
        );
    }
}
