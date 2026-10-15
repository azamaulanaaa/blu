// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Content round-tripping through backup and restore.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;

use common::{assert_round_trip, noisy};

/// Build a tree that exercises the shapes a backup tool has to survive.
/// The tree has nested directories.
/// The files are smaller and much larger than the chunker configuration.
///
/// Empty files, empty directories and links are covered separately in
/// `tests/fidelity.rs`.
fn build_tree(root: &std::path::Path) {
    fs::create_dir_all(root.join("nested/deep")).unwrap();
    fs::write(root.join("large.bin"), vec![0xabu8; 300_000]).unwrap();
    fs::write(root.join("nested/small.txt"), b"hello world\n").unwrap();
    fs::write(root.join("nested/deep/tiny"), b"x").unwrap();
}

#[tokio::test]
async fn round_trip_reproduces_nested_tree() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    build_tree(&source);

    assert_round_trip(&source).await;
}

#[tokio::test]
async fn round_trip_preserves_exact_bytes_across_chunk_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Sizes straddling the 8 KiB minimum and 16 KiB average so that chunks
    // both fall inside a single file and span a file boundary.
    for size in [1usize, 8_191, 8_192, 16_384, 65_536, 200_000] {
        fs::write(source.join(format!("f{size}.bin")), noisy(size)).unwrap();
    }

    assert_round_trip(&source).await;
}

#[tokio::test]
async fn round_trip_reproduces_a_file_larger_than_the_chunk_maximum() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // 1 MiB of non-repeating data forces many chunks and exercises the
    // multi-chunk write path within a single file.
    fs::write(source.join("big.bin"), noisy(1_048_576)).unwrap();

    assert_round_trip(&source).await;
}

/// The restore builder writes a path selection and skips the rest.
///
/// The plan names the selected file only. The run writes that file.
/// The unselected file stays absent from the destination.
#[tokio::test]
async fn restore_builder_writes_the_selected_paths_only() {
    use blulib::{
        command::{RestoreBuilder, RestorePlanBuilder},
        storage::BlobStore,
    };

    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("keep.bin"), noisy(50_000)).unwrap();
    fs::write(source.join("skip.bin"), noisy(50_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    let plan = RestorePlanBuilder::new(committed.snapshot(), store)
        .files(["keep.bin"])
        .run()
        .await
        .unwrap();
    assert_eq!(plan.files, 1, "the plan covers the selection only");

    let destination = dir.path().join("restored");
    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    RestoreBuilder::new(&destination, committed.snapshot(), store)
        .files(["keep.bin"])
        .run()
        .await
        .unwrap();
    assert!(
        destination.join("keep.bin").exists(),
        "the selected file lands"
    );
    assert!(
        !destination.join("skip.bin").exists(),
        "the unselected file stays absent"
    );
}
