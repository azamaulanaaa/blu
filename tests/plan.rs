// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A plan resolves a snapshot against a store and yields an ordered chunk list.
//!
//! The property that matters is determinism.
//! A plan is ordered by the snapshot's own chunk table, not by the order chunks
//! happened to be written.
//! So two backups of one tree produce the same plan.
//! Without that, anything built from a plan differs between runs.
//! That includes a pack, a diff and a delta download.
//! The snapshot's determinism then buys nothing.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;

use blulib::{
    metadata::Snapshot,
    storage::{BlobId, BlobStore, Plan, Read, StorageGet},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Resolve a store's latest version into a plan.
async fn plan_of_latest(path: &std::path::Path) -> Plan {
    let store = BlobStore::open_for_reading(path).await.unwrap();
    let snapshot = latest_snapshot(&store).await;
    Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap()
}

/// The snapshot a store's latest version names.
async fn latest_snapshot(store: &BlobStore<Read>) -> Snapshot {
    use tokio::io::AsyncReadExt;

    let id = store.latest().expect("a committed version").1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    Snapshot::try_from(buffer.as_slice()).unwrap()
}

#[tokio::test]
async fn a_plan_covers_every_chunk_the_snapshot_reads() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), common::noisy(300_000)).unwrap();
    fs::write(source.join("b.bin"), common::noisy(120_000)).unwrap();

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = latest_snapshot(&store).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    assert_eq!(plan.len(), snapshot.chunks.len());

    // The plan counts each chunk once, so it is a pack's size, not the tree's.
    // It is smaller than the sum of the files whenever a chunk is shared.
    // The slice lengths add up to the files exactly.
    let slices: u64 = snapshot
        .file_chunks
        .iter()
        .map(|s| u64::from(s.length))
        .sum();
    assert_eq!(slices, 420_000, "the slices should cover both files");
    assert!(
        plan.total_bytes() < slices,
        "with two independent files nothing is shared, so the plan should \
         account for the same {slices} bytes"
    );

    // Every planned chunk is readable at the place the plan says it is, and has
    // the length the plan claims.
    //
    // `raw_len`, not `stored_len`: a plan's `length` is in content bytes.
    // A plan describes a file rather than a payload.
    // For a compressed blob the content is *longer* than what is on disk.
    // The old test asserted `length <= stored_len`.
    // That is true for uncompressed content and false for compressed.
    // So the test only passed because this fixture happens to be incompressible.
    for chunk in plan.chunks() {
        assert!(chunk.length <= chunk.placement.raw_len);
        let mut reader = store.get(&chunk.id).await.unwrap();
        let mut buffer = vec![0u8; usize::try_from(chunk.length).unwrap()];
        reader.read_exact(&mut buffer).await.unwrap();
        assert_eq!(buffer.len() as u64, chunk.length);
    }
}

/// A store's framing keeps two chunks a restore reads back to back apart.
///
/// This used to be asserted through `Plan::ranges`, which merged adjacent ranges
/// and was asked to produce one range per chunk.
/// It is worth keeping, because it is the reason a delta download cannot be served
/// straight out of a store.
/// It is also the reason a pack is a separate artifact.
/// But the property belongs to the *store*, not to a method that has been deleted.
/// So the property is now stated directly: there is a gap between consecutive chunks
/// in a segment, and that gap is a record header.
#[tokio::test]
async fn a_stores_framing_separates_the_chunks_a_restore_reads_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..8 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(60_000 + i * 4_000),
        )
        .unwrap();
    }

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = latest_snapshot(&store).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    let chunks = plan.chunks();
    assert!(
        chunks.len() > 1,
        "the fixture needs several chunks or there is nothing to be separated from"
    );

    // The whole payload, and the store is bigger than that.
    // The difference is the framing plus the snapshot record.
    assert_eq!(
        plan.total_bytes(),
        chunks.iter().map(|c| c.length).sum::<u64>()
    );
    assert!(
        common::set_size(&blob) > plan.total_bytes(),
        "a store holding one version's chunks should be larger than the chunks, \
         or the framing is not where it was expected"
    );

    // No two chunks the plan needs are adjacent in a segment.
    // If any pair were, a restore could have fetched them in one read.
    // Then the argument for packing would be weaker than this crate says it is.
    let mut touching = 0;
    for pair in chunks.windows(2) {
        let (before, after) = (&pair[0], &pair[1]);
        if before.placement.segment == after.placement.segment
            && before.placement.offset + before.placement.stored_len == after.placement.offset
        {
            touching += 1;
        }
    }
    assert_eq!(
        touching, 0,
        "two chunks are contiguous in a store, so they could have been fetched \
         together"
    );
}

/// The determinism the plan exists for.
///
/// Two backups of one tree are separate runs.
/// Chunks are stored in whatever order concurrent uploads finished and land at
/// different offsets.
/// The plan must not care: same snapshot, same chunk list, same total, whatever
/// the store's layout is.
#[tokio::test]
async fn two_backups_of_one_tree_produce_the_same_plan() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..6 {
        let mut bytes = common::noisy(40_000 + i * 3_000);
        // Shared content between files, so a run has chunks to deduplicate as
        // well as chunks to keep in order.
        bytes.extend_from_slice(&common::noisy(48 * 1024));
        fs::write(source.join(format!("f{i}.bin")), bytes).unwrap();
    }

    let first = dir.path().join("first.bin");
    let second = dir.path().join("second.bin");
    common::run_backup(&source, &first).await;
    common::run_backup(&source, &second).await;

    let a = plan_of_latest(&first).await;
    let b = plan_of_latest(&second).await;

    assert_eq!(a.total_bytes(), b.total_bytes());
    assert_eq!(a.len(), b.len());

    let ids = |p: &Plan| -> Vec<BlobId> { p.iter().map(|c| c.id).collect() };
    assert_eq!(
        ids(&a),
        ids(&b),
        "the plan's order came from how the uploads finished rather than from \
         the snapshot, so two backups of one tree disagree"
    );
}

/// A plan is what a pack writer needs, so the obvious end of it is the smallest
/// possible pack: the payloads, in plan order.
/// Checking that here rather than in a pack's own test is deliberate.
/// It says the plan is already sufficient.
#[tokio::test]
async fn a_plans_payloads_concatenate_into_the_files_it_describes() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let body = common::noisy(200_000);
    fs::write(source.join("a.bin"), &body).unwrap();

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = latest_snapshot(&store).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    // Read every planned chunk and lay it out in plan order, which is exactly
    // what a pack writer does.
    let mut packed = Vec::new();
    for chunk in plan.chunks() {
        let mut reader = store.get(&chunk.id).await.unwrap();
        let mut buffer = vec![0u8; usize::try_from(chunk.length).unwrap()];
        reader.read_exact(&mut buffer).await.unwrap();
        packed.extend_from_slice(&buffer);
    }

    assert_eq!(packed.len() as u64, plan.total_bytes());

    // And reassembling the file from the plan's chunks reproduces the source.
    let file = &snapshot.files[0];
    let mut rebuilt = vec![0u8; usize::try_from(file.len).unwrap()];

    for slice in &snapshot.file_chunks {
        let id = snapshot.chunks[slice.chunk_index as usize];
        let planned = plan
            .chunks()
            .iter()
            .find(|c| c.id == id)
            .expect("every chunk a slice names is in the plan");

        let mut reader = store.get(&id).await.unwrap();
        reader
            .seek(std::io::SeekFrom::Start(u64::from(slice.chunk_offset)))
            .await
            .unwrap();

        let start = usize::try_from(slice.file_offset).unwrap();
        let end = start + slice.length as usize;
        reader
            .take(u64::from(slice.length))
            .read_exact(&mut rebuilt[start..end])
            .await
            .unwrap();

        assert!(planned.length >= u64::from(slice.length));
    }

    assert_eq!(rebuilt, body, "the file did not come back out of the plan");
}
