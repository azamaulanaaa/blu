//! A plan resolves a snapshot against a store and yields byte ranges.
//!
//! The property that matters is determinism: a plan is ordered by the snapshot's
//! own chunk table, not by the order chunks happened to be written, so two
//! backups of one tree produce the same plan. Without that, anything built from a
//! plan — a pack, a diff, a delta download — differs between runs, and the
//! snapshot's determinism buys nothing.

// `.clippy.toml` exempts code inside a `#[test]` function; the helper below is a
// plain async fn called from one, so it needs the exemption spelled out.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;

use lepatch::{
    metadata::Snapshot,
    storage::{BlobStore, Plan, SegmentLimit, StorageGet},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Resolve a store's latest version into a plan.
async fn plan_of_latest(path: &std::path::Path) -> Plan {
    let store = BlobStore::<false>::open(path, SegmentLimit::default())
        .await
        .unwrap();
    let snapshot = latest_snapshot(&store).await;
    Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap()
}

/// The snapshot a store's latest version names.
async fn latest_snapshot(store: &BlobStore<false>) -> Snapshot {
    use tokio::io::AsyncReadExt;

    let id = store.latest().expect("a committed version").1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    Snapshot::from_bytes(&buffer).unwrap()
}

#[tokio::test]
async fn a_plan_covers_every_chunk_the_snapshot_reads() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), common::noisy(300_000)).unwrap();
    fs::write(source.join("b.bin"), common::noisy(120_000)).unwrap();

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::<false>::open(&blob, SegmentLimit::default())
        .await
        .unwrap();
    let snapshot = latest_snapshot(&store).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    assert_eq!(plan.len(), snapshot.chunks.len());

    // The plan counts each chunk once, so it is a pack's size, not the tree's.
    // It is smaller than the sum of the files whenever a chunk is shared, and
    // the slice lengths add up to the files exactly.
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
    for chunk in plan.chunks() {
        assert!(chunk.length <= chunk.placement.len);
        let mut reader = store.get(&chunk.id).await.unwrap();
        let mut buffer = vec![0u8; usize::try_from(chunk.length).unwrap()];
        reader.read_exact(&mut buffer).await.unwrap();
        assert_eq!(buffer.len() as u64, chunk.length);
    }
}

#[tokio::test]
async fn the_ranges_of_a_plan_cover_every_byte_it_needs() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..8 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(60_000 + i * 4_000),
        )
        .unwrap();
    }

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::<false>::open(&blob, SegmentLimit::default())
        .await
        .unwrap();
    let snapshot = latest_snapshot(&store).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    let ranges = plan.ranges().unwrap();

    // A framed store keeps a record header between every payload, so two chunks
    // a restore reads back to back are never *contiguous* here and no two ranges
    // merge. That is worth asserting rather than discovering: it is the reason a
    // delta download cannot be served out of the store directly, and the reason
    // a pack — payloads laid end to end with no framing between them — is worth
    // building at all.
    assert_eq!(
        ranges.len(),
        plan.len(),
        "a framed store should not produce adjacent ranges, so {} ranges for {} \
         chunks means the framing is not where it was expected",
        ranges.len(),
        plan.len()
    );

    // Each range is exactly the part of the chunk the version reads, and the
    // covered total matches the plan.
    let covered: u64 = ranges.iter().map(lepatch::storage::Range::len).sum();
    assert_eq!(covered, plan.total_bytes());

    // Ranges are ordered and disjoint, so a concatenation of them is the plan.
    for pair in ranges.windows(2) {
        assert!(
            pair[0].end <= pair[1].start,
            "ranges overlap or are out of order: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }

    // And no range reaches into a record header, which would mean reading framing
    // as if it were chunk data.
    let store_len = common::set_size(&blob);
    for range in &ranges {
        assert!(
            range.end <= store_len,
            "a range runs past the end of the store"
        );
        for chunk in plan.chunks() {
            if chunk.placement.offset == range.start {
                assert!(
                    range.end <= chunk.placement.offset + chunk.placement.len,
                    "a range reads past the chunk it starts at"
                );
            }
        }
    }
}

/// The determinism the plan exists for.
///
/// Two backups of one tree are separate runs, so chunks are stored in whatever
/// order concurrent uploads finished and land at different offsets. The plan
/// must not care: same snapshot, same chunk list, same total, whatever the
/// store's layout is.
#[tokio::test]
async fn two_backups_of_one_tree_produce_the_same_plan() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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

    let ids = |p: &Plan| -> Vec<[u8; 32]> { p.chunks().iter().map(|c| c.id).collect() };
    assert_eq!(
        ids(&a),
        ids(&b),
        "the plan's order came from how the uploads finished rather than from \
         the snapshot, so two backups of one tree disagree"
    );
}

/// A plan is what a pack writer needs, so the obvious end of it is the smallest
/// possible pack: the payloads, in plan order. Checking that here rather than in
/// a pack's own test is deliberate — it says the plan is already sufficient.
#[tokio::test]
async fn a_plans_payloads_concatenate_into_the_files_it_describes() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let body = common::noisy(200_000);
    fs::write(source.join("a.bin"), &body).unwrap();

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::<false>::open(&blob, SegmentLimit::default())
        .await
        .unwrap();
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
