//! Segment rollover: a set under a size limit, and what the limit buys.
//!
//! The tests run against a limit far below the 4 GiB default, because the
//! property under test is the arithmetic of a boundary and it is the same
//! arithmetic at any size.
//!
//! One consequence of the design is worth stating before reading them: **a
//! snapshot is a single record**, so the limit has to exceed a snapshot even when
//! the data itself spreads over many segments. [`tiny`] is 4 KiB and is used only
//! for hand-placed blobs; a real backup needs [`small`], which is still 65,000x
//! under the default and still rolls over a few hundred kilobytes of tree.

// `.clippy.toml` exempts code inside a `#[test]` function; the helpers below are
// plain functions that tests call, so they need the exemption spelled out.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{fs, path::Path};

use lepatch::{
    index::IndexSet,
    storage::{BlobId, BlobStore, SegmentLimit, StorageGet, StoragePut},
};

/// A limit small enough that a few hand-placed blobs roll over.
fn tiny() -> SegmentLimit {
    SegmentLimit::new(4096)
}

/// A limit small enough for a real tree to roll over, and large enough to hold a
/// snapshot.
fn small() -> SegmentLimit {
    SegmentLimit::new(64 * 1024)
}

/// Store `payloads` one at a time, returning the ids in the order stored.
async fn put_all(blob: &Path, limit: SegmentLimit, payloads: &[Vec<u8>]) -> Vec<BlobId> {
    let store = BlobStore::<true>::open(blob, limit).await.unwrap();

    let mut ids = Vec::new();
    for payload in payloads {
        store
            .put(
                &common::id_of(payload),
                common::reader(payload.clone()),
                common::len_of(payload),
            )
            .await
            .unwrap();
        ids.push(common::id_of(payload));
    }

    store.sync_data().await.unwrap();
    ids
}

/// Content that is not a repeating pattern, so the chunker is irrelevant here but
/// the hashes are still distinct.
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i * 31 + usize::from(seed) * 7) % 251).unwrap())
        .collect()
}

/// A set that outgrows its limit has more than one segment.
#[tokio::test]
async fn a_set_that_outgrows_its_limit_rolls_over() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..8).map(|i| payload(i, 900)).collect();
    let ids = put_all(&blob, tiny(), &payloads).await;

    let store = BlobStore::<false>::open(&blob, tiny()).await.unwrap();
    assert!(
        store.segment_count() > 1,
        "8 payloads of 900 bytes under a 4 KiB limit should have needed \
         {} segment(s), and they all went into {}",
        store.segment_count(),
        store.segment_count()
    );

    // Everything is still readable, and byte for byte.
    for (payload, id) in payloads.iter().zip(&ids) {
        let mut reader = store.get(id).await.unwrap();
        let mut read = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut read)
            .await
            .unwrap();
        assert_eq!(&read, payload);
    }
}

/// No segment exceeds the limit.
///
/// The whole point of the limit. A record that would push a segment past it starts
/// the next segment instead, so this holds for every segment rather than on
/// average — and a record larger than a whole segment is an error rather than a
/// segment that quietly breaks the promise.
#[tokio::test]
async fn no_segment_exceeds_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let limit = tiny();

    let payloads: Vec<Vec<u8>> = (0..12).map(|i| payload(i, 700)).collect();
    put_all(&blob, limit, &payloads).await;

    for segment in IndexSet::new(&blob).segments().unwrap() {
        let len = fs::metadata(&segment).unwrap().len();
        assert!(
            len <= limit.bytes(),
            "{} is {len} bytes, over the {}-byte limit",
            segment.display(),
            limit.bytes()
        );
    }
}

/// A record larger than the whole limit is refused, and the set is left alone.
///
/// Silently exceeding the limit would defeat the reason segments exist. The error
/// names the limit so the caller can raise it deliberately rather than discover
/// afterwards that one file is 5 GiB.
#[tokio::test]
async fn a_record_larger_than_the_limit_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let limit = tiny();

    let big = payload(1, 8_192);
    let store = BlobStore::<true>::open(&blob, limit).await.unwrap();

    let error = store
        .put(
            &common::id_of(&big),
            common::reader(big.clone()),
            common::len_of(&big),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("does not fit in a segment"),
        "the error should name the problem, got: {error}"
    );
    assert!(
        error.to_string().contains(&limit.bytes().to_string()),
        "the error should quantify the limit, got: {error}"
    );

    // And no second segment was created for it.
    assert_eq!(
        IndexSet::new(&blob).segments().unwrap().len(),
        1,
        "a refused record should not leave a segment behind"
    );
}

/// A run that crosses a boundary produces a set that reads back whole.
#[tokio::test]
async fn a_set_built_across_several_segments_restores() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // Big enough to need several segments, varied enough to produce many chunks.
    for i in 0..4 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(120_000 + i * 9_000),
        )
        .unwrap();
    }

    let blob = dir.path().join("store");
    let committed = common::run_backup_limited(&source, &blob, small()).await;

    let store = BlobStore::<false>::open(&blob, small()).await.unwrap();
    assert!(
        store.segment_count() > 1,
        "a ~500 KB tree under a 4 KiB limit should have produced many segments"
    );

    let destination = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &destination).await;

    assert_eq!(
        common::describe(&source),
        common::describe(&destination),
        "a set spread over many segments should still restore the whole tree"
    );
}

/// A version is addressable no matter which segment it was committed in.
///
/// A commit record lands wherever the log happened to be, and a run that filled
/// the last segment commits into the next one. Both have to be reachable by
/// number, or a long-lived set develops versions that cannot be restored.
#[tokio::test]
async fn a_version_committed_in_a_later_segment_is_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), common::noisy(50_000)).unwrap();

    let blob = dir.path().join("store");

    let mut versions = Vec::new();
    for i in 0..4 {
        fs::write(source.join("a.bin"), common::noisy(50_000 + i * 7_000)).unwrap();
        versions.push(common::run_backup_limited(&source, &blob, small()).await);
    }

    let store = BlobStore::<false>::open(&blob, small()).await.unwrap();
    assert!(store.segment_count() > 1, "the set should have rolled over");

    // Every version, each in whichever segment holds its commit record.
    for expected in versions {
        let snapshot = store.snapshot(expected.version).unwrap();
        assert_eq!(snapshot, expected.snapshot);

        let destination = dir.path().join(format!("out{}", expected.version));
        common::run_restore(&blob, snapshot, &destination).await;
    }
}

/// A sealed segment with a torn tail is refused, not truncated.
///
/// Every segment but the last is sealed: nothing writes to it again. So a tail
/// that does not verify in one of them is damage, not an unfinished run, and the
/// one operation that could destroy evidence of it — truncating — must not run.
/// This is the strongest property the layout buys and the one worth a test.
#[tokio::test]
async fn a_sealed_segment_with_a_damaged_tail_is_refused_not_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..10).map(|i| payload(i, 900)).collect();
    put_all(&blob, tiny(), &payloads).await;

    let segments = IndexSet::new(&blob).segments().unwrap();
    assert!(segments.len() > 1, "the set should have rolled over");

    // A sealed one: anything but the last.
    let sealed = &segments[0];
    let sealed_claim = BlobStore::<false>::open(&blob, tiny()).await.unwrap();
    let first_placement = sealed_claim.locate(&common::id_of(&payloads[0])).unwrap();
    assert_eq!(
        first_placement.segment, 0,
        "the first payload should be in segment 0, which is the sealed one"
    );

    // Append a record header with no payload behind it, which is what a torn
    // write at the tail of a segment looks like.
    let mut bytes = fs::read(sealed).unwrap();
    bytes.extend_from_slice(b"LPB1\x00\x80\x01");
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(b"half a record");
    let damaged_len = bytes.len();
    fs::write(sealed, &bytes).unwrap();

    let error = BlobStore::<true>::open(&blob, tiny()).await.unwrap_err();
    assert!(
        error.to_string().contains("sealed segment"),
        "a sealed segment's damage should be named as such, got: {error}"
    );

    assert_eq!(
        fs::metadata(sealed).unwrap().len(),
        u64::try_from(damaged_len).unwrap(),
        "the damaged sealed segment was truncated, which is the one thing this \
         pass must never do"
    );
}

/// The last segment's torn tail *is* reclaimed, by a run.
///
/// The complement of the sealed case: the one segment that was being written to
/// can have a torn tail from a killed run, and the next run discards it. That is
/// the only repair this layout does.
#[tokio::test]
async fn the_last_segments_torn_tail_is_reclaimed_by_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), common::noisy(40_000)).unwrap();

    let blob = dir.path().join("store");
    common::run_backup_limited(&source, &blob, small()).await;

    let before = common::set_size(&blob);

    let last = IndexSet::new(&blob).segments().unwrap().pop().unwrap();

    let mut bytes = fs::read(&last).unwrap();
    bytes.extend_from_slice(b"LPB1\x00\x80\x01");
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(b"half a record");
    let damaged_len = u64::try_from(bytes.len()).unwrap();
    fs::write(&last, &bytes).unwrap();

    // A read refuses, and does not repair.
    assert!(BlobStore::<false>::open(&blob, small()).await.is_err());

    // A run truncates and carries on, publishing a second version over the first.
    let committed = common::run_backup_limited(&source, &blob, small()).await;

    assert_eq!(
        committed.version, 2,
        "the first version should still be there"
    );

    // The torn tail is gone and only the new run's records took its place, so the
    // set grew by a commit record's worth rather than keeping 30 bytes of garbage
    // and adding a whole run on top of it.
    let after = common::set_size(&blob);
    assert!(
        after < before + damaged_len,
        "the set grew from {before} to {after} after reclaiming a tail that had \
         made it {damaged_len} bytes; the torn tail was not discarded"
    );

    // And both versions still restore, which is the part that matters: a repair
    // that cost a version would not be a repair.
    let store = BlobStore::<false>::open(&blob, small()).await.unwrap();
    for version in 1..=2 {
        let destination = dir.path().join(format!("out{version}"));
        common::run_restore(&blob, store.snapshot(version).unwrap(), &destination).await;
        assert_eq!(
            common::describe(&source),
            common::describe(&destination),
            "version {version} should still restore after the repair"
        );
    }
}

/// A segment moved or renamed is refused rather than read in the wrong place.
///
/// Each segment's header says which segment it is. Without that, swapping two
/// files would produce a set that scans cleanly and holds the right bytes in the
/// wrong order — the kind of damage that verifies clean and restores wrong.
#[tokio::test]
async fn a_segment_renamed_into_another_position_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..10).map(|i| payload(i, 900)).collect();
    put_all(&blob, tiny(), &payloads).await;

    let set = IndexSet::new(&blob);
    let segments = set.segments().unwrap();
    assert!(segments.len() > 1);

    // Swap two sealed segments' contents.
    let (a, b) = (&segments[0], &segments[1]);
    let first = fs::read(a).unwrap();
    let second = fs::read(b).unwrap();
    fs::write(a, &second).unwrap();
    fs::write(b, &first).unwrap();

    let error = BlobStore::<false>::open(&blob, tiny()).await.unwrap_err();
    assert!(
        error.to_string().contains("header says it is segment"),
        "a segment in the wrong position should be refused by name, got: {error}"
    );
}

/// Concurrent writers across a rollover do not overlap.
///
/// The claim is now a short lock rather than one `fetch_add`, because a rollover
/// decision is not a single atomic. If that lock is wrong, two writers agree on an
/// offset, one clobbers the other, and both records still verify — the same
/// silent corruption the lock was introduced to avoid. Payloads are filled with
/// their own index so an overlap shows up as a blob holding someone else's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_across_a_rollover_do_not_overlap() {
    const BLOBS: usize = 48;

    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let limit = tiny();

    let payloads: Vec<Vec<u8>> = (0..BLOBS)
        .map(|i| payload(u8::try_from(i).unwrap(), 600 + i * 3))
        .collect();
    let store = std::sync::Arc::new(BlobStore::<true>::open(&blob, limit).await.unwrap());

    let mut tasks = Vec::new();
    for payload in payloads.iter().cloned() {
        let store = std::sync::Arc::clone(&store);
        let id = common::id_of(&payload);
        let len = common::len_of(&payload);
        tasks.push(tokio::spawn(async move {
            store.put(&id, common::reader(payload), len).await?;
            Ok::<_, std::io::Error>(id)
        }));
    }

    let mut stored = Vec::new();
    for task in tasks {
        stored.push(task.await.unwrap().unwrap());
    }

    let reader_store = BlobStore::<false>::open(&blob, limit).await.unwrap();
    assert!(
        reader_store.segment_count() > 1,
        "the set should have rolled over"
    );

    for (i, id) in stored.iter().enumerate() {
        let mut reader = reader_store.get(id).await.unwrap();
        let mut read = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut read)
            .await
            .unwrap();
        assert_eq!(
            &read, &payloads[i],
            "blob {i} came back holding another blob's bytes, so a range was \
             claimed twice"
        );
    }
}

/// The limit is a write policy, not part of the format.
///
/// A set written under one limit reads back under another, because no record
/// depends on where a boundary fell. That is what makes it safe to change the
/// limit mid-life — and it is why the limit is not in the store header, where it
/// would make an existing set unreadable the moment somebody changed their mind.
#[tokio::test]
async fn a_set_written_under_one_limit_reads_under_another() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..10).map(|i| payload(i, 900)).collect();
    let ids = put_all(&blob, tiny(), &payloads).await;

    // Opened with a limit a hundred times bigger, and one a hundred times smaller.
    for limit in [SegmentLimit::new(1 << 20), SegmentLimit::new(512)] {
        let store = BlobStore::<false>::open(&blob, limit).await.unwrap();

        for (payload, id) in payloads.iter().zip(&ids) {
            let mut reader = store.get(id).await.unwrap();
            let mut read = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut read)
                .await
                .unwrap();
            assert_eq!(
                &read,
                payload,
                "a set written under a 4 KiB limit should read back under {}",
                limit.bytes()
            );
        }
    }
}

/// A limit below one record's worth of space is raised to something usable.
///
/// A segment that cannot hold its own header cannot be a segment, and a limit
/// that produced one would fail every write with an error about a record not
/// fitting — which is true and useless.
#[test]
fn a_limit_too_small_to_hold_a_record_is_raised() {
    let limit = SegmentLimit::new(1);

    assert!(
        limit.bytes() > 1,
        "a limit of 1 byte should have been raised, got {}",
        limit.bytes()
    );
}
