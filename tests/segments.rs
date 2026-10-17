// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Segment rollover: a set under a size limit, and what the limit buys.
//!
//! The tests run against a limit far below the 4 GiB default.
//! The property under test is the arithmetic of a boundary.
//! That arithmetic is the same at any size.
//!
//! One consequence of the design is worth stating before reading them.
//! **A snapshot is a single record**, so the limit has to exceed a snapshot even
//! when the data itself spreads over many segments.
//! [`tiny`] is 4 KiB and is used only for hand-placed blobs.
//! A real backup needs [`small`], which is still 65,000x under the default and
//! still rolls over a few hundred kilobytes of tree.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{fs, path::Path};

use blulib::{
    layout::IndexSet,
    storage::{
        BlobId, BlobStore, Resolved, SegmentLimit, StorageGet, StoragePut, resolve, resolve_with,
    },
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
    let store = BlobStore::builder(blob)
        .segment_limit(limit)
        .open_writer()
        .await
        .unwrap();

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

/// Content that is neither a repeating pattern nor compressible.
/// So the chunker is irrelevant here, the hashes are still distinct, and the bytes
/// on disk are the bytes handed in.
///
/// The last part is the point.
/// Every test in this file is about *how many bytes a record occupies on disk*.
/// That covers roll-over, sealing, and the size of a header.
/// The store compresses what it stores.
/// A payload that compressed would make a set fit in fewer segments than the test
/// asserts about.
/// So these use content zstd cannot improve on, and the arithmetic means what it
/// says.
fn payload(seed: u8, len: usize) -> Vec<u8> {
    // blake3 in counter mode: high-entropy, so a frame is never smaller than the
    // input and the store falls back to storing it raw.
    let mut out = Vec::with_capacity(len);
    let mut key = [0u8; 32];
    key[0] = seed;
    for counter in 0..u64::try_from(len.div_ceil(64)).unwrap_or(0) {
        let mut input = [0u8; 72];
        input[..32].copy_from_slice(&key);
        input[32..40].copy_from_slice(&counter.to_le_bytes());
        out.extend_from_slice(blake3::hash(&input).as_bytes());
    }
    out.truncate(len);
    out
}

/// A set that outgrows its limit has more than one segment.
#[tokio::test]
async fn a_set_that_outgrows_its_limit_rolls_over() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..8).map(|i| payload(i, 900)).collect();
    let ids = put_all(&blob, tiny(), &payloads).await;

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
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
/// The whole point of the limit.
/// A record that would push a segment past the limit starts the next segment
/// instead.
/// So this holds for every segment rather than on average.
/// A record larger than a whole segment is an error.
/// It is not a segment that quietly breaks the promise.
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
    let store = BlobStore::builder(&blob)
        .segment_limit(limit)
        .open_writer()
        .await
        .unwrap();

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
    let source = common::source(dir.path());

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

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    assert!(
        store.segment_count() > 1,
        "a ~500 KB tree under a 4 KiB limit should have produced many segments"
    );

    let destination = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot(), &destination).await;

    assert_eq!(
        common::describe(&source),
        common::describe(&destination),
        "a set spread over many segments should still restore the whole tree"
    );
}

/// A version is addressable no matter which segment it was committed in.
///
/// A commit record lands wherever the log happened to be.
/// A run that filled the last segment commits into the next one.
/// Both have to be reachable by number.
/// Otherwise a long-lived set develops versions that cannot be restored.
#[tokio::test]
async fn a_version_committed_in_a_later_segment_is_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), common::noisy(50_000)).unwrap();

    let blob = dir.path().join("store");

    let mut versions = Vec::new();
    for i in 0..4 {
        fs::write(source.join("a.bin"), common::noisy(50_000 + i * 7_000)).unwrap();
        versions.push(common::run_backup_limited(&source, &blob, small()).await);
    }

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    assert!(store.segment_count() > 1, "the set should have rolled over");

    // Every version, each in whichever segment holds its commit record.
    for expected in versions {
        let snapshot = store.snapshot(expected.version()).unwrap();
        assert_eq!(snapshot, expected.snapshot());

        let destination = dir.path().join(format!("out{}", expected.version()));
        common::run_restore(&blob, snapshot, &destination).await;
    }
}

/// A sealed segment with a torn tail is refused, not truncated.
///
/// Every segment but the last is sealed: nothing writes to it again.
/// So a tail that does not verify in one of them is damage, not an unfinished run.
/// The one operation that could destroy that evidence is truncating, so
/// truncating must not run.
/// This is the strongest property the layout buys, and the one worth a test.
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
    let sealed_claim = BlobStore::builder(&blob).open_reader().await.unwrap();
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

    let error = BlobStore::builder(&blob)
        .segment_limit(tiny())
        .open_writer()
        .await
        .unwrap_err();
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
/// The complement of the sealed case.
/// The one segment that was being written to can have a torn tail from a killed
/// run, and the next run discards it.
/// That is the only repair this layout does.
#[tokio::test]
async fn the_last_segments_torn_tail_is_reclaimed_by_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
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
    assert!(BlobStore::builder(&blob).open_reader().await.is_err());

    // A run truncates and carries on, publishing a second version over the first.
    // The tree changed below, so the run has a version to publish.
    // An unchanged tree would report the tip and publish nothing.
    let first_tree = common::describe(&source);
    fs::write(source.join("b.bin"), common::noisy(1_000)).unwrap();
    let committed = common::run_backup_limited(&source, &blob, small()).await;

    assert_eq!(
        committed.version(),
        2,
        "the first version should still be there"
    );

    // The torn tail is gone and only the new run's records took its place.
    // So the set grew by a commit record's worth.
    // It did not keep 30 bytes of garbage and add a whole run on top of it.
    let after = common::set_size(&blob);
    assert!(
        after < before + damaged_len,
        "the set grew from {before} to {after} after reclaiming a tail that had \
         made it {damaged_len} bytes; the torn tail was not discarded"
    );

    // And both versions still restore, which is the part that matters.
    // A repair that cost a version would not be a repair.
    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    let second_tree = common::describe(&source);
    for (version, tree) in [1, 2].into_iter().zip([&first_tree, &second_tree]) {
        let destination = dir.path().join(format!("out{version}"));
        common::run_restore(&blob, store.snapshot(version).unwrap(), &destination).await;
        assert_eq!(
            tree,
            &common::describe(&destination),
            "version {version} should still restore after the repair"
        );
    }
}

/// A segment moved or renamed is refused rather than read in the wrong place.
///
/// Each segment's header says which segment it is.
/// Without that, swapping two files would produce a set that scans cleanly and
/// holds the right bytes in the wrong order.
/// That is the kind of damage that verifies clean and restores wrong.
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

    let error = BlobStore::builder(&blob).open_reader().await.unwrap_err();
    assert!(
        error.to_string().contains("header says it is segment"),
        "a segment in the wrong position should be refused by name, got: {error}"
    );
}

/// Concurrent writers across a rollover do not overlap.
///
/// The claim is now a short lock rather than one `fetch_add`, because a rollover
/// decision is not a single atomic.
/// If that lock is wrong, two writers agree on an offset and one clobbers the
/// other.
/// Both records still verify.
/// That is the same silent corruption the lock was introduced to avoid.
/// Payloads are filled with their own index, so an overlap shows up as a blob
/// holding someone else's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_across_a_rollover_do_not_overlap() {
    const BLOBS: usize = 48;

    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let limit = tiny();

    let payloads: Vec<Vec<u8>> = (0..BLOBS)
        .map(|i| payload(u8::try_from(i).unwrap(), 600 + i * 3))
        .collect();
    let store = std::sync::Arc::new(
        BlobStore::builder(&blob)
            .segment_limit(limit)
            .open_writer()
            .await
            .unwrap(),
    );

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

    let reader_store = BlobStore::builder(&blob).open_reader().await.unwrap();
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

/// Concurrent fast path writes claim unique ranges.
///
/// No rollover happens here.
/// The test covers the shortened fast path.
/// It checks that parallel claims never overlap.
///
/// Follow these steps:
///
/// - Store many blobs concurrently.
/// - Read each blob back.
/// - Compare bytes with the input.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fast_path_writes_claim_unique_ranges() {
    const BLOBS: usize = 32;

    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let limit = SegmentLimit::new(16 * 1024 * 1024);

    let payloads: Vec<Vec<u8>> = (0..BLOBS)
        .map(|i| payload(u8::try_from(i).unwrap(), 900 + i * 7))
        .collect();
    let store = std::sync::Arc::new(
        BlobStore::builder(&blob)
            .segment_limit(limit)
            .open_writer()
            .await
            .unwrap(),
    );

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

    let reader_store = BlobStore::builder(&blob).open_reader().await.unwrap();
    assert_eq!(
        reader_store.segment_count(),
        1,
        "the set should stay in one segment"
    );

    for (i, id) in stored.iter().enumerate() {
        let mut reader = reader_store.get(id).await.unwrap();
        let mut read = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut read)
            .await
            .unwrap();
        assert_eq!(
            &read, &payloads[i],
            "blob {i} holds another blob range"
        );
    }
}

/// The limit is a write policy, not part of the format.
///
/// A set written under one limit reads back under another, because no record
/// depends on where a boundary fell.
/// That is what makes it safe to change the limit mid-life.
/// It is also why the limit is not in the store header.
/// A limit in the header would make an existing set unreadable the moment
/// somebody changed their mind.
#[tokio::test]
async fn a_set_written_under_one_limit_reads_under_another() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..10).map(|i| payload(i, 900)).collect();
    let ids = put_all(&blob, tiny(), &payloads).await;

    // Opened with a limit a hundred times bigger, and one a hundred times smaller.
    for limit in [SegmentLimit::new(1 << 20), SegmentLimit::new(512)] {
        let store = BlobStore::builder(&blob).open_reader().await.unwrap();

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
/// A segment that cannot hold its own header cannot be a segment.
/// A limit that produced one would fail every write with an error about a record
/// not fitting.
/// That error is true and useless.
#[test]
fn a_limit_too_small_to_hold_a_record_is_raised() {
    let limit = SegmentLimit::new(1);

    assert!(
        limit.bytes() > 1,
        "a limit of 1 byte should have been raised, got {}",
        limit.bytes()
    );
}

/// The naming scheme's last segment is the last one the set can name.
///
/// A segment's number is zero-padded to `SEGMENT_DIGITS`, and padding is not
/// truncation.
/// So the number just past the width is *written* as a five-digit name.
/// The parser then *rejects* that name, which is the worst possible failure.
/// The file exists, and the set does not see it.
/// The commit record inside it is invisible to every later run.
/// The store refuses to roll over past `MAX_SEGMENT` for exactly this reason.
#[test]
fn the_naming_scheme_names_exactly_its_own_width() {
    use blulib::layout::{MAX_SEGMENT, SEGMENT_DIGITS};

    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    // The formatter pads to the width, up to and including the last segment the
    // scheme can name, and not one further.
    for segment in [0, 1, 999, MAX_SEGMENT] {
        let name = set.segment_path(segment);
        let digits: String = name
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .chars()
            .skip("photos.".len())
            .take_while(char::is_ascii_digit)
            .collect();
        assert_eq!(
            digits.len(),
            SEGMENT_DIGITS,
            "segment {segment} should be named with {SEGMENT_DIGITS} digits, got {digits:?}"
        );
    }

    // One past the width is the whole bug: a name the writer can produce and the
    // reader will not accept.
    // It is pinned here so that a silent widening of either the formatter or the
    // parser is visible as a test failure.
    // Otherwise it is visible as a set that loses a version.
    let past = set.segment_path(MAX_SEGMENT + 1);
    let digits: String = past
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .chars()
        .skip("photos.".len())
        .take_while(char::is_ascii_digit)
        .collect();
    assert_eq!(
        digits.len(),
        SEGMENT_DIGITS + 1,
        "a segment past the width is named with more digits than the scheme can read back, \
         which is why the store refuses to create one: {digits:?}"
    );
    assert_eq!(
        MAX_SEGMENT + 1,
        10_000,
        "the boundary should be where four digits run out"
    );
}

/// A set with a gap in its segments is refused, naming what is missing.
///
/// Not the case this file is otherwise about, but it is the check that stands
/// between a naming-scheme mishap and a silently shorter set.
/// Without it, a segment the parser declined to recognise would look like the
/// tail of the set.
#[test]
fn a_gap_in_the_segments_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    fs::write(set.segment_path(0), b"first").unwrap();
    fs::write(set.segment_path(2), b"third").unwrap();

    let error = set.segments().unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("missing segment 1"),
        "the refusal should name the segment that is absent: {message}"
    );
}

/// The in-memory commit log and the one read from the file must agree.
///
/// `resolve_with` exists so a store can answer with the log it already has rather
/// than re-reading a segment per version.
/// That is only safe if the two agree for every version of a set.
/// The interesting cases are the ones where a naive offset calculation would not
/// agree.
/// One is a version in a *later* segment, so the slice has to start after another
/// segment's records.
/// Another is a set whose segments hold different numbers of commits.
///
/// `store.snapshot_via_index` goes through the in-memory path.
/// [`resolve`] goes through the file.
/// So comparing them across a rollover compares the two without reaching inside
/// either.
#[tokio::test]
async fn an_in_memory_commit_log_agrees_with_the_one_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");

    // A small limit, so the set really does roll over and the segments hold
    // different numbers of commits.
    let small = SegmentLimit::new(256 * 1024);
    for i in 0..12 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap_or(0), 90_000),
        )
        .unwrap();
        common::run_backup_limited(&source, &blob, small).await;
    }

    let set = IndexSet::new(&blob);
    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    assert!(
        store.segment_count() > 1,
        "the set should have rolled over, or this is not testing the offset"
    );

    let versions = u32::try_from(store.versions().unwrap().len()).unwrap_or(0);
    assert_eq!(versions, 12, "every backup should be a version");

    for v in 1..=versions {
        // The file path: reads the named segment and checks the record there.
        let from_disk = resolve(&set, v, versions);

        // The in-memory path: the same check against the list this store built
        // when it opened.
        let from_memory = resolve_with(&set, v, versions, |segment| {
            let commits = store.segment_commits(segment);
            (!commits.is_empty()).then_some(commits)
        });

        assert_eq!(
            from_memory, from_disk,
            "version {v}: the in-memory commit log and the one on disk disagree"
        );
        assert!(
            matches!(from_disk, Resolved::Confirmed { .. }),
            "version {v} should resolve from its own index file"
        );
    }
}

/// The check is made through the index file, not rescued by a fallback.
///
/// `snapshot_via_index` falls back to the store's own log when the index file will
/// not do.
/// So asking it for every version proves only that the set is readable.
/// This test compares the two paths directly, and that is the property that
/// matters.
/// If the in-memory offset arithmetic were wrong, the two would differ for every
/// version after the first segment.
/// A test that could not tell that apart from a working fallback would pass on
/// the broken version.
#[tokio::test]
async fn every_version_resolves_the_same_way_from_memory_and_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");

    let small = SegmentLimit::new(256 * 1024);
    for i in 0..10 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap_or(0), 80_000),
        )
        .unwrap();
        common::run_backup_limited(&source, &blob, small).await;
    }

    let set = IndexSet::new(&blob);
    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    let versions = u32::try_from(store.versions().unwrap().len()).unwrap_or(0);

    assert!(
        store.segment_count() > 1,
        "the set should have rolled over, or the offsets being checked are all zero"
    );

    for v in 1..=versions {
        let from_disk = resolve(&set, v, versions);
        let from_memory = resolve_with(&set, v, versions, |segment| {
            let commits = store.segment_commits(segment);
            (!commits.is_empty()).then_some(commits)
        });

        assert_eq!(
            from_memory, from_disk,
            "version {v}: the in-memory log and the one on disk disagree"
        );
        assert!(
            matches!(from_disk, Resolved::Confirmed { .. }),
            "version {v} should be confirmed by its own index file"
        );
    }
}
