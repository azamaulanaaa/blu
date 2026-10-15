// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Resolving a version through its index file, and the three ways that can fail.
//!
//! [`resolve`] is the fast path, and everything about it is a question of what it
//! returns rather than what it does.
//! The store's commit records are the record of what versions exist.
//! This module only decides how much work it takes to locate the answer.
//! So a wrong answer here is never corruption.
//! It is a wasted scan, or a refusal to use a file that would have misled.
//!
//! The refusal cases are covered in `version_index.rs`, which is where the danger
//! of a believed file is argued.
//! What is here is what that file leaves untested:
//!
//! * [`Resolved::confirmed`], which is what a caller actually *acts* on.
//!   It is the difference between reading the answer and rescanning for it.
//!   A wrong `Some` hands back a snapshot the fast path never checked.
//! * an index naming a segment that is present but unreadable as a segment.
//!   That is a different failure from one naming a segment that is absent.
//! * the ordinal, which is a claim about *position*.
//!   The ordinal is checked separately from the snapshot hash, so it needs
//!   testing separately too.
//! * [`Resolved::reason`], which is the only part of this a person reads.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::fs;

use blulib::{
    layout::IndexSet,
    metadata::Timestamp,
    storage::{BlobStore, Resolved, SegmentLimit, VersionIndex, resolve},
};

/// A set with `versions` versions in it.
async fn set_with_versions(dir: &std::path::Path, versions: usize) -> std::path::PathBuf {
    let source = common::source(dir);
    let blob = dir.join("store");

    for i in 0..versions {
        fs::write(source.join("a.bin"), common::noisy(40_000 + i * 9_000)).unwrap();
        common::run_backup(&source, &blob).await;
    }

    blob
}

/// The index file a set wrote for `version`.
fn index_of(blob: &std::path::Path, version: u32) -> VersionIndex {
    let set = IndexSet::new(blob);
    VersionIndex::read(&set.index_path(version))
        .unwrap_or_else(|| panic!("version {version} should have an index file"))
}

/// `confirmed` is the answer a caller acts on, so it must never disagree with
/// `resolve` about which case it is in.
///
/// A `Some` where there should be a `None` is the dangerous direction and the
/// whole reason this is a test.
/// The caller would read the snapshot this returned without the store ever being
/// asked.
/// The other direction only costs a scan.
#[tokio::test]
async fn confirmed_agrees_with_what_resolve_found() {
    let dir = tempfile::tempdir().unwrap();
    let blob = set_with_versions(dir.path(), 3).await;
    let set = IndexSet::new(&blob);

    let confirmed = resolve(&set, 2, 3);
    assert_eq!(
        confirmed,
        Resolved::Confirmed {
            segment: index_of(&blob, 2).segment,
            snapshot: index_of(&blob, 2).snapshot,
        }
    );
    assert_eq!(
        confirmed.confirmed(),
        Some((index_of(&blob, 2).segment, index_of(&blob, 2).snapshot)),
        "a confirmed index must hand back the snapshot and segment it named"
    );

    // Absent, and Disagrees: both report no usable index, so both must be None.
    assert!(resolve(&set, 2, 3).confirmed().is_some());
    assert_eq!(Resolved::Absent.confirmed(), None);
    assert_eq!(Resolved::Disagrees.confirmed(), None);

    // The absent case reached for real rather than by constructing the variant.
    fs::remove_file(set.index_path(2)).unwrap();
    let absent = resolve(&set, 2, 3);
    assert_eq!(absent, Resolved::Absent);
    assert_eq!(
        absent.confirmed(),
        None,
        "a missing index must not confirm anything"
    );

    // And the disagrees case, for real.
    let mut wrong = index_of(&blob, 1);
    wrong.ordinal = 2;
    wrong.write_atomic(&set.index_path(1)).unwrap();
    let disagrees = resolve(&set, 1, 3);
    assert_eq!(disagrees, Resolved::Disagrees);
    assert_eq!(
        disagrees.confirmed(),
        None,
        "a refused index must not confirm anything"
    );
}

/// The ordinal is a claim about position, and it is checked apart from the hash.
///
/// An index can name the right segment and the right snapshot and still point at
/// the wrong *place*.
/// The position within the segment is recorded rather than derived.
/// If only the hash were checked, that index would confirm and hand back a
/// version number that addresses a different commit record.
#[tokio::test]
async fn an_index_naming_the_wrong_ordinal_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let blob = set_with_versions(dir.path(), 3).await;
    let set = IndexSet::new(&blob);

    // Same segment, same snapshot, wrong position.
    // That is the one combination a hash-only check would wave through.
    let real = index_of(&blob, 2);
    let mut moved = real;
    moved.ordinal = 0;
    moved.write_atomic(&set.index_path(2)).unwrap();

    assert_eq!(
        resolve(&set, 2, 3),
        Resolved::Disagrees,
        "an index pointing at the wrong commit record must be refused"
    );
}

/// A segment that is a valid store for a *different* segment is refused.
///
/// This is the case the store header carries, and it is the one the rest of
/// `resolve` cannot catch.
/// A scan of a file returns whatever records it holds, tagged with the segment
/// number it was *asked* about.
/// So asking about segment 1 and getting segment 0's records back produces a
/// plausible answer.
/// The snapshot really is at that ordinal, and the file really is named for that
/// segment.
/// The two facts are simply about different stores.
/// Only the header in the file says which.
#[tokio::test]
async fn a_segment_filed_under_the_wrong_number_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");

    // A limit small enough that a handful of versions roll over into several
    // segments, so there is a segment 1 to be wrong about.
    let limit = SegmentLimit::new(64 * 1024);
    for i in 0..4 {
        fs::write(source.join("a.bin"), common::noisy(40_000 + i * 9_000)).unwrap();
        common::run_backup_limited(&source, &blob, limit).await;
    }

    let set = IndexSet::new(&blob);
    assert!(
        set.segments().unwrap().len() > 1,
        "the fixture needs more than one segment, or there is nothing to be wrong about"
    );

    let last = u32::try_from(set.segments().unwrap().len()).unwrap() - 1;
    assert!(last > 0, "the fixture needs a segment 0 to be wrong about");

    // Version 1's snapshot, at ordinal 0 of segment 0.
    // An index naming exactly that, but pointing at segment `last`, is the only
    // shape that separates the header check from the rest.
    // The file it names will hold that snapshot at that ordinal.
    // So the hash and the position both agree.
    // Only the segment number in the file's own header says the file is not the
    // one meant.
    let first = index_of(&blob, 1);
    assert_eq!(first.segment, 0, "version 1 should be in the first segment");
    assert_eq!(
        first.ordinal, 0,
        "version 1 should be its segment's first commit"
    );

    // Put segment 0's file where segment `last` should be.
    // That is a real store, with real commit records, that is not the one the
    // index claims.
    fs::copy(set.segment_path(0), set.segment_path(last)).unwrap();

    let misfiled = VersionIndex {
        version: 1,
        segment: last,
        ordinal: 0,
        snapshot: first.snapshot,
        taken_at: Timestamp { secs: 1, nanos: 0 },
    };
    misfiled.write_atomic(&set.index_path(1)).unwrap();

    assert_eq!(
        resolve(&set, 1, 4),
        Resolved::Disagrees,
        "a segment holding another segment's records must not be read as this one, \
         even when the snapshot and ordinal it holds do match"
    );

    // The store enforces the same rule on the way in, and names both numbers.
    // Two layers refusing the same file is the point.
    // The index check is not the only thing standing between a misfiled segment
    // and a wrong snapshot.
    let err = BlobStore::builder(&blob)
        .open_reader()
        .await
        .expect_err("a set with a misfiled segment must not open");
    let why = err.to_string();
    assert!(
        why.contains("segment 0") && why.contains(&format!("segment {last}")),
        "the refusal should name both segments: {why}"
    );
}

/// A file that is present but is not a store at all is refused.
///
/// The other way the read can fail, and a different one from a missing segment.
/// `is_file` says yes and the parse says no.
/// The scan stops at the first record that is not valid.
/// So a file of nonsense yields no commits, and the ordinal lookup finds
/// nothing.
#[tokio::test]
async fn a_segment_that_is_present_but_unreadable_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let blob = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    let segment = index_of(&blob, 1).segment;
    let path = set.segment_path(segment);

    // The right name, the wrong contents: not a store header at all.
    fs::write(&path, b"this is not a segment, it is a sentence about one").unwrap();

    assert!(
        path.is_file(),
        "the fixture must still be a file, or this tests the missing-segment path"
    );
    assert_eq!(
        resolve(&set, 1, 2),
        Resolved::Disagrees,
        "a file that is not a segment must not be read as one"
    );
}

/// The reason is the only part of this a person reads, and it has to say which of
/// the two failures it was.
///
/// "No index file" and "the index does not describe this set" call for opposite
/// responses.
/// The first is a set that has been running, and the second is a file worth
/// deleting.
/// So collapsing them into one message would send someone to look in the wrong
/// place.
#[tokio::test]
async fn the_reason_distinguishes_the_two_failures() {
    let dir = tempfile::tempdir().unwrap();
    let blob = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    // A confirmed index needs no explanation, and inventing one would be noise on
    // the happy path.
    let confirmed = resolve(&set, 1, 2);
    assert!(
        confirmed.confirmed().is_some(),
        "the fixture should resolve, or this tests nothing"
    );
    assert_eq!(
        confirmed.reason(1, &set),
        "",
        "a confirmed index should have nothing to explain"
    );

    // Absent: names the version, and says the file is simply not there.
    fs::remove_file(set.index_path(1)).unwrap();
    let absent = resolve(&set, 1, 2);
    let why = absent.reason(1, &set);
    assert_eq!(absent, Resolved::Absent);
    assert!(
        why.contains("version 1") && why.contains("no index file"),
        "an absent index should say which version has no file: {why}"
    );

    // Disagrees: says the file exists and was not used, which is the different
    // instruction.
    let mut wrong = index_of(&blob, 2);
    wrong.ordinal = 0;
    wrong.write_atomic(&set.index_path(2)).unwrap();
    let disagrees = resolve(&set, 2, 2);
    let why = disagrees.reason(2, &set);
    assert_eq!(disagrees, Resolved::Disagrees);
    assert!(
        why.contains("version 2") && why.contains("does not describe this set"),
        "a refused index should say the file was not used: {why}"
    );
    assert_ne!(
        absent.reason(1, &set),
        disagrees.reason(2, &set),
        "the two failures must not read the same to a person"
    );
}

/// A store still answers correctly when every index file has been destroyed.
///
/// `resolve` refusing is only half the claim.
/// The other half is that refusing costs a scan and nothing more.
/// The commit records are the record.
/// So a caller that falls back gets the same answer a caller with no index files
/// at all would have got.
/// The two must not be different code paths with different answers.
#[tokio::test]
async fn a_destroyed_index_costs_a_scan_and_no_answer() {
    let dir = tempfile::tempdir().unwrap();
    let blob = set_with_versions(dir.path(), 3).await;
    let set = IndexSet::new(&blob);

    let expected: Vec<_> = (1..=3u32).map(|v| index_of(&blob, v).snapshot).collect();

    for version in 1..=3u32 {
        fs::remove_file(set.index_path(version)).unwrap();
    }

    for (i, version) in (1..=3u32).enumerate() {
        assert_eq!(resolve(&set, version, 3), Resolved::Absent);

        let store = BlobStore::builder(&blob).open_reader().await.unwrap();
        assert_eq!(
            store.snapshot(version).unwrap(),
            expected[i],
            "the store must still answer for version {version} with every index gone"
        );
    }
}
