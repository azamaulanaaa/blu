// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The manifest: a cache an apply writes so the next one need not read the tree.
//!
//! These tests are about one property above all: **a manifest can never make an
//! apply wrong**.
//! A cache that could would be worse than no cache.
//! It would turn a missing read into a silently incorrect directory.
//! So the cases here are mostly about the ways a manifest goes stale.
//! Every one of them must come back as "untrusted" rather than as "trusted".
//!
//! The trees are real backups wherever a test is about a real apply's output.
//! Hand-built snapshots appear only where the manifest's own arithmetic is what
//! is under test.
//! A manifest records the length and time a version *imposes*.
//! A test that recorded something else would be testing a manifest that nothing
//! produces.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{fs, path::Path};

use blulib::{
    command::{MANIFEST_SUFFIX, Manifest},
    metadata::{Attributes, File, Snapshot, Timestamp},
};

/// A time a version imposes, so a manifest built from it has something to compare.
const IMPOSED: i64 = 1_600_000_000;

/// A snapshot recording `files`, each with the given length and no slices.
///
/// Built directly rather than by backing a tree up.
/// What these tests are about is the manifest's own arithmetic.
/// A manifest written from a snapshot with slices behaves identically.
/// `tests/apply.rs` covers that path end to end through a real backup.
///
/// Every file gets the same imposed modification time.
/// A manifest records what the version imposes rather than what the filesystem
/// happened to report.
/// A snapshot with no modification time yields no record at all.
/// That is the case `a_version_that_records_no_time_produces_no_record` pins.
fn snapshot(files: &[(&str, u64)]) -> Snapshot {
    let mut snapshot = Snapshot::new(Timestamp {
        secs: IMPOSED,
        nanos: 0,
    });
    for (path, len) in files {
        snapshot.files.push(File {
            path: path.into(),
            len: *len,
            attrs: Attributes {
                owner: None,
                mode: None,
                mtime: Some(Timestamp {
                    secs: IMPOSED,
                    nanos: 0,
                }),
            },
        });
    }
    snapshot
}

/// A snapshot whose files record no modification time at all.
fn timeless(files: &[(&str, u64)]) -> Snapshot {
    let mut snapshot = Snapshot::new(Timestamp {
        secs: IMPOSED,
        nanos: 0,
    });
    for (path, len) in files {
        snapshot.files.push(File {
            path: path.into(),
            len: *len,
            attrs: Attributes::default(),
        });
    }
    snapshot
}

/// A directory holding `files`, with `contents` written into each.
fn tree(dir: &Path, files: &[(&str, &[u8])]) {
    for (path, contents) in files {
        let full = dir.join(path);
        fs::create_dir_all(full.parent().unwrap()).unwrap();
        fs::write(&full, contents).unwrap();
    }
}

/// Give a file the modification time a version imposes.
/// That is what an apply leaves behind.
/// It is therefore what a manifest built from that version records.
fn impose(path: &Path) {
    filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(IMPOSED, 0)).unwrap();
}

/// A manifest records a version, and records where it puts its file.
#[test]
fn a_manifest_lives_beside_the_destination_and_never_inside_it() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("photos");
    fs::create_dir(&destination).unwrap();

    let path = Manifest::path_for(&destination).unwrap();

    assert!(
        path.parent() == Some(dir.path()),
        "a manifest belongs beside the destination, got {}",
        path.display()
    );
    assert!(
        path.to_string_lossy().ends_with(MANIFEST_SUFFIX),
        "and should be named for what it is, got {}",
        path.display()
    );
    assert!(
        !path.starts_with(&destination),
        "and must never appear in the tree it describes"
    );
}

/// A manifest round-trips, and the round trip is what makes the cache usable.
/// A manifest that decoded into something else would be silently trusted.
#[test]
fn a_manifest_survives_a_round_trip_through_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(&destination, &[("a.bin", b"aaaa"), ("nested/b.bin", b"bb")]);
    impose(&destination.join("a.bin"));
    impose(&destination.join("nested/b.bin"));

    let version = common::id_of(&[7u8; 32]);
    let recorded = Manifest::record(
        &destination,
        version,
        &snapshot(&[("a.bin", 4), ("nested/b.bin", 2)]),
    )
    .unwrap();
    recorded.save(&destination).unwrap();

    let loaded = Manifest::load(&destination)
        .unwrap()
        .expect("it should be there");

    assert_eq!(loaded.version, version, "the version must survive");
    assert!(
        loaded.trusts(
            &destination,
            &snapshot(&[("a.bin", 4), ("nested/b.bin", 2)])
        ),
        "an untouched directory should be trusted"
    );
}

/// The whole point: a file whose bytes changed is not trusted.
///
/// This is the test that would catch the failure the design exists to prevent. A
/// manifest that trusted this file would let an apply skip it and leave the user's
/// edit in place, reporting success.
#[test]
fn a_file_that_has_been_edited_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(&destination, &[("a.bin", b"original")]);
    impose(&destination.join("a.bin"));

    let version = common::id_of(&[1u8; 32]);
    let recorded = Manifest::record(&destination, version, &snapshot(&[("a.bin", 8)])).unwrap();
    recorded.save(&destination).unwrap();

    // Same length, different bytes. The length alone cannot catch this — which is
    // why the modification time is in the manifest at all.
    fs::write(destination.join("a.bin"), b"modified").unwrap();

    let loaded = Manifest::load(&destination).unwrap().unwrap();
    let snap = snapshot(&[("a.bin", 8)]);

    assert!(
        !loaded.trusts(&destination, &snap),
        "an edited file must not be trusted"
    );
    assert_eq!(
        loaded.untrusted(&destination, &snap),
        vec![Path::new("a.bin").to_path_buf()],
        "and it should be named, so the caller reads exactly that one"
    );
}

/// Only the file that moved comes back untrusted.
///
/// A per-file answer, because the saving is precisely that one untouched file
/// need not be read. Reporting "something moved" would throw the saving away.
#[test]
fn only_the_file_that_moved_is_reported_untrusted() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(
        &destination,
        &[("a.bin", b"aaaa"), ("b.bin", b"bbbb"), ("c.bin", b"cccc")],
    );
    for name in ["a.bin", "b.bin", "c.bin"] {
        impose(&destination.join(name));
    }

    let version = common::id_of(&[2u8; 32]);
    let files = [("a.bin", 4), ("b.bin", 4), ("c.bin", 4)];
    let recorded = Manifest::record(&destination, version, &snapshot(&files)).unwrap();
    recorded.save(&destination).unwrap();

    // Move one file's time off what the version imposed.
    filetime::set_file_mtime(
        destination.join("b.bin"),
        filetime::FileTime::from_unix_time(IMPOSED - 100_000, 0),
    )
    .unwrap();

    let loaded = Manifest::load(&destination).unwrap().unwrap();
    let untrusted = loaded.untrusted(&destination, &snapshot(&files));

    assert_eq!(
        untrusted,
        vec![Path::new("b.bin").to_path_buf()],
        "only the file whose time moved should be reported"
    );
}

/// A file of a different length is untrusted even if the time somehow matches.
#[test]
fn a_file_of_a_different_length_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(&destination, &[("a.bin", b"aaaa")]);
    impose(&destination.join("a.bin"));

    let version = common::id_of(&[3u8; 32]);
    let recorded = Manifest::record(&destination, version, &snapshot(&[("a.bin", 4)])).unwrap();
    recorded.save(&destination).unwrap();

    // Grow the file, then put the time back to what the manifest recorded, so that
    // only the length can catch it. This is the defensive half of the same case: a
    // tool that had copied a file and reset its timestamp would fool a
    // time-only check.
    let path = destination.join("a.bin");
    let before = fs::metadata(&path).unwrap();
    fs::write(&path, b"aabbcc").unwrap();
    filetime::set_file_mtime(
        &path,
        filetime::FileTime::from_last_modification_time(&before),
    )
    .unwrap();

    let loaded = Manifest::load(&destination).unwrap().unwrap();
    assert!(
        !loaded.trusts(&destination, &snapshot(&[("a.bin", 4)])),
        "a file that changed length must not be trusted"
    );
}

/// A file the manifest never recorded is untrusted, because nothing vouches for it.
#[test]
fn a_file_the_manifest_never_saw_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(&destination, &[("a.bin", b"aaaa")]);
    impose(&destination.join("a.bin"));

    let version = common::id_of(&[4u8; 32]);
    let recorded = Manifest::record(&destination, version, &snapshot(&[("a.bin", 4)])).unwrap();
    recorded.save(&destination).unwrap();

    // A file the version has since added.
    // The manifest cannot vouch for it, and `a.bin` is still trusted.
    // So the answer is exactly the new file.
    // That is what makes the per-file fallback worth having.
    let snap = snapshot(&[("a.bin", 4), ("fresh.bin", 4)]);

    let loaded = Manifest::load(&destination).unwrap().unwrap();
    assert_eq!(
        loaded.untrusted(&destination, &snap),
        vec![Path::new("fresh.bin").to_path_buf()],
        "a file no manifest recorded is untrusted"
    );
}

/// A version that records no modification time yields no manifest record at all.
///
/// There is nothing to compare a file against, and recording a zero time would be
/// a claim.
/// That claim is false on any filesystem with a real epoch.
/// So the file is read next time, which is slower and correct.
#[test]
fn a_version_that_records_no_time_produces_no_record() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(&destination, &[("a.bin", b"aaaa")]);

    let version = common::id_of(&[11u8; 32]);
    let recorded = Manifest::record(&destination, version, &timeless(&[("a.bin", 4)])).unwrap();

    assert!(
        !recorded.trusts(&destination, &timeless(&[("a.bin", 4)])),
        "a file with no recorded time cannot be trusted, because there is nothing \
         to compare it against"
    );
}

/// No manifest is a cache miss, not an error.
///
/// A caller that cannot use the cache reads the tree instead, and there is nothing
/// for it to report.
/// So `load` returns `None` rather than failing.
/// Otherwise a first apply could never run.
#[test]
fn a_destination_with_no_manifest_is_a_miss_and_not_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    fs::create_dir_all(&destination).unwrap();

    assert!(
        Manifest::load(&destination).unwrap().is_none(),
        "nothing recorded means nothing to load"
    );

    // And a destination that does not exist yet is the ordinary first-apply case.
    assert!(
        Manifest::load(&dir.path().join("absent"))
            .unwrap()
            .is_none(),
        "so is a destination that has never been applied to"
    );
}

/// Bytes that are not a manifest are a miss, not a failure.
#[test]
fn a_manifest_that_is_not_one_is_a_miss() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    fs::create_dir_all(&destination).unwrap();

    let path = Manifest::path_for(&destination).unwrap();

    // Every way a manifest can be unusable, none of which should fail a caller.
    for bytes in [
        b"".to_vec(),
        b"garbage".to_vec(),
        vec![0u8; 64],
        // Right magic, truncated body.
        {
            let mut v = b"LPMANIF1".to_vec();
            v.extend_from_slice(&[0u8; 10]);
            v
        },
    ] {
        fs::write(&path, &bytes).unwrap();
        assert!(
            Manifest::load(&destination).unwrap().is_none(),
            "{} bytes should be a cache miss, not a manifest",
            bytes.len()
        );
    }
}

/// Saving is a rename, so a crash cannot leave a manifest that names some files.
#[test]
fn saving_leaves_no_partial_file_behind() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    fs::create_dir_all(&destination).unwrap();

    let version = common::id_of(&[5u8; 32]);
    let manifest = Manifest::record(
        &destination,
        version,
        &Snapshot::new(Timestamp {
            secs: IMPOSED,
            nanos: 0,
        }),
    )
    .unwrap();
    let saved = manifest.save(&destination).unwrap();

    assert!(saved.exists(), "the manifest should be in place");
    assert!(
        !saved.with_extension("partial").exists(),
        "and the temporary it was renamed from should not be"
    );
}

/// Removing a manifest that is not there is fine.
#[test]
fn removing_an_absent_manifest_does_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    fs::create_dir_all(&destination).unwrap();

    Manifest::remove(&destination).unwrap();

    let version = common::id_of(&[6u8; 32]);
    let manifest = Manifest::record(
        &destination,
        version,
        &Snapshot::new(Timestamp {
            secs: IMPOSED,
            nanos: 0,
        }),
    )
    .unwrap();
    manifest.save(&destination).unwrap();
    assert!(Manifest::load(&destination).unwrap().is_some());

    Manifest::remove(&destination).unwrap();
    assert!(
        Manifest::load(&destination).unwrap().is_none(),
        "and one that is there should go"
    );
}

/// A path with non-ASCII bytes in it round-trips.
#[test]
fn a_manifest_still_trusts_a_path_that_is_not_ascii() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    let name = "café–π.bin";
    tree(&destination, &[(name, b"data")]);
    impose(&destination.join(name));

    let version = common::id_of(&[8u8; 32]);
    let recorded = Manifest::record(&destination, version, &snapshot(&[(name, 4)])).unwrap();
    recorded.save(&destination).unwrap();

    let loaded = Manifest::load(&destination).unwrap().unwrap();
    assert!(
        loaded.trusts(&destination, &snapshot(&[(name, 4)])),
        "a path outside ASCII must survive the round trip"
    );
}

/// A path with a newline in it round-trips, which a length-prefixed format allows
/// and a line-oriented one would not.
#[test]
fn a_manifest_still_trusts_a_path_with_a_newline_in_it() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    let name = "two\nlines.bin";
    tree(&destination, &[(name, b"data")]);
    impose(&destination.join(name));

    let version = common::id_of(&[9u8; 32]);
    let recorded = Manifest::record(&destination, version, &snapshot(&[(name, 4)])).unwrap();
    recorded.save(&destination).unwrap();

    let loaded = Manifest::load(&destination).unwrap().unwrap();
    assert!(
        loaded.trusts(&destination, &snapshot(&[(name, 4)])),
        "a newline in a path must not split the entry"
    );
}

/// The same tree always produces the same manifest bytes.
///
/// Entries are sorted on the way out, so two manifests of one tree can be
/// compared rather than merely diffed by eye.
/// A caller caching by content then gets a hit.
#[test]
fn a_manifest_is_a_function_of_the_tree_it_records() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("tree");
    tree(&destination, &[("b.bin", b"bb"), ("a.bin", b"aaaa")]);
    impose(&destination.join("a.bin"));
    impose(&destination.join("b.bin"));

    let version = common::id_of(&[10u8; 32]);
    let files = [("a.bin", 4), ("b.bin", 2)];

    let one = Manifest::record(&destination, version, &snapshot(&files))
        .unwrap()
        .save(&destination)
        .unwrap();
    let first = fs::read(&one).unwrap();

    let two = Manifest::record(&destination, version, &snapshot(&files))
        .unwrap()
        .save(&destination)
        .unwrap();

    assert_eq!(first, fs::read(&two).unwrap());
}
