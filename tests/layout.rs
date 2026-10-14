// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The on-disk layout of a backup set: path naming, segment numbering, and the
//! write lock.
//!
//! Version numbering used to live here too, as a scan for `NNN.idx` files. It
//! does not any more: the store records its own versions in its commit records,
//! and a version number is a position in that log.
//! What is left of this module is the set's own business.
//! The module covers where the files go, how they are numbered, and how two
//! writers are kept apart.

use blulib::layout::{IndexSet, SEGMENT_DIGITS, SEGMENT_EXTENSION};

#[test]
fn derived_paths_do_not_clobber_an_extension_in_the_base_name() {
    // `Path::with_extension` turns this into `photos.bin` and drops `.tar`.
    let set = IndexSet::new("photos.tar");

    assert_eq!(
        set.segment_path(0),
        std::path::Path::new("photos.tar.0000.bin")
    );
    assert_eq!(set.lock_path(), std::path::Path::new("photos.tar.lock"));
}

#[test]
fn a_backup_set_is_a_sequence_of_segments_and_a_lock() {
    // The whole point of the commit record: publishing a version is a single
    // append to a single file.
    // So there is no second file whose contents have to be kept in step with the
    // first.
    // Segments are the *same* file's log split for size, not a second source of
    // truth.
    // That is why there is no index file, and why there never will be one again.
    let set = IndexSet::new("photos");

    assert_eq!(set.segment_path(0), std::path::Path::new("photos.0000.bin"));
    assert_eq!(set.lock_path(), std::path::Path::new("photos.lock"));
    assert_eq!(set.base(), std::path::Path::new("photos"));
}

/// The property the whole segment scheme rests on.
///
/// Segment numbers are zero-padded to a fixed width.
/// A directory listing sorts lexically, and the padding brings it back in
/// numeric order.
/// Without the padding, segment 10 would sort before segment 2.
/// Every scan that trusted the listing would read the records in the wrong order.
/// The scan would then report a store that holds the right bytes in the wrong
/// sequence.
#[test]
fn segment_numbers_sort_into_numerical_order() {
    let set = IndexSet::new("photos");

    let mut listing: Vec<_> = (0..12).map(|n| set.segment_path(n)).collect();
    listing.sort();

    let expected: Vec<_> = (0..12).map(|n| set.segment_path(n)).collect();
    assert_eq!(
        listing, expected,
        "a directory listing of {SEGMENT_DIGITS}-digit segments should already be \
         in order, so 0002 sorts before 0010"
    );

    // 12 segments is past the point where 3 digits would still sort right.
    // A narrower field gets that case wrong.
    assert!(
        set.segment_path(2) < set.segment_path(10),
        "segment 10 must sort after segment 2"
    );
    assert_eq!(SEGMENT_DIGITS, 4, "the width is part of the on-disk layout");
    assert_eq!(SEGMENT_EXTENSION, "bin");
}

#[test]
fn derived_paths_honour_a_directory_component() {
    let set = IndexSet::new("backups/photos");

    assert_eq!(
        set.segment_path(3),
        std::path::Path::new("backups/photos.0003.bin")
    );
    assert_eq!(
        set.index_path(3),
        std::path::Path::new("backups/photos.v0003.idx")
    );
    assert_eq!(set.lock_path(), std::path::Path::new("backups/photos.lock"));
    assert_eq!(set.directory(), std::path::Path::new("backups"));
}

/// An index file is named with a `v`, so a glob cannot confuse the two.
///
/// Segments are `base.NNNN.bin`.
/// Index files would otherwise be `base.NNNN.idx`.
/// Those are different things with the same digits in the same directory.
/// A script globbing one pattern, or a person clearing out old index files,
/// would hit both.
/// Deleting a segment is not something `reindex` can undo.
#[test]
fn an_index_file_is_never_confusable_with_a_segment() {
    let set = IndexSet::new("photos");

    for version in [1u32, 12, 123] {
        assert_eq!(
            set.index_path(version),
            std::path::Path::new(&format!("photos.v{version:04}.idx"))
        );
        assert_eq!(
            set.segment_path(version),
            std::path::Path::new(&format!("photos.{version:04}.bin"))
        );
    }
}

/// Segments and index files are told apart by name, in both directions.
///
/// Including the unprefixed spelling, which is exactly the trap the `v` avoids.
/// A file that looks like an index and is not one is ignored rather than
/// adopted.
#[test]
fn segments_and_index_files_are_told_apart_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    std::fs::write(set.segment_path(0), b"x").unwrap();
    std::fs::write(set.segment_path(1), b"x").unwrap();
    std::fs::write(set.index_path(1), b"x").unwrap();
    std::fs::write(set.index_path(2), b"x").unwrap();
    std::fs::write(dir.path().join("photos.0003.idx"), b"x").unwrap();

    assert_eq!(
        set.segments().unwrap(),
        vec![set.segment_path(0), set.segment_path(1)],
        "an index file must not be listed as a segment"
    );
    assert_eq!(
        set.indexed_versions().unwrap(),
        vec![1, 2],
        "a segment must not be listed as an index, and an unprefixed name is not ours"
    );
}

#[test]
fn a_bare_name_resolves_against_the_current_directory() {
    let set = IndexSet::new("photos");

    assert_eq!(set.directory(), std::path::Path::new("."));
}

#[test]
fn an_empty_set_lists_no_segments() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    assert_eq!(set.segments().unwrap(), Vec::<std::path::PathBuf>::new());
}

#[test]
fn segments_are_listed_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    // Written out of order, because a directory does not promise otherwise.
    for segment in [2u32, 0, 1] {
        std::fs::write(set.segment_path(segment), b"x").unwrap();
    }

    assert_eq!(
        set.segments().unwrap(),
        vec![
            set.segment_path(0),
            set.segment_path(1),
            set.segment_path(2)
        ]
    );
}

/// A gap in the numbering is refused, not tolerated.
///
/// The segments are the only place the data is.
/// A set missing one in the middle read as though its chunks had never been
/// stored.
/// That is a backup that quietly lost data rather than one that reported a
/// problem.
#[test]
fn a_missing_segment_in_the_middle_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    for segment in [0u32, 2] {
        std::fs::write(set.segment_path(segment), b"x").unwrap();
    }

    let error = set.segments().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("missing segment 1"),
        "the error should say what is wrong, got: {error}"
    );
}

#[test]
fn files_belonging_to_other_sets_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    std::fs::write(set.segment_path(0), b"x").unwrap();
    std::fs::write(set.segment_path(1), b"x").unwrap();

    // Another set, a different extension, and the lock.
    std::fs::write(dir.path().join("videos.0000.bin"), b"other").unwrap();
    std::fs::write(dir.path().join("photos.0000.txt"), b"other").unwrap();
    std::fs::write(dir.path().join("photos.lock"), b"other").unwrap();

    assert_eq!(
        set.segments().unwrap(),
        vec![set.segment_path(0), set.segment_path(1)],
        "a file that looks like another set's segment is not this set's"
    );
}

#[test]
fn a_lock_excludes_a_second_holder_and_is_released_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    {
        let _lock = set.lock().unwrap();
        assert!(set.lock_path().exists());

        let second = set.lock().unwrap_err();
        assert!(
            second.to_string().contains("remove it"),
            "the error should say how to recover, got: {second}"
        );
    }

    assert!(!set.lock_path().exists());
    assert!(set.lock().is_ok());
}

#[test]
fn taking_the_lock_creates_the_containing_directory() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("nested/deeper/photos"));

    assert!(!set.directory().exists());

    let _lock = set.lock().unwrap();

    assert!(set.directory().is_dir());
    assert!(set.lock_path().exists());
}

/// A set whose base path is not valid UTF-8 is a set.
///
/// The base path is the user's, and nothing about it has to be UTF-8.
/// A name restored from an archive is a perfectly ordinary thing to back up to.
/// A name typed on a system whose encoding is not UTF-8 is one too.
/// Every file of the set is named after it, and those names are legal files.
///
/// This used to work exactly once.
/// `suffixed` builds the file name from the `OsStr` and so kept the bytes.
/// The directory scan that finds them decoded the name and gave up on anything
/// that was not UTF-8.
/// Both ends had just agreed on a name, and then one of them forgot it:
///
/// ```text
/// $ blu backup src $'sto\xffre'
///   backup complete  segments=1
/// $ blu versions $'sto\xffre'
///   error: no backup set at sto?re (looked in .)
/// $ blu backup src $'sto\xffre'
///   error: File exists (os error 17)
/// ```
///
/// So the first backup reported success.
/// The store could not be read, listed, verified or restored.
/// A second backup failed against the segment that the first one had written.
/// `create_new` in `create_segment` is the only reason that last failure is an
/// error and not a truncated store.
/// The set was wedged rather than destroyed, and the data was intact throughout.
///
/// A person recovers by renaming the files, which works because nothing about
/// the bytes was ever wrong.
#[test]
#[cfg(unix)]
fn a_set_at_a_non_utf8_base_path_is_readable() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt as _;

    let dir = tempfile::tempdir().unwrap();

    // A file name with a byte that cannot appear in UTF-8.
    let base: OsString = OsString::from_vec(b"sto\xffre".to_vec());
    let store = dir.path().join(base);
    let set = IndexSet::new(&store);

    // The names are built from the real bytes, which is half the bug.
    // The names were always right on disk, and only unreadable in memory.
    let segment = set.segment_path(0);
    assert_eq!(
        segment.file_name().unwrap().as_encoded_bytes(),
        b"sto\xffre.0000.bin",
        "the segment should be named after the base path's own bytes"
    );

    let index = set.index_path(1);
    assert_eq!(
        index.file_name().unwrap().as_encoded_bytes(),
        b"sto\xffre.v0001.idx"
    );

    // Write two segments and one index file, the way a set looks on disk.
    std::fs::write(&segment, b"one").unwrap();
    std::fs::write(set.segment_path(1), b"two").unwrap();
    std::fs::write(&index, b"idx").unwrap();

    assert_eq!(
        set.segments().unwrap(),
        vec![segment.clone(), set.segment_path(1)],
        "both segments should be found despite the base path not being UTF-8"
    );
    assert_eq!(set.indexed_versions().unwrap(), vec![1]);
}
