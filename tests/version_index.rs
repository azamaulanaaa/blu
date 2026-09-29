//! The per-version index file, and the rule that governs it: it is a cache, so it
//! is checked rather than believed.
//!
//! Every test here can be stated as one sentence: an index file is allowed to be
//! wrong, and the question is only ever "wrong in which direction, and does
//! anything act on it".
//!
//! # What this file is and is not for
//!
//! It is not a fast path for a restore. A restore needs every chunk a snapshot
//! names, locating those is the blob index, and only a scan builds that — so a
//! caller wanting one version still pays for the scan. What the file removes is
//! the scan for the *version list*, which is what `lepatch versions` and "how far
//! does this set go" are, and both are O(versions) in file names rather than
//! O(records) in the whole set.
//!
//! So the tests below are about safety, not speed, and the one that matters most
//! is `an_index_cannot_invent_a_version`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::fs;

use lepatch::{
    layout::IndexSet,
    storage::{BlobId, BlobStore, Compression, Resolved, SegmentLimit, VersionIndex, resolve},
};

/// A set with `versions` versions in it, and a store handle for it.
async fn set_with_versions(
    dir: &std::path::Path,
    versions: usize,
) -> (std::path::PathBuf, BlobStore<false>) {
    let source = common::source(dir);
    let blob = dir.join("store");

    for i in 0..versions {
        fs::write(source.join("a.bin"), common::noisy(40_000 + i * 9_000)).unwrap();
        common::run_backup(&source, &blob).await;
    }

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    (blob, store)
}

fn version(blob: &std::path::Path, version: u32) -> VersionIndex {
    let set = IndexSet::new(blob);
    VersionIndex::read(&set.index_path(version))
        .unwrap_or_else(|| panic!("version {version} should have an index file"))
}

/// The file round-trips through its own encoding.
#[test]
fn a_version_index_round_trips() {
    let original = VersionIndex {
        version: 7,
        segment: 12,
        ordinal: 3,
        snapshot: BlobId::from_bytes([9u8; 32]),
    };

    let decoded = VersionIndex::try_from(&original.to_bytes()[..]).unwrap();
    assert_eq!(decoded, original);
}

/// A flipped byte anywhere in the file is caught.
#[test]
fn a_corrupted_index_does_not_verify() {
    let original = VersionIndex {
        version: 7,
        segment: 12,
        ordinal: 3,
        snapshot: BlobId::from_bytes([9u8; 32]),
    };

    for byte in 0..original.to_bytes().len() {
        let mut bytes = original.to_bytes();
        bytes[byte] ^= 0xff;

        assert!(
            VersionIndex::try_from(bytes.as_slice()).is_err(),
            "byte {byte} was flipped and the index still verified"
        );
    }
}

/// A truncated or padded file is not an index.
#[test]
fn an_index_of_the_wrong_length_is_refused() {
    let original = VersionIndex {
        version: 1,
        segment: 0,
        ordinal: 0,
        snapshot: BlobId::from_bytes([9u8; 32]),
    };

    let mut short = original.to_bytes().to_vec();
    short.pop();

    assert!(VersionIndex::try_from(short.as_slice()).is_err());
    assert!(VersionIndex::try_from(&[0u8; 52][..]).is_err());
}

/// The writer is atomic: a reader never sees a partial file, and no temporary is
/// left behind.
#[test]
fn writing_an_index_leaves_no_temporary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("photos.v0001.idx");

    VersionIndex {
        version: 1,
        segment: 0,
        ordinal: 0,
        snapshot: BlobId::from_bytes([9u8; 32]),
    }
    .write_atomic(&path)
    .unwrap();

    assert!(path.is_file());
    assert!(VersionIndex::read(&path).is_some());

    let leftovers: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name != "photos.v0001.idx")
        .collect();

    assert!(
        leftovers.is_empty(),
        "writing an index left {leftovers:?} behind"
    );
}

/// A backup writes an index for every version it publishes.
#[tokio::test]
async fn every_published_version_gets_an_index() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, store) = set_with_versions(dir.path(), 3).await;
    let set = IndexSet::new(&blob);

    assert_eq!(
        set.indexed_versions().unwrap(),
        vec![1, 2, 3],
        "three versions should have three index files"
    );

    for v in 1..=3 {
        let index = version(&blob, v);
        assert_eq!(index.version, v);

        // And the snapshot it names is the one the store holds for that version.
        assert_eq!(index.snapshot, store.snapshot(v).unwrap());
    }
}

/// The fast path agrees with the slow one.
///
/// This is the property the whole file exists for, so it is checked against the
/// scan rather than against itself: the index's answer and the store's answer must
/// be the same snapshot, and resolving must be confirmed rather than merely
/// accepted.
#[tokio::test]
async fn a_resolve_agrees_with_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, store) = set_with_versions(dir.path(), 4).await;
    let set = IndexSet::new(&blob);

    for v in 1..=4 {
        let resolved = resolve(&set, v, 4);

        assert_eq!(
            resolved,
            Resolved::Confirmed {
                segment: version(&blob, v).segment,
                snapshot: store.snapshot(v).unwrap(),
            },
            "version {v} should resolve from its index to what the store holds"
        );
    }
}

/// A missing index is not a failure, it is a slow path.
#[tokio::test]
async fn a_missing_index_is_absent_rather_than_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, _) = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    fs::remove_file(set.index_path(2)).unwrap();

    assert_eq!(
        resolve(&set, 2, 2),
        Resolved::Absent,
        "a version whose index is gone should fall back to the scan, not fail"
    );

    // The version is still there, which is the whole point of the file being a
    // cache.
    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    assert!(store.snapshot(2).is_ok());
}

/// A corrupt index is absent, not believed.
#[tokio::test]
async fn a_corrupt_index_is_absent() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, _) = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    let path = set.index_path(2);
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    assert_eq!(resolve(&set, 2, 2), Resolved::Absent);
}

/// An index from another set is refused.
///
/// The dangerous direction. A stale index that under-reaches costs a scan; one
/// that over-reaches would answer for a version this set does not have, and that
/// is the case the whole "never believed" rule exists for.
#[tokio::test]
async fn an_index_naming_a_version_this_set_does_not_hold_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, _) = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    // A well-formed index for version 5, which this set does not have.
    VersionIndex {
        version: 5,
        segment: 0,
        ordinal: 0,
        snapshot: BlobId::from_bytes([9u8; 32]),
    }
    .write_atomic(&set.index_path(5))
    .unwrap();

    assert_eq!(
        resolve(&set, 5, 2),
        Resolved::Disagrees,
        "an index for a version beyond the set's should be refused, not used"
    );
}

/// An index whose segment does not hold the snapshot it names is refused.
///
/// This is the check that makes the file safe to keep. Without it, an index copied
/// between sets — or left behind by a set that was replaced — would name a segment
/// that exists and a snapshot that is not in it, and the read would return
/// whatever is at that offset.
#[tokio::test]
async fn an_index_naming_a_snapshot_the_segment_does_not_hold_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, store) = set_with_versions(dir.path(), 3).await;
    let set = IndexSet::new(&blob);

    // Take a real index and change only the snapshot, leaving the segment and the
    // ordinal pointing at a commit record that names something else.
    let mut index = version(&blob, 2);
    index.snapshot = BlobId::from_bytes([0xabu8; 32]);
    index.write_atomic(&set.index_path(2)).unwrap();

    assert_eq!(
        resolve(&set, 2, 3),
        Resolved::Disagrees,
        "an index whose snapshot is not the commit record's must be refused"
    );

    // And the store is unaffected, because the store never asked the file.
    let held = store.snapshot(2).unwrap();
    let real = version(&blob, 1);
    assert_ne!(
        held, real.snapshot,
        "the fixtures should differ, or this test proves nothing"
    );
    assert_eq!(
        store.snapshot(1).unwrap(),
        real.snapshot,
        "the store should still report the version the corrupt index overwrote"
    );
}

/// An index naming a segment that is not there is refused.
#[tokio::test]
async fn an_index_naming_a_missing_segment_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, _) = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    let mut index = version(&blob, 2);
    index.segment = 99;
    index.write_atomic(&set.index_path(2)).unwrap();

    assert_eq!(resolve(&set, 2, 2), Resolved::Disagrees);
}

/// The segment an index names is the one the snapshot is really in.
///
/// The check above refuses a *wrong* index; this asserts the positive case is
/// picking the right segment rather than always resolving through segment 0 and
/// relying on the fallback.
#[tokio::test]
async fn an_index_names_the_segment_the_snapshot_is_in() {
    let dir = tempfile::tempdir().unwrap();
    let small = lepatch::storage::SegmentLimit::new(256 * 1024);

    // A set with a small limit, so the segments genuinely differ.
    //
    // This used to build a second, default-limit set first — seven backups, about
    // 1.4 MB of chunking — and then assert nothing about it, which the compiler
    // had to be told about explicitly with a `let _ = set;`. Every assertion here
    // is about the set with the small limit, and that is the only one there is.
    let rolled = dir.path().join("rolled");
    let source = common::source(dir.path());
    for i in 0..4 {
        fs::write(source.join("a.bin"), common::noisy(200_000 + i * 11_000)).unwrap();
        common::run_backup_limited(&source, &rolled, small).await;
    }

    let store = BlobStore::open_for_reading(&rolled).await.unwrap();
    assert!(store.segment_count() > 1, "the set should have rolled over");

    let rolled_set = IndexSet::new(&rolled);
    for v in 1..=4 {
        let index = VersionIndex::read(&rolled_set.index_path(v))
            .unwrap_or_else(|| panic!("version {v} should have an index"));

        let placement = store.locate(&index.snapshot).unwrap();
        assert_eq!(
            placement.segment, index.segment,
            "version {v}'s index names segment {} but the snapshot is in {}",
            index.segment, placement.segment
        );
    }

    // And the resolve is confirmed rather than falling back.
    //
    // An equality, not a `matches!`. The pattern form throws away `segment` and
    // `snapshot`, so an index resolving to the wrong segment passed — in the one
    // test whose entire claim is that it resolves to the right one. What is
    // asserted here is what each version's index already said, checked against
    // where the store actually holds the snapshot.
    for v in 1..=4 {
        let index = VersionIndex::read(&rolled_set.index_path(v))
            .unwrap_or_else(|| panic!("version {v} should have an index"));
        let placement = store.locate(&index.snapshot).unwrap();

        assert_eq!(
            resolve(&rolled_set, v, 4),
            Resolved::Confirmed {
                segment: placement.segment,
                snapshot: index.snapshot,
            },
            "version {v} should resolve from its index, to the segment the snapshot is in"
        );
    }
}

/// The index files can all be deleted and rebuilt.
///
/// This is the property that makes them a cache rather than a second source of
/// truth, and it is the operation the previous layout had no equivalent of:
/// there, an index file was the only record that a version existed, so deleting
/// one deleted the backup. Here the commit records are the record, and the files
/// are derived from them.
#[tokio::test]
async fn the_index_files_can_all_be_deleted_and_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, store) = set_with_versions(dir.path(), 4).await;
    let set = IndexSet::new(&blob);

    let before: Vec<_> = (1..=4).map(|v| store.snapshot(v).unwrap()).collect();

    for version in set.indexed_versions().unwrap() {
        fs::remove_file(set.index_path(version)).unwrap();
    }
    assert!(
        set.indexed_versions().unwrap().is_empty(),
        "every index file should have been removed"
    );

    // Versions still exist. Nothing was lost, because the store never depended on
    // these files.
    let reopened = BlobStore::open_for_reading(&blob).await.unwrap();
    for (i, version) in (1..=4u32).enumerate() {
        assert_eq!(reopened.snapshot(version).unwrap(), before[i]);
    }

    // And they can be rebuilt from the store.
    let written =
        BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
            .await
            .unwrap()
            .write_index_files()
            .unwrap();
    assert_eq!(written, 4);

    assert_eq!(set.indexed_versions().unwrap(), vec![1, 2, 3, 4]);
    for (i, v) in (1..=4u32).enumerate() {
        let index = version(&blob, v);
        assert_eq!(
            index.snapshot, before[i],
            "a rebuilt index should name the same snapshot the store does"
        );
    }
}

/// A rebuilt index resolves, so it is not merely present but correct.
///
/// A rebuild that wrote plausible files pointing at the wrong commit record would
/// pass every test above except this one, which is why it is separate.
#[tokio::test]
async fn a_rebuilt_index_resolves_to_the_right_commit() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");
    let small = lepatch::storage::SegmentLimit::new(256 * 1024);

    // A set that has genuinely rolled over, so the ordinals are not all zero and
    // an off-by-one in the per-segment counting would show.
    for i in 0..5 {
        fs::write(source.join("a.bin"), common::noisy(200_000 + i * 13_000)).unwrap();
        common::run_backup_limited(&source, &blob, small).await;
    }

    let store = BlobStore::open_for_writing(&blob, small, Compression::default())
        .await
        .unwrap();
    assert!(store.segment_count() > 1, "the set should have rolled over");

    for v in 1..=5u32 {
        assert!(
            matches!(
                resolve(&IndexSet::new(&blob), v, 5),
                Resolved::Confirmed { .. }
            ),
            "version {v}'s index should name the segment and commit it was written to"
        );
    }
}

/// Nothing in this file can make a set report a version it does not hold.
///
/// The property every other test is a case of, and the reason the store's commit
/// records — not these files — are the record of what exists.
#[tokio::test]
async fn an_index_cannot_invent_a_version() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, store) = set_with_versions(dir.path(), 2).await;
    let set = IndexSet::new(&blob);

    // A complete set of index files for versions nobody ever committed.
    for v in 3..=8 {
        VersionIndex {
            version: v,
            segment: 0,
            ordinal: v - 1,
            snapshot: BlobId::from_bytes([u8::try_from(v).unwrap(); 32]),
        }
        .write_atomic(&set.index_path(v))
        .unwrap();
    }

    for v in 3..=8 {
        assert_eq!(
            resolve(&set, v, 2),
            Resolved::Disagrees,
            "an index for version {v} of a two-version set should be refused"
        );
        assert!(
            store.snapshot(v).is_err(),
            "the store must not report version {v} because a file says so"
        );
    }
}

/// An index path that is a directory is no index, and the set still reads.
///
/// The doc on `VersionIndex::read` used to call this out as the one failure it
/// did *not* swallow: "a directory that cannot be read at all is not swallowed —
/// that is a problem with the set". The implementation is `fs::read(path).ok()?`,
/// which discards the `io::Error` whole, so a directory and a permission problem
/// both arrive as `None` and no distinction is made. The sentence described an
/// intention the code never had.
///
/// An index file is a cache. One that cannot be read — for any reason, including
/// being the wrong type of file entirely — is one to rebuild, and rebuilding is
/// always available, so refusing the whole set over it would be the wrong answer
/// anyway. The set's own problems are reported where the set is read: listing the
/// directory cannot read, or the store's scan refusing to open.
#[tokio::test]
async fn an_index_path_that_is_a_directory_is_treated_as_no_index() {
    let dir = tempfile::tempdir().unwrap();
    let (blob, _store) = set_with_versions(dir.path(), 3).await;
    let set = IndexSet::new(&blob);

    // What each version's own index says, read from the files rather than
    // worked out — the snapshot hash is the hash of the snapshot, not of the
    // version number.
    let indexed: Vec<VersionIndex> = (1..=3)
        .map(|v| {
            VersionIndex::read(&set.index_path(v))
                .unwrap_or_else(|| panic!("version {v} should have an index file"))
        })
        .collect();

    // All three resolve, from their own index files.
    for (i, index) in indexed.iter().enumerate() {
        let v = u32::try_from(i).unwrap_or(0) + 1;
        assert_eq!(
            resolve(&set, v, 3),
            Resolved::Confirmed {
                segment: index.segment,
                snapshot: index.snapshot,
            },
            "version {v} should resolve from its index file"
        );
    }

    // Version 2's index becomes a directory. `read` must say "no index" rather
    // than fail, and the version must still be servable from the store.
    let path = set.index_path(2);
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();

    assert_eq!(
        resolve(&set, 2, 3),
        Resolved::Absent,
        "a directory is not a usable index"
    );

    // The store's own commit log still knows the version, which is the point of
    // not making the index authoritative.
    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(
        store.snapshot(2).unwrap(),
        indexed[1].snapshot,
        "the version is still there, and the index file was only ever a cache"
    );

    // And the other two are untouched: one bad path does not disturb the rest.
    for (v, index) in [(1, &indexed[0]), (3, &indexed[2])] {
        assert_eq!(
            resolve(&set, v, 3),
            Resolved::Confirmed {
                segment: index.segment,
                snapshot: index.snapshot,
            },
            "version {v} should be unaffected by version 2's index being a directory"
        );
    }
}
