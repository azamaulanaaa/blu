// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Shared helpers for the integration tests.

// These are plain functions that tests call.
// They fall outside the `#[test]`-only exemption in `.clippy.toml`.
// A helper that cannot read the tree it was asked to describe has nothing useful
// to report.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    fs,
    io::Cursor,
    path::{Path, PathBuf},
};

/// The hand-rolled HTTP/1.1 client, for the `serve` tests.
///
/// A submodule rather than more of this file, because it is only used by
/// `tests/serve.rs` and it pulls `serde_json` into any test binary that declares
/// `common`. See the module's own docs for why this is not `awc`.
pub(crate) mod http;

/// A `blu serve` running as a real process.
#[cfg(any(test, doc))]
pub(crate) mod server;

use blulib::{
    command::{Committed, Options, backup, restore},
    layout::IndexSet,
    storage::{BlobId, BlobStore, Compression, SegmentLimit, StreamReadSeeker},
};

/// An empty source tree inside `dir`, ready to be written into.
///
/// Fifty-one tests opened with the same two lines, and the pair is a unit.
/// A directory that has not had its `source` subdirectory created is a thing every
/// one of them then had to remember.
/// Creating it here means a test that wants a tree cannot forget to.
/// A "source tree" is a name and a pre-created subdirectory.
/// A change to either is one edit rather than fifty-one.
///
/// Takes a path rather than a `TempDir`.
/// The tests that pass a fixture down to a helper have only a `&Path` by then.
pub(crate) fn source(dir: &Path) -> PathBuf {
    let source = dir.join("source");
    fs::create_dir_all(&source).expect("a source directory in a fresh temp dir");
    source
}

/// A reader over `bytes`, for handing a store something to put.
pub(crate) fn reader(bytes: Vec<u8>) -> StreamReadSeeker {
    Box::new(Cursor::new(bytes))
}

/// The address a payload is stored under: the BLAKE3 of its contents.
pub(crate) fn id_of(bytes: &[u8]) -> BlobId {
    blake3::hash(bytes)
}

/// A length as the store's `u64`, from a slice the test already has in hand.
pub(crate) fn len_of(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap()
}

/// Pseudo-random bytes with no long runs.
///
/// Deliberately not a repeating pattern.
/// Content-defined chunking finds boundaries where the data changes.
/// A constant byte stream produces one enormous chunk.
/// A short-cycle byte stream collapses to a single deduplicated entry.
/// Neither exercises what these tests are for.
pub(crate) fn noisy(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            // A splitmix64 finaliser, truncated to a byte.
            let mut x = u64::try_from(i)
                .unwrap_or(0)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
            x ^= x >> 30;
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x ^= x >> 27;
            u8::try_from(x & 0xff).unwrap_or(0)
        })
        .collect()
}

/// Pseudo-random bytes that are not a prefix of [`noisy`], one distinct stream per
/// `tag`.
///
/// [`noisy`] is a *function of position only*.
/// So `noisy(n)` is the first n bytes of `noisy(m)` for any larger m.
/// A tree built from it is therefore degenerate in two ways at once.
/// Every file is a prefix of the next.
/// Deduplication then collapses far more than it would on real data.
/// Rewriting a file by asking for a shorter `noisy` leaves the same byte stream
/// truncated.
/// That truncation is the single most favourable thing that can happen to
/// content-defined chunking.
///
/// Tests about *deltas* need a tree where those two shortcuts are unavailable.
/// This splices a per-tag block over the second half of the stream.
/// No file is a prefix of another.
/// A rewrite is new content rather than a truncation.
pub(crate) fn untagged_stream(tag: u8, len: usize) -> Vec<u8> {
    let mut bytes = noisy(len);
    let block = blake3::hash(&[tag]).to_hex().to_string().into_bytes();
    let block = block.repeat(len / 64 + 1);
    let half = len / 2;
    bytes.truncate(half);
    bytes.extend_from_slice(&block[..len - half]);
    bytes
}

/// A flattened description of an entry in a directory tree.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Entry {
    /// A regular file and its exact contents.
    File(Vec<u8>),
    /// A symbolic link and the raw text of its target.
    Symlink(PathBuf),
    /// A hard link, with the path that shares its inode.
    HardLink(PathBuf),
    /// A directory.
    Dir,
}

/// Describe `root` recursively, and resolve hard links to the first path that
/// shares their inode.
/// The comparison then uses link structure rather than contents.
pub(crate) fn describe(root: &Path) -> BTreeMap<PathBuf, Entry> {
    fn walk(
        root: &Path,
        dir: &Path,
        out: &mut BTreeMap<PathBuf, Entry>,
        inodes: &mut BTreeMap<(u64, u64), PathBuf>,
    ) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };

        let mut names: Vec<_> = entries.filter_map(Result::ok).collect();
        names.sort_by_key(std::fs::DirEntry::file_name);

        for entry in names {
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();

            // `symlink_metadata` does not follow the link.
            let Ok(meta) = fs::symlink_metadata(&path) else {
                continue;
            };

            if meta.file_type().is_symlink() {
                let target = fs::read_link(&path).unwrap();
                out.insert(relative, Entry::Symlink(target));
                continue;
            }

            if meta.is_dir() {
                out.insert(relative.clone(), Entry::Dir);
                walk(root, &path, out, inodes);
                continue;
            }

            // Track the first path seen for each inode so later paths that
            // share it are reported as hard links.
            #[cfg(unix)]
            let key = {
                use std::os::unix::fs::MetadataExt;
                Some((meta.dev(), meta.ino()))
            };
            #[cfg(not(unix))]
            let key: Option<(u64, u64)> = None;

            if let Some(first) = key.and_then(|key| inodes.get(&key)) {
                out.insert(relative, Entry::HardLink(first.clone()));
                continue;
            }

            if let Some(key) = key {
                inodes.insert(key, relative.clone());
            }

            let contents = fs::read(&path).unwrap();
            out.insert(relative, Entry::File(contents));
        }
    }

    let mut out = BTreeMap::new();
    let mut inodes = BTreeMap::new();
    walk(root, root, &mut out, &mut inodes);
    out
}

/// Total bytes of the set at `base`, across every segment.
///
/// A set is several files, so a test that wants to know how much a run added has
/// to add them up.
/// Deliberately sums the whole set rather than one segment.
/// A backup that rolled over grew by more than a segment.
/// A test measuring only the last one would call that no growth at all.
pub(crate) fn set_size(base: &Path) -> u64 {
    IndexSet::new(base)
        .segments()
        .expect("the set should be readable")
        .iter()
        .map(|segment| fs::metadata(segment).map_or(0, |meta| meta.len()))
        .sum()
}

/// Back up `source` into a fresh store and return what was committed.
///
/// There is no base snapshot to pass.
/// Deduplication is the store's own business.
/// A run hands the store every chunk it read.
/// The store decides what is new.
pub(crate) async fn run_backup(source: &Path, blob: &Path) -> Committed {
    run_backup_with(source, blob, Options::default()).await
}

/// The stamp of a set's newest version, when the set holds one.
///
/// Reads the set through the open handle. A fresh set holds nothing.
async fn latest_of<C: blulib::storage::Compressor>(
    storage: &BlobStore<blulib::storage::Write, C>,
) -> Option<blulib::metadata::Timestamp> {
    let (version, _) = storage.latest()?;
    storage.version_time(version).await.ok()
}

/// Back up `source` with specific options, for tests that need to pin a knob the
/// defaults would otherwise choose.
pub(crate) async fn run_backup_with(source: &Path, blob: &Path, opts: Options) -> Committed {
    let storage =
        BlobStore::open_for_writing(blob, SegmentLimit::default(), Compression::default())
            .await
            .unwrap();
    let latest = latest_of(&storage).await;
    backup(source, opts, storage, latest).await.unwrap()
}

/// Back up `source` into a store with a given segment limit.
///
/// The limit belongs to the store rather than to the run.
/// The limit is a property of the files on disk.
/// A test that wants a small limit has to open the store itself.
/// This function opens the store itself.
pub(crate) async fn run_backup_limited(
    source: &Path,
    blob: &Path,
    limit: SegmentLimit,
) -> Committed {
    let storage = BlobStore::open_for_writing(blob, limit, Compression::default())
        .await
        .unwrap();
    let latest = latest_of(&storage).await;
    backup(source, Options::default(), storage, latest)
        .await
        .unwrap()
}

/// Restore the snapshot named by `snapshot` from the store at `blob`.
pub(crate) async fn run_restore(blob: &Path, snapshot: BlobId, destination: &Path) {
    let storage = BlobStore::open_for_reading(blob).await.unwrap();
    restore(destination, snapshot, storage).await.unwrap();
}

/// Back up `source` into a store with a given compression policy.
///
/// Compression is a property of the *store* rather than of the run's options.
/// The store frames and writes each record.
/// So this opens the store itself rather than going through [`run_backup`].
pub(crate) async fn run_backup_compressed(
    source: &Path,
    blob: &Path,
    compression: blulib::storage::Compression,
) -> Committed {
    let storage = BlobStore::open_for_writing(blob, SegmentLimit::default(), compression)
        .await
        .unwrap();
    let latest = latest_of(&storage).await;
    backup(source, Options::default(), storage, latest)
        .await
        .unwrap()
}

/// Restore one specific version of a set.
pub(crate) async fn run_restore_version(blob: &Path, destination: &Path, version: u32) {
    let storage = BlobStore::open_for_reading(blob).await.unwrap();
    let snapshot = storage.snapshot(version).unwrap();
    restore(destination, snapshot, storage).await.unwrap();
}

/// Assert that two trees are identical, naming every difference.
///
/// Shared by the round-trip tests and the apply tests.
/// Both assert the same thing by different routes.
/// `assert_round_trip` is "back this up and get it back".
/// An apply test is "bring this directory up to that version".
/// Both end here.
pub(crate) fn assert_same_tree(left: &Path, right: &Path) {
    let before = describe(left);
    let after = describe(right);

    let mut problems = Vec::new();
    for (path, expected) in &before {
        // The comparison is exact.
        // Only the failure message renders the path lossily.
        // That is the right trade for readability.
        match after.get(path) {
            Some(actual) if actual == expected => {}
            Some(actual) => {
                problems.push(format!(
                    "{}: expected {expected:?}, got {actual:?}",
                    path.display()
                ));
            }
            None => problems.push(format!(
                "{}: missing from restore ({expected:?})",
                path.display()
            )),
        }
    }
    for path in after.keys() {
        if !before.contains_key(path) {
            problems.push(format!(
                "{}: unexpectedly present in restore",
                path.display()
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "round trip did not reproduce the source tree:\n  {}",
        problems.join("\n  ")
    );
}

/// Back up `source`, restore it, and assert the two trees are identical.
pub(crate) async fn assert_round_trip(source: &Path) {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");
    let committed = run_backup(source, &blob).await;
    let destination = dir.path().join("restored");

    run_restore(&blob, committed.snapshot, &destination).await;

    assert_same_tree(source, &destination);
}
