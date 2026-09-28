//! Shared helpers for the integration tests.

// The crate denies `unwrap_used` and `expect_used`, and `.clippy.toml` exempts
// code inside a `#[test]` function. These helpers are plain functions that tests
// call, so they fall outside that exemption and need their own: a helper that
// cannot read the tree it was asked to describe has nothing useful to report.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use lepatch::{
    command::{Committed, Options, backup, restore},
    storage::{BlobId, BlobStore},
};

/// Default chunker configuration, mirroring the CLI defaults.
pub(crate) fn options() -> Options {
    Options::default()
}

/// Pseudo-random bytes with no long runs.
///
/// Deliberately not a repeating pattern: content-defined chunking finds
/// boundaries where the data changes, so a constant or short-cycle byte stream
/// either produces one enormous chunk or collapses to a single deduplicated
/// entry, and neither exercises what these tests are for.
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

/// A flattened description of an entry in a directory tree.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Entry {
    /// A regular file and its exact contents.
    File(Vec<u8>),
    /// A symbolic link and the raw text of its target.
    Symlink(PathBuf),
    /// A hard link, identified by the target it shares an inode with.
    HardLink(PathBuf),
    /// A directory.
    Dir,
}

/// Recursively describe `root`, resolving hard links to the first path that
/// shares their inode so that link structure is compared rather than contents.
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

/// Back up `source` into a fresh store and return what was committed.
///
/// There is no base snapshot to pass: deduplication is the store's own business,
/// so a run hands it every chunk it read and the store decides what is new.
pub(crate) async fn run_backup(source: &Path, blob: &Path) -> Committed {
    run_backup_with(source, blob, Options::default()).await
}

/// Back up `source` with specific options, for tests that need to pin a knob the
/// defaults would otherwise choose.
pub(crate) async fn run_backup_with(source: &Path, blob: &Path, opts: Options) -> Committed {
    let storage = BlobStore::<true>::open(blob).await.unwrap();
    backup(source, opts, storage).await.unwrap()
}

/// Restore the snapshot named by `snapshot` from the store at `blob`.
pub(crate) async fn run_restore(blob: &Path, snapshot: BlobId, destination: &Path) {
    let storage = BlobStore::<false>::open(blob).await.unwrap();
    restore(destination, snapshot, storage).await.unwrap();
}

/// Back up `source`, restore it, and assert the two trees are identical.
pub(crate) async fn assert_round_trip(source: &Path) {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");
    let committed = run_backup(source, &blob).await;
    let destination = dir.path().join("restored");

    run_restore(&blob, committed.snapshot, &destination).await;

    let before = describe(source);
    let after = describe(&destination);

    let mut problems = Vec::new();
    for (path, expected) in &before {
        // The comparison is exact; only the failure message renders the path
        // lossily, which is the right trade for readability.
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
