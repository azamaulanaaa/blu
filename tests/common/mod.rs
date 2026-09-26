//! Shared helpers for the integration tests.

#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use lepatch::{
    command::{Options, backup, restore},
    storage::BlobFileStorage,
};

/// Default chunker configuration, mirroring the CLI defaults.
pub fn options() -> Options {
    Options::default()
}

/// A flattened description of an entry in a directory tree.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Entry {
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
pub fn describe(root: &Path) -> BTreeMap<PathBuf, Entry> {
    fn walk(
        root: &Path,
        dir: &Path,
        out: &mut BTreeMap<PathBuf, Entry>,
        inodes: &mut BTreeMap<(u64, u64), PathBuf>,
    ) {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        let mut names: Vec<_> = entries.filter_map(|e| e.ok()).collect();
        names.sort_by_key(|e| e.file_name());

        for entry in names {
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();

            // `symlink_metadata` does not follow the link.
            let meta = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(_) => continue,
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

            match (key, inodes.get(&key.unwrap())) {
                (Some(_), Some(first)) => {
                    out.insert(relative, Entry::HardLink(first.clone()));
                }
                _ => {
                    if let Some(key) = key {
                        inodes.insert(key, relative.clone());
                    }
                    let contents = fs::read(&path).unwrap();
                    out.insert(relative, Entry::File(contents));
                }
            }
        }
    }

    let mut out = BTreeMap::new();
    let mut inodes = BTreeMap::new();
    walk(root, root, &mut out, &mut inodes);
    out
}

/// Back up `source` into a fresh blob store and return the snapshot key.
pub async fn run_backup(source: &Path, blob: &Path, base_key: Option<String>) -> String {
    let storage = BlobFileStorage::<true>::new(blob).await.unwrap();
    let opts = Options {
        base_key,
        ..options()
    };
    backup(source, opts, storage).await.unwrap()
}

/// Restore `key` from the blob store at `blob` into `destination`.
pub async fn run_restore(blob: &Path, key: &str, destination: &Path) {
    let storage = BlobFileStorage::<false>::new(blob).await.unwrap();
    restore(destination, key.to_string(), storage)
        .await
        .unwrap();
}

/// Back up `source`, restore it, and assert the two trees are identical.
pub async fn assert_round_trip(source: &Path) {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");
    let key = run_backup(source, &blob, None).await;
    let destination = dir.path().join("restored");

    run_restore(&blob, &key, &destination).await;

    let before = describe(source);
    let after = describe(&destination);

    let mut problems = Vec::new();
    for (path, expected) in &before {
        match after.get(path) {
            Some(actual) if actual == expected => {}
            Some(actual) => problems.push(format!("{path:?}: expected {expected:?}, got {actual:?}")),
            None => problems.push(format!("{path:?}: missing from restore ({expected:?})")),
        }
    }
    for path in after.keys() {
        if !before.contains_key(path) {
            problems.push(format!("{path:?}: unexpectedly present in restore"));
        }
    }

    assert!(
        problems.is_empty(),
        "round trip did not reproduce the source tree:\n  {}",
        problems.join("\n  ")
    );
}
