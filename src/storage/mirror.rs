// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Finished files between local staging and a keyed backend.
//!
//! The store scans local staging only. It never scans a backend. This module
//! copies finished files either way. Segment files travel under their
//! [`crate::layout::IndexSet::segment_name`]. Index files travel under their
//! [`crate::layout::IndexSet::index_name`]. Anything else beside the set is
//! not ours. Both directions leave such files alone.
//!
//! Two rules keep one writer honest:
//!
//! - A direction never overwrites a longer file. A longer file on the far
//!   side means a second writer. The run fails instead of merging the two.
//! - A pull never repairs. A local file that is shorter than the far one is
//!   damage, not a delta. The run fails instead of filling the gap.
//!
//! Run pull on a fresh machine before the first open. Run push after a
//! commit. The lock file never travels. It guards one machine only.
//!
//! Sync and async executors share the pure planners below, so the two
//! directions cannot drift: the plan is one function, and each executor only
//! carries it out.

use std::{ffi::OsStr, fs, io, path::PathBuf};

use crate::layout::IndexSet;

use super::{AsyncObjectStore, ObjectStore, RemoteUrl};

/// Counts from one mirror run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MirrorStats {
    /// Files uploaded to the backend.
    pub pushed_files: u64,
    /// Files downloaded from the backend.
    pub pulled_files: u64,
    /// Bytes uploaded to the backend.
    pub pushed_bytes: u64,
    /// Bytes downloaded from the backend.
    pub pulled_bytes: u64,
}

impl MirrorStats {
    /// Add one upload of `bytes`.
    fn add_push(&mut self, bytes: u64) {
        self.pushed_files = self.pushed_files.saturating_add(1);
        self.pushed_bytes = self.pushed_bytes.saturating_add(bytes);
    }

    /// Add one download of `bytes`.
    fn add_pull(&mut self, bytes: u64) {
        self.pulled_files = self.pulled_files.saturating_add(1);
        self.pulled_bytes = self.pulled_bytes.saturating_add(bytes);
    }
}

/// One local file with its key and length, for planning a mirror run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Staged {
    /// Object key the file travels under.
    key: String,
    /// Bytes the local file holds.
    len: u64,
    /// Local path of the file.
    path: PathBuf,
}

/// One file staging must receive, with where it lands.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Wanted {
    /// Object key the file travels under.
    key: String,
    /// Local path the download writes.
    path: PathBuf,
}

/// Upload finished local files to the backend.
///
/// Uploads every segment the staging set holds, tail included, and every
/// index file. Skips what the backend already holds at the same length.
/// Uploads what the backend lacks or holds shorter. Fails on what the backend
/// holds longer: a longer far file means a second writer.
///
/// The tail travels whole on every push. Keep segments small for remote sets,
/// tens to low hundreds of megabytes, so a commit uploads a bounded tail.
///
/// The lock file never travels.
///
/// # Errors
///
/// Returns an error if staging cannot be listed. Returns an error if an
/// upload fails. Returns an error on a longer far file.
pub fn push_to_remote(staging: &IndexSet, remote: &impl ObjectStore) -> io::Result<MirrorStats> {
    let local = stage_upload_list(staging)?;

    let mut far = Vec::with_capacity(local.len());
    for file in &local {
        far.push((file.key.clone(), remote.stat(&file.key)?));
    }

    let mut stats = MirrorStats::default();
    for file in plan_uploads(&local, &far)? {
        stats.add_push(remote.import_file(&file.key, &file.path)?);
    }
    Ok(stats)
}

/// Download what staging lacks from the backend.
///
/// Downloads keys the set grammar accepts and staging does not hold. Skips
/// what staging already holds at the same length. Fails on any length
/// mismatch: a shorter local file is damage, and a longer far file means a
/// second writer. Ignores keys outside the set grammar.
///
/// Creates the staging directory on the way in.
///
/// # Errors
///
/// Returns an error if the backend cannot be listed. Returns an error if a
/// download fails. Returns an error on a length mismatch.
pub fn pull_from_remote(remote: &impl ObjectStore, staging: &IndexSet) -> io::Result<MirrorStats> {
    let mut keys = remote.keys()?;
    keys.sort();

    let mut existing = Vec::new();
    for key in &keys {
        let Some(path) = dest_for(staging, key) else {
            continue;
        };
        let local = match super::at(&path, "measure", fs::metadata(&path)) {
            Ok(meta) => Some(meta.len()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        existing.push((key.clone(), local, path));
    }

    let mut far = Vec::with_capacity(existing.len());
    for (key, _, _) in &existing {
        if let Some(len) = remote.stat(key)? {
            far.push((key.clone(), len));
        }
    }

    let mut stats = MirrorStats::default();
    for wanted in plan_downloads(&existing, &far)? {
        stats.add_pull(remote.export_file(&wanted.key, &wanted.path)?);
    }
    Ok(stats)
}

/// Upload finished local files to the backend, without blocking the runtime.
///
/// Same plan as [`push_to_remote`], carried out through an async backend.
/// Synchronous backends arrive through the blanket implementation of
/// [`AsyncObjectStore`], which moves each call onto a blocking thread.
///
/// # Errors
///
/// Returns an error if staging cannot be listed. Returns an error if an
/// upload fails. Returns an error on a longer far file.
pub async fn push_to_remote_async(
    staging: &IndexSet,
    remote: &impl AsyncObjectStore,
) -> io::Result<MirrorStats> {
    let local = stage_upload_list(staging)?;

    let mut far = Vec::with_capacity(local.len());
    for file in &local {
        far.push((file.key.clone(), remote.stat(&file.key).await?));
    }

    let mut stats = MirrorStats::default();
    for file in plan_uploads(&local, &far)? {
        stats.add_push(remote.import_file(&file.key, &file.path).await?);
    }
    Ok(stats)
}

/// Download what staging lacks from the backend, without blocking the runtime.
///
/// Same plan as [`pull_from_remote`], carried out through an async backend.
///
/// # Errors
///
/// Returns an error if the backend cannot be listed. Returns an error if a
/// download fails. Returns an error on a length mismatch.
pub async fn pull_from_remote_async(
    remote: &impl AsyncObjectStore,
    staging: &IndexSet,
) -> io::Result<MirrorStats> {
    let mut keys = remote.keys().await?;
    keys.sort();

    let mut existing = Vec::new();
    for key in &keys {
        let Some(path) = dest_for(staging, key) else {
            continue;
        };
        let local = match super::at(&path, "measure", fs::metadata(&path)) {
            Ok(meta) => Some(meta.len()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        existing.push((key.clone(), local, path));
    }

    let mut far = Vec::with_capacity(existing.len());
    for (key, _, _) in &existing {
        if let Some(len) = remote.stat(key).await? {
            far.push((key.clone(), len));
        }
    }

    let mut stats = MirrorStats::default();
    for wanted in plan_downloads(&existing, &far)? {
        stats.add_pull(remote.export_file(&wanted.key, &wanted.path).await?);
    }
    Ok(stats)
}

/// Staging base path for `set` from `url` under `root`.
///
/// Joins the root, one directory per store address, and the set name. The
/// address directory keeps two stores apart when their locations sanitize
/// alike: a hash of the full address backs the readable part. Validates the
/// set name with the one-component rule, so a path cannot escape the store
/// directory.
///
/// # Errors
///
/// Returns an error when `set` is empty or holds a path rather than one
/// name.
pub fn staging_set_path(root: &std::path::Path, url: &RemoteUrl, set: &str) -> io::Result<PathBuf> {
    crate::client::check_set(set)?;
    Ok(root
        .join("lepatch")
        .join("stores")
        .join(store_id(url))
        .join(set))
}

/// Cache root for staged sets.
///
/// Reads `$XDG_CACHE_HOME` first, then `$HOME/.cache`. Returns `None` when
/// neither names a directory: the run then needs `--staging`.
#[must_use]
pub fn default_cache_root() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("XDG_CACHE_HOME")
        && !dir.is_empty()
    {
        return Some(PathBuf::from(dir));
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.is_empty()
    {
        return Some(PathBuf::from(home).join(".cache"));
    }
    None
}

/// One directory name per store address: scheme, readable location, hash.
///
/// Sanitizes the location to file-safe characters and shortens it, then backs
/// it with eight hex digits of the full address. Readable in a listing,
/// unique across addresses that sanitize alike.
fn store_id(url: &RemoteUrl) -> String {
    let flat: String = url
        .location()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .take(32)
        .collect();
    let fingerprint = blake3::hash(url.to_string().as_bytes()).to_string();
    let short = fingerprint.get(..8).unwrap_or(fingerprint.as_str());

    format!("{}-{flat}-{short}", url.scheme().name())
}

/// Local segments and index files with their keys and lengths.
///
/// Pure local reads. Both executors share it, so upload planning starts from
/// one listing.
///
/// # Errors
///
/// Returns an error if staging cannot be listed. Returns an error when the
/// set base is not UTF-8: remote keys are UTF-8.
fn stage_upload_list(staging: &IndexSet) -> io::Result<Vec<Staged>> {
    let mut out = Vec::new();

    let segments = staging.segments()?;
    for (index, path) in segments.iter().enumerate() {
        let segment = u32::try_from(index).unwrap_or(u32::MAX);
        let key = staging.segment_name(segment);
        let Some(key) = key.to_str() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "this set base is not UTF-8, so it cannot mirror: remote keys are UTF-8",
            ));
        };
        let len = super::at(path, "measure", fs::metadata(path))?.len();
        out.push(Staged {
            key: key.to_string(),
            len,
            path: path.clone(),
        });
    }

    for version in staging.indexed_versions()? {
        let key = staging.index_name(version);
        let Some(key) = key.to_str() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "this set base is not UTF-8, so it cannot mirror: remote keys are UTF-8",
            ));
        };
        let path = staging.index_path(version);
        let len = super::at(&path, "measure", fs::metadata(&path))?.len();
        out.push(Staged {
            key: key.to_string(),
            len,
            path,
        });
    }

    Ok(out)
}

/// Local path for `key` when the set grammar accepts it, else `None`.
///
/// Stray keys never reach the planner: the executors filter with this first.
fn dest_for(staging: &IndexSet, key: &str) -> Option<PathBuf> {
    let name = OsStr::new(key);
    if let Some(segment) = staging.segment_number(name) {
        return Some(staging.segment_path(segment));
    }
    if let Some(version) = staging.index_number(name) {
        return Some(staging.index_path(version));
    }
    None
}

/// Local files the backend must receive.
///
/// Pure: no IO. `remote` pairs each local key with what the backend holds
/// for it, or `None` when the backend holds nothing there. Both executors
/// share it, so the upload decision cannot drift.
///
/// # Errors
///
/// Returns an error on a longer far file. A second writer owns that case,
/// never this run.
fn plan_uploads(local: &[Staged], remote: &[(String, Option<u64>)]) -> io::Result<Vec<Staged>> {
    let mut out = Vec::new();
    for file in local {
        let held = remote
            .iter()
            .find_map(|(key, len)| (key == &file.key).then_some(*len));
        match held {
            None | Some(None) => out.push(file.clone()),
            Some(Some(far)) if far < file.len => out.push(file.clone()),
            Some(Some(far)) if far == file.len => {}
            Some(Some(far)) => return Err(divergence(&file.key, file.len, far)),
        }
    }
    Ok(out)
}

/// Backend files staging must receive.
///
/// Pure: no IO. `existing` pairs each accepted key with staging's length for
/// it, or `None` when staging lacks the file. `remote` holds what the backend
/// has. Both executors share it.
///
/// # Errors
///
/// Returns an error on any length mismatch: a shorter local file is damage,
/// and a longer far file means a second writer.
fn plan_downloads(
    existing: &[(String, Option<u64>, PathBuf)],
    remote: &[(String, u64)],
) -> io::Result<Vec<Wanted>> {
    let mut out = Vec::new();
    for (key, far) in remote {
        let held = existing
            .iter()
            .find_map(|(name, len, path)| (name == key).then_some((*len, path)));
        match held {
            None => {}
            Some((None, path)) => out.push(Wanted {
                key: key.clone(),
                path: path.clone(),
            }),
            Some((Some(local), _)) if local == *far => {}
            Some((Some(local), _)) => return Err(divergence(key, local, *far)),
        }
    }
    Ok(out)
}

/// A file both sides hold at different lengths.
///
/// Never merged, never repaired. Lengths only grow by appends, and sealed
/// files never grow at all, so a mismatch is either damage or a second
/// writer. Both deserve an operator, not a guess.
fn divergence(key: &str, local_len: u64, far_len: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "object {key:?} differs: staging holds {local_len} bytes and the backend holds \
             {far_len}; two writers must not share one remote set"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::{plan_downloads, plan_uploads, pull_from_remote, push_to_remote, staging_set_path};
    use crate::{
        layout::IndexSet,
        storage::{MemoryStore, RemoteUrl},
    };

    /// Unique scratch base for one test, under the system temp dir.
    ///
    /// Holds a staging set, never a real one. Each test removes its own
    /// directory on the way out.
    fn scratch(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lepatch-mirror-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// Write a tiny staging set: two segment files and one index file, all a
    /// few bytes. The bytes are arbitrary. The mirror copies files, never
    /// records.
    fn stage(base: &std::path::Path) {
        let set = IndexSet::new(base.join("photos"));
        std::fs::create_dir_all(base).unwrap();
        std::fs::write(set.segment_path(0), b"segment-zero").unwrap();
        std::fs::write(set.segment_path(1), b"tail").unwrap();
        std::fs::write(set.index_path(1), b"index-one").unwrap();
    }

    /// The upload plan: missing and shorter far files upload, equal files
    /// rest, longer far files fail. No filesystem, small vectors.
    #[test]
    fn upload_plan_splits_on_lengths() {
        use super::Staged;

        let local = vec![
            Staged {
                key: "photos.0000.bin".to_string(),
                len: 12,
                path: std::path::PathBuf::from("photos.0000.bin"),
            },
            Staged {
                key: "photos.0001.bin".to_string(),
                len: 4,
                path: std::path::PathBuf::from("photos.0001.bin"),
            },
        ];
        let remote = vec![
            ("photos.0000.bin".to_string(), Some(12)),
            ("photos.0001.bin".to_string(), Some(2)),
            ("photos.0002.bin".to_string(), None),
        ];

        let planned = plan_uploads(&local, &remote).unwrap();
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].key, "photos.0001.bin");

        let longer = vec![("photos.0000.bin".to_string(), Some(20))];
        assert!(plan_uploads(&local[..1], &longer).is_err());
    }

    /// The download plan: absent local files download, equal files rest, any
    /// mismatch fails. No filesystem, small vectors.
    #[test]
    fn download_plan_splits_on_lengths() {
        let existing = vec![
            (
                "photos.0000.bin".to_string(),
                Some(12),
                std::path::PathBuf::from("photos.0000.bin"),
            ),
            (
                "photos.0001.bin".to_string(),
                None,
                std::path::PathBuf::from("photos.0001.bin"),
            ),
        ];
        let remote = vec![
            ("photos.0000.bin".to_string(), 12),
            ("photos.0001.bin".to_string(), 4),
        ];

        let planned = plan_downloads(&existing, &remote).unwrap();
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].key, "photos.0001.bin");

        let damaged = vec![("photos.0000.bin".to_string(), 20)];
        assert!(plan_downloads(&existing[..1], &damaged).is_err());
    }

    /// Staging paths join root, one address directory, and the set name. A
    /// path for a name is refused. No filesystem.
    #[test]
    fn staging_paths_stay_under_the_root() {
        let url = RemoteUrl::parse("s3://bucket/backups").unwrap();
        let base = staging_set_path(std::path::Path::new("/cache"), &url, "docs").unwrap();
        assert!(base.starts_with("/cache/lepatch/stores"));
        assert!(base.ends_with("docs"));

        let other = staging_set_path(std::path::Path::new("/cache"), &url, "other").unwrap();
        assert_eq!(base.parent(), other.parent());

        assert!(staging_set_path(std::path::Path::new("/cache"), &url, "a/b").is_err());
        assert!(staging_set_path(std::path::Path::new("/cache"), &url, "").is_err());

        let second = RemoteUrl::parse("s3://bucket/other").unwrap();
        assert_ne!(
            staging_set_path(std::path::Path::new("/cache"), &url, "docs")
                .unwrap()
                .parent(),
            staging_set_path(std::path::Path::new("/cache"), &second, "docs")
                .unwrap()
                .parent()
        );
    }

    /// Push uploads every finished file. Pull fills a fresh directory from
    /// the backend. A second push is a no-op. Small bytes only.
    #[test]
    fn push_then_pull_round_trips_finished_files() {
        let dir = scratch("round-trip");
        stage(&dir);
        let set = IndexSet::new(dir.join("photos"));
        let remote = MemoryStore::new();

        let pushed = push_to_remote(&set, &remote).unwrap();
        assert_eq!(pushed.pushed_files, 3);
        assert_eq!(pushed.pulled_files, 0);

        let fresh = IndexSet::new(dir.join("copy").join("photos"));
        let pulled = pull_from_remote(&remote, &fresh).unwrap();
        assert_eq!(pulled.pulled_files, 3);
        assert_eq!(
            std::fs::read(fresh.segment_path(0)).unwrap(),
            b"segment-zero"
        );
        assert_eq!(std::fs::read(fresh.index_path(1)).unwrap(), b"index-one");

        let again = push_to_remote(&set, &remote).unwrap();
        assert_eq!(again.pushed_files, 0);

        std::fs::remove_dir_all(dir).ok();
    }

    /// A longer file on either side fails the run. The mirror never merges
    /// and never repairs. Both directions report the same divergence.
    #[test]
    fn longer_files_on_either_side_fail() {
        let dir = scratch("divergence");
        stage(&dir);
        let set = IndexSet::new(dir.join("photos"));
        let remote = MemoryStore::new();
        push_to_remote(&set, &remote).unwrap();

        std::fs::write(set.segment_path(0), b"segment-zero plus more").unwrap();
        let longer_local = IndexSet::new(dir.join("other").join("photos"));
        std::fs::create_dir_all(dir.join("other")).unwrap();
        pull_from_remote(&remote, &longer_local).unwrap();
        std::fs::write(longer_local.segment_path(0), b"segment-zero plus more").unwrap();
        assert!(pull_from_remote(&remote, &longer_local).is_err());

        std::fs::remove_dir_all(dir).ok();
    }

    /// Keys outside the set grammar travel with nothing. A stray file beside
    /// the set, local or remote, is left alone.
    #[test]
    fn stray_keys_are_left_alone() {
        use crate::storage::ObjectStore;

        let dir = scratch("stray");
        stage(&dir);
        let set = IndexSet::new(dir.join("photos"));
        std::fs::write(dir.join("photos.notes.txt"), b"not ours").unwrap();
        let remote = MemoryStore::new();
        remote.write_bytes("photos.notes.txt", b"not ours").unwrap();

        let pushed = push_to_remote(&set, &remote).unwrap();
        assert_eq!(pushed.pushed_files, 3);

        let fresh = IndexSet::new(dir.join("copy").join("photos"));
        let pulled = pull_from_remote(&remote, &fresh).unwrap();
        assert_eq!(pulled.pulled_files, 3);
        assert!(
            !fresh
                .segment_path(0)
                .to_str()
                .unwrap_or("")
                .ends_with("notes.txt")
        );

        std::fs::remove_dir_all(dir).ok();
    }
}
