// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use tokio::{fs as fs_async, io::AsyncSeekExt, task::JoinSet};
use tracing::instrument;

use crate::{
    fsattr,
    metadata::{self, Snapshot},
    progress::{Counter, Phase, Progress},
    slice,
    storage::{self, StreamReadSeeker},
};

use super::filter::{Filter, dirs_of};

/// Files to write at once. A restore spends most of its time waiting, so several
/// files in flight keep the store and the disk busy. The cap stops a tree of a
/// million small files from opening a million of them.
const RESTORE_CONCURRENCY: usize = 8;

/// How many files to write at once.
fn restore_concurrency() -> usize {
    super::concurrency(RESTORE_CONCURRENCY)
}

/// What a restore put into the destination.
///
/// A restore merges by default. This struct is the only way to tell from the
/// outside that the destination may hold more than the snapshot accounts for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restored {
    /// Regular files written.
    pub files: u64,
    /// Directories created.
    pub dirs: u64,
    /// Hard and symbolic links created.
    pub links: u64,
    /// Distinct chunks read back out of the store.
    pub chunks: u64,
    /// Bytes of file content written.
    pub bytes: u64,
    /// Entries that recorded an owner this process could not set.
    ///
    /// Every one of them, for an unprivileged caller restoring a tree containing
    /// files it does not own. Reported rather than refused because the alternative is
    /// a half-written tree with the wrong owner already on disk.
    pub unowned: u64,
    /// Entries whose extended attributes this process could not set.
    ///
    /// Extended attributes are xattrs on Linux and access lists on Windows.
    /// A filesystem without support for them fails every entry that records
    /// any. Reported rather than refused for the same reason as `unowned`.
    pub unattributed: u64,
}

/// What a restore of a version would put into a destination, worked out by reading
/// the version and the store and by writing nothing.
///
/// The figures are the ones a real restore reports, from the same snapshot and the
/// same checks. A plan that says 12,000 bytes would be written describes a restore
/// that writes 12,000 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RestorePlan {
    /// Regular files the version records.
    pub files: u64,
    /// Directories it records.
    pub dirs: u64,
    /// Hard and symbolic links it records.
    pub links: u64,
    /// Distinct chunks it would read out of the store.
    pub chunks: u64,
    /// Bytes of file content it would write.
    ///
    /// Every recorded file's length, which is what a restore writes. Each file is
    /// created at that length before its chunks go in. The length is set again
    /// afterwards.
    pub bytes: u64,
    /// Entries that record an owner, which this process may not be able to set.
    ///
    /// A bound rather than a count, and deliberately so. Only *setting* an owner
    /// says whether this process is allowed to. A run that has written nothing
    /// cannot know how many of these it would have failed on. Every entry that
    /// records an owner is named, which is the most that could fail.
    pub unowned: u64,
    /// Entries that record extended attributes, which this process may fail to set.
    ///
    /// A bound rather than a count for the same reason as `unowned`. Extended
    /// attributes are xattrs on Linux and access lists on Windows.
    pub unattributed: u64,
}

/// Reassemble a snapshot under a destination.
///
/// Start with [`RestoreBuilder::new`]. Select paths with `files`.
/// Call `run` to write. Call `plan` through the plan builder to preview.
///
/// A restore merges by default. It writes snapshot files and leaves the rest.
/// The destination may hold more than the snapshot records when the run ends.
///
/// The run checks every chunk before writing one byte.
/// A damaged chunk refuses the whole run. Nothing is half written.
#[derive(Debug)]
pub struct RestoreBuilder<S> {
    root: PathBuf,
    snapshot_id: storage::BlobId,
    store: S,
    files: Vec<PathBuf>,
    cache_bytes: usize,
}

impl<S: storage::StorageGet + 'static> RestoreBuilder<S> {
    /// Start a restore of `snapshot_id` under `root` from `store`.
    ///
    /// The builder restores the whole snapshot. Select less with `files`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, snapshot_id: storage::BlobId, store: S) -> Self {
        Self {
            root: root.into(),
            snapshot_id,
            store,
            files: Vec::new(),
            cache_bytes: storage::DEFAULT_CACHE_BYTES,
        }
    }

    /// Restore only these paths.
    ///
    /// Empty means the whole snapshot. Parents are built as scaffolding.
    /// Everything else stays alone.
    #[must_use]
    pub fn files<I, P>(mut self, files: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.files.extend(files.into_iter().map(Into::into));
        self
    }

    /// Set the bytes of whole chunks the restore pass may keep in memory.
    ///
    /// Defaults to [`storage::DEFAULT_CACHE_BYTES`].
    /// One store read then serves every slice that names a held chunk.
    /// The restore lowers the budget to the bytes its snapshot references.
    /// The budget never exceeds the cache default.
    /// A budget of zero keeps nothing, and reads every slice from the store.
    #[must_use]
    pub fn cache_bytes(mut self, bytes: usize) -> Self {
        self.cache_bytes = bytes;
        self
    }

    /// Reassemble the snapshot under the destination.
    ///
    /// Restoration runs in four phases. Directories land first so parents exist.
    /// Files land next at their final length. Links land after their targets.
    /// Attributes land last so link creation cannot move directory times.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot cannot be read or decoded.
    /// Returns an error when the snapshot names a missing file or chunk.
    /// Returns an error when the tree cannot be written.
    /// Returns an error when a `--files` value is empty, absolute, or holds `..`.
    #[instrument(level = "info", skip(self), err)]
    pub async fn run(self) -> io::Result<Restored> {
        run_restore(
            self.root,
            self.snapshot_id,
            self.store,
            &self.files,
            self.cache_bytes,
        )
        .await
    }
}

/// Work out what a restore would write, and write nothing.
///
/// Start with [`RestorePlanBuilder::new`]. Select paths with `files`.
/// Call `run` to work out the figures.
///
/// The plan reads the whole version and hashes it. That is most of a restore.
/// A dry run saves the writes, not the reading.
/// A plan that succeeds describes a restore that would succeed.
#[derive(Debug)]
pub struct RestorePlanBuilder<S> {
    snapshot_id: storage::BlobId,
    store: S,
    files: Vec<PathBuf>,
}

impl<S: storage::StorageGet + 'static> RestorePlanBuilder<S> {
    /// Start a plan for `snapshot_id` from `store`.
    ///
    /// The builder plans the whole snapshot. Select less with `files`.
    #[must_use]
    pub fn new(snapshot_id: storage::BlobId, store: S) -> Self {
        Self {
            snapshot_id,
            store,
            files: Vec::new(),
        }
    }

    /// Plan only these paths.
    ///
    /// Empty means the whole snapshot. The figures describe the selection.
    #[must_use]
    pub fn files<I, P>(mut self, files: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.files.extend(files.into_iter().map(Into::into));
        self
    }

    /// Work out the figures and write nothing.
    ///
    /// A plan refuses what a restore refuses. A chunk that does not verify
    /// refuses the plan. A path that escapes the destination refuses it.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot cannot be read or decoded.
    /// Returns an error when a chunk does not verify.
    /// Returns an error when a path would land outside the destination.
    /// Returns an error when a recorded length mismatches its own slices.
    /// Returns an error when a `--files` value is empty, absolute, or holds `..`.
    #[instrument(level = "info", skip(self), err)]
    pub async fn run(self) -> io::Result<RestorePlan> {
        run_restore_plan(self.snapshot_id, self.store, &self.files).await
    }
}

/// Reassemble the snapshot under `root`, limited to `files`.
async fn run_restore<S: storage::StorageGet + 'static>(
    root: PathBuf,
    snapshot_id: storage::BlobId,
    storage: S,
    files: &[PathBuf],
    cache_bytes: usize,
) -> io::Result<Restored> {
    // Shared, not copied: the write tasks need owned data, and a clone of the
    // chunk table would be one allocation per chunk in the store.
    // A full run skips the view: selecting with no query would clone the whole
    // table to return it unchanged.
    let full = super::load_snapshot(&storage, snapshot_id).await?;
    let snapshot = if files.is_empty() {
        Arc::new(full)
    } else {
        let filter = Filter::new(files, &dirs_of(&full))?;
        Arc::new(filter.select(&full))
    };

    let root = root.as_path();
    let storage = Arc::new(storage);

    // Nothing below can detect damaged chunk data on its own. A short read would
    // leave a file quietly filled with the wrong bytes. So the whole snapshot is
    // checked before a single byte is written.
    super::verify::check(&snapshot, Arc::clone(&storage))
        .await
        .into_result()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("refusing to restore snapshot {snapshot_id}: {e}"),
            )
        })?;

    // And the paths it would write, for the same reason. A snapshot is untrusted
    // input. Nothing goes on disk until the code knows it is safe there.
    check_paths(&snapshot).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("refusing to restore snapshot {snapshot_id}: {e}"),
        )
    })?;

    // And that each file's recorded length is what its slices account for.
    //
    // This one is not about the store being damaged. `check` above covers that,
    // and a short read is caught per slice. It is about `File::len` being a
    // number in a blob that nothing downstream can check. Each file is `set_len`ed
    // to that number after its slices are written, to close a gap *between* them.
    // A length longer than the slices does not fail. The code zero-fills it, the
    // file comes out exactly the right size, and the restore reports success
    // having lost the tail. The same arithmetic runs before a backup is
    // committed. That is why it existed, on the side where the threat is a file
    // changing rather than a lie.
    snapshot
        .verify_file_lengths("does not match its own chunks")
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("refusing to restore snapshot {snapshot_id}: {e}"),
            )
        })?;

    restore_directories(root, &snapshot).await?;

    // The check above read every chunk whole through the store. The restore
    // pass reads each chunk again for every slice that names it, so it runs
    // behind a cache. The cache is built here and not earlier, so the check
    // keeps its own reads.
    //
    // The budget follows the work that is there. A restore over a small tree
    // keeps a small budget, and a restore over a shared tree keeps the chunks
    // it needs.
    //
    // A budget of zero keeps nothing, and the pass reads every slice from the
    // store. That path holds no cache and no lock.
    let budget = cache_budget(cache_bytes, referenced_bytes(&snapshot));
    let written = if budget == 0 {
        restore_files(root, Arc::clone(&snapshot), Arc::clone(&storage)).await?
    } else {
        let cached = Arc::new(storage::ChunkCache::with_bytes(
            Arc::clone(&storage),
            budget,
        ));
        restore_files(root, Arc::clone(&snapshot), cached).await?
    };

    // Off the runtime thread, and **in this order**. `restore_links` creates
    // symbolic links. Creating them changes the modification time of the directory
    // holding them. So `restore_attributes` has to set that time afterwards, or it
    // sets one the links immediately invalidate. That is a real ordering
    // constraint, not a habit. It is why these two are not run concurrently with
    // each other, even though they touch disjoint sets of paths.
    //
    // What is safe is moving each of them off the runtime. Both are plain
    // synchronous filesystem work in an async function. Every `symlink`, `link`
    // and `set_permissions` they make blocks a worker thread that other tasks
    // need. `open` and `open_reader` already send their scans to
    // `spawn_blocking` for the same reason this module is careful about
    // elsewhere. A tree of many links is where that shows.
    let for_links = root.to_path_buf();
    tokio::task::spawn_blocking({
        let root = for_links;
        let snapshot = Arc::clone(&snapshot);
        move || restore_links(&root, &snapshot)
    })
    .await
    .map_err(|e| join_panic(&e))??;

    let for_attributes = root.to_path_buf();
    let attrs = tokio::task::spawn_blocking({
        let root = for_attributes;
        let snapshot = Arc::clone(&snapshot);
        move || restore_attributes(&root, &snapshot)
    })
    .await
    .map_err(|e| join_panic(&e))??;

    Ok(Restored {
        files: u64::try_from(snapshot.files.len()).unwrap_or(0),
        dirs: u64::try_from(snapshot.dirs.len()).unwrap_or(0),
        links: u64::try_from(snapshot.links.len()).unwrap_or(0),
        chunks: u64::try_from(snapshot.chunks.len()).unwrap_or(0),
        bytes: written,
        unowned: attrs.unowned,
        unattributed: attrs.unattributed,
    })
}

/// Work out the figures a restore would report, and write nothing.
async fn run_restore_plan<S: storage::StorageGet + 'static>(
    snapshot_id: storage::BlobId,
    storage: S,
    files: &[PathBuf],
) -> io::Result<RestorePlan> {
    let full = super::load_snapshot(&storage, snapshot_id).await?;
    let snapshot = if files.is_empty() {
        full
    } else {
        let filter = Filter::new(files, &dirs_of(&full))?;
        filter.select(&full)
    };

    super::verify::check(&snapshot, Arc::new(storage))
        .await
        .into_result()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("refusing to restore snapshot {snapshot_id}: {e}"),
            )
        })?;

    super::restore::check_paths(&snapshot).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("refusing to restore snapshot {snapshot_id}: {e}"),
        )
    })?;

    snapshot
        .verify_file_lengths("does not match its own chunks")
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("refusing to restore snapshot {snapshot_id}: {e}"),
            )
        })?;

    Ok(RestorePlan {
        files: u64::try_from(snapshot.files.len()).unwrap_or(0),
        dirs: u64::try_from(snapshot.dirs.len()).unwrap_or(0),
        links: u64::try_from(snapshot.links.len()).unwrap_or(0),
        chunks: u64::try_from(snapshot.chunks.len()).unwrap_or(0),
        bytes: snapshot
            .files
            .iter()
            .fold(0u64, |total, file| total.saturating_add(file.len)),
        unowned: super::apply::owners(&snapshot),
        unattributed: super::apply::extended(&snapshot),
    })
}

/// Resolve a path recorded in a snapshot against the restore root, refusing
/// anything that would land outside it.
///
/// A snapshot is a blob in a file that gets synced, shared and backed up. Its
/// contents are untrusted input. Otherwise a hostile store would choose what a
/// restore writes and where.
///
/// Every recorded path goes through here. This covers directories, files, and
/// both halves of a link. The check uses the *recorded* path, before joining, so
/// a `..` anywhere is caught.
///
/// Only `..` and absolute paths are refused. `.` and repeated separators
/// normalise to the same place.
///
/// Shared with [`super::apply`], which writes a snapshot into a tree the same
/// way. It has the same reason to refuse. A snapshot arriving inside a
/// downloaded pack is no more trustworthy than one arriving from a store.
pub(crate) fn resolve(root: &Path, recorded: &Path, kind: &str) -> io::Result<PathBuf> {
    if recorded.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the snapshot records {kind} with an empty path"),
        ));
    }

    // On Windows this also covers a drive or UNC prefix, which `is_absolute`
    // would catch but a bare `C:foo` would not.
    for component in recorded.components() {
        match component {
            Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{kind} {} steps outside the restore root",
                        recorded.display()
                    ),
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{kind} {} is an absolute path, which a restore cannot place",
                        recorded.display()
                    ),
                ));
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }

    Ok(root.join(recorded))
}

/// Check every path a snapshot records, before anything is written.
///
/// The whole snapshot is checked before a byte is written. Finding out later that
/// entry 900,001 of a million escapes the destination would leave a tree. That
/// tree is neither the old one nor the new one.
///
/// Shared with [`super::apply`] for the same reason [`resolve`] is. Both write a
/// snapshot into a tree. A snapshot is untrusted input wherever it came from.
pub(crate) fn check_paths(snapshot: &Snapshot) -> io::Result<()> {
    let probe = Path::new(".");

    for dir in &snapshot.dirs {
        resolve(probe, &dir.path, "a directory")?;
    }

    for file in &snapshot.files {
        resolve(probe, &file.path, "a file")?;
    }

    for link in &snapshot.links {
        // A hard link names another entry in the tree, so its second path is
        // resolved against the root and has to stay inside it. A symbolic link's
        // is not resolved at all: it is written verbatim and followed by whoever
        // ends up following it. A tree containing a link to `/etc/passwd` is an
        // ordinary tree, and reproducing it faithfully is the job.
        match link {
            metadata::Link::Symbolic { path, .. } => {
                resolve(probe, path, "a link")?;
            }
            metadata::Link::Hard { path, shares } => {
                resolve(probe, path, "a link")?;
                resolve(probe, shares, "a hard link target")?;
            }
        }
    }

    Ok(())
}

/// Suffix of the file that marks a destination as being restored into.
const DESTINATION_LOCK: &str = "blu-restore.lock";

/// Suffix of the directory a clean restore is assembled in before being swapped
/// into place.
pub const PARTIAL_SUFFIX: &str = "blu-restore-partial";

/// Suffix of the directory the previous contents are moved aside to during a
/// swap.
pub const PREVIOUS_SUFFIX: &str = "blu-restore-previous";

/// Held for as long as a destination is being written to.
///
/// Two restores into one destination would interleave. One would truncate a file
/// while the other is halfway through writing it. The claim is a file *beside* the
/// destination, so it never appears in the restored output.
///
/// Its `Drop` removes the claim together with any `--clean` scratch directories.
/// A run killed rather than failing drops the work future. An interrupted restore
/// therefore leaves no half-written tree.
///
/// **Which** directory that is depends on how the destination was spelled, and
/// this type is the only place that decides. "Beside the destination" is resolved
/// once, here, by [`DestinationLock::resolved`], and everything that needs a
/// scratch path asks this type for it. It used to be decided twice. This type
/// decided it once, by canonicalising. The binary's own `sibling` decided it
/// again, without canonicalising. Restoring through a symlink therefore staged the
/// tree beside the *link* while the claim watched beside the *target*. A failed
/// clean restore then left a half-written tree on disk that the claim's own
/// cleanup could not reach. That is what the sentence above promises never
/// happens.
#[derive(Debug)]
pub struct DestinationLock {
    path: PathBuf,
    /// The destination as this claim resolved it, which is what scratch paths are
    /// named after.
    resolved: PathBuf,
    /// Sibling directories this run may have created, removed on the way out.
    scratch: Vec<PathBuf>,
    /// Held open for the lifetime of the claim. Dropping the `Lock` drops this.
    _file: fs::File,
}

impl DestinationLock {
    /// The destination as this claim resolved it.
    ///
    /// The staging directory for a `--clean` restore is `self.scratch_dir` of
    /// *this* value, and the swap is done against it. Passing the caller's own
    /// spelling of the destination instead is what let the two disagree.
    #[must_use]
    pub fn resolved(&self) -> &Path {
        &self.resolved
    }

    /// A path beside the destination, named for it and `suffix`.
    ///
    /// The same one this claim will clean up on the way out, because it is built
    /// from the same resolution. The binary had its own copy of this rule and it
    /// did not canonicalise, which is the bug this exists to end.
    #[must_use]
    pub fn scratch_dir(&self, suffix: &str) -> PathBuf {
        let name = self
            .resolved
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());

        self.resolved.with_file_name(format!("{name}.{suffix}"))
    }
}

impl Drop for DestinationLock {
    fn drop(&mut self) {
        for path in &self.scratch {
            // A successful swap has already renamed them away.
            let _ = fs::remove_dir_all(path);
        }

        let _ = fs::remove_file(&self.path);
    }
}

/// Claim exclusive use of a restore destination.
///
/// # Errors
///
/// Returns an error if the claim cannot be created, including when another
/// restore already holds it. A claim left behind by a killed run must be removed
/// by hand, exactly as for a backup set.
pub fn lock_destination(destination: &Path) -> io::Result<DestinationLock> {
    // `.` and `/` have no file name, and a lock beside them would be named after
    // nothing. Canonicalising gives every spelling of a directory one real name.
    // One that does not exist yet keeps the name it was given, since that is the
    // directory being claimed.
    let resolved = fs::canonicalize(destination).unwrap_or_else(|_| destination.to_path_buf());

    let name = resolved
        .file_name()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} has no directory name to take a restore claim beside",
                    destination.display()
                ),
            )
        })?
        .to_string_lossy()
        .into_owned();

    let path = resolved.with_file_name(format!("{name}.{DESTINATION_LOCK}"));

    // The same two paths `DestinationLock::scratch_dir` hands out, registered
    // before the claim is taken so nothing can create one without also being
    // covered by the cleanup.
    let scratch = [PARTIAL_SUFFIX, PREVIOUS_SUFFIX]
        .map(|suffix| resolved.with_file_name(format!("{name}.{suffix}")))
        .into();

    // The claim is a sibling of the destination, so it needs the destination's
    // parent to exist. Restoring into `mnt/new/photos` is an ordinary thing to
    // want, and without this the run fails with a bare ENOENT before it starts.
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.is_dir()
    {
        fs::create_dir_all(parent).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "could not create {} to hold the restore claim: {e}",
                    parent.display()
                ),
            )
        })?;
    }

    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                io::Error::new(
                    e.kind(),
                    format!(
                        "{} is being restored into by another blu process; \
                         if none is running, remove it",
                        path.display()
                    ),
                )
            } else {
                e
            }
        })?;

    Ok(DestinationLock {
        path,
        resolved,
        scratch,
        _file: file,
    })
}

/// Create every recorded directory, including ones that hold no files.
///
/// Concurrent, and the concurrency is the same cap `restore_files` uses rather
/// than all of them at once. `create_dir_all` creates the whole chain above its
/// argument, so these do not have to run parents-first. Two tasks racing on
/// `/a/b` and `/a/b/c` each create whatever is missing, and neither fails. The
/// end state is the same set of directories whichever order they finish in. That
/// makes the stage safe to parallelise. This matters because the stage does one
/// syscall per directory, one after another. A tree of a million directories
/// spent its whole restore waiting on a million sequential `mkdir` calls.
///
/// The containment check is still done first, in order, and still fails fast.
/// Resolving every path before any directory is created is what makes the code
/// refuse a snapshot that escapes the root *before* it touches the destination.
/// This is the property `tests/containment.rs` pins. The parallelism applies to
/// the syscall, not to the check.
pub(crate) async fn restore_directories(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    let mut paths = Vec::with_capacity(snapshot.dirs.len());
    for dir in &snapshot.dirs {
        paths.push(resolve(root, &dir.path, "a directory")?);
    }

    let concurrency = restore_concurrency();
    let mut creating = JoinSet::new();
    let mut next = 0usize;

    loop {
        while next < paths.len() && creating.len() < concurrency {
            let path = paths[next].clone();
            next += 1;
            creating.spawn(async move { fs_async::create_dir_all(path).await });
        }

        // The first error is the one to report. Joining in the order the
        // directories were recorded makes "the first" mean something a caller
        // can act on. The order decides, not whichever task failed first.
        let Some(joined) = creating.join_next().await else {
            break;
        };
        joined.map_err(|e| join_panic(&e))??;
    }

    Ok(())
}

/// An `io::Error` for a task that did not return one.
///
/// A restore task can only stop by panicking. It has no error to carry. The
/// alternative is a second panic in the middle of reporting the first. Saying
/// so is better than either.
fn join_panic(join: &tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("a restore task did not finish: {join}"))
}

/// Create and fill every regular file, a few at a time.
async fn restore_files<S: storage::StorageGet + 'static>(
    root: &Path,
    snapshot: Arc<Snapshot>,
    storage: Arc<S>,
) -> io::Result<u64> {
    // Grouped by *index* into `snapshot.file_chunks`. Copying the slices would
    // cost a second full set on top of the snapshot. Cloning the snapshot
    // would cost an allocation per chunk in the store.
    let mut slices: Vec<Vec<usize>> = vec![Vec::new(); snapshot.files.len()];
    for (position, file_chunk) in snapshot.file_chunks.iter().enumerate() {
        let index = file_chunk.file_index as usize;
        let bucket = slices.get_mut(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk slice references file {index}, which the snapshot does not contain"),
            )
        })?;
        bucket.push(position);
    }

    let concurrency = restore_concurrency();
    let total_files = u64::try_from(snapshot.files.len()).unwrap_or(0);

    // Unlike a backup, the total is known up front, so this reports a fraction.
    let mut progress = Progress::new(Phase::Restore);
    progress.set(Counter::Files, total_files);
    progress.report();

    let mut written: u64 = 0;

    let mut writing = JoinSet::new();
    let mut next = 0usize;

    loop {
        // Keep as many files in flight as asked for.
        while next < snapshot.files.len() && writing.len() < concurrency {
            let job = RestoreJob {
                path: resolve(root, &snapshot.files[next].path, "a file")?,
                file: snapshot.files[next].clone(),
                slices: std::mem::take(&mut slices[next]),
                snapshot: Arc::clone(&snapshot),
            };
            next += 1;

            writing.spawn(write_file(job, Arc::clone(&storage)));
        }

        // The loop ends when there is nothing left to wait for.
        //
        // Getting here means the refill loop above stopped. One of two things
        // caused that. `next` has reached the end of the file list, or `writing`
        // is at capacity. `restore_concurrency` never returns less than one, so
        // at capacity means non-empty. Then `join_next` blocks for a result
        // rather than returning `None`. The only way to see `None` is both
        // exhausted at once, which is the end of the run.
        //
        // This used to test `writing.is_empty()` and spell that same pair of
        // conditions out by hand. It also had a `None` arm on the join below
        // that could not be reached, because the emptiness case was already
        // handled. The version had a `continue` for the case where neither
        // condition held, which cannot occur. If `next` has not reached the end
        // then the refill loop stopped at capacity. Capacity is at least one.
        let Some(result) = writing.join_next().await else {
            break;
        };

        // The first failure ends the run. Files already written stay written:
        // a restore is not atomic, and pretending otherwise would be a bigger
        // change than making the loop concurrent.
        match result {
            Ok(Err(e)) => return Err(e),
            Err(join) => {
                return Err(io::Error::other(format!("a restore task failed: {join}")));
            }
            // A finished file reports the bytes it actually wrote, which is what
            // the rate is computed from.
            Ok(Ok(file_bytes)) => {
                progress.add(Counter::Done, 1);
                progress.add(Counter::Written, file_bytes);
                progress.report();
                written = written.saturating_add(file_bytes);
            }
        }
    }

    crate::telemetry::metrics::record_run(&progress);

    Ok(written)
}

/// One file's worth of work, owned so it can outlive the loop that queued it.
///
/// The snapshot is shared rather than copied, so a task can hold a reference
/// without paying an allocation per chunk in the store.
struct RestoreJob {
    /// Where the file goes, already joined onto the restore root.
    path: PathBuf,
    file: metadata::File,
    /// Positions in `snapshot.file_chunks` belonging to this file.
    slices: Vec<usize>,
    snapshot: Arc<Snapshot>,
}

/// Create one file at its final length and write its chunk slices into it.
async fn write_file<S: storage::StorageGet>(job: RestoreJob, storage: Arc<S>) -> io::Result<u64> {
    let RestoreJob {
        path,
        file,
        slices,
        snapshot,
    } = job;

    // Created before the content, at its final size, so an empty file still
    // comes back and the file is never momentarily the wrong length.
    if let Some(parent) = path.parent() {
        fs_async::create_dir_all(parent).await?;
    }

    // Removed rather than opened and truncated in place. Opening for write
    // *follows* a symbolic link. A `link -> /etc` left by an earlier run would
    // otherwise be written through, and its target truncated. Links are removed
    // before they are recreated for the same reason.
    remove_existing(&path)?;

    let mut handle = fs_async::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await?;

    handle.set_len(file.len).await?;

    for position in slices {
        // Borrowed from the shared snapshot, which is why positions were kept
        // rather than copies of the slices.
        let Some(file_chunk) = snapshot.file_chunks.get(position) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk slice {position} is not in the snapshot"),
            ));
        };

        let chunk = snapshot
            .chunks
            .get(file_chunk.chunk_index as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "chunk slice references chunk {}, which the snapshot does not contain",
                        file_chunk.chunk_index
                    ),
                )
            })?;

        let length = u64::from(file_chunk.length);

        handle
            .seek(io::SeekFrom::Start(file_chunk.file_offset))
            .await?;

        let mut source: StreamReadSeeker = storage.get(chunk).await?;
        source
            .seek(io::SeekFrom::Start(file_chunk.chunk_offset.into()))
            .await?;

        let mut source = slice::SliceAsyncReader::new(source, length);
        let mut sink = slice::SliceAsyncWriter::new(&mut handle, length);

        let written = tokio::io::copy(&mut source, &mut sink).await?;

        if written != length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "chunk {} is shorter than the {length} bytes recorded for {}",
                    chunk,
                    file.path.display()
                ),
            ));
        }
    }

    // Closes a recorded slice gap, which would otherwise leave zeroes mid-file.
    handle.set_len(file.len).await?;

    Ok(file.len)
}

/// Recreate hard links and symbolic links.
///
/// Shared with [`super::apply`], which creates the same links from the same
/// version. Only the *files* differ between the two. A restore writes every byte.
/// An apply writes only the bytes a destination lacks. This stage does the same
/// work in both. Duplicating it would give two places to keep in step for no
/// reason.
pub(crate) fn restore_links(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for link in &snapshot.links {
        // Restoring over an existing tree must not trip over the old entry, so
        // each arm removes before it creates. The code uses two arms rather than one
        // `match` on a `kind` field. The whole point of the type is that the two
        // cases are not the same operation on the same field.
        match link {
            metadata::Link::Symbolic { path, target } => {
                let path = resolve(root, path, "a link")?;
                remove_existing(&path)?;

                fsattr::symlink(target, &path).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "failed to create symlink {} -> {}: {e}",
                            path.display(),
                            target.display()
                        ),
                    )
                })?;
            }
            metadata::Link::Hard { path, shares } => {
                let path = resolve(root, path, "a link")?;
                remove_existing(&path)?;

                let target = resolve(root, shares, "a hard link target")?;
                fsattr::hard_link(&target, &path).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "failed to link {} to {}: {e}",
                            path.display(),
                            target.display()
                        ),
                    )
                })?;
            }
        }
    }

    Ok(())
}

/// What one attribute stage recorded about failures.
///
/// Both counts cover files and directories. The stage reports them together
/// because it walks both in one pass. Callers split them into their reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AttrCounts {
    /// Entries whose owner could not be set.
    pub unowned: u64,
    /// Entries whose extended attributes could not be set.
    pub unattributed: u64,
}

/// Apply owners and permission bits and times and extended attributes.
///
/// Files come before directories. Shared with [`super::apply`] for the same
/// reason [`restore_links`] is. An apply prunes before calling this. Removing
/// an entry moves the time of the directory that held it. A directory time is
/// set only once nothing else moves it.
///
/// Returns how many entries missed an owner or extended attributes. An
/// unprivileged caller misses every owned entry. A filesystem without
/// attribute support misses every entry that records any. Both are counts
/// rather than errors. Stopping at the first miss leaves a half written tree
/// with nothing to retry.
///
/// # Errors
///
/// Returns an error if a path escapes the root. Returns an error if a mode or
/// a time cannot be applied. Owner and extended misses stay quiet.
pub(crate) fn restore_attributes(root: &Path, snapshot: &Snapshot) -> io::Result<AttrCounts> {
    let mut counts = AttrCounts::default();

    for file in &snapshot.files {
        let path = resolve(root, &file.path, "a file")?;
        let outcome = file.attrs.apply_restoring(&path)?;
        if !outcome.owner_applied {
            counts.unowned = counts.unowned.saturating_add(1);
        }
        if !outcome.xattrs_applied || !outcome.acl_applied {
            counts.unattributed = counts.unattributed.saturating_add(1);
        }
    }

    for dir in &snapshot.dirs {
        let path = resolve(root, &dir.path, "a directory")?;
        let outcome = dir.attrs.apply_restoring(&path)?;
        if !outcome.owner_applied {
            counts.unowned = counts.unowned.saturating_add(1);
        }
        if !outcome.xattrs_applied || !outcome.acl_applied {
            counts.unattributed = counts.unattributed.saturating_add(1);
        }
    }

    Ok(counts)
}

/// The bytes of whole chunks the restore pass may keep in memory.
///
/// The budget follows the work that is there.
/// The cache holds at most the bytes the snapshot references.
/// The budget never exceeds the cache default either.
/// A budget of zero keeps nothing, and reads every slice from the store.
fn cache_budget(requested: usize, referenced: u64) -> usize {
    let referenced = usize::try_from(referenced).unwrap_or(usize::MAX);
    requested.min(referenced).min(storage::DEFAULT_CACHE_BYTES)
}

/// The bytes a snapshot references.
///
/// Every slice counts once, because the restore writes the range of each slice.
fn referenced_bytes(snapshot: &Snapshot) -> u64 {
    snapshot.file_chunks.iter().fold(0_u64, |total, slice| {
        total.saturating_add(u64::from(slice.length))
    })
}

/// Remove whatever currently occupies `path`, if anything.
///
/// Shared with [`super::apply`], which prunes the entries a version does not
/// record with it. It needs exactly this behaviour. The thing occupying a
/// recorded path may be a directory, a file or a link. Each one has to go the way
/// its own kind does.
pub(crate) fn remove_existing(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::cache_budget;
    use crate::storage::DEFAULT_CACHE_BYTES;

    #[test]
    fn a_budget_follows_the_bytes_the_snapshot_references() {
        let budget = cache_budget(DEFAULT_CACHE_BYTES, 1024);

        assert_eq!(budget, 1024);
    }

    #[test]
    fn a_budget_never_exceeds_the_cache_default() {
        let budget = cache_budget(usize::MAX, u64::MAX);

        assert_eq!(budget, DEFAULT_CACHE_BYTES);
    }

    #[test]
    fn a_zero_budget_keeps_nothing() {
        let budget = cache_budget(0, 1024);

        assert_eq!(budget, 0);
    }
}
