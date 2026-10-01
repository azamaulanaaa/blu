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

/// Files to write at once. Restore is almost entirely waiting, so several in
/// flight is what keeps the store and the disk both busy; the cap keeps a tree of
/// a million small files from opening a million of them.
const RESTORE_CONCURRENCY: usize = 8;

/// How many files to write at once.
fn restore_concurrency() -> usize {
    super::concurrency(RESTORE_CONCURRENCY)
}

/// What a restore put into the destination.
///
/// A restore merges by default, so this is also the only way to tell from the
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
}

/// Reassemble the snapshot identified by `key` under `root`.
///
/// Restoration runs in four phases so that ordering constraints hold:
///
/// 1. directories, so that every parent exists;
/// 2. regular files, created at their final length before any content is
///    written, which is what makes empty files and hard link targets exist;
/// 3. links, which may only be created once their targets do;
/// 4. attributes, last, because writing into a directory updates its
///    modification time.
///
/// # Errors
///
/// Returns an error if the snapshot cannot be read or decoded, if it refers to
/// a file or chunk it does not contain, or if the tree cannot be written.
#[instrument(level = "info", skip(storage, root), err)]
pub async fn restore<P: AsRef<Path>, S: storage::StorageGet + 'static>(
    root: P,
    snapshot_id: storage::BlobId,
    storage: S,
) -> io::Result<Restored> {
    // Shared, not copied: the write tasks need owned data, and a clone of the
    // chunk table would be one allocation per chunk in the store.
    let snapshot = Arc::new(super::load_snapshot(&storage, snapshot_id).await?);

    let root = root.as_ref();
    let storage = Arc::new(storage);

    // Nothing below can detect damaged chunk data on its own — a short read would
    // leave a file quietly filled with the wrong bytes — so the whole snapshot is
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

    // And the paths it would write, for the same reason: a snapshot is untrusted
    // input, and nothing goes on disk until it is known to be safe there.
    check_paths(&snapshot).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("refusing to restore snapshot {snapshot_id}: {e}"),
        )
    })?;

    // And that each file's recorded length is what its slices account for.
    //
    // This one is not about the store being damaged — `check` above covers that,
    // and a short read is caught per slice. It is about `File::len` being a
    // number in a blob that nothing downstream can check: each file is `set_len`ed
    // to it after its slices are written, to close a gap *between* them, so a
    // length longer than the slices does not fail. It zero-fills, the file comes
    // out exactly the right size, and the restore reports success having lost
    // the tail. The same arithmetic runs before a backup is committed, which is
    // why it existed — on the side where the threat is a file changing rather
    // than a lie.
    snapshot
        .verify_file_lengths("does not match its own chunks")
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("refusing to restore snapshot {snapshot_id}: {e}"),
            )
        })?;

    restore_directories(root, &snapshot).await?;
    let written = restore_files(root, Arc::clone(&snapshot), Arc::clone(&storage)).await?;

    // Off the runtime thread, and **in this order**. `restore_links` creates
    // symbolic links, which changes the modification time of the directory
    // holding them, so `restore_attributes` has to set that time afterwards or it
    // sets one the links immediately invalidate. That is a real ordering
    // constraint, not a habit — it is why these two are not run concurrently with
    // each other, even though they touch disjoint sets of paths.
    //
    // What is safe is moving each of them off the runtime. Both are plain
    // synchronous filesystem work in an async function, so every `symlink`,
    // `link` and `set_permissions` they make blocks a worker thread that other
    // tasks need — and `open` and `open_for_reading` already send their scans to
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
    tokio::task::spawn_blocking({
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
    })
}

/// Resolve a path recorded in a snapshot against the restore root, refusing
/// anything that would land outside it.
///
/// A snapshot is a blob in a file that gets synced, shared and backed up, so its
/// contents are untrusted input: a hostile store would otherwise choose what a
/// restore writes and where. Every recorded path goes through here — directories,
/// files, and both halves of a link — and the check is on the *recorded* path,
/// before joining, so a `..` anywhere is caught.
///
/// Only `..` and absolute paths are refused; `.` and repeated separators
/// normalise to the same place.
///
/// Shared with [`super::apply`], which writes a snapshot into a tree the same way
/// and has the same reason to refuse: a snapshot arriving inside a downloaded pack
/// is no more trustworthy than one arriving from a store.
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
/// The whole snapshot is checked before a byte is written, so finding out that
/// entry 900,001 of a million escapes the destination afterwards would leave a
/// tree that is neither the old one nor the new one.
///
/// Shared with [`super::apply`] for the same reason [`resolve`] is: both write a
/// snapshot into a tree, and a snapshot is untrusted input wherever it came from.
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
const DESTINATION_LOCK: &str = "lepatch-restore.lock";

/// Suffix of the directory a clean restore is assembled in before being swapped
/// into place.
pub const PARTIAL_SUFFIX: &str = "lepatch-restore-partial";

/// Suffix of the directory the previous contents are moved aside to during a
/// swap.
pub const PREVIOUS_SUFFIX: &str = "lepatch-restore-previous";

/// Held for as long as a destination is being written to.
///
/// Two restores into one destination would interleave — one truncating a file
/// while the other is halfway through writing it. The claim is a file *beside* the
/// destination, so it never appears in the restored output.
///
/// Its `Drop` removes the claim together with any `--clean` scratch directories,
/// and a run killed rather than failing drops the work future, so an interrupted
/// restore leaves no half-written tree.
///
/// **Which** directory that is depends on how the destination was spelled, and
/// this type is the only place that decides. "Beside the destination" is resolved
/// once, here, by [`DestinationLock::resolved`], and everything that needs a
/// scratch path asks this type for it. It used to be decided twice — once here,
/// canonicalising, and once in the binary's own `sibling`, which did not — so
/// restoring through a symlink staged the tree beside the *link* while the claim
/// watched beside the *target*. A failed clean restore then left a half-written
/// tree on disk that the claim's own cleanup could not reach, which is the thing
/// the sentence above promises never happens.
#[derive(Debug)]
pub struct DestinationLock {
    path: PathBuf,
    /// The destination as this claim resolved it, which is what scratch paths are
    /// named after.
    resolved: PathBuf,
    /// Sibling directories this run may have created, removed on the way out.
    scratch: Vec<PathBuf>,
    /// Held open for the lifetime of the claim; dropping the `Lock` drops this.
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
    // nothing. Canonicalising gives every spelling of a directory one real name;
    // one that does not exist yet keeps the name it was given, since that is the
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
                        "{} is being restored into by another lepatch process; \
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
/// argument, so these do not have to run parents-first: two tasks racing on
/// `/a/b` and `/a/b/c` each create whatever is missing and neither fails, and the
/// end state is the same set of directories whichever order they finish in. That
/// makes the stage safe to parallelise, which matters because it is one syscall
/// per directory done one after another — a tree of a million directories spent
/// its whole restore waiting on a million sequential `mkdir` calls.
///
/// The containment check is still done first, in order, and still fails fast.
/// Resolving every path before any directory is created is what makes a snapshot
/// that escapes the root refused *before* the destination is touched at all, which
/// is the property `tests/containment.rs` pins — so the parallelism is applied to
/// the syscall and not to the check.
async fn restore_directories(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
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

        // The first error is the one to report, and joining in the order the
        // directories were recorded is what makes "the first" mean something a
        // caller can act on rather than whichever task happened to fail first.
        let Some(joined) = creating.join_next().await else {
            break;
        };
        joined.map_err(|e| join_panic(&e))??;
    }

    Ok(())
}

/// An `io::Error` for a task that did not return one.
///
/// A restore task can only stop by panicking, so there is no error to carry and
/// the alternative is a second panic in the middle of reporting the first. Saying
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
    // cost a second full set on top of the snapshot, and cloning the snapshot
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
        // Getting here means the refill loop above stopped, so one of two
        // things: `next` has reached the end of the file list, or `writing` is at
        // capacity. `restore_concurrency` never returns less than one, so at
        // capacity means non-empty, and `join_next` therefore blocks for a result
        // rather than returning `None`. The only way to see `None` is both
        // exhausted at once, which is the end of the run.
        //
        // This used to test `writing.is_empty()` and spell that same pair of
        // conditions out by hand, with a `None` arm on the join below that could
        // not be reached because the emptiness case was already handled. The
        // version also had a `continue` for the case where neither condition
        // held, which cannot occur: if `next` has not reached the end then the
        // refill loop stopped at capacity, and capacity is at least one.
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

    // Removed rather than opened and truncated in place: opening for write
    // *follows* a symbolic link, so a `link -> /etc` left by an earlier run would
    // otherwise be written through and its target truncated. Links are removed
    // before being recreated for the same reason.
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
fn restore_links(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for link in &snapshot.links {
        // Restoring over an existing tree must not trip over the old entry, so
        // each arm removes before it creates. Two arms rather than one `match` on
        // a `kind` field, because the whole point of the type is that the two
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

/// Apply permission bits and modification times, files before directories.
fn restore_attributes(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for file in &snapshot.files {
        let path = resolve(root, &file.path, "a file")?;
        file.attrs.apply(&path)?;
    }

    for dir in &snapshot.dirs {
        let path = resolve(root, &dir.path, "a directory")?;
        dir.attrs.apply(&path)?;
    }

    Ok(())
}

/// Remove whatever currently occupies `path`, if anything.
fn remove_existing(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
