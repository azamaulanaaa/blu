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
    progress::Progress,
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
                format!(
                    "refusing to restore snapshot {}: {e}",
                    storage::Hex(&snapshot_id)
                ),
            )
        })?;

    // And the paths it would write, for the same reason: a snapshot is untrusted
    // input, and nothing goes on disk until it is known to be safe there.
    check_paths(&snapshot).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "refusing to restore snapshot {}: {e}",
                storage::Hex(&snapshot_id)
            ),
        )
    })?;

    restore_directories(root, &snapshot).await?;
    let written = restore_files(root, Arc::clone(&snapshot), Arc::clone(&storage)).await?;
    restore_links(root, &snapshot)?;
    restore_attributes(root, &snapshot)?;

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
fn resolve(root: &Path, recorded: &Path, kind: &str) -> io::Result<PathBuf> {
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
fn check_paths(snapshot: &Snapshot) -> io::Result<()> {
    let probe = Path::new(".");

    for dir in &snapshot.dirs {
        resolve(probe, &dir.path, "a directory")?;
    }

    for file in &snapshot.files {
        resolve(probe, &file.path, "a file")?;
    }

    for link in &snapshot.links {
        resolve(probe, &link.path, "a link")?;

        // A hard link's target names another entry in the tree, so it is resolved
        // against the root and has to stay inside it. A symbolic link's target
        // does not: it is written verbatim and resolved by whoever follows it.
        // A tree containing a link to `/etc/passwd` is an ordinary tree, and
        // reproducing it faithfully is the job.
        if link.kind == metadata::LinkKind::Hard {
            resolve(probe, &link.target, "a hard link target")?;
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
#[derive(Debug)]
pub struct DestinationLock {
    path: PathBuf,
    /// Sibling directories this run may have created, removed on the way out.
    scratch: Vec<PathBuf>,
    /// Held open for the lifetime of the claim; dropping the `Lock` drops this.
    _file: fs::File,
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

    // Registered before the claim is taken, so nothing can create them without
    // also being covered by the cleanup.
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
        scratch,
        _file: file,
    })
}

/// Create every recorded directory, including ones that hold no files.
async fn restore_directories(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for dir in &snapshot.dirs {
        fs_async::create_dir_all(resolve(root, &dir.path, "a directory")?).await?;
    }

    Ok(())
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
    let mut progress = Progress::new("restore");
    progress.set("files", total_files);
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

        if writing.is_empty() {
            if next >= snapshot.files.len() {
                break;
            }
            continue;
        }

        // The first failure ends the run. Files already written stay written:
        // a restore is not atomic, and pretending otherwise would be a bigger
        // change than making the loop concurrent.
        match writing.join_next().await {
            Some(Ok(Err(e))) => return Err(e),
            Some(Err(join)) => {
                return Err(io::Error::other(format!("a restore task failed: {join}")));
            }
            // A finished file reports the bytes it actually wrote, which is what
            // the rate is computed from.
            Some(Ok(Ok(file_bytes))) => {
                progress.add("done", 1);
                progress.add("written", file_bytes);
                progress.report();
                written = written.saturating_add(file_bytes);
            }
            None => {}
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
                    storage::Hex(chunk),
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
        let path = resolve(root, &link.path, "a link")?;

        // Restoring over an existing tree must not trip over the old entry.
        remove_existing(&path)?;

        match link.kind {
            metadata::LinkKind::Hard => {
                let target = resolve(root, &link.target, "a hard link target")?;
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
            metadata::LinkKind::Symbolic => {
                let target_is_dir = symlink_points_at_directory(&path, &link.target);
                fsattr::symlink(&link.target, &path, target_is_dir).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "failed to create symlink {} -> {}: {e}",
                            path.display(),
                            link.target.display()
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
        apply(&path, file.mode, file.mtime)?;
    }

    for dir in &snapshot.dirs {
        let path = resolve(root, &dir.path, "a directory")?;
        apply(&path, dir.mode, dir.mtime)?;
    }

    Ok(())
}

/// Apply one entry's recorded mode and modification time.
fn apply(path: &Path, mode: u32, mtime: Option<metadata::Timestamp>) -> io::Result<()> {
    fsattr::set_mode(path, mode)?;
    if let Some(mtime) = mtime {
        fsattr::set_mtime(path, mtime)?;
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

/// Resolve a link target the way the link itself would, and report whether it
/// names a directory. Only Windows needs this to pick a symlink flavour.
fn symlink_points_at_directory(link: &Path, target: &Path) -> bool {
    if target.is_absolute() {
        return target.is_dir();
    }

    match link.parent() {
        Some(parent) => parent.join(target).is_dir(),
        None => target.is_dir(),
    }
}
