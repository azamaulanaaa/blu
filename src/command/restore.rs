use std::{
    fs, io,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use tokio::{
    fs as fs_async,
    io::{AsyncReadExt, AsyncSeekExt},
    task::JoinSet,
};
use tracing::instrument;

use crate::{
    fsattr,
    metadata::{self, FileChunk, Snapshot},
    progress::Progress,
    slice,
    storage::{self, StreamReadSeeker},
};

/// Files to write at once.
///
/// Restore is almost entirely waiting — on the store, and on the disk it is
/// writing to — so several files in flight is what keeps either from sitting
/// idle. The cap is what keeps a tree of a million small files from opening a
/// million of them at once.
const RESTORE_CONCURRENCY: usize = 8;

/// How many files to write at once.
fn restore_concurrency() -> usize {
    std::thread::available_parallelism().map_or(RESTORE_CONCURRENCY, |n| {
        n.get().clamp(1, RESTORE_CONCURRENCY)
    })
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
    key: String,
    storage: S,
) -> io::Result<()> {
    let snapshot = {
        let mut reader: StreamReadSeeker = storage.get(&key).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        metadata::Snapshot::from_bytes(buffer.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    };

    let root = root.as_ref();
    let storage = Arc::new(storage);

    // Nothing below can detect damaged chunk data on its own — a short read
    // would just leave a file quietly filled with the wrong bytes — so the
    // whole snapshot is checked against the hashes recorded at backup time
    // before a single byte is written.
    super::verify::check(&snapshot, Arc::clone(&storage))
        .await
        .into_result()
        .map_err(|e| io::Error::new(e.kind(), format!("refusing to restore {key}: {e}")))?;

    // And the paths it would write are checked here, for the same reason and
    // with the same guarantee: a snapshot is untrusted input, and nothing should
    // be created on disk until it is known to be safe to create it.
    check_paths(&snapshot)
        .map_err(|e| io::Error::new(e.kind(), format!("refusing to restore {key}: {e}")))?;

    restore_directories(root, &snapshot).await?;
    restore_files(root, &snapshot, Arc::clone(&storage)).await?;
    restore_links(root, &snapshot)?;
    restore_attributes(root, &snapshot)?;

    Ok(())
}

/// Resolve a path recorded in a snapshot against the restore root, refusing
/// anything that would land outside it.
///
/// A snapshot is a blob in a file that gets synced, shared and backed up like
/// anything else, so its contents are untrusted input: a tampered or hostile
/// store would otherwise choose what a restore writes and where. Every recorded
/// path goes through here — directories, files, and both halves of a link — and
/// the check is on the *recorded* path, before it is joined, because a `..`
/// buried in the middle is just as effective as one at the front.
///
/// Only `..` and absolute paths are rejected. `.` and repeated separators are
/// harmless and normalise to the same place, so there is no reason to refuse
/// them.
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
/// The verify pass already refuses a damaged store before a single byte is
/// written, and a hostile snapshot deserves the same courtesy: finding out that
/// entry 900,001 of a million escapes the destination after writing the other
/// 900,000 would leave a tree that is neither the old one nor the new one.
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

        // A hard link's target names another entry in the tree, so it is
        // resolved against the root and has to stay inside it.
        //
        // A symbolic link's target does not: it is written into the link
        // verbatim and resolved by whoever follows it later, wherever it
        // happens to point. A tree containing a link to `/etc/passwd` is an
        // ordinary tree, and reproducing it faithfully is the whole job — so
        // refusing those would break restoring real backups to accommodate a
        // threat that does not apply to them.
        if link.kind == metadata::LinkKind::Hard {
            resolve(probe, &link.target, "a hard link target")?;
        }
    }

    Ok(())
}

/// Create every recorded directory, including ones that hold no files.
async fn restore_directories(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for dir in &snapshot.dirs {
        fs_async::create_dir_all(resolve(root, &dir.path, "a directory")?).await?;
    }

    Ok(())
}

/// Create and fill every regular file, a few at a time.
///
/// Files are independent of each other here: hard links are made in a later
/// phase, once every file is at its final size, so nothing about this loop can
/// be observed by another file being written at the same time.
async fn restore_files<S: storage::StorageGet + 'static>(
    root: &Path,
    snapshot: &Snapshot,
    storage: Arc<S>,
) -> io::Result<()> {
    // Group the chunk slices by the file they belong to so that each
    // destination is opened once rather than once per slice.
    let mut slices: Vec<Vec<FileChunk>> = vec![Vec::new(); snapshot.files.len()];
    for file_chunk in &snapshot.file_chunks {
        let index = file_chunk.file_index as usize;
        let bucket = slices.get_mut(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk slice references file {index}, which the snapshot does not contain"),
            )
        })?;
        bucket.push(file_chunk.clone());
    }

    let chunks = Arc::new(snapshot.chunks.clone());
    let concurrency = restore_concurrency();
    let total_files = u64::try_from(snapshot.files.len()).unwrap_or(0);

    // Unlike a backup, the work is known up front: the snapshot says how many
    // files there are, so this one can say how far along it is.
    let mut progress = Progress::new("restore");
    progress.set("files", total_files);
    progress.report();

    let mut writing = JoinSet::new();
    let mut next = 0usize;

    loop {
        // Keep as many files in flight as asked for.
        while next < snapshot.files.len() && writing.len() < concurrency {
            let job = RestoreJob {
                path: resolve(root, &snapshot.files[next].path, "a file")?,
                file: snapshot.files[next].clone(),
                slices: std::mem::take(&mut slices[next]),
                chunks: Arc::clone(&chunks),
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
            // A finished file reports how many bytes it actually wrote, which is
            // what the rate is computed from.
            Some(Ok(Ok(written))) => {
                progress.add("done", 1);
                progress.add("written", written);
                progress.report();
            }
            // An empty set means keep going.
            None => {}
        }
    }

    Ok(())
}

/// One file's worth of work, owned so it can outlive the snapshot it came from.
struct RestoreJob {
    /// Where the file goes, already joined onto the restore root.
    path: PathBuf,
    file: metadata::File,
    slices: Vec<FileChunk>,
    chunks: Arc<Vec<metadata::Chunk>>,
}

/// Create one file at its final length and write its chunk slices into it.
async fn write_file<S: storage::StorageGet>(job: RestoreJob, storage: Arc<S>) -> io::Result<u64> {
    let RestoreJob {
        path,
        file,
        slices,
        chunks,
    } = job;

    // Created before the content, at its final size, so that a file with no
    // chunk slices still comes back and so there is never a moment where the
    // file exists at the wrong length.
    if let Some(parent) = path.parent() {
        fs_async::create_dir_all(parent).await?;
    }

    // Anything already at this path is removed before the file is created,
    // rather than being opened and truncated in place.
    //
    // A restore merges into its destination, so on a second run the path may
    // hold a symbolic link from the first — and opening a file for writing
    // *follows* a symbolic link. A tree that once had `link -> /etc` would end
    // up with the next restore writing through `link` and truncating whatever
    // it pointed at. Links are already removed before they are recreated; this
    // is the same rule applied to files, and it closes the same hole.
    remove_existing(&path)?;

    let mut handle = fs_async::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await?;

    handle.set_len(file.len).await?;

    for file_chunk in slices {
        let chunk = chunks.get(file_chunk.chunk_index as usize).ok_or_else(|| {
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

        let mut source: StreamReadSeeker = storage.get(&chunk.location).await?;
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
                    "chunk at {} is shorter than the {} bytes recorded for {}",
                    chunk.location,
                    length,
                    file.path.display()
                ),
            ));
        }
    }

    // Guard against a gap in the recorded slices, which would otherwise leave a
    // hole of zeroes in the middle of the file.
    handle.set_len(file.len).await?;

    // What the file turned out to be, for the progress line.
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
