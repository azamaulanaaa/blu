use std::{
    fs, io,
    path::{Path, PathBuf},
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

    restore_directories(root, &snapshot).await?;
    restore_files(root, &snapshot, Arc::clone(&storage)).await?;
    restore_links(root, &snapshot)?;
    restore_attributes(root, &snapshot)?;

    Ok(())
}

/// Create every recorded directory, including ones that hold no files.
async fn restore_directories(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for dir in &snapshot.dirs {
        fs_async::create_dir_all(root.join(&dir.path)).await?;
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

    let mut writing = JoinSet::new();
    let mut next = 0usize;

    loop {
        // Keep as many files in flight as asked for.
        while next < snapshot.files.len() && writing.len() < concurrency {
            let job = RestoreJob {
                path: root.join(&snapshot.files[next].path),
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
            // A file that finished, or an empty set, both mean keep going.
            _ => {}
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
async fn write_file<S: storage::StorageGet>(job: RestoreJob, storage: Arc<S>) -> io::Result<()> {
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

    Ok(())
}

/// Recreate hard links and symbolic links.
fn restore_links(root: &Path, snapshot: &Snapshot) -> io::Result<()> {
    for link in &snapshot.links {
        let path = root.join(&link.path);

        // Restoring over an existing tree must not trip over the old entry.
        remove_existing(&path)?;

        match link.kind {
            metadata::LinkKind::Hard => {
                let target = root.join(&link.target);
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
        let path = root.join(&file.path);
        apply(&path, file.mode, file.mtime)?;
    }

    for dir in &snapshot.dirs {
        let path = root.join(&dir.path);
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
