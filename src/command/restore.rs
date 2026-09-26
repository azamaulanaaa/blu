use std::{
    fs,
    io,
    path::Path,
};

use tokio::{
    fs as fs_async,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::instrument;

use crate::{
    fsattr,
    metadata::{self, FileChunk, Snapshot},
    reader::{self, StreamReadSeeker},
    storage, writer,
};

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
#[instrument(skip(storage, root), ret, err)]
pub async fn restore<P: AsRef<Path>, S: storage::StorageGet>(
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

    // Nothing below can detect damaged chunk data on its own — a short read
    // would just leave a file quietly filled with the wrong bytes — so the
    // whole snapshot is checked against the hashes recorded at backup time
    // before a single byte is written.
    super::verify::check(&snapshot, &storage)
        .await
        .into_result()
        .map_err(|e| {
            io::Error::new(e.kind(), format!("refusing to restore {key}: {e}"))
        })?;

    restore_directories(root, &snapshot).await?;
    restore_files(root, &snapshot, &storage).await?;
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

/// Create every regular file at its final size, then fill in the content.
async fn restore_files<S: storage::StorageGet>(
    root: &Path,
    snapshot: &Snapshot,
    storage: &S,
) -> io::Result<()> {
    // Create every file up front so that hard links have something to point
    // at, and so that a file with no chunk slices still comes back.
    for file in &snapshot.files {
        let path = root.join(&file.path);

        if let Some(parent) = path.parent() {
            fs_async::create_dir_all(parent).await?;
        }

        let handle = fs_async::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;

        handle.set_len(file.len).await?;
    }

    // Group the chunk slices by the file they belong to so that each
    // destination is opened once rather than once per slice.
    let mut slices: Vec<Vec<&FileChunk>> = vec![Vec::new(); snapshot.files.len()];
    for file_chunk in &snapshot.file_chunks {
        let index = file_chunk.file_index as usize;
        let bucket = slices.get_mut(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunk slice references file {index}, which the snapshot does not contain"
                ),
            )
        })?;
        bucket.push(file_chunk);
    }

    for (index, file) in snapshot.files.iter().enumerate() {
        if slices[index].is_empty() {
            continue;
        }

        let mut handle = fs_async::OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(root.join(&file.path))
            .await?;

        for file_chunk in slices[index].iter().copied() {
            let chunk = snapshot.chunks.get(file_chunk.chunk_index as usize).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "chunk slice references chunk {}, which the snapshot does not contain",
                        file_chunk.chunk_index
                    ),
                )
            })?;

            let length = u64::from(file_chunk.length);

            handle.seek(io::SeekFrom::Start(file_chunk.file_offset)).await?;

            let mut source: StreamReadSeeker = storage.get(&chunk.location).await?;
            source
                .seek(io::SeekFrom::Start(file_chunk.chunk_offset.into()))
                .await?;

            let mut source = reader::SliceAsyncReader::new(source, length);
            let mut sink = writer::SliceAsyncWriter::new(&mut handle, length);

            let written = tokio::io::copy(&mut source, &mut sink).await?;

            if written != length {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "chunk at {} is shorter than the {} bytes recorded for {}",
                        chunk.location, length, file.path.display()
                    ),
                ));
            }
        }

        // Guard against a gap in the recorded slices, which would otherwise
        // leave a hole of zeroes in the middle of the file.
        handle.set_len(file.len).await?;
    }

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
                        format!("failed to link {} to {}: {e}", path.display(), target.display()),
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
