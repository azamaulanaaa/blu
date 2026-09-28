//! Recording what a directory tree contains, and handing the contents over to be
//! stored.
//!
//! This is the part of a backup that knows about the filesystem: walking, telling
//! a file from a link from a directory, and writing down everything a restore
//! will need to put the tree back. What happens to the file *contents* once they
//! have been read belongs to [`super::pipeline`].

use std::{
    collections::HashMap,
    fmt::Debug,
    fs,
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::instrument;
use walkdir::WalkDir;

use crate::{command::pipeline, fsattr, metadata, reader, storage};

/// Uploads to run at once when the caller has not asked for a specific number.
///
/// Uploads are I/O bound, so oversubscribing the machine a little is what keeps
/// them from stalling on round trips. The cap stops a many-core host from
/// opening an unbounded number of blobs at the store at once, which for a
/// remote backend is a way to get rate limited rather than to go faster.
const DEFAULT_CONCURRENCY: usize = 8;

/// Hard ceiling on [`Options::concurrency`], whatever the caller asks for.
const MAX_CONCURRENCY: usize = 32;

/// Knobs for a single [`backup`] run.
#[derive(Debug, Clone)]
pub struct Options {
    /// Paths to leave out of the backup, compared after normalisation. The
    /// caller should exclude its own output artifacts: backing up a store that
    /// is being appended to while it is read corrupts the run.
    pub exclude: Vec<PathBuf>,
    /// Content-defined chunking parameters.
    pub chunker: reader::ChunkerConfig,
    /// How many chunks to upload at once.
    ///
    /// `1` runs the pipeline strictly one chunk at a time. Higher values overlap
    /// the round trips to the store, and do not change what comes out: chunk
    /// order, and therefore the snapshot, is decided by the order chunks were
    /// *read* in, not by the order they finished uploading.
    ///
    /// Values above 32 are treated as 32, and `0` as `1`, rather than being
    /// rejected — this is a tuning knob, not a correctness setting.
    pub concurrency: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            exclude: Vec::new(),
            chunker: reader::ChunkerConfig {
                min_size: 8 * 1024,
                avg_size: 16 * 1024,
                max_size: 64 * 1024,
            },
            concurrency: default_concurrency(),
        }
    }
}

/// A version that has been committed to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    /// The version number the run was given.
    pub version: u32,
    /// Hash of the snapshot blob, which is how the store names this version.
    pub snapshot: storage::BlobId,
}

/// Uploads to run at once when the caller has not asked for a specific number.
fn default_concurrency() -> usize {
    super::concurrency(DEFAULT_CONCURRENCY)
}

/// Clamp a requested concurrency to something that can only help.
fn effective_concurrency(requested: usize) -> usize {
    requested.clamp(1, MAX_CONCURRENCY)
}

/// What the walk found, before any content was read.
struct Walked {
    /// Snapshot with its directories, links and file records filled in. Chunk
    /// and chunk-slice lists are still empty.
    snapshot: metadata::Snapshot,
    /// Each regular file's index in `snapshot.files`, by relative path.
    file_index: HashMap<PathBuf, u32>,
    /// The regular files' paths, in walk order, for the chunker to read.
    paths: Vec<PathBuf>,
}

/// Snapshot a directory tree and commit it to the store as a new version.
///
/// Deduplication is the store's business: a chunk whose content it already holds
/// is not written again, so an unchanged file costs nothing after the first time
/// it is seen, and so does one that was last seen in any version rather than
/// only the most recent.
///
/// # Errors
///
/// Returns an error if the tree cannot be walked, if a file changes while it is
/// being read, if the store rejects a write, or if the run cannot be committed.
/// A run that returns an error has written no commit record, so the store is
/// left exactly as it was — the space is reclaimed by the next run's recovery.
#[instrument(level = "info", skip(storage, options), err)]
pub async fn backup<P: AsRef<Path> + Debug, S: storage::StoragePut + 'static>(
    root: P,
    options: Options,
    storage: S,
) -> io::Result<Committed> {
    let Options {
        exclude,
        chunker: config,
        concurrency,
    } = options;

    let root = root.as_ref();
    let concurrency = effective_concurrency(concurrency);

    // Walking stats every entry in the tree, which blocks; reading and hashing
    // the contents blocks and burns CPU. Both halves of that are the pipeline's
    // job, run on blocking threads, while the only part that actually waits on
    // anything — storing a chunk — runs concurrently on the async pool.
    //
    // The walk goes first and on its own, because the snapshot it records and
    // the file index the pipeline needs have to exist before the first chunk
    // comes back. It touches metadata only, so it is cheap next to reading the
    // contents that follows it.
    let walk_root = root.to_path_buf();
    let excluded: Vec<PathBuf> = exclude.iter().map(|p| normalise(p)).collect();

    let Walked {
        mut snapshot,
        file_index,
        paths,
    } = tokio::task::spawn_blocking(move || walk(&walk_root, &excluded))
        .await
        .map_err(|join| io::Error::other(format!("the walk thread failed: {join}")))??;

    // Counted before the index is moved into the pipeline, for its opening
    // progress line: the walk knows how many files there are before a single one
    // has been read.
    let files = u64::try_from(file_index.len()).unwrap_or(0);

    let pipeline = pipeline::Pipeline::new(root.to_path_buf(), file_index, storage);

    Arc::clone(&pipeline)
        .run(paths, config, concurrency, files, &mut snapshot)
        .await?;

    // The chunker stats each file when it is registered and reads it again
    // later, so a file that changed size in between would silently shift every
    // subsequent chunk into the wrong file. Checking the reassembled lengths
    // against what the walk recorded turns that into a hard failure.
    verify_file_lengths(&snapshot)?;

    // Infallible: every field of a snapshot is a number, a byte string or a
    // byte array, so there is nothing here that can fail to be represented.
    let buffer = snapshot.to_bytes();

    // The snapshot is a blob like any other, addressed by the hash of its own
    // bytes — which is what the commit record names.
    let id = *blake3::hash(&buffer).as_bytes();

    let len = u64::try_from(buffer.len())
        .map_err(|_| io::Error::other("the snapshot is too large to store"))?;

    pipeline
        .storage
        .put(&id, Box::new(Cursor::new(buffer)), len)
        .await?;

    // The run's one data sync. Everything this run wrote — every chunk and the
    // snapshot — is on the device before the commit record exists, which is what
    // makes the commit safe to make durable. See `storage::blob` for why the
    // commit is a second sync rather than part of this one.
    pipeline.storage.sync_data().await?;

    let version = pipeline.storage.commit(&id).await?;

    Ok(Committed {
        version,
        snapshot: id,
    })
}

/// Walk the tree, recording everything that is not file content.
fn walk(root: &Path, excluded: &[PathBuf]) -> io::Result<Walked> {
    let mut snapshot = metadata::Snapshot::new();
    let mut inode_map: HashMap<FileId, PathBuf> = HashMap::new();

    let entries = WalkDir::new(root)
        .sort_by_file_name()
        .into_iter()
        .map(|entry| {
            entry
                .map(walkdir::DirEntry::into_path)
                .map_err(|e| io::Error::other(format!("failed to walk backup root: {e}")))
        })
        .collect::<io::Result<Vec<_>>>()?;

    let paths = entries
        .into_iter()
        .filter(|path| !excluded.contains(&normalise(path)))
        .map(|path| {
            let meta = fs::symlink_metadata(&path)?;

            let relative_path = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_path_buf();

            // The backup root itself is the destination on restore, so it is
            // never recorded as an entry of its own.
            if relative_path.as_os_str().is_empty() && meta.is_dir() {
                return Ok(None);
            }

            // A symlink is recorded as a link and never followed, including one
            // that resolves to a directory.
            if meta.file_type().is_symlink() {
                snapshot.links.push(metadata::Link {
                    path: relative_path,
                    target: path.read_link()?,
                    kind: metadata::LinkKind::Symbolic,
                });
                return Ok(None);
            }

            // Directories are recorded so that empty ones and their permissions
            // survive a restore.
            if meta.is_dir() {
                snapshot.dirs.push(metadata::Dir {
                    path: relative_path,
                    mode: fsattr::mode_of(&meta),
                    mtime: fsattr::mtime_of(&meta),
                });
                return Ok(None);
            }

            // Anything that is not a regular file and not one of the three cases
            // above is a named pipe, a socket or a device node. Reading one is not
            // slow, it never returns: opening a FIFO for reading blocks until a
            // writer appears, so a tree containing one would hang the run
            // forever — and, because this binary handles SIGTERM to release its
            // claims, the signal would be caught by a handler that could not
            // unwind a thread parked in `read`.
            //
            // So they are skipped, and said so, because a file in the tree that
            // the backup does not contain is exactly the thing a user needs to
            // know rather than discover.
            if !meta.is_file() {
                tracing::info!(
                    path = %relative_path.display(),
                    "skipping a named pipe, socket or device node",
                );
                return Ok(None);
            }

            // The first path to reference an inode carries the content; every
            // later path sharing it becomes a hard link to that one.
            if let Some(file_id) = FileId::from_metadata(&meta) {
                if let Some(first) = inode_map.get(&file_id) {
                    snapshot.links.push(metadata::Link {
                        path: relative_path,
                        target: first.clone(),
                        kind: metadata::LinkKind::Hard,
                    });

                    return Ok(None);
                }

                inode_map.insert(file_id, relative_path.clone());
            }

            snapshot.files.push(metadata::File {
                path: relative_path,
                len: meta.len(),
                mode: fsattr::mode_of(&meta),
                mtime: fsattr::mtime_of(&meta),
            });

            Ok(Some(path))
        })
        .filter_map(Result::transpose)
        .collect::<io::Result<Vec<_>>>()?;

    // Indexed by relative path so a chunk source resolves in constant time
    // instead of relying on the chunker emitting files in order.
    let file_index = snapshot
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| Ok((file.path.clone(), index_to_u32(index)?)))
        .collect::<io::Result<_>>()?;

    Ok(Walked {
        snapshot,
        file_index,
        paths,
    })
}

/// Best-effort normalisation used only for comparing excluded paths.
fn normalise(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Narrow a collection index to the `u32` the snapshot format stores.
///
/// Wrapping instead of failing would point a file or chunk reference at a
/// different entry, so the limit is reported rather than crossed. At the default
/// chunk size `u32::MAX` chunks is a quarter of a petabyte of data, and
/// `u32::MAX` files is unreachable in practice.
pub(super) fn index_to_u32(index: usize) -> io::Result<u32> {
    u32::try_from(index).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the tree needs more than {} entries", u32::MAX),
        )
    })
}

/// Assert that every file's chunk slices add up to the length recorded during
/// the walk, and that no slice claims space outside its file.
fn verify_file_lengths(snapshot: &metadata::Snapshot) -> io::Result<()> {
    let mut totals = vec![0u64; snapshot.files.len()];
    let mut extents: Vec<Option<(u64, u64)>> = vec![None; snapshot.files.len()];

    for slice in &snapshot.file_chunks {
        let index = slice.file_index as usize;
        let file = snapshot.files.get(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk references file {index}, which the snapshot does not contain"),
            )
        })?;

        let end = slice
            .file_offset
            .checked_add(slice.length.into())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk range overflows in {}", file.path.display()),
                )
            })?;

        totals[index] += u64::from(slice.length);

        let extent = extents[index].get_or_insert((slice.file_offset, end));
        extent.0 = extent.0.min(slice.file_offset);
        extent.1 = extent.1.max(end);
    }

    for (index, file) in snapshot.files.iter().enumerate() {
        let covered = extents[index].map_or(0, |(start, end)| end - start);

        if totals[index] != file.len || covered != file.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} changed while it was being backed up: recorded {} bytes, chunked {} bytes covering {covered} bytes",
                    file.path.display(),
                    file.len,
                    totals[index]
                ),
            ));
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct FileId {
    volume_id: u64,
    file_index: u64,
}

impl FileId {
    #[cfg(not(any(unix, windows)))]
    fn from_metadata(_meta: &fs::Metadata) -> Option<FileId> {
        None
    }

    // The signature is uniform across platforms even though the unix
    // implementation below always succeeds: the other `cfg` variants genuinely
    // return `None`, and callers should not have to care which one they got.
    #[cfg(unix)]
    #[allow(clippy::unnecessary_wraps)]
    fn from_metadata(meta: &fs::Metadata) -> Option<FileId> {
        use std::os::unix::fs::MetadataExt;
        Some(FileId {
            volume_id: meta.dev(),
            file_index: meta.ino(),
        })
    }

    #[cfg(all(windows, feature = "experimental"))]
    fn from_metadata(meta: &fs::Metadata) -> Option<FileId> {
        use std::os::windows::fs::MetadataExt;
        Some(FileId {
            volume_id: meta.volume_serial_number()?.into(),
            file_index: meta.file_index()?,
        })
    }

    #[cfg(all(windows, not(feature = "experimental")))]
    fn from_metadata(_meta: &fs::Metadata) -> Option<FileId> {
        None
    }
}
