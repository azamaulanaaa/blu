use std::{
    collections::HashMap,
    fmt::Debug,
    fs,
    io::{self, Cursor},
    path::{Path, PathBuf},
};

use tokio::io::AsyncReadExt;
use tracing::instrument;
use walkdir::WalkDir;

use crate::{fsattr, metadata, reader, storage};

/// Knobs for a single [`backup`] run.
#[derive(Debug, Clone)]
pub struct Options {
    /// Key of a previous snapshot whose chunks may be reused instead of being
    /// uploaded again. Deduplication against this snapshot only.
    pub base_key: Option<String>,
    /// Paths to leave out of the backup, compared after normalisation. The
    /// caller should exclude its own output artifacts: backing up a blob store
    /// that is being appended to while it is read corrupts the chunk offsets.
    pub exclude: Vec<PathBuf>,
    /// Content-defined chunking parameters.
    pub chunker: reader::ChunkerConfig,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            base_key: None,
            exclude: Vec::new(),
            chunker: reader::ChunkerConfig {
                min_size: 8 * 1024,
                avg_size: 16 * 1024,
                max_size: 64 * 1024,
            },
        }
    }
}

/// Snapshot a directory tree and return the storage key of the snapshot.
///
/// The returned snapshot can be passed back as [`Options::base_key`] on a later
/// run so that unchanged chunks are not uploaded again.
///
/// # Errors
///
/// Returns an error if the tree cannot be walked, if a file changes while it is
/// being read, or if the storage backend rejects a write.
#[instrument(level = "info", skip(storage, options), err)]
pub async fn backup<P: AsRef<Path> + Debug, S: storage::StoragePut + storage::StorageGet>(
    root: P,
    options: Options,
    storage: S,
) -> io::Result<String> {
    let Options {
        base_key,
        exclude,
        chunker: config,
    } = options;

    let base_chunks: HashMap<[u8; 32], metadata::Chunk> = match base_key {
        Some(v) => {
            let mut reader = storage.get(&v).await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;
            let snapshot = metadata::Snapshot::from_bytes(buffer.as_slice())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            snapshot
                .chunks
                .into_iter()
                .map(|chunk| (chunk.hash, chunk))
                .collect()
        }
        None => HashMap::new(),
    };

    // Chunks already emitted by this run, so repeated content is only ever
    // uploaded once regardless of whether a base snapshot was supplied.
    let mut emitted: HashMap<[u8; 32], u32> = HashMap::new();

    let mut snapshot = metadata::Snapshot::new();

    let mut inode_map: HashMap<FileId, PathBuf> = HashMap::new();

    // Normalise once so exclusion works regardless of how the caller spelled
    // the paths.
    let excluded: Vec<PathBuf> = exclude.iter().map(|p| normalise(p)).collect();

    let paths = WalkDir::new(&root)
        .sort_by_file_name()
        .into_iter()
        .map(|entry| {
            entry
                .map(walkdir::DirEntry::into_path)
                .map_err(|e| io::Error::other(format!("failed to walk backup root: {e}")))
        })
        .collect::<io::Result<Vec<_>>>()?;

    let paths = paths
        .into_iter()
        .filter(|path| !excluded.contains(&normalise(path)))
        .map(|path| {
            let meta = fs::symlink_metadata(&path)?;

            let relative_path = path
                .strip_prefix(&root)
                .map_err(|e| io::Error::other(e))?
                .to_path_buf();

            // The backup root itself is the destination on restore, so it is
            // never recorded as an entry of its own.
            if relative_path.as_os_str().is_empty() && meta.is_dir() {
                return Ok(None);
            }

            // A symlink is recorded as a link and never followed, including
            // one that resolves to a directory.
            if meta.file_type().is_symlink() {
                snapshot.links.push(metadata::Link {
                    path: relative_path,
                    target: path.read_link()?,
                    kind: metadata::LinkKind::Symbolic,
                });
                return Ok(None);
            }

            // Directories are recorded so that empty ones and their
            // permissions survive a restore.
            if meta.is_dir() {
                snapshot.dirs.push(metadata::Dir {
                    path: relative_path,
                    mode: fsattr::mode_of(&meta),
                    mtime: fsattr::mtime_of(&meta),
                });
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
        .filter_map(|v| v.transpose())
        .collect::<io::Result<Vec<_>>>()?;

    // Index the files by relative path so chunk sources resolve in constant
    // time instead of relying on the chunker emitting files in order.
    let file_index: HashMap<&Path, u32> = snapshot
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| (file.path.as_path(), index as u32))
        .collect();

    let chunker = reader::Chunker::new(paths, config)?;

    for chunk in chunker {
        let mut chunk = chunk?;

        let buffer = {
            let mut buffer = Vec::new();
            chunk.reader.read_to_end(&mut buffer).await?;
            buffer
        };

        let hash = *blake3::hash(&buffer).as_bytes();

        let chunk_index = match emitted.get(&hash) {
            // Already emitted by this run, or carried over from the base
            // snapshot on first sight.
            Some(&index) => index,
            None => {
                let chunk = match base_chunks.get(&hash) {
                    Some(chunk) => chunk.clone(),
                    None => {
                        let len = buffer.len() as u64;
                        let key = storage
                            .put(Box::new(Cursor::new(buffer)), len)
                            .await?;

                        metadata::Chunk { hash, location: key }
                    }
                };

                let index = snapshot.chunks.len() as u32;
                snapshot.chunks.push(chunk);
                emitted.insert(hash, index);

                index
            }
        };

        let mut chunk_offset = 0;
        for source in chunk.sources {
            let source_rel_path = source
                .path
                .strip_prefix(&root)
                .map_err(|e| io::Error::other(e))?;

            let Some(&file_index) = file_index.get(source_rel_path) else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "chunk source {} is not a file in the snapshot",
                        source.path.display()
                    ),
                ));
            };

            snapshot.file_chunks.push(metadata::FileChunk {
                chunk_index,
                file_index,
                chunk_offset,
                file_offset: source.offset,
                length: source.length,
            });

            chunk_offset += source.length;
        }
    }

    // The chunker stats each file when it is registered and reads it again
    // later, so a file that changed size in between would silently shift every
    // subsequent chunk into the wrong file. Checking the reassembled lengths
    // against what the walk recorded turns that into a hard failure.
    verify_file_lengths(&snapshot)?;

    let key = {
        let buffer = snapshot
            .to_bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let len = buffer.len() as u64;
        let reader = Box::new(Cursor::new(buffer));
        let key = storage.put(reader, len).await?;

        key
    };

    Ok(key)
}

/// Best-effort normalisation used only for comparing excluded paths.
fn normalise(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
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

    #[cfg(unix)]
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
