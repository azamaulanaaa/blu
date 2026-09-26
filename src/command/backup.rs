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

use crate::{metadata, reader, storage};

enum ChunkStatus {
    Available(metadata::Chunk),
    Reuse(u32),
}

#[instrument(skip(storage), ret, err)]
pub async fn backup<P: AsRef<Path> + Debug, S: storage::StoragePut + storage::StorageGet>(
    root: P,
    base_key: Option<String>,
    storage: S,
    config: reader::ChunkerConfig,
) -> io::Result<String> {
    let mut dedup_cache = match base_key {
        Some(v) => {
            let mut reader = storage.get(&v).await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;
            let snapshot = metadata::Snapshot::from_bytes(buffer.as_slice())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let map: HashMap<[u8; 32], ChunkStatus> = snapshot
                .chunks
                .into_iter()
                .map(|v| (v.hash, ChunkStatus::Available(v)))
                .collect();

            Some(map)
        }
        None => None,
    };

    let mut snapshot = metadata::Snapshot::new();

    let mut inode_map: HashMap<FileId, PathBuf> = HashMap::new();

    let paths = WalkDir::new(&root)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|v| v.map(|v| v.into_path()).ok())
        .collect::<Vec<_>>();

    let paths = paths
        .into_iter()
        .map(|path| {
            if path.is_dir() {
                return Ok(None);
            }

            let meta = fs::symlink_metadata(&path)?;

            let relative_path = path
                .strip_prefix(&root)
                .map_err(|e| io::Error::other(e))?
                .to_path_buf();

            if meta.is_symlink() {
                snapshot.file_symlink.push(metadata::FileSymlink {
                    path: relative_path.clone(),
                    source: path.read_link()?,
                    is_hard: false,
                });
                return Ok(None);
            }

            let is_new_file = FileId::from_metadata(&meta)
                .map(|file_id| {
                    if let Some(existing_relative_path) = inode_map.get(&file_id) {
                        snapshot.file_symlink.push(metadata::FileSymlink {
                            path: relative_path.clone(),
                            source: existing_relative_path.clone(),
                            is_hard: true,
                        });

                        return false;
                    }

                    inode_map.insert(file_id, relative_path.clone());
                    true
                })
                .unwrap_or(true);

            if is_new_file {
                snapshot.files.push(metadata::File {
                    path: relative_path.clone(),
                });
                return Ok(Some(path));
            }

            Ok(None)
        })
        .filter_map(|v| v.transpose())
        .collect::<io::Result<Vec<_>>>()?;

    let chunker = reader::Chunker::new(paths, config)?;

    let mut current_file_index = 0;
    for chunk in chunker {
        let mut chunk = chunk?;

        let buffer = {
            let mut buffer = Vec::new();
            let n = chunk.reader.read_to_end(&mut buffer).await?;
            buffer.truncate(n);

            buffer
        };

        let hash = *blake3::hash(&buffer).as_bytes();

        let chunk_index = {
            let ref_index = match dedup_cache.as_mut() {
                Some(map) => match map.get(&hash) {
                    Some(ChunkStatus::Available(chunk)) => {
                        let index = snapshot.chunks.len() as u32;
                        snapshot.chunks.push(chunk.clone());
                        let _ = map.insert(hash, ChunkStatus::Reuse(index));

                        Some(index)
                    }
                    Some(ChunkStatus::Reuse(index)) => Some(*index),
                    None => None,
                },
                None => None,
            };

            match ref_index {
                Some(index) => index,
                None => {
                    let index = snapshot.chunks.len() as u32;
                    let len = buffer.len() as u64;

                    let key = {
                        let reader = Box::new(Cursor::new(buffer));
                        let key = storage.put(reader, len).await?;

                        key
                    };

                    snapshot.chunks.push(metadata::Chunk {
                        hash,
                        location: key,
                    });

                    index
                }
            }
        };

        let mut chunk_offset = 0;
        for source in chunk.sources {
            let source_rel_path = source
                .path
                .strip_prefix(&root)
                .map_err(|e| io::Error::other(e))?;

            while current_file_index < snapshot.files.len() {
                if snapshot.files[current_file_index].path == source_rel_path {
                    break;
                }
                current_file_index += 1;
            }

            if current_file_index >= snapshot.files.len() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Chunk source path mismatch: Chunker yielded a file not in snapshot",
                ));
            }

            snapshot.file_chunks.push(metadata::FileChunk {
                chunk_index,
                file_index: current_file_index as u32,
                chunk_offset: chunk_offset,
                file_offset: source.offset,
                length: source.length,
            });

            chunk_offset += source.length;
        }
    }

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
