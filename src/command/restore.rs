use std::io;

use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{
    metadata::{self, FileChunk},
    reader::{self, StreamReadSeeker},
    storage, writer,
};

/// Reassemble the snapshot identified by `key` under `root`.
///
/// # Errors
///
/// Returns an error if the snapshot cannot be read or decoded, if it refers to
/// a file or chunk it does not contain, or if a destination cannot be written.
pub async fn restore<P: AsRef<std::path::Path>, S: storage::StorageGet>(
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

        let file_path = root.join(&file.path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Opened without truncation: slices are written at their recorded
        // offsets and the final length is asserted by the caller.
        let mut handle = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&file_path)
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
    }

    Ok(())
}
