use std::{io, path::Path};

use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{
    metadata,
    reader::{self, StreamReadSeeker},
    storage, writer,
};

pub async fn restore<P: AsRef<Path>, S: storage::StorageGet>(
    root: P,
    key: String,
    storage: S,
) -> io::Result<()> {
    let snapshot = {
        let mut reader: StreamReadSeeker = storage.get(&key).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        let snapshot = metadata::Snapshot::from_bytes(buffer.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        snapshot
    };

    let root = root.as_ref();

    for file_chunk in snapshot.file_chunks.iter() {
        let file = snapshot
            .files
            .get(file_chunk.file_index as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file metadata not found for given file chunk",
                )
            })?;
        let chunk = snapshot
            .chunks
            .get(file_chunk.chunk_index as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk metadata not found for given file chunk",
                )
            })?;

        let mut file = {
            let file_path = root.join(&file.path);

            let parent = &file_path
                .parent()
                .ok_or_else(|| io::Error::other("internal error, file parent not found"))?;
            fs::create_dir_all(parent).await?;

            let mut file: fs::File = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .await?;

            file.seek(io::SeekFrom::Start(file_chunk.file_offset))
                .await?;

            writer::SliceAsyncWriter::new(file, file_chunk.length.into())
        };

        let mut chunk = {
            let mut chunk: StreamReadSeeker = storage.get(&chunk.location).await?;
            chunk
                .seek(io::SeekFrom::Start(file_chunk.chunk_offset.into()))
                .await?;

            reader::SliceAsyncReader::new(chunk, file_chunk.length.into())
        };

        tokio::io::copy(&mut chunk, &mut file).await?;
    }

    Ok(())
}
