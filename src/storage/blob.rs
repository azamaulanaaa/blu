use std::{
    fmt::Debug,
    io::{self, SeekFrom},
    path::PathBuf,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Mutex,
};
use tracing::instrument;

use crate::{
    reader::{self, SliceAsyncReader},
    storage,
};

/// Layout version of a blob key, so a key written by an incompatible build is
/// reported as such rather than misparsed.
const KEY_VERSION: u32 = 1;

/// Where a blob lives inside the store.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct BlobEntry {
    /// Key layout version.
    version: u32,
    /// Byte offset of the blob within the store.
    offset: u64,
    /// Length of the blob in bytes.
    length: u64,
}

/// A storage backend backed by a single append-only file.
///
/// `WRITE` is a const generic rather than two types so that a read-only handle
/// cannot be used to write: the distinction is part of the type, so a restore
/// path that only needs to read cannot accidentally append.
#[derive(Debug)]
pub struct BlobFileStorage<const WRITE: bool> {
    file_path: PathBuf,
    /// Serialises appends, so the offset recorded for a blob is always the
    /// store's length at the time the write began.
    ///
    /// This only excludes other writers in *this* process; writers in other
    /// processes are excluded by the caller holding the backup set's lock.
    write_lock: Mutex<()>,
}

impl<const WRITE: bool> BlobFileStorage<WRITE> {
    /// Prepare a store at `path`, creating its directory if needed.
    ///
    /// For a writable store the file itself is created too, so that a backup
    /// of an empty tree still leaves a store behind.
    #[instrument(skip_all, err)]
    pub async fn new<P: Into<PathBuf> + Debug>(path: P) -> io::Result<Self> {
        let file_path = path.into();

        if let Some(parent) = file_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }

        if WRITE {
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)
                .await?;
        }

        Ok(Self {
            file_path,
            write_lock: Mutex::new(()),
        })
    }

    /// Decode a blob key, rejecting anything that is not one of ours.
    fn decode(key: &str) -> io::Result<BlobEntry> {
        let entry: BlobEntry = serde_json::from_str(key).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid blob key {key:?}: {e}"),
            )
        })?;

        if entry.version != KEY_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "blob key has version {} but this build understands {KEY_VERSION}",
                    entry.version
                ),
            ));
        }

        Ok(entry)
    }
}

#[async_trait]
impl<const WRITE: bool> storage::StorageGet for BlobFileStorage<WRITE> {
    #[instrument(skip_all, err)]
    async fn get(&self, key: &str) -> io::Result<reader::StreamReadSeeker> {
        let entry = Self::decode(key)?;

        let file = fs::File::open(&self.file_path).await?;

        // Reject a key that points outside the store instead of handing back a
        // reader that will quietly stop short.
        let len = file.metadata().await?.len();
        let end = entry.offset.checked_add(entry.length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "blob key range overflows")
        })?;

        if end > len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "blob at {} needs bytes {}..{end} but the store is only {len} bytes long",
                    self.file_path.display(),
                    entry.offset
                ),
            ));
        }

        // No lock is taken: appends never move data that already exists, so a
        // slice handed out now stays valid however much is written after it.
        let mut file = file;
        file.seek(SeekFrom::Start(entry.offset)).await?;

        Ok(Box::new(SliceAsyncReader::new(file, entry.length)))
    }
}

#[async_trait]
impl storage::StoragePut for BlobFileStorage<true> {
    /// Append `len` bytes from `reader` and return a key for them.
    ///
    /// The reader must yield exactly `len` bytes. A short read leaves a
    /// truncated blob that nothing would ever be able to read back whole, and a
    /// long one would silently misalign every later blob's recorded offset, so
    /// both are rejected.
    #[instrument(skip(reader), err)]
    async fn put(&self, mut reader: reader::StreamReadSeeker, len: u64) -> io::Result<String> {
        let _guard = self.write_lock.lock().await;

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .truncate(false)
            .open(&self.file_path)
            .await?;

        let offset = file.metadata().await?.len();

        let written = tokio::io::copy(&mut reader.as_mut().take(len), &mut file).await?;

        if written != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("declared {len} bytes but the reader yielded only {written}"),
            ));
        }

        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing).await? != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the reader yielded more than the declared {len} bytes"),
            ));
        }

        let key = serde_json::to_string(&BlobEntry {
            version: KEY_VERSION,
            offset,
            length: written,
        })
        .map_err(io::Error::other)?;

        tracing::trace!(offset, length = written, "appended blob");

        Ok(key)
    }
}
