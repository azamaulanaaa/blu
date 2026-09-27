use std::{
    fmt::Debug,
    io::{self, SeekFrom},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};
use tracing::instrument;

use crate::{
    slice::SliceAsyncReader,
    storage::{self, StreamReadSeeker},
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
    /// First byte of the store that has not been handed out to a blob yet.
    ///
    /// This is a reservation counter rather than the file's length, and the
    /// difference is the whole design. A writer claims its range here and only
    /// then starts copying into it, so any number of writers can be copying at
    /// once. Were the end taken from the file instead, two writers would both
    /// read the same length while the first was still writing and would pick the
    /// same offset — which is why this used to hold a mutex across the entire
    /// copy, and why every upload was serialised behind the last.
    ///
    /// It is seeded from the file's length when the store is opened and only
    /// moves forward, so it is never behind the bytes actually in the file. When
    /// a copy fails, its claimed range is abandoned rather than reclaimed: the
    /// bytes are never referenced by any key, and reclaiming them would mean
    /// truncating the file, which is exactly what a concurrent writer past that
    /// point has already promised not to happen.
    ///
    /// Only used when `WRITE` is set.
    reserved: AtomicU64,
}

impl<const WRITE: bool> BlobFileStorage<WRITE> {
    /// Prepare a store at `path`, creating its directory if needed.
    ///
    /// For a writable store the file itself is created too, so that a backup
    /// of an empty tree still leaves a store behind.
    ///
    /// # Errors
    ///
    /// Returns an error if the containing directory or the store cannot be
    /// created, for instance because the path is a directory or the caller
    /// lacks permission.
    #[instrument(skip_all, err)]
    pub async fn new<P: Into<PathBuf> + Debug>(path: P) -> io::Result<Self> {
        let file_path = path.into();

        if let Some(parent) = file_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }

        let mut reserved = 0;
        if WRITE {
            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)
                .await?;
            reserved = file.metadata().await?.len();
        }

        Ok(Self {
            file_path,
            reserved: AtomicU64::new(reserved),
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
    async fn get(&self, key: &str) -> io::Result<StreamReadSeeker> {
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
    /// Store `len` bytes from `reader` and return a key for them.
    ///
    /// The reader must yield exactly `len` bytes. A short read leaves a
    /// truncated blob that nothing would ever be able to read back whole, and a
    /// long one would mean the store and the reader disagree about the size, so
    /// both are rejected.
    ///
    /// Only the claim on the range is serialised. The copy itself is not, and
    /// that is the point: this used to hold a mutex from before the first read
    /// until after the last, which made the number of concurrent uploads a lie.
    async fn put(&self, mut reader: StreamReadSeeker, len: u64) -> io::Result<String> {
        // Claim the range, then let go. `fetch_add` hands out a disjoint range
        // per caller, so it needs no lock and no ordering beyond its own
        // atomicity: the bytes go to a region nobody else has been given, and
        // the key is returned by the same task that wrote them.
        let offset = self.reserved.fetch_add(len, Ordering::Relaxed);

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.file_path)
            .await?;

        // Positional, never append. `append` would redirect every write to
        // whatever the end of the file happened to be, which is the offset we
        // just decided not to use.
        file.seek(SeekFrom::Start(offset)).await?;

        let mut source = BufReader::with_capacity(256 * 1024, reader.as_mut().take(len));
        let written = tokio::io::copy_buf(&mut source, &mut file).await?;

        if written != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("declared {len} bytes but the reader yielded only {written}"),
            ));
        }

        // `copy` returning is not the bytes having reached the file: the last
        // few kilobytes can still be in the writer's buffer. The key is about
        // to be recorded, so flush before it exists rather than trust what drop
        // happens to do.
        file.flush().await?;

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

        tracing::trace!(offset, length = written, "stored blob");

        Ok(key)
    }
}
