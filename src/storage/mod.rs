//! Where chunk and snapshot data is kept.
//!
//! A backend is split into reading and writing so that a restore, which only
//! ever reads, cannot be handed a handle that can append to the store.
//!
//! Blobs are addressed by the BLAKE3 hash of their contents, and the backend is
//! responsible for deduplicating them: a `put` of content the store already
//! holds writes nothing and returns success. That is what lets a backup
//! deduplicate against every version the store has ever held rather than only
//! against the one before it, and it is why the caller does not have to know
//! what is already there.

use std::io;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek};

pub use blob::BlobStore;
pub use locate::{Resolved, resolve};
pub use plan::{Plan, Planned};

pub use version_index::VersionIndex;

pub use record::Codec;

mod blob;
mod locate;
mod plan;
mod record;
mod version_index;

/// The first `N` bytes of a BLAKE3 hash, for a checksum over fixed-size bytes.
///
/// The record header and the version index file both use it.
pub(super) fn truncated_hash<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    out.copy_from_slice(&blake3::hash(bytes).as_bytes()[..N]);
    out
}

/// Where a blob's payload sits in a store.
///
/// A location in this file, an object key there: an implementation detail of
/// whichever backend holds the blob.
pub use record::Placement;

/// Address of a blob: the BLAKE3 hash of its contents.
///
/// Both a chunk's identity in a snapshot and its key in the store's index.
pub type BlobId = [u8; 32];

/// How large a store segment may get before a new one is started.
///
/// A write policy, not part of the format: a set written under one limit reads
/// back under any other, because no record depends on where a segment boundary
/// fell. A reader is given a limit only so one value can be passed to both
/// constructors, and ignores it.
///
/// The default is 4 GiB, under what FAT32 and several older archive formats
/// refuse, and small enough that no single file needs 64-bit offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentLimit(u64);

impl SegmentLimit {
    /// A limit of `bytes`.
    ///
    /// Clamped to a floor of one store header plus one record, because a segment
    /// that cannot hold its own header cannot be a segment.
    #[must_use]
    pub fn new(bytes: u64) -> Self {
        let floor = u64::try_from(record::STORE_HEADER).unwrap_or(u64::MAX) + MIN_RECORD;
        Self(bytes.max(floor))
    }

    /// The limit in bytes.
    #[must_use]
    pub fn bytes(self) -> u64 {
        self.0
    }
}

impl Default for SegmentLimit {
    fn default() -> Self {
        Self(4 * 1024 * 1024 * 1024)
    }
}

/// How record payloads are compressed on the way in.
///
/// A write policy, not a format property: every record names the codec it was
/// written with and hashes the content rather than the stored bytes, so a set
/// written at mixed levels — or with compression off — reads back and
/// deduplicates correctly. The default is [`Compression::Zstd`] at level 1; the
/// measurements behind that are in the README.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Store payloads as they are. What [`Compression::from_level`] returns for 0.
    Off,
    /// Compress payloads as zstd frames at `level`.
    Zstd {
        /// The zstd compression level, 1 (fastest, default) to 9 (smallest).
        level: u8,
    },
}

/// The highest level offered. zstd goes further; see [`Compression`].
pub const MAX_COMPRESSION_LEVEL: u8 = 9;

impl Default for Compression {
    fn default() -> Self {
        Self::Zstd { level: 1 }
    }
}

impl Compression {
    /// The policy for a level the way a user counts them: 0 stores payloads as they
    /// are, and 1 to 9 are zstd levels.
    ///
    /// Out-of-range values are clamped rather than rejected, so a bad number gives
    /// a working policy rather than a panic. A command line rejects them instead,
    /// where the user can be told.
    #[must_use]
    pub fn from_level(level: u8) -> Self {
        match level {
            0 => Self::Off,
            _ => Self::Zstd {
                level: level.clamp(1, MAX_COMPRESSION_LEVEL),
            },
        }
    }

    /// zstd at `level`, clamped to 1..=[`MAX_COMPRESSION_LEVEL`].
    #[must_use]
    pub fn zstd(level: u8) -> Self {
        Self::Zstd {
            level: level.clamp(1, MAX_COMPRESSION_LEVEL),
        }
    }

    /// The zstd level, or `None` when compression is off.
    #[must_use]
    pub fn level(self) -> Option<u8> {
        match self {
            Self::Off => None,
            Self::Zstd { level } => Some(level),
        }
    }

    /// Compress `raw`, returning the codec to record and the bytes to store.
    ///
    /// A payload becomes a zstd frame only when that frame is strictly smaller
    /// than the input; otherwise [`Codec::Raw`] and the bytes unchanged, so
    /// already-compressed content never grows by a frame header.
    ///
    /// The return type is a [`Vec`], so the fallback paths copy.
    pub(super) fn compress(self, raw: &[u8]) -> (Codec, Vec<u8>) {
        let Some(level) = self.level() else {
            return (Codec::Raw, raw.to_vec());
        };

        // A compressor failure is not an error: the fallback stores the bytes as
        // they are, which is a correct record and beats failing a backup.
        let Ok(encoded) = zstd::bulk::compress(raw, i32::from(level)) else {
            return (Codec::Raw, raw.to_vec());
        };

        if encoded.len() >= raw.len() {
            return (Codec::Raw, raw.to_vec());
        }

        (Codec::Zstd, encoded)
    }
}

/// The smallest record that can be written: a header and an empty payload.
const MIN_RECORD: u64 = 6 + 2 + 32 + 8;

/// A [`BlobId`] in lowercase hex, for messages a person can read off a screen
/// and compare against a `blake3` sum they ran themselves.
#[must_use]
pub fn hex(id: &BlobId) -> String {
    use std::fmt::Write as _;

    id.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// A stream that is both [`AsyncRead`] and [`AsyncSeek`].
///
/// Blanket-implemented, never by hand: use [`StreamReadSeeker`].
pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

/// A boxed, seekable async stream, as handed out by a storage backend.
///
/// Seekable because a blob is a region of a larger store: reading one means
/// seeking to where it starts rather than reading the store from the beginning.
pub type StreamReadSeeker = Box<dyn AsyncReadSeek + Unpin + Send>;

/// Reading blobs back out of a store.
#[async_trait]
pub trait StorageGet: Send + Sync {
    /// Open the blob whose contents hash to `id`.
    ///
    /// The returned stream is bounded to the blob, so a blob cannot be used to
    /// read past itself. Implementations must reject an `id` the store does not
    /// hold rather than handing back a short read.
    ///
    /// # Errors
    ///
    /// Returns an error if the store does not hold that blob, or if it holds it
    /// but the bytes cannot be read.
    async fn get(&self, id: &BlobId) -> io::Result<StreamReadSeeker>;
}

/// Appending blobs to a store, and publishing what has been appended.
#[async_trait]
pub trait StoragePut: Send + Sync {
    /// Store `len` bytes read from `reader`, whose contents hash to `id`.
    ///
    /// A blob the store already holds is not written again.
    ///
    /// `reader` must yield exactly `len` bytes. A short read would leave a blob
    /// that can never be read back whole, and a long one would leave bytes past
    /// the end of the record, so implementations reject both.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails, if the length does not match, or if
    /// the content does not hash to `id`.
    async fn put(&self, id: &BlobId, reader: StreamReadSeeker, len: u64) -> io::Result<()>;

    /// Make every record written so far durable.
    ///
    /// A run calls this once, after its last data record and before
    /// [`StoragePut::commit`]. The run's durability point, not each blob's.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be made durable.
    async fn sync_data(&self) -> io::Result<()>;

    /// Publish `snapshot` as the next version, and return the number it got.
    ///
    /// Called after [`StoragePut::sync_data`], and itself durable: a run that
    /// stops before this leaves nothing behind, and one that reports success
    /// without it publishes a version nothing can be restored from.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit record cannot be written or made durable,
    /// in which case the run is not published.
    async fn commit(&self, snapshot: &BlobId) -> io::Result<u32>;
}
