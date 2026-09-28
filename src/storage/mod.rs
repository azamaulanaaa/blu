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
//!
//! The stream types live here rather than alongside the chunker because they are
//! this module's own vocabulary: [`StorageGet::get`] hands one back, and
//! [`StoragePut::put`] takes one. Keeping them with the chunker made the
//! dependency run the wrong way, from storage up into chunking.

use std::io;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek};

pub use blob::BlobStore;
pub use locate::{Resolved, resolve};
pub use plan::{Plan, Planned, Range};
pub use record::Kind;
pub use version_index::VersionIndex;

mod blob;
mod locate;
mod plan;
mod record;
mod version_index;

/// Where a blob's payload sits in a store.
///
/// A blob's offset and length are an implementation detail of the store holding
/// it — a location in this file, an object key there — so a plan resolves a
/// snapshot against a store and speaks in terms a pack writer can use.
pub use record::Placement;

/// Address of a blob: the BLAKE3 hash of its contents.
///
/// The store's index is keyed by this and the snapshot records it and nothing
/// else, so the same type is a chunk's identity in a snapshot and its location
/// in the store.
pub type BlobId = [u8; 32];

/// How large a store segment may get before a new one is started.
///
/// A write policy, not part of the format, and deliberately so: a set written
/// under one limit reads back under any other, because no record depends on where
/// a segment boundary fell. A reader is given a limit only so that one value can
/// be passed to both constructors, and ignores it.
///
/// The default is 4 GiB, which is chosen for the limits other tools impose
/// rather than for lepatch's own: it is under the 4 GiB that FAT32 and several
/// older archive formats refuse, and small enough that no single file needs 64-bit
/// offsets to be handled by anything that moves data around.
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

/// The smallest record that can be written: a header and an empty payload.
const MIN_RECORD: u64 = 46;

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
/// This exists because `trait_alias` is still unstable. Every type that is both
/// implements it automatically, so it is never implemented by hand; use
/// [`StreamReadSeeker`] rather than naming it.
pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

/// A boxed, seekable async stream, as handed out by a storage backend.
///
/// Seekable because a blob is a region of a larger store: reading one means
/// opening the store and seeking to where it starts, not reading the store from
/// the beginning. That is what keeps deduplicated storage from turning into
/// deduplicated *downloads*.
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
    /// A run calls this once, after its last data record and before committing.
    /// It is a separate call rather than something `put` does because the
    /// durability of a backup is the commit's business, not each blob's: a
    /// store that synced every blob would pay for millions of them.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be made durable.
    async fn sync_data(&self) -> io::Result<()>;

    /// Publish `snapshot` as the next version, and return the number it got.
    ///
    /// Called after [`StoragePut::sync_data`], and durable itself. The caller
    /// must not report a run as successful before this returns: a run that stops
    /// before it leaves nothing behind, which is the guarantee, and a run that
    /// claims success without it would leave a version nothing can be restored
    /// from.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit record cannot be written or made durable,
    /// in which case the run is not published.
    async fn commit(&self, snapshot: &BlobId) -> io::Result<u32>;
}
