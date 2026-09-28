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
pub use record::Kind;

mod blob;
mod record;

/// Address of a blob: the BLAKE3 hash of its contents.
///
/// The store's index is keyed by this and the snapshot records it and nothing
/// else, so the same type is a chunk's identity in a snapshot and its location
/// in the store.
pub type BlobId = [u8; 32];

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
