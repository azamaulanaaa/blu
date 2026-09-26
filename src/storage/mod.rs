//! Where chunk and snapshot data is kept.
//!
//! A backend is split into reading and writing so that a restore, which only
//! ever reads, cannot be handed a handle that can append to the store.
//!
//! The stream types live here rather than alongside the chunker because they are
//! this module's own vocabulary: [`StorageGet::get`] hands one back, and
//! [`StoragePut::put`] takes one. Keeping them with the chunker made the
//! dependency run the wrong way, from storage up into chunking.

use std::io;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek};

pub use blob::BlobFileStorage;

mod blob;

/// A stream that is both [`AsyncRead`] and [`AsyncSeek`].
///
/// This exists because `trait_alias` is still unstable. Every type that is both
/// implements it automatically, so it is never implemented by hand; use
/// [`StreamReadSeeker`] rather than naming it.
pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

/// A boxed, seekable async stream, as handed out by a storage backend.
///
/// Seekable because a chunk is a slice of a larger blob store: reading one means
/// opening the store and seeking to where that chunk starts, not downloading it
/// from the beginning. That is what keeps deduplicated storage from turning into
/// deduplicated *downloads*.
pub type StreamReadSeeker = Box<dyn AsyncReadSeek + Unpin + Send>;

/// Reading blobs back out of a store.
#[async_trait]
pub trait StorageGet: Send + Sync {
    /// Open the blob named by `key`.
    ///
    /// The returned stream is bounded to the blob, so a key cannot be used to
    /// read past it. Implementations must reject a key that does not lie within
    /// the store rather than handing back a short read.
    async fn get(&self, key: &str) -> io::Result<StreamReadSeeker>;
}

/// Appending blobs to a store.
#[async_trait]
pub trait StoragePut: Send + Sync {
    /// Store `len` bytes read from `reader` and return a key for them.
    ///
    /// `reader` must yield exactly `len` bytes. A short read would leave a blob
    /// that can never be read back whole, and a long one would misalign the
    /// recorded position of every later blob, so implementations should reject
    /// both.
    async fn put(&self, reader: StreamReadSeeker, len: u64) -> io::Result<String>;
}
