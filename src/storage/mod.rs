//! Where chunk and snapshot data is kept.
//!
//! A backend is split into reading and writing so that a restore, which only
//! ever reads, cannot be handed a handle that can append to the store.

use std::io;

use async_trait::async_trait;

use crate::reader;
pub use blob::BlobFileStorage;

mod blob;

/// Reading blobs back out of a store.
#[async_trait]
pub trait StorageGet: Send + Sync {
    /// Open the blob named by `key`.
    ///
    /// The returned stream is bounded to the blob, so a key cannot be used to
    /// read past it. Implementations must reject a key that does not lie within
    /// the store rather than handing back a short read.
    async fn get(&self, key: &str) -> io::Result<reader::StreamReadSeeker>;
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
    async fn put(&self, reader: reader::StreamReadSeeker, len: u64) -> io::Result<String>;
}
