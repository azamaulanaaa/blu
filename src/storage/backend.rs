//! What a store has to be able to do: hand a blob back, and take one in.
//!
//! Two traits, split so that a restore — which only ever reads — cannot be
//! handed a handle that can append to the store. [`super::blob::BlobStore`] is
//! the implementation, and a pack can be another source of [`StorageGet`] without
//! a restore having to ask which of the two it is holding.
//!
//! The stream types come first because the traits are written in them: every
//! caller of [`StorageGet::get`] receives a [`StreamReadSeeker`], because a blob
//! is a region of a larger store rather than a file of its own.

use std::io;

use tokio::io::{AsyncRead, AsyncSeek};

use super::BlobId;

/// A stream that is both [`AsyncRead`] and [`AsyncSeek`].
///
/// Blanket-implemented, never by hand: use [`StreamReadSeeker`].
///
/// **This has to be `pub`, and the reason is the language rather than taste.** An
/// audit proposed making it `pub(crate)`, on the grounds that nobody implements it
/// by hand and so nobody needs to name it. The compiler disagrees, twice:
///
/// * A trait object may have only one *non-auto* trait as its principal trait, so
///   the alias below cannot be written `Box<dyn AsyncRead + AsyncSeek + ..>` —
///   that is E0225. This name is the only way to say "a stream that is both",
///   which is what every caller of [`StorageGet::get`] receives.
/// * And a `pub(crate)` trait in the signature of a `pub` one is E0446, because
///   [`StorageGet`] is public and its `get` returns this.
///
/// So the trait is the minimum public surface that can express the type, rather
/// than an interface left exposed by accident. A backend outside this crate does
/// not need it: unsizing coercion is driven by the type being coerced *to*, so
/// `Box::new(my_stream)` as a `StreamReadSeeker` needs no name for the trait —
/// which is what the blanket impl is for.
pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

/// A boxed, seekable async stream, as handed out by a storage backend.
///
/// Seekable because a blob is a region of a larger store: reading one means
/// seeking to where it starts rather than reading the store from the beginning.
pub type StreamReadSeeker = Box<dyn AsyncReadSeek + Unpin + Send>;

/// Reading blobs back out of a store.
///
/// Return-position `impl Future` rather than `async fn`, for the `+ Send`. An
/// `async fn` in a trait says nothing about whether its future can cross a thread,
/// and these are awaited inside `tokio::spawn`, so it has to say. Spelling the
/// return type out is what says it — and it is also what makes these traits
/// non-`dyn`-compatible, which is the trade this is making and the one thing to
/// know before adding a `dyn StorageGet`.
///
/// **These traits are not object safe, and nothing needs them to be.** There is no
/// `dyn StorageGet` or `dyn StoragePut` anywhere in the crate or its tests; every
/// caller is generic over `S: StorageGet`, which monomorphises and needs no
/// vtable. That is what makes the alternative worth having at all: the `#[async_trait]`
/// these replace turned every `get` and every `put` into a boxed future, so a
/// backup paid one heap allocation per chunk to reach a method the compiler could
/// have inlined, and a dependency existed only to do it.
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
    fn get(&self, id: &BlobId) -> impl Future<Output = io::Result<StreamReadSeeker>> + Send;
}

/// Appending blobs to a store, and publishing what has been appended.
///
/// Not object safe, and does not need to be — see [`StorageGet`].
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
    fn put(
        &self,
        id: &BlobId,
        reader: StreamReadSeeker,
        len: u64,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Make every record written so far durable.
    ///
    /// A run calls this once, after its last data record and before
    /// [`StoragePut::commit`]. The run's durability point, not each blob's.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be made durable.
    fn sync_data(&self) -> impl Future<Output = io::Result<()>> + Send;

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
    fn commit(&self, snapshot: &BlobId) -> impl Future<Output = io::Result<u32>> + Send;
}
