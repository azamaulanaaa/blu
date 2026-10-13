//! What a store has to be able to do: hand a blob back, and take one in.
//!
//! Two traits, split so that a restore cannot be handed a handle that can append
//! to the store. A restore only ever reads. [`super::blob::BlobStore`] is the
//! implementation. A pack can be another source of [`StorageGet`]. A restore does
//! not have to ask which of the two it is holding.
//!
//! The stream types come first because the traits are written in them. Every
//! caller of [`StorageGet::get`] receives a [`StreamReadSeeker`]. A blob is a
//! region of a larger store rather than a file of its own.

use std::io;

use tokio::io::{AsyncRead, AsyncSeek};

use super::{BlobId, Codec};

/// A stream that is both [`AsyncRead`] and [`AsyncSeek`].
///
/// Blanket-implemented, never by hand: use [`StreamReadSeeker`].
///
/// **This has to be `pub`. The reason is the language rather than taste.** An
/// audit proposed making it `pub(crate)`. The audit argued that nobody implements
/// the trait by hand, so nobody needs to name it. The compiler disagrees, twice:
///
/// * A trait object may have only one *non-auto* trait as its principal trait.
///   The compiler rejects `Box<dyn AsyncRead + AsyncSeek + ..>` with E0225 for
///   that reason. This name is the only way to say "a stream that is both". Every
///   caller of [`StorageGet::get`] receives such a stream.
/// * A `pub(crate)` trait in the signature of a `pub` one is E0446, because
///   [`StorageGet`] is public and its `get` returns this trait.
///
/// So the trait is the minimum public surface that can express the type. It is
/// not an interface left exposed by accident. A backend outside this crate does
/// not need the trait. Unsizing coercion is driven by the type being coerced
/// *to*. `Box::new(my_stream)` as a `StreamReadSeeker` needs no name for the
/// trait. The blanket impl is for that case.
pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

/// A boxed, seekable async stream, as handed out by a storage backend.
///
/// Seekable because a blob is a region of a larger store. Reading one means
/// seeking to where it starts rather than reading the store from its beginning.
pub type StreamReadSeeker = Box<dyn AsyncReadSeek + Unpin + Send>;

/// Reading blobs back out of a store.
///
/// Return-position `impl Future` rather than `async fn`, for the `+ Send`. An
/// `async fn` in a trait says nothing about whether its future can cross a thread.
/// The crate awaits these inside `tokio::spawn`, so the return type has to say it.
/// Spelling the return type out is what says it. Spelling it out also makes these
/// traits non-`dyn`-compatible. That is the trade this makes. Know it before you
/// add a `dyn StorageGet`.
///
/// **These traits are not object safe, and nothing needs them to be.** No
/// `dyn StorageGet` or `dyn StoragePut` appears in the crate or its tests. Every
/// caller is generic over `S: StorageGet`, which monomorphises and needs no
/// vtable. That is what makes the alternative worth having at all. The
/// `#[async_trait]` that these traits replace turned every `get` and every `put`
/// into a boxed future. A backup then paid one heap allocation per chunk to reach
/// a method the compiler could have inlined. A dependency existed only to do
/// that.
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
/// Not object safe, and does not need to be. See [`StorageGet`].
pub trait StoragePut: Send + Sync {
    /// Store `len` bytes read from `reader`, whose contents hash to `id`.
    ///
    /// A blob the store already holds is not written again.
    ///
    /// `reader` must yield exactly `len` bytes. A short read would leave a blob
    /// that can never be read back whole. A long read would leave bytes past
    /// the end of the record. Implementations reject both.
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

    /// Store `stored_len` bytes read from `reader`, already in the form they are
    /// stored in, which decode to `raw_len` bytes of content hashing to `id`.
    ///
    /// The counterpart to [`StoragePut::put`] for a writer that is handed a
    /// payload which has already been encoded. A pack carries each chunk as the
    /// record its source set already holds. Re-encoding that record on the way in
    /// would decode every chunk and compress it again for nothing.
    ///
    /// **The content is decoded and hashed anyway.** This looks like the half of
    /// the work the method exists to avoid. This code deliberately does not skip
    /// it. A chunk's identity is the hash of its *content*. The bytes this method
    /// receives are therefore a claim about what a chunk is, not evidence of it.
    /// Only the content itself can check that claim. Trusting the caller's hash
    /// instead would make `keep` mean "believe this file". A damaged download or a
    /// hostile one could then store bytes under a name they are not. Nothing
    /// downstream could tell, because every reader looks the chunk up by that name
    /// and finds a record that verifies. The method gives up one decode per chunk.
    /// It keeps the guarantee `put` makes, on a path that exists to be cheaper
    /// than `put`.
    ///
    /// **A frame is kept only when it is smaller.** The store must also be able
    /// to decode it. `put` applies the same rule to what its own compressor
    /// returns, for the same reason. A zstd frame carries a header of its own, so
    /// storing a frame that did not shrink its content would grow the set. A codec
    /// this store cannot read would be bytes no reader could serve. Both lengths
    /// are known here, so the check costs two integer comparisons. If the check
    /// fails, the method stores the decoded content raw instead. That is a correct
    /// record whatever the caller had.
    ///
    /// `reader` must yield exactly `stored_len` bytes, for the reason
    /// [`StoragePut::put`] gives.
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails, or if the length does not match. The
    /// method also returns an error if the bytes do not decode to `raw_len`
    /// bytes. It also returns an error if the content does not hash to `id`.
    fn put_stored(
        &self,
        id: &BlobId,
        reader: StreamReadSeeker,
        stored_len: u64,
        raw_len: u64,
        codec: Codec,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Make every record written so far durable.
    ///
    /// A run calls this once, after its last data record and before
    /// [`StoragePut::commit`]. The run's durability point, not each blob's.
    ///
    /// A remote backend provides this by making the uploaded bytes durable.
    /// It finishes the object writes. It does not use `fsync`.
    ///
    /// The ordering matters. The syscall does not matter. The backend makes
    /// data durable first. It shows the commit after.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be made durable.
    fn sync_data(&self) -> impl Future<Output = io::Result<()>> + Send;

    /// Publish `snapshot` as the next version, and return the number it got.
    ///
    /// Called after [`StoragePut::sync_data`], and itself durable. A run that
    /// stops before this leaves nothing behind. A run that reports success without
    /// this publishes a version that nothing can restore from.
    ///
    /// A remote backend shows the commit only after the named data is
    /// durable. It treats sealed segments as immutable objects. It uploads
    /// each object whole. It never appends at an offset. It never truncates
    /// a remote tail.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit record cannot be written or made durable,
    /// in which case the run is not published.
    fn commit(&self, snapshot: &BlobId) -> impl Future<Output = io::Result<u32>> + Send;
}
