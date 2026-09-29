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

use std::{io, path::Path};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek};

pub use blob::BlobStore;
pub use locate::{Resolved, resolve, resolve_with};
pub use plan::{Plan, PlannedChunk};

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

/// A filesystem call's error, naming the path and what was being done to it.
///
/// A bare `?` on `fs::File::open(path)` produces `No such file or directory (os
/// error 2)`, which is a true sentence about a file the user has to find. The
/// crate already has `not_a_store` and `not_a_pack` for the errors that mean
/// "this is not a store" and "this is not a pack", and it has around forty of
/// these — the calls where the operation failed for an ordinary reason and the
/// message is whatever the operating system said.
///
/// The kind is carried through, which is the whole reason this is a function and
/// not a `format!`. A bare `io::Error::new(Other, ...)` would be a lie about a
/// permission problem, and a crate that hands `io::Error` to callers has already
/// established that the kinds are the contract.
///
/// ```no_run
/// # fn main() -> std::io::Result<()> {
/// # let path = std::path::Path::new("photos.0000.bin");
/// let file = lepatch::storage::at(path, "open", std::fs::File::open(path))?;
/// # let _ = file;
/// # Ok(())
/// # }
/// ```
///
/// One construct rather than a `map_err` at each site, because a `map_err` needs
/// the error and the path in the right order to be readable, and gets them the
/// wrong way round about as often as not.
///
/// # Errors
///
/// Returns `Ok` for `Ok`, and otherwise the same error with `doing` and the path
/// put in front of the operating system's own message. The [`io::ErrorKind`] is
/// the one the error came in with, unchanged — that is the reason this is a
/// function and not a `format!`.
pub fn at<T>(path: &Path, doing: &str, result: io::Result<T>) -> io::Result<T> {
    result.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("could not {doing} {}: {e}", path.display()),
        )
    })
}

/// [`at`], for a call that returns a future.
///
/// A second function rather than one overloaded on both, because a future is not
/// an `io::Result` and a wrapper taking either would need a second type parameter
/// to say which. Two names is the smaller cost.
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// # let path = std::path::Path::new("photos.0000.bin");
/// let file = lepatch::storage::at_async(
///     path,
///     "open",
///     tokio::fs::File::open(path),
/// ).await?;
/// # let _ = file;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// The same as [`at`]: `Ok` for `Ok`, and otherwise the error with `doing` and
/// the path in front of it, keeping its [`io::ErrorKind`].
pub async fn at_async<T, F>(path: &Path, doing: &str, result: F) -> io::Result<T>
where
    F: std::future::Future<Output = io::Result<T>>,
{
    at(path, doing, result.await)
}

/// A zeroed buffer of exactly `bytes`, or an error saying what would not fit.
///
/// Used wherever a length read out of a file becomes an allocation. The reason
/// this is a function rather than three `map_err`s is that the tempting shortcut
/// in every one of those cases is a zero, and **a zero-length buffer is not a
/// small buffer**: `read_exact` into one succeeds, because there is nothing to
/// read. So the failure is not caught later. It is passed on — an index that
/// decodes as empty, a chunk that decompresses from nothing, a pack that is short
/// by every chunk while its own index claims otherwise.
///
/// There is no fallback to choose between here. A length this platform cannot
/// address is not a small length, and the alternative to an error is not a worse
/// answer but a wrong one. `what` is the caller's description of the thing being
/// sized, so the message says which one rather than "a length".
pub(crate) fn zeroed(bytes: u64, what: &str) -> io::Result<Vec<u8>> {
    usize::try_from(bytes).map(|n| vec![0u8; n]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what} is {bytes} bytes, which this platform cannot address"),
        )
    })
}

/// Where a blob's payload sits in a store.
///
/// A location in this file, an object key there: an implementation detail of
/// whichever backend holds the blob.
pub use record::Placement;

/// Address of a blob: the BLAKE3 hash of its contents.
///
/// Both a chunk's identity in a snapshot and its key in the store's index.
///
/// This is [`blake3::Hash`] rather than a `[u8; 32]` of our own, which is what
/// the `traits-preview` feature on the `blake3` dependency has been providing
/// this whole time. The alias it replaced was not a newtype — `BlobId` and
/// `[u8; 32]` were the same type, interchangeable in both directions, so it
/// distinguished nothing — and it was spelled three ways across the crate:
/// `BlobId` in most places, `[u8; 32]` in `command::pipeline` and `metadata`,
/// and `[u8; HASH]` in `storage::record`, with `HASH` being 32.
///
/// `blake3::Hash` brings what this crate was writing by hand:
///
/// * [`Display`](std::fmt::Display) as lowercase hex, which is what every
///   message and log line in the crate wanted, and is why a `Hex` wrapper
///   existed until this landed;
/// * [`From<[u8; 32]>`](blake3::Hash::from_bytes) and
///   [`as_bytes`](blake3::Hash::as_bytes), so the record header and the
///   snapshot's chunk table keep writing the same 32 bytes;
/// * `PartialEq` against `[u8; 32]` and `[u8]`, both constant-time, so
///   comparing a computed hash to a decoded one needs no conversion;
/// * [`Copy`], [`Hash`] and [`Eq`], so it is usable as a map key as it was.
///
/// It deliberately has no `Ord`, no `AsRef<[u8]>` and no `Deref`: asking for the
/// bytes is how you get them, and a `Deref` to a byte array would hand out the
/// non-constant-time comparison that `Hash` exists to avoid.
///
/// [`from_slice`](blake3::Hash::from_slice) is `TryFrom<&[u8]>`, so a length
/// error is a type error rather than a check.
pub type BlobId = blake3::Hash;

/// How large a store segment may get before a new one is started.
///
/// A write policy, not part of the format: a set written under one limit reads
/// back under any other, because no record depends on where a segment boundary
/// fell. A reader is given a limit only so one value can be passed to both
/// constructors, and ignores it.
///
/// The default is 4 GiB, under what FAT32 and several older archive formats
/// refuse, and small enough that no single file needs 64-bit offsets.
/// A write policy, and the floor it cannot go below.
///
/// # Example
///
/// Runs, not `no_run`: `new` and `bytes` are pure, and an example that only
/// compiles is an example whose arithmetic can be wrong.
///
/// ```
/// # fn main() -> std::io::Result<()> {
/// use lepatch::storage::SegmentLimit;
///
/// assert_eq!(SegmentLimit::default().bytes(), 4 * 1024 * 1024 * 1024);
///
/// // A smaller limit is honoured: a set of smaller segments reads back the same,
/// // because where a boundary fell is not recorded anywhere.
/// assert_eq!(SegmentLimit::new(64 * 1024 * 1024).bytes(), 64 * 1024 * 1024);
///
/// // A limit too small to hold a store header and one record is raised to that
/// // floor rather than refused, because a segment that cannot hold a record
/// // cannot be a segment. The floor is a property of the format rather than of
/// // this policy, so `new` has no way to express anything below it.
/// assert!(SegmentLimit::new(1).bytes() > 1, "clamped up to a usable segment");
/// # Ok(())
/// # }
/// ```
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
/// A write policy, chosen per run and recorded per record.
///
/// # Example
///
/// `no_run`, unlike the examples elsewhere in this module: opening a store
/// creates a segment on disk, so this one compiles but must not execute.
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// use lepatch::storage::{BlobStore, Compression, SegmentLimit};
///
/// // The defaults: zstd level 1, four gibibytes per segment.
/// let store = BlobStore::<true>::open(
///     "photos",
///     SegmentLimit::default(),
///     Compression::default(),
/// ).await?;
///
/// // Or not. Both are write policies, and a set written under one reads back
/// // under the other: a compressed record says so in its own header, and which
/// // records are compressed is a question each record answers for itself.
/// let store = BlobStore::<true>::open(
///     "photos",
///     SegmentLimit::new(64 * 1024 * 1024),
///     Compression::Off,
/// ).await?;
/// # Ok(())
/// # }
/// ```
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
    /// zstd at `level`, clamped to 1..=[`MAX_COMPRESSION_LEVEL`].
    ///
    /// The only way to name a zstd policy. A level of 0 clamps to 1, not to
    /// [`Compression::Off`], because the caller asked for zstd and the two are
    /// different requests: `Off` is a decision to store payloads as they are, and
    /// silently taking it would be a store written at a policy nobody chose.
    /// [`Compression::from_level`] is how a user's 0 becomes `Off`, and it is a
    /// separate function because that is a separate question.
    #[must_use]
    pub fn zstd(level: u8) -> Self {
        Self::Zstd {
            level: level.clamp(1, MAX_COMPRESSION_LEVEL),
        }
    }

    /// The policy for a level the way a user counts them: 0 stores payloads as they
    /// are, and 1 to 9 are zstd levels.
    ///
    /// The difference from [`Compression::zstd`] is the `0` arm and nothing else,
    /// which is the whole reason they are two functions. One of them means "use
    /// zstd at this level" and the other means "this is a level from a command
    /// line, where 0 has always meant off". A single function would have to
    /// answer both questions, and it would answer the first one wrongly for
    /// anyone who passed 0 meaning zstd's floor.
    ///
    /// Out-of-range values are clamped rather than rejected, so a bad number gives
    /// a working policy rather than a panic. A command line rejects them instead,
    /// where the user can be told.
    #[must_use]
    pub fn from_level(level: u8) -> Self {
        if level == 0 {
            Self::Off
        } else {
            Self::zstd(level)
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
///
/// Derived from the record format's own constant rather than re-spelling its
/// parts. It was `6 + 2 + 32 + 8`, one module over from the `PREFIX + 2 + HASH +
/// CHECK` it is a copy of, with the `2` — two one-byte lengths of zero —
/// documented in the original and here. A header that grew a field would move
/// `MIN_HEADER` and leave this behind, and the only thing it would break is
/// `SegmentLimit::new`'s floor, silently, on the way to a store that refuses its
/// own records.
const MIN_RECORD: u64 = record::MIN_HEADER as u64;

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
