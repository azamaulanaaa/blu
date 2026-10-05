//! Where chunk and snapshot data is kept.
//!
//! Blobs are addressed by the BLAKE3 hash of their contents, and the backend is
//! responsible for deduplicating them. A `put` of content the store already
//! holds writes nothing and returns success. A backup can then deduplicate
//! against every version the store has ever held, not only against the one before
//! it. That is also why the caller does not have to know what is already there.
//!
//! # Layout
//!
//! One file per thing, so that reading one does not mean reading the others.
//! These files are private. The crate's whole surface to a caller is the list of
//! re-exports below.
//!
//! * `backend` says what a store can be asked to do. It splits into reading and
//!   writing, so a restore cannot be handed something that appends.
//! * `blob` is the store itself: segments, records, and deduplication.
//! * `record` holds the framing of a record, and how a store is read back.
//! * `hash` holds what a chunk is named by, and the truncated digest a header
//!   checks itself with.
//! * `compression` holds the seams for how a payload becomes stored bytes and
//!   how they come back. It also holds the write policy this crate ships.
//! * `segment` holds the write policy for how large a segment may get.
//! * `plan` turns a snapshot into the ordered list of chunks a store holds.
//! * `locate` finds a version's snapshot without scanning the whole set.
//! * `version_index` holds the per-version index file that makes that cheap.
//! * `error` says what a filesystem call's error reports.

mod backend;
mod blob;
mod compression;
mod error;
mod hash;
mod locate;
mod plan;
mod record;
mod segment;
mod version_index;

pub use backend::{AsyncReadSeek, StorageGet, StoragePut, StreamReadSeeker};
pub use blob::{BlobStore, Read, Write, WriteMode};
pub use compression::{Compression, Compressor, Decompressor, MAX_COMPRESSION_LEVEL, Zstd};
pub use error::{at, at_async};
pub use hash::BlobId;
pub use locate::{Resolved, resolve, resolve_with};
pub use plan::{Plan, PlannedChunk};
pub use record::Codec;
/// The layout version of the store's records, served at `GET /v1/version`.
///
/// Public so a client can ask what a server can read before it sends it
/// anything. See [`record::FORMAT_VERSION`].
pub use record::FORMAT_VERSION;
pub use segment::SegmentLimit;
pub use version_index::VersionIndex;

/// Where a blob's payload sits in a store.
///
/// A location in this file, an object key there. Both are an implementation
/// detail of whichever backend holds the blob.
pub use record::Placement;

// The record framing itself, for the pack: its payload region is a run of
// records. The store writes and reads them through its own encoder and decoder.
// The pack does not keep a second copy of either. See the note in `record` on who
// else frames a record.
pub(crate) use record::{
    Header as RecordHeader, Kind as RecordKind, MAX_HEADER as MAX_RECORD_HEADER,
    MIN_HEADER as MIN_RECORD_HEADER, decode_header, encode_header, header_len,
};

// Crate-internal rather than re-exported from `error` at every call site: `pack`
// and the store both size a buffer from a length read out of a file. One refusal
// for a length this platform cannot address is the whole point of it.
pub(crate) use error::zeroed;
