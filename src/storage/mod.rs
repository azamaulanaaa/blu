//! Where chunk and snapshot data is kept.
//!
//! Blobs are addressed by the BLAKE3 hash of their contents, and the backend is
//! responsible for deduplicating them: a `put` of content the store already
//! holds writes nothing and returns success. That is what lets a backup
//! deduplicate against every version the store has ever held rather than only
//! against the one before it, and it is why the caller does not have to know
//! what is already there.
//!
//! # Layout
//!
//! One file per thing, so that reading one does not mean reading the others.
//! These are private: the crate's whole surface to a caller is the list of
//! re-exports below.
//!
//! * `backend` — what a store can be asked to do, split into reading and writing
//!   so a restore cannot be handed something that appends.
//! * `blob` — the store itself: segments, records, and deduplication.
//! * `record` — the framing of a record, and how a store is read back.
//! * `hash` — what a chunk is named by, and the truncated digest a header checks
//!   itself with.
//! * `compression` — the write policy for record payloads.
//! * `segment` — the write policy for how large a segment may get.
//! * `plan` — a snapshot turned into the ordered list of chunks a store holds.
//! * `locate` — finding a version's snapshot without scanning the whole set.
//! * `version_index` — the per-version index file that makes that cheap.
//! * `error` — what a filesystem call's error says.

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
pub use compression::{Compression, MAX_COMPRESSION_LEVEL};
pub use error::{at, at_async};
pub use hash::BlobId;
pub use locate::{Resolved, resolve, resolve_with};
pub use plan::{Plan, PlannedChunk};
pub use record::Codec;
pub use segment::SegmentLimit;
pub use version_index::VersionIndex;

/// Where a blob's payload sits in a store.
///
/// A location in this file, an object key there: an implementation detail of
/// whichever backend holds the blob.
pub use record::Placement;

// Crate-internal rather than re-exported from `error` at every call site: `pack`
// and the store both size a buffer from a length read out of a file, and one
// refusal for a length this platform cannot address is the whole point of it.
pub(crate) use error::zeroed;
