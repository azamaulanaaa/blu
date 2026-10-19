// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A pack: one version's chunks, concatenated into a single distributable file.
//!
//! A store is a deduplicated pool, and the wrong shape for serving one version: a
//! restore out of a store is a request per chunk. A pack lays the same chunks end
//! to end, so a version's bytes are contiguous and are one range request rather
//! than thousands. The measurements behind that are in the README.
//!
//! ## The payload is a run of store records
//!
//! Each chunk is written as the record the source store already holds for it.
//! The header and the codec byte are written too, rather than bare content.
//! The cost is a header per chunk. The saving is that a chunk arrives in the form
//! it was deduplicated in. Packing a compressed set copies frames rather than
//! inflating every one of them. The receiving side re-encodes under its own
//! policy instead of carrying the sender's.
//!
//! **The bytes are the store's, produced by the store's own encoder.** A
//! second encoder here would be a second record format. It would agree with the
//! first until the first changed. [`crate::storage`]'s `encode_header` writes
//! them and its `decode_header` reads them back.
//!
//! ## Determinism
//!
//! A pack is a function of its [`crate::storage::Plan`] **and of the stored form
//! the chunks had in the set it was packed from**. A plan is ordered by the
//! snapshot's chunk table rather than by the order chunks happened to be written.
//! The same tree packs to the same bytes every time. That is what makes a pack
//! nameable by the hash of its own contents. The result is an immutable artifact
//! a CDN can cache forever and a client can verify with one comparison. The stored
//! form is part of that function on purpose, rather than a leak of the sender's
//! policy into the artifact. Two sets holding one tree at the same compression produce
//! the same pack. Two sets holding it at different compressions produce different
//! packs, which is true of the sets themselves.
//!
//! ## The index is at the end
//!
//! ```text
//! magic (4) | format version (4) | version (4) | snapshot hash (32) |
//! UTC seconds (8) | nanos (4) | chunks (varint) |
//! payload bytes (varint) [| base flag (1) | base hash (32)]
//! ...records, end to end, in plan order: a store record per chunk...
//! ...snapshot: the encoded snapshot this version is, verbatim...
//! ...index: per chunk, hash (32) | offset (varint) | content length (varint)...
//! index length (8) | snapshot length (8) | magic (4)
//! ```
//!
//! The payload length is the one count a writer may not know when it starts the
//! header. A pack that re-encodes under a level learns each frame's width as it
//! makes the frame. Its header therefore reserves ten bytes for that length.
//! The writer patches the real total in once the payload is on disk. A reader
//! sees an ordinary number, because a padded LEB128 number ends within its ten
//! bytes. [`Pack::open`] stops at the end as it does for any other.
//!
//! The header repeats the snapshot stamp, so a listing reads no snapshot
//! section. The stamp matches the one inside the carried snapshot.
//!
//! `offset` is where a chunk's *record* starts. The record's own header already
//! says how long the record is. The header is the first thing in the record, and
//! the store's encoder writes it. The index does not say the length a second
//! time. Two copies of one number in one file is two claims that can disagree,
//! and a pack is a file somebody downloaded.
//!
//! A range drawn from the index still has to cover a chunk whole, header
//! included. It does because the records are written end to end. An entry's record
//! runs from its own offset to the next entry's. A client that fetches everything
//! between two offsets has everything a chunk needs to be read. The header is
//! inside that run rather than across it.
//!
//! `content length` is the same number after decoding. A snapshot counts its
//! lengths in that number, and `put` hashes it. The index keeps this one length,
//! because the header does not say it in the form a caller wants. It is in content
//! bytes. The header's own lengths are the payload's on disk and its bytes once
//! decoded.
//!
//! A player fetches the last twenty bytes. The player learns where the index is
//! and how long it is. The player fetches *that* range, and then knows every
//! chunk's offset without having read a byte of payload. The cost of finding out
//! what a pack holds does not depend on how much it holds.
//!
//! The trailing magic is what makes that reverse read reliable. The length alone
//! is indistinguishable from payload bytes, and a truncated download would
//! otherwise be read as a short index.
//!
//! ## It carries the snapshot
//!
//! A pack holds the bytes but not what they *mean*. The file table lives in the
//! snapshot. The file table lists paths, lengths, attributes, and which slice of
//! which chunk lands at which offset. The snapshot is otherwise just another blob
//! in the store. A client handed a pack and nothing else could locate a chunk but
//! could not lay a single one of them into a file. That is the other half of what
//! a pack is for, which is applying one version on top of another.
//!
//! The snapshot therefore travels with the payload, and its hash is in the
//! header, so a pack is one self-contained artifact. It sits *between* the
//! payloads and the index rather than after the index. That placement preserves
//! the property above. `index length` alone still locates the index exactly. A
//! client that only wants offsets reads the trailer and one range and never
//! touches the snapshot. Only a client that means to apply the pack pays for it.
//! The range is proportional to the metadata rather than to the tree.
//!
//! What this buys is not the bytes, because they were in the set either way. What
//! it buys is the ability to hand someone one file. Two files, a pack and a
//! snapshot, cannot be published as one immutable CDN object with one name. One
//! file can.
//!
//! ## It names the version it was built from
//!
//! The header carries a number. The number is the version of *the set the pack was
//! written from*. It is the one thing in the header that is not about the bytes
//! in the file. It is there because a receiver otherwise cannot see the most
//! ordinary event in a transfer at all. The sender built a pack of a version that
//! arrived before, and the transfer was retried. The version the receiver holds is
//! already here under some number that means nothing where the pack came from.
//!
//! **It is the origin set's numbering.** It means nothing as a coordinate here.
//! Two sets number independently. In a pack, "version 3" says which version of
//! the set it was packed from. It says nothing about where it lands.
//! The version a merge publishes is the receiving set's own, allocated at commit
//! and reported as such. The pack's number is what a warning can compare against
//! the set's tip to notice that a pack has fallen behind. It is a hint for a person,
//! not a mapping. A client that treated it as a coordinate would try to publish
//! version 3 into a set that is on version 7. There would be no version 3 to
//! overwrite, because the two numberings never met.
//!
//! Nothing here depends on it being *right*, which is why it does not get to
//! refuse anything. A set renumbered, restored elsewhere, or repacked from a
//! different machine's numbering can all make it stale. What it buys is that the
//! staleness is visible at the moment a transfer is received. Otherwise the
//! staleness is inferred later from a restore that returned an older version than
//! the one asked for.
//!
//! ## Derived, never authoritative
//!
//! A pack is output. The store and the snapshot alone regenerate it. Losing one
//! loses nothing, because `blu pack` writes it again, byte for byte. The
//! store's commit records remain the record of what versions exist, which is why
//! nothing reads a pack to answer that.

use std::{
    collections::{HashSet, hash_map::RandomState},
    fs,
    hash::{BuildHasher, Hasher},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use futures_util::StreamExt as _;
use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::instrument;

use crate::metadata::Timestamp;
use crate::storage::{
    BlobId, Codec, Decompressor, MAX_RECORD_HEADER, Plan, PlannedChunk, RecordKind, StorageGet,
    StreamReadSeeker, Zstd,
};

/// Largest chunk the chunker can produce, and so the most a pack will decode into
/// memory in one piece.
///
/// `simd_cdc::MAXIMUM_MAX`, the ceiling `ChunkerConfig::validate` enforces on
/// `max_size`, which makes it the largest content any chunk in a set this crate
/// wrote can have. The bound is on the *record's claim* rather than on anything
/// measured. A pack is a file that was downloaded, so a frame that declares a
/// terabyte would otherwise have this process allocate one to find out.
///
/// The largest chunk any named profile cuts is 256 KiB. That figure is tighter and
/// more usual. It is not the one used here, because a caller may configure a
/// chunker with a larger `max_size`. A pack of such a set has to remain readable.
pub(crate) const MAX_CHUNK: u64 = crate::simd_cdc::MAXIMUM_MAX as u64;

/// Most chunks a pack's order can name, which is `u32::MAX`.
///
/// A consequence of [`Pack::order`] holding positions in a `u32`. It is not a limit
/// anybody would notice hitting. Four billion records is over two hundred
/// gigabytes of payload, and the file would have to be read to be opened. The
/// number is stated here because the refusal that enforces it must name it.
const MAX_PACK_CHUNKS: u32 = u32::MAX;

/// The writer fetches this many chunks at once.
///
/// The writer writes the records in plan order.
/// A window holds at most this many chunks.
const PREFETCH_WINDOW: usize = 16;

/// The writer holds at most this many bytes of fetched chunks at once.
///
/// The plan gives the size of every chunk, so the writer knows the size of a
/// chunk before it reads the chunk. A window stops before it would hold more
/// than this. The window fetches a chunk that is larger than this on its own.
///
/// The value is the size of the largest chunk the chunker can produce. A pack of
/// large chunks therefore holds one chunk at a time. A pack of small chunks
/// holds [`PREFETCH_WINDOW`] of them, because they fit in the budget.
const PREFETCH_BYTES: u64 = MAX_CHUNK;

/// Magic at the start of a pack, and again at the very end.
const MAGIC: [u8; 4] = *b"LPK1";

/// Layout version, so a pack this build does not understand is reported as such.
///
/// `7` is a pack whose header records the version time in UTC.
/// `6` is the same file without that stamp, so its header is twelve bytes
/// shorter. It would decode here as a version time that is really the base
/// flag and the counts. `6` is also a pack whose index does not repeat the
/// record length each chunk's own header already carries. `5` is the same payload region with that length
/// duplicated in the index, so its index entries are wider. They would decode here
/// as offsets that are really something else. `4` is a pack whose payload region
/// is the chunks' content end to end with nothing between them. `3` recorded the
/// number of the version it was packed from, which `4`'s predecessor did not.
/// `2` carried its snapshot and `1` did not. `1` also has a twelve-byte trailer
/// where these have twenty. Those two cannot be told apart by their contents, and
/// they would take their index length from the wrong place.
///
/// Public because a client has to be able to ask what a server can read before it
/// uploads anything. A pack this build cannot read is refused with a message
/// naming the version it wanted, and the version a client wants is not in the
/// pack — so the answer has to come from somewhere else. `blu serve` serves it
/// at `GET /v1/version`.
///
/// **Every earlier version is refused rather than guessed at**, and the error names
/// both numbers, this build's and the pack's. A client holding a pack this build
/// cannot read is otherwise left guessing which command produces one. That is
/// affordable here for the same reason it was for `4`. A pack is derived output, so
/// re-running `blu pack` regenerates a pack that an earlier build wrote, and
/// nothing is lost but the file.
///
/// A `4` read as if its payloads were records would not fail at the header. The
/// first chunk's bytes would be read as a record header and refused for a magic
/// that is not there, so the damage is loud. It is refused by name anyway, because
/// the alternative is a reader that works out what a file is by trying.
pub const FORMAT_VERSION: u32 = 7;

/// Set in the header when a base snapshot hash follows it, making this a delta.
const BASE_FLAG: u8 = 0x01;

/// Bytes of fixed header, before any base hash and the two varint counts.
///
/// The stamp is fixed width, so the base flag sits at a fixed offset too.
/// The base is variable because a delta carries one and a full pack does not, so
/// the base cannot be part of this. That is why [`open`] reads the byte after it
/// to learn how long the header is, rather than assuming.
///
/// The recorded version *is* fixed, though it is one fact about the pack rather
/// than a fact about its geometry. That is the reason rather than an accident. It
/// costs four bytes in a header written once per pack. It bought them back
/// by being readable without touching the variable section. A reader that
/// walks the base flag to learn the version must parse the pack to learn what
/// the pack *is*. Four bytes say it at a fixed offset. The version becomes
/// usable by anything holding the header without holding a [`Pack`].
const HEADER: usize = 4 + 4 + 4 + 32 + 8 + 4;

/// Bytes of trailer: the two section lengths and the trailing magic.
const TRAILER: usize = 8 + 8 + 4;

/// Longest the header's two varint counts can be. A [`read_varint`] accepts at
/// most ten bytes, so the pair is at most twenty. The fixed part is therefore
/// never the whole of the header.
const COUNTS_MAX: usize = 20;

/// Where a chunk sits in a pack.
///
/// `offset` and `content_len` are what the index carries. The record's own
/// length is not among them. The record's header is the first bytes of the record,
/// and the store's own encoder writes it, so the header already carries the
/// length. A second copy in the index is a second claim about the same bytes that
/// the two could disagree about. What a reader has instead is the fact the index
/// does state. The records tile the payload, so a record runs from its entry's
/// offset to the next entry's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located {
    /// The chunk's content hash.
    pub id: BlobId,
    /// Byte offset of its record, relative to [`Pack::payload_start`].
    ///
    /// The record's start, header included. A range that covers a chunk starts
    /// here.
    pub offset: u64,
    /// Bytes of content the record's payload is once decoded.
    ///
    /// What a snapshot's lengths are counted in. It is also what [`StoragePut::put`]
    /// is told to read, so it is what a merge hands over.
    ///
    /// [`StoragePut::put`]: crate::storage::StoragePut::put
    pub content_len: u64,
}

/// A chunk as the store holds it. That is its record's payload, and what that
/// record says about it.
///
/// `raw_len` is the pack's own [`Located::content_len`] again. It is kept here
/// beside the bytes because the two are what one chunk's record is. The payload
/// occupies `bytes.len()` bytes on disk and decodes to `raw_len` of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// The payload exactly as it sits in the source. It is a frame where the record
    /// says `Zstd`, and the content itself where it says `Raw`.
    pub bytes: Vec<u8>,
    /// How those bytes are stored.
    pub codec: crate::storage::Codec,
    /// Bytes of content they decode to, which is what the record's hash covers.
    pub raw_len: u64,
}

/// Reading a chunk's **stored** bytes, which is what the pack writer reads from.
///
/// The mirror of [`StorageGet`], and separate from it for the reason the store's
/// own [`StoragePut`](crate::storage::StoragePut) is. `get` answers a *reader*,
/// who wants content and is owed a decompressed stream. A pack is not a reader.
/// Copying content into a pack and writing it back as a fresh record would
/// decode every chunk of a compressed set. It would also re-encode every one of
/// them. That is the work this format exists to avoid. It would also make a
/// pack's bytes a function of the reading path rather than of what the set
/// holds.
///
/// [`BlobStore`](crate::storage::BlobStore) implements it, which is the only
/// implementation this crate ships.
pub trait StorageStored: Send + Sync {
    /// The blob whose contents hash to `id`, as the store holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if the store does not hold that blob, or if the bytes
    /// cannot be read.
    fn stored(&self, id: &BlobId) -> impl Future<Output = io::Result<Stored>> + Send;
}

impl<W, C> StorageStored for crate::storage::BlobStore<W, C>
where
    W: crate::storage::WriteMode,
    C: crate::storage::Compressor,
{
    /// Read the blob's payload out of the segment it sits in, without decoding.
    ///
    /// Bounded to the record's stored length, so a record whose index entry is
    /// wrong cannot pull in its neighbour. The store refuses a segment shorter
    /// than the index says. `get` makes the same check for the same reason, since
    /// a blob handed out now has to be the blob that was indexed.
    #[instrument(skip_all, err)]
    async fn stored(&self, id: &BlobId) -> io::Result<Stored> {
        let (bytes, codec, raw_len) = self.read_stored_bytes(id).await?;
        Ok(Stored {
            bytes,
            codec,
            raw_len,
        })
    }
}

/// A half-open byte range of a pack.
///
/// This is [`std::ops::Range<u64>`] with a `start`, an `end`, and two things that
/// range does not have. An audit proposed the standard type, and this declines
/// that proposal because both of those things would be lost.
///
/// **A `len()` that cannot panic.** `Range<u64>` has no `len()` at all, and that
/// is not an oversight. It would be `end - start`, which panics under a debug
/// assertion and wraps to a near-`u64::MAX` in release. Both fields are public and
/// the type is part of the client protocol, so an inverted range is a value a
/// caller can produce. `len` here saturates, and `needed_bytes` folds that rather
/// than `sum()` for the same reason.
///
/// **`is_empty()` means "well-formed and covers nothing", not `end <= start`.**
/// `Range<u64>::is_empty` is `start >= end`, so an inverted range would answer
/// `true`. An inverted range is not empty, it is *malformed*, and the two want
/// different responses. `tests/pack.rs::an_inverted_range_covers_nothing`
/// pins this deliberately. The same call must be able to say "no bytes" (via
/// `len`) and "not a range" (via `is_empty`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    /// First byte.
    pub start: u64,
    /// One past the last byte.
    pub end: u64,
}

impl Range {
    /// Bytes the range covers.
    ///
    /// Saturating, because both fields are public. A range that a client built with
    /// `end` below `start` is a value this type has to survive rather than panic
    /// on. It covers nothing.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range covers nothing *and is well formed*.
    ///
    /// Not `end <= start`, which is what [`std::ops::Range::is_empty`] does and
    /// which reports an inverted range as empty. An inverted range covers no bytes,
    /// which [`Range::len`] says. It is not empty, it is malformed, and a caller
    /// acting on the two differently is the point.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }
}

/// A pack on disk: the snapshot it holds, the payload region, and the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pack {
    path: PathBuf,
    /// Where the payloads start.
    payload_start: u64,
    /// Total bytes of payload.
    payload_len: u64,
    /// The version this pack was built from, in the set that built it.
    version: u32,
    /// The snapshot this pack holds.
    snapshot: BlobId,
    /// Time the packed version ran, in UTC seconds and nanos.
    ///
    /// The value matches the stamp inside the carried snapshot.
    /// A listing reads the header rather than the snapshot section.
    taken_at: Timestamp,
    /// The version this pack is a delta against, if it is one.
    base: Option<BlobId>,
    /// Where the snapshot's own bytes start, relative to the file.
    snapshot_start: u64,
    /// How many bytes of snapshot the pack carries.
    snapshot_len: u64,
    /// Every chunk, in hash order.
    chunks: Vec<Located>,
    /// Where each chunk sits in `chunks`, in payload order.
    ///
    /// The index is held in hash order because that is what [`Pack::locate`]
    /// binary-searches, and hash order is not payload order. Every question about
    /// byte ranges therefore has to get from one order to the other, and this is
    /// the map that does it. It holds positions rather than a second copy of the
    /// entries. Each entry is 32 bytes of hash and two numbers. Duplicating them
    /// to answer a question about offsets would cost more than the index itself.
    ///
    /// **`u32` rather than `usize`.** A pack with more entries than that is
    /// refused. Four bytes per chunk against eight, over an index of fifty-six,
    /// is the difference between a pack's bookkeeping costing a tenth of its index
    /// and a fifth of it. The price is one branch that no test can reach.
    /// [`MAX_PACK_CHUNKS`] chunks is four billion records, over two hundred
    /// gigabytes of payload. It is an error rather than a wrapped number. A
    /// position that silently wrapped would hand a client every range from the
    /// wrong end of the pack. A line coverage report will list it as missed,
    /// and that is the honest reading of it.
    ///
    /// Built once, where the offsets are already known. A pack written records its
    /// chunks as they are appended in. A pack read is checked for tiling by walking
    /// its records in offset order, which produces exactly this. Nothing downstream
    /// sorts.
    order: Vec<u32>,
}

/// How the writer learns a header's payload length.
///
/// A pack's header carries the payload's total length, and the writer writes the
/// header before the payload. So the two routes decide whether the total is
/// final when the header starts. It is the only decision they make.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// The writer knows the total before the payload starts, so the header is
    /// final.
    ///
    /// A record's width follows from two lengths. The plan already resolved both
    /// lengths of every record. This is the route for a pack that copies the
    /// set's own bytes, and for one that writes the content raw.
    Verbatim,
    /// The total is a property of frames the run has not made yet.
    ///
    /// The header reserves [`crate::varint::MAX`] bytes for the length. The
    /// writer writes the payload and then seeks back and patches the real total
    /// in. This is the route for a pack that re-encodes under a zstd level.
    /// Nothing can state a frame's width before zstd makes the frame.
    Reencoded,
}

impl Pack {
    /// Write a pack of `plan` to `path`, reading payloads from `store`.
    ///
    /// It is written to a sibling temporary and renamed into place when it is
    /// whole. A run that fails part-way leaves the pack that was already at
    /// `path` alone. It does not replace that pack with a file that cannot be
    /// opened. A pack is derived output, so a partial one is a file to write
    /// again rather than a thing to recover. The pack a *previous* run wrote is
    /// not this run's to lose, and the pack's own path usually names one.
    /// The writer holds one window of chunks at a time, never a whole version.
    /// A window holds at most sixteen chunks and at most sixteen mebibytes of
    /// fetched chunk data. The index and the snapshot both grow with what is
    /// being packed.
    ///
    /// `version` is the number the version has *in the set this is being packed
    /// from*. It goes in the header so a receiver can see where the pack came from.
    /// It is a parameter rather than something worked out from the plan, because
    /// the plan does not know it. A plan is a function of a snapshot's chunks, and
    /// two sets number their versions independently. The same plan is version 2 of
    /// one set and version 9 of another. A default here would be a `0` in the
    /// header of every pack written by a caller that forgot. A `0` reads as a
    /// real version number to whatever reads it back.
    ///
    /// **Each chunk is written whole, and in the stored form it has in the set.**
    /// Whole, because a record's hash covers the chunk's entire content. A record
    /// holding only the part a version reads would not hash to the name it is
    /// addressed by. A merge hands those bytes straight to `put`, which checks that
    /// hash. It uses the stored form. A pack that decoded every chunk on the
    /// way out would do the expensive half of the transfer twice. It would also
    /// re-encode the chunk on the way in. That also makes the artifact's bytes
    /// depend on the reading path rather than on the set.
    ///
    /// The header comes first and its counts are the plan's. They are `plan.len()`
    /// chunks and, for the payload, the sum of the records the plan's placements
    /// describe. The header is therefore written before a byte of payload, and both
    /// agree by construction. Every one of those terms is checked against what
    /// the store yields as each chunk is written. A set that changed underneath
    /// the run is refused. The run does not write it into a pack whose own
    /// counts are wrong.
    ///
    /// The snapshot is written as `plan`'s own description of the version, so a
    /// pack *is* the version rather than a reference to it. The hash in the
    /// header is over exactly the bytes that follow the records.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be recorded on this platform, or if
    /// the store cannot yield a chunk the plan names. It returns an error if what
    /// the store yields is not the stored form the plan resolved. It returns an
    /// error if the file cannot be written.
    ///
    /// Generic over [`StorageStored`] because that is the whole of what it needs,
    /// which is the bytes and the codec byte beside them. It was bound to a
    /// concrete `&BlobStore<Read>`. That made `blu pack` the one command
    /// that could not use another backend. It could not use a read-only store
    /// over a directory. It could not use a store held in memory. A pack is
    /// derived output, so there is no reason it should care where the bytes
    /// came from.
    pub async fn write<S>(
        store: &S,
        plan: &Plan,
        snapshot: &crate::metadata::Snapshot,
        version: u32,
        path: &Path,
    ) -> io::Result<Self>
    where
        S: StorageStored + ?Sized,
    {
        Self::write_from(store, plan, snapshot, version, None, Route::Verbatim, path).await
    }

    /// Write a pack of `plan` that assumes its reader already holds `base`.
    ///
    /// A delta: the same file, carrying the same snapshot, holding only the chunks
    /// a holder of `base` is missing. Nothing about the format changes, because the
    /// index simply names fewer chunks. Everything downstream is therefore
    /// unchanged.
    ///
    /// **The base is a promise, and this is where it is made.** A chunk left out is
    /// one the reader is asserted to have. Nothing in the pack can check that,
    /// because the bytes that would prove it are exactly the bytes that were left
    /// out. So the base is recorded in the header, which lets [`Pack::base`] report
    /// a mismatch rather than have one happen quietly. A reader that cannot account
    /// for the base must refuse the pack, not apply what it can.
    ///
    /// Pass nothing to [`Self::write`] for a full pack, which is correct
    /// against any reader and needs no such accounting.
    ///
    /// # Errors
    ///
    /// As [`Self::write`].
    pub async fn write_delta<S>(
        store: &S,
        plan: &Plan,
        snapshot: &crate::metadata::Snapshot,
        base: BlobId,
        version: u32,
        path: &Path,
    ) -> io::Result<Self>
    where
        S: StorageStored + ?Sized,
    {
        Self::write_from(
            store,
            plan,
            snapshot,
            version,
            Some(base),
            Route::Verbatim,
            path,
        )
        .await
    }

    /// [`Self::write`] or [`Self::write_delta`], by whether a base was given.
    ///
    /// `route` decides how the header learns the payload's length. Under
    /// [`Route::Verbatim`] the writer writes the plan's total and the header is
    /// final. Under [`Route::Reencoded`] the run encodes the payload as it writes
    /// it, so the total is unknown when the header starts. The header then
    /// reserves [`crate::varint::MAX`] bytes for the length. The writer seeks back
    /// and patches the total in once the payload is on disk. Either route puts
    /// the payload region at the same place, so nothing after the header moves.
    #[expect(
        clippy::too_many_arguments,
        reason = "a pack's own facts, threaded from the two callers that name them"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "one pack's whole write: header, payload, patch, index and trailer"
    )]
    pub(crate) async fn write_from<S>(
        store: &S,
        plan: &Plan,
        snapshot: &crate::metadata::Snapshot,
        version: u32,
        base: Option<BlobId>,
        route: Route,
        path: &Path,
    ) -> io::Result<Self>
    where
        S: StorageStored + ?Sized,
    {
        // Only fails where a path in the snapshot cannot be represented on this
        // platform, which on unix cannot happen. Invalid input rather than
        // damaged data, because nothing is wrong with the snapshot. This build
        // cannot record it.
        let snapshot_bytes = snapshot.to_bytes().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("this version cannot be packed on this platform: {e}"),
            )
        })?;
        let snapshot_id = blake3::hash(&snapshot_bytes);

        // Removed on drop unless persisted below. A temporary beside a
        // finished pack is a file with no meaning, and one beside a failed run is
        // the same file.
        let (mut file, temp_path, tmp_path) = temp_file_in(path).await?;

        let planned_count = u64::try_from(plan.len()).unwrap_or(0);
        let planned_bytes = planned_payload_len(plan);

        let mut chunks = Vec::with_capacity(plan.len());
        let mut offset = 0u64;

        // The header is built before the payload starts, because its own length
        // decides where the payload starts.
        let header = Header {
            snapshot: snapshot_id,
            taken_at: snapshot.taken_at,
            version,
            base,
            chunks: planned_count,
            payload_len: planned_bytes,
        };

        // A re-encoded header reserves the widest a varint can be for the length
        // and keeps the position of that field. The patch after the payload then
        // does not restate the header's layout.
        let (header_bytes, reserved_len_at) = match route {
            Route::Verbatim => (encode_header(&header), None),
            Route::Reencoded => {
                let (bytes, at) = encode_header_padded(&header);
                (bytes, Some(at))
            }
        };
        let payload_start = u64::try_from(header_bytes.len()).unwrap_or(u64::MAX);

        file.write_all(&header_bytes).await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not write {}: {e}", tmp_path.display()),
            )
        })?;

        let planned_chunks = plan.chunks();
        let mut start = 0usize;
        while start < planned_chunks.len() {
            let end = start + window_end(&planned_chunks[start..]);
            let window = &planned_chunks[start..end];

            // Fetch the window at the same time.
            // The writes follow in plan order once the window is in hand.
            // The first error in plan order wins.
            let fetched = fetch_window(store, window).await?;
            for (planned, stored) in window.iter().zip(fetched) {
                let (mut located, record_len) =
                    write_fetched(&mut file, planned, &stored, route).await?;
                located.offset = offset;

                // Saturating, to match the count the header carries. The
                // header reports one total and the index is built on the other. A plain
                // `+=` would panic where both would have clamped.
                offset = offset.saturating_add(record_len);
                chunks.push(located);
            }

            start = end;
        }

        // Between the payloads and the index, so `index length` alone
        // still locates the index. See the module docs.
        let snapshot_start = payload_start.saturating_add(offset);

        // Put the payload's real length where a re-encoded header reserved it.
        // The reserved width is the widest a varint can be, so the patch cannot
        // move a byte of what follows it.
        if let Some(at) = reserved_len_at {
            let mut length = Vec::with_capacity(crate::varint::MAX);
            crate::varint::write_padded(&mut length, offset);

            file.seek(SeekFrom::Start(u64::try_from(at).unwrap_or(u64::MAX)))
                .await?;
            file.write_all(&length).await?;

            // The cursor is inside the header now. The sections that follow
            // start where the payload ends.
            file.seek(SeekFrom::Start(snapshot_start)).await?;
        }

        file.write_all(&snapshot_bytes).await?;

        let index = encode_index(
            chunks
                .iter()
                .map(|chunk| (chunk.id, chunk.offset, chunk.content_len)),
        );

        file.write_all(&index).await?;
        file.write_all(&encode_trailer(
            u64::try_from(index.len()).unwrap_or(0),
            u64::try_from(snapshot_bytes.len()).unwrap_or(0),
        ))
        .await?;
        file.write_all(&MAGIC).await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not write {}: {e}", tmp_path.display()),
            )
        })?;
        file.sync_all().await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not sync {}: {e}", tmp_path.display()),
            )
        })?;
        drop(file);

        // Into place only once it is whole, so a run that fails part-way
        // leaves the pack that was already at this path alone. It does not replace
        // that pack with a file that cannot be opened. `blu pack` overwrites,
        // so the path is usually one a previous run wrote and somebody wants back.
        temp_path.persist(path).map_err(|e| {
            io::Error::new(
                e.error.kind(),
                format!(
                    "could not rename temporary into {}: {}",
                    path.display(),
                    e.error
                ),
            )
        })?;

        // Sorted by hash for everything that is not the file. The index on
        // disk keeps payload order, because that is the order the records were
        // written in and a reader sorts what it needs. Everything in this process
        // looks a chunk up by hash, and wants the two to agree on what order that
        // is. A pack that was just written and the same pack read back are the
        // same value, which is a thing callers compare.
        chunks.sort_unstable_by(|a, b| a.id.as_bytes().cmp(b.id.as_bytes()));

        // `order` is the way back, and it is built from the sort rather
        // than from the appends before it. It holds positions into `chunks`, which
        // the sort has just moved. The same four lines run in `open`, so a pack
        // read back has the same `order` as the one that was written.
        let order = payload_order(&chunks, path)?;

        Ok(Self {
            path: path.to_path_buf(),
            payload_start,
            payload_len: offset,
            version,
            snapshot: snapshot_id,
            taken_at: snapshot.taken_at,
            base,
            snapshot_start,
            snapshot_len: u64::try_from(snapshot_bytes.len()).unwrap_or(0),
            chunks,
            order,
        })
    }
    /// Open a pack, reading only its trailer and index.
    ///
    /// Cost is proportional to the index and not to the payload, which is the
    /// point of putting the index at the end. A client can run this before it
    /// knows whether it wants the pack. The snapshot is located but **not read**.
    /// A client asking only [`Pack::needed`] never pays for it, and one that does
    /// want it calls [`Pack::snapshot`].
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, measured, seeked or read.
    /// It returns an error if the file is too short to hold a trailer. It returns
    /// an error if the file does not end with the pack magic. It returns an error
    /// if the file does not begin with one. It returns an error if the file is a
    /// format version this build does not read, or if its index does not decode. A
    /// header that disagrees with its own index, on the chunk count or on where
    /// the payload ends, is refused rather than believed.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = crate::storage::at(path, "open", fs::File::open(path))?;
        let len = file.metadata()?.len();

        // The flag byte and the fixed part, plus a trailer. A delta's
        // base hash is read below and checked against the section lengths there.
        // This floor is for the shortest pack there is.
        if len < u64::try_from(HEADER + 1 + TRAILER).unwrap_or(u64::MAX) {
            return Err(not_a_pack(path, "it is too short to hold a pack"));
        }

        let mut trailer = [0u8; TRAILER];
        file.seek(SeekFrom::Start(
            len - u64::try_from(TRAILER).unwrap_or(u64::MAX),
        ))?;
        file.read_exact(&mut trailer)?;

        if trailer[16..] != MAGIC {
            return Err(not_a_pack(
                path,
                "it does not end with a pack's magic, so it is truncated or is not one",
            ));
        }

        let index_len = u64::from_le_bytes(trailer[..8].try_into().unwrap_or([0; 8]));
        let snapshot_len = u64::from_le_bytes(trailer[8..16].try_into().unwrap_or([0; 8]));

        // Both lengths are claims from the file, and a file can lie about
        // them. A sparse file reports any length it likes. So each is subtracted
        // from the end of the file it bounds, rather than added to a computed
        // offset.
        let index_start = len
            .checked_sub(u64::try_from(TRAILER).unwrap_or(u64::MAX))
            .and_then(|e| e.checked_sub(index_len))
            .ok_or_else(|| not_a_pack(path, "its index length runs past the start of the file"))?;

        let snapshot_start = index_start.checked_sub(snapshot_len).ok_or_else(|| {
            not_a_pack(path, "its snapshot length runs past the start of the file")
        })?;

        let mut index = crate::storage::zeroed(index_len, &format!("{}'s index", path.display()))?;
        file.seek(SeekFrom::Start(index_start))?;
        file.read_exact(&mut index)?;

        let mut chunks = decode_index(&index)?;
        chunks.sort_unstable_by(|a, b| a.id.as_bytes().cmp(b.id.as_bytes()));

        // The header, for the payload geometry and the snapshot.
        let mut header = [0u8; HEADER + 1];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;

        if header[..4] != MAGIC {
            return Err(not_a_pack(path, "it does not begin with a pack's magic"));
        }

        let format = u32::from_le_bytes(header[4..8].try_into().unwrap_or([0; 4]));
        if format != FORMAT_VERSION {
            return Err(not_a_pack(
                path,
                &format!("it is pack format {format}, but this build reads {FORMAT_VERSION}"),
            ));
        }

        // Read only after the format check above, which is what makes
        // the refusal for an older pack happen before anything downstream looks at
        // the bytes that follow. A `3` has no version here, and its four bytes
        // that would be read as one are the first four of its snapshot hash.
        let version = u32::from_le_bytes(header[8..12].try_into().unwrap_or([0; 4]));

        let mut snapshot_raw = [0u8; 32];
        snapshot_raw.copy_from_slice(&header[12..44]);
        let snapshot = BlobId::from_bytes(snapshot_raw);
        let taken_at = header_taken_at(&header);

        // A base hash, when this is a delta. Read before the counts, because its
        // presence is what decides where they start. Inferred rather than declared
        // it would be indistinguishable from a small full pack. A delta's payload
        // is short for the ordinary reason that it holds fewer chunks.
        let (base, after_base) = read_base(&mut file, &header, path)?;

        // The two varints, which are part of the header. Reading the fixed
        // part alone would start the payload at the wrong offset, shifting every
        // chunk rather than failing. Only what lies between the fixed part and the
        // index is read, since a pack of an empty plan has two one-byte counts
        // there and no more.
        let mut head = [0u8; COUNTS_MAX];
        // Against the snapshot's start, for the same reason the payload
        // geometry is checked against it below.
        let available =
            snapshot_start.saturating_sub(u64::try_from(after_base).unwrap_or(u64::MAX));
        let to_read = usize::try_from(available.min(u64::try_from(COUNTS_MAX).unwrap_or(u64::MAX)))
            .unwrap_or(0);
        file.seek(SeekFrom::Start(
            u64::try_from(after_base).unwrap_or(u64::MAX),
        ))?;
        file.read_exact(&mut head[..to_read])?;

        let (count, count_len) = read_varint(&head, 0)?;
        let (payload_len, payload_len_len) = read_varint(&head, count_len)?;

        let payload_start = u64::try_from(after_base)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(count_len).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(payload_len_len).unwrap_or(u64::MAX));

        // The counts are redundant with the index, which is what locates
        // chunks, so they are a claim to check rather than a source of truth.
        if count != u64::try_from(chunks.len()).unwrap_or(u64::MAX) {
            return Err(not_a_pack(
                path,
                &format!(
                    "its header claims {count} chunks but its index lists {}",
                    chunks.len()
                ),
            ));
        }

        // Against where the snapshot begins, not where the index does. The
        // snapshot sits between them, so the payload's real end is `snapshot_start`.
        // Checking it against `index_start` would be off by the whole snapshot.
        let end_of_payload = payload_start.saturating_add(payload_len);
        if end_of_payload != snapshot_start {
            return Err(not_a_pack(
                path,
                &format!(
                    "its header says the payload ends at {end_of_payload}, but the snapshot \
                     starts at {snapshot_start}"
                ),
            ));
        }

        let order = check_index_geometry(path, &chunks, payload_len)?;

        Ok(Self {
            path: path.to_path_buf(),
            payload_start,
            payload_len,
            version,
            snapshot,
            taken_at,
            base,
            snapshot_start,
            snapshot_len,
            chunks,
            order,
        })
    }

    /// Where the pack's payload region starts in the file.
    ///
    /// A [`Located::offset`] is relative to this, so a client turning a chunk into
    /// a byte range adds it. This is not a constant, because the header's counts
    /// are varints.
    #[must_use]
    pub fn payload_start(&self) -> u64 {
        self.payload_start
    }

    /// The version this pack was built from, in the set that built it.
    ///
    /// **The origin set's numbering, not a coordinate in this one.** Two sets
    /// number their versions independently. This says which version of the set
    /// the pack was packed from. It says nothing about where it lands here.
    /// Merging
    /// a pack of origin version 3 into a set on version 7 publishes version 8, and
    /// the 3 is not the 8. It is good for noticing that a pack has fallen behind.
    /// Compared against the receiving set's tip, a number below it means the sender
    /// was working from an older version. It is also good for saying so at the
    /// moment a transfer arrives, which is the one moment somebody can act on it.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The hash of the snapshot this pack holds.
    ///
    /// It is also the name the store gives that version. A client holding a
    /// snapshot from elsewhere can check that this pack carries the same one. The
    /// client checks before trusting either. Reading the snapshot itself is
    /// [`Pack::snapshot`].
    #[must_use]
    pub fn snapshot_id(&self) -> BlobId {
        self.snapshot
    }

    /// Time the packed version ran, in UTC seconds and nanos.
    ///
    /// The value matches the stamp inside the carried snapshot.
    #[must_use]
    pub fn taken_at(&self) -> Timestamp {
        self.taken_at
    }

    /// The version this pack assumes its reader already holds, if it is a delta.
    ///
    /// `None` for a full pack, which needs no such accounting.
    ///
    /// A delta omits the chunks this names, and cannot check that its reader has
    /// them. The bytes that would prove it are the bytes it left out. So a reader
    /// has to establish the base *itself* and refuse on a mismatch. A reader can
    /// do that in two ways. One way is a record of what the last apply left
    /// beside the destination. The other is a full read of the tree. Nothing
    /// about the pack makes this free, and nothing makes it skippable.
    #[must_use]
    pub fn base(&self) -> Option<BlobId> {
        self.base
    }

    /// The version this pack holds, read out of the pack and decoded.
    ///
    /// The file table travels with the payload, so this is everything needed to
    /// apply the pack. It says which files exist, how long each is, and which
    /// slice of which chunk lands at which offset. It is read on demand rather
    /// than at [`Pack::open`], because a client that only wants to know *which
    /// ranges it needs* never decodes a version. [`Pack::needed`] is the cheaper
    /// question to ask.
    ///
    /// **Checked, not believed.** The bytes are hashed and compared against
    /// [`Pack::snapshot_id`] before they are parsed. A pack whose snapshot
    /// section is damaged or edited is refused here. It is not decoded into a
    /// file table for a directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read. It returns an error if
    /// its bytes do not hash to the header's snapshot hash. It returns an error
    /// if they do not decode as a snapshot.
    pub fn snapshot(&self) -> io::Result<crate::metadata::Snapshot> {
        let bytes = self.snapshot_bytes()?;

        if blake3::hash(&bytes) != self.snapshot {
            return Err(not_a_pack(
                &self.path,
                "its snapshot does not hash to the one its header names",
            ));
        }

        crate::metadata::Snapshot::try_from(bytes.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Where the snapshot's own bytes start in the file.
    ///
    /// A client holding the pack somewhere else entirely, such as an object store or
    /// a CDN, fetches this range to get the file table. It is bounded by
    /// [`Pack::snapshot_len`] rather than by the index, so it is one range and not
    /// the rest of the file.
    #[must_use]
    pub fn snapshot_start(&self) -> u64 {
        self.snapshot_start
    }

    /// How many bytes of file table the pack carries.
    #[must_use]
    pub fn snapshot_len(&self) -> u64 {
        self.snapshot_len
    }

    /// The snapshot's own bytes, as the pack carries them.
    ///
    /// The encoded form, not a [`crate::metadata::Snapshot`]. A client with its own
    /// copy can compare against [`Pack::snapshot_id`] without paying to decode one.
    ///
    /// # Errors
    ///
    /// Returns an error if the section cannot be read.
    pub fn snapshot_bytes(&self) -> io::Result<Vec<u8>> {
        let mut file = crate::storage::at(&self.path, "open", fs::File::open(&self.path))?;

        let mut bytes = crate::storage::zeroed(
            self.snapshot_len,
            &format!("{}'s snapshot", self.path.display()),
        )?;
        file.seek(SeekFrom::Start(self.snapshot_start))?;
        file.read_exact(&mut bytes)?;

        Ok(bytes)
    }

    /// Where the pack is on disk.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Every chunk, in hash order.
    #[must_use]
    pub fn chunks(&self) -> &[Located] {
        &self.chunks
    }

    /// The chunks, in hash order.
    ///
    /// [`Pack`] is also [`IntoIterator`] and [`AsRef<[Located]>`], so
    /// `for chunk in &pack` works without the `pack.chunks().iter()` double hop.
    ///
    /// **Hash order, not payload order.** A lookup binary-searches this order.
    /// [`Pack::open`] reads a pack's index into this order. The order on disk
    /// does not matter. A question about byte ranges sorts by offset
    /// itself rather than reading an order into this.
    pub fn iter(&self) -> std::slice::Iter<'_, Located> {
        self.chunks.iter()
    }

    /// How many chunks the pack holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Whether the pack holds no chunks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Bytes of payload, which is the file minus its header and index.
    #[must_use]
    pub fn payload_len(&self) -> u64 {
        self.payload_len
    }

    /// The file's total size.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be measured.
    pub fn file_len(&self) -> io::Result<u64> {
        fs::metadata(&self.path).map(|m| m.len())
    }

    /// The bytes a client not holding `have` still needs, as half-open ranges in
    /// *file* coordinates, which are [`Pack::payload_start`] plus a
    /// [`Located::offset`].
    ///
    /// Stated as what the client lacks. Holding every chunk gets nothing, and
    /// holding nothing gets the whole payload region as one range. Adjacent ranges
    /// are merged, which the payload's layout makes worthwhile. Chunks a client
    /// needs are written end to end. A run of them is one range. The record
    /// header between two of them is inside the run, not across it.
    ///
    /// **A range's end comes from the next record in the payload, not from a
    /// length.** The index does not carry the record's length. The record's own
    /// header does, and a range has to be answerable before a byte of payload has
    /// been read. So a chunk's record runs from its own offset to the start of
    /// the record after it. The last record runs to the end of the payload.
    /// That is only sound because the records do not overlap, which
    /// [`Pack::open`] checks
    /// and this crate's writer makes true.
    ///
    /// **And so the entries are walked in payload order.** The index is held in
    /// hash order, because that is what [`Pack::locate`] binary-searches. Hash
    /// order is not payload order, so a merge that ran along the index would find
    /// two ranges meeting nowhere. It would answer with one range per chunk, which
    /// is the entire cost a pack exists to remove. The order to walk is the one
    /// `open` built while checking that records tile. A pack that was just
    /// written carries that same order.
    ///
    /// A set rather than anything ordered, because the set is only ever asked
    /// whether it holds a chunk. The order of the result comes from the order of
    /// the payload, not from the set, so nothing observable depends on it. Also,
    /// `BlobId` is a `blake3::Hash`, which deliberately has no `Ord`. Hash order
    /// is not content order, and pretending otherwise would be a lie a caller
    /// could come to rely on.
    #[must_use]
    pub fn needed(&self, have: &HashSet<BlobId>) -> Vec<Range> {
        let mut ranges: Vec<Range> = Vec::new();

        for (chunk, end) in self.records() {
            if have.contains(&chunk.id) {
                continue;
            }

            // Saturating for the same reason `open` checks it. These two
            // sums are over numbers that came out of the index, and this type is
            // what a client is told to add them up with.
            let start = self.payload_start.saturating_add(chunk.offset);

            match ranges.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => ranges.push(Range { start, end }),
            }
        }

        ranges
    }

    /// Every record in payload order, with the byte range it occupies.
    ///
    /// Over [`Pack::order`] rather than over the index, which is in hash order.
    /// The entries are the same ones either way and only the order differs, so
    /// nothing here copies an entry to walk it. Nothing is collected either. The
    /// two callers walk every record once between them, and the third, which asks
    /// about one record at a time, goes through [`Pack::after`].
    fn records(&self) -> impl Iterator<Item = (Located, u64)> + '_ {
        self.order.iter().enumerate().map(|(at, chunk)| {
            let chunk = position(*chunk);
            // The next record's start, which for the last one is the payload's end.
            let end = self
                .order
                .get(at + 1)
                .map_or(self.payload_len, |next| self.chunks[position(*next)].offset);

            (self.chunks[chunk], self.payload_start.saturating_add(end))
        })
    }

    /// Bytes a client already holding `have` still needs.
    #[must_use]
    pub fn needed_bytes(&self, have: &HashSet<BlobId>) -> u64 {
        // Folded rather than `sum()`, for the same reason. A total
        // that wrapped would report that a client needs fewer bytes than the ranges
        // describe.
        self.needed(have)
            .iter()
            .fold(0u64, |total, range| total.saturating_add(range.len()))
    }

    /// Where a chunk is, if the pack holds it.
    ///
    /// A binary search over [`Pack::chunks`], which is why that is sorted by hash.
    ///
    /// **Why not a hash map from chunk to position.** A hash map is the
    /// obvious thing for a lookup table. It is worse on both counts that matter
    /// here. Memory first. The index is already resident in full after
    /// [`Pack::open`], because a client that only wants offsets pays for the whole
    /// of it either way. A map would be a second copy of every key, not a
    /// structure that exists instead of one. The index's own memory is not saved by
    /// looking things up in something cheaper. A second structure is added to it.
    /// Then speed. A map lookup hashes 32 bytes and chases a bucket. A binary
    /// search here compares 32 bytes about `log2(n)` times. That data is
    /// already in cache. For the sizes a pack reaches, thousands of chunks or tens
    /// of thousands, the difference is a handful of cache lines. A restore pays
    /// that difference on every chunk it reads.
    #[must_use]
    pub fn locate(&self, id: &BlobId) -> Option<Located> {
        self.position(id).map(|at| self.chunks[at])
    }

    /// Where a chunk sits in the index, which is what the range questions need.
    ///
    /// Not `locate`'s answer. `needed` and `chunks_covering` have to know which
    /// record a chunk's is. That is a position in the index, not a copy
    /// of an entry. The span of one record is a fact about where the next one
    /// starts, which an entry no longer says on its own.
    fn position(&self, id: &BlobId) -> Option<usize> {
        self.chunks
            .binary_search_by(|chunk| chunk.id.as_bytes().cmp(id.as_bytes()))
            .ok()
    }

    /// Where the record after the one at `offset` begins, or the payload's end.
    ///
    /// **The next record's offset, not a length.** The index does not carry each
    /// record's length. The record's own header does, and whoever reads the record
    /// reads the header. So the extent comes from the layout instead. The writer
    /// lays records end to end. [`Pack::open`] refuses an index whose records
    /// overlap. So a record runs to the record after it. The last one runs to
    /// the end of the payload.
    ///
    /// A binary search over [`Pack::order`], which is the shape [`Pack::records`]
    /// walks. Both ask the same question of the same list, and there is one list
    /// because it was built when the pack was opened. It used to be a linear scan
    /// for the smallest offset above this one. The argument was that it is one
    /// call per chunk read, and so `O(n)` overall. That is true of a scan and
    /// false of a
    /// restore. A restore reads the chunks of a whole version one after
    /// another. Each one pays the whole index again.
    ///
    /// The last case is where an over-claim rather than a short answer comes from.
    /// A writer that left a gap before the snapshot would have the last record's
    /// range reach past its own end. A client then fetches a few bytes it does not
    /// need. Under-claiming would be the other way round and cannot happen, which
    /// is the direction worth being wrong in.
    fn after(&self, offset: u64) -> u64 {
        let next = self
            .order
            .partition_point(|at| self.chunks[position(*at)].offset <= offset);

        self.order
            .get(next)
            .map_or(self.payload_len, |at| self.chunks[position(*at)].offset)
    }

    /// The bytes the record at `offset` occupies.
    fn record_span(&self, offset: u64) -> u64 {
        self.after(offset).saturating_sub(offset)
    }
}

/// Bytes the records for `plan` will occupy, before a byte of them is read.
///
/// Also what `blu pack --dry-run` reports, which is why it is not folded
/// into the writer. A figure the report gives and a figure the file carries are
/// only the same figure if someone computes them once.
///
/// Every term comes from the plan's own placement, which the caller resolved
/// against the store being packed. A record's width is decided by the two lengths
/// it carries, and those are known from the store's index without reading a
/// payload. That is what lets a verbatim header come first and still carry the
/// right total. It is also why the writer checks the bytes against the same
/// numbers, rather than trusting that they still hold.
///
/// **A re-encoded pack's total is not this figure.** The run makes its records
/// while it writes them, so the writer reserves the length's width and patches
/// the real total in afterwards. See [`Route::Reencoded`]. This figure is still
/// what a dry run reports and what a verbatim header carries from the start.
///
/// Saturating throughout, because the header's count and the index built beside
/// it are both sums of claims.
pub(crate) fn planned_payload_len(plan: &Plan) -> u64 {
    plan.chunks().iter().fold(0u64, |total, planned| {
        total
            .saturating_add(crate::storage::header_len(
                planned.placement.stored_len,
                planned.placement.raw_len,
            ))
            .saturating_add(planned.placement.stored_len)
    })
}

/// Where a window of fetched chunks ends.
///
/// The window holds at most [`PREFETCH_WINDOW`] chunks.
/// The window holds at most [`PREFETCH_BYTES`] bytes of chunk data.
/// The window fetches a chunk that is larger than the budget on its own.
///
/// The size of a chunk comes from the plan, so the writer knows it before it
/// reads the chunk.
fn window_end(window: &[PlannedChunk]) -> usize {
    let mut bytes = 0u64;
    for (at, planned) in window.iter().enumerate() {
        if at == PREFETCH_WINDOW {
            return at;
        }
        let len = planned.placement.stored_len;
        if at > 0 && bytes.saturating_add(len) > PREFETCH_BYTES {
            return at;
        }
        bytes = bytes.saturating_add(len);
    }
    window.len()
}

/// Fetch the chunks of one window at the same time.
///
/// The window holds every chunk it fetched, in plan order.
/// The window reports the first error in plan order.
/// The window stops waiting for the other fetches once it knows that error.
/// The window drops the other fetches, so their results are not used.
///
/// # Errors
///
/// Returns an error if a fetch fails.
/// The error is the first one in plan order.
async fn fetch_window<S>(store: &S, window: &[PlannedChunk]) -> io::Result<Vec<Stored>>
where
    S: StorageStored + ?Sized,
{
    let mut pending = futures_util::stream::FuturesUnordered::new();
    for (at, planned) in window.iter().enumerate() {
        pending.push(async move { (at, store.stored(&planned.id).await) });
    }

    // The chunk of every fetch, in plan order.
    // A slot stays empty until its fetch finishes.
    let mut fetched: Vec<Option<Stored>> = vec![None; window.len()];
    // The first failure in plan order, and the place it sits.
    let mut first_failure: Option<(usize, io::Error)> = None;

    while let Some((at, result)) = pending.next().await {
        match result {
            Ok(stored) => fetched[at] = Some(stored),
            Err(error) => {
                if first_failure
                    .as_ref()
                    .is_none_or(|(earlier, _)| at < *earlier)
                {
                    first_failure = Some((at, error));
                }
            }
        }

        let Some((earlier, _)) = first_failure.as_ref() else {
            continue;
        };
        // A fetch before the first failure may fail earlier in plan order, so
        // the window waits for those fetches. A fetch after it cannot be the
        // first failure, so the window stops as soon as the earlier fetches
        // have finished. The window drops the fetches it does not wait for.
        if fetched[..*earlier].iter().all(Option::is_some) {
            break;
        }
    }

    if let Some((_, error)) = first_failure {
        return Err(error);
    }

    // Every fetch of the window finished, so every slot holds its chunk.
    Ok(fetched.into_iter().flatten().collect())
}

/// Write one fetched chunk.
///
/// Compare the bytes with the plan.
/// Encode the header.
/// Append the record.
///
/// **What the comparison can see depends on the route.** Under
/// [`Route::Verbatim`] the record is the form the plan resolved. The check then
/// covers the codec, the stored length, and the content length. Under
/// [`Route::Reencoded`] the form is this run's own output, and the plan says
/// nothing about it. The check then covers the content length only. Both the
/// store and the plan make that one claim about a chunk under either route.
///
/// # Errors
///
/// Return an error if the bytes differ from the plan.
/// Return an error if the file write fails.
async fn write_fetched(
    file: &mut tfs::File,
    planned: &PlannedChunk,
    stored: &Stored,
    route: Route,
) -> io::Result<(Located, u64)> {
    let stored_len = u64::try_from(stored.bytes.len()).unwrap_or(u64::MAX);

    // Checked rather than assumed, because the header's payload length
    // was worked out from the plan's placements. This is the only place the store
    // gets to disagree with them. It can disagree, because a set is appended to
    // while a pack of it is written. A store that moved under the run would
    // otherwise produce a pack whose header contradicts its own records. `open`
    // refuses that pack, naming a count, long after the run that caused it had
    // exited successfully.
    let placement = planned.placement;
    let disagrees = stored.raw_len != placement.raw_len
        || (route == Route::Verbatim
            && (stored.codec != placement.codec || stored_len != placement.stored_len));
    if disagrees {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            match route {
                Route::Verbatim => format!(
                    "chunk {} is stored as {} bytes of {} decoding to {} here, but the plan \
                 resolved it as {} bytes of {} decoding to {}; the set changed while this \
                 pack was being written",
                    planned.id,
                    stored_len,
                    name_of(stored.codec),
                    stored.raw_len,
                    placement.stored_len,
                    name_of(placement.codec),
                    placement.raw_len,
                ),
                Route::Reencoded => format!(
                    "chunk {} holds {} bytes of content here, but the plan resolved it as \
                 {}; the set changed while this pack was being written",
                    planned.id, stored.raw_len, placement.raw_len,
                ),
            },
        ));
    }

    // The store's own encoder, so these bytes are what a store
    // record would be. Hand-rolling the header here would make the pack a second
    // record format that agreed with the first until the first moved.
    let mut record = Vec::with_capacity(
        usize::try_from(crate::storage::header_len(stored_len, stored.raw_len)).unwrap_or(0),
    );
    crate::storage::encode_header(
        &mut record,
        crate::storage::RecordKind::Blob,
        stored.codec,
        planned.id.as_bytes(),
        stored_len,
        stored.raw_len,
    );

    file.write_all(&record).await?;
    file.write_all(&stored.bytes).await?;

    Ok((
        Located {
            id: planned.id,
            offset: 0,
            content_len: stored.raw_len,
        },
        u64::try_from(record.len())
            .unwrap_or(u64::MAX)
            .saturating_add(stored_len),
    ))
}

/// Build a temporary pack file beside `path`, on a blocking thread.
///
/// Use the same directory, because `rename` stays atomic in one filesystem.
/// The file disappears on drop unless the call persists it.
/// A temporary beside a finished pack has no meaning.
/// A temporary beside a failed run is the same file.
///
/// The call creates the file on a blocking thread.
/// The unlink on drop stays synchronous, because a drop cannot await.
async fn temp_file_in(path: &Path) -> io::Result<(tfs::File, tempfile::TempPath, PathBuf)> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        let tmp = tempfile::NamedTempFile::new_in(&dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not create temporary in {}: {e}", dir.display()),
            )
        })?;
        let tmp_path = tmp.path().to_path_buf();
        let (file, temp_path) = tmp.into_parts();
        Ok((tfs::File::from_std(file), temp_path, tmp_path))
    })
    .await
    .map_err(|join| io::Error::other(format!("a temporary-file task failed: {join}")))?
}

/// How a codec is named in a message about two of them disagreeing.
///
/// Spelled out rather than derived, because `{:?}` would print a variant name that
/// means nothing to anybody reading an error about a file they downloaded.
const fn name_of(codec: Codec) -> &'static str {
    match codec {
        Codec::Raw => "raw content",
        Codec::Zstd => "a zstd frame",
    }
}
impl AsRef<[Located]> for Pack {
    fn as_ref(&self) -> &[Located] {
        &self.chunks
    }
}

impl<'a> IntoIterator for &'a Pack {
    type Item = &'a Located;
    type IntoIter = std::slice::Iter<'a, Located>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.iter()
    }
}

impl IntoIterator for Pack {
    type Item = Located;
    type IntoIter = std::vec::IntoIter<Located>;

    /// Takes the index rather than copying it.
    fn into_iter(self) -> Self::IntoIter {
        self.chunks.into_iter()
    }
}

/// The header's fixed part plus its two varint counts.
/// Read a delta's base version, and where the counts after it begin.
///
/// A flag byte rather than an inferred length. A delta's payload is short
/// because it holds fewer chunks. A small full pack and a
/// delta cannot be told apart by length.
fn read_base(
    file: &mut fs::File,
    header: &[u8],
    path: &Path,
) -> io::Result<(Option<BlobId>, usize)> {
    let flags = *header
        .get(HEADER)
        .ok_or_else(|| not_a_pack(path, "its header is too short to hold a flag byte"))?;

    let after_flag = HEADER + 1;
    if flags & BASE_FLAG == 0 {
        return Ok((None, after_flag));
    }

    let mut raw = [0u8; 32];
    file.seek(SeekFrom::Start(
        u64::try_from(after_flag).unwrap_or(u64::MAX),
    ))?;
    file.read_exact(&mut raw)?;

    Ok((Some(BlobId::from_bytes(raw)), after_flag + 32))
}

/// The version time in a pack header, read from its fixed offsets.
///
/// Twelve bytes after the snapshot hash: eight of seconds, four of nanos.
/// The caller passes the full fixed header. [`Pack::open`] always holds it.
fn header_taken_at(header: &[u8]) -> Timestamp {
    let mut secs = [0u8; 8];
    secs.copy_from_slice(&header[44..52]);
    let mut nanos = [0u8; 4];
    nanos.copy_from_slice(&header[52..56]);
    Timestamp {
        secs: i64::from_le_bytes(secs),
        nanos: u32::from_le_bytes(nanos),
    }
}

/// A header's fixed part, its flag byte, and its chunk count.
///
/// A header also holds the payload length. [`encode_header`] or
/// [`encode_header_padded`] appends that field, and a header comes in those two
/// shapes.
fn encode_header_prefix(header: &Header) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + COUNTS_MAX);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&header.version.to_le_bytes());
    out.extend_from_slice(header.snapshot.as_bytes());
    out.extend_from_slice(&header.taken_at.secs.to_le_bytes());
    out.extend_from_slice(&header.taken_at.nanos.to_le_bytes());
    // Always written, so the counts start at a fixed distance from the
    // front for both kinds of pack. A reader that had to infer this would infer
    // it from a payload length, which a small full pack and a delta cannot be
    // told apart by.
    match header.base {
        Some(base) => {
            out.extend_from_slice(&[BASE_FLAG]);
            out.extend_from_slice(base.as_bytes());
        }
        None => out.push(0),
    }
    write_varint(&mut out, header.chunks);

    out
}

/// A header whose payload length is the shortest varint that holds it.
///
/// This is the shape a reader finds in every pack on disk except one written by
/// [`Route::Reencoded`]. The length is final when the writer starts the header,
/// so there is nothing to patch afterwards.
fn encode_header(header: &Header) -> Vec<u8> {
    let mut out = encode_header_prefix(header);
    write_varint(&mut out, header.payload_len);

    out
}

/// A header that pads its payload length to [`crate::varint::MAX`] bytes, and
/// the offset the length starts at.
///
/// The padded width is the widest a `u64` can occupy. The writer can then seek
/// back and overwrite the field once the payload is on disk. The length does not
/// then move a byte of what follows it. The offset travels with the bytes, so
/// nothing else has to know where the field is.
///
/// A reader needs no change. A padded varint is an ordinary LEB128 number. It
/// happens to end in its tenth byte, and [`read_varint`] stops there like it
/// stops anywhere.
fn encode_header_padded(header: &Header) -> (Vec<u8>, usize) {
    let mut out = encode_header_prefix(header);
    let at = out.len();
    crate::varint::write_padded(&mut out, header.payload_len);

    (out, at)
}

/// What a header records, and how much of it is fixed-length.
///
/// The counts are varints and so are not fixed, but they come last. Everything a
/// reader needs to find the payload is a fixed prefix. That is what lets [`open`]
/// read the geometry without decoding the counts first.
///
/// A re-encoded header pads its payload length, so its counts region has a fixed
/// width too. A reader stops at the end of each number rather than at a width, so
/// both shapes of header read the same way.
struct Header {
    snapshot: BlobId,
    taken_at: Timestamp,
    version: u32,
    base: Option<BlobId>,
    chunks: u64,
    payload_len: u64,
}

/// The index: one entry per chunk, in pack order.
///
/// The two numbers are the record's offset and the chunk's content length. The
/// record's own length is *not* here. It is in the record's header, which is the
/// first thing the record holds. The index says where the records begin rather than
/// how long each one is.
///
/// Each entry is a chunk hash, an offset and a content length.
/// One encoder serves the writer and the virtual pack.
/// Two encoders would drift apart, and the pack bytes would depend on which one
/// ran.
fn encode_index(entries: impl Iterator<Item = (BlobId, u64, u64)>) -> Vec<u8> {
    let mut out = Vec::with_capacity(entries.size_hint().0.saturating_mul(32 + 16));
    for (id, offset, content_len) in entries {
        out.extend_from_slice(id.as_bytes());
        write_varint(&mut out, offset);
        write_varint(&mut out, content_len);
    }

    out
}

/// Seed for the fold behind the duplicate scan.
///
/// Standard sets pair random keys with a `SipHash` fold. The random keys block
/// crafted input from forcing collisions. The `SipHash` fold costs more per
/// hash than the scan around it. This keeps the random keys and replaces the
/// fold with a cheaper one.
///
/// Each table takes one seed when the scan builds it. A later open never
/// reuses that seed.
struct IndexSeed(u64);

impl IndexSeed {
    /// Generate one fresh seed for one duplicate scan.
    ///
    /// A throwaway standard builder supplies the seed.
    fn fresh() -> Self {
        Self(RandomState::new().build_hasher().finish())
    }
}

impl BuildHasher for IndexSeed {
    type Hasher = IndexFold;

    fn build_hasher(&self) -> Self::Hasher {
        IndexFold(self.0)
    }
}

/// Fold for [`IndexSeed`].
///
/// The fold mixes each 8-byte lane once with a rotate, a xor and a multiply.
/// Short input spreads across buckets without further passes.
struct IndexFold(u64);

impl IndexFold {
    /// Mix one lane into the running state.
    ///
    /// A rotate spreads nearby bits. A xor merges the lane with the state. A
    /// multiply spreads the result across the state.
    fn mix(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl Hasher for IndexFold {
    fn write(&mut self, bytes: &[u8]) {
        let (lanes, rest) = bytes.as_chunks::<8>();
        for lane in lanes {
            self.mix(u64::from_le_bytes(*lane));
        }
        if !rest.is_empty() {
            let mut tail = [0u8; 8];
            tail[..rest.len()].copy_from_slice(rest);
            self.mix(u64::from_le_bytes(tail));
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// The index read back.
///
/// # Errors
///
/// Returns an error if a 32-byte hash runs past the end of `bytes`. It returns
/// an error if either of the two varints runs past the end of `bytes`. It
/// returns an error if a number is overlong or overflows. It returns an error
/// if a hash is repeated. A repeated hash would mean two chunks at one offset.
///
/// **The scan reserves the table up front.** The index length gives an upper
/// bound on the number of entries. The table never regrows while the scan runs.
///
/// **Each open seeds the fold.** The scan generates a fresh seed. A crafted
/// pack cannot compute where a hash lands, so it cannot fill one bucket on
/// purpose.
///
/// **The duplicate check does not change.** The table compares the hashes
/// themselves, so a repeated hash is still reported at the same place with the
/// same message. The fold only picks the bucket.
fn decode_index(bytes: &[u8]) -> io::Result<Vec<Located>> {
    let mut out = Vec::new();
    // An upper bound on the entry count. Each entry holds 32 bytes of hash
    // and two varints of at least one byte each, so this never undercounts.
    // The table below then never regrows while the scan runs.
    //
    // The bound can overcount, because an entry with a long payload holds more
    // than 34 bytes. The table then holds more buckets than it needs. The
    // memory it takes is a few percent of the index.
    //
    // A collision costs a probe, not a mistake. The table compares the hashes
    // themselves, so a repeated hash is still a repeated hash.
    let mut seen = HashSet::with_capacity_and_hasher(bytes.len() / 34, IndexSeed::fresh());
    let mut at = 0usize;

    while at < bytes.len() {
        let truncated = || io::Error::new(io::ErrorKind::InvalidData, "a pack index is truncated");

        let end = at.checked_add(32).ok_or_else(truncated)?;
        let mut raw = [0u8; 32];
        raw.copy_from_slice(bytes.get(at..end).ok_or_else(truncated)?);
        at = end;
        // A 32-byte wire field, read into the type that addresses a chunk. The
        // slice above is exactly 32 bytes, so the array is fully initialised.
        let id = BlobId::from_bytes(raw);

        let (offset, used) = read_varint(bytes, at)?;
        at = at.checked_add(used).ok_or_else(truncated)?;
        let (content_len, used) = read_varint(bytes, at)?;
        at = at.checked_add(used).ok_or_else(truncated)?;

        // Two entries for one hash would mean the same chunk at two places, and
        // `locate` answers with the first. A client asking for a chunk it already
        // had would then be told to fetch a different one.
        if !seen.insert(id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a pack index lists the same chunk twice",
            ));
        }

        out.push(Located {
            id,
            offset,
            content_len,
        });
    }

    Ok(out)
}

fn write_varint(out: &mut Vec<u8>, value: u64) {
    crate::varint::write(out, value);
}

/// A LEB128 number at `at`, and how many bytes it occupied.
///
/// # Errors
///
/// Returns an error if the number runs past the end of `bytes`, is over ten
/// bytes, or would overflow a `u64`. Each is named, because a pack is a file a
/// client downloaded. "It is not readable" is not something a person can act on.
fn read_varint(bytes: &[u8], at: usize) -> io::Result<(u64, usize)> {
    crate::varint::read(bytes, at).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            match e {
                crate::varint::Error::Truncated => "a pack index ends mid-number",
                crate::varint::Error::Overlong => "a pack index has an overlong number",
            },
        )
    })
}

/// The two section lengths, in that order, for the trailer.
fn encode_trailer(index_len: u64, snapshot_len: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&index_len.to_le_bytes());
    out[8..].copy_from_slice(&snapshot_len.to_le_bytes());
    out
}

fn not_a_pack(path: &Path, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{} is not a pack this build can read: {why}",
            path.display()
        ),
    )
}

impl Pack {
    /// The chunks a client must already hold to avoid a range request.
    ///
    /// The inverse of [`Pack::needed`]. It takes a set of ranges and returns the
    /// chunks those ranges cover, so a client can record them after fetching and
    /// ask again. A range covering only part of a chunk is not that chunk and
    /// records nothing.
    ///
    /// A method rather than a free function taking a `&Pack`. It reads three
    /// of the pack's own fields. It is the other half of `needed`.
    /// As a free function the receiver was the first argument. The naming
    /// gave no hint that the two belonged together:
    ///
    /// ```text
    /// pack.needed(&have)                  // what the client lacks
    /// chunks_covering(&pack, &ranges)     // what the client now has
    /// ```
    ///
    /// A chunk is recorded only where a range covers its **whole record**,
    /// header included. That span runs from the record's own offset to the
    /// next record's, as in [`Pack::needed`]. The record is taken in payload
    /// order rather than in the order the index happens to hold.
    ///
    /// It sorts the ranges once. It sweeps the records in one pass. It keeps
    /// the furthest end seen so far. One range covers a record only where
    /// that range holds the whole span of the record. The answer matches a
    /// check of every record against every range.
    ///
    /// **One sort and one sweep replace the per-record scan.** The count is
    /// `O(n + m log m)`, where `n` is the record count and `m` is the range
    /// count. The old scan asked every record about every range, so its count
    /// was `O(n * m)`. Only the sort grows with the range count.
    ///
    /// **The win needs a plan with many ranges.** A synthetic plan with 2.17
    /// million ranges took 21.5 ms. The old scan took 24,037 ms for the same
    /// plan. That plan holds a copy of every range, so the sweep needs more
    /// memory than the old scan. A plan with one range gained nothing in the
    /// measurement. The two times were 1.26 ms and 1.45 ms, and the timer
    /// cannot separate them.
    ///
    /// **The sweep answers the same as the old scan.** The ranges need not be
    /// sorted. Two ranges may be the same range, or may overlap. The sort
    /// changes only the order in which the code asks about the ranges. The
    /// answer is a set of chunk ids, and a set keeps no order.
    #[must_use]
    pub fn chunks_covering(&self, ranges: &[Range]) -> HashSet<BlobId> {
        let mut ordered: Vec<Range> = ranges.to_vec();
        ordered.sort_by_key(|range| range.start);

        let mut out = HashSet::new();
        let mut seen = 0usize;
        let mut covered_until = 0u64;

        for (chunk, end) in self.records() {
            let start = self.payload_start.saturating_add(chunk.offset);

            while let Some(range) = ordered.get(seen) {
                if range.start > start {
                    break;
                }
                covered_until = covered_until.max(range.end);
                seen += 1;
            }

            if seen > 0 && covered_until >= end {
                out.insert(chunk.id);
            }
        }

        out
    }
}

/// A pack read as a store, so everything that consumes a store consumes a pack.
///
/// This is what lets the two halves of a backup meet. A restore takes a
/// [`Snapshot`] and a [`StorageGet`], and had only ever been handed a
/// [`crate::storage::BlobStore`]. The bytes it needed were therefore spread across
/// a segment file's record headers, one read per chunk. A pack, which holds exactly
/// the same bytes laid end to end, could not be restored from at all.
///
/// Implementing the trait rather than adding a parallel `restore_from_pack` is
/// the whole of the reuse. The restore verifies the snapshot, resolves every
/// recorded path, checks the slices account for each file, and then orders its
/// writes. None of that knows or cares that the bytes arrived from a different
/// place, and all of it would have to be written twice otherwise.
///
/// [`Snapshot`]: crate::metadata::Snapshot
impl StorageGet for Pack {
    /// Open the chunk with this content hash, out of the payload region.
    ///
    /// The stream yields the chunk's **content**, decoded if its record is a zstd
    /// frame, and is bounded to that content. A caller comparing against a length
    /// the snapshot recorded is therefore comparing like with like. A caller that
    /// reads past the end gets end of stream, not the next chunk's data.
    /// The stream is seekable within those bytes, which is what a chunk that a file
    /// uses only part of needs. `restore` seeks to the slice's offset within the
    /// chunk before reading.
    ///
    /// **The record's own header is read first, and it is what says how the
    /// payload is stored.** Nothing in the index duplicates the codec. A pack is a
    /// file that was downloaded, and a byte at an offset it claims is not this
    /// crate's to believe. The store's own decoder decodes the header. A header
    /// whose eight-byte check does not verify stops the chunk here. It is not
    /// read as content.
    ///
    /// A compressed chunk is buffered whole and served from memory, which is the
    /// same trade the store's read path makes for the same reason. A frame cannot
    /// seek backwards, and every consumer of a [`StorageGet`] stream seeks. The
    /// buffer is one chunk, bounded by the largest the chunker can produce. That
    /// bound is `fastcdc`'s maximum, not this crate's to raise, and the buffer is
    /// never the pack.
    ///
    /// A raw chunk is streamed straight out of the file. The stream is bounded
    /// to the content length. There is nothing to decode. A file handle costs
    /// nothing to keep.
    ///
    /// For the same chunk as the pack stores it, without the decode, see
    /// [`StorageStored::stored`]. Both open the one record the same way. They seek,
    /// read its header, and hold it to the index. They cannot disagree about which
    /// bytes it is.
    ///
    /// # Errors
    ///
    /// Returns an error if the pack does not hold that chunk. It returns an error
    /// if the record's header does not decode. It returns an error if the header
    /// does not agree with the index about the chunk. It returns an error if the
    /// header does not agree with the index about its lengths. It returns an
    /// error if the record reaches past the room the index gives it. It returns
    /// an error if a frame does not decode to the length its record claims. It
    /// returns an error if the file cannot be opened, seeked or read. The stream
    /// itself carries any later read failure.
    #[instrument(skip_all, err)]
    async fn get(&self, id: &BlobId) -> io::Result<StreamReadSeeker> {
        let Opened {
            mut file,
            header,
            start,
            ..
        } = self.open_record(id).await?;

        match header.codec {
            Codec::Raw => {
                // Seeked rather than left where the header read finished it. The
                // buffer holds the widest header a record can have, so where it
                // ended is not necessarily where this record's payload starts.
                file.seek(io::SeekFrom::Start(start.saturating_add(header.total_len)))
                    .await?;
                Ok(Box::new(crate::slice::SliceAsyncReader::new(
                    file,
                    header.raw_len,
                )))
            }
            Codec::Zstd => {
                file.seek(io::SeekFrom::Start(start.saturating_add(header.total_len)))
                    .await?;
                let mut frame = crate::storage::zeroed(
                    header.stored_len,
                    &format!("the compressed frame of chunk {id}"),
                )?;
                file.read_exact(&mut frame).await?;

                // Through [`Zstd`] rather than through `DecompressReader::new`, so
                // there is one implementation of "what a zstd frame means and what
                // it must produce" rather than two that can disagree. This is the
                // match on the codec byte. A read is answered by the record, not by
                // whatever policy wrote it.
                let bytes = Decompressor::decompress(&Zstd, frame, header.raw_len).await?;

                Ok(Box::new(crate::slice::DecompressReader::from_decoded(
                    bytes,
                )))
            }
        }
    }
}

/// A record opened out of the payload: the handle it is behind and the header
/// that says how it is stored.
struct Opened {
    /// The file, seeked to the record's first byte.
    file: tfs::File,
    /// The record's own header, decoded and checked against the index.
    header: crate::storage::RecordHeader,
    /// Where the record begins, in file coordinates.
    start: u64,
}

/// Read many pack records with one open file.
///
/// Open the pack once. Reuse the file for each read. Use this for a sequential pass over many chunks. Prefer this to opening the file for each chunk.
///
/// This covers the passes that read records. A merge reads its chunks through one of these, and so does the verify pass of an apply. A pass that writes the records it reads opens the pack for each chunk instead: the write loop of an apply is one.
#[derive(Debug)]
pub struct PackReader<'a> {
    pack: &'a Pack,
    file: tfs::File,
}

impl<'a> PackReader<'a> {
    /// Return a reader that reuses one file handle.
    ///
    /// Keep one file handle for the life of the reader. Reuse that file for each read.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the file fails.
    pub async fn open(pack: &'a Pack) -> io::Result<Self> {
        let file =
            crate::storage::at_async(pack.path(), "open", tfs::File::open(pack.path())).await?;
        Ok(Self { pack, file })
    }

    /// Read the stored form of a chunk.
    ///
    /// Reuse the open file. Check the header against the index. Return the payload as the store holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if the pack misses the chunk. Returns an error if the header fails to decode. Returns an error if the header disagrees with the index. Returns an error if reading the payload fails.
    pub async fn read_stored(&mut self, id: &BlobId) -> io::Result<Stored> {
        let (located, span, start) = self.pack.record_bounds(id)?;
        let header =
            read_header_at(&mut self.file, self.pack.path(), id, located, span, start).await?;
        self.file
            .seek(io::SeekFrom::Start(start.saturating_add(header.total_len)))
            .await?;
        let mut bytes = crate::storage::zeroed(
            header.stored_len,
            &format!("chunk {id} as {} stores it", self.pack.path().display()),
        )?;
        self.file.read_exact(&mut bytes).await?;
        Ok(Stored {
            bytes,
            codec: header.codec,
            raw_len: header.raw_len,
        })
    }

    /// Read the content of a chunk.
    ///
    /// Reuse the open file. Check the header against the index. Decode the frame where the record holds one. Return the content bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the pack misses the chunk. Returns an error if the header fails to decode. Returns an error if the header disagrees with the index. Returns an error if the frame fails to decode. Returns an error if reading the payload fails.
    pub async fn read_content(&mut self, id: &BlobId) -> io::Result<Vec<u8>> {
        let (located, span, start) = self.pack.record_bounds(id)?;
        let header =
            read_header_at(&mut self.file, self.pack.path(), id, located, span, start).await?;
        self.file
            .seek(io::SeekFrom::Start(start.saturating_add(header.total_len)))
            .await?;
        match header.codec {
            Codec::Raw => {
                let mut bytes =
                    crate::storage::zeroed(header.raw_len, &format!("the content of chunk {id}"))?;
                self.file.read_exact(&mut bytes).await?;
                Ok(bytes)
            }
            Codec::Zstd => {
                let mut frame = crate::storage::zeroed(
                    header.stored_len,
                    &format!("the compressed frame of chunk {id}"),
                )?;
                self.file.read_exact(&mut frame).await?;
                Decompressor::decompress(&Zstd, frame, header.raw_len).await
            }
        }
    }
}

/// Return the checked header of the record at `start`.
///
/// Seek to the record. Read the header bytes. Decode the header. Check the header against the index. Return the header.
async fn read_header_at(
    file: &mut tfs::File,
    path: &Path,
    id: &BlobId,
    located: &Located,
    span: u64,
    start: u64,
) -> io::Result<crate::storage::RecordHeader> {
    file.seek(io::SeekFrom::Start(start)).await?;
    let head_len = span.min(u64::try_from(MAX_RECORD_HEADER).unwrap_or(u64::MAX));
    let mut head = crate::storage::zeroed(
        head_len,
        &format!("the record header of chunk {id} in {}", path.display()),
    )?;
    file.read_exact(&mut head).await?;
    let Some(header) = crate::storage::decode_header(&head, 0) else {
        return Err(not_a_pack(
            path,
            &format!(
                "the record it holds for chunk {id} does not begin with a record header: \
                 the {head_len} bytes the index leaves for it hold no header — its magic, \
                 kind and codec are not there, or its own check does not verify",
            ),
        ));
    };
    check_record(path, id, &header, located, span)?;
    Ok(header)
}

/// Check a decoded header against the index entry.
///
/// Compare the record length against the room. Compare the content length against the index. Bound the content claim. Return the header when all checks pass.
fn check_record(
    path: &Path,
    id: &BlobId,
    header: &crate::storage::RecordHeader,
    located: &Located,
    span: u64,
) -> io::Result<()> {
    let total = header.total_len.saturating_add(header.stored_len);
    if header.kind != RecordKind::Blob
        || BlobId::from_bytes(header.hash) != *id
        || total > span
        || header.raw_len != located.content_len
        || (header.codec == Codec::Raw && header.stored_len != header.raw_len)
    {
        return Err(not_a_pack(
            path,
            &format!(
                "the record it holds for chunk {id} is not the one its index names: it \
                 claims {total} bytes where the index gives it {span}, and {} bytes of \
                 content where the index says {}, stored as {:?}",
                header.raw_len, located.content_len, header.codec,
            ),
        ));
    }
    if header.raw_len > MAX_CHUNK {
        return Err(not_a_pack(
            path,
            &format!(
                "chunk {id} claims {} bytes of content, which is more than the largest \
                 chunk this crate's chunker produces ({MAX_CHUNK})",
                header.raw_len
            ),
        ));
    }
    Ok(())
}

/// A pack read as the store a merge writes from, so both halves read the same way.
///
/// [`StorageGet`] is [`StorageStored`] plus the decode. One record is opened and
/// checked the same way, then either served as content or handed over as it stands.
/// `merge`'s keep path had its own copy of the first half, which was six steps,
/// the same bounds, and the same message. A copy is a second answer to the
/// question "which bytes is this chunk". That is the one question a damaged
/// pack makes expensive to get wrong.
impl StorageStored for Pack {
    /// The chunk's payload as this pack stores it: a frame where the record says
    /// `Zstd`, and the content itself where it says `Raw`.
    ///
    /// # Errors
    ///
    /// Returns an error if the pack does not hold that chunk. It returns an error
    /// if the record's header does not decode. It returns an error if the header
    /// does not agree with the index. It returns an error if the payload
    /// cannot be read.
    #[instrument(skip_all, err)]
    async fn stored(&self, id: &BlobId) -> io::Result<Stored> {
        let Opened {
            mut file,
            header,
            start,
            ..
        } = self.open_record(id).await?;

        // Seeked rather than left where the header read finished it, for the
        // reason `get` seeks. The buffer holds the widest header a record can
        // have, so where the read ended is not where the payload starts.
        file.seek(io::SeekFrom::Start(start.saturating_add(header.total_len)))
            .await?;

        let mut bytes = crate::storage::zeroed(
            header.stored_len,
            &format!("chunk {id} as {} stores it", self.path().display()),
        )?;
        file.read_exact(&mut bytes).await?;

        Ok(Stored {
            bytes,
            codec: header.codec,
            raw_len: header.raw_len,
        })
    }
}

impl Pack {
    /// Open the record holding `id` and hold it to the index.
    ///
    /// Everything the two ways of reading a chunk do together, and nothing that
    /// depends on what the caller wants afterwards. This seeks the file to the
    /// record and reads its header through the store's own decoder. It then
    /// checks the two claims about those bytes against each other, which are the
    /// header's claim and the index's claim. A reader that serves content and a
    /// writer that copies the stored form have to agree about which bytes a chunk
    /// is. This is where they agree.
    ///
    /// **The record's own header is what says how the payload is stored.**
    /// Nothing in the index duplicates the codec. A pack is a file that was
    /// downloaded, and a byte at an offset it claims is not this crate's to
    /// believe. The store's own decoder decodes the header. A header
    /// whose eight-byte check does not verify stops the chunk here. It is not
    /// read as content.
    ///
    /// **And the two are checked against each other**, because they are two claims
    /// about the same bytes. The header says how long this record is and the index
    /// says how much room it has. A record claiming to reach past its own slot
    /// would be read into the next chunk's bytes. A record claiming a different
    /// content length is not the chunk this entry names. A raw record's two
    /// lengths have to be the same number as well, because it is those bytes that
    /// are served rather than decoded.
    ///
    /// # Errors
    ///
    /// Returns an error if the pack does not hold that chunk. It returns an error
    /// if the record's header does not decode. It returns an error if the header
    /// does not agree with the index. It returns an error if the file
    /// cannot be opened, seeked or read.
    async fn open_record(&self, id: &BlobId) -> io::Result<Opened> {
        let (located, span, start) = self.record_bounds(id)?;
        let mut file =
            crate::storage::at_async(self.path(), "open", tfs::File::open(self.path())).await?;
        let header = read_header_at(&mut file, self.path(), id, located, span, start).await?;
        Ok(Opened {
            file,
            header,
            start,
        })
    }

    /// Return the bounds of the record for `id`.
    ///
    /// Returns the index entry and the room the index gives the record and the
    /// file offset where the record starts. Refuses a chunk the pack does not
    /// hold.
    fn record_bounds(&self, id: &BlobId) -> io::Result<(&Located, u64, u64)> {
        let at = self.position(id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("this pack does not hold a chunk hashing to {id}"),
            )
        })?;
        let located = &self.chunks[at];
        let span = self.record_span(located.offset);
        let start = self.payload_start.saturating_add(located.offset);
        Ok((located, span, start))
    }
}

/// The positions of `chunks`, in payload order, as the narrow numbers [`Pack`] holds.
///
/// Both constructors call this, and it is the only place a position becomes a
/// `u32`. That is one call rather than the same conversion in two places, where
/// one of them would eventually be missed.
fn payload_order(chunks: &[Located], path: &Path) -> io::Result<Vec<u32>> {
    let mut order: Vec<u32> = (0..chunks.len())
        .map(|at| {
            u32::try_from(at).map_err(|_| {
                not_a_pack(
                    path,
                    &format!(
                        "it names more chunks than a pack can order ({MAX_PACK_CHUNKS}), and a \
                         position past the end of the index would name the wrong record"
                    ),
                )
            })
        })
        .collect::<io::Result<_>>()?;

    order.sort_unstable_by_key(|at| chunks[position(*at)].offset);

    Ok(order)
}

/// A position in [`Pack::order`] as an index into the index itself.
///
/// `usize::from` is not defined for `u32`, because it is not lossless on every
/// platform this compiles for. This is therefore a cast, and the cast cannot
/// truncate. Every position came out of a `usize` range over this pack's own chunk
/// count, which [`payload_order`] refused to build above [`MAX_PACK_CHUNKS`]. A
/// position that did not fit a `usize` would be a chunk count no array could hold.
const fn position(at: u32) -> usize {
    at as usize
}

/// Check that the records the index names are inside the payload and do not
/// overlap, and say in what order they sit.
///
/// The index is a claim about where each chunk sits, and `open` checks only the
/// payload's *extent*. What the index no longer carries is each record's length,
/// because the record's own header carries that. A second copy here would be a
/// second claim about the same bytes. So what the index does make is that each
/// record begins at or after where the one before it ends. `needed` and
/// `chunks_covering` both turn a chunk into a byte range from exactly that, and
/// hand the answer to a client that cannot check it. So it is checked here, so
/// that every later consumer can do the arithmetic without re-deriving whether it
/// holds.
///
/// Three ways it can fail, and all are refused. One way is a record placed
/// before the one before it ends. Another is a record placed past the payload.
/// A third is an index naming no records at all over a payload that is not
/// empty. What *cannot* be checked here is that the
/// records exactly fill the payload, because that would mean reading every header.
/// Reading every header is the cost the index at the end exists to avoid. So the
/// last record's end is taken as the payload's, which over-claims rather than
/// under-claims if a writer left a gap.
///
/// The walk is in offset order because that is the order the question is about.
/// The order it produces is [`Pack::order`]. It is not something thrown away.
/// This is the one place a pack is already paying to know that order. A sort every
/// later caller would otherwise repeat, and `after` repeated per chunk read in
/// linear time, which is `O(n²)` over a restore.
///
/// # Errors
///
/// Returns an error naming the offset that does not fit and why.
///
/// It also returns an error if the index names no records at all over a payload
/// that is not empty. That refusal names no offset, because there is none.
fn check_index_geometry(path: &Path, chunks: &[Located], payload_len: u64) -> io::Result<Vec<u32>> {
    let min_header = u64::try_from(crate::storage::MIN_RECORD_HEADER).unwrap_or(u64::MAX);

    // Sorted by offset, not walked in index order. What has to hold is
    // a fact about the payload, and the order the index happens to keep its
    // entries in is the index's own business.
    let order = payload_order(chunks, path)?;

    // Where the record before this one has to have ended, at the least.
    let mut earliest = 0u64;

    for at in &order {
        let chunk = &chunks[position(*at)];
        if chunk.offset < earliest {
            return Err(not_a_pack(
                path,
                &format!(
                    "its index places a chunk at {}, where the record before it has not ended, \
                     so the records it names do not tile the {payload_len}-byte payload",
                    chunk.offset
                ),
            ));
        }

        // The far end of the same question. A record cannot be shorter than
        // its own header, so an entry a handful of bytes from the end names
        // something that is not a record. `get` would refuse that a chunk at a
        // time, where refusing the pack says it once, before anything is served.
        earliest = chunk
            .offset
            .checked_add(min_header)
            .filter(|end| *end <= payload_len)
            .ok_or_else(|| {
                not_a_pack(
                    path,
                    &format!(
                        "its index places a chunk at {}, with fewer than the {min_header} bytes \
                         a record header occupies left before the {payload_len}-byte payload ends",
                        chunk.offset
                    ),
                )
            })?;
    }

    if chunks.is_empty() && payload_len != 0 {
        return Err(not_a_pack(
            path,
            &format!(
                "its index names no chunks, but the payload it describes is {payload_len} bytes"
            ),
        ));
    }

    Ok(order)
}

/// Hash a pack file to name it.
///
/// The function runs on the blocking pool. The file read and the hash both
/// happen on that thread.
///
/// The function reads the file in blocks of 256 kibibytes. Each block goes
/// straight from the read into the BLAKE3 hasher, so the bytes are not copied
/// into a second buffer first. The file is never held whole in memory.
///
/// The hash covers the whole file. That is the header, the payload, the index
/// and the trailer. Call it after the write completes.
///
/// # Errors
///
/// Returns an error if it cannot open the pack file.
/// Returns an error if it cannot read the pack file.
/// Returns an error if the hashing task panics.
pub async fn hash_file(path: &Path) -> io::Result<BlobId> {
    let owned = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut file = crate::storage::at(&owned, "open", fs::File::open(&owned))?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0u8; 256 * 1024];

        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }

        Ok(hasher.finalize())
    })
    .await
    .map_err(|join| io::Error::other(format!("hashing panicked: {join}")))?
}

/// A pack that is never written: the same bytes, computed where they are asked
/// for.
///
/// # Why this exists
///
/// `blu serve` serves a pack over HTTP. The obvious way is to build the file
/// and hand out ranges of it. That works, and it costs disk a pack's worth per
/// version any client has asked for. A pack is *derived output* — re-running
/// `blu pack` regenerates one, and nothing is lost but the file — so storing
/// derived output is the one thing this format does not need.
///
/// Every byte of a pack is already determined by two things: the [`Plan`], which
/// says which chunks and in what order, and the [`crate::storage::Placement`]s
/// that plan resolved, which say how each one is stored. Given those, the header, every
/// record header, the snapshot section, the index and the trailer are all
/// computable without reading a chunk. So this type computes them, and answers
/// "give me bytes *a* to *b*" from the set's own segment files.
///
/// **It is valid only for `keep`.** A pack written under a compression policy
/// re-encodes every record, so its bytes are not in the set and not derivable
/// from it. A request for one has to materialise the file instead. That is the
/// caller's check to make; [`VirtualPack::new`] is what then proves it.
///
/// # The guarantee it rests on
///
/// **A [`VirtualPack`]'s bytes are byte-identical to the file [`Pack::write`]
/// produces for the same plan.** Not "equivalent", not "a pack that reads the
/// same". The same bytes, so a client may fetch some of its ranges from one and
/// the rest from the other and still hold one file.
///
/// This is a strong claim about two code paths sharing an encoder, and it is
/// load-bearing: a client that got it wrong would hand a file to `Pack::open`
/// that it refuses, naming an offset and a count nowhere near the cause.
/// `tests/pack_geometry.rs` pins it against real stores — a full pack, a delta, a
/// compressed set, and a set that has rolled over a segment.
///
/// # What is in memory
///
/// The header, the snapshot section, the index, and one small record per chunk.
/// **Not** the payload, and not a chunk. The index is proportional to
/// the chunk count and nothing else, which is the same cost [`Pack::open`] pays
/// and is what makes a pack's index-at-the-end layout worth its trailer.
/// The constructor caches each record header in one byte store.
/// The pack reuses those bytes for each range.
/// The constructor builds the header store before the pack answers a range.
/// A pack that answers one range pays the build for one answer.
/// The saving grows with the number of ranges the pack answers.
/// `blu serve` builds a pack per request, so a request pays the build once.
#[derive(Debug, Clone)]
pub struct VirtualPack {
    /// The header, the whole of it, at offset zero.
    header: Vec<u8>,
    /// One entry per record, in payload order. The same order [`Pack::write`]
    /// writes them in, which is the order a range walk needs.
    records: Vec<VirtualRecord>,
    /// The header store holds all record headers in payload order.
    header_store: Vec<u8>,
    /// Bytes the payload region occupies.
    payload_len: u64,
    /// The snapshot section, verbatim, as [`Pack::write`] writes it.
    snapshot: Vec<u8>,
    /// The index, as [`Pack::write`] writes it.
    index: Vec<u8>,
    /// The trailer, as [`Pack::write`] writes it: the two section lengths and
    /// then the magic. [`TRAILER`] bytes, of which [`encode_trailer`] supplies
    /// all but the last four.
    trailer: Vec<u8>,
    /// Length of the file these bytes would make.
    length: u64,
}

/// One record's place in the payload, and where its payload sits in the store.
///
/// The two offsets are different things, and holding them in one struct is what
/// makes the difference hard to forget. `start` is where the *record* is in the
/// pack, which is what a range request covers. `segment` and `payload_offset`
/// are where the *payload* is in the segment file, which is what a read is made
/// from. The header store holds each record header. The constructor writes each
/// one once.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VirtualRecord {
    /// The chunk's content hash, and the identity it is addressed by.
    id: BlobId,
    /// Byte offset of this record within the payload region.
    start: u64,
    /// Bytes the record occupies, its header included.
    len: u64,
    /// The payload's own length, which is not [`VirtualRecord::len`].
    stored_len: u64,
    /// Bytes the payload decodes to, which is what the index and a header carry
    /// as the raw length.
    raw_len: u64,
    /// How the payload is stored.
    codec: crate::storage::Codec,
    /// The offset locates the header bytes in the header store.
    header_start: usize,
    /// The length measures the header bytes in the header store.
    header_len: usize,
    /// The file the payload is in.
    ///
    /// Resolved once, at construction, rather than by a second mechanism at serve
    /// time. A caller used to hand `VirtualPack::new` a `resolve` closure whose
    /// result this loop threw away, and carry a separate `Vec<PathBuf>` of
    /// segments alongside — which meant a directory scan on every pack fetch, and
    /// two answers to "which file is segment 3 in" that could disagree.
    path: std::path::PathBuf,
    /// Byte offset of the payload within that file.
    payload_offset: u64,
}

/// Where some bytes of a [`VirtualPack`] come from.
///
/// A pack is a header, a run of records, a snapshot, an index and a trailer, and
/// a byte range may cross from one of those to the next. So an answer is a list,
/// and each part says where its own bytes live. Holding the small regions as
/// owned bytes is right: a header is 48 bytes and an index is proportional to
/// the chunk count, and either is cheaper to rebuild for one request than to
/// keep for the life of the process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// Bytes that are already in memory.
    Bytes(Vec<u8>),
    /// Bytes to read from a segment file.
    Segment {
        /// The file to read from.
        path: std::path::PathBuf,
        /// Byte offset within it.
        offset: u64,
        /// How many bytes.
        len: u64,
    },
}

impl VirtualPack {
    /// Compute the pack a `plan` makes, for `version` and an optional base.
    ///
    /// `resolve` names the file a segment number is in. It is a parameter rather
    /// than a set because a pack is not tied to one: a `VirtualPack` computed
    /// against set *A* serves the same bytes as one computed against set *B*, for
    /// as long as the two resolve their chunks the same way.
    ///
    /// No chunk is read, so this cannot fail because of damaged data. A chunk
    /// that is damaged fails when its bytes are served, by the same check a real
    /// read makes.
    ///
    /// The constructor encodes each record header once.
    /// The pack reuses those bytes for each range.
    /// The constructor builds the header store before the pack answers a range.
    /// The build costs one encode per record.
    /// A pack that answers one range pays the build for one answer.
    /// `blu serve` builds a pack per request, so a request pays the build once.
    ///
    /// # Errors
    ///
    /// Returns an error if `snapshot` cannot be encoded on this platform, which
    /// on unix cannot happen, and reports it as `InvalidInput` because nothing
    /// is wrong with the snapshot — this build simply cannot record it.
    pub fn new<F>(
        plan: &Plan,
        snapshot: &crate::metadata::Snapshot,
        version: u32,
        base: Option<BlobId>,
        resolve: F,
    ) -> io::Result<Self>
    where
        F: Fn(u32) -> std::path::PathBuf,
    {
        let snapshot_bytes = snapshot.to_bytes().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("this version cannot be packed on this platform: {e}"),
            )
        })?;
        let snapshot_id = blake3::hash(&snapshot_bytes);

        // The header `write_from` builds on its verbatim route, over the same two
        // counts. Built before the payload, because a varint's length depends on
        // the numbers it carries, so the header's own length is only known once
        // it is built.
        let header = encode_header(&Header {
            snapshot: snapshot_id,
            taken_at: snapshot.taken_at,
            version,
            base,
            chunks: u64::try_from(plan.len()).unwrap_or(0),
            payload_len: planned_payload_len(plan),
        });

        // The same walk `write_from` does, over the same plan and the same
        // placements, accumulating the same offsets. This is the half of the
        // byte-identity guarantee that `tests/pack_geometry.rs` checks.
        // Each header is encoded once here and cached in the header store,
        // so serving a range slices bytes instead of encoding them again.
        let mut records = Vec::with_capacity(plan.len());
        let mut header_store = Vec::with_capacity(plan.len().saturating_mul(64));
        let mut offset = 0u64;

        for planned in plan.chunks() {
            let stored_len = planned.placement.stored_len;
            let header_start = header_store.len();
            append_record_header(
                &mut header_store,
                &planned.id,
                planned.placement.codec,
                stored_len,
                planned.placement.raw_len,
            );
            let header_len = header_store.len().saturating_sub(header_start);
            let len = u64::try_from(header_len)
                .unwrap_or(u64::MAX)
                .saturating_add(stored_len);

            records.push(VirtualRecord {
                id: planned.id,
                start: offset,
                len,
                stored_len,
                raw_len: planned.placement.raw_len,
                codec: planned.placement.codec,
                header_start,
                header_len,
                path: resolve(planned.placement.segment),
                payload_offset: planned.placement.offset,
            });

            offset = offset.saturating_add(len);
        }

        let payload_len = offset;
        let index = encode_index(
            records
                .iter()
                .map(|record| (record.id, record.start, record.raw_len)),
        );
        let trailer_len = u64::try_from(TRAILER).unwrap_or(0);
        let mut trailer = encode_trailer(
            u64::try_from(index.len()).unwrap_or(0),
            u64::try_from(snapshot_bytes.len()).unwrap_or(0),
        )
        .to_vec();
        // The magic is written after the two lengths, not inside them: it is what
        // a reader finds at the *end* of the file, so a pack that was truncated
        // is refused before its trailer is parsed at all.
        trailer.extend_from_slice(&MAGIC);

        let snapshot_start = u64::try_from(header.len())
            .unwrap_or(0)
            .saturating_add(payload_len);
        let length = snapshot_start
            .saturating_add(u64::try_from(snapshot_bytes.len()).unwrap_or(0))
            .saturating_add(u64::try_from(index.len()).unwrap_or(0))
            .saturating_add(trailer_len);

        Ok(Self {
            header,
            records,
            header_store,
            payload_len,
            snapshot: snapshot_bytes,
            index,
            trailer,
            length,
        })
    }

    /// Length of the file this describes.
    ///
    /// Named for the file rather than left as a bare `len`, for the reason
    /// [`Pack::file_len`] is: it is the length of a thing on disk, and `len` on a
    /// value that has a `chunks()` beside it reads as "how many chunks". There is
    /// no `is_empty`, because a pack is never empty — a version with no chunks is
    /// still a header, a snapshot and an index.
    #[must_use]
    pub fn file_len(&self) -> u64 {
        self.length
    }

    /// Where the payload region starts, which is just after the header.
    #[must_use]
    pub fn payload_start(&self) -> u64 {
        u64::try_from(self.header.len()).unwrap_or(0)
    }

    /// Bytes the payload region occupies.
    #[must_use]
    pub fn payload_len(&self) -> u64 {
        self.payload_len
    }

    /// How many chunks this pack carries.
    ///
    /// May be zero: a pack of an empty tree still has a header, a snapshot and an
    /// index, and is a file worth fetching.
    #[must_use]
    pub fn chunks(&self) -> usize {
        self.records.len()
    }

    /// Where the header sits.
    #[must_use]
    pub fn header_range(&self) -> Range {
        Range {
            start: 0,
            end: self.payload_start(),
        }
    }

    /// Where the payload region sits.
    #[must_use]
    pub fn payload_range(&self) -> Range {
        let start = self.payload_start();
        Range {
            start,
            end: start.saturating_add(self.payload_len),
        }
    }

    /// Where the snapshot section sits.
    #[must_use]
    pub fn snapshot_range(&self) -> Range {
        let start = self.payload_range().end;
        Range {
            start,
            end: start.saturating_add(u64::try_from(self.snapshot.len()).unwrap_or(0)),
        }
    }

    /// Where the index sits.
    #[must_use]
    pub fn index_range(&self) -> Range {
        let start = self.snapshot_range().end;
        Range {
            start,
            end: start.saturating_add(u64::try_from(self.index.len()).unwrap_or(0)),
        }
    }

    /// Where the trailer sits, at the very end.
    #[must_use]
    pub fn trailer_range(&self) -> Range {
        let end = self.length;
        Range {
            start: end.saturating_sub(u64::try_from(TRAILER).unwrap_or(0)),
            end,
        }
    }

    /// The byte ranges a client holding `have` is missing, in pack coordinates.
    ///
    /// **The same answer [`Pack::needed`] gives**, from the same walk in the same
    /// order, for the same reason: a run of consecutive missing records is one
    /// range, which is the entire cost a pack exists to remove.
    ///
    /// **And it is not enough on its own.** A client that fetched only these
    /// would have a run of records and no header, no snapshot, no index and no
    /// trailer, and [`Pack::open`] would refuse the file it assembled — on the
    /// trailer, since a trailer is not there.
    /// [`VirtualPack::fetch_plan`] is what a client actually wants.
    #[must_use]
    pub fn needed(&self, have: &HashSet<BlobId>) -> Vec<Range> {
        let mut ranges: Vec<Range> = Vec::new();

        for record in &self.records {
            if have.contains(&record.id) {
                continue;
            }

            let start = self.payload_start().saturating_add(record.start);
            let end = start.saturating_add(record.len);

            match ranges.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => ranges.push(Range { start, end }),
            }
        }

        ranges
    }

    /// What a client holding `have` should fetch: [`needed`](Self::needed) plus
    /// the fixed regions, merged and sorted.
    ///
    /// The fixed regions are the header, the snapshot, the index and the trailer.
    /// They are the same for every client, so they are always fetched, and they
    /// are what makes the fetched bytes a file rather than a payload.
    #[must_use]
    pub fn fetch_plan(&self, have: &HashSet<BlobId>) -> Vec<Range> {
        let mut ranges = self.needed(have);
        ranges.push(self.header_range());
        ranges.push(self.snapshot_range());
        ranges.push(self.index_range());
        ranges.push(self.trailer_range());

        ranges.sort_unstable_by(|a, b| a.start.cmp(&b.start).then(a.end.cmp(&b.end)));

        // Merged, because two ranges that touch or overlap are one fetch. A
        // fixed region can land next to a run of records, and a client that
        // asked for them separately would make two requests for one run of bytes.
        let mut merged: Vec<Range> = Vec::with_capacity(ranges.len());
        for range in ranges {
            match merged.last_mut() {
                Some(last) if range.start <= last.end => {
                    last.end = last.end.max(range.end);
                }
                _ => merged.push(range),
            }
        }

        merged
    }

    /// The bytes of `range`, and where each part of them comes from.
    ///
    /// Nothing is read here: the returned [`Source::Segment`]s name files and
    /// offsets and the caller reads them. Each file was resolved when this pack
    /// was made, so answering a range needs no directory scan. So a segment truncated since this pack
    /// was computed fails at the read, which is the same place a real transfer
    /// would fail and with the same error.
    ///
    /// The call slices cached header bytes.
    /// The call names segment reads.
    ///
    /// # Errors
    ///
    /// Returns an error if `range` is inverted or reaches past the end of the
    /// pack. An inverted range is refused rather than answered as empty, because
    /// an empty answer to a range a client believed it had would leave a file
    /// with a hole in it and nothing saying so.
    pub fn source(&self, range: Range) -> io::Result<Vec<Source>> {
        if range.start >= range.end || range.end > self.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "bytes {}..{} are not inside a pack of {} bytes",
                    range.start, range.end, self.length
                ),
            ));
        }

        let mut out = Vec::new();
        let mut at = range.start;
        let end = range.end;

        // Sliced through `usize`, and the offsets came from this type's own
        // arithmetic over lengths it already holds, so a value past `usize` on a
        // 32-bit target cannot occur. It is *checked* rather than cast, because the
        // crate denies `cast_possible_truncation` and a truncating slice index is
        // a panic with an unhelpful message.
        let push = |out: &mut Vec<Source>, bytes: &[u8], from: u64, to: u64| {
            let (Some(from), Some(to)) = (usize::try_from(from).ok(), usize::try_from(to).ok())
            else {
                return;
            };

            if from < to && to <= bytes.len() {
                out.push(Source::Bytes(bytes[from..to].to_vec()));
            }
        };

        // The header.
        let header_range = self.header_range();
        if at < header_range.end {
            let to = end.min(header_range.end);
            push(&mut out, &self.header, at, to);
            at = to;
        }

        // The payload, record by record. A range can begin inside a record and
        // end inside a later one, so this is a walk and not a lookup. Records
        // are in payload order and the walk is in that order too, so it stops at
        // the first one past `end`.
        for record in &self.records {
            if at >= end {
                break;
            }

            let record_range = self.record_range(record);
            if record_range.end <= at {
                continue;
            }

            let from = at.max(record_range.start);
            let to = end.min(record_range.end);

            // A record is its header followed by its payload. A range can land in
            // either, in both, or across the join.
            let header_bytes = u64::try_from(record.header_len).unwrap_or(u64::MAX);
            let payload_from = from.saturating_sub(record_range.start);
            let payload_to = to.saturating_sub(record_range.start);
            let cached = self
                .header_store
                .get(record.header_start..record.header_start.saturating_add(record.header_len))
                .unwrap_or(&[]);

            if payload_from < header_bytes && payload_to <= header_bytes {
                // Entirely in the header, which is cached rather than read.
                push(&mut out, cached, payload_from, payload_to);
            } else if payload_from >= header_bytes {
                // Entirely in the payload, which is read out of the segment.
                out.push(Source::Segment {
                    path: record.path.clone(),
                    offset: record
                        .payload_offset
                        .saturating_add(payload_from.saturating_sub(header_bytes)),
                    len: payload_to.saturating_sub(payload_from),
                });
            } else {
                // The join. The header's tail, then the payload's head.
                push(&mut out, cached, payload_from, header_bytes);
                out.push(Source::Segment {
                    path: record.path.clone(),
                    offset: record.payload_offset,
                    len: payload_to.saturating_sub(header_bytes),
                });
            }

            at = to;
        }

        // The snapshot, the index and the trailer, in the order they sit. Each
        // region is checked by whether `at` is inside it, so a range that skipped
        // a region entirely does not read it.
        for region in [
            self.snapshot_range(),
            self.index_range(),
            self.trailer_range(),
        ] {
            if at >= region.start && at < region.end {
                let to = end.min(region.end);
                let (from, bytes): (u64, &[u8]) = if region.start == self.snapshot_range().start {
                    (at - region.start, &self.snapshot)
                } else if region.start == self.index_range().start {
                    (at - region.start, &self.index)
                } else {
                    (at - region.start, &self.trailer)
                };
                push(&mut out, bytes, from, to - region.start);
                at = to;
            }
        }

        debug_assert_eq!(at, end, "the walk must consume the whole range");

        Ok(out)
    }

    /// Where one record sits in the pack, header included.
    fn record_range(&self, record: &VirtualRecord) -> Range {
        let start = self.payload_start().saturating_add(record.start);
        Range {
            start,
            end: start.saturating_add(record.len),
        }
    }
}

/// Append one header to the header store.
///
/// Use the store encoder for the given lengths.
/// Keep the bytes equal to the bytes [`Pack::write`] writes.
fn append_record_header(
    out: &mut Vec<u8>,
    id: &BlobId,
    codec: Codec,
    stored_len: u64,
    raw_len: u64,
) {
    crate::storage::encode_header(
        out,
        crate::storage::RecordKind::Blob,
        codec,
        id.as_bytes(),
        stored_len,
        raw_len,
    );
}
