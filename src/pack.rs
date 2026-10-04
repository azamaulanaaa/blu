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
//! magic (4) | format version (4) | version (4) | snapshot hash (32) | chunks (varint) |
//! payload bytes (varint) [| base flag (1) | base hash (32)]
//! ...records, end to end, in plan order: a store record per chunk...
//! ...snapshot: the encoded snapshot this version is, verbatim...
//! ...index: per chunk, hash (32) | offset (varint) | content length (varint)...
//! index length (8) | snapshot length (8) | magic (4)
//! ```
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
//! loses nothing, because `lepatch pack` writes it again, byte for byte. The
//! store's commit records remain the record of what versions exist, which is why
//! nothing reads a pack to answer that.

use std::{
    collections::HashSet,
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::instrument;

use crate::storage::{
    BlobId, Codec, Decompressor, MAX_RECORD_HEADER, Plan, PlannedChunk, RecordKind, StorageGet,
    StreamReadSeeker, Zstd,
};

/// Largest chunk the chunker can produce, and so the most a pack will decode into
/// memory in one piece.
///
/// `ronomon::MAXIMUM_MAX`, the ceiling `ChunkerConfig::validate` enforces on
/// `max_size`, which makes it the largest content any chunk in a set this crate
/// wrote can have. The bound is on the *record's claim* rather than on anything
/// measured. A pack is a file that was downloaded, so a frame that declares a
/// terabyte would otherwise have this process allocate one to find out.
///
/// The largest chunk any named profile cuts is 256 KiB. That figure is tighter and
/// more usual. It is not the one used here, because a caller may configure a
/// chunker with a larger `max_size`. A pack of such a set has to remain readable.
pub(crate) const MAX_CHUNK: u64 = fastcdc::ronomon::MAXIMUM_MAX as u64;

/// Most chunks a pack's order can name, which is `u32::MAX`.
///
/// A consequence of [`Pack::order`] holding positions in a `u32`. It is not a limit
/// anybody would notice hitting. Four billion records is over two hundred
/// gigabytes of payload, and the file would have to be read to be opened. The
/// number is stated here because the refusal that enforces it must name it.
const MAX_PACK_CHUNKS: u32 = u32::MAX;

/// Magic at the start of a pack, and again at the very end.
const MAGIC: [u8; 4] = *b"LPK1";

/// Layout version, so a pack this build does not understand is reported as such.
///
/// `6` is a pack whose index does not repeat the record length each chunk's own
/// header already carries. `5` is the same payload region with that length
/// duplicated in the index, so its index entries are wider. They would decode here
/// as offsets that are really something else. `4` is a pack whose payload region
/// is the chunks' content end to end with nothing between them. `3` recorded the
/// number of the version it was packed from, which `4`'s predecessor did not.
/// `2` carried its snapshot and `1` did not. `1` also has a twelve-byte trailer
/// where these have twenty. Those two cannot be told apart by their contents, and
/// they would take their index length from the wrong place.
///
/// **Every earlier version is refused rather than guessed at**, and the error names
/// both numbers, this build's and the pack's. A client holding a pack this build
/// cannot read is otherwise left guessing which command produces one. That is
/// affordable here for the same reason it was for `4`. A pack is derived output, so
/// re-running `lepatch pack` regenerates a pack that an earlier build wrote, and
/// nothing is lost but the file.
///
/// A `4` read as if its payloads were records would not fail at the header. The
/// first chunk's bytes would be read as a record header and refused for a magic
/// that is not there, so the damage is loud. It is refused by name anyway, because
/// the alternative is a reader that works out what a file is by trying.
const FORMAT_VERSION: u32 = 6;

/// Set in the header when a base snapshot hash follows it, making this a delta.
const BASE_FLAG: u8 = 0x01;

/// Bytes of fixed header, before any base hash and the two varint counts.
///
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
const HEADER: usize = 4 + 4 + 4 + 32;

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
        let placement = self.locate(id)?;
        let path = self.segment_path(placement.segment);

        let mut file = crate::storage::at_async(&path, "open", tfs::File::open(&path)).await?;
        let len = file.metadata().await?.len();
        let end = placement
            .offset
            .checked_add(placement.stored_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "a blob's range overflows")
            })?;

        if end > len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "blob at {} in segment {} needs bytes {}..{end} but that segment is \
                     only {len} bytes long",
                    path.display(),
                    placement.segment,
                    placement.offset
                ),
            ));
        }

        file.seek(io::SeekFrom::Start(placement.offset)).await?;
        let mut bytes = crate::storage::zeroed(
            placement.stored_len,
            &format!("chunk {id} as {} stores it", path.display()),
        )?;
        file.read_exact(&mut bytes).await?;

        Ok(Stored {
            bytes,
            codec: placement.codec,
            raw_len: placement.raw_len,
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

impl Pack {
    /// Write a pack of `plan` to `path`, reading payloads from `store`.
    ///
    /// It is written to a sibling temporary and renamed into place when it is
    /// whole. A run that fails part-way leaves the pack that was already at
    /// `path` alone. It does not replace that pack with a file that cannot be
    /// opened. A pack is derived output, so a partial one is a file to write
    /// again rather than a thing to recover. The pack a *previous* run wrote is
    /// not this run's to lose, and `--out` usually names one.
    /// One chunk is buffered at a time, never a whole version. The index and the
    /// snapshot both grow with what is being packed.
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
    /// concrete `&BlobStore<Read>`. That made `lepatch pack` the one command
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
        Self::write_from(store, plan, snapshot, version, None, path).await
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
        Self::write_from(store, plan, snapshot, version, Some(base), path).await
    }

    /// [`Self::write`] or [`Self::write_delta`], by whether a base was given.
    async fn write_from<S>(
        store: &S,
        plan: &Plan,
        snapshot: &crate::metadata::Snapshot,
        version: u32,
        base: Option<BlobId>,
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

        let temporary = temporary_for(path);
        // Removed however this function leaves. A temporary beside a
        // finished pack is a file with no meaning, and one beside a failed run is
        // the same file.
        let mut unfinished = Temporary(Some(temporary.clone()));

        let planned_count = u64::try_from(plan.len()).unwrap_or(0);
        let planned_bytes = planned_payload_len(plan);

        let mut chunks = Vec::with_capacity(plan.len());
        let mut offset = 0u64;

        // The header's length depends on the counts, since a varint runs
        // until its high bit clears, so the header is built before the payload
        // starts.
        let header = encode_header(&Header {
            snapshot: snapshot_id,
            version,
            base,
            chunks: planned_count,
            payload_len: planned_bytes,
        });
        let payload_start = u64::try_from(header.len()).unwrap_or(u64::MAX);

        let mut file =
            crate::storage::at_async(&temporary, "create", tfs::File::create(&temporary)).await?;
        file.write_all(&header).await?;

        for planned in plan.chunks() {
            let (mut located, record_len) = write_record(&mut file, store, planned).await?;
            located.offset = offset;

            // Saturating, to match the count the header carries. The
            // header reports one total and the index is built on the other. A plain
            // `+=` would panic where both would have clamped.
            offset = offset.saturating_add(record_len);
            chunks.push(located);
        }

        // Between the payloads and the index, so `index length` alone
        // still locates the index. See the module docs.
        let snapshot_start = payload_start.saturating_add(offset);
        file.write_all(&snapshot_bytes).await?;

        let index = encode_index(&chunks);

        file.write_all(&index).await?;
        file.write_all(&encode_trailer(
            u64::try_from(index.len()).unwrap_or(0),
            u64::try_from(snapshot_bytes.len()).unwrap_or(0),
        ))
        .await?;
        file.write_all(&MAGIC).await?;
        file.sync_all().await?;
        drop(file);

        // Into place only once it is whole, so a run that fails part-way
        // leaves the pack that was already at this path alone. It does not replace
        // that pack with a file that cannot be opened. `lepatch pack` overwrites,
        // so the path is usually one a previous run wrote and somebody wants back.
        crate::storage::at_async(path, "rename into place", tfs::rename(&temporary, path)).await?;
        unfinished.disarm();

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
    /// Returns an error if the file is too short to hold a trailer. It returns an
    /// error if the file does not end with the pack magic. It returns an error
    /// if the file does not begin with one. It returns an
    /// error if the file is a format version this build does not read, or if its
    /// index does not decode. A header that disagrees with its own index, on the
    /// chunk count or on where the payload ends, is refused rather than believed.
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
/// Also what `lepatch pack --dry-run` reports, which is why it is not folded
/// into the writer. A figure the report gives and a figure the file carries are
/// only the same figure if someone computes them once.
///
/// Every term comes from the plan's own placement, which the caller resolved
/// against the store being packed. A record's width is decided by the two lengths
/// it carries, and those are known from the store's index without reading a
/// payload. That is what lets the header be written first and still carry the
/// right total. It is also why the writer checks what it is given against the
/// same numbers, rather than trusting that they still hold.
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

/// Write one chunk's record and say where it landed, and how long it is.
///
/// The caller fills in the offset later. The offset is the sum of every record
/// before this one. This function does not know them. The length
/// comes back rather than staying in the returned [`Located`], because the index
/// does not carry it. It is in the record's header, which was just written. A
/// second copy in the index would be a second claim about the same bytes.
///
/// # Errors
///
/// Returns an error if the store cannot yield the chunk. It returns an error if
/// what the store yields is not the stored form the plan resolved. The plan saw
/// a set that changed while the pack was being written. It returns an error if the
/// file cannot be written.
async fn write_record<S>(
    file: &mut tfs::File,
    store: &S,
    planned: &PlannedChunk,
) -> io::Result<(Located, u64)>
where
    S: StorageStored + ?Sized,
{
    let stored = store.stored(&planned.id).await?;
    let stored_len = u64::try_from(stored.bytes.len()).unwrap_or(u64::MAX);

    // Checked rather than assumed, because the header's payload length
    // was worked out from the plan's placements. This is the only place the store
    // gets to disagree with them. It can disagree, because a set is appended to
    // while a pack of it is written. A store that moved under the run would
    // otherwise produce a pack whose header contradicts its own records. `open`
    // refuses that pack, naming a count, long after the run that caused it had
    // exited successfully.
    let placement = planned.placement;
    if stored.codec != placement.codec
        || stored_len != placement.stored_len
        || stored.raw_len != placement.raw_len
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
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

/// A sibling path to write a pack to first, before renaming it.
///
/// In the same directory, because `rename` is only atomic within one filesystem.
///
/// A fixed suffix rather than a pid or a counter. Two runs cannot be writing the
/// same pack to the same path at once, because the set they are packing holds its
/// lock.
fn temporary_for(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
    name.push(".tmp");
    path.with_file_name(name)
}

/// The temporary a pack is written to, removed unless it was renamed into place.
///
/// The lock file makes the same trade for the same reason. However the run ends,
/// the temporary does not outlive it. This is a `Drop` rather than a cleanup at
/// each exit. The exits are one rename that may fail and a dozen errors. Those
/// errors may come from anywhere in between.
struct Temporary(Option<PathBuf>);

impl Temporary {
    /// The run has renamed this into place, so there is nothing left to remove.
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for Temporary {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
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

/// A header's fixed part. It holds the magic and the format version. It holds
/// the version the pack was built from. It holds the snapshot hash. It holds
/// the base flag and hash if there is one.
/// Then come the two counts as varints.
fn encode_header(header: &Header) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + COUNTS_MAX);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&header.version.to_le_bytes());
    out.extend_from_slice(header.snapshot.as_bytes());
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
    write_varint(&mut out, header.payload_len);

    out
}

/// What a header records, and how much of it is fixed-length.
///
/// The counts are varints and so are not fixed, but they come last. Everything a
/// reader needs to find the payload is a fixed prefix, which is what lets [`open`]
/// read the geometry without decoding the counts first.
struct Header {
    snapshot: BlobId,
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
fn encode_index(chunks: &[Located]) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunks.len() * (32 + 16));
    for chunk in chunks {
        out.extend_from_slice(chunk.id.as_bytes());
        write_varint(&mut out, chunk.offset);
        write_varint(&mut out, chunk.content_len);
    }

    out
}

/// The index read back.
///
/// # Errors
///
/// Returns an error if a 32-byte hash runs past the end of `bytes`. It returns
/// an error if either of the two varints runs past the end of `bytes`. It
/// returns an error if a number is overlong or overflows. It returns an error
/// if a hash is repeated. A repeated hash would mean two chunks at one offset.
fn decode_index(bytes: &[u8]) -> io::Result<Vec<Located>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
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
    #[must_use]
    pub fn chunks_covering(&self, ranges: &[Range]) -> HashSet<BlobId> {
        let mut out = HashSet::new();

        for (chunk, end) in self.records() {
            let start = self.payload_start.saturating_add(chunk.offset);

            if ranges.iter().any(|r| r.start <= start && end <= r.end) {
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
        let at = self.position(id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("this pack does not hold a chunk hashing to {id}"),
            )
        })?;
        let located = &self.chunks[at];
        let span = self.record_span(located.offset);

        let mut file =
            crate::storage::at_async(self.path(), "open", tfs::File::open(self.path())).await?;

        // Saturating for the same reason `needed` saturates. These are two numbers
        // out of an index, and `open` checked they add up, but the sum is still
        // arithmetic on a claim.
        let start = self.payload_start.saturating_add(located.offset);
        file.seek(io::SeekFrom::Start(start)).await?;

        // The header is variable width, so it is read rather than stepped over, and
        // bounded by the record's own span. A header that did not fit inside the
        // span the index gives the record is not this record's header, and reading
        // on would pull in the next chunk's bytes.
        let head_len = span.min(u64::try_from(MAX_RECORD_HEADER).unwrap_or(u64::MAX));
        let mut head = crate::storage::zeroed(
            head_len,
            &format!(
                "the record header of chunk {id} in {}",
                self.path().display()
            ),
        )?;
        file.read_exact(&mut head).await?;

        let Some(header) = crate::storage::decode_header(&head, 0) else {
            return Err(not_a_pack(
                self.path(),
                &format!(
                    "the record it holds for chunk {id} does not begin with a record header: \
                     the {head_len} bytes the index leaves for it hold no header — its magic, \
                     kind and codec are not there, or its own check does not verify",
                ),
            ));
        };

        let total = header.total_len.saturating_add(header.stored_len);
        if header.kind != RecordKind::Blob
            || BlobId::from_bytes(header.hash) != *id
            || total > span
            || header.raw_len != located.content_len
            || (header.codec == Codec::Raw && header.stored_len != header.raw_len)
        {
            return Err(not_a_pack(
                self.path(),
                &format!(
                    "the record it holds for chunk {id} is not the one its index names: it \
                     claims {total} bytes where the index gives it {span}, and {} bytes of \
                     content where the index says {}, stored as {:?}",
                    header.raw_len, located.content_len, header.codec,
                ),
            ));
        }

        // The content claim is bounded before it is believed, and before anything
        // reserves it. A frame declaring more content than the chunker can ever
        // produce would have the decode allocate that much before finding out.
        // The index bounds the claim only as far as the index is honest, and the
        // index is as downloaded as the header is.
        if header.raw_len > MAX_CHUNK {
            return Err(not_a_pack(
                self.path(),
                &format!(
                    "chunk {id} claims {} bytes of content, which is more than the largest \
                     chunk this crate's chunker produces ({MAX_CHUNK})",
                    header.raw_len
                ),
            ));
        }

        Ok(Opened {
            file,
            header,
            start,
        })
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

/// The hash of a pack's contents, for naming it.
///
/// The whole file, which is the header, payloads, index and trailer. The name
/// therefore covers the format too, and a pack written by a future format version
/// is a different name rather than a collision. Streamed once in constant memory.
/// Call it after writing.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read.
pub async fn hash_file(path: &Path) -> io::Result<BlobId> {
    let mut file = crate::storage::at_async(path, "open", tfs::File::open(path)).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 256 * 1024];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize())
}
