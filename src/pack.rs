//! A pack: one version's chunks, concatenated into a single distributable file.
//!
//! A store is a deduplicated pool, and the wrong shape for serving one version: a
//! restore out of a store is a request per chunk. A pack lays the same chunks end
//! to end, so a version's bytes are contiguous and are one range request rather
//! than thousands. The measurements behind that are in the README.
//!
//! ## The payload is a run of store records
//!
//! Each chunk is written as the record the source store already holds for it —
//! header, codec byte and all — rather than as bare content. The cost is a
//! header per chunk; the saving is that a chunk arrives in the form it was
//! deduplicated in, so packing a compressed set copies frames rather than
//! inflating every one of them, and the receiving side re-encodes under its own
//! policy instead of carrying the sender's.
//!
//! **The bytes are the store's, produced by the store's own encoder.** A second
//! encoder here would be a second record format that agreed with the first until
//! the first changed, so [`crate::storage`]'s `encode_header` writes them and its
//! `decode_header` reads them back.
//!
//! ## Determinism
//!
//! A pack is a function of its [`crate::storage::Plan`] **and of the stored form
//! the chunks had in the set it was packed from**, and a plan is ordered by the
//! snapshot's chunk table rather than by the order chunks happened to be
//! written. So the same tree packs to the same bytes every time, which is what
//! makes a pack nameable by the hash of its own contents: an immutable artifact a
//! CDN can cache forever and a client can verify with one comparison. The stored
//! form is part of that function on purpose rather than a leak of the sender's
//! policy into the artifact: two sets holding one tree at the same compression
//! produce the same pack, and two sets holding it at different compressions
//! produce different packs, which is true of the sets themselves.
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
//! `offset` is where a chunk's *record* starts. How long that record is, the
//! record's own header already says — the header is the first thing in it, and
//! it is written by the store's encoder — so the index does not say it a second
//! time. Two copies of one number in one file is two claims that can disagree,
//! and a pack is a file somebody downloaded.
//!
//! A range drawn from the index still has to cover a chunk whole, header
//! included, and it does because the records are written end to end: an entry's
//! record runs from its own offset to the next entry's, so a client that fetches
//! everything between two offsets has everything a chunk needs to be read. The
//! header is inside that run rather than across it.
//!
//! `content length` is the same number after decoding, which is what a
//! snapshot's lengths are counted in and what `put` hashes. It is the one length
//! the index keeps, because it is the one thing about a chunk the header does
//! not say in the form a caller wants: it is in content bytes, and the header's
//! own lengths are the payload's on disk and its bytes once decoded.
//!
//! A player fetches the last twenty bytes, learns where the index is and how long
//! it is, fetches *that* range, and then knows every chunk's offset without having
//! read a byte of payload — so the cost of finding out what a pack holds does not
//! depend on how much it holds.
//!
//! The trailing magic is what makes that reverse read reliable: the length alone
//! is indistinguishable from payload bytes, and a truncated download would
//! otherwise be read as a short index.
//!
//! ## It carries the snapshot
//!
//! A pack holds the bytes but not what they *mean*. The file table — paths,
//! lengths, attributes, and which slice of which chunk lands at which offset —
//! lives in the snapshot, and the snapshot is otherwise just another blob in the
//! store. So a client handed a pack and nothing else could locate a chunk but
//! could not lay a single one of them into a file, which is the other half of what
//! a pack is for: applying one version on top of another.
//!
//! The snapshot therefore travels with the payload, and its hash is in the
//! header, so a pack is one self-contained artifact. It sits *between* the
//! payloads and the index rather than after the index, and that placement is what
//! preserves the property above: `index length` alone still locates the index
//! exactly, so a client that only wants offsets reads the trailer and one range
//! and never touches the snapshot. Only a client that means to apply the pack
//! pays for it, and it is a range proportional to the metadata rather than to the
//! tree.
//!
//! What this buys is not the bytes — they were in the set either way — but the
//! ability to hand someone one file. Two files, a pack and a snapshot, cannot be
//! published as one immutable CDN object with one name; one file can.
//!
//! ## It names the version it was built from
//!
//! The header carries a number: the version of *the set the pack was written
//! from*. It is the one thing in the header that is not about the bytes in the
//! file, and it is there because a receiver otherwise cannot see the most
//! ordinary event in a transfer at all — the sender built a pack of a version
//! that arrived before, the transfer was retried, and the version it holds is
//! already here under some number that means nothing where the pack came from.
//!
//! **It is the origin set's numbering, and it means nothing as a coordinate
//! here.** Two sets number independently, so "version 3" in a pack says which
//! version of the set it was packed from and nothing about where it lands. The
//! version a merge publishes is the receiving set's own, allocated at commit and
//! reported as such; the pack's number is what a warning can compare against the
//! set's tip to notice that a pack has fallen behind, and it is a hint for a
//! person, not a mapping. A client that treated it as a coordinate would try to
//! publish version 3 into a set that is on version 7, and there would be no
//! version 3 to overwrite because the two numberings never met.
//!
//! Nothing here depends on it being *right*, which is why it does not get to
//! refuse anything: a set renumbered, restored elsewhere, or repacked from a
//! different machine's numbering can all make it stale. What it buys is that the
//! staleness is visible at the moment a transfer is received, rather than
//! inferred later from a restore that returned an older version than the one
//! asked for.
//!
//! ## Derived, never authoritative
//!
//! A pack is output. It is regenerable from the store and the snapshot alone, and
//! losing one loses nothing: `lepatch pack` writes it again, byte for byte. The
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
/// measured: a pack is a file that was downloaded, and a frame that declares a
/// terabyte would otherwise have this process allocate one to find out.
///
/// The largest chunk any named profile cuts is 256 KiB, which is the tighter and
/// more usual figure; it is not the one used here, because a caller may configure
/// a chunker with a larger `max_size` and a pack of such a set has to remain
/// readable.
const MAX_CHUNK: u64 = fastcdc::ronomon::MAXIMUM_MAX as u64;

/// Magic at the start of a pack, and again at the very end.
const MAGIC: [u8; 4] = *b"LPK1";

/// Layout version, so a pack this build does not understand is reported as such.
///
/// `6` is a pack whose index does not repeat the record length each chunk's own
/// header already carries; `5` is the same payload region with that length
/// duplicated in the index, so its index entries are wider and would be decoded
/// here as offsets that are really something else. `4` is a pack whose payload
/// region is the chunks' content end to end with nothing between them; `3`
/// recorded the number of the version it was packed from, which `4`'s predecessor
/// did not; `2` carried its snapshot and did not, and `1` did not either and has
/// a twelve-byte trailer where these have twenty, so those two cannot be told
/// apart by their contents and would take their index length from the wrong
/// place.
///
/// **Every earlier version is refused rather than guessed at**, and the error names
/// both — this build's number and the pack's — because a client holding a pack
/// this build cannot read is otherwise left guessing which command produces one.
/// That is affordable here for the same reason it was for `4`: a pack is derived
/// output, so a pack written by an earlier build is regenerated by re-running
/// `lepatch pack` and nothing is lost but the file.
///
/// A `4` read as if its payloads were records would not fail at the header: the
/// first chunk's bytes would be read as a record header and refused for a magic
/// that is not there, so the damage is loud. It is refused by name anyway,
/// because the alternative is a reader that works out what a file is by trying.
const FORMAT_VERSION: u32 = 6;

/// Set in the header when a base snapshot hash follows it, making this a delta.
const BASE_FLAG: u8 = 0x01;

/// Bytes of fixed header, before any base hash and the two varint counts.
///
/// The base is variable because a delta carries one and a full pack does not, so it
/// cannot be part of this. It is why [`open`] reads the byte after it to learn how
/// long the header is, rather than assuming.
///
/// The recorded version *is* fixed, though it is one fact about the pack rather
/// than a fact about its geometry, and that is the reason rather than an
/// accident: it costs four bytes in a header that is written once per pack and
/// bought it back by being readable without touching the variable section. A
/// reader that has to walk the base flag to find out which version a pack carries
/// has had to parse the pack to learn what the pack *is*; four bytes say it at a
/// fixed offset, and the version becomes usable by anything holding the header
/// without holding a [`Pack`].
const HEADER: usize = 4 + 4 + 4 + 32;

/// Bytes of trailer: the two section lengths and the trailing magic.
const TRAILER: usize = 8 + 8 + 4;

/// Longest the header's two varint counts can be: a [`read_varint`] accepts at
/// most ten bytes, so the pair is at most twenty and the fixed part is never the
/// whole of it.
const COUNTS_MAX: usize = 20;

/// Where a chunk sits in a pack.
///
/// `offset` and `content_len` are what the index carries. The record's own
/// length is not among them, because the record's header — the first bytes of the
/// record, written by the store's own encoder — already carries it, and a second
/// copy in the index is a second claim about the same bytes that the two could
/// disagree about. What a reader has instead is the fact the index does state:
/// the records tile the payload, so a record runs from its entry's offset to the
/// next entry's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located {
    /// The chunk's content hash.
    pub id: BlobId,
    /// Byte offset of its record, relative to [`Pack::payload_start`].
    ///
    /// The record's start, header included: a range covering a chunk starts here.
    pub offset: u64,
    /// Bytes of content the record's payload is once decoded.
    ///
    /// What a snapshot's lengths are counted in and what [`StoragePut::put`] is
    /// told to read, so it is what a merge hands over.
    ///
    /// [`StoragePut::put`]: crate::storage::StoragePut::put
    pub content_len: u64,
}

/// A chunk as the store holds it: its record's payload, and what that record
/// says about it.
///
/// `raw_len` is the pack's own [`Located::content_len`] again, kept here beside
/// the bytes because the two are what one chunk's record is: a payload that
/// occupies `bytes.len()` bytes on disk and decodes to `raw_len` of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// The payload exactly as it sits in the source: a frame where the record
    /// says `Zstd`, and the content itself where it says `Raw`.
    pub bytes: Vec<u8>,
    /// How those bytes are stored.
    pub codec: crate::storage::Codec,
    /// Bytes of content they decode to, which is what the record's hash covers.
    pub raw_len: u64,
}

/// Reading a chunk's **stored** bytes, which is what a pack is written from.
///
/// The mirror of [`StorageGet`], and separate from it for the reason the store's
/// own [`StoragePut`](crate::storage::StoragePut) is: `get` answers a *reader*,
/// who wants content and is owed a decompressed stream, and a pack is not a
/// reader. Copying content into a pack and writing it back out as a fresh record
/// would decode every chunk of a compressed set and re-encode every one of them,
/// which is the work this format exists to avoid — and it would make a pack's
/// bytes a function of the reading path rather than of what the set holds.
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
    /// wrong cannot pull in its neighbour, and refused outright if the segment is
    /// shorter than the index says — the same check `get` makes, for the same
    /// reason: a blob handed out now has to be the blob that was indexed.
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
/// range does not have, and the suggestion to use the standard type — which is
/// what an audit proposed — is declined here because both are lost.
///
/// **A `len()` that cannot panic.** `Range<u64>` has no `len()` at all, and that
/// is not an oversight: it would be `end - start`, which panics under a debug
/// assertion and wraps to a near-`u64::MAX` in release. Both fields are public and
/// the type is part of the client protocol, so an inverted range is a value a
/// caller can produce. `len` here saturates, and `needed_bytes` folds that rather
/// than `sum()` for the same reason.
///
/// **`is_empty()` means "well-formed and covers nothing", not `end <= start`.**
/// `Range<u64>::is_empty` is `start >= end`, so an inverted range would answer
/// `true` — and an inverted range is not empty, it is *malformed*, and the two
/// want different responses. `tests/pack.rs::an_inverted_range_covers_nothing`
/// pins this deliberately: the same call must be able to say "no bytes" (via
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
    /// Saturating, because both fields are public and a range a client built with
    /// `end` below `start` is a value this type has to survive rather than
    /// panic on: it covers nothing.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range covers nothing *and is well formed*.
    ///
    /// Not `end <= start`, which is what [`std::ops::Range::is_empty`] does and
    /// which would report an inverted range as empty. An inverted range covers no
    /// bytes — [`Range::len`] says so — but it is not empty, it is malformed, and
    /// a caller acting on the two differently is the point.
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
    /// Every chunk, in pack order.
    chunks: Vec<Located>,
}

impl Pack {
    /// Write a pack of `plan` to `path`, reading payloads from `store`.
    ///
    /// Written in place and complete when this returns; a pack is derived output,
    /// so a partial one is a file to write again rather than a thing to recover.
    /// One chunk is buffered at a time, never a whole version, though the index
    /// and the snapshot both grow with what is being packed.
    ///
    /// `version` is the number the version has *in the set this is being packed
    /// from*, and it goes in the header so a receiver can see where the pack came
    /// from. It is a parameter rather than something worked out from the plan
    /// because the plan does not know it: a plan is a function of a snapshot's
    /// chunks, and two sets number their versions independently, so the same plan
    /// is version 2 of one set and version 9 of another. A default here would be
    /// a `0` in the header of every pack written by a caller that forgot, and a
    /// `0` reads as a real version number to whatever reads it back.
    ///
    /// **Each chunk is written whole, and in the stored form it has in the set.**
    /// Whole, because a record's hash is of the chunk's entire content, so a
    /// record holding only the part a version reads would not hash to the name it
    /// is addressed by — and a merge hands those bytes straight to `put`, which
    /// checks that hash. In its stored form, because a pack that decoded every
    /// chunk on the way out and re-encoded it on the way in would be doing the
    /// expensive half of the transfer twice over, and would make the artifact's
    /// bytes depend on the reading path rather than on the set.
    ///
    /// The header comes first and its counts are the plan's — `plan.len()` chunks
    /// and, for the payload, the sum of the records the plan's placements describe
    /// — so the header is written before a byte of payload and both agree by
    /// construction. Every one of those terms is checked against what the store
    /// actually yields as each chunk is written, so a set that changed underneath
    /// the run is refused rather than written into a pack whose own counts are
    /// wrong.
    ///
    /// The snapshot is written as `plan`'s own description of the version, so a
    /// pack *is* the version rather than a reference to it: the hash in the header
    /// is over exactly the bytes that follow the records.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be recorded on this platform, if
    /// the store cannot yield a chunk the plan names, if what it yields is not
    /// the stored form the plan resolved, or if the file cannot be written.
    ///
    /// Generic over [`StorageStored`] because that is the whole of what it needs:
    /// the bytes and the codec byte beside them. It was bound to a concrete
    /// `&BlobStore<Read>`, which meant `lepatch pack` was the one command that
    /// could not be pointed at any other backend — a read-only store over a
    /// directory, say, or a store held in memory. A pack is derived output, so
    /// there is no reason it should care where the bytes came from.
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
    /// a holder of `base` is missing. Nothing about the format changes — the index
    /// simply names fewer chunks — so everything downstream is unchanged.
    ///
    /// **The base is a promise, and this is where it is made.** A chunk left out is
    /// one the reader is asserted to have, and nothing in the pack can check that:
    /// the bytes that would prove it are exactly the bytes that were left out. So
    /// the base is recorded in the header, which lets [`Pack::base`] report a
    /// mismatch rather than have one happen quietly. A reader that cannot account
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
        // platform, which on unix cannot happen. Invalid input rather than damaged
        // data — nothing is wrong with the snapshot, this build cannot record it.
        let snapshot_bytes = snapshot.to_bytes().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("this version cannot be packed on this platform: {e}"),
            )
        })?;
        let snapshot_id = blake3::hash(&snapshot_bytes);

        let planned_count = u64::try_from(plan.len()).unwrap_or(0);
        let planned_bytes = planned_payload_len(plan);

        let mut chunks = Vec::with_capacity(plan.len());
        let mut offset = 0u64;

        // The header's length depends on the counts, since a varint runs until its
        // high bit clears, so it is built before the payload starts.
        let header = encode_header(&Header {
            snapshot: snapshot_id,
            version,
            base,
            chunks: planned_count,
            payload_len: planned_bytes,
        });
        let payload_start = u64::try_from(header.len()).unwrap_or(u64::MAX);

        let mut file = crate::storage::at_async(path, "create", tfs::File::create(path)).await?;
        file.write_all(&header).await?;

        for planned in plan.chunks() {
            let (mut located, record_len) = write_record(&mut file, store, planned).await?;
            located.offset = offset;

            // Saturating, to match the count the header carries: the header
            // reports one total and the index is built on the other, and a plain
            // `+=` would panic where both would have clamped.
            offset = offset.saturating_add(record_len);
            chunks.push(located);
        }

        // Between the payloads and the index, so `index length` alone still
        // locates the index — see the module docs.
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
        })
    }
    /// Open a pack, reading only its trailer and index.
    ///
    /// Cost is proportional to the index and not to the payload, which is the
    /// point of putting the index at the end: this can run before a client knows
    /// whether it wants the pack. The snapshot is located but **not read** — a
    /// client asking only [`Pack::needed`] never pays for it, and one that does
    /// want it calls [`Pack::snapshot`].
    ///
    /// # Errors
    ///
    /// Returns an error if the file is too short to hold a trailer, does not end
    /// with the pack magic, does not begin with one, is a format version this
    /// build does not read, or if its index does not decode. A header that
    /// disagrees with its own index — on the chunk count, or on where the
    /// payload ends — is refused rather than believed.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = crate::storage::at(path, "open", fs::File::open(path))?;
        let len = file.metadata()?.len();

        // The flag byte and the fixed part, plus a trailer. A delta's base hash is read
        // below and checked against the section lengths then, rather than assumed
        // here: this floor is for the shortest pack there is.
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

        // Both lengths are claims from the file, and a file can lie about them: a
        // sparse file reports any length it likes. So each is subtracted from the
        // end of the file it bounds rather than added to a computed offset.
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

        let chunks = decode_index(&index)?;

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

        // Read only after the format check above, which is what makes the refusal
        // for an older pack happen before anything downstream looks at the bytes
        // that follow: a `3` has no version here, and its four bytes that would be
        // read as one are the first four of its snapshot hash.
        let version = u32::from_le_bytes(header[8..12].try_into().unwrap_or([0; 4]));

        let mut snapshot_raw = [0u8; 32];
        snapshot_raw.copy_from_slice(&header[12..44]);
        let snapshot = BlobId::from_bytes(snapshot_raw);

        // A base hash, when this is a delta. Read before the counts, because its
        // presence is what decides where they start. Inferred rather than declared
        // it would be indistinguishable from a small full pack: a delta's payload is
        // short for the ordinary reason that it holds fewer chunks.
        let (base, after_base) = read_base(&mut file, &header, path)?;

        // The two varints, which are part of the header: reading the fixed part
        // alone would start the payload at the wrong offset, shifting every
        // chunk rather than failing. Only what lies between the fixed part and
        // the index is read, since a pack of an empty plan has two one-byte
        // counts there and no more.
        let mut head = [0u8; COUNTS_MAX];
        // Against the snapshot's start, for the same reason the payload geometry is
        // checked against it below.
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

        // The counts are redundant with the index, which is what locates chunks,
        // so they are a claim to check rather than a source of truth.
        if count != u64::try_from(chunks.len()).unwrap_or(u64::MAX) {
            return Err(not_a_pack(
                path,
                &format!(
                    "its header claims {count} chunks but its index lists {}",
                    chunks.len()
                ),
            ));
        }

        // Against where the snapshot begins, not where the index does: the
        // snapshot sits between them, so the payload's real end is `snapshot_start`
        // and checking it against `index_start` would be off by the whole snapshot.
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

        check_index_geometry(path, &chunks, payload_len)?;

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
        })
    }

    /// Where the pack's payload region starts in the file.
    ///
    /// A [`Located::offset`] is relative to this, so a client turning a chunk into
    /// a byte range adds it. Not a constant, because the header's counts are
    /// varints.
    #[must_use]
    pub fn payload_start(&self) -> u64 {
        self.payload_start
    }

    /// The version this pack was built from, in the set that built it.
    ///
    /// **The origin set's numbering, not a coordinate in this one.** Two sets
    /// number their versions independently, so this says which version of the set
    /// the pack was packed from and nothing about where it lands here: merging a
    /// pack of origin version 3 into a set on version 7 publishes version 8, and
    /// the 3 is not the 8. What it is good for is noticing that a pack has fallen
    /// behind — compared against the receiving set's tip, a number below it means
    /// the sender was working from an older version — and for saying so at the
    /// moment a transfer arrives, which is the one moment somebody can act on it.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The hash of the snapshot this pack holds.
    ///
    /// Also the name the store gives that version, so a client holding a snapshot
    /// from elsewhere can check that it is the same one this pack carries before
    /// trusting either. Reading the snapshot itself is [`Pack::snapshot`].
    #[must_use]
    pub fn snapshot_id(&self) -> BlobId {
        self.snapshot
    }

    /// The version this pack assumes its reader already holds, if it is a delta.
    ///
    /// `None` for a full pack, which needs no such accounting.
    ///
    /// A delta omits the chunks this names, and cannot check that its reader has
    /// them — the bytes that would prove it are the bytes it left out. So a reader
    /// has to establish the base *itself* and refuse on a mismatch. The two ways it
    /// can: a record of what the last apply left beside the destination, or a full
    /// read of the tree. Nothing about the pack makes this free, and nothing makes
    /// it skippable.
    #[must_use]
    pub fn base(&self) -> Option<BlobId> {
        self.base
    }

    /// The version this pack holds, read out of the pack and decoded.
    ///
    /// The file table travels with the payload, so this is everything needed to
    /// apply the pack: which files exist, how long each is, and which slice of
    /// which chunk lands at which offset. Read on demand rather than at
    /// [`Pack::open`], because a client that only wants to know *which ranges it
    /// needs* — [`Pack::needed`] — never decodes a version, and that is the
    /// cheaper question to ask.
    ///
    /// **Checked, not believed.** The bytes are hashed and compared against
    /// [`Pack::snapshot_id`] before they are parsed, so a pack whose snapshot
    /// section is damaged or edited is refused here rather than decoded into a
    /// file table that would then be written into a directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be read, if its bytes do not hash
    /// to the header's snapshot hash, or if they do not decode as a snapshot.
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
    /// A client holding the pack somewhere else entirely — an object store, a CDN —
    /// fetches this range to get the file table. It is bounded by
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
    /// The encoded form, not a [`crate::metadata::Snapshot`]: a client with its own
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

    /// Every chunk, in pack order.
    #[must_use]
    pub fn chunks(&self) -> &[Located] {
        &self.chunks
    }

    /// The chunks, in pack order.
    ///
    /// [`Pack`] is also [`IntoIterator`] and [`AsRef<[Located]>`], so
    /// `for chunk in &pack` works without the `pack.chunks().iter()` double hop.
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
    /// *file* coordinates — [`Pack::payload_start`] plus a [`Located::offset`].
    ///
    /// Stated as what the client lacks: holding every chunk gets nothing, holding
    /// nothing gets the whole payload region as one range. Adjacent ranges are
    /// merged, which the payload's layout is what makes worthwhile — chunks a
    /// client needs are written end to end, so a run of them is one range, and the
    /// record header between two of them is inside the run rather than across it.
    ///
    /// **A range's end comes from the next entry in the index, not from a length.**
    /// The index does not carry the record's length — the record's own header does,
    /// and a range has to be answerable before a byte of payload has been read —
    /// so a chunk's record runs from its own offset to where the record after it
    /// begins, and the last one to the end of the payload. That is only sound
    /// because the records do not overlap, which [`open`] checks and this crate's
    /// writer is what makes true.
    ///
    /// A set rather than anything ordered, because the set is only ever asked
    /// whether it holds a chunk. The order of the result comes from the order of
    /// [`Pack::chunks`], not from the set, so nothing observable depends on it —
    /// and `BlobId` is a `blake3::Hash`, which deliberately has no `Ord`: hash
    /// order is not content order and pretending otherwise would be a lie a
    /// caller could come to rely on.
    #[must_use]
    pub fn needed(&self, have: &HashSet<BlobId>) -> Vec<Range> {
        let mut ranges: Vec<Range> = Vec::new();

        for (at, chunk) in self.chunks.iter().enumerate() {
            if have.contains(&chunk.id) {
                continue;
            }

            // Saturating for the same reason `open` checks it: these two sums
            // are over numbers that came out of the index, and this type is what
            // a client is told to add them up with.
            let start = self.payload_start.saturating_add(chunk.offset);
            let end = start.saturating_add(self.record_span(at));

            match ranges.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => ranges.push(Range { start, end }),
            }
        }

        ranges
    }

    /// Bytes a client already holding `have` still needs.
    #[must_use]
    pub fn needed_bytes(&self, have: &HashSet<BlobId>) -> u64 {
        // Folded rather than `sum()`, for the same reason: a total that wrapped
        // would report a client needs fewer bytes than the ranges describe.
        self.needed(have)
            .iter()
            .fold(0u64, |total, range| total.saturating_add(range.len()))
    }

    /// Where a chunk is, if the pack holds it.
    #[must_use]
    pub fn locate(&self, id: &BlobId) -> Option<Located> {
        self.position(id).map(|at| self.chunks[at])
    }

    /// Where a chunk sits in the index, which is what the range questions need.
    ///
    /// Not `locate`'s answer: `needed` and `chunks_covering` have to know which
    /// record a chunk's is, and that is a position in the index rather than a
    /// copy of an entry — the span of one record is a fact about where the next
    /// one starts, which an entry no longer says on its own.
    fn position(&self, id: &BlobId) -> Option<usize> {
        self.chunks.iter().position(|chunk| chunk.id == *id)
    }

    /// The bytes the record at `at` of the index occupies.
    ///
    /// **From where the next record begins, not from a length.** The index does not
    /// carry each record's length — the record's own header does, and it is read
    /// by whoever reads the record — so the extent comes from the layout instead:
    /// records are written end to end and [`Pack::open`] refuses an index whose
    /// records overlap, so a record runs to the record after it and the last one
    /// runs to the end of the payload.
    ///
    /// The last case is where an over-claim rather than a short answer comes from:
    /// a writer that left a gap before the snapshot would have the last record's
    /// range reach past its own end, so a client fetches a few bytes it does not
    /// need. Under-claiming would be the other way round and cannot happen, which
    /// is the direction worth being wrong in.
    fn record_span(&self, at: usize) -> u64 {
        let end = self
            .chunks
            .get(at + 1)
            .map_or(self.payload_len, |next| next.offset);

        end.saturating_sub(self.chunks[at].offset)
    }
}

/// Bytes the records for `plan` will occupy, before a byte of them is read.
///
/// Every term comes from the plan's own placement, which the caller resolved
/// against the store being packed: a record's width is decided by the two lengths
/// it carries, and those are known from the store's index without reading a
/// payload. That is what lets the header be written first and still carry the
/// right total, and it is why the writer checks what it is given against the same
/// numbers rather than trusting that they still hold.
///
/// Saturating throughout, because the header's count and the index built beside
/// it are both sums of claims.
fn planned_payload_len(plan: &Plan) -> u64 {
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
/// The offset is left at zero for the caller to fill in, because it is the sum of
/// every record before this one and this function does not know them. The length
/// comes back rather than staying in the returned [`Located`], because the index
/// does not carry it: it is in the record's header, which was just written, and a
/// second copy of it in the index would be a second claim about the same bytes.
///
/// # Errors
///
/// Returns an error if the store cannot yield the chunk, if what it yields is not
/// the stored form the plan resolved — which is a set that changed while the pack
/// was being written — or if the file cannot be written.
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

    // Checked rather than assumed, because the header's payload length was worked
    // out from the plan's placements and this is the only place the store gets to
    // disagree with them. It can: a set is appended to while a pack of it is
    // written, and a store that moved under the run would otherwise produce a
    // pack whose header contradicts its own records — which `open` refuses, naming
    // a count, long after the run that caused it had exited successfully.
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

    // The store's own encoder, so these bytes are what a store record would be.
    // Hand-rolling the header here would make the pack a second record format
    // that agreed with the first until the first moved.
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
/// A flag byte rather than an inferred length, because a delta's payload is short
/// for the ordinary reason that it holds fewer chunks: a small full pack and a
/// delta cannot be told apart by how much is there.
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

/// A header's fixed part: magic, format version, the version the pack was built
/// from, the snapshot hash, the base flag and hash if there is one, then the two
/// counts as varints.
fn encode_header(header: &Header) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + COUNTS_MAX);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&header.version.to_le_bytes());
    out.extend_from_slice(header.snapshot.as_bytes());
    // Always written, so the counts start at a fixed distance from the front for
    // both kinds of pack. A reader that had to infer this would be inferring it
    // from a payload length, which a small full pack and a delta cannot be told
    // apart by.
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
/// The counts are varints and so are not fixed, but they come last — everything a
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
/// record's own length is *not* here: it is in the record's header, which is the
/// first thing the record holds, and the index says where the records begin
/// rather than how long each one is.
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
/// Returns an error if a 32-byte hash or either of the two varints runs past the
/// end of `bytes`, if a number is overlong or overflows, or if a hash is
/// repeated — which would mean two chunks at one offset.
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
        // `locate` answers with the first — so a client asking for a chunk it
        // already had would be told to fetch a different one.
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
/// client downloaded and "it is not readable" is not something a person can act
/// on.
fn read_varint(bytes: &[u8], at: usize) -> io::Result<(u64, usize)> {
    crate::varint::read(bytes, at).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            match e {
                crate::varint::Error::Truncated => "a pack index ends mid-number",
                crate::varint::Error::Overlong => "a pack index has an overlong number",
                crate::varint::Error::Overflow => "a pack index number overflows",
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
    /// The inverse of [`Pack::needed`]: given a set of ranges, the chunks those
    /// ranges cover, so a client can record them after fetching and ask again. A
    /// range covering only part of a chunk is not that chunk and records nothing.
    ///
    /// A method rather than a free function taking a `&Pack`, because it reads
    /// three of the pack's own fields and is the other half of `needed`. As a
    /// free function the receiver was the first argument and the naming gave no
    /// hint that the two belonged together:
    ///
    /// ```text
    /// pack.needed(&have)                  // what the client lacks
    /// chunks_covering(&pack, &ranges)     // what the client now has
    /// ```
    ///
    /// A chunk is recorded only where a range covers its **whole record**, header
    /// included, which — as in [`Pack::needed`] — is the span from its own offset
    /// to the next record's.
    #[must_use]
    pub fn chunks_covering(&self, ranges: &[Range]) -> HashSet<BlobId> {
        let mut out = HashSet::new();

        for (at, chunk) in self.chunks.iter().enumerate() {
            let start = self.payload_start.saturating_add(chunk.offset);
            let end = start.saturating_add(self.record_span(at));

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
/// [`crate::storage::BlobStore`] — so the bytes it needed were spread across a
/// segment file's record headers, one read per chunk, and a pack, which holds
/// exactly the same bytes laid end to end, could not be restored from at all.
///
/// Implementing the trait rather than adding a parallel `restore_from_pack` is
/// the whole of the reuse: the restore verifies the snapshot, resolves every
/// recorded path, checks the slices account for each file, and then orders its
/// writes — none of which knows or cares that the bytes arrived from a different
/// place, and all of which would have to be written twice otherwise.
///
/// [`Snapshot`]: crate::metadata::Snapshot
impl StorageGet for Pack {
    /// Open the chunk with this content hash, out of the payload region.
    ///
    /// The stream yields the chunk's **content**, decoded if its record is a zstd
    /// frame, and is bounded to that content — so a caller comparing against a
    /// length the snapshot recorded is comparing like with like, and one that
    /// reads past the end gets end of stream rather than the next chunk's data.
    /// It is seekable within those bytes, which is what a chunk that a file uses
    /// only part of needs — `restore` seeks to the slice's offset within the chunk
    /// before reading.
    ///
    /// **The record's own header is read first, and it is what says how the
    /// payload is stored.** Nothing in the index duplicates the codec: a pack is a
    /// file that was downloaded, and a byte at an offset it claims is not this
    /// crate's to believe. The header is decoded by the store's own decoder, so a
    /// header whose eight-byte check does not verify stops the chunk here rather
    /// than being read as content.
    ///
    /// A compressed chunk is buffered whole and served from memory, which is the
    /// same trade the store's read path makes and for the same reason: a frame
    /// cannot seek backwards, and every consumer of a [`StorageGet`] stream seeks.
    /// The buffer is one chunk — bounded by [`MAX_CHUNK`], which is the largest
    /// the chunker can produce — and never the pack.
    ///
    /// A raw chunk is streamed straight out of the file, bounded to its content
    /// length, because there is nothing to decode and a file handle costs nothing
    /// to keep.
    ///
    /// # Errors
    ///
    /// Returns an error if the pack does not hold that chunk, if its record's
    /// header does not decode or does not agree with the index about the chunk,
    /// its lengths or the room the index gives it, if a frame does not decode to
    /// the length its record claims, or if the file cannot be opened, seeked or
    /// read. The stream itself carries any later read failure.
    #[instrument(skip_all, err)]
    async fn get(&self, id: &BlobId) -> io::Result<StreamReadSeeker> {
        let at = self.position(id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("this pack does not hold a chunk hashing to {id}"),
            )
        })?;
        let located = self.chunks[at];
        let span = self.record_span(at);

        let mut file =
            crate::storage::at_async(self.path(), "open", tfs::File::open(self.path())).await?;

        // Saturating for the same reason `needed` saturates: these are two numbers
        // out of an index, and `open` checked they add up, but the sum is still
        // arithmetic on a claim.
        let start = self.payload_start.saturating_add(located.offset);
        file.seek(io::SeekFrom::Start(start)).await?;

        // The header is variable width, so it is read rather than stepped over, and
        // bounded by the record's own span: a header that did not fit inside the
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
                     its magic, kind, codec or its own check do not verify"
                ),
            ));
        };

        // The record's header and the index are two claims about the same bytes, so
        // they are checked against each other. The header says how long this record
        // is and the index says how much room it has: a record claiming to reach
        // past its own slot would be read into the next chunk's bytes, and one
        // claiming a different content length is not the chunk this entry names.
        // A raw record's two lengths have to be the same number as well, because it
        // is those bytes that are served rather than decoded.
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

        match header.codec {
            Codec::Raw => {
                // Seeked rather than left where the header read finished it: the
                // buffer holds the widest header a record can have, so where it
                // ended is not necessarily where this record's payload starts.
                file.seek(io::SeekFrom::Start(start + header.total_len))
                    .await?;
                Ok(Box::new(crate::slice::SliceAsyncReader::new(
                    file,
                    header.raw_len,
                )))
            }
            Codec::Zstd => {
                if header.raw_len > MAX_CHUNK {
                    return Err(not_a_pack(
                        self.path(),
                        &format!(
                            "chunk {id} claims {} bytes of content, which is more than the \
                             largest chunk this crate's chunker produces ({MAX_CHUNK})",
                            header.raw_len
                        ),
                    ));
                }

                file.seek(io::SeekFrom::Start(start + header.total_len))
                    .await?;
                let mut frame = crate::storage::zeroed(
                    header.stored_len,
                    &format!("the compressed frame of chunk {id}"),
                )?;
                file.read_exact(&mut frame).await?;

                // Through [`Zstd`] rather than through `DecompressReader::new`, so
                // there is one implementation of "what a zstd frame means and what
                // it must produce" rather than two that can disagree. This is the
                // match on the codec byte: a read is answered by the record, not by
                // whatever policy wrote it.
                let bytes = Decompressor::decompress(&Zstd, frame, header.raw_len).await?;

                Ok(Box::new(crate::slice::DecompressReader::from_decoded(
                    bytes,
                )))
            }
        }
    }
}

/// Check that the records the index names are inside the payload and do not
/// overlap.
///
/// The index is a claim about where each chunk sits, and `open` checks only the
/// payload's *extent*. What the index no longer carries is each record's length —
/// the record's own header carries that, and a second copy here would be a second
/// claim about the same bytes — so what it does make is that each record begins
/// at or after where the one before it ends. `needed` and `chunks_covering` both
/// turn a chunk into a byte range from exactly that, and hand the answer to a
/// client that cannot check it, so it is checked here that every later consumer
/// can do the arithmetic without re-deriving whether it holds.
///
/// Three ways it can fail, all refused: a record placed before the one before it
/// ends, a record placed past the payload, and an index naming no records at all
/// over a payload that is not empty. What *cannot* be checked here is that the
/// records exactly fill the payload — that would mean reading every header, which
/// is the cost the index at the end exists to avoid — so the last record's end is
/// taken as the payload's, which over-claims rather than under-claims if a writer
/// left a gap.
///
/// # Errors
///
/// Returns an error naming the offset that does not fit and why.
fn check_index_geometry(path: &Path, chunks: &[Located], payload_len: u64) -> io::Result<()> {
    let min_header = u64::try_from(crate::storage::MIN_RECORD_HEADER).unwrap_or(u64::MAX);

    // Sorted by offset, not walked in index order: what has to hold is a fact
    // about the payload, and the order the index happens to keep its entries in
    // is the index's own business.
    let mut placed: Vec<&Located> = chunks.iter().collect();
    placed.sort_unstable_by_key(|chunk| chunk.offset);

    // Where the record before this one has to have ended, at the least.
    let mut earliest = 0u64;

    for chunk in placed {
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

        // The far end of the same question: a record cannot be shorter than its
        // own header, so an entry a handful of bytes from the end names something
        // that is not a record — and `get` would refuse that a chunk at a time,
        // where refusing the pack says it once, before anything is served.
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

    Ok(())
}

/// The hash of a pack's contents, for naming it.
///
/// The whole file — header, payloads, index and trailer — so the name covers the
/// format too, and a pack written by a future format version is a different name
/// rather than a collision. Streamed once in constant memory; call it after
/// writing.
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
