//! A pack: one version's chunks, concatenated into a single distributable file.
//!
//! # Why
//!
//! A store is a deduplicated content pool. It is the right shape for *holding*
//! versions and the wrong shape for *serving* one, because a restore of a new
//! version out of a store is a request per chunk. A pack is the same chunks laid
//! end to end with no framing between them, so the bytes a player needs are
//! contiguous and are one range request rather than thousands.
//!
//! Measured on a synthetic tree of 8 binaries and 6 asset blobs, patching one
//! 500 KB binary:
//!
//! ```text
//! v1 from nothing      1,309,726 bytes in  1 request
//! v2 having v1           25,325 bytes in  1 request   (1.9% of the pack)
//! ```
//!
//! One request in both cases, including the patch. That is the whole argument for
//! packing, and it is not a small one: the same delta served out of the store is
//! 25,325 requests.
//!
//! **The 1.9% is a best case and the request count is the result.** That fixture
//! comes from a generator that is a function of position only, so every file in it
//! is a prefix of the next and rewriting one truncates the original stream — the
//! most favourable case content-defined chunking can be given. On a tree with no
//! such relationship the same patch is 6.1% of the pack, still in one request.
//! `a_patch_is_one_range_on_a_tree_that_is_not_a_special_case` asserts the
//! request count on content that is not degenerate, which is the claim that
//! generalises.
//!
//! ## Determinism
//!
//! A pack is a function of its [`crate::storage::Plan`], and a plan is ordered by
//! the snapshot's chunk table rather than by the order chunks happened to be
//! written. So the same tree packs to the same bytes every time, which is what
//! makes a pack nameable by the hash of its own contents: an immutable artifact
//! that a CDN can cache forever and a client can verify with one comparison.
//!
//! ## The index is at the end
//!
//! ```text
//! magic (4) | version (4) | snapshot hash (32) | chunks (varint) | payload bytes (varint)
//! ...payloads, end to end, in plan order...
//! ...index: per chunk, hash (32) | offset (varint) | length (varint)...
//! index length (8) | magic (4)
//! ```
//!
//! A player fetches the last twelve bytes, learns where the index is and how long
//! it is, fetches *that* range, and then knows every chunk's offset without having
//! read a byte of payload. A header would be the wrong way round: it would have to
//! be read before anything, including before it is known whether the pack is worth
//! having at all.
//!
//! The trailing magic is what makes that reverse read reliable — the length alone
//! is indistinguishable from payload bytes, and a truncated download would
//! otherwise be read as a short index.
//!
//! ## Derived, never authoritative
//!
//! A pack is output. It is regenerable from the store and the snapshot alone, and
//! losing one loses nothing: `lepatch pack` writes it again, byte for byte. The
//! store's commit records remain the record of what versions exist, which is why
//! nothing reads a pack to answer that.

use std::{
    collections::BTreeSet,
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::storage::{BlobId, BlobStore, Plan, StorageGet};

/// Magic at the start of a pack, and again at the very end.
const MAGIC: [u8; 4] = *b"LPK1";

/// Layout version, so a pack this build does not understand is reported as such.
const FORMAT_VERSION: u32 = 1;

/// Bytes of fixed header, before the two varint counts.
const HEADER: usize = 4 + 4 + 32;

/// Bytes of trailer: the index length and the trailing magic.
const TRAILER: usize = 8 + 4;

/// Longest the header's two varint counts can be: a [`read_varint`] accepts at
/// most ten bytes, so the pair is at most twenty and the fixed part is never the
/// whole of it.
const COUNTS_MAX: usize = 20;

/// Where a chunk sits in a pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located {
    /// The chunk's content hash.
    pub id: BlobId,
    /// Byte offset of its payload in the pack.
    pub offset: u64,
    /// Length of its payload.
    pub len: u64,
}

/// A half-open byte range of a pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    /// First byte.
    pub start: u64,
    /// One past the last byte.
    pub end: u64,
}

impl Range {
    /// Bytes the range covers.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Whether the range covers nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }
}

/// A pack on disk: the payload region, the chunk index, and nothing else.
#[derive(Debug, Clone)]
pub struct Pack {
    path: PathBuf,
    /// Where the payloads start.
    payload_start: u64,
    /// Total bytes of payload.
    payload_len: u64,
    /// The snapshot this pack holds.
    snapshot: BlobId,
    /// Every chunk, in pack order.
    chunks: Vec<Located>,
}

impl Pack {
    /// Write a pack of `plan` to `path`, reading payloads from `store`.
    ///
    /// The file is written to `path` directly and is complete when this returns;
    /// a pack is a derived artifact, so a partial one is a file to be written
    /// again rather than a thing to be recovered.
    ///
    /// Memory is one chunk, not one version. The payload streams straight to the
    /// file as it is read, because a version of a game is the size of a game and
    /// buffering one to write it out would make the largest input the thing that
    /// does not fit in memory. The index does grow with the chunk count, at about
    /// forty bytes per chunk against the chunk's own bytes.
    ///
    /// The header comes first and its counts are known before the walk, because
    /// they are the plan's: `plan.len()` chunks and `plan.total_bytes()` bytes.
    /// The walk accumulates the same totals to build the index, saturating, so the
    /// two agree by construction rather than by comparison.
    ///
    /// Only the used prefix of each chunk is read, which is what the version needs
    /// and not the whole blob: a chunk that spans a file boundary is stored whole
    /// but used in part.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot yield a chunk the plan names, or if
    /// the file cannot be written. A store that yields fewer bytes than the plan
    /// says is a short read, which is an error here rather than a hole in the
    /// payload.
    pub async fn write(
        store: &BlobStore<false>,
        plan: &Plan,
        snapshot: &BlobId,
        path: &Path,
    ) -> io::Result<Self> {
        let planned_count = u64::try_from(plan.len()).unwrap_or(0);
        let planned_bytes = plan.total_bytes();

        let mut chunks = Vec::with_capacity(plan.len());
        let mut offset = 0u64;

        // The header's length depends on the counts — each varint runs until its
        // high bit clears, so a version over 127 chunks needs a second byte — which
        // is why it is built before the payload starts rather than reserved.
        let header = encode_header(planned_count, planned_bytes, *snapshot);
        let payload_start = u64::try_from(header.len()).unwrap_or(u64::MAX);

        let mut file = tfs::File::create(path).await?;
        file.write_all(&header).await?;

        for planned in plan.chunks() {
            let length = usize::try_from(planned.length).unwrap_or(0);
            let mut buffer = vec![0u8; length];

            let mut reader = store.get(&planned.id).await?;
            reader.read_exact(&mut buffer).await?;

            file.write_all(&buffer).await?;

            chunks.push(Located {
                id: planned.id,
                offset,
                len: planned.length,
            });

            // Saturating, because `Plan::total_bytes` is the same fold and the two
            // have to agree: the header reports one and the index is built on the
            // other. A plain `+=` would panic on a version large enough to wrap
            // where `total_bytes` would have clamped, which is the kind of
            // difference that only shows up at 16 exabytes.
            offset = offset.saturating_add(planned.length);
        }

        let index = encode_index(&chunks);

        file.write_all(&index).await?;
        file.write_all(&index_length(u64::try_from(index.len()).unwrap_or(0)))
            .await?;
        file.write_all(&MAGIC).await?;
        file.sync_all().await?;
        drop(file);

        Ok(Self {
            path: path.to_path_buf(),
            payload_start,
            payload_len: offset,
            snapshot: *snapshot,
            chunks,
        })
    }

    /// Open a pack, reading only its trailer and index.
    ///
    /// Cost is proportional to the index and not to the payload, which is the
    /// point of putting the index at the end: this can run before a client knows
    /// whether it wants the pack.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is too short to hold a trailer, does not end
    /// with the pack magic, does not begin with one, is a format version this
    /// build does not read, or if its index does not decode. A header that
    /// disagrees with its own index — on the chunk count, or on where the
    /// payload ends — is refused rather than believed.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = fs::File::open(path)?;
        let len = file.metadata()?.len();

        if len < u64::try_from(HEADER + TRAILER).unwrap_or(u64::MAX) {
            return Err(not_a_pack(path, "it is too short to hold a pack"));
        }

        let mut trailer = [0u8; TRAILER];
        file.seek(SeekFrom::Start(
            len - u64::try_from(TRAILER).unwrap_or(u64::MAX),
        ))?;
        file.read_exact(&mut trailer)?;

        if trailer[8..] != MAGIC {
            return Err(not_a_pack(
                path,
                "it does not end with a pack's magic, so it is truncated or is not one",
            ));
        }

        let index_len = u64::from_le_bytes(trailer[..8].try_into().unwrap_or([0; 8]));
        let index_start = len
            .checked_sub(u64::try_from(TRAILER).unwrap_or(u64::MAX))
            .and_then(|e| e.checked_sub(index_len))
            .ok_or_else(|| not_a_pack(path, "its index length runs past the start of the file"))?;

        let mut index = vec![0u8; usize::try_from(index_len).unwrap_or(0)];
        file.seek(SeekFrom::Start(index_start))?;
        file.read_exact(&mut index)?;

        let chunks = decode_index(&index)?;

        // The header, for the payload geometry and the snapshot.
        let mut header = [0u8; HEADER];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;

        if header[..4] != MAGIC {
            return Err(not_a_pack(path, "it does not begin with a pack's magic"));
        }

        let version = u32::from_le_bytes(header[4..8].try_into().unwrap_or([0; 4]));
        if version != FORMAT_VERSION {
            return Err(not_a_pack(
                path,
                &format!("it is pack format {version}, but this build reads {FORMAT_VERSION}"),
            ));
        }

        let mut snapshot = [0u8; 32];
        snapshot.copy_from_slice(&header[8..40]);

        // The two varints. Reading the header's fixed part alone would leave the
        // payload starting at the wrong offset — the counts are part of it, and
        // getting this wrong shifts every chunk by a couple of bytes rather than
        // failing. Only what lies between the fixed part and the index is read:
        // two varints need at most 20 bytes, but a pack of an empty plan has
        // just the two one-byte counts there, and asking for 20 regardless turns
        // a readable pack into a read error.
        let mut head = [0u8; COUNTS_MAX];
        let available = index_start.saturating_sub(u64::try_from(HEADER).unwrap_or(u64::MAX));
        let to_read = usize::try_from(available.min(u64::try_from(COUNTS_MAX).unwrap_or(u64::MAX)))
            .unwrap_or(0);
        file.seek(SeekFrom::Start(u64::try_from(HEADER).unwrap_or(u64::MAX)))?;
        file.read_exact(&mut head[..to_read])?;

        let (count, count_len) = read_varint(&head, 0)?;
        let (payload_len, payload_len_len) = read_varint(&head, count_len)?;

        let payload_start = u64::try_from(HEADER)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(count_len).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(payload_len_len).unwrap_or(u64::MAX));

        // The counts are redundant with the index — the index is what locates
        // chunks — so they are a claim to check rather than a source of truth. A
        // pack whose header disagrees with its own index is not one worth reading.
        if count != u64::try_from(chunks.len()).unwrap_or(u64::MAX) {
            return Err(not_a_pack(
                path,
                &format!(
                    "its header claims {count} chunks but its index lists {}",
                    chunks.len()
                ),
            ));
        }

        let end_of_payload = payload_start.saturating_add(payload_len);
        if end_of_payload != index_start {
            return Err(not_a_pack(
                path,
                &format!(
                    "its header says the payload ends at {end_of_payload}, but the index \
                     starts at {index_start}"
                ),
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            payload_start,
            payload_len,
            snapshot,
            chunks,
        })
    }

    /// Where the pack's payload region starts in the file.
    ///
    /// A [`Located::offset`] is relative to this, so a client turning a chunk into
    /// a byte range adds it. Exposed because the two differ: the header's counts
    /// are varints, and hardcoding the header's size would shift every offset.
    #[must_use]
    pub fn payload_start(&self) -> u64 {
        self.payload_start
    }

    /// The snapshot this pack holds.
    #[must_use]
    pub fn snapshot(&self) -> BlobId {
        self.snapshot
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

    /// The bytes a client already holding `have` still needs from this pack.
    ///
    /// This is the question a delta download asks, and the answer is deliberately
    /// in terms of *what it does not have*: a client that already holds every
    /// chunk gets nothing, and one holding nothing gets the whole payload region
    /// as a single range.
    ///
    /// Adjacent ranges are merged, which is the entire value of packing — the
    /// payload is laid end to end with no framing, so chunks a client needs are
    /// contiguous and the merged count is small. The measured case is one request,
    /// for a patch of between 2% and 6% of the pack depending on the tree.
    #[must_use]
    pub fn needed(&self, have: &BTreeSet<BlobId>) -> Vec<Range> {
        let mut ranges: Vec<Range> = Vec::new();

        for chunk in &self.chunks {
            if have.contains(&chunk.id) {
                continue;
            }

            let start = self.payload_start + chunk.offset;
            let end = start + chunk.len;

            match ranges.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => ranges.push(Range { start, end }),
            }
        }

        ranges
    }

    /// Bytes a client already holding `have` still needs.
    #[must_use]
    pub fn needed_bytes(&self, have: &BTreeSet<BlobId>) -> u64 {
        self.needed(have).iter().map(Range::len).sum()
    }

    /// Where a chunk is, if the pack holds it.
    #[must_use]
    pub fn locate(&self, id: &BlobId) -> Option<Located> {
        self.chunks.iter().find(|c| c.id == *id).copied()
    }
}

/// The header's fixed part plus its two varint counts.
fn encode_header(chunks: u64, payload_len: u64, snapshot: BlobId) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + COUNTS_MAX);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&snapshot);
    write_varint(&mut out, chunks);
    write_varint(&mut out, payload_len);

    out
}

/// The index: one entry per chunk, in pack order.
fn encode_index(chunks: &[Located]) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunks.len() * (32 + 18));
    for chunk in chunks {
        out.extend_from_slice(&chunk.id);
        write_varint(&mut out, chunk.offset);
        write_varint(&mut out, chunk.len);
    }

    out
}

fn decode_index(bytes: &[u8]) -> io::Result<Vec<Located>> {
    let mut out = Vec::new();
    let mut at = 0usize;

    while at < bytes.len() {
        let truncated = || io::Error::new(io::ErrorKind::InvalidData, "a pack index is truncated");

        let end = at.checked_add(32).ok_or_else(truncated)?;
        let mut id = [0u8; 32];
        id.copy_from_slice(bytes.get(at..end).ok_or_else(truncated)?);
        at = end;

        let (offset, used) = read_varint(bytes, at)?;
        at = at.checked_add(used).ok_or_else(truncated)?;
        let (len, used) = read_varint(bytes, at)?;
        at = at.checked_add(used).ok_or_else(truncated)?;

        out.push(Located { id, offset, len });
    }

    Ok(out)
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap_or(0);
        value >>= 7;

        if value == 0 {
            out.push(byte);
            return;
        }

        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], at: usize) -> io::Result<(u64, usize)> {
    let mut out: u64 = 0;
    let mut shift: u32 = 0;

    for index in 0..10usize {
        let byte = *bytes.get(at + index).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "a pack index ends mid-number")
        })?;

        // The tenth byte can only contribute the last bit of a u64; more than
        // that is an overlong encoding of something shorter.
        if index == 9 && byte > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a pack index has an overlong number",
            ));
        }

        out |= u64::from(byte & 0x7f).checked_shl(shift).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "a pack index number overflows")
        })?;

        if byte & 0x80 == 0 {
            return Ok((out, index + 1));
        }
        shift += 7;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "a pack index has an overlong number",
    ))
}

fn index_length(len: u64) -> [u8; 8] {
    len.to_le_bytes()
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

/// The chunks a client must already hold to avoid a range request.
///
/// The inverse of [`Pack::needed`]: given a pack and a set of ranges, the chunks
/// those ranges cover, so a client can record them after fetching and ask again.
#[must_use]
pub fn chunks_covering(pack: &Pack, ranges: &[Range]) -> BTreeSet<BlobId> {
    let mut out = BTreeSet::new();

    for chunk in &pack.chunks {
        let start = pack.payload_start + chunk.offset;
        let end = start + chunk.len;

        if ranges.iter().any(|r| r.start <= start && end <= r.end) {
            out.insert(chunk.id);
        }
    }

    out
}

/// The hash of a pack's contents, for naming it.
///
/// Naming by content is what makes a pack immutable: the name is the identity, so
/// two packs with the same name are the same bytes, and a cache can hold one
/// forever. It is the whole file — header, payloads, index and trailer — so the
/// name covers the format too, and a pack written by a future format version is a
/// different name rather than a collision.
///
/// Read once, after writing, in constant memory. A second pass over a pack the
/// size of a game is a real cost, and a hash that can only be computed by holding
/// the file would be a hash nobody computes.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read.
pub async fn hash_file(path: &Path) -> io::Result<BlobId> {
    let mut file = tfs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 256 * 1024];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(*hasher.finalize().as_bytes())
}
