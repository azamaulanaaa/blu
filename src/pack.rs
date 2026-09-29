//! A pack: one version's chunks, concatenated into a single distributable file.
//!
//! A store is a deduplicated pool, and the wrong shape for serving one version: a
//! restore out of a store is a request per chunk. A pack lays the same chunks end
//! to end with no framing between them, so a version's bytes are contiguous and
//! are one range request rather than thousands. The measurements behind that are
//! in the README.
//!
//! ## Determinism
//!
//! A pack is a function of its [`crate::storage::Plan`], and a plan is ordered by
//! the snapshot's chunk table rather than by the order chunks happened to be
//! written. So the same tree packs to the same bytes every time, which is what
//! makes a pack nameable by the hash of its own contents: an immutable artifact a
//! CDN can cache forever and a client can verify with one comparison.
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
//! read a byte of payload — so the cost of finding out what a pack holds does not
//! depend on how much it holds.
//!
//! The trailing magic is what makes that reverse read reliable: the length alone
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
    collections::HashSet,
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
    /// Byte offset of its payload, relative to [`Pack::payload_start`].
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
    ///
    /// Saturating, because both fields are public and a range a client built with
    /// `end` below `start` is a value this type has to survive rather than
    /// panic on: it covers nothing.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range covers nothing.
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
    /// The snapshot this pack holds.
    snapshot: BlobId,
    /// Every chunk, in pack order.
    chunks: Vec<Located>,
}

impl Pack {
    /// Write a pack of `plan` to `path`, reading payloads from `store`.
    ///
    /// Written in place and complete when this returns; a pack is derived output,
    /// so a partial one is a file to write again rather than a thing to recover.
    /// One chunk is buffered at a time, never a whole version, though the index
    /// does grow with the chunk count.
    ///
    /// The header comes first and its counts are the plan's — `plan.len()` chunks
    /// and `plan.total_bytes()` bytes — while the walk accumulates the same
    /// totals to build the index, saturating, so the two agree by construction.
    ///
    /// Only the used prefix of each chunk is read: a chunk spanning a file
    /// boundary is stored whole but used in part.
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

        // The header's length depends on the counts, since a varint runs until its
        // high bit clears, so it is built before the payload starts.
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

            // Saturating, to match `Plan::total_bytes`: the header reports one
            // total and the index is built on the other, and a plain `+=` would
            // panic where that would have clamped.
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

        // The two varints, which are part of the header: reading the fixed part
        // alone would start the payload at the wrong offset, shifting every
        // chunk rather than failing. Only what lies between the fixed part and
        // the index is read, since a pack of an empty plan has two one-byte
        // counts there and no more.
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

        // The index is a claim about where each chunk sits, and the only thing
        // checked above is the payload's extent. A chunk may therefore name an
        // offset and a length that reach past the payload, or wrap around it, and
        // the arithmetic that combines them — `payload_start + offset + len` —
        // would overflow rather than fail. Checking here means every later
        // consumer can add those numbers without re-deriving whether they hold.
        for chunk in &chunks {
            let end = chunk.offset.checked_add(chunk.len).ok_or_else(|| {
                not_a_pack(
                    path,
                    &format!(
                        "its index places a chunk at {} for {} bytes, which is not a \
                         position in any file",
                        chunk.offset, chunk.len
                    ),
                )
            })?;
            if end > payload_len {
                return Err(not_a_pack(
                    path,
                    &format!(
                        "its index places a chunk at {} for {} bytes, past the {payload_len}-byte \
                         payload",
                        chunk.offset, chunk.len
                    ),
                ));
            }
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
    /// a byte range adds it. Not a constant, because the header's counts are
    /// varints.
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

    /// The bytes a client not holding `have` still needs, as half-open ranges in
    /// *file* coordinates — [`Pack::payload_start`] plus a [`Located::offset`].
    ///
    /// Stated as what the client lacks: holding every chunk gets nothing, holding
    /// nothing gets the whole payload region as one range. Adjacent ranges are
    /// merged, which the no-framing layout is what makes worthwhile — chunks a
    /// client needs are contiguous, so the merged count is small.
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

        for chunk in &self.chunks {
            if have.contains(&chunk.id) {
                continue;
            }

            // Saturating for the same reason `open` checks it: these two sums
            // are over numbers that came out of the index, and this type is what
            // a client is told to add them up with.
            let start = self.payload_start.saturating_add(chunk.offset);
            let end = start.saturating_add(chunk.len);

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

/// The index read back.
///
/// # Errors
///
/// Returns an error if a 32-byte hash or either varint runs past the end of
/// `bytes`, if a number is overlong or overflows, or if a hash is repeated —
/// which would mean two chunks at one offset.
fn decode_index(bytes: &[u8]) -> io::Result<Vec<Located>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
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

        // Two entries for one hash would mean the same chunk at two places, and
        // `locate` answers with the first — so a client asking for a chunk it
        // already had would be told to fetch a different one.
        if !seen.insert(id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a pack index lists the same chunk twice",
            ));
        }

        out.push(Located { id, offset, len });
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
/// A range covering only part of a chunk is not that chunk and records nothing.
#[must_use]
pub fn chunks_covering(pack: &Pack, ranges: &[Range]) -> HashSet<BlobId> {
    let mut out = HashSet::new();

    for chunk in &pack.chunks {
        let start = pack.payload_start.saturating_add(chunk.offset);
        let end = start.saturating_add(chunk.len);

        if ranges.iter().any(|r| r.start <= start && end <= r.end) {
            out.insert(chunk.id);
        }
    }

    out
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
