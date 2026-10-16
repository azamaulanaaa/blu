// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! How the store's records are framed, and how a store is read back.
//!
//! A record is
//!
//! ```text
//! magic (4) | kind (1) | codec (1) | stored length (varint) | raw length (varint)
//!          | hash (32) | check (8) | payload
//! ```
//!
//! The two lengths are different numbers. That difference is most of what the
//! codec byte is for. A record's length lives in its own header, so a payload is
//! compressed *before* the header is written. A scan skips the stored length. A
//! reader receives the raw length. The hash is always of the raw content. Two
//! stores which compressed the same bytes differently therefore agree on its name.
//!
//! **Every record is self-synchronising.** The scanner stops at the first header
//! whose magic or check does not verify. It never guesses past that header. A torn
//! write at the tail therefore cannot make it read a record boundary that is not
//! one.
//!
//! **The payload needs no checksum of its own.** The header's hash is the BLAKE3
//! of the payload as it was read. For a chunk, this is the hash that deduplication
//! is keyed on, so verifying stored content costs nothing extra. That leaves
//! `check` to cover only the few dozen bytes of header ahead of it. The check is a
//! truncated BLAKE3 rather than a CRC. Computing it more cheaply costs nothing
//! next to the seek and read that each record already needs. A cryptographic digest
//! rejects *garbage*, such as a run of zeroes left by an abandoned write. It also
//! rejects a length field from another format. It does both with probability
//! 2^-64 rather than 2^-32. The check does not defend against an adversary who
//! can write to the file. That adversary can compute a valid check for anything
//! they like.
//!
//! ## Who else frames a record
//!
//! [`crate::pack`]'s payload region is a run of these records rather than a run
//! of bare payloads. A chunk therefore keeps its codec across a pack. A reader is
//! handed content rather than a frame. That makes the pack's writer a second
//! producer of these bytes and its reader a second consumer. Both go through
//! [`encode_header`] and [`decode_header`] below rather than through a copy of
//! them. Two encoders would be two formats. They would disagree the first time a
//! field moved. The encoder, the decoder, the decoded [`Header`], the record kind
//! and the two header bounds are therefore crate-visible. Nothing else here is.

use std::{
    collections::HashMap,
    io::{self, Read, Seek, SeekFrom},
};

use super::BlobId;

/// Magic at the start of a store file.
///
/// A file that does not begin with this is not a store, and the crate never
/// writes to one. A recovery scan therefore cannot decide that some other file
/// is a store and truncate it.
pub(super) const STORE_MAGIC: [u8; 4] = *b"LPST";

/// Magic at the start of every record.
pub(super) const RECORD_MAGIC: [u8; 4] = *b"LPB1";

/// Layout version of the store, so a store written by an incompatible build is
/// reported as such rather than misparsed.
///
/// **2** added a codec byte and a second length to every record header. A store
/// written by version 1 is *refused*, not converted. Its headers have a different
/// shape. Only this number tells the two shapes apart.
///
/// Public for the same reason [`crate::pack::FORMAT_VERSION`] is: a client cannot
/// work the version out of a set it has not been sent, and a refusal to read one
/// arrives after the upload. `blu serve` serves it at `GET /v1/version`.
pub const FORMAT_VERSION: u32 = 2;

/// Bytes of store header: magic, version, and this segment's own number.
pub(super) const STORE_HEADER: usize = 12;

/// Bytes of record header before the first length: magic, kind and codec.
pub(crate) const PREFIX: usize = 6;

/// Length of the content hash in a header.
const HASH: usize = 32;

/// Length of the header's own checksum.
const CHECK: usize = 8;

/// Bytes in the smallest possible header: two one-byte lengths of zero.
///
/// The `2` counts those two lengths at their minimum. This is also the smallest
/// record there is, which is a header with an empty payload. `SegmentLimit::new`
/// uses it as the floor. It is therefore not a header constant alone.
pub(crate) const MIN_HEADER: usize = PREFIX + 2 + HASH + CHECK;

/// Bytes in the largest possible header: two varints at their widest.
///
/// What a scan must have in hand before it tries to decode a header. Refilling
/// against [`MIN_HEADER`] is not enough. A header that carries two three-byte
/// varints is 52 bytes against a 48-byte floor. A record boundary in the last few
/// bytes of the buffer then fails to decode. A good segment is reported as
/// damaged.
pub(crate) const MAX_HEADER: usize = PREFIX + 2 * crate::varint::MAX + HASH + CHECK;

/// Bytes read at a time while scanning.
///
/// A record is skipped with a seek rather than read. The scan is therefore
/// proportional to the number of records and not to the size of the store. This
/// constant sets what it is proportional to in *reads*. The buffer is refilled only
/// when the next header would not fit in what is left. A refill therefore always
/// leaves [`MAX_HEADER`] bytes available to decode one.
const SCAN_BUFFER: usize = 1024 * 1024;

/// What a record holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Content, addressed by the BLAKE3 of its payload. Chunks and snapshots are
    /// both this, and are told apart only by what refers to them.
    Blob,
    /// The commit record that publishes a snapshot. Its payload is the 32-byte
    /// hash of that snapshot and nothing else. A version number is a commit
    /// record's position in the log. A version number therefore cannot disagree
    /// with the log.
    Commit,
}

impl Kind {
    fn to_byte(self) -> u8 {
        match self {
            Self::Blob => 0,
            Self::Commit => 1,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Blob),
            1 => Some(Self::Commit),
            _ => None,
        }
    }
}

/// How a record's payload is stored.
///
/// One byte after the kind. It has to be in the header rather than implied by the
/// length, because the two lengths are different things. A compressed record is
/// *not* as long as its content. A reader that mistook one length for the other
/// would produce a blob of the wrong size under the right name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    /// Stored as it was hashed, byte for byte.
    ///
    /// The default, and the fallback whenever a zstd frame did not come out
    /// smaller than its input.
    #[default]
    Raw,
    /// A zstd frame, decompressed on the way out.
    Zstd,
}

impl Codec {
    fn to_byte(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::Zstd => 1,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Raw),
            1 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// A record header, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    /// What the record holds.
    pub kind: Kind,
    /// How the payload is stored.
    pub codec: Codec,
    /// BLAKE3 of the payload **as it was read**, before any compression.
    ///
    /// Always of the content, never of the stored bytes. A chunk is addressed by
    /// what it contains. Two stores that compressed the same content differently
    /// must therefore still agree on its name.
    pub hash: [u8; HASH],
    /// Bytes the payload occupies in the file, which is what a scan skips.
    ///
    /// This is the length on disk, and for a compressed record it is *not* the
    /// length of the content. [`Header::raw_len`] is that.
    pub stored_len: u64,
    /// Bytes the payload is once decompressed, which is what a reader is given.
    pub raw_len: u64,
    /// Where the payload starts, counting from the start of the store.
    pub payload_offset: u64,
    /// Bytes the record occupies, header included.
    pub total_len: u64,
}

/// Where a blob's payload sits in the store.
///
/// A [`Placement`] names a *segment* and an offset within it. A store is
/// several files, so an offset is only meaningful relative to the one it is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// Which segment of the store the payload is in, counting from zero.
    pub segment: u32,
    /// Byte offset of the payload within that segment.
    pub offset: u64,
    /// Bytes the payload occupies in the file, which is what a reader must skip
    /// and what bounds the bytes it reads. This is the length on disk. For a
    /// compressed record it is *not* the length of the content. That length is
    /// [`Placement::raw_len`].
    ///
    /// Named for the length rather than `len`. A store has two of them, and the
    /// shorter name was the one that got read as the other. The two are equal
    /// for an uncompressed blob and different for every compressed one. Nothing
    /// about the type distinguishes them. Only the name does.
    pub stored_len: u64,
    /// Bytes the payload is once decompressed.
    ///
    /// The one to compare against a length a snapshot recorded: a snapshot's
    /// lengths are in content bytes, because it describes files rather than
    /// storage.
    pub raw_len: u64,
    /// How the payload is stored.
    pub codec: Codec,
}

/// What a scan of a store found.
#[derive(Debug, Default)]
pub(super) struct Scan {
    /// Every blob the store holds, by content hash.
    ///
    /// From record headers only. The scan never reads a payload, so a blob with
    /// a damaged payload is still listed here. That blob fails when it is read.
    pub blobs: HashMap<BlobId, Placement>,
    /// The snapshot hash of each committed version, oldest first.
    ///
    /// A version number is an index into this, plus one, so the numbering can
    /// never drift from the log.
    pub commits: Vec<BlobId>,
    /// First byte past the last valid record, where the store should be truncated.
    pub end: u64,
}

/// Bytes a record header occupies, given the two lengths it carries.
///
/// A writer needs this before writing anything, since the header precedes a
/// payload that is a stream. `raw_len` is written even for a raw record, where it
/// equals `stored_len`, rather than inferred from the codec byte.
pub(crate) fn header_len(stored_len: u64, raw_len: u64) -> u64 {
    (PREFIX + varint_len(stored_len) + varint_len(raw_len) + HASH + CHECK) as u64
}

/// The header of a segment file.
pub(super) fn encode_store_header(segment: u32) -> [u8; STORE_HEADER] {
    let mut out = [0u8; STORE_HEADER];
    out[..4].copy_from_slice(&STORE_MAGIC);
    out[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    out[8..].copy_from_slice(&segment.to_le_bytes());

    out
}

/// Why a segment header is not one this build reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HeaderFault {
    /// The bytes do not begin with the store magic, so this is not a store file.
    NotAStore,
    /// A store, of a layout version this build does not read.
    UnsupportedVersion,
}

/// Read a segment header, and say which segment it claims to be.
///
/// The segment's number is in the header so a file that does not belong is
/// detected rather than read as though it did. Without the number, two swapped
/// segment files would produce a store that scans cleanly and holds the wrong
/// records in the wrong order.
pub(super) fn decode_store_header(bytes: &[u8]) -> Result<u32, HeaderFault> {
    if bytes.len() < STORE_HEADER || bytes[..4] != STORE_MAGIC {
        return Err(HeaderFault::NotAStore);
    }

    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != FORMAT_VERSION {
        return Err(HeaderFault::UnsupportedVersion);
    }

    Ok(u32::from_le_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11],
    ]))
}

/// Bytes `value` takes as a LEB128 integer.
fn varint_len(value: u64) -> usize {
    crate::varint::len(value)
}

/// Append the header for a record to `out`.
///
/// `stored_len` is what the payload occupies in the file and `raw_len` is what it
/// is once decompressed. For a raw record they are equal. Both lengths must be
/// known first, because a header cannot be written twice.
pub(crate) fn encode_header(
    out: &mut Vec<u8>,
    kind: Kind,
    codec: Codec,
    hash: &[u8; HASH],
    stored_len: u64,
    raw_len: u64,
) {
    let start = out.len();

    out.extend_from_slice(&RECORD_MAGIC);
    out.push(kind.to_byte());
    out.push(codec.to_byte());
    write_varint(out, stored_len);
    write_varint(out, raw_len);
    out.extend_from_slice(hash);
    out.extend_from_slice(&check(&out[start..]));

    debug_assert_eq!(
        out.len() - start,
        usize::try_from(header_len(stored_len, raw_len)).unwrap_or(0)
    );
}

fn write_varint(out: &mut Vec<u8>, value: u64) {
    crate::varint::write(out, value);
}

fn check(header_without_check: &[u8]) -> [u8; CHECK] {
    super::hash::truncated_hash(header_without_check)
}

/// Decode the header of the record starting at `at` in the store.
///
/// `buffer` must begin at that record's first byte. `at` is therefore the
/// record's absolute offset. The scan uses it only to work out where the payload
/// lands.
///
/// `None` means the bytes here are not the start of a valid record. The buffer ran
/// out. The magic is wrong. The kind or codec is unknown. The check does not
/// verify. All of these mean the same thing to a scanner, which is to stop here.
/// None of them is distinguishable from a tail torn by a power cut.
pub(crate) fn decode_header(buffer: &[u8], at: u64) -> Option<Header> {
    if buffer.len() < MIN_HEADER {
        return None;
    }

    if buffer[..4] != RECORD_MAGIC {
        return None;
    }

    let kind = Kind::from_byte(buffer[4])?;
    let codec = Codec::from_byte(buffer[5])?;

    let (stored_len, stored_bytes) = read_varint(&buffer[PREFIX..])?;
    let (raw_len, raw_bytes) = read_varint(&buffer[PREFIX + stored_bytes..])?;

    // Both lengths must use the fewest bytes that hold them. Otherwise this is
    // not a header that this build wrote. Everything below measures the header
    // twice. It measures once with the widths a length takes. It measures once
    // with the widths the bytes took. A number written in more bytes than it needs
    // is the one input that makes the two disagree. Refusing it is a small thing
    // to do. Reading it is not a small thing to do. `hash_at` then points past
    // the header `total` describes, and the check below indexes with it. A
    // downloaded pack or a copied segment is exactly where such a header comes
    // from. The disagreement is therefore refused here rather than measured.
    if stored_bytes != varint_len(stored_len) || raw_bytes != varint_len(raw_len) {
        return None;
    }

    let total = header_len(stored_len, raw_len);

    // A header that does not fit in the buffer was never written, not merely
    // unreadable: the buffer always holds at least MIN_HEADER.
    let total = usize::try_from(total).ok()?;
    if total > buffer.len() {
        return None;
    }

    let hash_at = PREFIX + stored_bytes + raw_bytes;
    let check_at = hash_at + HASH;

    // The check covers every byte of the header ahead of itself. It is verified
    // before the hash is believed. A record whose two lengths do not account for
    // the header it stands in cannot be safely skipped. The code treats it the
    // same as one that was never written.
    if buffer[check_at..total] != check(&buffer[..check_at]) {
        return None;
    }

    let mut hash = [0u8; HASH];
    hash.copy_from_slice(&buffer[hash_at..check_at]);

    Some(Header {
        kind,
        codec,
        hash,
        stored_len,
        raw_len,
        // The payload starts one header-length past the record. Deriving that any
        // other way borrows from a length with nothing to do with where the
        // header ends. It underflows for any record bigger than what precedes it.
        payload_offset: at.saturating_add(total as u64),
        total_len: total as u64,
    })
}

/// Read a LEB128 integer, returning it and how many bytes it took.
///
/// A number that cannot be read is not an error here. A scan uses this to walk
/// records it has not verified. Its whole contract is that a damaged tail
/// and a plain end of file mean the same thing. The scan stops there and
/// lets the caller decide. See the module doc.
fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    crate::varint::read(bytes, 0).ok()
}

/// What a commit-only scan of one segment found.
///
/// The scan reads record headers and keeps commit hashes.
/// It keeps no blob map.
/// A caller that lists versions thus avoids the map cost.
#[derive(Debug, Default)]
pub(super) struct CommitScan {
    /// The snapshot hash of each committed version, oldest first.
    pub commits: Vec<BlobId>,
    /// First byte past the last valid record.
    pub end: u64,
}

/// Read one segment's records, and keep only the commit records among them.
///
/// One segment's worth of headers and no payloads: what a lookup that would
/// otherwise scan the whole set costs instead.
///
/// This function takes any `Read + Seek` source. It does not need a file
/// handle. A future remote backend can scan bytes it fetched with ranged
/// reads. It needs no local file. The source can start at any position. The
/// scan seeks for itself.
pub(super) fn commits_in(file: &mut (impl Read + Seek), segment: u32) -> io::Result<Vec<BlobId>> {
    Ok(scan(file, segment, Blobs::Discard)?.commits)
}

/// Read one segment and keep only its commit records.
///
/// The call takes any `Read + Seek` source.
/// It seeks for itself.
/// It never builds a blob map.
pub(super) fn scan_commits(
    file: &mut (impl Read + Seek),
    segment: u32,
) -> io::Result<CommitScan> {
    scan_commits_with_buffer(file, segment, SCAN_BUFFER)
}

/// Read commits, with the read size chosen rather than fixed.
///
/// The caller picks the buffer size.
/// A small size slows the scan.
/// It never changes the result.
pub(super) fn scan_commits_with_buffer(
    file: &mut (impl Read + Seek),
    _segment: u32,
    scan_buffer: usize,
) -> io::Result<CommitScan> {
    let file_len = file.seek(SeekFrom::End(0))?;
    let mut scan = CommitScan::default();

    let mut buffer: Vec<u8> = Vec::new();
    let mut filled = 0usize;
    let mut base = STORE_HEADER as u64;
    let mut position = base;

    while position < file_len {
        if position - base + MAX_HEADER as u64 > filled as u64 {
            file.seek(SeekFrom::Start(position))?;
            let want = usize::try_from((file_len - position).min(scan_buffer as u64))
                .unwrap_or(scan_buffer);
            if buffer.len() < want {
                buffer.resize(want, 0);
            }
            filled = 0;
            while filled < want {
                let read = file.read(&mut buffer[filled..])?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            base = position;

            if filled < MIN_HEADER {
                break;
            }
        }

        let at = usize::try_from(position - base).ok();
        let Some(at) = at.filter(|a| *a < filled) else {
            break;
        };

        let Some(header) = decode_header(&buffer[at..filled], position) else {
            break;
        };

        let end = position
            .checked_add(header.total_len)
            .and_then(|e| e.checked_add(header.stored_len))
            .filter(|e| *e <= file_len);

        let Some(end) = end else { break };

        if header.kind == Kind::Commit
            && header.raw_len == HASH as u64
            && header.codec == Codec::Raw
        {
            let start = usize::try_from(header.payload_offset - base).ok();
            let start = start.filter(|s| *s + HASH <= filled);

            if let Some(snapshot) = start.map(|s| {
                let mut hash = [0u8; HASH];
                hash.copy_from_slice(&buffer[s..s + HASH]);
                BlobId::from_bytes(hash)
            }) {
                scan.commits.push(snapshot);
            } else {
                let mut hash = [0u8; HASH];
                file.seek(SeekFrom::Start(header.payload_offset))?;
                if read_exact_or_eof(file, &mut hash)? {
                    scan.commits.push(BlobId::from_bytes(hash));
                } else {
                    break;
                }
            }
        }

        position = end;
    }

    scan.end = position;
    Ok(scan)
}

/// What a scan is being asked to collect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Blobs {
    /// Index every blob record, for a caller that is about to read from the store
    /// and needs to know where each chunk is.
    Index,
    /// Skip them, for a caller that wants only the commit records.
    ///
    /// The commit-only walk does not build the index. It reads every header
    /// and keeps the commit records. A scan that indexes builds a `HashMap`
    /// entry per blob record. At the default 4 GiB segment size and 16 KiB
    /// average chunks that is 262,144 entries, about 42 MB.
    ///
    /// This is not a micro-optimisation. The caller that wants only commits
    /// asks once per version listed. A set of a thousand versions that share
    /// a segment therefore walks that segment a thousand times over to print a
    /// list.
    Discard,
}

impl Blobs {
    /// Whether blob records go into the index.
    const fn indexes(self) -> bool {
        matches!(self, Self::Index)
    }
}

/// Read one segment's records, stopping at the first one that is not valid.
///
/// `file` may be positioned anywhere. The scan seeks for itself. It never reads a
/// payload except a commit's 32 bytes. Every [`Placement`] is tagged with
/// `segment`. A set is therefore scanned one segment at a time, never as a
/// concatenation.
///
/// This function takes any `Read + Seek` source. It does not need a file
/// handle. The same walk works over a local file. It works over an in-memory
/// buffer. It works over bytes a remote backend fetched with ranged reads.
/// The scan finds the length by seeking to the end. It does not read file
/// metadata.
///
/// # Errors
///
/// Returns an error if the seek or read fails. A file whose contents are not a
/// store is not an error here. That check belongs to [`crate::storage::blob`],
/// because only that module can tell a truncated tail from a file that was never
/// a store.
pub(super) fn scan(file: &mut (impl Read + Seek), segment: u32, blobs: Blobs) -> io::Result<Scan> {
    if blobs == Blobs::Discard {
        let commits = scan_commits(file, segment)?;
        return Ok(Scan {
            blobs: HashMap::new(),
            commits: commits.commits,
            end: commits.end,
        });
    }
    scan_with_buffer(file, segment, blobs, SCAN_BUFFER)
}

/// [`scan`], with the read size chosen rather than fixed.
///
/// `scan_buffer` is a parameter so that `benches/throughput.rs` can measure it.
/// The value was a `const`. That is the right thing for a value nobody has ever
/// varied. A constant that has never been measured is a constant nobody can say is
/// right. This is the one constant that decides how many `read` syscalls a
/// whole-set scan makes and how large a buffer it holds while doing it.
///
/// A record is skipped with a seek rather than read. The scan is therefore
/// proportional to the *record count*, not to the size of the store. This is
/// what that count is proportional to as well: one read per refill rather
/// than one per record.
///
/// Below one buffer's worth the scan still works. The refill guarantees
/// [`MAX_HEADER`] bytes whatever the size. A caller that passes something tiny
/// therefore gets a slower scan and not a wrong answer. There is no floor to
/// enforce. A floor would be a claim about the size being *sensible*, and the
/// caller is the only one who knows what it is measuring.
pub(super) fn scan_with_buffer(
    file: &mut (impl Read + Seek),
    segment: u32,
    blobs: Blobs,
    scan_buffer: usize,
) -> io::Result<Scan> {
    if blobs == Blobs::Discard {
        let commits = scan_commits_with_buffer(file, segment, scan_buffer)?;
        return Ok(Scan {
            blobs: HashMap::new(),
            commits: commits.commits,
            end: commits.end,
        });
    }
    // This code finds the length by seeking. It does not read file metadata.
    // The source can be any seekable bytes. A remote backend reports the
    // object length separately. It provides a seekable view of the fetched
    // bytes. Both cases take this path.
    let file_len = file.seek(SeekFrom::End(0))?;
    let mut scan = Scan::default();

    // The allocation and how much of it is valid are separate, deliberately.
    //
    // `resize(n, 0)` zeroes n bytes. Every one of them is then overwritten by the
    // read that follows. The old shape therefore spent a megabyte of memset on
    // every refill. That doubled the memory traffic of the scan's inner loop and
    // produced bytes that nobody read. Keeping the `Vec` at its full size and
    // tracking the valid length separately means the allocation is zeroed once,
    // when it grows, and never again.
    //
    // The cost of doing it this way is that `buffer` now holds stale bytes past
    // the valid length. Therefore *every* read from it has to be bounded by
    // `filled` rather than by `buffer.len()`. That is the part to get right. A
    // header read out of stale bytes could decode to something plausible. The
    // file's tail is the one place where fewer than `MAX_HEADER` bytes are
    // available.
    let mut buffer: Vec<u8> = Vec::new();
    let mut filled = 0usize;
    let mut base = STORE_HEADER as u64;
    let mut position = base;

    while position < file_len {
        // Refill only when the next header would not fit. A run of small records
        // then costs one read per buffer rather than one read per record. What has
        // to fit is `MAX_HEADER`, not `MIN_HEADER`. A header is variable width, and
        // the shortfall here would be read as damage rather than as a boundary to
        // move past.
        if position - base + MAX_HEADER as u64 > filled as u64 {
            file.seek(SeekFrom::Start(position))?;
            let want = usize::try_from((file_len - position).min(scan_buffer as u64))
                .unwrap_or(scan_buffer);
            if buffer.len() < want {
                buffer.resize(want, 0);
            }
            filled = 0;
            while filled < want {
                let read = file.read(&mut buffer[filled..])?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            base = position;

            if filled < MIN_HEADER {
                break;
            }
        }

        // `buffer` begins at `base`. That is this record's position only when the
        // buffer was just refilled. The header gets the slice that actually starts
        // at this record, and where that is in the file, so it can report an
        // absolute payload offset.
        //
        // Bounded by `filled`, not `buffer.len()`. A torn tail leaves fewer than
        // `MAX_HEADER` bytes. A slice that reaches into the stale part of the
        // buffer could hand the decoder a header that looks well formed. That is
        // the one case where the buffer holds a plausible answer for a question
        // the file cannot answer.
        let at = usize::try_from(position - base).ok();
        let Some(at) = at.filter(|a| *a < filled) else {
            break;
        };

        let Some(header) = decode_header(&buffer[at..filled], position) else {
            break;
        };

        // A record whose payload runs past the end of the file was torn by
        // something other than a clean append.
        let end = position
            .checked_add(header.total_len)
            .and_then(|e| e.checked_add(header.stored_len))
            .filter(|e| *e <= file_len);

        let Some(end) = end else { break };

        match header.kind {
            Kind::Blob => {
                // Discard returns through `scan_commits` above.
                // This loop thus always indexes.
                // No branch remains here.
                debug_assert!(blobs.indexes());
                scan.blobs.insert(
                    BlobId::from_bytes(header.hash),
                    Placement {
                        segment,
                        offset: header.payload_offset,
                        stored_len: header.stored_len,
                        raw_len: header.raw_len,
                        codec: header.codec,
                    },
                );
            }
            Kind::Commit => {
                // A commit record whose payload is not exactly a hash is not one
                // that this build wrote. Treating it as one would publish a version
                // that points at nothing. The raw length decides. A commit is 32
                // bytes of content whatever it occupies on disk.
                if header.raw_len == HASH as u64 && header.codec == Codec::Raw {
                    let start = usize::try_from(header.payload_offset - base).ok();
                    let start = start.filter(|s| *s + HASH <= filled);

                    if let Some(snapshot) = start.map(|s| {
                        let mut hash = [0u8; HASH];
                        hash.copy_from_slice(&buffer[s..s + HASH]);
                        BlobId::from_bytes(hash)
                    }) {
                        scan.commits.push(snapshot);
                    } else {
                        // A commit that straddles a buffer boundary has its payload
                        // in the file but not in the buffer read for the header. The
                        // code reads it directly rather than call a real version
                        // damaged.
                        let mut hash = [0u8; HASH];
                        file.seek(SeekFrom::Start(header.payload_offset))?;
                        if read_exact_or_eof(file, &mut hash)? {
                            scan.commits.push(BlobId::from_bytes(hash));
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        position = end;
    }

    scan.end = position;
    Ok(scan)
}

/// Fill `buffer`, reporting whether the whole of it was read.
fn read_exact_or_eof(file: &mut impl Read, buffer: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..])? {
            0 => return Ok(false),
            read => filled += read,
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scan works over any `Read + Seek` source. It does not need files.
    ///
    /// A remote backend provides bytes it fetched with ranged reads. It does
    /// not provide a file handle. Scanning those bytes must give the same
    /// answer as scanning the file they came from. The scan needs no
    /// filesystem. The test stays small on purpose. It uses one blob and one
    /// commit. It uses a few hundred bytes in memory.
    #[test]
    fn a_scan_over_in_memory_bytes_finds_the_same_records() {
        use std::io::Cursor;

        let mut bytes = encode_store_header(0).to_vec();

        let mut hash = [0u8; HASH];
        hash[0] = 1;
        let mut header = Vec::new();
        encode_header(&mut header, Kind::Blob, Codec::Raw, &hash, 3, 3);
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&[7u8; 3]);

        let mut snapshot = [0u8; HASH];
        snapshot[0] = 9;
        let mut commit_header = Vec::new();
        encode_header(
            &mut commit_header,
            Kind::Commit,
            Codec::Raw,
            &snapshot,
            HASH as u64,
            HASH as u64,
        );
        bytes.extend_from_slice(&commit_header);
        bytes.extend_from_slice(&snapshot);

        let mut cursor = Cursor::new(bytes.clone());
        let scanned = scan(&mut cursor, 0, Blobs::Index).unwrap();
        assert_eq!(scanned.blobs.len(), 1);
        assert_eq!(scanned.commits, vec![BlobId::from_bytes(snapshot)]);
        assert_eq!(scanned.end, bytes.len() as u64);

        let mut cursor = Cursor::new(bytes);
        let commits = commits_in(&mut cursor, 0).unwrap();
        assert_eq!(commits, vec![BlobId::from_bytes(snapshot)]);
    }

    /// A segment whose records are sized so that a header lands in the last few
    /// bytes of the scan buffer.
    ///
    /// The refill guarantees `MAX_HEADER` (66) bytes, which is the widest a header
    /// can be. That width is `PREFIX` (6), plus two varints at their ten-byte
    /// maximum (20), plus `HASH` (32) and `CHECK` (8). Guaranteeing only
    /// `MIN_HEADER` (48) was the bug. A header that carries two three-byte varints
    /// is 52 bytes. A record boundary in `[buffer.len() - 51, buffer.len() - 48]`
    /// therefore could not be decoded. The scan reported the rest of a perfectly
    /// good segment as damaged. That was a false positive on a sealed segment,
    /// which means refusing to open the set at all.
    ///
    /// Two records of 524 263 bytes put the third header at buffer offset
    /// 1 048 526, which is in that window. Buffer offset, not file offset. The
    /// file offset is 1 048 538. The scan's base is the store header, so the two
    /// offsets differ by `STORE_HEADER`.
    #[test]
    fn a_header_in_the_last_bytes_of_the_scan_buffer_is_not_damage() {
        const RECORD: u64 = 524_263;

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("scan.bin");

        let mut bytes = encode_store_header(0).to_vec();
        for n in 0u8..4 {
            let mut hash = [0u8; HASH];
            hash[0] = n;
            let payload = RECORD - header_len(RECORD, RECORD);
            let mut header = Vec::new();
            encode_header(&mut header, Kind::Blob, Codec::Raw, &hash, payload, payload);
            let total = u64::try_from(header.len()).unwrap() + payload;
            assert_eq!(
                total, RECORD,
                "the fixture must be exactly RECORD bytes per record"
            );
            bytes.extend_from_slice(&header);
            bytes.extend(std::iter::repeat_n(0u8, usize::try_from(payload).unwrap()));
        }

        std::fs::write(&file, &bytes).unwrap();

        let mut handle = std::fs::File::open(&file).unwrap();
        let scan = scan(&mut handle, 0, Blobs::Index).unwrap();
        drop(handle);

        assert_eq!(
            scan.end,
            u64::try_from(bytes.len()).unwrap(),
            "the scan stopped early: a valid record boundary was read as damage"
        );
        assert_eq!(scan.blobs.len(), 4, "every record should be indexed");
    }

    /// A scan that is not asked for the blob index finds the same records.
    ///
    /// `Blobs::Discard` skips the `HashMap` insert and nothing else. The header is
    /// still decoded, still bounds-checked against the file length, and the scan
    /// still advances by the record's own total length. If it did less than that,
    /// a commit record past a discarded blob would be missed. `commits_in` would
    /// return a version list missing whatever came after it.
    #[test]
    fn a_scan_that_skips_the_index_finds_the_same_records() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("scan.bin");

        // Four blobs and two commits, so the commits are not simply "everything
        // at the start". One of them follows a discarded blob record.
        let mut bytes = encode_store_header(0).to_vec();
        let mut commits = Vec::new();
        for n in 0u8..6 {
            if n % 3 == 2 {
                let mut hash = [0u8; HASH];
                hash[0] = 0xff;
                hash[1] = n;
                // A commit's payload is the snapshot hash, which is 32 bytes. Both
                // of its lengths say so. Without the payload the record is
                // malformed. The scan would then read the next record's header as
                // this one's content.
                let payload = HASH as u64;
                let mut header = Vec::new();
                encode_header(
                    &mut header,
                    Kind::Commit,
                    Codec::Raw,
                    &hash,
                    payload,
                    payload,
                );
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(&hash);
                commits.push(hash);
            } else {
                let mut hash = [0u8; HASH];
                hash[0] = n;
                let payload = 100;
                let mut header = Vec::new();
                encode_header(&mut header, Kind::Blob, Codec::Raw, &hash, payload, payload);
                bytes.extend_from_slice(&header);
                bytes.extend(std::iter::repeat_n(0u8, usize::try_from(payload).unwrap()));
            }
        }
        std::fs::write(&file, &bytes).unwrap();

        let indexed = {
            let mut handle = std::fs::File::open(&file).unwrap();
            scan(&mut handle, 0, Blobs::Index).unwrap()
        };
        let skipped = {
            let mut handle = std::fs::File::open(&file).unwrap();
            scan(&mut handle, 0, Blobs::Discard).unwrap()
        };

        assert_eq!(indexed.blobs.len(), 4);
        assert!(
            skipped.blobs.is_empty(),
            "the index should not have been built"
        );
        assert_eq!(
            skipped.commits, indexed.commits,
            "a skipped blob record must not hide the commit after it"
        );
        assert_eq!(skipped.commits, commits);
        assert_eq!(
            skipped.end, indexed.end,
            "both modes must agree on where the segment's records end"
        );
    }

    /// Commit-only scans skip the blob map when they list versions.
    ///
    /// The test shows that path matches the full scan on mixed records.
    #[test]
    fn a_commit_scan_matches_the_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("scan.bin");

        // Four blobs and two commits, so the commits are not simply "everything
        // at the start". One of them follows a skipped blob record.
        let mut bytes = encode_store_header(0).to_vec();
        let mut commits = Vec::new();
        for n in 0u8..6 {
            if n % 3 == 2 {
                let mut hash = [0u8; HASH];
                hash[0] = 0xff;
                hash[1] = n;
                // A commit's payload is the snapshot hash, which is 32 bytes. Both
                // of its lengths say so. Without the payload the record is
                // malformed. The scan would then read the next record's header as
                // this one's content.
                let payload = HASH as u64;
                let mut header = Vec::new();
                encode_header(
                    &mut header,
                    Kind::Commit,
                    Codec::Raw,
                    &hash,
                    payload,
                    payload,
                );
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(&hash);
                commits.push(BlobId::from_bytes(hash));
            } else {
                let mut hash = [0u8; HASH];
                hash[0] = n;
                let payload = 100;
                let mut header = Vec::new();
                encode_header(&mut header, Kind::Blob, Codec::Raw, &hash, payload, payload);
                bytes.extend_from_slice(&header);
                bytes.extend(std::iter::repeat_n(0u8, usize::try_from(payload).unwrap()));
            }
        }
        std::fs::write(&file, &bytes).unwrap();

        let indexed = {
            let mut handle = std::fs::File::open(&file).unwrap();
            scan(&mut handle, 0, Blobs::Index).unwrap()
        };
        let commit_only = {
            let mut handle = std::fs::File::open(&file).unwrap();
            scan_commits(&mut handle, 0).unwrap()
        };

        assert_eq!(
            commit_only.commits, indexed.commits,
            "a skipped blob record must not hide the commit after it"
        );
        assert_eq!(commit_only.commits, commits);
        assert_eq!(
            commit_only.end, indexed.end,
            "both modes must agree on where the segment's records end"
        );
    }
}

/// Every byte of a record header, pinned.
///
/// This is the only test that would notice a change to the record format. It is
/// here because nothing else could. `tests/pack.rs` has the equivalent for the
/// pack format. The record format had nothing at all. The one test that touched
/// `encode_header` built its fixture *with* `encode_header`. An encoding change
/// was therefore invisible to it by construction.
///
/// The bytes below are written out by hand, not captured. A captured golden pins
/// a bug just as faithfully as a correct one. Nobody reading a captured blob can
/// tell which it is. Each is annotated with what the field is and why it is there,
/// so a change to any of them reads as a decision.
///
/// A change to this test is a change to the on-disk format. [`FORMAT_VERSION`]
/// goes up. Every existing set is then either refused or unreadable. That is
/// written down in [`FORMAT_VERSION`] itself.
#[cfg(test)]
mod golden {
    use super::{Codec, Kind, decode_header, encode_header, header_len};

    /// A blob with an empty payload, so both lengths are at their one-byte
    /// minimum and the header is exactly `MIN_HEADER`.
    ///
    /// ```text
    /// 4c 50 42 31   "LPB1"          RECORD_MAGIC
    /// 00            kind Blob       0
    /// 00            codec Raw       0
    /// 00            stored_len 0    LEB128
    /// 00            raw_len 0       LEB128
    /// 00 x 32       hash            BLAKE3 of the payload, which is nothing
    /// 28 9d ae 86 ab e9 36 a5       check: BLAKE3 of the 40 bytes above
    /// ```
    const BLOB_EMPTY: &str = "4c504231000000000000000000000000000000000000000000000000000000000000000000000000289dae86abe936a5";

    /// A commit record, which is the shape the scan looks for to find versions.
    ///
    /// ```text
    /// 4c 50 42 31   "LPB1"
    /// 01            kind Commit     1
    /// 00            codec Raw       0 — always, or the scan would not find it
    /// 20            stored_len 32   LEB128: 0x20 is 32 with the top bit clear
    /// 20            raw_len 32      the same width: a commit holds only a hash
    /// ab x 32       hash            the snapshot this version publishes
    /// ce 68 48 cd 62 23 ad e8       check
    /// ```
    const COMMIT: &str = "4c50423101002020ababababababababababababababababababababababababababababababababce6848cd6223ade8";

    /// A compressed blob, chosen so both varints are more than one byte. That is
    /// the only way the two lengths and the `PREFIX`/`HASH`/`CHECK` offsets are
    /// all exercised at once.
    ///
    /// ```text
    /// 4c 50 42 31   "LPB1"
    /// 00            kind Blob
    /// 01            codec Zstd      1
    /// ac 02         stored_len 300  LEB128: 300 = 0b10_0101100, 44|0x80 then 2
    /// a0 8d 06      raw_len 100000  100000 = 0b1_1000_0110_1010_0000
    ///                               -> 32|0x80, 13|0x80, 6
    /// 11 x 32       hash            of the *content*, not of the zstd frame
    /// d2 30 0c b8 99 05 7d c4       check
    /// ```
    ///
    /// The two lengths differ by design. A reader that mixed them up would hand
    /// back a blob of the wrong size under the right name. That is why `Codec` is
    /// in the header at all. That is also why this case is pinned rather than just
    /// the easy uncompressed one.
    const BLOB_COMPRESSED: &str = "4c5042310001ac02a08d061111111111111111111111111111111111111111111111111111111111111111d2300cb899057dc4";

    fn hex(s: &str) -> Vec<u8> {
        let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        assert_eq!(clean.len() % 2, 0, "a hex literal has an even length");

        clean
            .as_bytes()
            .chunks(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).expect("hex is ascii");
                u8::from_str_radix(text, 16).expect("hex literal")
            })
            .collect()
    }

    /// One header to encode, and the bytes it has to come out as.
    struct Case {
        /// What it is, for the failure message.
        what: &'static str,
        /// The bytes, written out above.
        expected: &'static str,
        kind: Kind,
        codec: Codec,
        hash: [u8; 32],
        stored_len: u64,
        raw_len: u64,
    }

    /// The literal bytes are the record.
    #[test]
    fn a_header_is_the_bytes_this_test_says_it_is() {
        let cases = [
            Case {
                what: "an empty blob",
                expected: BLOB_EMPTY,
                kind: Kind::Blob,
                codec: Codec::Raw,
                hash: [0u8; 32],
                stored_len: 0,
                raw_len: 0,
            },
            Case {
                what: "a commit",
                expected: COMMIT,
                kind: Kind::Commit,
                codec: Codec::Raw,
                hash: [0xab; 32],
                stored_len: 32,
                raw_len: 32,
            },
            Case {
                what: "a compressed blob",
                expected: BLOB_COMPRESSED,
                kind: Kind::Blob,
                codec: Codec::Zstd,
                hash: [0x11; 32],
                stored_len: 300,
                raw_len: 100_000,
            },
        ];

        for case in &cases {
            let mut out = Vec::new();
            encode_header(
                &mut out,
                case.kind,
                case.codec,
                &case.hash,
                case.stored_len,
                case.raw_len,
            );

            assert_eq!(
                out,
                hex(case.expected),
                "the header for {} changed",
                case.what
            );
            assert_eq!(
                out.len(),
                usize::try_from(header_len(case.stored_len, case.raw_len)).unwrap_or(0),
                "and `header_len` agrees with what was written for {}",
                case.what
            );
        }
    }

    /// The decoder reads back what the encoder wrote, field by field.
    ///
    /// Separate from the bytes above on purpose. The test above says the format
    /// has not moved. This test says the two halves still agree. A change that
    /// moved both together would pass the first and fail this.
    #[test]
    fn a_header_survives_its_own_round_trip() {
        let cases = [
            Case {
                what: "an empty blob",
                expected: "",
                kind: Kind::Blob,
                codec: Codec::Raw,
                hash: [0u8; 32],
                stored_len: 0,
                raw_len: 0,
            },
            Case {
                what: "a commit",
                expected: "",
                kind: Kind::Commit,
                codec: Codec::Raw,
                hash: [0xab; 32],
                stored_len: 32,
                raw_len: 32,
            },
            Case {
                what: "a compressed blob",
                expected: "",
                kind: Kind::Blob,
                codec: Codec::Zstd,
                hash: [0x11; 32],
                stored_len: 300,
                raw_len: 100_000,
            },
            Case {
                what: "the widest varints a u64 can hold",
                expected: "",
                kind: Kind::Blob,
                codec: Codec::Raw,
                hash: [0xff; 32],
                stored_len: u64::MAX,
                raw_len: u64::MAX,
            },
        ];

        for case in &cases {
            let mut encoded = Vec::new();
            encode_header(
                &mut encoded,
                case.kind,
                case.codec,
                &case.hash,
                case.stored_len,
                case.raw_len,
            );

            let decoded = decode_header(&encoded, 0)
                .unwrap_or_else(|| panic!("{} did not decode", case.what));

            assert_eq!(decoded.kind, case.kind, "{}", case.what);
            assert_eq!(decoded.codec, case.codec, "{}", case.what);
            assert_eq!(decoded.hash, case.hash, "{}", case.what);
            assert_eq!(decoded.stored_len, case.stored_len, "{}", case.what);
            assert_eq!(decoded.raw_len, case.raw_len, "{}", case.what);
            assert_eq!(
                decoded.payload_offset,
                u64::try_from(encoded.len()).unwrap_or(0),
                "the payload of {} starts where its header ends",
                case.what
            );

            // The widest header is `MAX_HEADER`, and the scan's refill guarantee
            // rests on that number. Asserting it here means the two constants and
            // the encoder cannot drift apart unnoticed.
            if case.stored_len == u64::MAX {
                assert_eq!(
                    encoded.len(),
                    super::MAX_HEADER,
                    "two maximal varints are the widest header there is"
                );
            }
        }
    }

    /// The two header bounds are the ones the format actually has.
    ///
    /// `MIN_HEADER` is also the floor `SegmentLimit::new` enforces. It is
    /// therefore not only a header constant. Getting it wrong does not produce a
    /// rejected write. It produces a store that refuses to open itself.
    #[test]
    fn a_header_bound_is_the_size_the_format_specifies() {
        // 6 (magic, kind, codec) + 2 (two one-byte lengths) + 32 + 8
        assert_eq!(super::MIN_HEADER, 48);
        // The same, with both varints at their ten-byte maximum.
        assert_eq!(super::MAX_HEADER, 66);

        // And the empty blob really is the smallest header there is.
        let mut smallest = Vec::new();
        encode_header(&mut smallest, Kind::Blob, Codec::Raw, &[0u8; 32], 0, 0);
        assert_eq!(smallest.len(), super::MIN_HEADER);
    }
}

/// Measure the read size, which is the one constant here nobody had ever varied.
///
/// `SCAN_BUFFER` decides how many `read` syscalls a whole-set scan makes and how
/// large a buffer it holds while doing so. It was a `const`. That is the right
/// thing for a value nobody has questioned and a poor thing for a value nobody can
/// check. Criterion cannot help. A bench target is an external crate, and
/// everything in this module is `pub(super)`. Exposing a knob to measure it would
/// put an internal tuning parameter in the public API. The measurement therefore
/// lives here, where the function is.
///
/// Ignored by default because it writes a temporary file and takes a second or
/// two. Run it with:
///
/// ```text
/// cargo test --release -- --ignored --nocapture scan_read_size
/// ```
///
/// It prints a table and asserts nothing about the numbers. The output is for a
/// person. The machine it runs on is not the machine the constant was chosen on.
/// What it does assert is that every size produces the same answer. A buffer
/// below `MAX_HEADER` is the interesting boundary. A size that changed the result
/// would be a bug rather than a tuning finding.
#[test]
#[ignore = "writes a temporary file and takes a second; run it by hand"]
fn a_scan_read_size_measurement_reports_the_same_records() {
    /// Records, and bytes per record, chosen so the file is a few tens of
    /// megabytes. That is big enough that the read count is the thing being timed.
    /// It is small enough to build in a test.
    const RECORDS: usize = 20_000;
    const PAYLOAD: usize = 1024;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("scan.bin");

    let mut bytes = encode_store_header(0).to_vec();
    for n in 0..RECORDS {
        let mut hash = [0u8; HASH];
        hash[..8].copy_from_slice(&u64::try_from(n).unwrap().to_le_bytes());
        let mut header = Vec::new();
        let payload = u64::try_from(PAYLOAD).expect("fits in u64");
        encode_header(&mut header, Kind::Blob, Codec::Raw, &hash, payload, payload);
        bytes.extend_from_slice(&header);
        bytes.extend(std::iter::repeat_n(u8::try_from(n % 251).unwrap(), PAYLOAD));
    }
    std::fs::write(&file, &bytes).unwrap();
    let size = u64::try_from(bytes.len()).unwrap();

    println!("\n{RECORDS} records, {size} bytes, {SCAN_BUFFER} default");
    println!("{:>12}  {:>10}  {:>12}", "buffer", "time", "MiB/s");

    for buffer in [
        4 * 1024,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        SCAN_BUFFER,
        4 * SCAN_BUFFER,
    ] {
        // The smallest buffer that the refill guarantee makes legal, and one below
        // it. Both must give the same answer, which is the part worth asserting.
        let mut slowest = std::time::Duration::ZERO;
        let mut found = 0usize;

        for _ in 0..3 {
            let mut handle = std::fs::File::open(&file).unwrap();
            let started = std::time::Instant::now();
            let scan = scan_with_buffer(&mut handle, 0, Blobs::Index, buffer).unwrap();
            slowest += started.elapsed();
            found = scan.blobs.len();
        }

        assert_eq!(
            found, RECORDS,
            "a {buffer}-byte buffer found a different number of records"
        );

        let mib = f64::from(u32::try_from(size / (1024 * 1024)).unwrap_or(u32::MAX))
            / slowest.as_secs_f64();
        println!(
            "{buffer:>12}  {:>9.1}ms  {mib:>12.0}",
            slowest.as_secs_f64() * 1000.0
        );
    }
}

/// A length written in more bytes than it needs is not a header.
///
/// The decoder measures the header twice. It takes `total` from the widths the two
/// lengths take. It takes `hash_at` from the widths the bytes in front of them
/// actually took. `varint::read` accepts a number written wider than its own
/// size. It refuses only what does not fit a `u64`. The two counts can therefore
/// disagree. Where they disagree, the hash begins past the end of the header the
/// lengths describe. That is an index rather than a value. The record it belongs
/// to is a pack that was downloaded or a segment that was copied.
///
/// The check is recomputed over the widened header, so the width is the only
/// thing wrong with these bytes. The refusal is the width's and not the check's.
#[test]
fn a_length_wider_than_its_own_size_is_refused() {
    let mut canonical = Vec::new();
    encode_header(&mut canonical, Kind::Blob, Codec::Raw, &[0x5a; HASH], 5, 5);

    assert!(
        decode_header(&canonical, 0).is_some(),
        "the header this build writes must decode"
    );

    // The stored length's one byte becomes the same value in three: `0x85 0x80
    // 0x00`. Every byte after it moves two places along, including the check.
    let mut widened = Vec::new();
    widened.extend_from_slice(&canonical[..PREFIX]);
    widened.extend_from_slice(&[0x85, 0x80, 0x00]);
    widened.extend_from_slice(&canonical[PREFIX + 1..]);

    let without_check = widened.len() - CHECK;
    let recomputed = check(&widened[..without_check]);
    widened[without_check..].copy_from_slice(&recomputed);

    assert_eq!(
        read_varint(&widened[PREFIX..]),
        Some((5, 3)),
        "the fixture must be a length read back from three bytes, or it is not \
         the case this test is about"
    );
    assert_eq!(
        varint_len(5),
        1,
        "and it must be a length this crate would write in one"
    );
    assert!(
        decode_header(&widened, 0).is_none(),
        "a header whose two measurements of itself disagree was accepted"
    );
}

/// A torn tail is never mistaken for a record, at any length.
///
/// The scan refills only when the next header would not fit in what it has read.
/// The scan sizes a refill by what is *left in the file*. At the end of a file
/// that is less than a maximum header, so the read comes up short. The iteration
/// after that refills again, reads zero bytes, finds fewer than a minimum header
/// and stops. The tail is therefore discarded without ever reaching
/// `decode_header`. A file that ends in a complete, well-formed record header and
/// half a payload therefore still reports exactly the records that are whole.
///
/// Worth pinning because the other answer is silent. A header is small enough that
/// a plausible-looking one at a buffer's edge could decode to a blob that nothing
/// in the file contains. The index would carry it as though the store held it.
/// The test probes across tail lengths from 0 to a full buffer to establish which
/// branch this is, rather than assuming the branch from reading the loop. Every
/// one of them reports the same count.
#[test]
fn a_torn_tail_is_never_taken_for_a_record() {
    const PAYLOAD: usize = 512;
    /// Small enough that the file takes several refills. The tail is therefore
    /// read into a buffer that holds stale bytes from an earlier one.
    const SCAN_BUFFER: usize = 4096;
    const RECORDS: usize = 40;

    let mut header = Vec::new();
    let mut planted = [0u8; HASH];
    planted[..8].copy_from_slice(&u64::MAX.to_le_bytes());
    encode_header(
        &mut header,
        Kind::Blob,
        Codec::Raw,
        &planted,
        u64::try_from(PAYLOAD).expect("fits in u64"),
        u64::try_from(PAYLOAD).expect("fits in u64"),
    );
    let planted = BlobId::from_bytes(planted);

    // From nothing at all, to a header cut mid-length, to the whole header and
    // half its payload. Each is a shape that a killed run can leave behind.
    let tails: [(&str, &[u8]); 5] = [
        ("no tail", &[]),
        ("a bare magic", &header[..RECORD_MAGIC.len()]),
        ("a header prefix", &header[..PREFIX + 2 + HASH]),
        ("a header and half a payload", &header[..header.len() / 2]),
        ("a whole header", &header[..header.len()]),
    ];

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("tail.bin");

    for (what, tail) in tails {
        let mut bytes = encode_store_header(0).to_vec();
        for n in 0..RECORDS {
            let mut hash = [0u8; HASH];
            hash[..8].copy_from_slice(&u64::try_from(n).unwrap().to_le_bytes());
            let payload = u64::try_from(PAYLOAD).expect("fits in u64");
            encode_header(&mut bytes, Kind::Blob, Codec::Raw, &hash, payload, payload);
            bytes.extend(std::iter::repeat_n(u8::try_from(n % 251).unwrap(), PAYLOAD));
        }
        bytes.extend_from_slice(tail);
        std::fs::write(&file, &bytes).unwrap();

        let mut handle = std::fs::File::open(&file).unwrap();
        let scan = scan_with_buffer(&mut handle, 0, Blobs::Index, SCAN_BUFFER).unwrap();
        drop(handle);

        assert_eq!(
            scan.blobs.len(),
            RECORDS,
            "{what}: the scan must stop at the damage, finding only the {RECORDS} \
             records that are whole"
        );
        assert!(
            !scan.blobs.contains_key(&planted),
            "{what}: the tail's own hash reached the index, so a record was read out \
             of bytes that do not form one"
        );
    }
}
