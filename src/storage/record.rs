//! How the store's records are framed, and how a store is read back.
//!
//! A record is
//!
//! ```text
//! magic (4) | kind (1) | payload length (varint) | hash (32) | check (8) | payload
//! ```
//!
//! Two properties make the rest of the design possible.
//!
//! **Every record is self-synchronising.** The scanner stops at the first header
//! whose magic or check does not verify, and never guesses past it, so a torn
//! write at the tail cannot make it read a record boundary that is not one.
//!
//! **The header is small and the payload needs no checksum of its own.** The hash
//! in the header is the BLAKE3 of the payload — for a chunk that hash is already
//! computed for deduplication, so verifying stored content costs nothing extra,
//! and the `check` field only has to cover the 37-plus bytes of header ahead of
//! it.
//!
//! The check is a truncated BLAKE3 rather than a CRC. CRC32C would be the
//! textbook choice for 45 bytes and is far cheaper, but the cost is invisible
//! next to the seek and read each record already needs, and a cryptographic
//! digest rejects *garbage* — a region of zeroes left by an abandoned write, a
//! length field from another format — with probability 2^-64 rather than 2^-32.
//! Rejecting garbage well is the whole job here; the checksum is not defending
//! against an adversary who can write to the file, since such an adversary can
//! compute a valid check for anything they like.

use std::{
    collections::HashMap,
    io::{self, Read, Seek, SeekFrom},
};

/// Magic at the start of a store file.
///
/// A file that does not begin with this is not a store and is never written to.
/// That is what stops a recovery scan from deciding that some other file is a
/// store full of garbage and truncating it away.
pub(super) const STORE_MAGIC: [u8; 4] = *b"LPST";

/// Magic at the start of every record.
pub(super) const RECORD_MAGIC: [u8; 4] = *b"LPB1";

/// Layout version of the store, so a store written by an incompatible build is
/// reported as such rather than misparsed.
pub(super) const FORMAT_VERSION: u32 = 1;

/// Bytes of store header: magic, version, and this segment's own index.
pub(super) const STORE_HEADER: usize = 12;

/// Bytes of record header before the length: magic and kind.
const PREFIX: usize = 5;

/// Length of the content hash in a header.
const HASH: usize = 32;

/// Length of the header's own checksum.
const CHECK: usize = 8;

/// Bytes in the smallest possible header: a one-byte length of zero.
const MIN_HEADER: usize = PREFIX + 1 + HASH + CHECK;

/// Longest LEB128 encoding of a `u64`.
const MAX_VARINT: usize = 10;

/// Bytes read at a time while scanning.
///
/// A record is skipped with a seek rather than read, so the scan is proportional
/// to the number of records and not to the size of the store. What it is
/// proportional to in *reads* is set here: the buffer is refilled only when the
/// next header would not fit in what is left, so a store of small records costs
/// one read per this many bytes.
const SCAN_BUFFER: usize = 1024 * 1024;

/// What a record holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Content, addressed by the BLAKE3 of its payload. Chunks and snapshots are
    /// both this, and are told apart only by what refers to them.
    Blob,
    /// The commit record that publishes a snapshot. Its payload is the 32-byte
    /// hash of that snapshot and nothing else: a version number is a commit
    /// record's position in the log, so it cannot disagree with it.
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

/// A record header, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Header {
    /// What the record holds.
    pub kind: Kind,
    /// BLAKE3 of the payload.
    pub hash: [u8; HASH],
    /// Length of the payload.
    pub payload_len: u64,
    /// Where the payload starts, counting from the start of the store.
    pub payload_offset: u64,
    /// Bytes the record occupies, header included.
    pub total_len: u64,
}

/// Where a blob's payload sits in the store.
///
/// A [`Placement`] names a *segment* and an offset within it. The two cannot be
/// one number, because a store is several files: an offset is only meaningful
/// relative to the file it is in, and a set that is a single 40 GB file is a set
/// that will hit a filesystem limit sooner than it hits anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// Which segment of the store the payload is in, counting from zero.
    pub segment: u32,
    /// Byte offset of the payload within that segment.
    pub offset: u64,
    /// Length of the payload.
    pub len: u64,
}

/// What a scan of a store found.
#[derive(Debug, Default)]
pub(super) struct Scan {
    /// Every blob the store holds, by content hash.
    ///
    /// Populated from record headers only: the scan never reads a payload, so it
    /// is proportional to the number of records rather than to the bytes. A blob
    /// whose payload is damaged is still listed here and is caught when it is
    /// read, which is what `lepatch verify` is for.
    pub blobs: HashMap<[u8; HASH], Placement>,
    /// The snapshot hash of each committed version, oldest first.
    ///
    /// A version number is an index into this, plus one, so the numbering can
    /// never drift from the log.
    pub commits: Vec<[u8; HASH]>,
    /// First byte past the last valid record, where the store should be truncated.
    pub end: u64,
}

/// Bytes a record header for `payload_len` occupies.
///
/// A writer needs this before it writes anything, because the header goes in
/// front of the payload and the payload is a stream.
pub(super) fn header_len(payload_len: u64) -> u64 {
    (PREFIX + varint_len(payload_len) + HASH + CHECK) as u64
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
    /// The bytes do not begin with the store magic, so this is not a store file at
    /// all — somebody's data that this build does not understand.
    NotAStore,
    /// A store, of a layout version this build does not read.
    UnsupportedVersion,
}

/// Read a segment header, and say which segment it claims to be.
///
/// The segment index is in the header so that a file which has been renamed or
/// moved into another set is *detected* rather than being read as though it
/// belonged. Without it, swapping two segment files would produce a store that
/// scans cleanly and holds the wrong records in the wrong order.
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
fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// Append the header for a record to `out`.
///
/// The length must match what the reader will be asked to produce, which the
/// caller knows before it starts writing because it already has the payload's
/// size. Nothing is written to the store until then.
pub(super) fn encode_header(out: &mut Vec<u8>, kind: Kind, hash: &[u8; HASH], payload_len: u64) {
    let start = out.len();

    out.extend_from_slice(&RECORD_MAGIC);
    out.push(kind.to_byte());
    write_varint(out, payload_len);
    out.extend_from_slice(hash);
    out.extend_from_slice(&check(&out[start..]));

    debug_assert_eq!(
        out.len() - start,
        usize::try_from(header_len(payload_len)).unwrap_or(0)
    );
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

/// The header's own checksum: the first [`CHECK`] bytes of its hash.
fn check(header_without_check: &[u8]) -> [u8; CHECK] {
    let mut out = [0u8; CHECK];
    out.copy_from_slice(&blake3::hash(header_without_check).as_bytes()[..CHECK]);
    out
}

/// Decode the header of the record starting at `at` in the store.
///
/// `buffer` must begin at that record's first byte, so `at` is the record's
/// absolute offset and is only used to work out where the payload lands.
///
/// `None` means the bytes here are not the start of a valid record: the buffer
/// ran out, the magic is wrong, the kind is unknown, or the check does not
/// verify. All four mean the same thing to a scanner — stop here — and none of
/// them is distinguishable from a tail torn by a power cut, which is the case
/// this has to get right.
fn decode_header(buffer: &[u8], at: u64) -> Option<Header> {
    if buffer.len() < MIN_HEADER {
        return None;
    }

    if buffer[..4] != RECORD_MAGIC {
        return None;
    }

    let kind = Kind::from_byte(buffer[4])?;

    let (payload_len, len_bytes) = read_varint(&buffer[PREFIX..])?;
    let total = header_len(payload_len);

    // A header that does not fit in the buffer is a header that was never
    // written, not one this scanner is too small to read: the buffer always
    // holds at least MIN_HEADER, and any longer header was fully read.
    let total = usize::try_from(total).ok()?;
    if total > buffer.len() {
        return None;
    }

    let hash_at = PREFIX + len_bytes;
    let check_at = hash_at + HASH;

    // The check covers every byte of the header ahead of itself, so it is
    // verified before the hash is believed.
    if buffer[check_at..total] != check(&buffer[..check_at]) {
        return None;
    }

    let mut hash = [0u8; HASH];
    hash.copy_from_slice(&buffer[hash_at..check_at]);

    Some(Header {
        kind,
        hash,
        payload_len,
        // The payload follows the header, so it starts one header-length past
        // the record. Deriving it any other way — by subtracting the payload
        // length, say — borrows from a length that has nothing to do with where
        // the header ends, and underflows for every record bigger than the bytes
        // in front of it.
        payload_offset: at.saturating_add(total as u64),
        total_len: total as u64,
    })
}

/// Read a LEB128 integer, returning it and how many bytes it took.
fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut out: u64 = 0;
    let mut shift: u32 = 0;

    for (index, &byte) in bytes.iter().take(MAX_VARINT).enumerate() {
        // The tenth byte can only contribute the last bit of a u64; more than
        // that is an overlong encoding of something shorter.
        if index == MAX_VARINT - 1 && byte > 1 {
            return None;
        }

        out |= u64::from(byte & 0x7f).checked_shl(shift)?;

        if byte & 0x80 == 0 {
            return Some((out, index + 1));
        }

        shift += 7;
    }

    None
}

/// Read one segment's records, stopping at the first one that is not valid.
///
/// `file` must be positioned anywhere; the scan seeks for itself. It never reads
/// a payload except for a commit record, which holds 32 bytes.
///
/// Every [`Placement`] this produces is tagged with `segment`, so a scan of the
/// whole set is a scan of each segment in turn rather than a scan of a
/// concatenation — the records are in separate files and the gaps between them
/// are other files' business.
///
/// # Errors
///
/// Returns an error if the seek or read fails. A file whose contents are not a
/// store is not an error here — that is [`crate::storage::blob`]'s business,
/// because only it can tell a truncated tail from a file that was never a store.
pub(super) fn scan(file: &mut std::fs::File, segment: u32) -> io::Result<Scan> {
    let file_len = file.metadata()?.len();
    let mut scan = Scan::default();

    let mut buffer = Vec::new();
    let mut base = STORE_HEADER as u64;
    let mut position = base;

    while position < file_len {
        // Refill only when the next header would not fit, so a run of small
        // records costs one read per buffer rather than one read per record.
        if position - base + MIN_HEADER as u64 > buffer.len() as u64 {
            file.seek(SeekFrom::Start(position))?;
            buffer.clear();
            buffer.resize(
                usize::try_from((file_len - position).min(SCAN_BUFFER as u64))
                    .unwrap_or(SCAN_BUFFER),
                0,
            );
            let mut filled = 0;
            while filled < buffer.len() {
                let read = file.read(&mut buffer[filled..])?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            buffer.truncate(filled);
            base = position;

            if buffer.len() < MIN_HEADER {
                break;
            }
        }

        // `buffer` begins at `base`, which is the record's position only when the
        // buffer was refilled just now. On any later iteration of the run it
        // still holds several records, so the header is given the slice that
        // actually starts at this record and told where that is in the file, so
        // it can report an absolute payload offset.
        let at = usize::try_from(position - base).ok();
        let Some(at) = at.filter(|a| *a < buffer.len()) else {
            break;
        };

        let Some(header) = decode_header(&buffer[at..], position) else {
            break;
        };

        // A record whose payload runs past the end of the file was torn by
        // something other than a clean append.
        let end = position
            .checked_add(header.total_len)
            .and_then(|e| e.checked_add(header.payload_len))
            .filter(|e| *e <= file_len);

        let Some(end) = end else { break };

        match header.kind {
            Kind::Blob => {
                scan.blobs.insert(
                    header.hash,
                    Placement {
                        segment,
                        offset: header.payload_offset,
                        len: header.payload_len,
                    },
                );
            }
            Kind::Commit => {
                // A commit record with a payload that is not exactly a hash is
                // not a commit record this build wrote, and treating it as one
                // would publish a version pointing at nothing.
                if header.payload_len == HASH as u64 {
                    let start = usize::try_from(header.payload_offset - base).ok();
                    let start = start.filter(|s| *s + HASH <= buffer.len());

                    if let Some(snapshot) = start.map(|s| {
                        let mut hash = [0u8; HASH];
                        hash.copy_from_slice(&buffer[s..s + HASH]);
                        hash
                    }) {
                        scan.commits.push(snapshot);
                    } else {
                        // The payload is in the file but not in the buffer that
                        // was read for the header, which happens when a commit
                        // record straddles a buffer boundary. Read it directly
                        // rather than treating a real version as damaged.
                        let mut hash = [0u8; HASH];
                        file.seek(SeekFrom::Start(header.payload_offset))?;
                        if read_exact_or_eof(file, &mut hash)? {
                            scan.commits.push(hash);
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
fn read_exact_or_eof(file: &mut std::fs::File, buffer: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..])? {
            0 => return Ok(false),
            read => filled += read,
        }
    }
    Ok(true)
}
