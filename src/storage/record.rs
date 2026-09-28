//! How the store's records are framed, and how a store is read back.
//!
//! A record is
//!
//! ```text
//! magic (4) | kind (1) | codec (1) | stored length (varint) | raw length (varint)
//!          | hash (32) | check (8) | payload
//! ```
//!
//! The two lengths are different numbers, which is most of what the codec byte is
//! for. A record's length lives in its own header, so a payload is compressed
//! *before* the header is written. The stored length is what a scan skips, the
//! raw length is what a reader is handed, and the hash is always of the raw
//! content, so two stores which compressed the same bytes differently still agree
//! on its name.
//!
//! **Every record is self-synchronising.** The scanner stops at the first header
//! whose magic or check does not verify and never guesses past it, so a torn write
//! at the tail cannot make it read a record boundary that is not one.
//!
//! **The payload needs no checksum of its own.** The header's hash is the BLAKE3
//! of the payload as it was read — for a chunk, the hash deduplication is keyed
//! on, so verifying stored content costs nothing extra — which leaves `check` to
//! cover only the few dozen bytes of header ahead of it. The check is a truncated
//! BLAKE3 rather than a CRC: cheaper to compute is invisible next to the seek and
//! read each record already needs, and a cryptographic digest rejects *garbage*
//! — a run of zeroes left by an abandoned write, a length field from another
//! format — with probability 2^-64 rather than 2^-32. It is not defending against
//! an adversary who can write to the file, who can compute a valid check for
//! anything they like.

use std::{
    collections::HashMap,
    io::{self, Read, Seek, SeekFrom},
};

/// Magic at the start of a store file.
///
/// A file that does not begin with this is not a store and is never written to,
/// so a recovery scan cannot decide some other file is a store and truncate it.
pub(super) const STORE_MAGIC: [u8; 4] = *b"LPST";

/// Magic at the start of every record.
pub(super) const RECORD_MAGIC: [u8; 4] = *b"LPB1";

/// Layout version of the store, so a store written by an incompatible build is
/// reported as such rather than misparsed.
///
/// **2** added a codec byte and a second length to every record header. A store
/// written by version 1 is *refused*, not converted: its headers are a different
/// shape, and the only way to tell them apart is this number.
pub(super) const FORMAT_VERSION: u32 = 2;

/// Bytes of store header: magic, version, and this segment's own number.
pub(super) const STORE_HEADER: usize = 12;

/// Bytes of record header before the first length: magic, kind and codec.
const PREFIX: usize = 6;

/// Length of the content hash in a header.
const HASH: usize = 32;

/// Length of the header's own checksum.
const CHECK: usize = 8;

/// Bytes in the smallest possible header: two one-byte lengths of zero.
const MIN_HEADER: usize = PREFIX + 2 + HASH + CHECK;

/// Longest LEB128 encoding of a `u64`.
const MAX_VARINT: usize = 10;

/// Bytes read at a time while scanning.
///
/// A record is skipped with a seek rather than read, so the scan is proportional
/// to the number of records and not to the size of the store. This sets what it
/// is proportional to in *reads*: the buffer is refilled only when the next
/// header would not fit in what is left, and always holds at least
/// [`MIN_HEADER`].
const SCAN_BUFFER: usize = 1024 * 1024;

/// What a record holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
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

/// How a record's payload is stored.
///
/// One byte after the kind. It has to be in the header rather than implied by the
/// length, because the two lengths are different things: a compressed record is
/// *not* as long as its content, and a reader that mistook one for the other
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
pub(super) struct Header {
    /// What the record holds.
    pub kind: Kind,
    /// How the payload is stored.
    pub codec: Codec,
    /// BLAKE3 of the payload **as it was read**, before any compression.
    ///
    /// Always of the content, never of the stored bytes: a chunk is addressed by
    /// what it contains, so two stores that compressed the same content
    /// differently must still agree on its name.
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
/// A [`Placement`] names a *segment* and an offset within it, because a store is
/// several files and an offset is only meaningful relative to the one it is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// Which segment of the store the payload is in, counting from zero.
    pub segment: u32,
    /// Byte offset of the payload within that segment.
    pub offset: u64,
    /// Bytes the payload occupies in the file, which is what a reader must skip
    /// and what bounds the bytes it reads. Not the content length for a compressed
    /// blob — that is [`Placement::raw_len`].
    pub len: u64,
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
    /// From record headers only: the scan never reads a payload, so a blob with a
    /// damaged payload is still listed here and fails when it is read.
    pub blobs: HashMap<[u8; HASH], Placement>,
    /// The snapshot hash of each committed version, oldest first.
    ///
    /// A version number is an index into this, plus one, so the numbering can
    /// never drift from the log.
    pub commits: Vec<[u8; HASH]>,
    /// First byte past the last valid record, where the store should be truncated.
    pub end: u64,
}

/// Bytes a record header occupies, given the two lengths it carries.
///
/// A writer needs this before writing anything, since the header precedes a
/// payload that is a stream. `raw_len` is written even for a raw record, where it
/// equals `stored_len`, rather than inferred from the codec byte.
pub(super) fn header_len(stored_len: u64, raw_len: u64) -> u64 {
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
/// detected rather than read as though it did: without it, two swapped segment
/// files would produce a store that scans cleanly and holds the wrong records in
/// the wrong order.
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
/// `stored_len` is what the payload occupies in the file and `raw_len` what it is
/// once decompressed; for a raw record they are equal. Both must be known first,
/// because a header cannot be written twice.
pub(super) fn encode_header(
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

fn check(header_without_check: &[u8]) -> [u8; CHECK] {
    super::truncated_hash(header_without_check)
}

/// Decode the header of the record starting at `at` in the store.
///
/// `buffer` must begin at that record's first byte, so `at` is the record's
/// absolute offset and is only used to work out where the payload lands.
///
/// `None` means the bytes here are not the start of a valid record: the buffer
/// ran out, the magic is wrong, the kind or codec is unknown, or the check does
/// not verify. All of them mean the same thing to a scanner — stop here — and
/// none is distinguishable from a tail torn by a power cut.
fn decode_header(buffer: &[u8], at: u64) -> Option<Header> {
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
    let total = header_len(stored_len, raw_len);

    // A header that does not fit in the buffer was never written, not merely
    // unreadable: the buffer always holds at least MIN_HEADER.
    let total = usize::try_from(total).ok()?;
    if total > buffer.len() {
        return None;
    }

    let hash_at = PREFIX + stored_bytes + raw_bytes;
    let check_at = hash_at + HASH;

    // The check covers every byte of the header ahead of itself, so it is
    // verified before the hash is believed. A record whose two lengths do not
    // account for the header it stands in cannot be safely skipped, and is
    // treated the same as one that was never written.
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
        // header ends, and underflows for any record bigger than what precedes it.
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

/// Read one segment's records, and keep only the commit records among them.
///
/// One segment's worth of headers and no payloads: what a lookup that would
/// otherwise scan the whole set costs instead.
pub(super) fn commits_in(file: &mut std::fs::File, segment: u32) -> io::Result<Vec<[u8; HASH]>> {
    Ok(scan(file, segment)?.commits)
}

/// Read one segment's records, stopping at the first one that is not valid.
///
/// `file` may be positioned anywhere; the scan seeks for itself, and never reads a
/// payload except a commit's 32 bytes. Every [`Placement`] is tagged with
/// `segment`, so a set is scanned one segment at a time and never as a
/// concatenation.
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

        // `buffer` begins at `base`, which is this record's position only when the
        // buffer was just refilled. Give the header the slice that actually
        // starts at this record, and where that is in the file, so it can report
        // an absolute payload offset.
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
            .and_then(|e| e.checked_add(header.stored_len))
            .filter(|e| *e <= file_len);

        let Some(end) = end else { break };

        match header.kind {
            Kind::Blob => {
                scan.blobs.insert(
                    header.hash,
                    Placement {
                        segment,
                        offset: header.payload_offset,
                        len: header.stored_len,
                        raw_len: header.raw_len,
                        codec: header.codec,
                    },
                );
            }
            Kind::Commit => {
                // A commit record whose payload is not exactly a hash is not one
                // this build wrote, and treating it as one would publish a
                // version pointing at nothing. The raw length decides: a commit
                // is 32 bytes of content whatever it occupies on disk.
                if header.raw_len == HASH as u64 && header.codec == Codec::Raw {
                    let start = usize::try_from(header.payload_offset - base).ok();
                    let start = start.filter(|s| *s + HASH <= buffer.len());

                    if let Some(snapshot) = start.map(|s| {
                        let mut hash = [0u8; HASH];
                        hash.copy_from_slice(&buffer[s..s + HASH]);
                        hash
                    }) {
                        scan.commits.push(snapshot);
                    } else {
                        // A commit straddling a buffer boundary has its payload in
                        // the file but not in the buffer read for the header; read
                        // it directly rather than call a real version damaged.
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
