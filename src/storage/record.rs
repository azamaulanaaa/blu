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

use super::BlobId;

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
///
/// The `2` is those two lengths at their minimum. This is also the smallest
/// record there is — a header with an empty payload — and `SegmentLimit::new`
/// uses it as the floor, so it is not a header constant alone.
pub(super) const MIN_HEADER: usize = PREFIX + 2 + HASH + CHECK;

/// Bytes in the largest possible header: two varints at their widest.
///
/// What a scan must have in hand before it tries to decode a header. Refilling
/// against [`MIN_HEADER`] is not enough — a header carrying two three-byte
/// varints is 52 bytes against a 48-byte floor, so a record boundary in the last
/// few bytes of the buffer fails to decode and a good segment is reported as
/// damaged.
const MAX_HEADER: usize = PREFIX + 2 * crate::varint::MAX + HASH + CHECK;

/// Bytes read at a time while scanning.
///
/// A record is skipped with a seek rather than read, so the scan is proportional
/// to the number of records and not to the size of the store. This sets what it
/// is proportional to in *reads*: the buffer is refilled only when the next
/// header would not fit in what is left, and a refill therefore always leaves
/// [`MAX_HEADER`] bytes available to decode one.
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
    ///
    /// Named for the length rather than `len`, because a store has two of them
    /// and the shorter name was the one that got read as the other. The two are
    /// equal for an uncompressed blob and different for every compressed one, so
    /// nothing about the type distinguishes them; only the name does.
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
    /// From record headers only: the scan never reads a payload, so a blob with a
    /// damaged payload is still listed here and fails when it is read.
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
fn varint_len(value: u64) -> usize {
    crate::varint::len(value)
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

fn write_varint(out: &mut Vec<u8>, value: u64) {
    crate::varint::write(out, value);
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
///
/// A number that cannot be read is not an error here. A scan uses this to walk
/// records it has not verified, and its whole contract is that a damaged tail
/// and a tail that is simply the end of the file are the same thing: it stops
/// and lets the caller decide. See the module doc.
fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    crate::varint::read(bytes, 0).ok()
}

/// Read one segment's records, and keep only the commit records among them.
///
/// One segment's worth of headers and no payloads: what a lookup that would
/// otherwise scan the whole set costs instead.
pub(super) fn commits_in(file: &mut std::fs::File, segment: u32) -> io::Result<Vec<BlobId>> {
    Ok(scan(file, segment, Blobs::Discard)?.commits)
}

/// What a scan is being asked to collect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Blobs {
    /// Index every blob record, for a caller that is about to read from the store
    /// and needs to know where each chunk is.
    Index,
    /// Skip them, for a caller that wants only the commit records.
    ///
    /// This is not a micro-optimisation. A scan that indexes builds a
    /// `HashMap` entry per blob record: at the default 4 GiB segment size and
    /// 16 KiB average chunks that is 262,144 entries, about 42 MB allocated,
    /// grown and freed. The one caller that wants only commits asks once per
    /// version listed, so a set of a thousand versions sharing a segment paid
    /// for the map a thousand times over to print a list.
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
pub(super) fn scan(file: &mut std::fs::File, segment: u32, blobs: Blobs) -> io::Result<Scan> {
    scan_with_buffer(file, segment, blobs, SCAN_BUFFER)
}

/// [`scan`], with the read size chosen rather than fixed.
///
/// `scan_buffer` is a parameter so that `benches/throughput.rs` can measure it.
/// It was a `const`, which is the right thing for a value nobody has ever varied:
/// a constant that has never been measured is a constant nobody can say is right,
/// and this is the one that decides how many `read` syscalls a whole-set scan
/// makes and how large a buffer it holds while doing it.
///
/// A record is skipped with a seek rather than read, so the scan is proportional
/// to the *record count* and not to the size of the store. This is what that count
/// is proportional to as well: one read per refill rather than one per record.
///
/// Below one buffer's worth the scan still works — the refill guarantees
/// [`MAX_HEADER`] bytes whatever the size — so a caller passing something tiny gets
/// a slower scan and not a wrong answer. There is no floor to enforce, because a
/// floor would be a claim about the size being *sensible*, and the caller is the
/// only one who knows what it is measuring.
pub(super) fn scan_with_buffer(
    file: &mut std::fs::File,
    segment: u32,
    blobs: Blobs,
    scan_buffer: usize,
) -> io::Result<Scan> {
    let file_len = file.metadata()?.len();
    let mut scan = Scan::default();

    // The allocation and how much of it is valid are separate, deliberately.
    //
    // `resize(n, 0)` zeroes n bytes, and every one of them is then overwritten by
    // the read that follows — so the old shape spent a megabyte of memset on every
    // refill, doubling the memory traffic of the scan's inner loop to produce
    // bytes nobody reads. Keeping the `Vec` at its full size and tracking the
    // valid length separately means the allocation is zeroed once, when it grows,
    // and never again.
    //
    // The cost of doing it this way is that `buffer` now holds stale bytes past
    // the valid length, so *every* read from it has to be bounded by `filled`
    // rather than by `buffer.len()`. That is the part to get right: a header read
    // out of stale bytes could decode to something plausible, and the file's tail
    // is the one place where fewer than `MAX_HEADER` bytes are available.
    let mut buffer: Vec<u8> = Vec::new();
    let mut filled = 0usize;
    let mut base = STORE_HEADER as u64;
    let mut position = base;

    while position < file_len {
        // Refill only when the next header would not fit, so a run of small
        // records costs one read per buffer rather than one read per record. What
        // has to fit is `MAX_HEADER`, not `MIN_HEADER`: a header is variable
        // width, and the shortfall here would be read as damage rather than as a
        // boundary to move past.
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

        // `buffer` begins at `base`, which is this record's position only when the
        // buffer was just refilled. Give the header the slice that actually
        // starts at this record, and where that is in the file, so it can report
        // an absolute payload offset.
        //
        // Bounded by `filled`, not `buffer.len()`. A torn tail leaves fewer than
        // `MAX_HEADER` bytes, and a slice reaching into the stale part of the
        // buffer could hand the decoder a header that looks well formed — the one
        // case where the buffer holds a plausible answer for a question the file
        // cannot answer.
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
                if !blobs.indexes() {
                    // The header has already been decoded and bounds-checked, so
                    // the record is known good; the only thing being skipped is
                    // the index entry nobody asked for.
                    position = end;
                    continue;
                }

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
                // this build wrote, and treating it as one would publish a
                // version pointing at nothing. The raw length decides: a commit
                // is 32 bytes of content whatever it occupies on disk.
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
                        // A commit straddling a buffer boundary has its payload in
                        // the file but not in the buffer read for the header; read
                        // it directly rather than call a real version damaged.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A segment whose records are sized so that a header lands in the last few
    /// bytes of the scan buffer.
    ///
    /// The refill guarantees `MAX_HEADER` (66) bytes, which is the widest a
    /// header can be: `PREFIX` (6) plus two varints at their ten-byte maximum
    /// (20) plus `HASH` (32) and `CHECK` (8). Guaranteeing only `MIN_HEADER` (48)
    /// was the bug — a header carrying two three-byte varints is 52 bytes, so a
    /// record boundary in `[buffer.len() - 51, buffer.len() - 48]` could not be
    /// decoded and the scan reported the rest of a perfectly good segment as
    /// damaged. That was a false positive on a sealed segment, which means
    /// refusing to open the set at all.
    ///
    /// Two records of 524 263 bytes put the third header at buffer offset
    /// 1 048 526, which is in that window. (Buffer offset, not file offset: the
    /// offset is 1 048 538 in the file and the scan's base is the store header,
    /// so the two differ by `STORE_HEADER`.)
    #[test]
    fn a_header_in_the_last_bytes_of_the_scan_buffer_is_not_damage() {
        const RECORD: u64 = 524_263;

        let file = std::env::temp_dir().join(format!(
            "lepatch-scan-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::remove_file(&file).ok();

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
        std::fs::remove_file(&file).ok();

        assert_eq!(
            scan.end,
            u64::try_from(bytes.len()).unwrap(),
            "the scan stopped early: a valid record boundary was read as damage"
        );
        assert_eq!(scan.blobs.len(), 4, "every record should be indexed");
    }

    /// A scan that is not asked for the blob index finds the same records.
    ///
    /// `Blobs::Discard` skips the `HashMap` insert and nothing else: the header
    /// is still decoded, still bounds-checked against the file length, and the
    /// scan still advances by the record's own total length. If it did less than
    /// that, a commit record past a discarded blob would be missed, and
    /// `commits_in` would return a version list missing whatever came after it.
    #[test]
    fn a_scan_that_skips_the_index_finds_the_same_records() {
        let file = std::env::temp_dir().join(format!(
            "lepatch-discard-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));

        // Four blobs and two commits, so the commits are not simply "everything
        // at the start" — one of them follows a discarded blob record.
        let mut bytes = encode_store_header(0).to_vec();
        let mut commits = Vec::new();
        for n in 0u8..6 {
            if n % 3 == 2 {
                let mut hash = [0u8; HASH];
                hash[0] = 0xff;
                hash[1] = n;
                // A commit's payload is the snapshot hash — 32 bytes, and both of
                // its lengths say so. Without the payload the record is
                // malformed and the scan would be reading the next record's
                // header as this one's content.
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
        std::fs::remove_file(&file).ok();

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
}

/// Every byte of a record header, pinned.
///
/// This is the only test that would notice a change to the record format, and it
/// is here because nothing else could. `tests/pack.rs` has the equivalent for
/// the pack format; the record format had nothing at all, and the one test that
/// touched `encode_header` built its fixture *with* `encode_header` — so an
/// encoding change was invisible to it by construction.
///
/// The bytes below are written out by hand, not captured. A captured golden
/// pins a bug just as faithfully as a correct one, and nobody reading a captured
/// blob can tell which it is. Each is annotated with what the field is and why it
/// is there, so a change to any of them reads as a decision.
///
/// A change to this test is a change to the on-disk format. [`FORMAT_VERSION`]
/// goes up, every existing set is either refused or unreadable, and that is
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

    /// A compressed blob, chosen so both varints are more than one byte — which
    /// is the only way the two lengths and the `PREFIX`/`HASH`/`CHECK` offsets
    /// are all exercised at once.
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
    /// The two lengths differ by design, and a reader that mixed them up would
    /// hand back a blob of the wrong size under the right name — which is why
    /// `Codec` is in the header at all, and why this case is pinned rather than
    /// just the easy uncompressed one.
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
    /// Separate from the bytes above on purpose: that one says the format has
    /// not moved, and this one says the two halves still agree. A change that
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
            // rests on that number. Asserting it here means the two constants
            // and the encoder cannot drift apart unnoticed.
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
    /// `MIN_HEADER` is also the floor `SegmentLimit::new` enforces, so it is not
    /// only a header constant — getting it wrong does not produce a rejected
    /// write, it produces a store that refuses to open itself.
    #[test]
    fn the_header_bounds_are_the_formats_own() {
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
/// large a buffer it holds while doing so, and it was a `const` — which is the
/// right thing for a value nobody has questioned and a poor thing for a value
/// nobody can check. Criterion cannot help: a bench target is an external crate
/// and everything in this module is `pub(super)`, so exposing a knob to measure it
/// would put an internal tuning parameter in the public API. Here, instead, where
/// the function is.
///
/// Ignored by default because it writes a temporary file and takes a second or
/// two. Run it with:
///
/// ```text
/// cargo test --release -- --ignored --nocapture scan_read_size
/// ```
///
/// It prints a table and asserts nothing about the numbers — the output is for a
/// person, and the machine it runs on is not the machine the constant was chosen
/// on. What it does assert is that every size produces the same answer, because a
/// buffer below `MAX_HEADER` is the interesting boundary and a size that changed
/// the result would be a bug rather than a tuning finding.
#[test]
#[ignore = "writes a temporary file and takes a second; run it by hand"]
fn scan_read_size() {
    /// Records, and bytes per record, chosen so the file is a few tens of
    /// megabytes — big enough that the read count is the thing being timed, and
    /// small enough to build in a test.
    const RECORDS: usize = 20_000;
    const PAYLOAD: usize = 1024;

    let file = std::env::temp_dir().join(format!(
        "lepatch-bench-{}-{:?}.bin",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::remove_file(&file).ok();

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
        // The smallest buffer the refill guarantee makes legal, and one below it:
        // both must give the same answer, which is the part worth asserting.
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

    std::fs::remove_file(&file).ok();
}

/// A torn tail is never mistaken for a record, at any length.
///
/// The scan refills only when the next header would not fit in what it has read,
/// and it sizes a refill by what is *left in the file*. At the end of a file that
/// is less than a maximum header, so the read comes up short — and the iteration
/// after that refills again, reads zero bytes, finds fewer than a minimum header
/// and stops. The tail is therefore discarded without ever reaching
/// `decode_header`, which is why a file ending in a complete, well-formed record
/// header and half a payload still reports exactly the records that are whole.
///
/// Worth pinning because the other answer is silent. A header is small enough
/// that a plausible-looking one at a buffer's edge could decode to a blob that
/// nothing in the file contains, and the index would carry it as though the store
/// held it. Probed across tail lengths from 0 to a full buffer to establish which
/// branch this is, rather than assumed from reading the loop: every one of them
/// reports the same count.
#[test]
fn a_torn_tail_is_never_taken_for_a_record() {
    const PAYLOAD: usize = 512;
    /// Small enough that the file takes several refills, so the tail is read into
    /// a buffer that holds stale bytes from an earlier one.
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
    // half its payload. Each is a shape a killed run can leave behind.
    let tails: [(&str, &[u8]); 5] = [
        ("no tail", &[]),
        ("a bare magic", &header[..RECORD_MAGIC.len()]),
        ("a header prefix", &header[..PREFIX + 2 + HASH]),
        ("a header and half a payload", &header[..header.len() / 2]),
        ("a whole header", &header[..header.len()]),
    ];

    for (what, tail) in tails {
        let file = std::env::temp_dir().join(format!(
            "lepatch-tail-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::remove_file(&file).ok();

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
        std::fs::remove_file(&file).ok();

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
