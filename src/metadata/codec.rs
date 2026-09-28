//! The snapshot wire format.
//!
//! Hand-written rather than delegated, and that is the whole point of it. The
//! reason `bincode` ended up on an advisory list is that a project's policy
//! decision became this tool's problem: the encoding of a user's backup set is
//! the one thing that has to keep working, and it was at the mercy of a crate
//! whose maintainers had walked away. Moving to a different third party lowers
//! the probability and repeats the exposure. Owning the format removes it.
//!
//! The format is small enough to own. A snapshot is six flat structs of numbers,
//! byte strings and byte arrays — no polymorphism, no generics, no maps — so
//! the codec is a couple of hundred lines with nothing to get subtly wrong.
//!
//! ```text
//! magic     8 bytes,  b"LEPATCH\0"
//! version   u32, little endian
//! body:
//!   varint   count of files
//!     per file:   path, u64 len, u32 mode, optional timestamp
//!   varint   count of directories
//!     per dir:    path, u32 mode, optional timestamp
//!   varint   count of distinct chunks
//!     per chunk:  32 raw bytes of hash
//!   varint   count of chunk slices
//!     per slice:  varint chunk, varint file, varint chunk offset,
//!                 varint file offset, varint length
//!   varint   count of links
//!     per link:   path, path, u8 kind
//! ```
//!
//! A `varint` is LEB128. Lengths and indices are variable width because most are
//! small; byte counts, modes and file lengths are fixed width because they are
//! not.
//!
//! # Decoding is the security-relevant half
//!
//! A snapshot is untrusted input: it comes out of a file that gets synced,
//! shared and backed up like anything else, and a tampered one should not be
//! able to do anything but fail. So every length is checked against the bytes
//! actually remaining *before* anything is allocated for it, which is what stops
//! a four-byte count from asking for four gigabytes. A count can never exceed the
//! input it describes, so no allocation is ever larger than the blob.
//!
//! Trailing bytes after the body are refused. The blob's length is recorded in
//! its key, so a blob with anything after the snapshot in it is damaged, and
//! quietly ignoring the difference would hide exactly the kind of corruption
//! this whole check exists to catch.

use std::path::{Path, PathBuf};

use super::{Dir, File, FileChunk, Link, LinkKind, Snapshot, Timestamp};

/// Refuse a count that cannot possibly fit in the remaining input.
///
/// Each entry of every collection needs at least one byte on the wire, so a count
/// larger than the bytes left is malformed however it came to be. This is the
/// check that makes a hostile count harmless rather than merely unlikely to
/// cause trouble.
fn plausible(count: u64, remaining: usize) -> bool {
    count <= remaining as u64
}

/// A cursor over the bytes being decoded.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

/// Why a body could not be decoded.
///
/// Distinct from [`super::Error`] in that every case here means the input is
/// malformed, and the message is written for whoever has to work out which blob
/// is damaged.
#[derive(Debug)]
pub struct Malformed(pub &'static str);

impl std::fmt::Display for Malformed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Malformed {}

type Result<T> = std::result::Result<T, Malformed>;

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    fn is_done(&self) -> bool {
        self.at == self.bytes.len()
    }

    /// Take `count` bytes, or fail if there are not that many left.
    ///
    /// The bounds check is here, before the slice, so nothing is ever read past
    /// the end and nothing is allocated for a length the input cannot support.
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Malformed("the snapshot ends in the middle of a value"))?;

        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        let mut out = [0u8; 8];
        out.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(out))
    }

    fn i64(&mut self) -> Result<i64> {
        self.u64().map(|v| i64::from_le_bytes(v.to_le_bytes()))
    }

    /// An LEB128 unsigned integer.
    ///
    /// At most ten bytes: a `u64` needs at most that many, and continuing past
    /// it would mean either an overflow or a number no length could describe.
    fn varint(&mut self) -> Result<u64> {
        let mut out: u64 = 0;
        let mut shift: u32 = 0;

        for index in 0..10 {
            let byte = self.u8()?;

            // The tenth byte can only contribute the single remaining bit of a
            // u64; anything more is an overlong encoding of something shorter.
            if index == 9 && byte > 1 {
                return Err(Malformed("a length in the snapshot is too large"));
            }

            out |= u64::from(byte & 0x7f)
                .checked_shl(shift)
                .ok_or(Malformed("a length in the snapshot is too large"))?;

            if byte & 0x80 == 0 {
                return Ok(out);
            }

            shift += 7;
        }

        Err(Malformed("a length in the snapshot is too large"))
    }

    /// A count, checked against what is left of the input before it is used.
    fn count(&mut self) -> Result<usize> {
        let count = self.varint()?;
        let count = usize::try_from(count).map_err(|_| Malformed("a count is too large"))?;

        if !plausible(count as u64, self.remaining()) {
            return Err(Malformed(
                "the snapshot claims more entries than it has bytes",
            ));
        }

        Ok(count)
    }

    /// A path, as raw bytes.
    ///
    /// Not as a string, because a path is not required to be valid UTF-8 and
    /// refusing to store one loses a file. This is the one place the old serde
    /// helper existed, and doing it directly means no wrapper type and no
    /// per-field attribute.
    fn path(&mut self) -> Result<PathBuf> {
        let len =
            usize::try_from(self.varint()?).map_err(|_| Malformed("a path length is too large"))?;

        let bytes = self.take(len)?;

        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;
            Ok(PathBuf::from(OsString::from_vec(bytes.to_vec())))
        }

        #[cfg(not(unix))]
        {
            // Elsewhere a path is already text, so a byte sequence that is not
            // UTF-8 cannot have come from a real filesystem.
            let text = std::str::from_utf8(bytes)
                .map_err(|_| Malformed("a path is not valid Unicode on this platform"))?;
            Ok(PathBuf::from(text))
        }
    }

    /// An optional timestamp, as a presence tag then the two numbers.
    fn optional_timestamp(&mut self) -> Result<Option<Timestamp>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(Timestamp {
                secs: self.i64()?,
                nanos: self.u32()?,
            })),
            _ => Err(Malformed("a timestamp has an invalid presence tag")),
        }
    }

    fn hash(&mut self) -> Result<[u8; 32]> {
        let bytes = self.take(32)?;
        let mut out = [0u8; 32];
        out.copy_from_slice(bytes);
        Ok(out)
    }
}

/// Append-only buffer for the encode half.
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn varint(&mut self, mut value: u64) {
        loop {
            let byte = u8::try_from(value & 0x7f).unwrap_or(0);
            value >>= 7;

            if value == 0 {
                self.bytes.push(byte);
                return;
            }

            self.bytes.push(byte | 0x80);
        }
    }

    fn count(&mut self, count: usize) {
        self.varint(u64::try_from(count).unwrap_or(u64::MAX));
    }

    fn path(&mut self, path: &Path) {
        #[cfg(unix)]
        let bytes = {
            use std::os::unix::ffi::OsStrExt;
            path.as_os_str().as_bytes()
        };

        #[cfg(not(unix))]
        let bytes = path.to_string_lossy().as_bytes();

        self.varint(bytes.len() as u64);
        self.bytes.extend_from_slice(bytes);
    }

    fn optional_timestamp(&mut self, mtime: Option<Timestamp>) {
        match mtime {
            None => self.u8(0),
            Some(t) => {
                self.u8(1);
                self.i64(t.secs);
                self.u32(t.nanos);
            }
        }
    }

    fn hash(&mut self, hash: &[u8; 32]) {
        self.bytes.extend_from_slice(hash);
    }
}

/// Encode a snapshot's body, without the magic or version header.
pub(crate) fn encode_body(snapshot: &Snapshot) -> Vec<u8> {
    let mut out = Writer::new();

    out.count(snapshot.files.len());
    for file in &snapshot.files {
        out.path(&file.path);
        out.u64(file.len);
        out.u32(file.mode);
        out.optional_timestamp(file.mtime);
    }

    out.count(snapshot.dirs.len());
    for dir in &snapshot.dirs {
        out.path(&dir.path);
        out.u32(dir.mode);
        out.optional_timestamp(dir.mtime);
    }

    out.count(snapshot.chunks.len());
    for chunk in &snapshot.chunks {
        out.hash(chunk);
    }

    out.count(snapshot.file_chunks.len());
    for slice in &snapshot.file_chunks {
        out.varint(u64::from(slice.chunk_index));
        out.varint(u64::from(slice.file_index));
        out.varint(u64::from(slice.chunk_offset));
        out.varint(slice.file_offset);
        out.varint(u64::from(slice.length));
    }

    out.count(snapshot.links.len());
    for link in &snapshot.links {
        out.path(&link.path);
        out.path(&link.target);
        // One byte, and an unknown value is refused on the way back in rather
        // than being mapped onto some other kind of link.
        out.u8(match link.kind {
            LinkKind::Hard => 0,
            LinkKind::Symbolic => 1,
        });
    }

    out.bytes
}

/// Decode a snapshot's body, which must be the whole of `bytes`.
pub(crate) fn decode_body(bytes: &[u8]) -> Result<Snapshot> {
    let mut reader = Reader::new(bytes);

    let files = (0..reader.count()?)
        .map(|_| {
            Ok(File {
                path: reader.path()?,
                len: reader.u64()?,
                mode: reader.u32()?,
                mtime: reader.optional_timestamp()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let dirs = (0..reader.count()?)
        .map(|_| {
            Ok(Dir {
                path: reader.path()?,
                mode: reader.u32()?,
                mtime: reader.optional_timestamp()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let chunks = (0..reader.count()?)
        .map(|_| reader.hash())
        .collect::<Result<Vec<_>>>()?;

    let file_chunks = (0..reader.count()?)
        .map(|_| {
            let chunk_index = u32::try_from(reader.varint()?)
                .map_err(|_| Malformed("a chunk index is too large"))?;
            let file_index = u32::try_from(reader.varint()?)
                .map_err(|_| Malformed("a file index is too large"))?;
            let chunk_offset = u32::try_from(reader.varint()?)
                .map_err(|_| Malformed("a chunk offset is too large"))?;
            let file_offset = reader.varint()?;
            let length = u32::try_from(reader.varint()?)
                .map_err(|_| Malformed("a chunk length is too large"))?;

            Ok(FileChunk {
                chunk_index,
                file_index,
                chunk_offset,
                file_offset,
                length,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let links = (0..reader.count()?)
        .map(|_| {
            let path = reader.path()?;
            let target = reader.path()?;
            let kind = match reader.u8()? {
                0 => LinkKind::Hard,
                1 => LinkKind::Symbolic,
                _ => return Err(Malformed("a link has an unknown kind")),
            };

            Ok(Link { path, target, kind })
        })
        .collect::<Result<Vec<_>>>()?;

    // Anything after the snapshot is damage, not padding: the blob's length is
    // recorded in its key, so a mismatch means the two disagree about what this
    // blob is, which is the sort of thing the verify pass exists to catch.
    if !reader.is_done() {
        return Err(Malformed("the snapshot has trailing bytes"));
    }

    Ok(Snapshot {
        // Set by the caller from the header, which has already checked it.
        version: super::FORMAT_VERSION,
        files,
        dirs,
        chunks,
        file_chunks,
        links,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        let mut snapshot = Snapshot::new();
        snapshot.dirs.push(Dir {
            path: PathBuf::from("a/b"),
            mode: 0o755,
            mtime: Some(Timestamp { secs: -5, nanos: 7 }),
        });
        snapshot.files.push(File {
            path: PathBuf::from("a/b/c.bin"),
            len: 300,
            mode: 0o644,
            mtime: None,
        });
        snapshot.chunks.push([3u8; 32]);
        snapshot.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 0,
            length: 3,
        });
        snapshot.links.push(Link {
            path: PathBuf::from("a/link"),
            target: PathBuf::from("../elsewhere"),
            kind: LinkKind::Symbolic,
        });
        snapshot
    }

    #[test]
    fn a_snapshot_survives_a_round_trip() {
        let snapshot = sample();
        let decoded = decode_body(&encode_body(&snapshot)).expect("decodes");
        assert_eq!(decoded.files, snapshot.files);
        assert_eq!(decoded.dirs, snapshot.dirs);
        assert_eq!(decoded.chunks, snapshot.chunks);
        assert_eq!(decoded.file_chunks, snapshot.file_chunks);
        assert_eq!(decoded.links, snapshot.links);
    }

    #[test]
    fn an_empty_snapshot_survives_a_round_trip() {
        let snapshot = Snapshot::new();
        let decoded = decode_body(&encode_body(&snapshot)).expect("decodes");
        assert!(decoded.files.is_empty());
        assert!(decoded.links.is_empty());
    }

    #[test]
    fn a_truncated_body_is_refused() {
        let bytes = encode_body(&sample());
        for cut in 0..bytes.len() {
            assert!(
                decode_body(&bytes[..cut]).is_err(),
                "a body truncated to {cut} of {} bytes decoded",
                bytes.len()
            );
        }
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = encode_body(&sample());
        bytes.push(0);
        assert!(decode_body(&bytes).is_err(), "trailing bytes were accepted");
    }

    #[test]
    fn an_absurd_count_cannot_allocate() {
        // The first count claims 2^40 files in a body nine bytes long. Nothing
        // should be allocated for it, and nothing should succeed.
        let mut body = Vec::new();
        Writer::new().varint(0);
        body.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);

        let decoded = decode_body(&body);
        assert!(decoded.is_err(), "an impossible count decoded");
    }

    #[test]
    fn an_overlong_varint_is_refused() {
        // Eleven continuation bytes: more than any u64 needs.
        let body = vec![0x80u8; 11];
        assert!(decode_body(&body).is_err());
    }

    #[test]
    fn a_varint_that_overflows_a_u64_is_refused() {
        // The tenth byte carries more than the one bit a u64 has left.
        let mut body = vec![0x80u8; 9];
        body.push(0x02);
        assert!(decode_body(&body).is_err());
    }

    #[test]
    fn an_unknown_link_kind_is_refused() {
        let snapshot = sample();
        let mut body = encode_body(&snapshot);
        // The kind byte is the last one written.
        let last = body.len() - 1;
        body[last] = 9;
        assert!(
            decode_body(&body).is_err(),
            "an unknown link kind was accepted"
        );
    }

    #[test]
    fn a_path_that_is_not_utf8_survives_on_unix() {
        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;

            let mut snapshot = Snapshot::new();
            snapshot.files.push(File {
                path: PathBuf::from(OsString::from_vec(vec![0xff, 0xfe, b'/'])),
                len: 1,
                mode: 0o644,
                mtime: None,
            });

            let decoded = decode_body(&encode_body(&snapshot)).expect("decodes");
            assert_eq!(decoded.files[0].path, snapshot.files[0].path);
        }
    }
}
