//! The snapshot wire format. Hand-written rather than delegated: the encoding of
//! a user's backup set is the one thing that has to keep working, and the
//! reasoning behind that decision is in the README.
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
//! A `varint` is LEB128. Lengths and indices are variable width; byte counts,
//! modes and file lengths are fixed width. An optional timestamp is a `u8` tag
//! (`0` absent, `1` present) then `i64` secs and `u32` nanos, and a link's `u8`
//! kind is `0` hard or `1` symbolic — any other value is refused.
//!
//! # Decoding is the security-relevant half
//!
//! A snapshot is untrusted input: it comes out of a file that gets synced,
//! shared and backed up like anything else, and a tampered one should do nothing
//! but fail. Every length is checked against the bytes remaining *before*
//! anything is allocated for it, so a count can never request more than the blob
//! holds, and trailing bytes after the body are refused.

use std::path::{Path, PathBuf};

use super::{Attributes, Dir, File, FileChunk, Link, Snapshot, Timestamp};

/// Refuse a count that cannot fit in the bytes left: every collection entry costs
/// at least one byte on the wire, so a count above the remaining input is
/// malformed.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The bounds check is before the slice, so nothing is read past the end and
    /// nothing is allocated for a length the input cannot support.
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

    /// An LEB128 unsigned integer: at most ten bytes, and the tenth may contribute
    /// only the last bit of a `u64`.
    ///
    /// A snapshot decoder is the security-relevant half of this module — the
    /// blob it reads is named by the hash of its own bytes, so nothing upstream
    /// vouched for its contents — which is why the tenth-byte rule lives in one
    /// place now rather than in a copy per encoding.
    ///
    /// The bytes are gathered up to the terminator and then handed over, rather
    /// than decoded in the loop: a number is self-delimiting, and reading a
    /// fixed ten would consume whatever follows it.
    fn varint(&mut self) -> Result<u64> {
        let mut window = [0u8; crate::varint::MAX];
        let mut taken = 0usize;

        while taken < window.len() {
            let byte = self.u8()?;
            window[taken] = byte;
            taken += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }

        crate::varint::read(&window[..taken], 0)
            .map(|(value, _)| value)
            .map_err(|_| Malformed("a length in the snapshot is too large"))
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

    /// A path, as a varint byte length then the raw bytes: a path need not be
    /// valid UTF-8, and refusing one would lose a file.
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

    fn hash(&mut self) -> Result<super::storage::BlobId> {
        let bytes = self.take(32)?;
        // The length is checked by `take`, so this cannot fail; a hash of the
        // wrong width is not a thing a snapshot can hold.
        Ok(super::storage::BlobId::from_bytes(
            bytes
                .try_into()
                .map_err(|_| Malformed("a chunk hash is not 32 bytes"))?,
        ))
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

    fn varint(&mut self, value: u64) {
        crate::varint::write(&mut self.bytes, value);
    }

    fn count(&mut self, count: usize) {
        self.varint(u64::try_from(count).unwrap_or(u64::MAX));
    }

    /// The bytes a path is stored as, or `None` if this platform cannot
    /// represent it.
    ///
    /// Two arms, and they are not interchangeable.
    ///
    /// On unix a path is a byte string, so its bytes go in as they are. That is
    /// the whole reason `tests/paths.rs` exists: a name that is not valid UTF-8 is
    /// a legal file name, a tree can contain one, and a backup that refused the
    /// tree over it would abandon the whole run. The decoder reads the same bytes
    /// back, so the round trip is exact.
    ///
    /// Everywhere else a path *is* text, and the only portable way to get at it is
    /// `to_str` — or `to_string_lossy`, which replaces what it cannot represent
    /// with `U+FFFD`. That was the arm in use, and it is worse than it looks: it
    /// does not fail, it silently succeeds with a *different path*. Two names that
    /// differ only in an unrepresentable byte encode to the same bytes, and a
    /// restore writes both at the same place, having recorded something that never
    /// existed on disk.
    ///
    /// So the answer is `None` and the caller refuses. A tree the format cannot
    /// represent is refused loudly at the point of encoding, which is where a
    /// person is told, rather than being backed up into a state that cannot be
    /// restored. `to_string_lossy` stays available on unix, where it is only a
    /// *rendering* choice and nothing is written through it.
    /// One signature for two arms, so the return is an `Option` even where it
    /// cannot be `None`.
    ///
    /// Clippy is right that on unix this never returns `None` and wrong to have
    /// it here: the caller has to be one piece of code for both platforms, and
    /// making the unix arm return a bare `&[u8]` would mean two callers. The
    /// allow is on the function and says so, rather than being a crate-wide
    /// concession.
    #[cfg_attr(unix, allow(clippy::unnecessary_wraps))]
    fn path_bytes(path: &Path) -> Option<&[u8]> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Some(path.as_os_str().as_bytes())
        }

        #[cfg(not(unix))]
        {
            path.to_str().map(str::as_bytes)
        }
    }

    fn path(&mut self, path: &Path) -> Result<()> {
        let bytes = Self::path_bytes(path).ok_or(Malformed(
            "a path is not valid text on this platform, and a lossy encoding \
             would record a name that does not exist",
        ))?;

        self.varint(bytes.len() as u64);
        self.bytes.extend_from_slice(bytes);
        Ok(())
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

    fn hash(&mut self, hash: &super::storage::BlobId) {
        self.bytes.extend_from_slice(hash.as_bytes());
    }
}

/// Encode a snapshot's body, without the magic or version header.
pub(crate) fn encode_body(snapshot: &Snapshot) -> Result<Vec<u8>> {
    let mut out = Writer::new();

    out.count(snapshot.files.len());
    for file in &snapshot.files {
        out.path(&file.path)?;
        out.u64(file.len);
        out.u32(file.attrs.mode.unwrap_or(0));
        out.optional_timestamp(file.attrs.mtime);
    }

    out.count(snapshot.dirs.len());
    for dir in &snapshot.dirs {
        out.path(&dir.path)?;
        out.u32(dir.attrs.mode.unwrap_or(0));
        out.optional_timestamp(dir.attrs.mtime);
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
        // Two paths and a tag, in that order, for both variants — the two cases
        // have always shared a field and the format has never distinguished them
        // beyond the tag, so this writes exactly the bytes it always wrote.
        let (path, second, kind) = match link {
            Link::Symbolic { path, target } => (path, target, 1),
            Link::Hard { path, shares } => (path, shares, 0),
        };
        out.path(path)?;
        out.path(second)?;
        out.u8(kind);
    }

    Ok(out.bytes)
}

/// Decode a snapshot's body, which must be the whole of `bytes`.
pub(crate) fn decode_body(bytes: &[u8]) -> Result<Snapshot> {
    let mut reader = Reader::new(bytes);

    let files = (0..reader.count()?)
        .map(|_| {
            let path = reader.path()?;
            let len = reader.u64()?;
            // `0` on the wire means "no mode recorded", which is what a snapshot
            // taken on a platform without permission bits has always encoded and
            // what a restore has always declined to apply. The bytes are unchanged.
            let mode = reader.u32()?;

            Ok(File {
                path,
                len,
                attrs: Attributes {
                    mode: (mode != 0).then_some(mode),
                    mtime: reader.optional_timestamp()?,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let dirs = (0..reader.count()?)
        .map(|_| {
            let path = reader.path()?;
            let mode = reader.u32()?;

            Ok(Dir {
                path,
                attrs: Attributes {
                    mode: (mode != 0).then_some(mode),
                    mtime: reader.optional_timestamp()?,
                },
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
            let second = reader.path()?;

            match reader.u8()? {
                0 => Ok(Link::Hard {
                    path,
                    shares: second,
                }),
                1 => Ok(Link::Symbolic {
                    path,
                    target: second,
                }),
                _ => Err(Malformed("a link has an unknown kind")),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    // Anything after the snapshot is damage, not padding.
    if !reader.is_done() {
        return Err(Malformed("the snapshot has trailing bytes"));
    }

    Ok(Snapshot {
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
            attrs: Attributes {
                mode: Some(0o755),
                mtime: Some(Timestamp { secs: -5, nanos: 7 }),
            },
        });
        snapshot.files.push(File {
            path: PathBuf::from("a/b/c.bin"),
            len: 300,
            attrs: Attributes {
                mode: Some(0o644),
                mtime: None,
            },
        });
        snapshot
            .chunks
            .push(crate::storage::BlobId::from_bytes([3u8; 32]));
        snapshot.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 0,
            length: 3,
        });
        snapshot.links.push(Link::Symbolic {
            path: PathBuf::from("a/link"),
            target: PathBuf::from("../elsewhere"),
        });
        snapshot
    }

    #[test]
    fn a_snapshot_survives_a_round_trip() {
        let snapshot = sample();
        let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn an_empty_snapshot_survives_a_round_trip() {
        let snapshot = Snapshot::new();
        let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
        assert!(decoded.files.is_empty());
        assert!(decoded.links.is_empty());
    }

    #[test]
    fn a_truncated_body_is_refused() {
        let bytes = encode_body(&sample()).expect("encodes");
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
        let mut bytes = encode_body(&sample()).expect("encodes");
        bytes.push(0);
        assert!(decode_body(&bytes).is_err(), "trailing bytes were accepted");
    }

    #[test]
    fn an_absurd_count_cannot_allocate() {
        // 2^40 files claimed in a nine-byte body.
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
        let mut body = encode_body(&snapshot).expect("encodes");
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
                attrs: Attributes {
                    mode: Some(0o644),
                    mtime: None,
                },
            });

            let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
            assert_eq!(decoded.files[0].path, snapshot.files[0].path);
        }
    }
}
