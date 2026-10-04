//! The snapshot wire format. Hand-written rather than delegated. The encoding of
//! a user's backup set is the one thing that has to keep working. The reasoning
//! behind that decision is in the README.
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
//! A `varint` is LEB128. Lengths and indices are variable width. Byte counts,
//! modes, and file lengths are fixed width. An optional timestamp is a `u8` tag
//! (`0` absent, `1` present). The tag is followed by `i64` secs and `u32` nanos.
//! A link's `u8` kind is `0` hard or `1` symbolic. Any other value is refused.
//!
//! # Decoding is the security-relevant half
//!
//! A snapshot is untrusted input. It comes out of a file that gets synced,
//! shared, and backed up like anything else. A tampered snapshot should do
//! nothing but fail. Every length is checked against the bytes remaining
//! *before* the code allocates anything for it. So a count can never request
//! more than the blob holds. Trailing bytes after the body are refused.

use std::path::{Path, PathBuf};

use super::{Attributes, Dir, File, FileChunk, Link, Snapshot, Timestamp};

/// Refuse a count that cannot fit in the bytes left. Every collection entry
/// costs at least one byte on the wire. So a count above the remaining input is
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
/// Distinct from [`super::Error`]. Every case here means the input is malformed.
/// The message is written for whoever has to work out which blob is damaged.
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
    /// A snapshot decoder is the security-relevant half of this module. The
    /// blob it reads is named by the hash of its own bytes, so nothing upstream
    /// vouched for its contents. So the tenth-byte rule lives in one place now,
    /// rather than in a copy per encoding.
    ///
    /// The bytes are gathered up to the terminator and then handed over. They are
    /// not decoded in the loop. A number is self-delimiting, and reading a fixed
    /// ten would consume whatever follows it.
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

    /// Read an owner written by [`Writer::owner`], or `None` for a layout that had
    /// no such field.
    ///
    /// The `recorded` flag is the snapshot's version, not the reader's ability.
    /// Before an owner was recorded there was nothing on the wire to read. Reading
    /// a `u32` from the mode that follows would put every later field out by four
    /// bytes. The result would be a snapshot that decodes into nonsense.
    fn owner(&mut self, recorded: bool) -> Result<Option<super::Owner>> {
        if !recorded {
            return Ok(None);
        }

        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(super::Owner {
                uid: self.u32()?,
                gid: self.u32()?,
            })),
            _ => Err(Malformed("an owner has an invalid presence tag")),
        }
    }

    fn hash(&mut self) -> Result<super::storage::BlobId> {
        let bytes = self.take(32)?;
        // The length is checked by `take`, so this cannot fail. A
        // hash of the wrong width is not a thing that a snapshot can hold.
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
    /// the whole reason `tests/paths.rs` exists. A name that is not valid UTF-8
    /// is a legal file name, and a tree can contain one. A backup that refused
    /// the tree over it would abandon the whole run. The decoder reads the same
    /// bytes back, so the round trip is exact.
    ///
    /// Everywhere else a path *is* text. The only portable way to get at it is
    /// `to_str`, or `to_string_lossy`. The `to_string_lossy` call replaces what
    /// it cannot represent with `U+FFFD`. That was the arm in use, and it is
    /// worse than it looks. It does not fail. It silently succeeds with a
    /// *different path*. Two names that differ only in an unrepresentable byte
    /// encode to the same bytes. A restore then writes both at the same place.
    /// The restore has recorded something that never existed on disk.
    ///
    /// So the answer is `None` and the caller refuses. A tree the format cannot
    /// represent is refused loudly at the point of encoding, which is where a
    /// person is told. The code does not back up such a tree into a state that
    /// cannot be restored. `to_string_lossy` stays available on unix. There it is
    /// only a *rendering* choice, and nothing is written through it.
    /// One signature for two arms, so the return is an `Option` even where it
    /// cannot be `None`.
    ///
    /// Clippy is right that on unix this never returns `None`. Clippy is
    /// wrong to have it here. The caller has to be one piece of code for both
    /// platforms. Making the unix arm return a bare `&[u8]` would mean two
    /// callers. The allow is on the function and says so, not a crate-wide
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

    /// Write an owner, tagged for presence.
    ///
    /// A tag rather than `0` for absent, which is how a mode is written. `0` is
    /// the root user's id, and it is also the root group's id. A sentinel that is
    /// a real value would make every root-owned file indistinguishable from a
    /// platform that has no users at all. Most of `/etc` and all of `/usr` is
    /// root-owned.
    ///
    /// One tag for the pair, not one each. They come from one `stat` and are
    /// recorded together. A byte each would waste one byte per entry on every
    /// snapshot ever taken.
    fn owner(&mut self, owner: Option<super::Owner>) {
        match owner {
            None => self.u8(0),
            Some(o) => {
                self.u8(1);
                self.u32(o.uid);
                self.u32(o.gid);
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
        out.owner(file.attrs.owner);
    }

    out.count(snapshot.dirs.len());
    for dir in &snapshot.dirs {
        out.path(&dir.path)?;
        out.u32(dir.attrs.mode.unwrap_or(0));
        out.optional_timestamp(dir.attrs.mtime);
        out.owner(dir.attrs.owner);
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
        // Two paths and a tag, in that order, for both variants. The two
        // cases have always shared a field. The format has never distinguished
        // them beyond the tag. So this writes exactly the bytes it always wrote.
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
///
/// `version` is the layout the bytes were written in, not the one this build
/// prefers. The decoder needs it because the owner was added partway through the
/// format's life. Every entry is variable width. A reader that guessed would read
/// the mode of the next file as an owner's user id. The same reader would read
/// every field after it from the wrong place.
pub(crate) fn decode_body(bytes: &[u8], version: u32) -> Result<Snapshot> {
    let mut reader = Reader::new(bytes);
    let with_owner = version >= super::OWNER_SINCE;

    let files = (0..reader.count()?)
        .map(|_| {
            let path = reader.path()?;
            let len = reader.u64()?;
            // `0` on the wire means "no mode recorded". That is what a
            // snapshot taken on a platform without permission bits has always
            // encoded. It is also what a restore has always declined to apply.
            // The bytes are unchanged.
            let mode = reader.u32()?;
            let mtime = reader.optional_timestamp()?;
            let owner = reader.owner(with_owner)?;

            Ok(File {
                path,
                len,
                attrs: Attributes {
                    owner,
                    mode: (mode != 0).then_some(mode),
                    mtime,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let dirs = (0..reader.count()?)
        .map(|_| {
            let path = reader.path()?;
            let mode = reader.u32()?;
            let mtime = reader.optional_timestamp()?;
            let owner = reader.owner(with_owner)?;

            Ok(Dir {
                path,
                attrs: Attributes {
                    owner,
                    mode: (mode != 0).then_some(mode),
                    mtime,
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
                owner: Some(super::super::Owner {
                    uid: 1000,
                    gid: 100,
                }),
                mode: Some(0o755),
                mtime: Some(Timestamp { secs: -5, nanos: 7 }),
            },
        });
        snapshot.files.push(File {
            path: PathBuf::from("a/b/c.bin"),
            len: 300,
            attrs: Attributes {
                owner: None,
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
        let decoded = decode_body(
            &encode_body(&snapshot).expect("encodes"),
            super::super::FORMAT_VERSION,
        )
        .expect("decodes");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn an_empty_snapshot_survives_a_round_trip() {
        let snapshot = Snapshot::new();
        let decoded = decode_body(
            &encode_body(&snapshot).expect("encodes"),
            super::super::FORMAT_VERSION,
        )
        .expect("decodes");
        assert_eq!(decoded.files, []);
        assert_eq!(decoded.links, []);
    }

    #[test]
    fn a_truncated_body_is_refused() {
        let bytes = encode_body(&sample()).expect("encodes");
        for cut in 0..bytes.len() {
            assert!(
                decode_body(&bytes[..cut], super::super::FORMAT_VERSION).is_err(),
                "a body truncated to {cut} of {} bytes decoded",
                bytes.len()
            );
        }
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = encode_body(&sample()).expect("encodes");
        bytes.push(0);
        assert!(
            decode_body(&bytes, super::super::FORMAT_VERSION).is_err(),
            "trailing bytes were accepted"
        );
    }

    #[test]
    fn an_absurd_count_cannot_allocate() {
        // 2^40 files claimed in a nine-byte body.
        let mut body = Vec::new();
        Writer::new().varint(0);
        body.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);

        let decoded = decode_body(&body, super::super::FORMAT_VERSION);
        assert!(decoded.is_err(), "an impossible count decoded");
    }

    #[test]
    fn an_overlong_varint_is_refused() {
        // Eleven continuation bytes: more than any u64 needs.
        let body = vec![0x80u8; 11];
        assert!(decode_body(&body, super::super::FORMAT_VERSION).is_err());
    }

    #[test]
    fn a_varint_that_overflows_a_u64_is_refused() {
        // The tenth byte carries more than the one bit a u64 has left.
        let mut body = vec![0x80u8; 9];
        body.push(0x02);
        assert!(decode_body(&body, super::super::FORMAT_VERSION).is_err());
    }

    #[test]
    fn an_unknown_link_kind_is_refused() {
        let snapshot = sample();
        let mut body = encode_body(&snapshot).expect("encodes");
        // The kind byte is the last one written.
        let last = body.len() - 1;
        body[last] = 9;
        assert!(
            decode_body(&body, super::super::FORMAT_VERSION).is_err(),
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
                    owner: None,
                    mode: Some(0o644),
                    mtime: None,
                },
            });

            let decoded = decode_body(
                &encode_body(&snapshot).expect("encodes"),
                super::super::FORMAT_VERSION,
            )
            .expect("decodes");
            assert_eq!(decoded.files[0].path, snapshot.files[0].path);
        }
    }

    /// Root is uid 0, and root's files are most of a system backup. An encoding
    /// that used `0` for "no owner" would drop the identity of exactly the files
    /// whose identity matters most. The encoding would also do it silently,
    /// decoding cleanly to a platform with no users at all.
    #[test]
    fn root_is_an_owner_and_not_an_absence() {
        let mut snapshot = Snapshot::new();
        snapshot.files.push(File {
            path: PathBuf::from("owned-by-root"),
            len: 1,
            attrs: Attributes {
                owner: Some(super::super::Owner { uid: 0, gid: 0 }),
                mode: Some(0o644),
                mtime: None,
            },
        });
        snapshot.dirs.push(Dir {
            path: PathBuf::from("d"),
            attrs: Attributes {
                owner: None,
                mode: Some(0o755),
                mtime: None,
            },
        });

        let decoded = decode_body(
            &encode_body(&snapshot).expect("encodes"),
            super::super::FORMAT_VERSION,
        )
        .expect("decodes");

        assert_eq!(
            decoded.files[0].attrs.owner,
            Some(super::super::Owner { uid: 0, gid: 0 }),
            "root is an owner, and must survive the round trip as itself"
        );
        assert_eq!(
            decoded.dirs[0].attrs.owner, None,
            "and an entry with no owner is still absent rather than becoming root"
        );
    }

    /// A version-3 snapshot has no owner field at all, and reads back with none.
    ///
    /// Not a compat shim, but an exact reading. A snapshot that recorded nothing
    /// about ownership had none to record. So `None` is the truth, not a gap.
    ///
    /// What this pins is the *reading*. That is the part that can go wrong
    /// silently. The field is not self-describing. A decoder that read one from a
    /// version 3 body would take the next entry's length as a uid. The same
    /// decoder would read everything after it from the wrong offset. That decoder
    /// would still decode, because a wrong uid is a valid uid.
    #[test]
    fn a_version_3_body_reads_with_no_owner() {
        let snapshot = sample();
        let body = encode_body_v3(&snapshot);

        let decoded = decode_body(&body, super::super::MIN_READABLE_VERSION).expect("decodes");

        assert!(
            decoded.files.iter().all(|f| f.attrs.owner.is_none()),
            "a version 3 snapshot recorded no owner, so none should be invented"
        );
        assert!(
            decoded.dirs.iter().all(|d| d.attrs.owner.is_none()),
            "for directories either"
        );
        assert_eq!(
            decoded.files[0].attrs.mode, snapshot.files[0].attrs.mode,
            "and the rest of the entry must still decode to the right values"
        );
        assert_eq!(
            decoded.files[0].len, snapshot.files[0].len,
            "including the length, which is the field a wrong offset would corrupt"
        );
        assert_eq!(
            decoded.dirs[0].attrs.mtime, snapshot.dirs[0].attrs.mtime,
            "and a timestamp, which sits after where the owner now does"
        );
        assert_eq!(
            decoded.files[0].path, snapshot.files[0].path,
            "and the path itself"
        );
        assert_eq!(
            decoded.chunks, snapshot.chunks,
            "and the chunk table, which is further along again"
        );
    }

    /// A version-4 body is nine bytes longer per owned entry. So a decoder that
    /// reads one as version 3 must produce a different answer.
    ///
    /// This test is the negative of the test above. It is what makes that test
    /// meaningful. If reading the wrong version happened to agree, then pinning
    /// the right answer would prove nothing.
    #[test]
    fn reading_a_version_4_body_as_version_3_does_not_agree() {
        let snapshot = sample();
        let modern = encode_body(&snapshot).expect("encodes");
        let older = encode_body_v3(&snapshot);

        assert_ne!(
            modern, older,
            "the two layouts must actually differ for this to be testing anything"
        );

        let wrong = decode_body(&modern, super::super::MIN_READABLE_VERSION);
        assert!(
            wrong.is_err() || wrong.as_ref().expect("decoded") != &snapshot,
            "reading a version 4 body as version 3 produced the same snapshot, which \
             means the version is not being consulted"
        );
    }

    /// Encode a snapshot the way version 3 did, with every field except the owner.
    ///
    /// The code builds this from the same [`Writer`] primitives the real encoder
    /// uses. So the field widths are the encoder's, not something restated here. A
    /// hand-rolled copy is how a test ends up agreeing with a layout change that it
    /// should have caught.
    fn encode_body_v3(snapshot: &Snapshot) -> Vec<u8> {
        let mut out = Writer::new();

        out.count(snapshot.files.len());
        for file in &snapshot.files {
            out.path(&file.path).expect("a path this platform can hold");
            out.u64(file.len);
            out.u32(file.attrs.mode.unwrap_or(0));
            out.optional_timestamp(file.attrs.mtime);
        }

        out.count(snapshot.dirs.len());
        for dir in &snapshot.dirs {
            out.path(&dir.path).expect("a path this platform can hold");
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
            match link {
                Link::Hard { path, shares } => {
                    out.path(path).expect("a path this platform can hold");
                    out.path(shares).expect("a path this platform can hold");
                    out.u8(0);
                }
                Link::Symbolic { path, target } => {
                    out.path(path).expect("a path this platform can hold");
                    out.path(target).expect("a path this platform can hold");
                    out.u8(1);
                }
            }
        }

        out.bytes
    }
}
