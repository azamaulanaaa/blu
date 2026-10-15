// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The snapshot wire format. Hand-written rather than delegated. The encoding of
//! a user's backup set is the one thing that has to keep working. The reasoning
//! behind that decision is in the README.
//!
//! ```text
//! magic     8 bytes,  b"BLU\0\0\0\0\0"
//! version   u32, little endian
//! body:
//!   i64      UTC seconds of the snapshot stamp
//!   u32      nanos past the second
//!   varint   count of files
//!     per file:   path, u64 len, u32 mode, optional timestamp, owner,
//!                 xattr count, per xattr, Windows access list
//!   varint   count of directories
//!     per dir:    path, u32 mode, optional timestamp, owner,
//!                 xattr count, per xattr, Windows access list
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
//! An xattr entry is a varint name length plus name bytes plus a varint value
//! length plus value bytes. A Windows access list is a `u8` tag (`0` absent,
//! `1` present) plus a varint entry count plus one record per entry. Each
//! entry holds a varint SID length plus SID bytes plus a `u8` type plus a `u8`
//! flags plus a `u32` mask.
//!
//! # Decoding is the security-relevant half
//!
//! A snapshot is untrusted input. It comes out of a file that gets synced,
//! shared, and backed up like anything else. A tampered snapshot should do
//! nothing but fail. Every length is checked against the bytes remaining
//! *before* the code allocates anything for it. So a count can never request
//! more than the blob holds. Trailing bytes after the body are refused.

use std::path::{Path, PathBuf};

use super::{Attributes, Dir, File, FileChunk, Link, Snapshot, Timestamp, WindowsAcl};

/// Longest accepted xattr name in bytes.
///
/// Linux limits names to 255 bytes including the namespace prefix. The decoder
/// accepts up to 1024 bytes. Real names never reach that bound. A hostile blob
/// claiming more is malformed.
const MAX_XATTR_NAME: usize = 1024;

/// Largest accepted xattr value in bytes.
///
/// Linux caps a value near 64 KiB. The decoder accepts up to 128 KiB. Real
/// values never reach that bound. A hostile blob claiming more is malformed.
const MAX_XATTR_VALUE: usize = 128 * 1024;

/// Longest accepted Windows SID string in bytes.
///
/// A SID string holds `S-` plus decimal fields. Real strings stay under 200
/// bytes. A hostile blob claiming more is malformed.
const MAX_SID: usize = 512;

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

    /// The snapshot stamp, as fixed i64 seconds and u32 nanos.
    fn timestamp(&mut self) -> Result<Timestamp> {
        Ok(Timestamp {
            secs: self.i64()?,
            nanos: self.u32()?,
        })
    }

    /// Read an owner written by [`Writer::owner`].
    ///
    /// Every snapshot records an owner tag per entry.
    /// A missing tag is malformed input rather than an old layout.
    fn owner(&mut self) -> Result<Option<super::Owner>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(super::Owner {
                uid: self.u32()?,
                gid: self.u32()?,
            })),
            _ => Err(Malformed("an owner has an invalid presence tag")),
        }
    }

    /// Read extended attributes written by [`Writer::xattrs`].
    ///
    /// The count is checked against the remaining input before use. Each name
    /// and each value length is checked before allocation. An overlong name or
    /// value is malformed input.
    fn xattrs(&mut self) -> Result<Vec<super::XAttr>> {
        let count = self.count()?;
        let mut out = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            let name_len = usize::try_from(self.varint()?)
                .map_err(|_| Malformed("an xattr name length is too large"))?;
            if name_len == 0 || name_len > MAX_XATTR_NAME {
                return Err(Malformed("an xattr name has a bad length"));
            }
            let name = self.take(name_len)?.to_vec();
            if name.contains(&0) {
                return Err(Malformed("an xattr name holds a zero byte"));
            }
            let value_len = usize::try_from(self.varint()?)
                .map_err(|_| Malformed("an xattr value length is too large"))?;
            if value_len > MAX_XATTR_VALUE {
                return Err(Malformed("an xattr value is too large"));
            }
            let value = self.take(value_len)?.to_vec();
            out.push(super::XAttr { name, value });
        }
        Ok(out)
    }

    /// Read a Windows access list written by [`Writer::windows_acl`].
    ///
    /// A `0` tag means no list. A `1` tag starts one. Any other tag is
    /// malformed input. Each SID length is checked before allocation.
    fn windows_acl(&mut self) -> Result<Option<WindowsAcl>> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let count = self.count()?;
                let mut aces = Vec::with_capacity(count.min(64));
                for _ in 0..count {
                    let sid_len = usize::try_from(self.varint()?)
                        .map_err(|_| Malformed("a SID length is too large"))?;
                    if sid_len == 0 || sid_len > MAX_SID {
                        return Err(Malformed("a SID has a bad length"));
                    }
                    let sid_bytes = self.take(sid_len)?;
                    let sid = std::str::from_utf8(sid_bytes)
                        .map_err(|_| Malformed("a SID is not valid text"))?;
                    let ace_type = self.u8()?;
                    if ace_type > 1 {
                        return Err(Malformed("an access entry has a bad type"));
                    }
                    let flags = self.u8()?;
                    let mask = self.u32()?;
                    aces.push(super::WindowsAce {
                        sid: sid.to_string(),
                        ace_type,
                        flags,
                        mask,
                    });
                }
                Ok(Some(WindowsAcl { aces }))
            }
            _ => Err(Malformed("an access list has an invalid tag")),
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

    /// Record the snapshot stamp as fixed seconds and nanos.
    ///
    /// Fixed width rather than tagged. Every snapshot carries a stamp.
    fn timestamp(&mut self, taken_at: Timestamp) {
        self.i64(taken_at.secs);
        self.u32(taken_at.nanos);
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

    /// Write extended attributes as a count plus name and value pairs.
    ///
    /// Names and values ride as varint lengths plus raw bytes. Empty lists
    /// cost one zero byte. The decoder checks every length before use.
    fn xattrs(&mut self, xattrs: &[super::XAttr]) {
        self.count(xattrs.len());
        for attr in xattrs {
            self.varint(attr.name.len() as u64);
            self.bytes.extend_from_slice(&attr.name);
            self.varint(attr.value.len() as u64);
            self.bytes.extend_from_slice(&attr.value);
        }
    }

    /// Write a Windows access list with a presence tag.
    ///
    /// Absent lists cost one zero byte. Present lists carry one record per
    /// entry. The decoder checks every length before use.
    fn windows_acl(&mut self, acl: Option<&super::WindowsAcl>) {
        match acl {
            None => self.u8(0),
            Some(list) => {
                self.u8(1);
                self.count(list.aces.len());
                for ace in &list.aces {
                    self.varint(ace.sid.len() as u64);
                    self.bytes.extend_from_slice(ace.sid.as_bytes());
                    self.u8(ace.ace_type);
                    self.u8(ace.flags);
                    self.u32(ace.mask);
                }
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

    out.timestamp(snapshot.taken_at);

    out.count(snapshot.files.len());
    for file in &snapshot.files {
        out.path(&file.path)?;
        out.u64(file.len);
        out.u32(file.attrs.mode.unwrap_or(0));
        out.optional_timestamp(file.attrs.mtime);
        out.owner(file.attrs.owner);
        out.xattrs(&file.attrs.xattrs);
        out.windows_acl(file.attrs.windows_acl.as_ref());
    }

    out.count(snapshot.dirs.len());
    for dir in &snapshot.dirs {
        out.path(&dir.path)?;
        out.u32(dir.attrs.mode.unwrap_or(0));
        out.optional_timestamp(dir.attrs.mtime);
        out.owner(dir.attrs.owner);
        out.xattrs(&dir.attrs.xattrs);
        out.windows_acl(dir.attrs.windows_acl.as_ref());
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
/// One layout only. The stamp leads the body at a fixed width.
/// Every entry carries an owner tag at a fixed position.
pub(crate) fn decode_body(bytes: &[u8]) -> Result<Snapshot> {
    let mut reader = Reader::new(bytes);
    let taken_at = reader.timestamp()?;

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
            let owner = reader.owner()?;
            let xattrs = reader.xattrs()?;
            let windows_acl = reader.windows_acl()?;

            Ok(File {
                path,
                len,
                attrs: Attributes {
                    owner,
                    mode: (mode != 0).then_some(mode),
                    mtime,
                    xattrs,
                    windows_acl,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let dirs = (0..reader.count()?)
        .map(|_| {
            let path = reader.path()?;
            let mode = reader.u32()?;
            let mtime = reader.optional_timestamp()?;
            let owner = reader.owner()?;
            let xattrs = reader.xattrs()?;
            let windows_acl = reader.windows_acl()?;

            Ok(Dir {
                path,
                attrs: Attributes {
                    owner,
                    mode: (mode != 0).then_some(mode),
                    mtime,
                    xattrs,
                    windows_acl,
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
        taken_at,
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

    /// Fixed stamp for deterministic tests. Real runs stamp the run time.
    fn stamped() -> Timestamp {
        Timestamp {
            secs: 1_700_000_000,
            nanos: 123_456_789,
        }
    }

    fn sample() -> Snapshot {
        let mut snapshot = Snapshot::new(stamped());
        snapshot.dirs.push(Dir {
            path: PathBuf::from("a/b"),
            attrs: Attributes {
                owner: Some(super::super::Owner {
                    uid: 1000,
                    gid: 100,
                }),
                mode: Some(0o755),
                mtime: Some(Timestamp { secs: -5, nanos: 7 }),
                xattrs: Vec::new(),
                windows_acl: None,
            },
        });
        snapshot.files.push(File {
            path: PathBuf::from("a/b/c.bin"),
            len: 300,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: None,
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
        let snapshot = Snapshot::new(stamped());
        let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
        assert_eq!(decoded.files, []);
        assert_eq!(decoded.links, []);
        assert_eq!(decoded.taken_at, stamped());
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
    fn a_body_with_trailing_bytes_is_refused() {
        let mut bytes = encode_body(&sample()).expect("encodes");
        bytes.push(0);
        assert!(decode_body(&bytes).is_err(), "trailing bytes were accepted");
    }

    #[test]
    fn an_absurd_count_cannot_allocate() {
        // Twelve stamp bytes lead every body, then 2^40 files in five bytes.
        let mut body = Vec::with_capacity(12 + 5);
        body.extend_from_slice(&stamped().secs.to_le_bytes());
        body.extend_from_slice(&stamped().nanos.to_le_bytes());
        body.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);

        let decoded = decode_body(&body);
        assert!(decoded.is_err(), "an impossible count decoded");
    }

    #[test]
    fn an_overlong_varint_is_refused() {
        // Eleven continuation bytes: more than any u64 needs.
        let mut body = Vec::with_capacity(12 + 11);
        body.extend_from_slice(&stamped().secs.to_le_bytes());
        body.extend_from_slice(&stamped().nanos.to_le_bytes());
        body.extend_from_slice(&[0x80u8; 11]);
        assert!(decode_body(&body).is_err());
    }

    #[test]
    fn a_varint_that_overflows_a_u64_is_refused() {
        // The tenth byte carries more than the one bit a u64 has left.
        let mut count = vec![0x80u8; 9];
        count.push(0x02);
        let mut body = Vec::with_capacity(12 + count.len());
        body.extend_from_slice(&stamped().secs.to_le_bytes());
        body.extend_from_slice(&stamped().nanos.to_le_bytes());
        body.extend_from_slice(&count);
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

            let mut snapshot = Snapshot::new(stamped());
            snapshot.files.push(File {
                path: PathBuf::from(OsString::from_vec(vec![0xff, 0xfe, b'/'])),
                len: 1,
                attrs: Attributes {
                    owner: None,
                    mode: Some(0o644),
                    mtime: None,
                    xattrs: Vec::new(),
                    windows_acl: None,
                },
            });

            let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
            assert_eq!(decoded.files[0].path, snapshot.files[0].path);
        }
    }

    /// Root is uid 0, and root's files are most of a system backup. An encoding
    /// that used `0` for "no owner" would drop the identity of exactly the files
    /// whose identity matters most. The encoding would also do it silently,
    /// decoding cleanly to a platform with no users at all.
    #[test]
    fn root_is_an_owner_and_not_an_absence() {
        let mut snapshot = Snapshot::new(stamped());
        snapshot.files.push(File {
            path: PathBuf::from("owned-by-root"),
            len: 1,
            attrs: Attributes {
                owner: Some(super::super::Owner { uid: 0, gid: 0 }),
                mode: Some(0o644),
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: None,
            },
        });
        snapshot.dirs.push(Dir {
            path: PathBuf::from("d"),
            attrs: Attributes {
                owner: None,
                mode: Some(0o755),
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: None,
            },
        });

        let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");

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

    /// The stamp leads the body at a fixed width.
    ///
    /// The first twelve bytes hold seconds and nanos in little endian.
    /// A reader that skips them reads the file count as a time.
    #[test]
    fn a_snapshot_records_taken_at_first_in_the_body() {
        let bytes = encode_body(&sample()).expect("encodes");

        assert_eq!(
            i64::from_le_bytes(bytes[..8].try_into().expect("eight stamp bytes")),
            stamped().secs
        );
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().expect("four stamp bytes")),
            stamped().nanos
        );
    }

    /// UTC text names the same instant the stamp holds.
    ///
    /// The text round-trips through the parser for times on both sides
    /// of the epoch, with and without nanos.
    #[test]
    fn a_stamp_round_trips_through_utc_text() {
        for taken_at in [
            Timestamp { secs: 0, nanos: 0 },
            Timestamp {
                secs: 1_700_000_000,
                nanos: 123_456_789,
            },
            Timestamp { secs: -5, nanos: 7 },
            Timestamp {
                secs: 1_700_000_000,
                nanos: 0,
            },
        ] {
            let text = taken_at.to_utc_string();
            let parsed = Timestamp::parse_utc(&text).expect("parses");

            assert_eq!(parsed, taken_at, "{text:?} did not parse back");
        }
    }

    /// UTC text has the RFC 3339 shape with a Z suffix.
    ///
    /// The shape is pinned so the CLI and the server print one form.
    #[test]
    fn utc_text_has_the_rfc_3339_shape() {
        assert_eq!(
            Timestamp { secs: 0, nanos: 0 }.to_utc_string(),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            Timestamp {
                secs: 1_700_000_000,
                nanos: 500_000_000,
            }
            .to_utc_string(),
            "2023-11-14T22:13:20.5Z"
        );
    }

    /// Local text names the same instant as UTC text.
    ///
    /// The zone changes the spelling, never the instant.
    /// Parsing the local form returns the same stamp.
    /// The form carries an offset and no zone name.
    #[test]
    fn local_text_names_the_same_instant() {
        let taken_at = stamped();
        let text = taken_at.to_local_string();

        assert!(
            !text.contains('['),
            "local text should carry an offset, not a zone name: {text}"
        );

        let parsed = Timestamp::parse_utc(&text).expect("parses");

        assert_eq!(parsed, taken_at);
    }

    /// Text that is not RFC 3339 time text is refused.
    #[test]
    fn non_time_text_is_refused() {
        for text in ["", "yesterday", "2023-13-45T99:99:99Z", "1700000000"] {
            assert!(
                Timestamp::parse_utc(text).is_err(),
                "{text:?} parsed as a stamp"
            );
        }
    }

    /// An older stamp is refused against a newer latest stamp.
    ///
    /// The refusal names both stamps in UTC.
    #[test]
    fn an_older_stamp_is_refused() {
        let latest = Timestamp {
            secs: 1_700_000_000,
            nanos: 0,
        };
        let next = Timestamp {
            secs: 1_699_999_999,
            nanos: 999_999_999,
        };

        let error =
            super::super::refuse_older_taken_at(Some(latest), next).expect_err("older decoded");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains(&next.to_utc_string()));
        assert!(error.to_string().contains(&latest.to_utc_string()));
    }

    /// An equal stamp passes, and a first version passes with no latest.
    #[test]
    fn an_equal_stamp_and_a_first_version_pass() {
        let taken_at = stamped();

        assert!(super::super::refuse_older_taken_at(Some(taken_at), taken_at).is_ok());
        assert!(super::super::refuse_older_taken_at(None, taken_at).is_ok());
        assert!(
            super::super::refuse_older_taken_at(
                Some(taken_at),
                Timestamp {
                    secs: taken_at.secs,
                    nanos: taken_at.nanos + 1
                }
            )
            .is_ok()
        );
    }

    /// Extended attributes survive the round trip.
    ///
    /// The test records user attributes and access lists. Both must decode to
    /// the same bytes. Order is significant. The encoder keeps walk order.
    #[test]
    fn extended_attributes_survive_a_round_trip() {
        let mut snapshot = Snapshot::new(stamped());
        snapshot.files.push(File {
            path: PathBuf::from("with-xattr"),
            len: 1,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
                xattrs: vec![
                    super::super::XAttr {
                        name: b"user.comment".to_vec(),
                        value: b"hello".to_vec(),
                    },
                    super::super::XAttr {
                        name: b"system.posix_acl_access".to_vec(),
                        value: vec![1, 2, 3, 4],
                    },
                ],
                windows_acl: None,
            },
        });

        let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
        assert_eq!(decoded, snapshot);
    }

    /// A Windows access list survives the round trip.
    ///
    /// The test records two entries. Both must decode to the same values.
    /// Order is significant. The encoder keeps capture order.
    #[test]
    fn a_windows_access_list_survives_a_round_trip() {
        let mut snapshot = Snapshot::new(stamped());
        snapshot.dirs.push(Dir {
            path: PathBuf::from("d"),
            attrs: Attributes {
                owner: None,
                mode: None,
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: Some(super::super::WindowsAcl {
                    aces: vec![
                        super::super::WindowsAce {
                            sid: "S-1-5-32-544".to_string(),
                            ace_type: 0,
                            flags: 3,
                            mask: 0x001f_01ff,
                        },
                        super::super::WindowsAce {
                            sid: "S-1-1-0".to_string(),
                            ace_type: 1,
                            flags: 0,
                            mask: 0x0010_0000,
                        },
                    ],
                }),
            },
        });

        let decoded = decode_body(&encode_body(&snapshot).expect("encodes")).expect("decodes");
        assert_eq!(decoded, snapshot);
    }

    /// An xattr name with a zero byte is refused.
    ///
    /// Names never hold zero bytes on Linux. The encoder writes raw bytes.
    /// The decoder checks each name before use.
    #[test]
    fn an_xattr_name_with_a_zero_byte_is_refused() {
        let mut snapshot = Snapshot::new(stamped());
        snapshot.files.push(File {
            path: PathBuf::from("bad"),
            len: 1,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
                xattrs: vec![super::super::XAttr {
                    name: b"user.\0bad".to_vec(),
                    value: Vec::new(),
                }],
                windows_acl: None,
            },
        });

        let bytes = encode_body(&snapshot).expect("encodes");
        assert!(decode_body(&bytes).is_err());
    }

    /// A Windows entry with a bad type is refused.
    ///
    /// Only allow and deny entries exist in this format. Any other type value
    /// is malformed input.
    #[test]
    fn a_windows_entry_with_a_bad_type_is_refused() {
        let mut snapshot = Snapshot::new(stamped());
        snapshot.files.push(File {
            path: PathBuf::from("bad"),
            len: 1,
            attrs: Attributes {
                owner: None,
                mode: None,
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: Some(super::super::WindowsAcl {
                    aces: vec![super::super::WindowsAce {
                        sid: "S-1-5-32-544".to_string(),
                        ace_type: 9,
                        flags: 0,
                        mask: 0,
                    }],
                }),
            },
        });

        let bytes = encode_body(&snapshot).expect("encodes");
        assert!(decode_body(&bytes).is_err());
    }
}
