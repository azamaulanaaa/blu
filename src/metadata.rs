//! On-disk description of a backup.
//!
//! A snapshot is serialised into a single blob in the storage backend, and the
//! store's commit record names it by the BLAKE3 hash of those bytes. Because the
//! blob is written once and read back arbitrarily far in the future, the
//! encoding is explicitly framed with a magic prefix and a format version so
//! that a layout change is reported as such instead of surfacing as a confusing
//! deserialisation error.
//!
//! The wire format itself is hand-written; see the `codec` submodule for why, and
//! for the layout.

mod codec;

use std::{fmt, path::PathBuf};

use crate::storage;

/// Magic prefix identifying a lepatch snapshot blob.
pub const MAGIC: &[u8; 8] = b"LEPATCH\x00";

/// Snapshot layout version understood by this build.
///
/// Bump this whenever the shape of [`Snapshot`], or the encoding in `codec`,
/// changes in a way that makes previously written blobs undecodable.
///
/// This is 3. Version 2 recorded, for each chunk, both its content hash and a
/// storage key naming a byte offset — the hash to deduplicate by, the offset to
/// read from. The store's own index answers "where is this content" now, so the
/// key is gone and a chunk is a bare hash. A snapshot is smaller for it, and it
/// is the reason a store can be read back without trusting anything outside it.
/// Version 1 was bincode, whose encoding was fixed-width little endian.
pub const FORMAT_VERSION: u32 = 3;

/// Reasons a snapshot blob could not be decoded.
///
/// There is no encode variant: encoding cannot fail. Every field is a number, a
/// byte string or a byte array, so a `Result` that is always `Ok` is a `Result`
/// nobody checks.
#[derive(Debug)]
pub enum Error {
    /// The body is malformed.
    Decode(codec::Malformed),
    /// The blob did not start with [`MAGIC`].
    BadMagic([u8; 8]),
    /// The blob was written by a newer or older layout than this build knows.
    UnsupportedVersion {
        /// The version the blob claims.
        found: u32,
        /// The version this build understands.
        supported: u32,
    },
    /// The blob ended before the header was complete.
    Truncated,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(e) => write!(f, "the snapshot is malformed: {e}"),
            Self::BadMagic(found) => write!(
                f,
                "not a lepatch snapshot: expected magic {MAGIC:?}, found {found:?}"
            ),
            Self::UnsupportedVersion { found, supported } => write!(
                f,
                "unsupported snapshot format version {found}, this build supports {supported}"
            ),
            Self::Truncated => write!(f, "snapshot is truncated"),
        }
    }
}

impl std::error::Error for Error {}

/// A point in time, stored explicitly so that the encoding does not depend on
/// the platform representation of `SystemTime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    /// Seconds since the Unix epoch.
    pub secs: i64,
    /// Sub-second component in nanoseconds.
    pub nanos: u32,
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(time: std::time::SystemTime) -> Self {
        match time.duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => Self {
                // Saturate rather than wrap: a timestamp beyond `i64::MAX`
                // seconds is far outside anything a filesystem reports, and a
                // wrapped negative value would be a worse answer than a clamp.
                secs: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
                nanos: d.subsec_nanos(),
            },
            // Before the epoch, report the distance as a negative offset.
            Err(e) => Self {
                secs: i64::try_from(e.duration().as_secs()).map_or(i64::MIN, |secs| -secs),
                nanos: e.duration().subsec_nanos(),
            },
        }
    }
}

impl From<Timestamp> for std::time::SystemTime {
    fn from(time: Timestamp) -> Self {
        if let Ok(secs) = u64::try_from(time.secs) {
            return std::time::UNIX_EPOCH + std::time::Duration::new(secs, time.nanos);
        }

        // `unsigned_abs` is exact for `i64::MIN`, where negating overflows.
        let before = std::time::Duration::new(time.secs.unsigned_abs(), time.nanos);
        std::time::UNIX_EPOCH - before
    }
}

/// A complete description of one backup: the directory tree, the set of
/// distinct chunk contents, and how the chunks map back onto the files.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Layout version of this snapshot, see [`FORMAT_VERSION`].
    pub version: u32,
    /// Regular files, in the order they were chunked.
    pub files: Vec<File>,
    /// Directories, including ones that contain no files.
    pub dirs: Vec<Dir>,
    /// Distinct chunk contents, referenced by `FileChunk::chunk_index`.
    ///
    /// Each entry is the BLAKE3 hash of a chunk's contents, and that hash is
    /// also how the store finds the bytes. A snapshot used to carry a second
    /// copy of where each chunk lived, as a string of JSON naming an offset and
    /// a length; the store keeps that mapping in its own index now, so a
    /// snapshot is a description of the tree and nothing about the file it
    /// happens to be in.
    pub chunks: Vec<storage::BlobId>,
    /// Placement of each chunk slice within each file.
    pub file_chunks: Vec<FileChunk>,
    /// Symbolic and hard links, which carry no content of their own.
    pub links: Vec<Link>,
}

impl Snapshot {
    /// An empty snapshot stamped with the current format version.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: FORMAT_VERSION,
            files: Vec::new(),
            dirs: Vec::new(),
            chunks: Vec::new(),
            file_chunks: Vec::new(),
            links: Vec::new(),
        }
    }

    /// Serialise into the framed on-disk representation.
    ///
    /// Infallible: every field of a snapshot is a number, a byte string or a
    /// byte array, so there is nothing here that can fail to be represented. That
    /// is also why the error enum has no encode variant -- a `Result` that cannot
    /// fail is a `Result` nobody checks.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let body = codec::encode_body(self);

        let mut out = Vec::with_capacity(MAGIC.len() + 4 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&body);

        out
    }

    /// Parse the framed on-disk representation, reporting a bad magic or an
    /// unsupported version as a distinct, actionable error.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Truncated`] if the header is incomplete,
    /// [`Error::BadMagic`] if the blob is not a snapshot,
    /// [`Error::UnsupportedVersion`] if it was written by an incompatible
    /// layout, and [`Error::Decode`] if the body does not match this version's
    /// shape.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let header = MAGIC.len() + 4;
        if bytes.len() < header {
            return Err(Error::Truncated);
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&bytes[..MAGIC.len()]);
        if &magic != MAGIC {
            return Err(Error::BadMagic(magic));
        }

        let mut version = [0u8; 4];
        version.copy_from_slice(&bytes[MAGIC.len()..header]);
        let version = u32::from_le_bytes(version);

        if version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: FORMAT_VERSION,
            });
        }

        codec::decode_body(&bytes[header..]).map_err(Error::Decode)
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether a [`Link`] shares an inode with another file or points elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkKind {
    /// Shares its contents with [`Link::target`], which is another path in the
    /// same backup.
    Hard,
    /// A symbolic link whose [`Link::target`] is stored verbatim and may point
    /// anywhere, including outside the backup.
    Symbolic,
}

/// A symbolic or hard link, recorded but never chunked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// Path of the link relative to the backup root.
    pub path: PathBuf,
    /// For a symbolic link, the raw link target; for a hard link, the path of
    /// the file within the backup whose contents are shared.
    pub target: PathBuf,
    /// Which kind of link this is.
    pub kind: LinkKind,
}

/// A regular file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    /// Path relative to the backup root.
    pub path: PathBuf,
    /// Length in bytes at the time the tree was walked. Restore asserts that
    /// the reassembled file reaches exactly this size.
    pub len: u64,
    /// Platform permission bits, see [`crate::fsattr::mode_of`].
    pub mode: u32,
    /// Last modification time, if the platform reports one.
    pub mtime: Option<Timestamp>,
}

/// A directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dir {
    /// Path relative to the backup root.
    pub path: PathBuf,
    /// Platform permission bits, see [`crate::fsattr::mode_of`].
    pub mode: u32,
    /// Last modification time, if the platform reports one.
    pub mtime: Option<Timestamp>,
}

/// The placement of one chunk slice within one file.
///
/// A single content chunk can be split across several files, and a single file
/// is covered by several chunk slices, so this is a many-to-many join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChunk {
    /// Index into [`Snapshot::chunks`].
    pub chunk_index: u32,
    /// Index into [`Snapshot::files`].
    pub file_index: u32,
    /// Byte offset of this slice within the chunk.
    pub chunk_offset: u32,
    /// Byte offset of this slice within the file.
    pub file_offset: u64,
    /// Length of this slice in bytes.
    pub length: u32,
}
