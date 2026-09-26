//! On-disk description of a backup.
//!
//! A snapshot is serialised into a single blob in the storage backend and is
//! referenced from an index file by its storage key. Because the blob is
//! written once and read back arbitrarily far in the future, the encoding is
//! explicitly framed with a magic prefix and a format version so that a
//! layout change is reported as such instead of surfacing as a confusing
//! deserialisation error.

use std::{fmt, path::PathBuf};

use serde::{Deserialize, Serialize};

/// Serde helpers for [`PathBuf`].
///
/// `PathBuf`'s own `Serialize` impl round-trips through `str`, so a single
/// path that is not valid UTF-8 fails the entire backup with "path contains
/// invalid UTF-8 characters" — after the storage backend has already been
/// written to. On unix the raw bytes are preserved instead; on other
/// platforms paths are already valid Unicode and travel as a string.
mod path_bytes {
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(path: &Path, serializer: S) -> Result<S::Ok, S::Error> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;

            serializer.serialize_bytes(path.as_os_str().as_bytes())
        }

        #[cfg(not(unix))]
        {
            match path.to_str() {
                Some(path) => serializer.serialize_str(path),
                None => Err(serde::ser::Error::custom(
                    "path is not valid Unicode and cannot be stored on this platform",
                )),
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PathBuf, D::Error> {
        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;

            let bytes = <Vec<u8> as Deserialize>::deserialize(deserializer)?;
            Ok(PathBuf::from(OsString::from_vec(bytes)))
        }

        #[cfg(not(unix))]
        {
            let path = <String as Deserialize>::deserialize(deserializer)?;
            Ok(PathBuf::from(path))
        }
    }
}

/// Magic prefix identifying a lepatch snapshot blob.
pub const MAGIC: &[u8; 8] = b"LEPATCH\x00";

/// Snapshot layout version understood by this build.
///
/// Bump this whenever the shape of [`Snapshot`] changes in a way that makes
/// previously written blobs undecodable.
pub const FORMAT_VERSION: u32 = 1;

/// Reasons a snapshot blob could not be encoded or decoded.
#[derive(Debug)]
pub enum Error {
    /// The blob could not be serialised.
    Encode(bincode::Error),
    /// The blob could not be deserialised.
    Decode(bincode::Error),
    /// The blob did not start with [`MAGIC`].
    BadMagic([u8; 8]),
    /// The blob was written by a newer or older layout than this build knows.
    UnsupportedVersion { found: u32, supported: u32 },
    /// The blob ended before the header was complete.
    Truncated,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "failed to encode snapshot: {e}"),
            Self::Decode(e) => write!(f, "failed to decode snapshot: {e}"),
            Self::BadMagic(found) => write!(
                f,
                "not a lepatch snapshot: expected magic {:?}, found {:?}",
                MAGIC, found
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
                secs: d.as_secs() as i64,
                nanos: d.subsec_nanos(),
            },
            // Before the epoch, report the distance as a negative offset.
            Err(e) => Self {
                secs: -(e.duration().as_secs() as i64),
                nanos: e.duration().subsec_nanos(),
            },
        }
    }
}

impl From<Timestamp> for std::time::SystemTime {
    fn from(time: Timestamp) -> Self {
        if time.secs >= 0 {
            std::time::UNIX_EPOCH + std::time::Duration::new(time.secs as u64, time.nanos)
        } else {
            std::time::UNIX_EPOCH - std::time::Duration::new(time.secs as u64, time.nanos)
        }
    }
}

/// A complete description of one backup: the directory tree, the set of
/// distinct chunk contents, and how the chunks map back onto the files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Layout version of this snapshot, see [`FORMAT_VERSION`].
    pub version: u32,
    /// Regular files, in the order they were chunked.
    pub files: Vec<File>,
    /// Directories, including ones that contain no files.
    pub dirs: Vec<Dir>,
    /// Distinct chunk contents, referenced by `FileChunk::chunk_index`.
    pub chunks: Vec<Chunk>,
    /// Placement of each chunk slice within each file.
    pub file_chunks: Vec<FileChunk>,
    /// Symbolic and hard links, which carry no content of their own.
    pub links: Vec<Link>,
}

impl Snapshot {
    /// An empty snapshot stamped with the current format version.
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
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let body = bincode::serialize(self).map_err(Error::Encode)?;

        let mut out = Vec::with_capacity(MAGIC.len() + 4 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&body);

        Ok(out)
    }

    /// Parse the framed on-disk representation, reporting a bad magic or an
    /// unsupported version as a distinct, actionable error.
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

        bincode::deserialize(&bytes[header..]).map_err(Error::Decode)
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// A distinct chunk of file content, addressed by its content hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// BLAKE3 hash of the chunk contents, used for deduplication and to verify
    /// the chunk on restore.
    pub hash: [u8; 32],
    /// Storage key the chunk contents can be read back from.
    pub location: String,
}

/// Whether a [`Link`] shares an inode with another file or points elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkKind {
    /// Shares its contents with [`Link::target`], which is another path in the
    /// same backup.
    Hard,
    /// A symbolic link whose [`Link::target`] is stored verbatim and may point
    /// anywhere, including outside the backup.
    Symbolic,
}

/// A symbolic or hard link, recorded but never chunked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    /// Path of the link relative to the backup root.
    #[serde(with = "path_bytes")]
    pub path: PathBuf,
    /// For a symbolic link, the raw link target; for a hard link, the path of
    /// the file within the backup whose contents are shared.
    #[serde(with = "path_bytes")]
    pub target: PathBuf,
    /// Which kind of link this is.
    pub kind: LinkKind,
}

/// A regular file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    /// Path relative to the backup root.
    #[serde(with = "path_bytes")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dir {
    /// Path relative to the backup root.
    #[serde(with = "path_bytes")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
