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

/// A complete description of one backup: the directory tree, the set of
/// distinct chunk contents, and how the chunks map back onto the files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Layout version of this snapshot, see [`FORMAT_VERSION`].
    pub version: u32,
    /// Regular files, in the order they were chunked.
    pub files: Vec<File>,
    /// Distinct chunk contents, referenced by `FileChunk::chunk_index`.
    pub chunks: Vec<Chunk>,
    /// Placement of each chunk slice within each file.
    pub file_chunks: Vec<FileChunk>,
    /// Symbolic and hard links.
    pub file_symlink: Vec<FileSymlink>,
}

impl Snapshot {
    /// An empty snapshot stamped with the current format version.
    pub fn new() -> Self {
        Self {
            version: FORMAT_VERSION,
            files: Vec::new(),
            chunks: Vec::new(),
            file_chunks: Vec::new(),
            file_symlink: Vec::new(),
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

/// A symbolic or hard link, recorded but not chunked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSymlink {
    /// Path of the link relative to the backup root.
    pub path: PathBuf,
    /// For a symbolic link, the raw link target; for a hard link, the path of
    /// the file whose contents are shared.
    pub source: PathBuf,
    /// Whether this is a hard link rather than a symbolic one.
    pub is_hard: bool,
}

/// A regular file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    /// Path relative to the backup root.
    pub path: PathBuf,
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
