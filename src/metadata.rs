//! On-disk description of a backup.
//!
//! A snapshot is serialised into a single blob in the storage backend, named by
//! the BLAKE3 hash of its bytes, and framed with a magic prefix and a format
//! version so a layout change is reported as such rather than as a confusing
//! deserialisation error. The wire format is in the `codec` submodule.

mod codec;

// `Malformed` is the payload of `Error::Decode`, which is a public variant, so
// it is already part of this module's public API whether or not it can be
// named. A caller can write `Error::Decode(_)` and match on the outer error,
// but cannot destructure the inner one, name it in a signature, or compare
// against it — which is the only reason it was `pub` inside a private module in
// the first place. Re-exported so the type and the variant that carries it
// agree about being public.
pub use codec::Malformed;

use std::{fmt, io, path::PathBuf};

use crate::storage;

/// Magic prefix identifying a lepatch snapshot blob.
pub const MAGIC: &[u8; 8] = b"LEPATCH\x00";

/// Snapshot layout version understood by this build.
///
/// Bump this whenever the shape of [`Snapshot`], or the encoding in `codec`,
/// changes in a way that makes previously written blobs undecodable.
///
/// This is 3. Version 2 recorded, per chunk, both its content hash and a storage
/// key naming a byte offset; the store's own index answers "where is this
/// content" now, so a chunk is a bare hash.
pub const FORMAT_VERSION: u32 = 3;

/// Reasons a snapshot blob could not be decoded. No encode variant, because
/// encoding cannot fail: every field is a number, a byte string or a byte array.
#[derive(Debug, Clone, PartialEq, Eq)]
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
                // Saturate rather than wrap: a wrapped negative would be a worse
                // answer than a clamp.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Regular files, in the order they were chunked.
    pub files: Vec<File>,
    /// Directories, including ones that contain no files.
    pub dirs: Vec<Dir>,
    /// Distinct chunk contents, referenced by `FileChunk::chunk_index`. Each
    /// entry is the BLAKE3 hash of the contents, and the store's index maps it.
    pub chunks: Vec<storage::BlobId>,
    /// Placement of each chunk slice within each file.
    pub file_chunks: Vec<FileChunk>,
    /// Symbolic and hard links, which carry no content of their own.
    pub links: Vec<Link>,
}

impl Snapshot {
    /// Assert that every file's chunk slices add up to its recorded length, and
    /// that no slice claims space outside it.
    ///
    /// Both directions of the round trip need this, and for different reasons.
    ///
    /// A **backup** writes it, and a tree can change between being walked and
    /// being read: the chunker stats each file when it registers it and reads it
    /// again later, so a file that grew in between shifts every subsequent chunk
    /// into the wrong one. The lengths recorded during the walk stop matching
    /// what was chunked, and the snapshot would be internally consistent and
    /// wrong.
    ///
    /// A **restore** reads a snapshot that is a blob in a store, and nothing
    /// about it is trustworthy. `File::len` is one number, and if it is longer
    /// than the slices then `set_len` at the end of each file — there to close a
    /// gap *between* slices — zero-fills the tail instead. That does not fail.
    /// The restore reports success and leaves a file of exactly the right size
    /// with a plausible amount of content and a tail of nothing. A check that
    /// exists only on the writing side, where a hostile snapshot is not the
    /// threat, is a check in the wrong place.
    ///
    /// Both the total and the covered extent are required to match. The total
    /// catches slices that are too long, or too short, or that overlap so the
    /// sum overcounts; the extent catches slices that leave a gap at the start,
    /// where the sum can be right and a hole is not.
    ///
    /// The extent is also what catches a slice reaching *past* the end of its
    /// file, so there is deliberately no separate check for that. It looks like
    /// the more direct thing to say, and it has a clearer message — but it fires
    /// first, on the path a growing file takes during a backup, and it replaces
    /// the one that has been telling a user their tree moved under them. Same
    /// refusal, worse message.
    ///
    /// `what` is the clause that goes between the file and the numbers, and it is
    /// the caller's because the two situations read differently. "Changed while
    /// it was being backed up" is a diagnosis; "does not match its own chunks" is
    /// a description, and a restore has no basis for the first. A clause rather
    /// than an enum because neither side branches on it — it is a sentence, and
    /// putting it in the message is the whole of what it is for.
    ///
    /// # Errors
    ///
    /// Returns an error naming the file, its recorded length, how many bytes the
    /// slices total, and how much of the file they cover — all four, because a
    /// mismatch between any two of them is a different mistake.
    pub fn verify_file_lengths(&self, what: &str) -> io::Result<()> {
        let mut totals = vec![0u64; self.files.len()];
        let mut extents: Vec<Option<(u64, u64)>> = vec![None; self.files.len()];

        for slice in &self.file_chunks {
            let index = slice.file_index as usize;
            let file = self.files.get(index).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk references file {index}, which the snapshot does not contain"),
                )
            })?;

            let end = slice
                .file_offset
                .checked_add(slice.length.into())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("chunk range overflows in {}", file.path.display()),
                    )
                })?;

            totals[index] += u64::from(slice.length);

            let extent = extents[index].get_or_insert((slice.file_offset, end));
            extent.0 = extent.0.min(slice.file_offset);
            extent.1 = extent.1.max(end);
        }

        for (index, file) in self.files.iter().enumerate() {
            let covered = extents[index].map_or(0, |(start, end)| end - start);
            let (recorded, chunked) = (file.len, totals[index]);

            if chunked != recorded || covered != recorded {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{path} {what}: recorded {recorded} bytes, chunked {chunked} bytes \
                         covering {covered} bytes",
                        path = file.path.display(),
                    ),
                ));
            }
        }

        Ok(())
    }

    /// An empty snapshot.
    ///
    /// There is no version field to stamp, and that is the point: the layout
    /// version is a property of the encoder, not of any one snapshot. A field
    /// could be set to a value this build then refused to read back, and
    /// nothing would catch it until the blob was decoded.
    #[must_use]
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            dirs: Vec::new(),
            chunks: Vec::new(),
            file_chunks: Vec::new(),
            links: Vec::new(),
        }
    }

    /// Serialise into the framed on-disk representation: [`MAGIC`], the version
    /// as a little-endian `u32`, then the body.
    ///
    /// The version written is [`FORMAT_VERSION`], always. There is nothing for a
    /// caller to choose here: a snapshot is not "in" a version, it is written by
    /// a build that emits one.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let body = codec::encode_body(self);

        let mut out = Vec::with_capacity(MAGIC.len() + 4 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&body);

        out
    }

    /// Parse the framed on-disk representation.
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

/// A snapshot of a tree with nothing in it.
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
    /// Length in bytes at the time the tree was walked.
    ///
    /// Restore creates the file at this length and truncates back to it at the
    /// end, so a gap in the recorded slices is zero-filled rather than detected.
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

/// The placement of one chunk slice within one file: a chunk can be split across
/// several files, and a file is covered by several slices.
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
