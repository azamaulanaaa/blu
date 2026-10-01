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

/// Snapshot layout version written by this build.
///
/// Bump this whenever the shape of [`Snapshot`], or the encoding in `codec`,
/// changes in a way that makes previously written blobs undecodable.
///
/// This is 4. Version 3 recorded a permission mode and a modification time per
/// entry; version 4 adds the owning user and group, so every entry is nine bytes
/// larger on a platform that has them. Version 2 recorded, per chunk, both its
/// content hash and a storage key naming a byte offset; the store's own index
/// answers "where is this content" now, so a chunk is a bare hash.
pub const FORMAT_VERSION: u32 = 4;

/// The first layout version that recorded an owner on every file and directory.
///
/// A decoder branches on this rather than on a try, because the field's width is
/// not self-describing: reading an entry laid out for version 3 with a version-4
/// reader would take the next entry's length as a uid and every field after it from
/// the wrong offset, producing a snapshot that decodes cleanly into nonsense. A
/// version 3 snapshot simply has no owner to read, which is a fact about it rather
/// than a gap in what this build knows.
pub const OWNER_SINCE: u32 = 4;

/// The oldest layout version this build can still read.
///
/// Version 3 and later. Refusing the ones below is not leniency — they are layouts
/// this code cannot decode, and a decoder that guessed at them would read a field
/// from the wrong place rather than fail.
pub const MIN_READABLE_VERSION: u32 = 3;

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
    ///
    /// # Errors
    ///
    /// Returns an error only where a path in this snapshot cannot be represented
    /// on this platform. On unix that cannot happen — a path is a byte string, so
    /// its bytes are written as they are and a name that is not valid UTF-8
    /// round-trips exactly. Everywhere else a path is text, and a name the
    /// platform's encoding cannot hold is **refused** rather than written with
    /// replacement characters, which would record a path that does not exist and
    /// collide two real ones. A tree the format cannot represent is refused here,
    /// at the point where a person can be told, rather than being backed up into
    /// a set that cannot be restored.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let body = codec::encode_body(self).map_err(Error::Decode)?;

        let mut out = Vec::with_capacity(MAGIC.len() + 4 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&body);

        Ok(out)
    }

    fn parse(bytes: &[u8]) -> Result<Self, Error> {
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

        // A range rather than an equality, because a version this build can read is
        // not a version it would have written. Version 3 is readable and predates the
        // owner field, which is exactly the case worth reading: a set taken before
        // this build existed is still a set, and refusing it would cost a user every
        // backup they had rather than costing them their ownership.
        if !(MIN_READABLE_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: FORMAT_VERSION,
            });
        }

        codec::decode_body(&bytes[header..], version).map_err(Error::Decode)
    }
}

impl TryFrom<&[u8]> for Snapshot {
    type Error = Error;

    /// Parse the framed on-disk representation.
    ///
    /// `TryFrom` rather than an inherent `from_bytes`, so that "these bytes are a
    /// snapshot" is a conversion like any other, and the same vocabulary as
    /// `VersionIndex`, which parses a different wire format into a different type
    /// and now says so the same way. One implementation, not an inherent method
    /// beside a trait impl.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Truncated`] if the header is incomplete,
    /// [`Error::BadMagic`] if the blob is not a snapshot,
    /// [`Error::UnsupportedVersion`] if it was written by an incompatible layout,
    /// and [`Error::Decode`] if the body does not match this version's shape.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::parse(bytes)
    }
}

/// A snapshot of a tree with nothing in it.
impl Default for Snapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// A symbolic or hard link, recorded but never chunked.
///
/// A sum type rather than a struct with a `kind`, because the two cases give the
/// same field two different meanings and subject it to two different rules.
///
/// `target` used to be "the raw link target" for a symbolic link and "the path of
/// the file within the backup whose contents are shared" for a hard link. A
/// restore does not treat them alike: a hard link's is resolved against the
/// restore root and must land inside it, while a symbolic link's is used verbatim
/// and is allowed to point anywhere including outside the backup. Two rules for
/// one field, and nothing in the type said which applied — the only signal was
/// the `kind` sitting next to it, and a field access reads as though it were just
/// a field.
///
/// So there are two variants and each names its own second path. `target` means
/// a link target and exists only on [`Link::Symbolic`]; a hard link has `shares`,
/// which is what it does rather than where it points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    /// Points elsewhere. The target is stored verbatim and may name anything,
    /// including a path outside the backup, because a symbolic link is only ever
    /// a string until something follows it.
    Symbolic {
        /// Path of the link relative to the backup root.
        path: PathBuf,
        /// Where it points, as written. Not resolved against the backup root and
        /// not required to be inside it.
        target: PathBuf,
    },
    /// Shares an inode with another file in the same backup, so there is no
    /// content of its own to store.
    Hard {
        /// Path of the link relative to the backup root.
        path: PathBuf,
        /// Path, within the backup, of the file whose contents this shares.
        ///
        /// Resolved against the restore root and required to land inside it, which
        /// is the whole difference from [`Link::Symbolic::target`]: a hard link
        /// is a claim about this tree, so a claim that names somewhere else is a
        /// damaged snapshot rather than a link.
        shares: PathBuf,
    },
}
/// Everything this platform records about one entry, and a restore reapplies.
///
/// All of it is the same kind of thing: something the platform may or may not have,
/// recorded if it does and reapplied if it is there. Each is an `Option`, and "this
/// platform has no permission bits" and "the permissions are the empty set" are
/// different facts a sentinel like `0` could not tell apart — a restore acts on them
/// differently, one is a no-op and the other is a set of permissions nobody has.
///
/// `owner` is the user and group as one value, because they are one identity and come
/// out of one `stat`. It is absent on a platform with no users, and absent in every
/// snapshot taken before this field existed — which is a fact rather than a guess, as
/// a snapshot that recorded nothing about ownership had none to record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Attributes {
    /// User and group that own the entry, or `None` where the platform has neither.
    pub owner: Option<Owner>,
    /// Permission bits, or `None` where the platform has none.
    pub mode: Option<u32>,
    /// Last modification time, or `None` where the platform reports none.
    pub mtime: Option<Timestamp>,
}

/// A user and a group, which are one identity rather than two numbers.
///
/// Ids and not names. A name means nothing on any machine but the one it was read
/// from — uid 1000 is one person here and another there — so a name would be a
/// portability bug stored alongside the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Owner {
    /// The owning user id.
    pub uid: u32,
    /// The owning group id.
    pub gid: u32,
}

impl Owner {
    /// The owner of what `meta` describes, or `None` where the platform has none.
    #[must_use]
    pub fn of(meta: &std::fs::Metadata) -> Option<Self> {
        crate::fsattr::owner_of(meta).map(|(uid, gid)| Self { uid, gid })
    }
}

impl Attributes {
    /// Capture whatever this platform records about `meta`.
    ///
    /// One call rather than three, because the interesting case is missing one: a
    /// backup that captured the mode and the time and forgot the owner would produce
    /// snapshots that look complete and restore a tree nobody can read.
    #[must_use]
    pub fn of(meta: &std::fs::Metadata) -> Self {
        Self {
            owner: Owner::of(meta),
            mode: crate::fsattr::mode_of(meta),
            mtime: crate::fsattr::mtime_of(meta),
        }
    }

    /// Whether this entry records an owner, and so has one to be set or not set.
    #[must_use]
    pub fn has_owner(&self) -> bool {
        self.owner.is_some()
    }

    /// Reapply all of them to `path`, in the order that survives.
    ///
    /// **Owner first, and that is not a style preference.** Linux clears a file's
    /// set-user-ID and set-group-ID bits when its owner changes, so setting the mode
    /// first and the owner second silently strips setuid from everything restored. It
    /// is silent in the worst way: the file is written, its bytes match, and the only
    /// difference is a bit nobody was looking for.
    ///
    /// Modification time is last for a different reason: creating a link inside a
    /// directory updates that directory's time, so a time set before the links would
    /// be overwritten by them.
    ///
    /// An owner that cannot be set is an error here. [`Self::apply_restoring`] is the
    /// other policy, for a caller that would rather report it than stop.
    ///
    /// # Errors
    ///
    /// Returns an error if the path cannot be found, or any attribute cannot be
    /// applied — for instance because the caller does not own the file, or cannot
    /// change its owner.
    pub fn apply(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.apply_inner(path, true).map(|_| ())
    }

    /// Reapply all of them, treating an owner that cannot be set as a fact to report
    /// rather than a failure.
    ///
    /// Returns whether the owner took, or `true` where there was none to set.
    ///
    /// Changing an owner takes privilege, and for an unprivileged caller that is the
    /// ordinary case rather than an exceptional one: restoring a tree containing a
    /// file you do not own is a normal thing to want. Stopping there would be the
    /// wrong answer twice over — the tree is left half-written, and the file that
    /// failed already exists with the wrong owner, so there is nothing to retry and
    /// no record that it is wrong.
    ///
    /// Everything else is still applied. A file with the right contents and mode but
    /// the wrong owner is a tree that is nearly right, which is a much better thing to
    /// hand back than an aborted run and a partial directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the path cannot be found, or if the mode or modification
    /// time cannot be applied. Only the owner is allowed to fail quietly.
    pub fn apply_restoring(&self, path: &std::path::Path) -> std::io::Result<bool> {
        self.apply_inner(path, false)
    }

    /// The one place the order is decided, whichever policy is in force.
    fn apply_inner(&self, path: &std::path::Path, owner_is_fatal: bool) -> std::io::Result<bool> {
        let mut owner_applied = true;

        if let Some(owner) = self.owner {
            match crate::fsattr::set_owner(path, Some((owner.uid, owner.gid))) {
                Ok(()) => {}
                Err(e) if !owner_is_fatal => {
                    tracing::debug!(
                        path = %path.display(),
                        error = %e,
                        "could not set an owner; leaving the one this process has"
                    );
                    owner_applied = false;
                }
                Err(e) => return Err(e),
            }
        }

        crate::fsattr::set_mode(path, self.mode)?;
        if let Some(mtime) = self.mtime {
            crate::fsattr::set_mtime(path, mtime)?;
        }

        Ok(owner_applied)
    }
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
    /// The permissions and modification time to restore.
    pub attrs: Attributes,
}

/// A directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dir {
    /// Path relative to the backup root.
    pub path: PathBuf,
    /// The permissions and modification time to restore.
    pub attrs: Attributes,
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
