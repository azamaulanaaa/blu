//! On-disk description of a backup.
//!
//! A snapshot is serialised into a single blob in the storage backend. The store
//! names the blob by the BLAKE3 hash of its bytes. The code frames the blob with
//! a magic prefix and a format version. So a layout change is reported as a
//! layout change, not as a confusing deserialisation error. The wire format is
//! in the `codec` submodule.

mod codec;

// `Malformed` is the payload of `Error::Decode`, and that variant is public. So
// `Malformed` is already part of this module's public API. That is true whether
// or not a caller can name the type. A caller can write `Error::Decode(_)` and
// match on the outer error. The caller cannot destructure the inner one. The
// caller cannot name it in a signature. The caller cannot compare against it.
// That is the only reason it was `pub` inside a private module in the first
// place. Re-exported so the type and the variant that carries it agree about
// being public.
pub use codec::Malformed;

use std::{fmt, io, path::PathBuf};

use crate::storage;

/// Magic prefix identifying a lepatch snapshot blob.
pub const MAGIC: &[u8; 8] = b"LEPATCH\x00";

/// Snapshot layout version written by this build.
///
/// Bump this when the shape of [`Snapshot`] changes.
/// This build writes 5 and reads 5 only.
/// Version 5 records one UTC stamp per snapshot.
/// Older layouts carry no snapshot stamp.
pub const FORMAT_VERSION: u32 = 5;

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

/// A point in time in UTC.
///
/// The store keeps seconds and nanos.
/// The form never depends on the platform form of `SystemTime`.
/// Display converts the value to local time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp {
    /// Seconds since the Unix epoch. It is negative before the epoch.
    pub secs: i64,
    /// Nanos past the second, from 0 to `999_999_999`.
    pub nanos: u32,
}

impl Timestamp {
    /// Current time from the system clock.
    ///
    /// Reads the clock once and converts the reading.
    #[must_use]
    pub fn now() -> Self {
        std::time::SystemTime::now().into()
    }

    /// UTC text in RFC 3339 form with a Z suffix.
    ///
    /// Hand-rolled date math renders the value.
    /// Rendering never fails and never reads the zone.
    #[must_use]
    pub fn to_utc_string(self) -> String {
        let days = self.secs.div_euclid(86_400);
        let day_secs = self.secs.rem_euclid(86_400);
        let (year, month, day) = civil_from_days(days);
        let hour = day_secs / 3_600;
        let minute = day_secs % 3_600 / 60;
        let second = day_secs % 60;
        let mut out = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
        if self.nanos != 0 {
            let frac = format!("{:09}", self.nanos);
            out.push('.');
            out.push_str(frac.trim_end_matches('0'));
        }
        out.push('Z');
        out
    }

    /// Local text for display with the system offset.
    ///
    /// Reads the system zone once per call.
    /// Uses UTC text where the zone or the value is out of range.
    #[must_use]
    pub fn to_local_string(self) -> String {
        let zoned = jiff::Timestamp::from_second(self.secs)
            .ok()
            .and_then(|base| {
                base.checked_add(jiff::SignedDuration::from_nanos(i64::from(self.nanos)))
                    .ok()
            })
            .map(|instant| instant.to_zoned(jiff::tz::TimeZone::system()));
        zoned.map_or_else(|| self.to_utc_string(), |zoned| zoned.to_string())
    }

    /// Parse RFC 3339 text into a stamp.
    ///
    /// Accepts the UTC form that [`Timestamp::to_utc_string`] renders.
    /// Accepts an explicit offset form as the same instant.
    ///
    /// # Errors
    ///
    /// Returns an error when the text is not RFC 3339 time text.
    pub fn parse_utc(text: &str) -> io::Result<Self> {
        text.parse::<jiff::Timestamp>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{text:?} is not RFC 3339 time text: {e}"),
                )
            })
            .map(|instant| {
                // `as_second` truncates toward zero, so a pre-epoch fraction
                // arrives with a negative sub-second part. Floor the seconds
                // instead, keeping nanos non-negative like the encoder does.
                let mut secs = instant.as_second();
                let mut nanos = instant.subsec_nanosecond();
                if nanos < 0 {
                    secs -= 1;
                    nanos += 1_000_000_000;
                }
                Self {
                    secs,
                    nanos: u32::try_from(nanos).unwrap_or(0),
                }
            })
    }
}

/// Days since the Unix epoch into year, month and day.
///
/// Proleptic Gregorian calendar math.
/// The caller splits the seconds into days and time of day.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Shift to the civil epoch of 0000-03-01 before the era math.
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = if month_part < 10 {
        month_part + 3
    } else {
        month_part - 9
    };
    let full_year = if month <= 2 { year + 1 } else { year };
    (
        full_year,
        u32::try_from(month).unwrap_or(0),
        u32::try_from(day).unwrap_or(0),
    )
}

/// Refuse a stamp older than the latest committed stamp.
///
/// Takes the latest committed stamp, when the set holds one.
/// Takes the stamp the new version carries.
/// Returns an error naming both stamps when the new one is older.
/// Equal stamps pass. A first version always passes.
///
/// # Errors
///
/// Returns an error when `next` predates `latest`.
pub fn refuse_older_taken_at(latest: Option<Timestamp>, next: Timestamp) -> io::Result<()> {
    if let Some(latest) = latest
        && next < latest
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to commit: the new version is {} but the latest version is {}. A new version never predates the latest one",
                next.to_utc_string(),
                latest.to_utc_string()
            ),
        ));
    }
    Ok(())
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(time: std::time::SystemTime) -> Self {
        match time.duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => Self {
                // Saturate rather than wrap. A wrapped negative would be a worse
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

/// A complete description of one backup: the time it ran, the directory tree,
/// the set of distinct chunk contents, and how the chunks map back onto files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Time the backup ran, in UTC seconds and nanos.
    ///
    /// The store keeps UTC. Display converts to local time.
    /// The value orders versions. A new version never predates the latest one.
    pub taken_at: Timestamp,
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
    /// A **backup** writes it. A tree can change between being walked and being
    /// read. The chunker stats each file when it registers it. The chunker reads
    /// that file again later. So a file that grew in between shifts every later
    /// chunk into the wrong one. The lengths recorded during the walk stop
    /// matching what the chunker read. The snapshot is then internally consistent
    /// and wrong.
    ///
    /// A **restore** reads a snapshot that is a blob in a store. Nothing about
    /// that blob is trustworthy. `File::len` is one number. If it is longer than
    /// the slices, then `set_len` at the end of each file zero-fills the tail
    /// instead. The `set_len` call exists to close a gap *between* slices. That
    /// does not fail. The restore reports success. The restore leaves a file of
    /// exactly the right size with a plausible amount of content and a tail of
    /// nothing. A check that exists only on the writing side is a check in the
    /// wrong place. A hostile snapshot is not the threat on the writing side.
    ///
    /// Both the total and the covered extent are required to match. The total
    /// catches slices that are too long, or too short, or that overlap so the
    /// sum overcounts. The extent catches slices that leave a gap at the start,
    /// where the sum can be right and a hole is not.
    ///
    /// The extent is also what catches a slice reaching *past* the end of its
    /// file. So there is deliberately no separate check for that. A separate check
    /// looks like the more direct thing to say. It also has a clearer message. But
    /// it fires first, on the path a growing file takes during a backup. It
    /// replaces the check that tells a user their tree moved under them. Same
    /// refusal, worse message.
    ///
    /// `what` is the clause that goes between the file and the numbers. The
    /// caller's clause is the one to use, because the two situations read
    /// differently. "Changed while it was being backed up" is a diagnosis.
    /// "Does not match its own chunks" is a description. A restore has no basis
    /// for the diagnosis. `what` is a clause rather than an enum, because neither
    /// side branches on it. `what` is a sentence. Putting that sentence in the
    /// message is the whole of what it is for.
    ///
    /// # Errors
    ///
    /// Returns an error naming the file, its recorded length, how many bytes the
    /// slices total, and how much of the file they cover. The error names all
    /// four, because a mismatch between any two of them is a different mistake.
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

    /// An empty snapshot stamped with `taken_at`.
    ///
    /// Takes the stamp the backup ran at.
    /// The caller stamps every snapshot with the run time.
    #[must_use]
    pub fn new(taken_at: Timestamp) -> Self {
        Self {
            taken_at,
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
    /// caller to choose here. A snapshot is not "in" a version. A build writes
    /// the snapshot, and the build emits one version.
    ///
    /// # Errors
    ///
    /// Returns an error only where a path in this snapshot cannot be represented
    /// on this platform. On unix that cannot happen. A path is a byte string
    /// there, so the code writes its bytes as they are. A name that is not valid
    /// UTF-8 round-trips exactly on unix. Everywhere else a path is text. A name
    /// that the platform's encoding cannot hold is **refused** there. The code
    /// does not write such a name with replacement characters. Replacement
    /// characters would record a path that does not exist. They would also
    /// collide two real paths. A tree that the format cannot represent is
    /// refused here. The refusal happens at the point where a person can be
    /// told. The code does not back up such a tree into a set that cannot be
    /// restored.
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

        // Exact match. This build writes one layout and reads one layout.
        // A range would invite a reader that decodes a layout it never wrote.
        if version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: FORMAT_VERSION,
            });
        }

        codec::decode_body(&bytes[header..]).map_err(Error::Decode)
    }
}

impl TryFrom<&[u8]> for Snapshot {
    type Error = Error;

    /// Parse the framed on-disk representation.
    ///
    /// `TryFrom` rather than an inherent `from_bytes`, so that "these bytes are a
    /// snapshot" is a conversion like any other. `VersionIndex` uses the same
    /// vocabulary. It parses a different wire format into a different type. It
    /// now says so the same way. There is one implementation, not an inherent
    /// method beside a trait impl.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Truncated`] if the header is incomplete. It returns
    /// [`Error::BadMagic`] if the blob is not a snapshot. It returns
    /// [`Error::UnsupportedVersion`] if an incompatible layout wrote it. It
    /// returns [`Error::Decode`] if the body does not match this version's
    /// shape.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::parse(bytes)
    }
}

/// A symbolic or hard link, recorded but never chunked.
///
/// A sum type rather than a struct with a `kind`. The two cases give the same
/// field two different meanings. They also subject it to two different rules.
///
/// `target` used to be "the raw link target" for a symbolic link. For a hard
/// link, `target` used to be "the path of the file within the backup whose
/// contents are shared". A restore does not treat them alike. A restore resolves
/// the hard link path against the restore root. The hard link path must land
/// inside that root. A restore uses the symbolic link target verbatim. The
/// symbolic link may point anywhere, including outside the backup. Two rules for
/// one field, and nothing in the type said which applied. The only signal was the
/// `kind` sitting next to it. A field access reads as though it were just a
/// field.
///
/// So there are two variants, and each names its own second path. `target` means
/// a link target and exists only on [`Link::Symbolic`]. A hard link has
/// `shares`, which is what it does rather than where it points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    /// Points elsewhere. The target is stored verbatim and may name anything,
    /// including a path outside the backup. A symbolic link is only ever a string
    /// until something follows it.
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
        /// Resolved against the restore root and required to land inside it. That is
        /// the whole difference from [`Link::Symbolic::target`]. A hard link is a
        /// claim about this tree. So a claim that names somewhere else is a
        /// damaged snapshot, not a link.
        shares: PathBuf,
    },
}
/// Everything this platform records about one entry, and a restore reapplies.
///
/// All of it is the same kind of thing. It is something the platform may or may
/// not have. The code records it if the platform has it. The code reapplies it
/// if it is there. Each field is an `Option`. "This platform has no permission
/// bits" and "the permissions are the empty set" are different facts. A sentinel
/// like `0` could not tell them apart. A restore acts on them differently. One
/// is a no-op and the other is a set of permissions nobody has.
///
/// `owner` is the user and group as one value. They are one identity, and they
/// come out of one `stat`. The field is absent on a platform with no users. The
/// field is also absent in every snapshot taken before this field existed. That
/// absence is a fact, not a guess. A snapshot that recorded nothing about
/// ownership had none to record.
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
/// Ids and not names. A name means nothing on any machine but the one it was
/// read from. A uid of 1000 is one person here and another person there. So a
/// name would be a portability bug stored alongside the answer.
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
    /// One call rather than three. The interesting case is a missing one. A backup
    /// that captured the mode and the time but forgot the owner would produce
    /// snapshots that look complete. Such snapshots restore a tree that nobody
    /// can read.
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
    /// set-user-ID and set-group-ID bits when its owner changes. So setting the
    /// mode first and the owner second silently strips setuid from everything
    /// restored. The failure is silent in the worst way. The file is written and
    /// its bytes match. The only difference is a bit nobody was looking for.
    ///
    /// Modification time is last for a different reason. Creating a link inside a
    /// directory updates that directory's time. So a time set before the links
    /// would be overwritten by them.
    ///
    /// An owner that cannot be set is an error here. [`Self::apply_restoring`] is
    /// the other policy, for a caller that would rather report it than stop.
    ///
    /// # Errors
    ///
    /// Returns an error if the path cannot be found, or if any attribute cannot
    /// be applied. One cause is that the caller does not own the file. Another
    /// cause is that the caller cannot change the owner.
    pub fn apply(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.apply_inner(path, true).map(|_| ())
    }

    /// Reapply all of them, treating an owner that cannot be set as a fact to report
    /// rather than a failure.
    ///
    /// Returns whether the owner took, or `true` where there was none to set.
    ///
    /// Changing an owner takes privilege. For an unprivileged caller, that is the
    /// ordinary case rather than an exceptional one. Restoring a tree that
    /// contains a file you do not own is a normal thing to want. Stopping there
    /// would be the wrong answer twice over. The tree is left half-written. The
    /// file that failed already exists with the wrong owner. So there is nothing
    /// to retry and no record that the owner is wrong.
    ///
    /// Everything else is still applied. A file with the right contents and mode
    /// but the wrong owner is a tree that is nearly right. A nearly right tree
    /// is a much better thing to hand back than an aborted run and a partial
    /// directory.
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
    /// A restore creates the file at this length. The restore truncates the file
    /// back to this length at the end. So a gap in the recorded slices is
    /// zero-filled rather than detected.
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
