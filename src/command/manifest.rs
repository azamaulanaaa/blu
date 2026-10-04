//! What an apply put into a directory, recorded so the next one need not read it.
//!
//! A manifest is a **cache, never a source of truth.** That distinction is the
//! whole design. It is the reason this is a separate file rather than a shortcut
//! inside [`super::probe`].
//!
//! ## The problem it solves
//!
//! An apply writes only the bytes a directory is missing.
//! Finding them means comparing every slice the version records against what is
//! on disk.
//! That comparison costs a full read of the tree.
//! For a directory nobody has touched since the last apply, that read is waste.
//! The bytes are right, and they were right an hour ago.
//!
//! ## Why a cache needs no format of its own
//!
//! The obvious design is a new on-disk structure recording which chunk sits where.
//! It is not needed, because a version's snapshot *already* says that.
//! `files` gives paths and lengths.
//! `file_chunks` gives `(file_offset, chunk_offset, length)`.
//! `chunks` gives the hashes.
//! A manifest is therefore the snapshot, plus one thing the snapshot cannot know.
//! That thing is what the filesystem said about each file when this tool last
//! wrote it.
//!
//! Reusing the snapshot encoding means the same decoder that has been reading
//! snapshots all along verifies the blob.
//! A manifest can never disagree with a version about chunk placement, because it
//! *is* one.
//!
//! ## Why it is not a source of truth
//!
//! A directory keeps changing after an apply writes it.
//! A user edits a file, and a manifest that still claims the old chunk layout is
//! worse than no manifest at all.
//! Such a manifest would let an apply skip a file that is now wrong and report
//! success.
//!
//! So nothing here is believed on sight. [`Manifest::untrusted`] re-stats every
//! file and compares length and modification time against what the version imposed.
//! It reports any file that moved as untrusted, so the caller reads it properly.
//! A manifest that does not exist, does not decode, or names a version the caller
//! is not applying is not an error.
//! It is a cache miss, and the caller falls back to reading the whole tree.
//!
//! ## What is recorded is what the version imposes
//!
//! Not what `stat` returns when the manifest is written.
//! A length and a modification time are known before the write.
//! Both are exactly what the tree will look like after, because they are what
//! `set_len` and the attribute stage put there.
//! Recording the pre-write state instead would make a manifest stale the moment
//! the apply that wrote it finished.
//!
//! That is why this file is written with [`fs::write`] through a temporary and a
//! rename rather than incrementally.
//! A manifest torn by a crash mid-write would name half its files.
//! The stat check would then pass for the half it did name.

use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
};

use crate::{fsattr, metadata::Snapshot, storage::BlobId};

/// Suffix of the file recording what an apply put into a directory.
///
/// Beside the destination rather than inside it, on the same terms as
/// [`super::restore`]'s lock and scratch directories.
/// It must not appear in the tree it describes.
/// Otherwise it would be backed up as part of that tree.
pub const MANIFEST_SUFFIX: &str = "lepatch-state";

/// What a filesystem reported about one file when a manifest was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Recorded {
    len: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
}

/// What an apply put into a directory.
///
/// Holds the version it applied and, for each file, the length and modification
/// time the filesystem had at that moment.
/// The chunk placement itself is not stored here.
/// It is read back out of the snapshot, so there is only ever one copy of it in
/// the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The hash of the snapshot this was written from.
    pub version: BlobId,
    /// Per recorded path: what the filesystem said at write time.
    recorded: HashMap<PathBuf, Recorded>,
}

impl Manifest {
    /// Where the manifest for `destination` lives.
    ///
    /// The resolved name's sibling, matching where a restore puts its claim. A
    /// destination that does not exist yet keeps the name it was given, since the
    /// directory is the thing being described.
    ///
    /// # Errors
    ///
    /// Returns an error if `destination` has no file name to take a suffix from.
    pub fn path_for(destination: &Path) -> io::Result<PathBuf> {
        let resolved = fs::canonicalize(destination).unwrap_or_else(|_| destination.to_path_buf());

        let name = resolved
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{} has no directory name to record a manifest beside",
                        destination.display()
                    ),
                )
            })?
            .to_string_lossy()
            .into_owned();

        Ok(resolved.with_file_name(format!("{name}.{MANIFEST_SUFFIX}")))
    }

    /// Record what `snapshot` should leave in `root`, by statting every file.
    ///
    /// Built from the version rather than from the filesystem, and that is the
    /// whole reason it works.
    ///
    /// What this records is what the version *imposes*: `file.len` and
    /// `file.attrs.mtime`.
    /// Both are known before a byte is written.
    /// Both are exactly what the tree will look like once `apply` has finished,
    /// because those are the values `set_len` and the attribute stage put there.
    ///
    /// Recording what `stat` returns instead was the first attempt.
    /// It is also the obvious reading of "record what is there".
    /// That approach records the *old* state for every file the apply is about to
    /// change, and then reports itself stale immediately afterwards.
    /// For a file being created it records nothing at all.
    /// A manifest that is wrong about the apply that wrote it is worse than none.
    /// A caller that believes such a manifest skips files it should have read.
    ///
    /// # Errors
    ///
    /// Returns an error only if the destination has no directory name to record
    /// anything beside.
    /// A version that records no modification time for a file yields no record for
    /// it.
    /// That file is read next time rather than trusted on nothing.
    pub fn record(root: &Path, version: BlobId, snapshot: &Snapshot) -> io::Result<Self> {
        // Resolved once, so a destination with no usable name is refused before a
        // version is walked rather than after.
        let _ = Self::path_for(root)?;

        let mut recorded = HashMap::with_capacity(snapshot.files.len());

        for file in &snapshot.files {
            // No recorded time means there is nothing to compare against, so the
            // file gets no entry and is read next time.
            // Recording a zero time would be a claim, and it would be a false one
            // on any filesystem with a real epoch.
            let Some(mtime) = file.attrs.mtime else {
                continue;
            };

            recorded.insert(
                file.path.clone(),
                Recorded {
                    len: file.len,
                    mtime_secs: mtime.secs,
                    mtime_nanos: mtime.nanos,
                },
            );
        }

        Ok(Self { version, recorded })
    }

    /// The paths in `snapshot` this manifest cannot vouch for.
    ///
    /// Empty means every file is where it was left, and a caller may skip reading
    /// the ones it recognises.
    /// Anything named here must be read properly.
    /// A manifest that claimed a file whose content has since changed is the one
    /// way this cache could make an apply wrong.
    /// The fallback is therefore per file and not all-or-nothing.
    ///
    /// A path in `snapshot` that the manifest has no record for is untrusted, as
    /// is one whose length or modification time has moved.
    /// A record for a path the version does not have is ignored.
    /// Such a record is a leftover, not a claim.
    #[must_use]
    pub fn untrusted(&self, root: &Path, snapshot: &Snapshot) -> Vec<PathBuf> {
        let mut out = Vec::new();

        for file in &snapshot.files {
            let trusted = match (
                self.recorded.get(&file.path),
                fs::metadata(root.join(&file.path)),
            ) {
                (Some(recorded), Ok(meta)) => fsattr::mtime_of(&meta).is_some_and(|mtime| {
                    meta.len() == recorded.len
                        && mtime.secs == recorded.mtime_secs
                        && mtime.nanos == recorded.mtime_nanos
                }),
                _ => false,
            };

            if !trusted {
                out.push(file.path.clone());
            }
        }

        out
    }

    /// Whether every file in `snapshot` is where this manifest left it.
    ///
    /// The cheap question, and the only one [`super::apply`] asks on its ordinary
    /// path. False means *some* file moved, not that nothing is known.
    #[must_use]
    pub fn trusts(&self, root: &Path, snapshot: &Snapshot) -> bool {
        self.untrusted(root, snapshot).is_empty()
    }

    /// Write this manifest beside its destination.
    ///
    /// Through a temporary and a rename, so a crash mid-write leaves either the
    /// old manifest or none.
    /// It never leaves a half-written one that would name some files and not
    /// others and pass the stat check for the ones it did name.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary cannot be created, written, or renamed
    /// into place.
    pub fn save(&self, destination: &Path) -> io::Result<PathBuf> {
        let path = Self::path_for(destination)?;
        let temporary = path.with_extension("partial");

        let bytes = encode(self)?;
        fs::write(&temporary, &bytes)?;
        fs::rename(&temporary, &path)?;

        Ok(path)
    }

    /// Read the manifest beside `destination`, if there is a usable one.
    ///
    /// Returns `Ok(None)` for every form of absence and every form of
    /// unreadability.
    /// A caller that cannot use the cache falls back to reading the tree, and
    /// there is nothing for it to report.
    /// Anything that touches the machine can delete a manifest, so its absence is
    /// routine rather than exceptional.
    /// A corrupt one is indistinguishable from an absent one as far as the work
    /// left to do.
    ///
    /// # Errors
    ///
    /// Returns an error only if the manifest's path cannot be formed.
    /// Every I/O failure while reading it is reported as a miss.
    /// None of them leaves a caller with anything it could not also do by finding
    /// no manifest at all.
    pub fn load(destination: &Path) -> io::Result<Option<Self>> {
        let Ok(path) = Self::path_for(destination) else {
            return Ok(None);
        };

        let Ok(bytes) = fs::read(&path) else {
            return Ok(None);
        };

        Ok(decode(&bytes))
    }

    /// Remove the manifest beside `destination`, if it is there.
    ///
    /// Used when an apply decides the cache no longer describes the tree.
    /// That happens for a destination that has been pruned or overwritten by
    /// something else.
    /// The next run must then not consult a record known to be wrong.
    ///
    /// # Errors
    ///
    /// Returns an error if a manifest exists and cannot be removed.
    pub fn remove(destination: &Path) -> io::Result<()> {
        let path = Self::path_for(destination)?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// A manifest's own header: magic, then the version hash.
const MANIFEST_MAGIC: &[u8; 8] = b"LPMANIF1";

/// Encode a manifest: the magic, the version hash, then one entry per file.
///
/// A count is written so a truncated file is refused on its own terms rather than
/// decoding as whatever entries happen to be complete.
fn encode(manifest: &Manifest) -> io::Result<Vec<u8>> {
    use std::io::Write as _;

    let mut out = Vec::with_capacity(64 + manifest.recorded.len() * 24);
    out.extend_from_slice(MANIFEST_MAGIC);
    out.extend_from_slice(manifest.version.as_bytes());

    write_varint(&mut out, manifest.recorded.len() as u64);

    // Sorted, so the same tree produces the same bytes and two manifests can be
    // compared rather than merely diffed by eye.
    let mut entries: Vec<(&PathBuf, &Recorded)> = manifest.recorded.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for (path, recorded) in entries {
        write_varint(&mut out, path.as_os_str().len() as u64);
        let name = path.as_os_str().as_encoded_bytes();
        out.write_all(name)
            .map_err(|e| io::Error::other(e.to_string()))?;
        write_varint(&mut out, recorded.len);
        write_varint(&mut out, zigzag(recorded.mtime_secs));
        write_varint(&mut out, u64::from(recorded.mtime_nanos));
    }

    Ok(out)
}

/// A signed time as one unsigned number, so a varint can carry it.
///
/// Zigzag, rather than a sign flag beside the magnitude.
/// Zigzag stores one value per field instead of two, and needs no special case
/// in the decoder.
/// It also round-trips the full `i64` range rather than the half a sign bit
/// would leave addressable.
fn zigzag(value: i64) -> u64 {
    let shifted = (value << 1) ^ (value >> 63);
    shifted.cast_unsigned()
}

/// The inverse of [`zigzag`].
fn unzigzag(value: u64) -> i64 {
    ((value >> 1) ^ (0u64.wrapping_sub(value & 1))).cast_signed()
}

/// Decode a manifest, or `None` if the bytes are not one.
///
/// Every failure is the same failure to a caller: unusable.
/// The reasons are distinguished in the tests rather than in the type.
/// There is one correct response to all of them, and it is to read the tree
/// instead.
fn decode(bytes: &[u8]) -> Option<Manifest> {
    let mut at = 0usize;

    if bytes.get(..MANIFEST_MAGIC.len())? != MANIFEST_MAGIC {
        return None;
    }
    at += MANIFEST_MAGIC.len();

    let hash = bytes.get(at..at + 32)?;
    at += 32;
    let version = BlobId::from_slice(hash).ok()?;

    let count = read_varint(bytes, &mut at)?;
    let mut recorded = HashMap::with_capacity(count.min(1024) as usize);

    for _ in 0..count {
        let name_len = usize::try_from(read_varint(bytes, &mut at)?).ok()?;
        let name = bytes.get(at..at + name_len)?;
        at += name_len;

        let len = read_varint(bytes, &mut at)?;
        let mtime_secs = unzigzag(read_varint(bytes, &mut at)?);
        let mtime_nanos = u32::try_from(read_varint(bytes, &mut at)?).ok()?;

        let path = PathBuf::from(std::ffi::OsString::from(
            String::from_utf8(name.to_vec()).ok()?,
        ));

        recorded.insert(
            path,
            Recorded {
                len,
                mtime_secs,
                mtime_nanos,
            },
        );
    }

    Some(Manifest { version, recorded })
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).unwrap_or(0) | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).unwrap_or(0));
}

fn read_varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;

    loop {
        let byte = *bytes.get(*at)?;
        *at += 1;

        value |= u64::from(byte & 0x7f).checked_shl(shift)?;

        if byte & 0x80 == 0 {
            return Some(value);
        }

        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}
