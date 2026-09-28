//! On-disk layout of a versioned backup set.
//!
//! A backup set is a sequence of numbered segments plus a lock, all named after a
//! common base path:
//!
//! ```text
//! photos.000.bin      segment 0: the first N bytes of records
//! photos.001.bin      segment 1, sealed once full and never written again
//! ...
//! photos.lock         held for the duration of a write
//! ```
//!
//! ## Why segments
//!
//! One file that grows without bound hits a limit sooner or later — a
//! filesystem's maximum file size, a backup tool that cannot read it, a transfer
//! that has to succeed in one piece. A set that is several files each under a
//! fixed limit has none of those problems, and the numbering means a segment can
//! be handled on its own.
//!
//! The property that makes this safe rather than merely possible is that **a
//! full segment is sealed**. Nothing writes to it again, so a torn write — which
//! the commit protocol already anticipated as a tail that fails to verify — can
//! only ever be in the last segment. Every other segment is immutable, which
//! means a failure inside one is permanent and detectable rather than something
//! the next open can quietly paper over by truncating it.
//!
//! Every segment carries its own index in its header, so a file renamed or moved
//! into another set is refused rather than read as though it belonged. Segments
//! are numbered from zero with no gaps; a gap is refused too, because a set
//! missing a segment in the middle cannot be read as though the missing chunk
//! data were simply not there.
//!
//! ## Why no index file
//!
//! Versions used to be recorded in one small index file each, and that was a
//! cross-file commit: an index named an offset inside the store, and nothing
//! could make the two land together. A power cut could leave the index durable
//! and the bytes it named unwritten, or the index's own directory entry missing
//! while its contents were on the device. Now the store records its own
//! versions, so publishing one is a single append to a single file and there is
//! no ordering between two files to get right.
//!
//! Losing an index used to mean losing a backup whose bytes were still in the
//! file, with no way to find them. Losing nothing now loses nothing.
//!
//! The base path is treated as an opaque prefix rather than a stem, so a base
//! of `photos.tar` yields `photos.tar.000.bin` instead of silently losing the
//! extension the way `Path::with_extension` would.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Extension of a store segment.
pub const BLOB_EXTENSION: &str = "bin";
/// Extension of the write lock.
pub const LOCK_EXTENSION: &str = "lock";

/// Digits in a segment's number.
///
/// Four is not arbitrary in the useful direction: a set of 10,000 segments at 4
/// GiB each is 40 TB, and four digits sorts lexically as it sorts numerically, so
/// a directory listing is already in order.
pub const SEGMENT_DIGITS: usize = 4;

/// A backup set rooted at a base path.
#[derive(Debug, Clone)]
pub struct IndexSet {
    base: PathBuf,
}

impl IndexSet {
    /// A backup set whose files are named after `base`.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// The base path this set is named after.
    #[must_use]
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Path of segment `segment` of the store.
    ///
    /// Zero-padded to [`SEGMENT_DIGITS`], so that lexical order over the files in
    /// a directory is numeric order over the segments — a listing that reads
    /// `010` before `002` is a set that restores its records out of order.
    #[must_use]
    pub fn blob_path(&self, segment: u32) -> PathBuf {
        self.suffixed(&format!("{segment:0SEGMENT_DIGITS$}.{BLOB_EXTENSION}"))
    }

    /// Every segment of the set that is on disk, in order.
    ///
    /// Reads the directory rather than probing for `0`, `1`, `2` until one is
    /// missing, so a set with a gap in it is reported as the set it is instead of
    /// as a shorter one. A base path whose directory does not exist yet is an
    /// empty set rather than an error, so the first backup of a fresh location
    /// does not have to special-case it.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be read, or if the set
    /// is missing a segment in the middle.
    pub fn segments(&self) -> io::Result<Vec<PathBuf>> {
        let prefix = self.segment_prefix();
        let directory = self.directory();

        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut found: Vec<(u32, PathBuf)> = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.parent() != Some(directory) {
                continue;
            }

            // Only the layout's own shape. A file that merely starts with
            // `photos.` is somebody's, and this reads a directory a person can put
            // things in — so anything that is not exactly `NNNN.bin` is left alone
            // rather than guessed at.
            let Some(number) = Self::segment_number(&path, &prefix) else {
                continue;
            };

            found.push((number, path));
        }

        found.sort_by_key(|(number, _)| *number);

        // A gap is refused rather than tolerated. The segments are the only place
        // the data is, and a set missing one in the middle would otherwise read
        // as though the chunks in it had never been stored.
        for (index, (number, _)) in found.iter().enumerate() {
            let expected = u32::try_from(index).unwrap_or(u32::MAX);
            if *number != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} is missing segment {expected}: the segments here stop at {} and then \
                         continue at {number}, so whatever is in between is not here",
                        self.base.display(),
                        expected.saturating_sub(1),
                    ),
                ));
            }
        }

        Ok(found.into_iter().map(|(_, path)| path).collect())
    }

    /// Path of the write lock.
    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.suffixed(LOCK_EXTENSION)
    }

    /// The `base.NNNN` part every segment of this set shares.
    fn segment_prefix(&self) -> String {
        let base = self
            .base
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();

        format!("{base}.")
    }

    /// Directory the set's files live in.
    #[must_use]
    pub fn directory(&self) -> &Path {
        self.base
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
    }

    /// Create the directory the set lives in, if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created, for instance
    /// because a parent is a regular file or the caller lacks permission.
    pub fn ensure_directory(&self) -> io::Result<()> {
        fs::create_dir_all(self.directory())
    }

    /// The segment number in `path`'s file name, if it is one of this set's.
    ///
    /// `None` for anything that is not exactly `base.NNNN.bin` — a neighbouring
    /// set, a different extension, a note somebody left. Those are skipped rather
    /// than reported, because they are not this set's business.
    fn segment_number(path: &Path, prefix: &str) -> Option<u32> {
        let name = path.file_name()?.to_str()?;

        if path.extension().is_none_or(|ext| ext != BLOB_EXTENSION) {
            return None;
        }

        let digits = name
            .strip_prefix(prefix)?
            .strip_suffix(&format!(".{BLOB_EXTENSION}"))?;

        if digits.len() != SEGMENT_DIGITS || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }

        digits.parse().ok()
    }

    /// Append `suffix` to the base path's file name, leaving the base itself
    /// untouched.
    fn suffixed(&self, suffix: &str) -> PathBuf {
        let mut name = self
            .base
            .file_name()
            .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
        name.push(".");
        name.push(suffix);

        self.base.with_file_name(name)
    }

    /// Take the write lock, excluding other processes.
    ///
    /// The lock is advisory and self-releasing only for a clean exit: if the
    /// process is killed the file survives, and the error message says how to
    /// recover.
    ///
    /// # Errors
    ///
    /// Returns an error if another process holds the lock, and an error if the
    /// lock file cannot be created.
    pub fn lock(&self) -> io::Result<Lock> {
        self.ensure_directory()?;

        let path = self.lock_path();

        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => Ok(Lock {
                path,
                file: Some(file),
            }),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
                e.kind(),
                format!(
                    "{} is locked by another lepatch process; if none is running, remove it",
                    path.display()
                ),
            )),
            Err(e) => Err(e),
        }
    }
}

/// The write lock, released on drop.
#[derive(Debug)]
pub struct Lock {
    path: PathBuf,
    file: Option<fs::File>,
}

impl Drop for Lock {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}
