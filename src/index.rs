//! On-disk layout of a versioned backup set.
//!
//! A backup set is a sequence of numbered segments plus a lock, all named after a
//! common base path:
//!
//! ```text
//! photos.0000.bin     segment 0: the first N bytes of records
//! photos.0001.bin     segment 1, sealed once full and never written again
//! ...
//! photos.v0001.idx    where version 1's snapshot is, so a restore need not scan
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
//! Every segment's header carries its own number, and a file whose header does
//! not match the number in its name is refused rather than read as though it
//! belonged — a segment copied in from elsewhere is caught that way. Segments
//! are numbered from zero with no gaps; a gap is refused too, because a set
//! missing a segment in the middle cannot be read as though the missing chunk
//! data were simply not there.
//!
//! ## Index files
//!
//! One small file per version, saying which segment that version's snapshot is
//! in. They exist for one reason: without them, answering "where is version 7's
//! snapshot" means reading the headers of every record in every segment, and that
//! scan is on the critical path of every restore and every verify.
//!
//! **They are a cache, not a second source of truth.** The store's commit records
//! are the record of what versions exist; an index file is derived from them and
//! can be lost, stale, or wrong without the store disagreeing about anything. If
//! the two conflict, the store wins and the index is rebuilt. That is the
//! inversion from the previous design, where an index file *was* the only place a
//! version existed and publishing it was the commit — a cross-file commit, since
//! nothing could make an index and the bytes it named land together. Now the
//! commit record is the commit, a single append to a single file, and the index
//! can be written after it, best-effort, and losing it costs a scan rather than a
//! backup.
//!
//! `lepatch reindex` rebuilds them from the store, which is the operation the
//! previous design had no equivalent of.
//!
//! The `v` in `photos.v001.idx` is deliberate. Without it, `photos.001.idx` and
//! `photos.001.bin` would name different things in the same directory, and a glob
//! or a person clearing out old index files would hit both.
//!
//! The base path is treated as an opaque prefix rather than a stem, so a base
//! of `photos.tar` yields `photos.tar.0000.bin` instead of silently losing the
//! extension the way `Path::with_extension` would.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Extension of a store segment.
pub const BLOB_EXTENSION: &str = "bin";
/// Extension of a per-version index file.
pub const INDEX_EXTENSION: &str = "idx";
/// Extension of the write lock.
pub const LOCK_EXTENSION: &str = "lock";

/// Digits in a segment's number.
///
/// The width both caps a set at 10,000 segments and makes lexical order over a
/// directory listing the same as numeric order, so a listing is already in order.
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
    /// `0010` before `0002` is a set that restores its records out of order.
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

    /// Path of the index file for `version`.
    #[must_use]
    pub fn index_path(&self, version: u32) -> PathBuf {
        self.suffixed(&format!("v{version:0SEGMENT_DIGITS$}.{INDEX_EXTENSION}"))
    }

    /// Every version that has an index file, in order.
    ///
    /// The names, not the contents: reading them is the cheap half, and the count
    /// of them is what lets an open know how far a set has got without scanning
    /// it. Which of them are *current* is the store's business, not this
    /// function's.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be read.
    pub fn indexed_versions(&self) -> io::Result<Vec<u32>> {
        let prefix = self.index_prefix();
        let directory = self.directory();

        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut found: Vec<u32> = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.parent() != Some(directory) {
                continue;
            }

            if let Some(version) = Self::indexed_version(&path, &prefix) {
                found.push(version);
            }
        }

        found.sort_unstable();
        Ok(found)
    }

    /// Path of the write lock.
    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.suffixed(LOCK_EXTENSION)
    }

    /// The `base.` part every file of this set shares.
    fn segment_prefix(&self) -> String {
        let base = self
            .base
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();

        format!("{base}.")
    }

    /// The `base.v` part every index file of this set shares.
    ///
    /// The `v` is what keeps an index file from ever being mistaken for a
    /// segment: both are `base.` plus digits plus an extension, and they live in
    /// the same directory.
    fn index_prefix(&self) -> String {
        let base = self
            .base
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();

        format!("{base}.v")
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

    /// The version in `path`'s file name, if it is one of this set's index files.
    ///
    /// `None` for anything that is not exactly `base.vNNNN.idx`.
    fn indexed_version(path: &Path, prefix: &str) -> Option<u32> {
        let name = path.file_name()?.to_str()?;

        if path.extension().is_none_or(|ext| ext != INDEX_EXTENSION) {
            return None;
        }

        let digits = name
            .strip_prefix(prefix)?
            .strip_suffix(&format!(".{INDEX_EXTENSION}"))?;

        if digits.len() != SEGMENT_DIGITS || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }

        digits.parse().ok()
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
