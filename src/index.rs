//! On-disk layout of a versioned backup set.
//!
//! A backup set is a single blob store plus one small index file per version,
//! all named after a common base path:
//!
//! ```text
//! photos.bin          blob store, append only
//! photos.001.idx      index for version 1, holding that version's snapshot key
//! photos.002.idx      index for version 2
//! photos.lock         held for the duration of a write
//! ```
//!
//! The base path is treated as an opaque prefix rather than a stem, so a base
//! of `photos.tar` yields `photos.tar.bin` instead of silently losing the
//! extension the way `Path::with_extension` would.

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

/// Extension of the blob store.
pub const BLOB_EXTENSION: &str = "bin";
/// Extension of a per-version index file.
pub const INDEX_EXTENSION: &str = "idx";
/// Extension of the write lock.
pub const LOCK_EXTENSION: &str = "lock";

/// What a scan of the backup set found on disk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Versions {
    /// Highest version with an index file, whether or not that file holds a
    /// usable key. New versions are numbered above this so a half-written
    /// index is never overwritten.
    pub highest_existing: Option<u32>,
    /// Highest version whose index holds a usable key, with that key.
    pub latest: Option<(u32, String)>,
}

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

    /// Path of the blob store.
    #[must_use]
    pub fn blob_path(&self) -> PathBuf {
        self.suffixed(BLOB_EXTENSION)
    }

    /// Path of the write lock.
    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.suffixed(LOCK_EXTENSION)
    }

    /// Path of the index file for `version`.
    #[must_use]
    pub fn index_path(&self, version: u32) -> PathBuf {
        self.suffixed(&format!("{version:03}.{INDEX_EXTENSION}"))
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

    /// Scan the set for the versions present on disk.
    ///
    /// A missing directory is reported as an empty set rather than an error, so
    /// that the first backup of a fresh location does not have to special-case
    /// it.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be read, or if an
    /// index file exists but cannot be read.
    pub fn versions(&self) -> io::Result<Versions> {
        let directory = self.directory();
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Versions::default()),
            Err(e) => return Err(e),
        };

        let mut versions = Versions::default();

        for entry in entries {
            let entry = entry?;
            let Some(version) = self.version_of(&entry.path()) else {
                continue;
            };

            versions.highest_existing = Some(
                versions
                    .highest_existing
                    .map_or(version, |v| v.max(version)),
            );

            // A version whose index is empty or malformed is still counted as
            // existing — it just is not a candidate for "latest", so a backup
            // interrupted midway cannot make the set look like it has a newer
            // usable version than it really does.
            if let Some(key) = read_key(&entry.path())? {
                let better = versions.latest.as_ref().is_none_or(|(v, _)| version > *v);
                if better {
                    versions.latest = Some((version, key));
                }
            }
        }

        Ok(versions)
    }

    /// Recognise `candidate` as an index file of this set, returning its
    /// version.
    fn version_of(&self, candidate: &Path) -> Option<u32> {
        if candidate.parent() != Some(self.directory()) {
            return None;
        }

        let base = self.base.file_name()?.to_str()?;
        let name = candidate.file_name()?.to_str()?;

        let rest = name.strip_prefix(base)?.strip_prefix('.')?;
        let (digits, extension) = rest.rsplit_once('.')?;

        if extension != INDEX_EXTENSION
            || digits.len() != 3
            || !digits.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }

        digits.parse().ok()
    }

    /// Read the snapshot key recorded for `version`.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotFound`] if that version has no index, or if
    /// its index is empty or malformed.
    pub fn read_key(&self, version: u32) -> io::Result<String> {
        let path = self.index_path(version);

        read_key(&path)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "index for version {version} at {} is empty or malformed",
                    path.display()
                ),
            )
        })
    }

    /// Claim the next free version number.
    ///
    /// The index file is created empty straight away so that a concurrent
    /// process claiming the same number loses the race instead of writing over
    /// this run's result. If the run then fails, the claim is rolled back; if
    /// the process is killed outright the number is merely skipped, and
    /// [`Versions::latest`] ignores the empty file it leaves behind.
    ///
    /// # Errors
    ///
    /// Returns an error if the containing directory cannot be created, if the
    /// index cannot be created, or if 64 consecutive version numbers are
    /// already claimed.
    pub fn reserve(&self) -> io::Result<Reservation> {
        self.ensure_directory()?;

        let mut version = self.versions()?.highest_existing.map_or(1, |v| v + 1);

        // Bounded: a collision means another process is claiming versions at
        // the same time, not that anything is wrong.
        for _ in 0..64 {
            let path = self.index_path(version);

            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(Reservation {
                        path,
                        version,
                        file: Some(file),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => version += 1,
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "could not claim a version number for {}",
                self.base.display()
            ),
        ))
    }

    /// Take the write lock, excluding other processes.
    ///
    /// The lock is advisory and self-releasing only for a clean exit: if the
    /// process is killed the file survives, and the error message says how to
    /// recover.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::AlreadyExists`] if another process holds the
    /// lock, and an error if the lock file cannot be created.
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

/// An index file claimed by [`IndexSet::reserve`], not yet filled in.
#[derive(Debug)]
pub struct Reservation {
    path: PathBuf,
    version: u32,
    file: Option<fs::File>,
}

impl Reservation {
    /// The version this reservation claimed.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Write `key` into the claimed index and flush it to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be written or flushed. Either way the
    /// claim is rolled back on drop, so the version is not left half-published.
    pub fn commit(mut self, key: &str) -> io::Result<()> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| io::Error::other("index reservation already used"))?;

        file.write_all(key.as_bytes())?;
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // Still holding the handle means `commit` never ran, so this run failed
        // and must not leave a claim behind.
        if self.file.is_some() {
            let _ = fs::remove_file(&self.path);
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

/// Read a key out of an index file, treating an empty or malformed file as
/// absent rather than as an error.
fn read_key(path: &Path) -> io::Result<Option<String>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let key = contents.trim();

    // One key per file: a newline would mean the file is not what it claims.
    if key.is_empty() || key.contains(['\n', '\r']) {
        return Ok(None);
    }

    Ok(Some(key.to_string()))
}
