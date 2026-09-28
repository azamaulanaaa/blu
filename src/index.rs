//! On-disk layout of a versioned backup set.
//!
//! A backup set is a single store file plus a lock, both named after a common
//! base path:
//!
//! ```text
//! photos.bin          the store: records, and the commit record of every version
//! photos.lock         held for the duration of a write
//! ```
//!
//! There is no third file. Versions used to be recorded in one small index file
//! each, and that was a cross-file commit: an index named an offset inside the
//! store, and nothing could make the two land together. A power cut could leave
//! the index durable and the bytes it named unwritten, or the index's own
//! directory entry missing while its contents were on the device. Now the store
//! records its own versions, so publishing one is a single append to a single
//! file and there is no ordering between two files to get right.
//!
//! Losing an index used to mean losing a backup whose bytes were still in the
//! file, with no way to find them. Losing nothing now loses nothing.
//!
//! The base path is treated as an opaque prefix rather than a stem, so a base
//! of `photos.tar` yields `photos.tar.bin` instead of silently losing the
//! extension the way `Path::with_extension` would.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Extension of the store.
pub const BLOB_EXTENSION: &str = "bin";
/// Extension of the write lock.
pub const LOCK_EXTENSION: &str = "lock";

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

    /// Path of the store.
    #[must_use]
    pub fn blob_path(&self) -> PathBuf {
        self.suffixed(BLOB_EXTENSION)
    }

    /// Path of the write lock.
    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.suffixed(LOCK_EXTENSION)
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
