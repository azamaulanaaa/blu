//! Capture and restoration of platform-specific file attributes.
//!
//! Unix and Windows expose different attribute sets, so everything that
//! cannot be expressed portably is funnelled through this module and compiled
//! per platform. An attribute a platform does not model is reported as `None`,
//! as `0`, or as a no-op rather than as a fabricated value, so a restore never
//! writes a bogus mode or timestamp.

use std::{fs, io, path::Path};

use crate::metadata::Timestamp;

/// Unix permission bits, or `0` on platforms without them.
#[must_use]
pub fn mode_of(meta: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        meta.permissions().mode()
    }

    #[cfg(not(unix))]
    {
        let _ = meta;
        0
    }
}

/// Last modification time, or `None` when the platform cannot report one.
#[must_use]
pub fn mtime_of(meta: &fs::Metadata) -> Option<Timestamp> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Some(Timestamp {
            secs: meta.mtime(),
            // The `max(0)` and `try_from` only cover a disagreeing platform.
            nanos: u32::try_from(meta.mtime_nsec().max(0)).unwrap_or(0),
        })
    }

    #[cfg(not(unix))]
    {
        meta.modified().ok().map(Into::into)
    }
}

/// Apply permission bits captured by [`mode_of`].
///
/// A no-op where the platform has no equivalent, and where `mode` is `0` — which
/// is what `mode_of` returns on such a platform, and what a non-unix snapshot
/// records.
///
/// # Errors
///
/// Returns an error if the path cannot be found, or its permissions cannot be
/// changed — for instance because the caller does not own the file.
pub fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if mode == 0 {
            return Ok(());
        }

        fs::set_permissions(path, fs::Permissions::from_mode(mode))
    }

    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Ok(())
    }
}

/// Apply a modification time captured by [`mtime_of`].
///
/// # Errors
///
/// Returns an error if the path cannot be found, or the timestamp cannot be
/// applied to it. A symbolic link is skipped rather than followed, so no error
/// is reported for one.
pub fn set_mtime(path: &Path, mtime: Timestamp) -> io::Result<()> {
    let time = filetime::FileTime::from_unix_time(mtime.secs, mtime.nanos);

    // A symlink's own mtime is not meaningful, and setting it would follow the
    // link and touch the target.
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Ok(());
    }

    filetime::set_file_mtime(path, time)
}

/// Create a symbolic link at `link` pointing at `target`.
///
/// `target_is_dir` selects the Windows flavour of symlink, which has no
/// equivalent on other platforms.
///
/// # Errors
///
/// Returns an error if `link` already exists, if its parent directory is
/// missing, if the platform does not support symbolic links, or — on Windows —
/// if the caller lacks the privilege required to create one.
pub fn symlink(target: &Path, link: &Path, target_is_dir: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        let _ = target_is_dir;

        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    {
        let _ = target_is_dir;

        if target_is_dir {
            std::os::windows::fs::symlink_dir(target, link)
        } else {
            std::os::windows::fs::symlink_file(target, link)
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (target, link, target_is_dir);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "symbolic links are not supported on this platform",
        ))
    }
}

/// Create a hard link at `link` sharing the contents of `existing`.
///
/// # Errors
///
/// Returns an error if `existing` does not exist, if `link` already exists, or
/// if the two paths are on different filesystems — which is the usual reason a
/// restore fails to recreate a hard link.
pub fn hard_link(existing: &Path, link: &Path) -> io::Result<()> {
    fs::hard_link(existing, link)
}
