//! Capture and restoration of platform-specific file attributes.
//!
//! Unix and Windows expose different attribute sets, so everything that
//! cannot be expressed portably is funnelled through this module and compiled
//! per platform. Attributes that a platform does not model are reported as
//! `None`/no-ops rather than silently recorded as zero, so a restore never
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
            // Sub-second precision is below one second by definition, so this
            // always fits; the fallback only covers a platform that disagrees.
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
/// A no-op where the platform has no equivalent, and where `mode` carries no
/// information.
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
pub fn set_mtime(path: &Path, mtime: Timestamp) -> io::Result<()> {
    let time = filetime::FileTime::from_unix_time(mtime.secs, mtime.nanos);

    // A symlink's own timestamp is not meaningful, and updating it would
    // follow the link and touch the target instead.
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Ok(());
    }

    filetime::set_file_mtime(path, time)
}

/// Create a symbolic link at `link` pointing at `target`.
///
/// `target_is_dir` selects the Windows flavour of symlink, which has no
/// equivalent on other platforms.
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
pub fn hard_link(existing: &Path, link: &Path) -> io::Result<()> {
    fs::hard_link(existing, link)
}
