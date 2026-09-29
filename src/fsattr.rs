//! Capture and restoration of platform-specific file attributes.
//!
//! Unix and Windows expose different attribute sets, so everything that
//! cannot be expressed portably is funnelled through this module and compiled
//! per platform. An attribute a platform does not model is reported as `None`,
//! as `0`, or as a no-op rather than as a fabricated value, so a restore never
//! writes a bogus mode or timestamp.

use std::{fs, io, path::Path};

use crate::metadata::Timestamp;

/// Unix permission bits, or `None` on a platform that has none.
///
/// `None` rather than `0`, because "this platform has no permission bits" and "the
/// permissions are the empty set" are different facts and a restore acts on them
/// differently — one is a no-op, the other is a set of permissions nobody has.
/// A `0` could not say which, so it was a sentinel, and a sentinel in a public
/// field is a thing every reader has to be told about.
#[must_use]
pub fn mode_of(meta: &fs::Metadata) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        Some(meta.permissions().mode())
    }

    #[cfg(not(unix))]
    {
        let _ = meta;
        None
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
/// A no-op where `mode` is `None` — which is what a platform without permission
/// bits reports, and what a snapshot taken on one records.
///
/// # Errors
///
/// Returns an error if the path cannot be found, or its permissions cannot be
/// changed — for instance because the caller does not own the file.
pub fn set_mode(path: &Path, mode: Option<u32>) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let Some(mode) = mode else {
            return Ok(());
        };

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
/// The caller does not say whether `target` is a directory, and that is the point:
/// only Windows needs to know, and it is a question about the filesystem rather
/// than about the snapshot being restored. The old signature took a
/// `target_is_dir: bool`, which meant the caller had to answer it on every
/// platform — paying a `stat` per symbolic link to produce a value that unix then
/// discarded with `let _ =`. So a restore of a tree with a thousand links paid a
/// thousand `stat` calls to build an argument nobody read.
///
/// The probe moves here instead, behind a `cfg`, where it is both used and
/// compiled. A relative target is resolved against the *link's* directory, which
/// is what the filesystem will do and what `target.is_dir()` would get wrong.
///
/// # Errors
///
/// Returns an error if `link` already exists, if its parent directory is
/// missing, if the platform does not support symbolic links, or — on Windows —
/// if the caller lacks the privilege required to create one.
pub fn symlink(target: &Path, link: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    {
        if points_at_directory(link, target) {
            std::os::windows::fs::symlink_dir(target, link)
        } else {
            std::os::windows::fs::symlink_file(target, link)
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (target, link);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "symbolic links are not supported on this platform",
        ))
    }
}

/// Whether `target`, as a symbolic link at `link`, names a directory.
///
/// Windows only, and only because it is the only place that has two kinds of
/// symbolic link to choose between. A relative target is resolved against the
/// link's own directory, which is the rule the filesystem applies.
#[cfg(windows)]
fn points_at_directory(link: &Path, target: &Path) -> bool {
    if target.is_absolute() {
        return target.is_dir();
    }

    match link.parent() {
        Some(parent) => parent.join(target).is_dir(),
        None => target.is_dir(),
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
