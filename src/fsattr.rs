//! Capture and restoration of platform-specific file attributes.
//!
//! Unix and Windows expose different attribute sets, so every attribute that
//! cannot be expressed portably goes through this module. The build compiles
//! this module once per platform. A platform that does not model an attribute
//! reports it as `None`, as `0`, or as a no-op. The module never reports a
//! fabricated value. A restore never writes a bogus mode or timestamp.

use std::{fs, io, path::Path};

use crate::metadata::Timestamp;

/// Unix permission bits, or `None` on a platform that has none.
///
/// `None` rather than `0`. "This platform has no permission bits" and "the
/// permissions are the empty set" are different facts. A restore acts on them
/// differently. One is a no-op. The other is a set of permissions nobody has.
/// A `0` could not say which. A `0` was a sentinel instead. A sentinel in a
/// public field is a thing that every reader has to be told about.
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

/// The user and group that own a file, or `None` where the platform has neither.
///
/// A pair rather than two functions. They come from one `stat`, and the code
/// records them together or not at all. An entry that has an owner has a group.
/// Splitting them would let a caller record one and not the other.
///
/// `None` on a platform with no users or groups, which is Windows. There is no
/// sensible substitute. A made-up id would be a claim about the filesystem.
/// The filesystem never made that claim.
#[must_use]
pub fn owner_of(meta: &fs::Metadata) -> Option<(u32, u32)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Some((meta.uid(), meta.gid()))
    }

    #[cfg(not(unix))]
    {
        let _ = meta;
        None
    }
}

/// Apply an owner and group captured by [`owner_of`].
///
/// A no-op where `owner` is `None`, which is what a platform without users reports
/// and what a snapshot taken on one records.
///
/// **Order matters, and this has to come before the mode.** On Linux, changing a
/// file's owner clears its set-user-ID and set-group-ID bits. A caller that sets
/// the mode first and the owner second silently strips setuid from everything it
/// restores. [`Attributes::apply`](crate::metadata::Attributes::apply) enforces
/// that order. A caller that applies attributes by hand is the risk.
///
/// Symbolic links are skipped, like [`set_mtime`]. Their owner is a property of
/// the link itself. Only `lchown` could set it, and a backup does not restore it.
/// A snapshot records a link's *target*. Following the link to change the owner
/// of its destination would be a different and much larger claim.
///
/// # Errors
///
/// Returns an error if the path cannot be found, or the owner cannot be
/// changed. For an unprivileged caller, a failed change is the ordinary case.
/// That failure is not a problem with the path.
pub fn set_owner(path: &Path, owner: Option<(u32, u32)>) -> io::Result<()> {
    let Some((uid, gid)) = owner else {
        return Ok(());
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::chown;

        if fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Ok(());
        }

        chown(path, Some(uid), Some(gid))
    }

    #[cfg(not(unix))]
    {
        let _ = (path, uid, gid);
        Ok(())
    }
}

/// Apply permission bits captured by [`mode_of`].
///
/// A no-op where `mode` is `None`. A platform without permission bits reports
/// `None`. A snapshot taken on one records `None`.
///
/// # Errors
///
/// Returns an error if the path cannot be found, or if its permissions cannot be
/// changed. One cause is that the caller does not own the file.
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
/// The caller does not say whether `target` is a directory, and that is the
/// point. Only Windows needs to know. The question is about the filesystem, not
/// about the snapshot being restored. The old signature took a
/// `target_is_dir: bool`. That signature required the caller to answer the
/// question on every platform. The caller paid a `stat` per symbolic link. The
/// caller used the result to build a value that unix then discarded with
/// `let _ =`. So a restore of a tree with a thousand links paid a thousand
/// `stat` calls to build an argument nobody read.
///
/// The probe moves here instead, behind a `cfg`, where it is both used and
/// compiled. A relative target is resolved against the *link's* directory, which
/// is what the filesystem will do and what `target.is_dir()` would get wrong.
///
/// # Errors
///
/// Returns an error if `link` already exists. The call also fails if the parent
/// directory is missing, or the platform does not support symbolic links. On
/// Windows, the call fails if the caller lacks the privilege that creates one.
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
/// Returns an error if `existing` does not exist, or if `link` already exists.
/// The call also fails if the two paths are on different filesystems. That layout
/// is the usual reason a restore fails to recreate a hard link.
pub fn hard_link(existing: &Path, link: &Path) -> io::Result<()> {
    fs::hard_link(existing, link)
}
