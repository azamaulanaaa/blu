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

/// Whether an xattr name belongs in a snapshot.
///
/// The backup keeps `user` attributes and POSIX access lists. It skips
/// `security` and `trusted` and other `system` names. Those need privilege to
/// read and to write. A hostile snapshot carrying `security.capability` would
/// hand capabilities to restored files. The allowlist keeps that out.
#[cfg(unix)]
fn keep_xattr(name: &[u8]) -> bool {
    name.starts_with(b"user.")
        || name == b"system.posix_acl_access"
        || name == b"system.posix_acl_default"
}

/// List the extended attributes of `path`.
///
/// Returns what the filesystem reports under the internal allowlist.
/// Returns an empty list on filesystems without attribute support. Returns an
/// empty list for symbolic links. A link records its target only. The target
/// carries its own entry with its own attributes.
///
/// The list is sorted by name. The same tree always encodes to the same bytes.
/// Unreadable entries are skipped. A file that loses one attribute between
/// listing and reading keeps the rest.
#[must_use]
pub fn xattrs_of(path: &Path) -> Vec<crate::metadata::XAttr> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        if fs::symlink_metadata(path).map_or(true, |meta| meta.file_type().is_symlink()) {
            return Vec::new();
        }
        let Ok(names) = xattr::list(path) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for name in names {
            let raw = name.into_vec();
            if !keep_xattr(&raw) {
                continue;
            }
            if let Ok(Some(value)) = xattr::get(path, std::ffi::OsStr::from_bytes(&raw)) {
                out.push(crate::metadata::XAttr { name: raw, value });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Vec::new()
    }
}

/// Read the Windows access list of `path`.
///
/// Returns the explicit allow and deny entries. Returns `None` where the file
/// carries inherited entries only. Returns `None` on platforms without access
/// lists. Other entry kinds are skipped. The restore only writes allow and
/// deny entries.
#[must_use]
pub fn windows_acl_of(path: &Path) -> Option<crate::metadata::WindowsAcl> {
    #[cfg(windows)]
    {
        let text = path.to_str()?;
        let list = windows_acl::acl::ACL::from_file_path(text, false).ok()?;
        let entries = list.all().ok()?;
        let mut aces = Vec::new();
        for entry in entries {
            if entry.flags & winapi::um::winnt::INHERITED_ACE != 0 {
                continue;
            }
            let ace_type = match entry.entry_type {
                windows_acl::acl::AceType::AccessAllow => 0,
                windows_acl::acl::AceType::AccessDeny => 1,
                _ => continue,
            };
            aces.push(crate::metadata::WindowsAce {
                sid: entry.string_sid,
                ace_type,
                flags: entry.flags,
                mask: entry.mask,
            });
        }
        if aces.is_empty() {
            return None;
        }
        aces.sort_by(|a, b| (&a.sid, a.ace_type).cmp(&(&b.sid, b.ace_type)));
        Some(crate::metadata::WindowsAcl { aces })
    }

    #[cfg(not(windows))]
    {
        let _ = path;
        None
    }
}

/// Apply extended attributes to `path`.
///
/// Writes plain attributes before access lists. Linux rewrites the access
/// mask when the mode changes. An access list written before the mode would
/// not survive it. The mode is already set by the caller. The list goes last.
///
/// Does nothing where the list is empty. Symbolic links are skipped. Their
/// target carries its own entry.
///
/// # Errors
///
/// Returns an error if the path is missing. Returns an error if any value
/// cannot be set on this filesystem.
pub fn set_xattrs(path: &Path, xattrs: &[crate::metadata::XAttr]) -> io::Result<()> {
    if xattrs.is_empty() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Ok(());
        }
        let mut ordered: Vec<&crate::metadata::XAttr> = xattrs.iter().collect();
        ordered.sort_by_key(|attr| attr.is_acl());
        for attr in ordered {
            xattr::set(path, std::ffi::OsStr::from_bytes(&attr.name), &attr.value)?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _ = (path, xattrs);
        Ok(())
    }
}

/// Apply a Windows access list to `path`.
///
/// Replaces the explicit entries with the recorded ones. Inherited entries
/// stay as the parent provides them. The write marks the list protected, as
/// the underlying call always does. Restored files block inheritance.
///
/// # Errors
///
/// Returns an error if the path is missing. Returns an error if any entry
/// cannot be set on this filesystem.
pub fn set_windows_acl(path: &Path, acl: &crate::metadata::WindowsAcl) -> io::Result<()> {
    #[cfg(windows)]
    {
        use windows_acl::acl::AceType;
        let text = path.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a Windows path that is not valid text cannot carry an access list",
            )
        })?;
        let mut list = windows_acl::acl::ACL::from_file_path(text, false).map_err(|code| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "could not read the access list of {}: {code}",
                    path.display()
                ),
            )
        })?;
        let current = list.all().map_err(|code| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "could not list the access list of {}: {code}",
                    path.display()
                ),
            )
        })?;
        for entry in &current {
            if entry.flags & winapi::um::winnt::INHERITED_ACE != 0 {
                continue;
            }
            let kind = match entry.entry_type {
                AceType::AccessAllow | AceType::AccessDeny => Some(entry.entry_type),
                _ => None,
            };
            let Some(kind) = kind else { continue };
            let raw = windows_acl::helper::string_to_sid(&entry.string_sid).map_err(|code| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("could not parse a SID on {}: {code}", path.display()),
                )
            })?;
            let _ = list.remove_entry(raw.as_ptr() as winapi::um::winnt::PSID, Some(kind), None);
        }
        for ace in &acl.aces {
            let kind = match ace.ace_type {
                0 => AceType::AccessAllow,
                _ => AceType::AccessDeny,
            };
            let raw = windows_acl::helper::string_to_sid(&ace.sid).map_err(|code| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("could not parse a SID on {}: {code}", path.display()),
                )
            })?;
            list.add_entry(
                raw.as_ptr() as winapi::um::winnt::PSID,
                kind,
                ace.flags,
                ace.mask,
            )
            .map_err(|code| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "could not set the access list of {}: {code}",
                        path.display()
                    ),
                )
            })?;
        }
        Ok(())
    }

    #[cfg(not(windows))]
    {
        let _ = (path, acl);
        Ok(())
    }
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
