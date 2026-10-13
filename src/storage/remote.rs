//! An address of a remote store, without a connection.
//!
//! Parsing first, backend later. The `url` crate splits the address into
//! scheme, authority, and path. It knows user info, ports, and IPv6 brackets.
//! This module adds the policy: the scheme allowlist, the bucket shape, and
//! the refusal of passwords in the address. It dials nothing. The connection
//! arrives with the operator backend.
//!
//! Three schemes:
//!
//! - `s3://bucket[/prefix]`.
//! - `sftp://[user@]host[:port][/prefix]`.
//! - `file:///dir` for a local directory through the same spelling.
//!
//! Secrets never live in the address. A password in the user info is refused.
//! Credentials arrive through the environment with the backend.

use std::io;

use url::Url;

/// Scheme of a remote store address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteScheme {
    /// `s3://bucket[/prefix]`.
    S3,
    /// `sftp://[user@]host[:port][/prefix]`.
    Sftp,
    /// `file:///dir`, a local directory through the URL spelling.
    File,
}

impl RemoteScheme {
    /// Scheme for `name`, lowercased by the caller, or `None`.
    fn of(name: &str) -> Option<Self> {
        match name {
            "s3" => Some(Self::S3),
            "sftp" => Some(Self::Sftp),
            "file" => Some(Self::File),
            _ => None,
        }
    }

    /// Spelling of the scheme in an address.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Sftp => "sftp",
            Self::File => "file",
        }
    }
}

/// A parsed remote store address.
///
/// The location means one thing per scheme: a bucket for `s3`, an authority
/// for `sftp`, a directory for `file`. The prefix is
/// a key scope inside the location. It is empty when the address names none.
/// It never starts or ends with `/`. It keeps percent-encoding as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteUrl {
    /// Which backend the address names.
    scheme: RemoteScheme,
    /// Bucket, authority, or directory, per scheme.
    location: String,
    /// Key scope inside the location. May be empty.
    prefix: String,
}

impl RemoteUrl {
    /// Split `address` into scheme, location, and prefix.
    ///
    /// Trims outer whitespace. Refuses empty addresses, unknown schemes,
    /// whitespace inside, queries, fragments, and passwords in the user info.
    ///
    /// # Errors
    ///
    /// Returns an error when `address` is not a remote store address this
    /// crate names.
    pub fn parse(address: &str) -> io::Result<Self> {
        let address = address.trim();

        if address.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no store address: a remote store looks like s3://bucket/backups",
            ));
        }

        // Before the parser: it strips tabs and newlines quietly, and a quiet
        // strip is a correction. An address holding whitespace is refused here
        // instead, where the message names the cause.
        if address.chars().any(char::is_whitespace) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{address:?} holds whitespace, which cannot be in an address"),
            ));
        }

        if address.find("://").is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{address:?} holds no ://: a remote store looks like s3://bucket/backups, \
                     sftp://host/backups, or file:///dir/backups"
                ),
            ));
        }

        let url = Url::parse(address).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{address:?} is not a store address: {e}"),
            )
        })?;

        let scheme = RemoteScheme::of(url.scheme()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{:?} is not a store scheme: use s3, sftp, or file",
                    url.scheme()
                ),
            )
        })?;

        if url.query().is_some() || url.fragment().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{address:?} carries a query or a fragment: an address is a scheme, a location, and a prefix"
                ),
            ));
        }

        if url.password().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the address holds a password: put credentials in the environment, not the address",
            ));
        }

        // The raw authority, as written. The parser checks its shape. The
        // spelling stays raw, so display prints back what the user typed.
        let raw_authority = &address[address.find("://").unwrap_or(0) + 3..]
            .split('/')
            .next()
            .unwrap_or("");

        let (location, prefix) = match scheme {
            RemoteScheme::S3 => split_bucket(&url, raw_authority)?,
            RemoteScheme::Sftp => split_authority(&url, raw_authority, true)?,
            RemoteScheme::File => split_file_path(&url)?,
        };

        Ok(Self {
            scheme,
            location,
            prefix,
        })
    }

    /// Which backend the address names.
    #[must_use]
    pub fn scheme(&self) -> RemoteScheme {
        self.scheme
    }

    /// Split a full set address into its store and its set name.
    ///
    /// A `TARGET` names both halves at once: `s3://bucket/backups/docs` is the
    /// store `s3://bucket/backups` holding the set `docs`, and
    /// `file:///mnt/drive/docs` is the directory `/mnt/drive` holding the set
    /// `docs`. The set is always the last path component, and the store is
    /// everything before it. The set name follows the one-component rule, so
    /// a path cannot escape its staging directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the address names no set (`s3://bucket` with no
    /// path), or when the last component is not a set name.
    pub fn split_set(address: &str) -> io::Result<(Self, String)> {
        let full = Self::parse(address)?;
        let (store, set) = match full.scheme {
            RemoteScheme::File => {
                let path = full.location.trim_end_matches('/');
                let Some(slash) = path.rfind('/') else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{address:?} names no set: append the set name (`{address}/docs`)"),
                    ));
                };
                let (parent, name) = path.split_at(slash);
                let name = &name[1..];
                let parent = if parent.is_empty() { "/" } else { parent };
                if name.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{address:?} names no set: append the set name (`{address}docs`)"),
                    ));
                }
                crate::client::check_set(name)?;
                (
                    Self {
                        scheme: RemoteScheme::File,
                        location: parent.to_string(),
                        prefix: String::new(),
                    },
                    name.to_string(),
                )
            }
            scheme => {
                if full.prefix.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{address:?} names no set: append the set name (`{address}/docs`)"),
                    ));
                }
                let (store_prefix, set) = match full.prefix.rfind('/') {
                    Some(i) => (
                        full.prefix[..i].to_string(),
                        full.prefix[i + 1..].to_string(),
                    ),
                    None => (String::new(), full.prefix.clone()),
                };
                crate::client::check_set(&set)?;
                (
                    Self {
                        scheme,
                        location: full.location.clone(),
                        prefix: store_prefix,
                    },
                    set,
                )
            }
        };
        Ok((store, set))
    }

    /// Bucket, authority, or directory, per scheme.
    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Key scope inside the location. Empty when the address names none.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl std::fmt::Display for RemoteUrl {
    /// The address back in URL spelling.
    ///
    /// Round-trips [`RemoteUrl::parse`]. Holds no secrets: passwords never
    /// parse, so displaying the address leaks nothing.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.scheme.name(), self.location)?;
        if !self.prefix.is_empty() {
            write!(f, "/{}", self.prefix)?;
        }
        Ok(())
    }
}

/// Split the `s3://` authority into bucket and prefix.
///
/// The bucket is the authority whole. The prefix is the path without leading
/// or trailing slashes.
fn split_bucket(url: &Url, raw_authority: &str) -> io::Result<(String, String)> {
    if raw_authority.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "s3:// names no bucket: a remote store looks like s3://bucket/backups",
        ));
    }

    let bucket_ok = raw_authority
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.');
    if !bucket_ok {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{raw_authority:?} is not a bucket: use lowercase letters, digits, dots, and dashes"
            ),
        ));
    }

    if raw_authority.bytes().any(|b| b.is_ascii_uppercase()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{raw_authority:?} is not a bucket: buckets use lowercase letters"),
        ));
    }

    let _ = url.host_str();
    Ok((raw_authority.to_string(), clean_prefix(url.path())?))
}

/// Split a `host[:port][/prefix]` authority into authority and prefix.
///
/// `user_info` allows one `user@` before the host, for `sftp`. The password
/// check already ran: anything after a colon in the user info fails there.
/// An `@` with no user fails here.
fn split_authority(
    url: &Url,
    raw_authority: &str,
    user_info: bool,
) -> io::Result<(String, String)> {
    if raw_authority.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the address names no host: a remote store looks like sftp://host/backups",
        ));
    }

    if raw_authority.contains('@') {
        if !user_info {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{raw_authority:?} holds user info, which this scheme refuses: put \
                     credentials in the environment, not the address"
                ),
            ));
        }
        if url.username().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{raw_authority:?} names no user before the @"),
            ));
        }
    }

    if url.host_str().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{raw_authority:?} names no host"),
        ));
    }

    Ok((raw_authority.to_string(), clean_prefix(url.path())?))
}

/// Split a `file://` address into directory and empty prefix.
///
/// The path is the location whole. Prefix scoping for local directories
/// stays future work.
fn split_file_path(url: &Url) -> io::Result<(String, String)> {
    if url.path().is_empty() || url.path() == "/" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file:// names no directory: a local store looks like file:///dir/backups",
        ));
    }

    if url.host_str().is_some_and(|host| !host.is_empty()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file:// names a local directory, not a host: use file:///dir/backups",
        ));
    }

    Ok((url.path().to_string(), String::new()))
}

/// Prefix without leading or trailing slashes.
///
/// Refuses `//` inside: an empty segment reads as a different scope on some
/// backends and as the same scope on others. One spelling only.
fn clean_prefix(path: &str) -> io::Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.contains("//") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{path:?} holds an empty segment: use single slashes between names"),
        ));
    }

    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::{RemoteScheme, RemoteUrl};

    /// Each scheme splits into location and prefix. Display prints the same
    /// address back. Strings only, no backends, no network.
    #[test]
    fn addresses_split_into_scheme_location_and_prefix() {
        let cases = [
            ("s3://bucket/backups", RemoteScheme::S3, "bucket", "backups"),
            ("s3://bucket", RemoteScheme::S3, "bucket", ""),
            ("s3://bucket/a/b", RemoteScheme::S3, "bucket", "a/b"),
            ("sftp://host/backups", RemoteScheme::Sftp, "host", "backups"),
            (
                "sftp://user@host:22/a",
                RemoteScheme::Sftp,
                "user@host:22",
                "a",
            ),
            (
                "sftp://user@[::1]:22/a",
                RemoteScheme::Sftp,
                "user@[::1]:22",
                "a",
            ),
            (
                "file:///dir/backups",
                RemoteScheme::File,
                "/dir/backups",
                "",
            ),
            ("S3://bucket/a", RemoteScheme::S3, "bucket", "a"),
        ];

        for (address, scheme, location, prefix) in cases {
            let parsed = RemoteUrl::parse(address).unwrap();
            assert_eq!(parsed.scheme(), scheme, "{address}");
            assert_eq!(parsed.location(), location, "{address}");
            assert_eq!(parsed.prefix(), prefix, "{address}");
            assert_eq!(
                parsed.to_string().to_ascii_lowercase(),
                address.to_ascii_lowercase()
            );
        }
    }

    /// Malformed addresses fail with `InvalidInput`. Each case names its own
    /// reason. Strings only.
    #[test]
    fn malformed_addresses_are_refused() {
        for address in [
            "",
            "   ",
            "bucket/backups",
            "ftp://host/backups",
            "s3://buck et/a",
            "s3://bucket/a?x=1",
            "s3://bucket/a#frag",
            "s3:///a",
            "s3://Bucket/a",
            "s3://bucket/a//b",
            "s3://host:99999/a",
            "sftp://user:pass@host/a",
            "sftp://@host/a",
            "sftp:///a",
            "file://",
            "file://host/dir",
        ] {
            let error = RemoteUrl::parse(address).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidInput,
                "{address:?} must be refused"
            );
        }
    }

    /// A full set address splits into its store and its set. The store keeps
    /// everything but the last component. Strings only.
    #[test]
    fn set_addresses_split_into_store_and_set() {
        let (store, set) = RemoteUrl::split_set("s3://bucket/backups/docs").unwrap();
        assert_eq!(store.to_string(), "s3://bucket/backups");
        assert_eq!(set, "docs");

        let (store, set) = RemoteUrl::split_set("s3://bucket/docs").unwrap();
        assert_eq!(store.to_string(), "s3://bucket");
        assert_eq!(set, "docs");

        let (store, set) = RemoteUrl::split_set("file:///mnt/drive/docs").unwrap();
        assert_eq!(store.to_string(), "file:///mnt/drive");
        assert_eq!(set, "docs");

        for address in ["s3://bucket", "s3://bucket/", "file:///"] {
            assert!(
                RemoteUrl::split_set(address).is_err(),
                "{address:?} names no set"
            );
        }
        assert!(RemoteUrl::split_set("s3://bucket/a/b/c").is_ok());
    }
}
