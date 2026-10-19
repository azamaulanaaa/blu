//! Where a backup set lives: a local path, a serve set, or a keyed store.
//!
//! One `TARGET` names all three. A local path (`photos`) names a set on this
//! machine. An `http://` or `https://` address names a set on a `blu serve`.
//! Any other `://` address names a set in a keyed store (`file://`, `sftp://`,
//! `ftp://`, `s3://`). Remote targets hold no local copy. A store target works
//! through a staging copy. A serve target works on the server.

use std::{
    io,
    path::{Path, PathBuf},
};

use crate::{
    client::Remote,
    storage::{self, RemoteUrl},
};

/// The set a command works on, and where it lives.
///
/// Parse one with [`Target::parse`]. Chain staging and token. Call `resolve`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A path on this machine.
    Local(PathBuf),
    /// A set on a `blu serve`.
    Serve {
        /// The server's address, without the set.
        address: String,
        /// The set's name there.
        set: String,
        /// The bearer token a proxy guards with, if any.
        token: Option<String>,
    },
    /// A set in a keyed store, with its staging copy.
    Store(StoreTarget),
}

/// A set held by a keyed store, and the staging copy.
///
/// Resolution only. Staged runners pull into staging. They run the local
/// command there. They push back after a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreTarget {
    /// The store holding the set, without the set.
    pub url: RemoteUrl,
    /// The set name on that store.
    pub set: String,
    /// The staging path on this machine.
    pub staging: PathBuf,
}

/// Parse a `TARGET` string into where the set lives.
///
/// Start with [`Target::parse`]. Name the command for error messages.
/// Add staging for store targets. Add a token for serve targets.
/// Call `resolve`.
///
/// The token travels beside the address, never inside it.
#[derive(Debug, Clone)]
pub struct TargetBuilder<'a> {
    raw: &'a str,
    command: &'a str,
    staging: Option<&'a Path>,
    token: Option<&'a str>,
}

impl Target {
    /// Start parsing `raw` as a target.
    ///
    /// The builder assumes no staging and no token. Add both below.
    /// The command name defaults to `blu`. Set it for clearer errors.
    #[must_use]
    pub fn parse(raw: &str) -> TargetBuilder<'_> {
        TargetBuilder {
            raw,
            command: "blu",
            staging: None,
            token: None,
        }
    }
}

impl<'a> TargetBuilder<'a> {
    /// Name the calling command in errors.
    ///
    /// Errors name the flag the caller misused. The command names it fully.
    #[must_use]
    pub fn command(mut self, command: &'a str) -> Self {
        self.command = command;
        self
    }

    /// Stage store copies under `dir`.
    ///
    /// Without it the system cache holds the copy. Refused for local and
    /// serve targets. Without a store there is no copy to stage.
    #[must_use]
    pub fn staging(mut self, dir: &'a Path) -> Self {
        self.staging = Some(dir);
        self
    }

    /// Forward this bearer token to the server.
    ///
    /// The client sends it as-is. A proxy before the server checks it.
    #[must_use]
    pub fn token(mut self, token: &'a str) -> Self {
        self.token = Some(token);
        self
    }

    /// Resolve the target.
    ///
    /// An `http://` or `https://` address splits into server and set.
    /// Any other `://` address splits into store and set.
    /// Anything else is a local path.
    ///
    /// # Errors
    ///
    /// Returns an error when the address is malformed.
    /// Returns an error when staging is set without a store target.
    /// Returns an error when no cache directory exists and no staging is set.
    pub fn resolve(self) -> io::Result<Target> {
        let command = self.command;
        let raw = self.raw;
        if raw.contains("://") {
            if raw.trim().to_ascii_lowercase().starts_with("http") {
                if self.staging.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "`blu {command} --staging` needs a store target. An `http://` or `https://` target works on the server and stages nothing here"
                        ),
                    ));
                }
                let (address, set) = crate::client::split_serve_target(raw)?;
                return Ok(Target::Serve {
                    address,
                    set,
                    token: self.token.map(str::to_string),
                });
            }
            let (url, set) = RemoteUrl::split_set(raw)?;
            let root = match self.staging {
                Some(dir) => dir.to_path_buf(),
                None => storage::default_cache_root().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "no cache directory holds a staging copy on this machine: pass `--staging DIR`",
                    )
                })?,
            };
            let staging = storage::staging_set_path(&root, &url, &set)?;
            return Ok(Target::Store(StoreTarget { url, set, staging }));
        }
        if self.staging.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "`blu {command} --staging` needs a store target. A local path stages nothing"
                ),
            ));
        }
        Ok(Target::Local(PathBuf::from(raw)))
    }
}

/// Build a serve client for `address`, with a bearer token when one is set.
///
/// The token travels as `Authorization: Bearer <token>`.
/// The client never checks it. A proxy before the server checks it.
///
/// # Errors
///
/// Returns an error when `address` is not a server address.
/// Returns an error when the token is empty or holds whitespace.
pub fn remote_for(address: &str, token: Option<&str>) -> io::Result<Remote> {
    match token {
        Some(value) => Remote::with_token(address, value),
        None => Remote::new(address),
    }
}

#[cfg(test)]
mod tests {
    use super::Target;

    /// A local path resolves without staging and without a token.
    #[test]
    fn target_builder_resolves_local_path() {
        let target = Target::parse("photos").resolve().unwrap();
        assert_eq!(target, Target::Local("photos".into()));
    }

    /// Staging with a local path is refused, and the error names the command.
    #[test]
    fn target_builder_refuses_staging_for_local_path() {
        let error = Target::parse("photos")
            .command("backup")
            .staging("stage".as_ref())
            .resolve()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("backup"),
            "the error names the command, got: {error}"
        );
    }

    /// A serve address splits into address and set, and keeps the token.
    #[test]
    fn target_builder_splits_serve_target_and_keeps_token() {
        let target = Target::parse("http://127.0.0.1:8480/docs")
            .token("secret")
            .resolve()
            .unwrap();
        assert_eq!(
            target,
            Target::Serve {
                address: "http://127.0.0.1:8480".to_string(),
                set: "docs".to_string(),
                token: Some("secret".to_string()),
            }
        );
    }

    /// A store address resolves into a staged target under the given dir.
    #[test]
    fn target_builder_stages_store_target() {
        let target = Target::parse("file:///mnt/drive/docs")
            .staging("stage".as_ref())
            .resolve()
            .unwrap();
        let Target::Store(stored) = target else {
            panic!("a file:// address is a store target, got: {target:?}");
        };
        assert_eq!(stored.set, "docs");
        assert!(stored.staging.starts_with("stage"));
    }
}
