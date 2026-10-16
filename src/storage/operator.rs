// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Keyed backends over the network, through `OpenDAL`.
//!
//! Three schemes share one trait implementation.
//! The operator speaks all three schemes on unix.
//! On other platforms the operator speaks `s3://` and `ftp://`.
//! `sftp://` needs `openssh`.
//! `openssh` works on unix only.
//! On other platforms `connect` fails with `Unsupported`.
//! The failure occurs before the operator reads any configuration.
//! [`crate::storage::AsyncObjectStore`] is the only shape the mirror needs.
//!
//! Configuration arrives through the environment.
//! One variable names each endpoint or secret.
//! Nothing is guessed.
//! A missing endpoint or identity fails before the first byte moves.
//!
//! The S3 transport speaks plain HTTP and verified HTTPS.
//! It verifies HTTPS against the system roots.
//! `ftp://` and `sftp://` never touch HTTP.
//!
//! Durability differs per service.
//! S3 finishes a write atomically on close.
//! `FTP` uploads are best-effort mid-transfer.
//! A torn object fails the run.
//! The next run reports the torn object rather than merging with it.
//! The local staging copy stays atomic either way.
//! Downloads land beside the target and rename into place.
//! A crash loses the fresh copy.
//! The next pull re-fetches the copy.
//!
//! Retries and timeouts wrap every operator.
//! Each operator tries three times.
//! Each operation allows thirty seconds.
//! Each stream allows thirty seconds.

use std::io;

use opendal::{Operator, layers, services};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{AsyncObjectStore, RemoteScheme, RemoteUrl};

/// Chunk for streaming imports and exports, in bytes.
///
/// Bounded and fixed. A large segment crosses in pieces of this size, so
/// memory stays flat whatever the object holds.
const COPY_CHUNK: u64 = 4 * 1024 * 1024;

/// Build an operator for `url`.
///
/// The call reads endpoints and secrets from the environment.
///
/// Use these variables per scheme.
///
/// - `s3://bucket[/prefix]` uses `BLU_S3_ENDPOINT` for the endpoint.
///   The endpoint is required.
///   It accepts `http://` and `https://`.
///   It verifies HTTPS against the system roots.
///   `BLU_S3_REGION` overrides the region.
///   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` name the identity.
///   The two travel together.
///   One without the other fails.
///   `AWS_SESSION_TOKEN` extends them.
///   Without any of the three the request goes unsigned.
///   Public buckets answer unsigned requests.
///   Private buckets refuse unsigned requests.
/// - `ftp://[user@]host[:port][/prefix]` uses the address user or `BLU_FTP_USER`.
///   The user defaults to `anonymous`.
///   `BLU_FTP_PASSWORD` names the password.
///   The password defaults to empty.
/// - `sftp://[user@]host[:port][/prefix]` uses the address user or `BLU_SFTP_USER`.
///   The user is required.
///   `BLU_SFTP_KEY` names the key file and is required.
///   This backend speaks key auth only.
///   Host keys stay strict.
///   This backend works on unix only.
///   On other platforms `connect` fails with `Unsupported`.
///
/// The call returns `InvalidInput` and names the missing piece.
/// The call never guesses.
///
/// # Errors
///
/// The call fails when configuration is missing.
/// The call fails when the operator refuses the configuration.
/// Building touches no network.
/// The connection stays lazy until the first operation.
pub fn connect(url: &RemoteUrl) -> io::Result<Operator> {
    opendal::init_default_registry();
    super::tls::install()?;

    let (scheme, config) = config_for(url)?;
    let operator =
        Operator::via_iter(scheme, config).map_err(|e| map_err("build the operator", &e))?;

    Ok(operator
        .layer(layers::RetryLayer::new().with_max_times(3))
        .layer(
            layers::TimeoutLayer::new()
                .with_timeout(std::time::Duration::from_secs(30))
                .with_io_timeout(std::time::Duration::from_secs(30)),
        ))
}

/// Scheme name and key-value configuration for `url`.
fn config_for(url: &RemoteUrl) -> io::Result<(&'static str, Vec<(String, String)>)> {
    match url.scheme() {
        RemoteScheme::S3 => Ok((services::S3_SCHEME, s3_config(url)?)),
        RemoteScheme::Ftp => Ok((services::FTP_SCHEME, ftp_config(url))),
        #[cfg(unix)]
        RemoteScheme::Sftp => Ok((services::SFTP_SCHEME, sftp_config(url)?)),
        // TODO(sftp-windows): support sftp:// on Windows by bypassing opendal
        // (blocked upstream: apache/opendal#2963) with a cross-platform
        // backend such as russh + russh-sftp implementing AsyncObjectStore.
        #[cfg(not(unix))]
        RemoteScheme::Sftp => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "sftp:// needs a unix build. This build speaks file://, ftp://, and s3:// only",
        )),
        RemoteScheme::File => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file:// stores need no operator: they are local directories",
        )),
    }
}

/// Root for `prefix`: the operator confines every key under it.
fn root_of(prefix: &str) -> String {
    if prefix.is_empty() {
        "/".to_string()
    } else {
        format!("{prefix}/")
    }
}

/// Value of `name`, or `None` when unset or empty.
///
/// Empty counts as unset: an exported-but-empty variable is a typo, not a
/// value.
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Build S3 configuration from the address and the environment.
///
/// The bucket comes from the address.
/// The endpoint and the identity come from the environment.
fn s3_config(url: &RemoteUrl) -> io::Result<Vec<(String, String)>> {
    let endpoint = env("BLU_S3_ENDPOINT").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "BLU_S3_ENDPOINT names the S3 endpoint and is required, e.g. \
             http://127.0.0.1:9000 for a local MinIO",
        )
    })?;

    s3_config_for(url, &endpoint)
}

/// Build S3 configuration for an explicit endpoint.
///
/// The splitter keeps this pure for tests.
/// Tests cover the shape without touching the process environment.
/// Rust forbids environment mutation outside `unsafe`.
fn s3_config_for(url: &RemoteUrl, endpoint: &str) -> io::Result<Vec<(String, String)>> {
    let mut config = vec![
        ("bucket".to_string(), url.location().to_string()),
        ("endpoint".to_string(), endpoint.to_string()),
        ("root".to_string(), root_of(url.prefix())),
    ];

    if let Some(region) = env("BLU_S3_REGION") {
        config.push(("region".to_string(), region));
    }

    match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
        (Some(id), Some(secret)) => {
            config.push(("access_key_id".to_string(), id));
            config.push(("secret_access_key".to_string(), secret));
            if let Some(token) = env("AWS_SESSION_TOKEN") {
                config.push(("session_token".to_string(), token));
            }
        }
        (None, None) => {}
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY travel together. One without the other fails",
            ));
        }
    }

    Ok(config)
}

/// Build FTP configuration.
///
/// The host comes from the address.
/// The identity comes from the address or the environment.
/// The user defaults to anonymous.
fn ftp_config(url: &RemoteUrl) -> Vec<(String, String)> {
    let (user, host) = split_user(url.location());

    let user = user
        .map(str::to_string)
        .or_else(|| env("BLU_FTP_USER"))
        .unwrap_or_else(|| "anonymous".to_string());
    let password = env("BLU_FTP_PASSWORD").unwrap_or_default();

    vec![
        ("endpoint".to_string(), format!("ftp://{host}")),
        ("root".to_string(), root_of(url.prefix())),
        ("user".to_string(), user),
        ("password".to_string(), password),
    ]
}

/// Build SFTP configuration.
///
/// The host comes from the address.
/// The identity is required.
/// This backend speaks key auth only.
/// Unix builds include this backend.
/// Other builds exclude it.
#[cfg(unix)]
fn sftp_config(url: &RemoteUrl) -> io::Result<Vec<(String, String)>> {
    let (user, host) = split_user(url.location());

    let user = user
        .map(str::to_string)
        .or_else(|| env("BLU_SFTP_USER"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "sftp:// names no user. Put one in the address (sftp://user@host) or in BLU_SFTP_USER",
            )
        })?;
    let key = env("BLU_SFTP_KEY").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "BLU_SFTP_KEY names the key file and is required. This backend speaks key auth only",
        )
    })?;

    Ok(vec![
        ("endpoint".to_string(), host.to_string()),
        ("root".to_string(), root_of(url.prefix())),
        ("user".to_string(), user),
        ("key".to_string(), key),
    ])
}

/// Split `[user@]authority` at the last `@`.
///
/// Usernames carry no raw `@`.
/// One `@` in the address is percent-encoded.
/// The last `@` is the separator.
/// The call returns no user when none is there.
fn split_user(authority: &str) -> (Option<&str>, &str) {
    match authority.rsplit_once('@') {
        Some((user, host)) => (Some(user), host),
        None => (None, authority),
    }
}

/// Refuse keys that are not bare file names.
///
/// The mirror passes grammar-checked names only.
/// A slash would leave the root on a naive backend.
fn check_key(key: &str) -> io::Result<()> {
    if key.is_empty() || key == "." || key == ".." || key.contains('/') || key.contains('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{key:?} is not an object key. A key is a file name"),
        ));
    }
    Ok(())
}

/// Build an `io::Error` for an operator failure.
///
/// The mapper keeps the names the mirror matches.
/// Missing stays missing.
/// Refused auth stays denied.
/// Bad configuration stays invalid input.
fn map_err(doing: &str, error: &opendal::Error) -> io::Error {
    let kind = match error.kind() {
        opendal::ErrorKind::NotFound => io::ErrorKind::NotFound,
        opendal::ErrorKind::AlreadyExists => io::ErrorKind::AlreadyExists,
        opendal::ErrorKind::PermissionDenied => io::ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => io::ErrorKind::Unsupported,
        opendal::ErrorKind::ConfigInvalid => io::ErrorKind::InvalidInput,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("could not {doing}: {error}"))
}

impl AsyncObjectStore for Operator {
    async fn keys(&self) -> io::Result<Vec<String>> {
        let entries = self
            .list("/")
            .await
            .map_err(|e| map_err("list the store", &e))?;

        let mut out = Vec::new();
        for entry in entries {
            let name = entry.name();
            if name.is_empty() || name.ends_with('/') {
                continue;
            }
            out.push(name.to_string());
        }
        out.sort();
        Ok(out)
    }

    async fn stat(&self, key: &str) -> io::Result<Option<u64>> {
        check_key(key)?;
        match self.stat(key).await {
            Ok(meta) => Ok(Some(meta.content_length())),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(map_err(&format!("measure object {key:?}"), &e)),
        }
    }

    async fn read_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        check_key(key)?;
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("range {offset}..{offset}+{len} of object {key:?} overflows"),
            )
        })?;

        let data = self
            .read_with(key)
            .range(offset..end)
            .await
            .map_err(|e| map_err(&format!("read object {key:?}"), &e))?
            .to_vec();
        let read = u64::try_from(data.len()).unwrap_or(u64::MAX);
        if read != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("object {key:?} ends at {read} bytes into a {len}-byte read"),
            ));
        }
        Ok(data)
    }

    async fn write_bytes(&self, key: &str, data: &[u8]) -> io::Result<()> {
        check_key(key)?;
        self.write(key, data.to_vec())
            .await
            .map_err(|e| map_err(&format!("write object {key:?}"), &e))?;
        Ok(())
    }

    async fn import_file(&self, key: &str, path: &std::path::Path) -> io::Result<u64> {
        check_key(key)?;
        let mut file = tokio::fs::File::open(path).await.map_err(|e| {
            io::Error::new(e.kind(), format!("could not open {}: {e}", path.display()))
        })?;
        let mut writer = self
            .writer(key)
            .await
            .map_err(|e| map_err(&format!("open object {key:?} for writing"), &e))?;

        let mut total = 0u64;
        let mut chunk = super::zeroed(COPY_CHUNK, "a copy chunk")?;
        loop {
            let read = file.read(&mut chunk).await.map_err(|e| {
                io::Error::new(e.kind(), format!("could not read {}: {e}", path.display()))
            })?;
            if read == 0 {
                break;
            }
            writer
                .write(chunk[..read].to_vec())
                .await
                .map_err(|e| map_err(&format!("write object {key:?}"), &e))?;
            total = total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        }
        writer
            .close()
            .await
            .map_err(|e| map_err(&format!("finish object {key:?}"), &e))?;
        Ok(total)
    }

    async fn export_file(&self, key: &str, path: &std::path::Path) -> io::Result<u64> {
        check_key(key)?;
        // Services refuse a range past the end rather than shortening it,
        // so the loop clamps its last chunk to the measured length.
        let length = match self.stat(key).await {
            Ok(meta) => Some(meta.content_length()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => None,
            Err(e) => return Err(map_err(&format!("measure object {key:?}"), &e)),
        }
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("object {key:?} is not in this store"),
            )
        })?;
        let reader = self
            .reader(key)
            .await
            .map_err(|e| map_err(&format!("open object {key:?} for reading"), &e))?;

        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("could not create {}: {e}", parent.display()),
                )
            })?;
        }
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        let tmp = tempfile::NamedTempFile::new_in(dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not create temporary in {}: {e}", dir.display()),
            )
        })?;
        let tmp_path = tmp.path().to_path_buf();
        let (file, temp_path) = tmp.into_parts();
        let mut dest = tokio::fs::File::from_std(file);

        let mut written = 0u64;
        let mut offset = 0u64;
        while offset < length {
            let end = offset.saturating_add(COPY_CHUNK).min(length);
            let chunk = reader
                .read(offset..end)
                .await
                .map_err(|e| map_err(&format!("read object {key:?}"), &e))?
                .to_vec();
            if chunk.is_empty() {
                break;
            }
            dest.write_all(&chunk).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("could not write {}: {e}", tmp_path.display()),
                )
            })?;
            offset = offset.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            written = offset;
        }
        dest.sync_all().await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not sync {}: {e}", tmp_path.display()),
            )
        })?;
        drop(dest);
        temp_path.persist(path).map_err(|e| {
            io::Error::new(
                e.error.kind(),
                format!(
                    "could not rename temporary into {}: {}",
                    path.display(),
                    e.error
                ),
            )
        })?;
        Ok(written)
    }
}

/// Build a memory operator for tests.
///
/// No network.
/// No credentials.
/// No filesystem.
/// Mirror round-trips run against it as against a service.
#[cfg(test)]
fn memory() -> io::Result<Operator> {
    Operator::via_iter(services::MEMORY_SCHEME, Vec::new())
        .map_err(|e| map_err("build the memory operator", &e))
}

#[cfg(test)]
mod tests {
    use super::{connect, memory, s3_config_for};
    use crate::storage::{MirrorStats, RemoteUrl, pull_from_remote_async, push_to_remote_async};

    /// Build a scratch directory for one test.
    ///
    /// The system temp dir holds it.
    /// The test removes it on exit.
    /// It holds a staging set.
    /// It never holds a real one.
    fn scratch() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    /// Build a current-thread runtime for async backends.
    ///
    /// The runtime uses no worker threads.
    /// The memory operator needs none.
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime")
    }

    /// Write a tiny staging set.
    ///
    /// The set holds two segment files and one index file.
    /// All files hold a few bytes.
    /// The bytes are arbitrary.
    /// The mirror copies files, never records.
    fn stage(base: &std::path::Path) {
        use crate::layout::IndexSet;

        let set = IndexSet::new(base.join("photos"));
        std::fs::create_dir_all(base).unwrap();
        std::fs::write(set.segment_path(0), b"segment-zero").unwrap();
        std::fs::write(set.segment_path(1), b"tail").unwrap();
        std::fs::write(set.index_path(1), b"index-one").unwrap();
    }

    /// Push then pull through the operator.
    ///
    /// Push uploads into the operator.
    /// Pull fills a fresh directory from it.
    /// The test uses the real async mirror path.
    /// It moves small bytes and no network.
    #[test]
    fn push_then_pull_through_operator() {
        use crate::layout::IndexSet;

        runtime().block_on(async {
            let tmp = scratch();
            let dir = tmp.path();
            stage(dir);
            let set = IndexSet::new(dir.join("photos"));
            let remote = memory().unwrap();

            let pushed = push_to_remote_async(&set, &remote).await.unwrap();
            assert_eq!(
                pushed,
                MirrorStats {
                    pushed_files: 3,
                    pulled_files: 0,
                    pushed_bytes: 12 + 4 + 9,
                    pulled_bytes: 0,
                }
            );

            let fresh = IndexSet::new(dir.join("copy").join("photos"));
            let pulled = pull_from_remote_async(&remote, &fresh).await.unwrap();
            assert_eq!(pulled.pulled_files, 3);
            assert_eq!(
                std::fs::read(fresh.segment_path(0)).unwrap(),
                b"segment-zero"
            );
        });
    }

    /// Refuse missing configuration before transfer.
    ///
    /// No network runs here.
    /// The builder refuses the configuration.
    /// The builder never dials.
    /// The test reads the environment.
    /// The test never writes the environment.
    #[test]
    fn refuse_missing_configuration() {
        {
            let (address, fragment) = ("s3://bucket/a", "BLU_S3_ENDPOINT");
            let url = RemoteUrl::parse(address).unwrap();
            let error = connect(&url).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                error.to_string().contains(fragment),
                "{address} must name {fragment}: {error}"
            );
        }

        #[cfg(unix)]
        for (address, fragment) in [
            ("sftp://host/a", "user"),
            ("sftp://user@host/a", "BLU_SFTP_KEY"),
        ] {
            let url = RemoteUrl::parse(address).unwrap();
            let error = connect(&url).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                error.to_string().contains(fragment),
                "{address} must name {fragment}: {error}"
            );
        }

        #[cfg(not(unix))]
        for address in ["sftp://host/a", "sftp://user@host/a"] {
            let url = RemoteUrl::parse(address).unwrap();
            let error = connect(&url).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::Unsupported,
                "{address} needs a unix build: {error}"
            );
        }
    }

    /// Accept HTTP and HTTPS endpoints.
    ///
    /// Both schemes configure.
    /// HTTPS verifies against the system roots.
    /// Building never dials.
    /// No network runs here.
    #[test]
    fn accept_http_and_https_endpoints() {
        let url = RemoteUrl::parse("s3://bucket/a").unwrap();

        let secure = s3_config_for(&url, "https://s3.amazonaws.com").unwrap();
        assert!(
            secure
                .iter()
                .any(|(k, v)| k == "endpoint" && v == "https://s3.amazonaws.com"),
            "an https endpoint configures: {secure:?}"
        );

        let plain = s3_config_for(&url, "http://127.0.0.1:9000").unwrap();
        assert!(
            plain
                .iter()
                .any(|(k, v)| k == "endpoint" && v == "http://127.0.0.1:9000"),
            "an http endpoint configures: {plain:?}"
        );
    }

    /// Refuse file addresses in the operator.
    ///
    /// `file://` names a local directory.
    /// Connecting it here would build a backend for a path.
    #[test]
    fn refuse_file_address_in_operator() {
        let url = RemoteUrl::parse("file:///dir/backups").unwrap();
        assert!(connect(&url).is_err());
    }
}
