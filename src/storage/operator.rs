//! Keyed backends over the network, through `OpenDAL`.
//!
//! Gated behind `--features remote`: without it this module does not exist
//! and every non-`file://` scheme fails closed. Three schemes share one trait
//! implementation: the operator speaks them all, and
//! [`crate::storage::AsyncObjectStore`] is the only shape the mirror needs.
//!
//! Configuration arrives through the environment, one variable per endpoint
//! or secret, named below. Nothing is guessed: a missing endpoint or identity
//! fails here, before the first byte moves.
//!
//! Durability differs per service, and the code does not paper over it. S3
//! finishes a write atomically on close. `FTP` uploads are
//! best-effort mid-transfer: a torn object fails the run, and the next run's
//! divergence check reports it rather than merging with it. Either way the
//! local staging copy stays atomic: downloads land beside the target and
//! rename into place. A crash loses the fresh copy, which the next pull
//! re-fetches.
//!
//! Retries and timeouts wrap every operator: three attempts, thirty seconds
//! per operation and per stream — the same thirty seconds the client allows a
//! server before calling it a failure.

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
/// Reads endpoints and secrets from the environment, per scheme:
///
/// - `s3://bucket[/prefix]`: `LEPATCH_S3_ENDPOINT` names the endpoint and is
///   required. `LEPATCH_S3_REGION` overrides the region.
///   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` name the identity and
///   travel together: one without the other is a typo, and fails.
///   `AWS_SESSION_TOKEN` extends them. Without any of the three, the request
///   goes unsigned: public buckets answer, private ones refuse.
/// - `ftp://[user@]host[:port][/prefix]`: the user comes from the address or
///   `LEPATCH_FTP_USER`, defaulting to `anonymous`. The password comes from
///   `LEPATCH_FTP_PASSWORD` and defaults to empty.
/// - `sftp://[user@]host[:port][/prefix]`: the user comes from the address or
///   `LEPATCH_SFTP_USER` and is required. The key file comes from
///   `LEPATCH_SFTP_KEY` and is required: this backend speaks key auth only.
///   Host keys stay strict.
///
/// `https://` endpoints need `--features remote-tls`, and fail
/// closed without it. With the feature they build like anything else.
///
/// Returns `InvalidInput` naming the missing piece. Never guesses.
///
/// # Errors
///
/// Returns an error when configuration is missing or the operator refuses
/// it. Building touches no network: the connection stays lazy until the
/// first operation.
pub fn connect(url: &RemoteUrl) -> io::Result<Operator> {
    opendal::init_default_registry();
    #[cfg(feature = "remote-tls")]
    super::tls::install()?;
    #[cfg(not(feature = "remote-tls"))]
    opendal_http_transport_reqwest::install_default();

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
        RemoteScheme::Sftp => Ok((services::SFTP_SCHEME, sftp_config(url)?)),
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

/// S3 configuration: bucket from the address, endpoint and identity from the
/// environment.
///
/// Refuses `https://` endpoints without `--features remote-tls`: without a
/// TLS transport they would fail mid-run. Refused here, they fail with the
/// reason instead.
fn s3_config(url: &RemoteUrl) -> io::Result<Vec<(String, String)>> {
    let endpoint = env("LEPATCH_S3_ENDPOINT").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "LEPATCH_S3_ENDPOINT names the S3 endpoint and is required, e.g. \
             http://127.0.0.1:9000 for a local MinIO",
        )
    })?;

    s3_config_for(url, &endpoint)
}

/// S3 configuration for an explicit endpoint.
///
/// Split from [`s3_config`] so tests cover the shape without touching the
/// process environment, which Rust forbids outside `unsafe`.
fn s3_config_for(url: &RemoteUrl, endpoint: &str) -> io::Result<Vec<(String, String)>> {
    if endpoint.starts_with("https://") && !tls_supported() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an https:// S3 endpoint needs --features remote-tls: this build links no \
             TLS transport, so it would fail mid-run. Rebuild with the feature, or use \
             an http:// endpoint on the same trust boundary",
        ));
    }

    let mut config = vec![
        ("bucket".to_string(), url.location().to_string()),
        ("endpoint".to_string(), endpoint.to_string()),
        ("root".to_string(), root_of(url.prefix())),
    ];

    if let Some(region) = env("LEPATCH_S3_REGION") {
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
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY travel together: one \
                 without the other is refused",
            ));
        }
    }

    Ok(config)
}

/// FTP configuration: host from the address, identity from the address or
/// the environment, defaulting to anonymous.
fn ftp_config(url: &RemoteUrl) -> Vec<(String, String)> {
    let (user, host) = split_user(url.location());

    let user = user
        .map(str::to_string)
        .or_else(|| env("LEPATCH_FTP_USER"))
        .unwrap_or_else(|| "anonymous".to_string());
    let password = env("LEPATCH_FTP_PASSWORD").unwrap_or_default();

    vec![
        ("endpoint".to_string(), format!("ftp://{host}")),
        ("root".to_string(), root_of(url.prefix())),
        ("user".to_string(), user),
        ("password".to_string(), password),
    ]
}

/// SFTP configuration: host from the address, identity required, key auth
/// only.
fn sftp_config(url: &RemoteUrl) -> io::Result<Vec<(String, String)>> {
    let (user, host) = split_user(url.location());

    let user = user
        .map(str::to_string)
        .or_else(|| env("LEPATCH_SFTP_USER"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "sftp:// names no user: put one in the address (sftp://user@host) or \
                 in LEPATCH_SFTP_USER",
            )
        })?;
    let key = env("LEPATCH_SFTP_KEY").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "LEPATCH_SFTP_KEY names the key file and is required: this backend speaks \
             key auth only",
        )
    })?;

    Ok(vec![
        ("endpoint".to_string(), host.to_string()),
        ("root".to_string(), root_of(url.prefix())),
        ("user".to_string(), user),
        ("key".to_string(), key),
    ])
}

/// Whether this build links a TLS transport.
///
/// One predicate for the `https://` gate:
///
/// without `remote-tls` it fails closed, with it it never fires.
fn tls_supported() -> bool {
    cfg!(feature = "remote-tls")
}

/// Split `[user@]authority` at the last `@`.
///
/// Usernames carry no raw `@`: one in the address is percent-encoded, so the
/// last `@` is the separator. Returns no user when none is there.
fn split_user(authority: &str) -> (Option<&str>, &str) {
    match authority.rsplit_once('@') {
        Some((user, host)) => (Some(user), host),
        None => (None, authority),
    }
}

/// Refuse keys that are not bare file names.
///
/// The mirror only passes grammar-checked names. A slash here would walk out
/// of the root on a backend that joins paths naively.
fn check_key(key: &str) -> io::Result<()> {
    if key.is_empty() || key == "." || key == ".." || key.contains('/') || key.contains('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{key:?} is not an object key: a key is a file name, never a path"),
        ));
    }
    Ok(())
}

/// An `io::Error` for an operator failure doing `doing`.
///
/// Keeps the names the mirror matches on: missing stays missing, refused
/// auth stays denied, bad configuration stays invalid input.
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
        let mut sibling = path
            .file_name()
            .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
        sibling.push(".tmp");
        let temporary = path.with_file_name(sibling);
        let mut dest = tokio::fs::File::create(&temporary).await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not create {}: {e}", temporary.display()),
            )
        })?;

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
                    format!("could not write {}: {e}", temporary.display()),
                )
            })?;
            offset = offset.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            written = offset;
        }
        dest.sync_all().await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not sync {}: {e}", temporary.display()),
            )
        })?;
        drop(dest);
        tokio::fs::rename(&temporary, path).await.map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not rename {} into place: {e}", temporary.display()),
            )
        })?;
        Ok(written)
    }
}

/// In-memory operator, for tests.
///
/// No network, no credentials, no filesystem. Mirror round-trips run against
/// it exactly as they run against a service.
#[cfg(test)]
fn memory() -> io::Result<Operator> {
    Operator::via_iter(services::MEMORY_SCHEME, Vec::new())
        .map_err(|e| map_err("build the memory operator", &e))
}

#[cfg(test)]
mod tests {
    use super::{connect, memory, s3_config_for};
    use crate::storage::{MirrorStats, RemoteUrl, pull_from_remote_async, push_to_remote_async};

    /// Unique scratch base for one test, under the system temp dir.
    ///
    /// Holds a staging set, never a real one. Each test removes its own
    /// directory on the way out.
    fn scratch(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lepatch-operator-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// One current-thread runtime for driving async backends in tests.
    ///
    /// No worker threads: the memory operator needs none, and a test that
    /// spawns threads to move bytes it already holds is measuring the
    /// scheduler rather than the backend.
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime")
    }

    /// Write a tiny staging set: two segment files and one index file, all a
    /// few bytes. The bytes are arbitrary. The mirror copies files, never
    /// records.
    fn stage(base: &std::path::Path) {
        use crate::layout::IndexSet;

        let set = IndexSet::new(base.join("photos"));
        std::fs::create_dir_all(base).unwrap();
        std::fs::write(set.segment_path(0), b"segment-zero").unwrap();
        std::fs::write(set.segment_path(1), b"tail").unwrap();
        std::fs::write(set.index_path(1), b"index-one").unwrap();
    }

    /// Push uploads into the operator and pull fills a fresh directory from
    /// it, through the real async mirror path. Small bytes, no network.
    #[test]
    fn push_then_pull_round_trips_through_the_operator() {
        use crate::layout::IndexSet;

        runtime().block_on(async {
            let dir = scratch("round-trip");
            stage(&dir);
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

            std::fs::remove_dir_all(dir).ok();
        });
    }

    /// Configuration gaps fail before the first byte moves. No network: the
    /// refusal happens in the builder, which never dials. Reads the
    /// environment but never writes it.
    #[test]
    fn missing_configuration_is_refused() {
        for (address, fragment) in [
            ("s3://bucket/a", "LEPATCH_S3_ENDPOINT"),
            ("sftp://host/a", "user"),
            ("sftp://user@host/a", "LEPATCH_SFTP_KEY"),
        ] {
            let url = RemoteUrl::parse(address).unwrap();
            let error = connect(&url).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                error.to_string().contains(fragment),
                "{address} must name {fragment}: {error}"
            );
        }
    }

    /// `https://` endpoints fail closed without
    /// `--features remote-tls`: this build links no TLS transport. No network
    /// and no environment writes: the S3 shape is covered through its pure
    /// half.
    #[cfg(not(feature = "remote-tls"))]
    #[test]
    fn tls_endpoints_are_refused_without_a_tls_transport() {
        let url = RemoteUrl::parse("s3://bucket/a").unwrap();
        let refused = s3_config_for(&url, "https://s3.amazonaws.com").unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            refused.to_string().contains("TLS"),
            "an https endpoint must name the missing transport: {refused}"
        );

        let plain = s3_config_for(&url, "http://127.0.0.1:9000").unwrap();
        assert!(
            plain
                .iter()
                .any(|(k, v)| k == "endpoint" && v == "http://127.0.0.1:9000"),
            "an http endpoint configures: {plain:?}"
        );
    }

    /// `https://` endpoints build with `--features
    /// remote-tls`: the transport verifies. No network: building never dials.
    #[cfg(feature = "remote-tls")]
    #[test]
    fn tls_endpoints_build_with_a_tls_transport() {
        let url = RemoteUrl::parse("s3://bucket/a").unwrap();
        let configured = s3_config_for(&url, "https://s3.amazonaws.com").unwrap();
        assert!(
            configured
                .iter()
                .any(|(k, v)| k == "endpoint" && v == "https://s3.amazonaws.com"),
            "an https endpoint configures under remote-tls: {configured:?}"
        );
    }

    /// `file://` never reaches the operator: it is a local directory, and
    /// connecting it here would be a backend for a path.
    #[test]
    fn file_addresses_do_not_reach_the_operator() {
        let url = RemoteUrl::parse("file:///dir/backups").unwrap();
        assert!(connect(&url).is_err());
    }
}
