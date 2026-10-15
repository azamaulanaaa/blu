// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Talking to a `blu serve` that is somewhere else.
//!
//! ## What this is
//!
//! **The client half of a backup, over a socket.** A local backup writes a set on
//! this machine. A remote one packs the same tree, uploads the pack, and asks the
//! server to merge it into a set the server keeps. So there are two halves here
//! and they answer different questions:
//!
//! * **What does that server hold?** [`Remote::versions`] is one `GET`, and it
//!   answers with the same numbers and the same snapshot hashes that
//!   `blu versions` reads off a local directory.
//! * **How does a tree get there?** [`Remote::begin_upload`] and [`Upload::send`]
//!   carry the pack — four requests, of which one is the transfer —
//!   [`Remote::start_merge`] asks a server to merge it, and [`Remote::await_job`]
//!   waits for the job that runs it. [`Remote::merge_pack`] is those four in the
//!   order they have to happen, with the cleanup a failed push owes the server
//!   wired in.
//! * **How does a tree come back?** [`Remote::plan_pack`] asks a server what it
//!   should still fetch, [`Remote::fetch_range`] gets one of those ranges, and
//!   [`Pull::fetch`] assembles them into a pack file at their offsets. A pack on a
//!   server is computed rather than stored, so this is a download and nothing
//!   else.
//!
//! ## Why the output is the same as a local run's
//!
//! **A script that works against a local set works unchanged against a remote
//! one.** So the values handed back are the same *types* a local run produces —
//! a [`std::io::Result`] carrying a [`std::io::Error`] of a chosen kind, and a
//! [`BlobId`](crate::storage::BlobId) rather than the text of one — and the
//! caller prints them through the same code. Nothing here formats a line for a
//! terminal, and nothing here decides which fields a line carries.
//!
//! ## Why an error names the address, and quotes the server
//!
//! Every failure here says **which server, and what it said**.
//!
//! That is not decoration. A connection failure in particular has a cause the
//! user can act on — the server is not running, the port is wrong, the name does
//! not resolve — and `could not read the response` throws all of it away. So a
//! connect failure carries the address it tried, and a refusal carries the
//! server's own sentence out of its problem document rather than a paraphrase of
//! it. The `error` module holds the table both ends agree on, and
//! [`Remote::versions`] shows the shape those errors take.
//!
//! ## Why these futures are not `Send`
//!
//! Every request here runs inside a [`tokio::task::LocalSet`] it enters itself.
//! That is `awc`'s requirement rather than this crate's, and [`Remote::versions`]
//! documents it in full. So a caller on a multi-threaded runtime spawns these with
//! [`tokio::task::spawn_local`], and nothing else about the call changes.
//!
//! ## TLS
//!
//! The client speaks plain HTTP and verified HTTPS.
//! It verifies HTTPS against the system roots.
//! `blu serve` offers plain HTTP only.
//! A server behind a terminator uses HTTPS before the terminator.
//! The terminator uses HTTP before the server.
//! The client never downgrades HTTPS before plain HTTP.
//! The client never upgrades HTTP before HTTPS.
//!
//! ## Auth
//!
//! The client forwards a bearer token when the caller holds one.
//! It sends `Authorization: Bearer <token>` on every request.
//! It never checks the token.
//! A proxy before the server checks it.
//! `blu serve` itself checks nothing.
//! Put the server behind a proxy when it needs auth.
//!
//! [`blu merge`]: crate::command::MergeBuilder

mod error;
mod history;
mod jobs;
mod pull;
mod uploads;
mod versions;

use std::io;

use std::fmt::Write as _;

pub use jobs::{
    Job, Kind, MergeCompression, Merged, MergedAlready, MergedCommitted, Outcome, PackSummary,
    State,
};
pub use pull::{Fetched, PackPlan, Pull};
pub use uploads::{PACK_MEDIA_TYPE, Upload, UploadCommit};
pub use versions::Version;

/// How long a request to a remote server may take before it is called a failure.
///
/// Long past any real listing, short enough that a server which has stopped
/// answering is an error rather than a hang.
const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A `blu serve` this client can ask.
///
/// Built from the address the server printed on its first line of stdout.
/// That line goes to stdout rather than stderr for this reason.
/// One client serves as many requests as the caller wants.
///
/// Use [`Remote::with_token`] when a proxy guards the server.
/// The client forwards the token as a Bearer header.
/// The client never checks the token.
/// The proxy checks it.
//
/// # Example
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// use blulib::client::Remote;
///
/// let remote = Remote::new("http://127.0.0.1:8480")?;
/// for listed in remote.versions("docs").await? {
///     println!("{}  {}", listed.version, listed.snapshot);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Remote {
    /// The address, without a trailing `/`, so a path can be appended to it.
    base: String,
    /// The underlying actix client.
    client: awc::Client,
    /// The bearer token the proxy checks, if any.
    token: Option<String>,
}

impl std::fmt::Debug for Remote {
    /// Show the address and hide the token.
    ///
    /// Hand-written because `awc::Client` is not the thing a reader of a
    /// `Debug` wants, and the token must never reach logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl Remote {
    /// Build a client for the server at `address`.
    ///
    /// The client accepts `http://` and `https://`.
    /// It verifies HTTPS against the system roots.
    /// The client sends no auth header.
    /// Use [`Remote::with_token`] when a proxy guards the server.
    ///
    /// # Errors
    ///
    /// The call fails when `address` is empty.
    /// The call fails when `address` starts with another scheme.
    /// The call fails when `address` carries a query or a fragment.
    /// The call fails when `address` holds whitespace.
    /// The call fails when `address` names no host.
    /// A trailing `/` is dropped.
    /// The address the server prints works.
    /// The address a user types works.
    pub fn new(address: &str) -> io::Result<Self> {
        Ok(Self::build(base_of(address)?, None))
    }

    /// Build a client that forwards a bearer token.
    ///
    /// The client sends `Authorization: Bearer <token>` on every request.
    /// The client never checks the token.
    /// A proxy before the server checks it.
    /// Use a JWT or another bearer token the proxy accepts.
    ///
    /// # Errors
    ///
    /// The call fails when `address` is not a server address.
    /// The call fails when `token` is empty.
    /// The call fails when `token` holds whitespace or control bytes.
    /// A token with a newline could split the header.
    pub fn with_token(address: &str, token: &str) -> io::Result<Self> {
        Ok(Self::build(
            base_of(address)?,
            Some(check_token(token)?.to_string()),
        ))
    }

    /// Build a client from a checked address and an optional token.
    fn build(base: String, token: Option<String>) -> Self {
        Self {
            base,
            client: awc::Client::builder()
                // A version listing is JSON and small, and a client that read an
                // unbounded body off a server would accept a decision nobody took.
                // `awc` has no builder for the ceiling, so `versions` enforces
                // its own. This is where the number lives.
                // A listing is one request and one response, and `awc`'s own
                // default of five seconds is short enough that a server busy
                // merging a pack would fail a `versions` that was about to
                // answer. Thirty is long past any real listing and short enough
                // that a wedged server is an error rather than a hang.
                .timeout(TIMEOUT)
                .finish(),
            token,
        }
    }

    /// Attach the bearer header to one request.
    ///
    /// The call adds nothing when the client holds no token.
    pub(crate) fn authorize(&self, req: awc::ClientRequest) -> awc::ClientRequest {
        match &self.token {
            Some(token) => req.insert_header(("authorization", format!("Bearer {token}"))),
            None => req,
        }
    }

    /// The address this client was built with, as normalised.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.base
    }

    /// Build the absolute URL of `path` on this server.
    ///
    /// # Errors
    ///
    /// The call fails when `path` does not start with `/`.
    /// A path without `/` could escape the server URL space.
    pub(crate) fn url(&self, path: &str) -> io::Result<String> {
        if !path.starts_with('/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{path:?} is not a path on this server. Start the path with `/`"),
            ));
        }

        Ok(format!("{}{path}", self.base))
    }
}

/// Check an address and drop the trailing `/`.
///
/// The address uses `http://` or `https://`.
/// The check is case-insensitive for the scheme.
/// The client verifies HTTPS against the system roots.
fn base_of(address: &str) -> io::Result<String> {
    let address = address.trim();

    if address.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no server address: use the address `blu serve` printed, such as http://127.0.0.1:8480",
        ));
    }

    // The scheme is case-insensitive.
    // A user who types HTTPS still types HTTPS.
    let lowered = address.to_ascii_lowercase();
    let rest = if lowered.starts_with("https://") {
        address.get("https://".len()..).unwrap_or_default()
    } else if lowered.starts_with("http://") {
        address.get("http://".len()..).unwrap_or_default()
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{address:?} is not a server address. Start the address with http:// or https://"
            ),
        ));
    };

    if address.chars().any(char::is_whitespace) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{address:?} holds whitespace, which cannot be in an address"),
        ));
    }

    if address.contains('?') || address.contains('#') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{address:?} carries a query or a fragment. A server address holds a scheme, a host and a port"
            ),
        ));
    }

    // Up to the first `/` is the authority.
    // Dropping the trailing slash turns `url` into an append.
    let host = rest.split('/').next().unwrap_or("");

    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{address:?} names no host"),
        ));
    }

    Ok(address.trim_end_matches('/').to_string())
}

/// Check a bearer token.
///
/// The token travels in one header value.
/// Whitespace would split the header.
/// Control bytes would split the header.
/// An empty token names nothing.
///
/// # Errors
///
/// The call fails when `token` is empty.
/// The call fails when `token` holds whitespace or control bytes.
fn check_token(token: &str) -> io::Result<&str> {
    if token.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no token: pass --token or set BLU_TOKEN",
        ));
    }

    if token
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bad token: a bearer token holds no whitespace",
        ));
    }

    Ok(token)
}

/// Encode a set name for a URL path.
///
/// # Errors
///
/// The call fails when `name` is empty.
/// The call fails when `name` is not one normal path component.
///
/// The rule matches the server rule.
/// A name accepted here is a name the server accepts.
/// The server rule was written first.
///
/// The encoder percent-encodes the name.
/// A set may use `photos 2024`.
/// A raw space would truncate the path.
/// The server would then read another set.
pub(crate) fn set_path(name: &str) -> io::Result<String> {
    use std::path::{Component, Path};

    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a set name may not be empty",
        ));
    }

    let mut components = Path::new(name).components();
    let only = components.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "a set name may not be empty")
    })?;

    if components.next().is_some() || only != Component::Normal(name.as_ref()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidFilename,
            format!(
                "{name:?} is a path. A set on a server uses one path component. Use the set name (`docs`)"
            ),
        ));
    }

    Ok(encode_component(name))
}

/// Percent-encode one path component.
///
/// One function encodes for both callers.
/// A set name and a server id differ in checks.
/// The output bytes stay the same.
///
/// The unreserved set follows RFC 3986.
/// It keeps `.` unencoded.
/// Set names carry dots, such as `photos.2024`.
/// Encoding dots would hide the name from requests.
///
/// History `files` values also use this encoder.
/// A path holds slashes and spaces.
/// Both survive the query string.
pub(crate) fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                // `write!` rather than `push_str(&format!(..))`, which
                // allocates a `String` per byte to append it to another one.
                let _ = write!(encoded, "%{byte:02X}");
            }
        }
    }

    encoded
}

/// Encode a server id for a URL path.
///
/// The encoder makes the id safe.
/// It does not check the id.
/// The caller types a set name.
/// The server selects an upload id or a job id.
/// The encoder stops the id from leaving its path segment.
/// A server that answers `{"job":"../../v1/version"}` cannot redirect this client.
/// An empty id is refused by the request that names it.
pub(crate) fn segment(id: &str) -> String {
    encode_component(id)
}

/// Check a set name before any transfer.
///
/// The check uses the same rule as `set_path`.
/// A command checks the name before it stages a backup.
/// The caller learns the server complaint before the transfer.
/// The caller avoids a failure after the transfer.
///
/// The call returns the same error as `set_path`.
///
/// # Errors
///
/// The call fails when `name` is empty.
/// The call fails when `name` is not one path component.
pub fn check_set(name: &str) -> io::Result<()> {
    set_path(name).map(|_| ())
}

/// Split a serve target into server address and set name.
///
/// A target names both halves at once.
/// `http://127.0.0.1:8480/docs` names server `http://127.0.0.1:8480`.
/// It names set `docs`.
/// `https://example.test/docs` names server `https://example.test`.
/// It names set `docs`.
/// The set is one path segment after the authority.
/// The splitter percent-decodes the segment.
/// The set follows the one-component rule.
///
/// # Errors
///
/// The call fails when the target uses another scheme.
/// The call fails when the target names no host.
/// The call fails when the target names no set.
/// The call fails when the target names more than one segment.
pub fn split_serve_target(target: &str) -> io::Result<(String, String)> {
    let trimmed = target.trim();
    let lowered = trimmed.to_ascii_lowercase();
    let (scheme, rest) = if lowered.starts_with("https://") {
        ("https", trimmed.get("https://".len()..).unwrap_or_default())
    } else if lowered.starts_with("http://") {
        ("http", trimmed.get("http://".len()..).unwrap_or_default())
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{target:?} is not a serve address. Start the address with http:// or https://"
            ),
        ));
    };
    if trimmed.chars().any(char::is_whitespace) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{target:?} holds whitespace, which cannot be in an address"),
        ));
    }
    if trimmed.contains('?') || trimmed.contains('#') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{target:?} carries a query or a fragment. A serve address holds a scheme, a host, a port and one set name"
            ),
        ));
    }
    let (host, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => ("", ""),
    };
    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{target:?} names no host"),
        ));
    }
    // Check the address half the same way `Remote::new` checks one.
    let address = format!("{scheme}://{host}");
    Remote::new(&address)?;
    let path = path.trim_matches('/');
    if path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{target:?} names no set. Append the set name (`{target}/docs`)"),
        ));
    }
    if path.contains('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidFilename,
            format!(
                "{path:?} is a path. A set on a server uses one path component. Use the set name (`docs`)"
            ),
        ));
    }
    let set = percent_decode(path)?;
    check_set(&set)?;
    Ok((address, set))
}

/// Decode one percent-encoded path segment.
///
/// # Errors
///
/// The call fails when the segment ends with incomplete encoding.
/// The call fails when the segment holds bad hex.
/// The call fails when the decoded bytes are not UTF-8.
fn percent_decode(segment: &str) -> io::Result<String> {
    let mut out = Vec::with_capacity(segment.len());
    let bytes = segment.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{segment:?} ends in an incomplete percent-encoding"),
                ));
            }
            let hex = &segment[i + 1..i + 3];
            let byte = u8::from_str_radix(hex, 16).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{segment:?} holds %{hex}, which is not percent-encoding"),
                )
            })?;
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{segment:?} is not UTF-8 once decoded"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{Remote, base_of, set_path};

    #[test]
    fn accept_address_from_server() {
        assert_eq!(
            base_of("http://127.0.0.1:8480").unwrap(),
            "http://127.0.0.1:8480"
        );
    }

    #[test]
    fn accept_address_with_trailing_slash() {
        assert_eq!(
            base_of("http://127.0.0.1:8480/").unwrap(),
            "http://127.0.0.1:8480"
        );
    }

    #[test]
    fn trim_space_around_address() {
        // Users paste an address from terminal output.
        // The pasted text holds spaces or a newline.
        assert_eq!(
            base_of("  http://127.0.0.1:8480\n").unwrap(),
            "http://127.0.0.1:8480"
        );
    }

    #[test]
    fn accept_https_address() {
        // The client verifies HTTPS against the system roots.
        assert_eq!(
            base_of("https://example.test").unwrap(),
            "https://example.test"
        );
    }

    #[test]
    fn accept_https_address_in_any_case() {
        // The scheme check ignores case.
        assert_eq!(
            base_of("HTTPS://example.test").unwrap(),
            "HTTPS://example.test"
        );
    }

    #[test]
    fn refuse_address_without_http_scheme() {
        for address in ["ftp://host", "127.0.0.1:8480", "//127.0.0.1:8480"] {
            let error = base_of(address).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidInput,
                "{address} must fail"
            );
        }
    }

    #[test]
    fn refuse_empty_address_with_example() {
        // The message shows a good address.
        // A user who types nothing asks what the option wants.
        let error = base_of("   ").unwrap_err();

        assert!(error.to_string().contains("http://127.0.0.1:8480"));
    }

    #[test]
    fn refuse_address_without_host() {
        assert!(base_of("http://").is_err());
        assert!(base_of("https://").is_err());
    }

    #[test]
    fn refuse_address_with_query() {
        assert!(base_of("http://host:8480/?a=b").is_err());
        assert!(base_of("http://host:8480/#frag").is_err());
        assert!(base_of("https://host:8480/?a=b").is_err());
    }

    #[test]
    fn accept_set_name_with_one_component() {
        assert_eq!(set_path("docs").unwrap(), "docs");
        assert_eq!(set_path("photos.2024").unwrap(), "photos.2024");
        assert_eq!(set_path("a-b_c~d").unwrap(), "a-b_c~d");
    }

    #[test]
    fn encode_set_name_with_percent_encoding() {
        // A raw space would end the path.
        // The server would then read another set name.
        assert_eq!(set_path("photos 2024").unwrap(), "photos%202024");
        assert!(set_path("a/b").is_err(), "a path is not a name");
        assert!(set_path("../etc").is_err());
        assert!(set_path("").is_err());
    }

    #[test]
    fn refuse_bad_set_name_before_transfer() {
        // `check_set` and `set_path` enforce one rule.
        // This test locks the two together.
        assert!(super::check_set("docs").is_ok());
        assert!(super::check_set("photos 2024").is_ok());

        for name in ["", "a/b", "../etc", "backups/docs"] {
            let early = super::check_set(name).unwrap_err();
            let late = set_path(name).unwrap_err();

            assert_eq!(early.kind(), late.kind(), "`--set {name:?}`");
            assert_eq!(
                early.to_string(),
                late.to_string(),
                "`--set {name:?}` should read the same either way"
            );
        }
    }

    #[test]
    fn refuse_path_with_clear_half_message() {
        let error = set_path("backups/docs").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidFilename);
        assert!(error.to_string().contains("is a path"));
    }

    #[test]
    fn build_path_by_append() {
        let remote = Remote::new("http://127.0.0.1:8480/").unwrap();

        assert_eq!(
            remote.url("/v1/sets/docs/versions").unwrap(),
            "http://127.0.0.1:8480/v1/sets/docs/versions"
        );

        let secure = Remote::new("https://example.test/").unwrap();

        assert_eq!(
            secure.url("/v1/sets/docs/versions").unwrap(),
            "https://example.test/v1/sets/docs/versions"
        );
    }

    #[test]
    fn refuse_relative_path() {
        let remote = Remote::new("http://127.0.0.1:8480").unwrap();

        // A path without root must fail.
        // Appending it would mix it with the address.
        assert!(remote.url("v1/sets").is_err());
    }

    #[test]
    fn encode_server_id_for_path_segment() {
        // The encoder makes a server id safe.
        // It does not check the id.
        // A hostile server id stays inside its segment.
        assert_eq!(super::segment("abc123"), "abc123");
        assert_eq!(super::segment("../../v1/version"), "..%2F..%2Fv1%2Fversion");
        assert_eq!(super::segment("a b"), "a%20b");
    }

    #[test]
    fn split_serve_target_into_address_and_set() {
        let (address, set) = super::split_serve_target("http://127.0.0.1:8480/docs").unwrap();
        assert_eq!(address, "http://127.0.0.1:8480");
        assert_eq!(set, "docs");

        let (address, set) =
            super::split_serve_target("http://127.0.0.1:8480/photos%202024").unwrap();
        assert_eq!(address, "http://127.0.0.1:8480");
        assert_eq!(set, "photos 2024");

        let (address, set) = super::split_serve_target("https://example.test/docs").unwrap();
        assert_eq!(address, "https://example.test");
        assert_eq!(set, "docs");

        for target in [
            "http://127.0.0.1:8480",
            "http://127.0.0.1:8480/",
            "http://127.0.0.1:8480/a/b",
            "photos",
            "http://",
            "https://",
        ] {
            assert!(
                super::split_serve_target(target).is_err(),
                "{target:?} must fail"
            );
        }
    }

    #[test]
    fn accept_token_with_bearer_shape() {
        // A JWT holds dots and base64url bytes.
        // It holds no whitespace.
        let remote = Remote::with_token(
            "http://127.0.0.1:8480",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJibHUifQ.c2ln",
        )
        .unwrap();

        assert_eq!(remote.address(), "http://127.0.0.1:8480");
    }

    #[test]
    fn refuse_bad_token_before_transfer() {
        // An empty token names nothing.
        // Whitespace would split the header.
        // Control bytes would split the header.
        for token in ["", "bad token", "bad\ntoken", "bad\rtoken", "bad\ttoken"] {
            let error = Remote::with_token("http://127.0.0.1:8480", token).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidInput,
                "{token:?} must fail"
            );
        }
    }

    #[test]
    fn hide_token_in_debug_text() {
        // Logs must never carry the bearer token.
        let remote = Remote::with_token("http://127.0.0.1:8480", "secret-jwt-value").unwrap();
        let text = format!("{remote:?}");

        assert!(text.contains("http://127.0.0.1:8480"));
        assert!(!text.contains("secret-jwt-value"));
    }
}
