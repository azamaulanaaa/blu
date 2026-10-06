//! Talking to a `lepatch serve` that is somewhere else.
//!
//! ## What this is
//!
//! **The reading half of a remote set, and only that.** A server keeps sets and
//! hands out what it holds; [`Remote::versions`] is one `GET`, and it answers
//! with the same numbers and the same snapshot hashes that `lepatch versions`
//! reads off a local directory.
//!
//! Nothing here writes. The pushing half is `lepatch pack` and `lepatch merge`
//! with a `curl` between them, and it has no client because it has no need of
//! one: the pack is a file, and `lepatch merge` already opens a file. What a
//! remote *listing* needs is a network round trip and nothing else, so this
//! module is one request and the parsing that keeps honest.
//!
//! ## Why the output is the same as a local listing
//!
//! **A script that works against a local set works unchanged against a remote
//! one.** So the values handed back are the same *types* a local listing
//! produces — a [`std::io::Result`] carrying a [`std::io::Error`] of a chosen
//! kind, and a [`BlobId`](crate::storage::BlobId) rather than the text of one
//! — and the caller prints them through the same `say!` in the same order.
//! Nothing here formats a line for a terminal, and nothing here decides which
//! fields a line carries.
//!
//! ## Why an error names the address
//!
//! Every failure here says **which server, and what it said**.
//!
//! That is not decoration. A connection failure in particular has a cause the
//! user can act on — the server is not running, the port is wrong, the name does
//! not resolve — and `could not read the response` throws all of it away. So a
//! connect failure carries the address it tried, and a refusal carries the
//! server's own sentence out of its problem document rather than a paraphrase of
//! it. See [`Remote::versions`] for the shape those take.
//!
//! ## TLS
//!
//! **There is none, and this client refuses to pretend otherwise.** `awc` is
//! built here with no TLS backend, `lepatch serve` offers no TLS either, and an
//! `https://` address is [`io::ErrorKind::InvalidInput`] rather than a
//! connection that fails somewhere confusing. A server reachable over TLS wants
//! a terminator in front of it, and that terminator speaks `http://` to this.
//!
//! [`lepatch merge`]: crate::command::merge

mod versions;

use std::io;

use std::fmt::Write as _;

pub use versions::Version;

/// How long a request to a remote server may take before it is called a failure.
///
/// Long past any real listing, short enough that a server which has stopped
/// answering is an error rather than a hang.
const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A `lepatch serve` this client can ask.
///
/// Built from the address the server printed on its first line of stdout — the
/// one reason that line goes to stdout rather than stderr — and held for as many
/// requests as the caller wants.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// use lepatch::client::Remote;
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
}

impl std::fmt::Debug for Remote {
    /// The address and nothing else.
    ///
    /// Hand-written because `awc::Client` is not the thing a reader of a
    /// `Debug` wants, and this type is put in error messages.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl Remote {
    /// A client for the server at `address`.
    ///
    /// # Errors
    ///
    /// Returns an error if `address` is not an `http://` address this client can
    /// use: empty, not HTTP, carrying a query or a fragment, holding whitespace,
    /// or naming no host. A trailing `/` is dropped, so the address the server
    /// prints and the address a user types both work.
    pub fn new(address: &str) -> io::Result<Self> {
        Ok(Self {
            base: base_of(address)?,
            client: awc::Client::builder()
                // A version listing is JSON and small, and a client that read an
                // unbounded body off a server with no authentication would be a
                // decision rather than a default. `awc` has no builder for the
                // ceiling, so `versions` enforces its own; this is where the
                // number lives.
                // A listing is one request and one response, and `awc`'s own
                // default of five seconds is short enough that a server busy
                // merging a pack would fail a `versions` that was about to
                // answer. Thirty is long past any real listing and short enough
                // that a wedged server is an error rather than a hang.
                .timeout(TIMEOUT)
                .finish(),
        })
    }

    /// The address this client was built with, as normalised.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.base
    }

    /// The absolute URL of `path` on this server.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` does not begin with a `/`, which is the only
    /// thing that would let it escape the server's own URL space.
    pub(crate) fn url(&self, path: &str) -> io::Result<String> {
        if !path.starts_with('/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{path:?} is not a path on this server: it has to begin with a `/`"),
            ));
        }

        Ok(format!("{}{path}", self.base))
    }
}

/// Normalise and check an address, without the trailing `/`.
fn base_of(address: &str) -> io::Result<String> {
    let address = address.trim();

    if address.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no server address: `--remote` wants the address `lepatch serve` printed, \
             which looks like http://127.0.0.1:8480",
        ));
    }

    // Case-insensitive, because a scheme is, and a user who types HTTPS in
    // capitals has still typed HTTPS.
    let lowered = address.to_ascii_lowercase();

    if lowered.starts_with("https://") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{address} needs TLS, and this client has none: `lepatch serve` offers no TLS \
                 either, so use the http:// address it printed, or put a terminator in front of it"
            ),
        ));
    }

    let Some(rest) = address.strip_prefix("http://") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{address:?} is not a server address: it has to begin with http://, and \
                 lepatch serve does not speak anything else"
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
                "{address:?} carries a query or a fragment; a server address is a scheme, a host \
                 and a port"
            ),
        ));
    }

    // Up to the first `/` is the authority. What is left may be a path, and
    // dropping the trailing slash makes `url` an append rather than a join.
    let host = rest.split('/').next().unwrap_or("");

    if host.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{address:?} names no host"),
        ));
    }

    Ok(address.trim_end_matches('/').to_string())
}

/// A set name as it goes into a URL path.
///
/// # Errors
///
/// Returns an error if `name` is empty or is not a single normal path
/// component.
///
/// **The same rule the server applies to the name it receives**, taken from
/// [`crate::serve`]'s own check, so a name accepted here is a name the server
/// will not refuse as a path. The two sides of a name have to agree on what a
/// name is, and the server's rule is the one that was written down first.
///
/// Percent-encoding is applied rather than assumed away: a set may be named
/// `photos 2024`, and a space sent raw would truncate the path into something
/// the server reads as a different set.
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
                "{name:?} is a path, and a set on a server is named by one path component: pass \
                 the set's name (`docs`), not where it lives (`backups/docs`)"
            ),
        ));
    }

    let mut encoded = String::with_capacity(name.len());
    for byte in name.bytes() {
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

    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::{Remote, base_of, set_path};

    #[test]
    fn the_address_the_server_prints_is_accepted() {
        assert_eq!(
            base_of("http://127.0.0.1:8480").unwrap(),
            "http://127.0.0.1:8480"
        );
    }

    #[test]
    fn a_trailing_slash_is_the_same_address() {
        assert_eq!(
            base_of("http://127.0.0.1:8480/").unwrap(),
            "http://127.0.0.1:8480"
        );
    }

    #[test]
    fn whitespace_around_the_address_is_trimmed() {
        // Typed, pasted, or copied out of a terminal line that had a prompt on
        // it. All the same mistake.
        assert_eq!(
            base_of("  http://127.0.0.1:8480\n").unwrap(),
            "http://127.0.0.1:8480"
        );
    }

    #[test]
    fn https_is_refused_with_the_reason() {
        // The error has to name what is wrong, not just that something is.
        let error = base_of("https://example.test").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("needs TLS"));
    }

    #[test]
    fn https_is_refused_whatever_the_case() {
        assert!(base_of("HTTPS://example.test").is_err());
    }

    #[test]
    fn an_address_that_is_not_http_is_refused() {
        for address in ["ftp://host", "127.0.0.1:8480", "//127.0.0.1:8480"] {
            let error = base_of(address).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidInput,
                "{address} should be refused"
            );
        }
    }

    #[test]
    fn an_empty_address_is_refused_with_an_example() {
        // The message carries the shape of a good one, because a user who typed
        // nothing is asking what this option wants.
        let error = base_of("   ").unwrap_err();

        assert!(error.to_string().contains("http://127.0.0.1:8480"));
    }

    #[test]
    fn an_address_with_no_host_is_refused() {
        assert!(base_of("http://").is_err());
    }

    #[test]
    fn an_address_carrying_a_query_is_refused() {
        assert!(base_of("http://host:8480/?a=b").is_err());
        assert!(base_of("http://host:8480/#frag").is_err());
    }

    #[test]
    fn a_set_name_is_one_path_component() {
        assert_eq!(set_path("docs").unwrap(), "docs");
        assert_eq!(set_path("photos.2024").unwrap(), "photos.2024");
        assert_eq!(set_path("a-b_c~d").unwrap(), "a-b_c~d");
    }

    #[test]
    fn a_set_name_is_percent_encoded_rather_than_assumed_safe() {
        // A space sent raw would end the path, and the server would read what
        // came before it as the set's name.
        assert_eq!(set_path("photos 2024").unwrap(), "photos%202024");
        assert!(set_path("a/b").is_err(), "a path is not a name");
        assert!(set_path("../etc").is_err());
        assert!(set_path("").is_err());
    }

    #[test]
    fn a_path_is_refused_with_a_message_that_says_which_half_is_wrong() {
        let error = set_path("backups/docs").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidFilename);
        assert!(error.to_string().contains("is a path"));
    }

    #[test]
    fn a_path_is_built_by_appending_and_never_by_joining() {
        let remote = Remote::new("http://127.0.0.1:8480/").unwrap();

        assert_eq!(
            remote.url("/v1/sets/docs/versions").unwrap(),
            "http://127.0.0.1:8480/v1/sets/docs/versions"
        );
    }

    #[test]
    fn a_relative_path_is_refused_rather_than_appended() {
        let remote = Remote::new("http://127.0.0.1:8480").unwrap();

        // Without this, a path that did not start at the root would be appended
        // to whatever the address already carried.
        assert!(remote.url("v1/sets").is_err());
    }
}
