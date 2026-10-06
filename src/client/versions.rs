//! `GET /v1/sets/{set}/versions` — the versions a remote set holds.
//!
//! ## The answer is what a local listing gives
//!
//! The server lists a set from its index files and resolves every snapshot hash
//! through the store, skipping a version it cannot serve — the same
//! lower-bound answer, with the same skips, that `lepatch versions` gives for a
//! directory. So this parses into the same [`Version`] values a local listing
//! produces, in the same order, and the caller prints them with the same code.
//!
//! ## Why an unknown shape is an error
//!
//! **A response this client cannot read must not become an empty listing.**
//!
//! That is the one way a remote listing could quietly lie. A server that
//! renamed `versions`, or an entry that lost its `snapshot`, would answer `200`
//! with something this parser does not understand. Read permissively — ask for
//! `versions`, take what is there, treat a missing array as no versions — and
//! the command prints nothing and exits 0. A script reading that believes the
//! set is empty, which is a claim, and the wrong one.
//!
//! So every field is asked for by name and its type checked: the document is an
//! object, `versions` is an array, each entry is an object, `version` is a
//! number that fits the set's own numbering, and `snapshot` is a hash this
//! crate's own hash type will accept. Anything else is
//! [`io::ErrorKind::InvalidData`] naming what was wrong, and nothing is printed.
//! An empty list is a thing the server can say (`"versions": []`); it is not a
//! thing this client infers from silence.
//!
//! ## Why a refusal is the server's own sentence
//!
//! `lepatch serve` sends RFC 9457 problem documents, and `detail` is the
//! [`std::io::Error`] the command line would have printed — the crate's errors
//! are flat and complete by design, and the server does not translate them. A
//! `404` therefore arrives here as a [`io::ErrorKind::NotFound`] carrying that
//! sentence verbatim, so a remote miss reads like the local one and a user is
//! not asked to learn a second vocabulary for one failure.
//!
//! Every other status is mapped back through the server's own table — the
//! inverse of the one in `src/serve/error.rs` — so a client and a server agree
//! on what a refusal means rather than each guessing.

use std::io;

use serde_json::Value;

use super::{Remote, set_path};
use crate::storage::BlobId;

/// The most this client will read for one version listing.
///
/// A version costs about a hundred bytes of JSON, so this is a set of roughly
/// six hundred thousand versions. A set bigger than that is a set this endpoint
/// is the wrong question for, and the refusal says so rather than truncating.
pub(super) const PAYLOAD_LIMIT: usize = 64 * 1024 * 1024;

/// One version, as the server named it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    /// The set's own number for it.
    pub version: u32,
    /// The snapshot's hash.
    pub snapshot: BlobId,
}

impl Remote {
    /// The versions `set` holds, oldest first.
    ///
    /// **This future is `!Send`, and that is `awc`'s requirement rather than
    /// this crate's.** `awc` is actix's client, and the HTTP/1 codec it builds
    /// starts a task through [`tokio::task::spawn_local`] to keep the `Date`
    /// header's second-resolution cache warm. On a current-thread runtime that
    /// is free; on a multi-thread one — which is what `#[tokio::main]` builds,
    /// and what every caller of this command runs in — `spawn_local` **panics**
    /// unless a [`tokio::task::LocalSet`] is entered. So one is entered here,
    /// inside the request rather than around the command, which puts the
    /// requirement where the dependency that has it lives.
    ///
    /// A caller spawning this on a worker therefore wants
    /// [`tokio::task::spawn_local`], and nothing else about the call changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if it refuses the set
    /// — a `404` is [`io::ErrorKind::NotFound`], carrying the server's own
    /// sentence — or if the answer is not the shape described in this module.
    /// Every one of those messages names the server, and none of them is an
    /// empty list.
    pub async fn versions(&self, set: &str) -> io::Result<Vec<Version>> {
        tokio::task::LocalSet::new()
            .run_until(self.versions_inner(set))
            .await
    }

    /// The request itself, for [`Remote::versions`] to run on a
    /// [`tokio::task::LocalSet`].
    async fn versions_inner(&self, set: &str) -> io::Result<Vec<Version>> {
        let url = self.url(&format!("/v1/sets/{}/versions", set_path(set)?))?;

        let mut response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let body = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &body));
        }

        parse(&url, &body)
    }
}

/// Read a response body whole, refusing one larger than [`PAYLOAD_LIMIT`].
///
/// **A ceiling rather than whatever arrives**, because this is a client of a
/// server with no authentication and an unbounded body is a decision nobody
/// took. `awc` caps at 2 MiB by default, which is about twenty thousand
/// versions; [`PAYLOAD_LIMIT`] is higher, and is still a ceiling: a listing past
/// it is an error naming the limit, never a list truncated at whatever happened
/// to arrive first, because a truncated list of versions looks complete.
async fn read(url: &str, response: &mut awc::ClientResponse) -> io::Result<Vec<u8>> {
    let read = response.body().limit(PAYLOAD_LIMIT).await;

    let bytes = read.map_err(|e| {
        if let awc::error::PayloadError::Overflow = e {
            return io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{url} is answering with more than the {PAYLOAD_LIMIT} bytes this client will \
                     read for a version listing"
                ),
            );
        }

        io::Error::other(format!("could not read the answer from {url}: {e}"))
    })?;

    Ok(bytes.to_vec())
}

/// The error a non-2xx answer becomes.
///
/// The problem document's `detail`, or its `title`, is the server's own message
/// and is carried through unchanged. `status_for` in `src/serve/error.rs` is the
/// table this inverts, so a `422` here is the `InvalidData` the server started
/// from.
fn refusal(status: u16, url: &str, body: &[u8]) -> io::Error {
    let said = said_by(body);
    let message = match said {
        Some(detail) => detail,
        // A body that is not a problem document is a body there is nothing to
        // quote, so the status and the address carry it. Never a bare "request
        // failed": the address is the one fact a reader cannot get elsewhere.
        None => format!("{url} answered {status} with no problem document in it"),
    };

    io::Error::new(kind_for(status), message)
}

/// The kind a status maps back to.
///
/// The inverse of [`crate::serve`]'s table, so the two ends agree. Anything
/// unmapped there is a `500` and so is anything unmapped here: inventing a kind
/// for a status nobody has classified would be a guess about what a client
/// ought to do about a failure nobody has seen.
fn kind_for(status: u16) -> io::ErrorKind {
    match status {
        // `400` and `416` together: the server sends `416` for a `Range` this
        // file cannot answer, and a listing asks for no range, so both are "a
        // request this client made that the server refused to fill in".
        400 | 416 => io::ErrorKind::InvalidInput,
        403 => io::ErrorKind::PermissionDenied,
        404 => io::ErrorKind::NotFound,
        409 => io::ErrorKind::AlreadyExists,
        422 => io::ErrorKind::InvalidData,
        _ => io::ErrorKind::Other,
    }
}

/// The server's own sentence, out of an RFC 9457 problem document.
///
/// `detail` first and `title` second, because this server writes the same
/// sentence into both. A document that is not JSON, or is JSON without either
/// field, has nothing to quote.
fn said_by(body: &[u8]) -> Option<String> {
    let document: Value = serde_json::from_slice(body).ok()?;

    ["detail", "title"]
        .into_iter()
        .find_map(|field| document.get(field)?.as_str().map(str::to_string))
        .filter(|said| !said.is_empty())
}

/// The listing, read strictly.
///
/// See this module's documentation for why every field is checked rather than
/// taken if it is there. `url` is in every message, so a failure says which
/// server and which set it was about.
fn parse(url: &str, body: &[u8]) -> io::Result<Vec<Version>> {
    let document: Value =
        serde_json::from_slice(body).map_err(|e| drift(url, &format!("it is not JSON ({e})")))?;

    if !document.is_object() {
        return Err(drift(url, "it is not a JSON object"));
    }

    let listed = document
        .get("versions")
        .ok_or_else(|| drift(url, "it carries no `versions` field"))?;

    let entries = listed
        .as_array()
        .ok_or_else(|| drift(url, "`versions` is not an array"))?;

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let version = entry
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|number| u32::try_from(number).ok())
            .ok_or_else(|| {
                drift(
                    url,
                    "a version entry carries no `version` number this client can use",
                )
            })?;

        let hex = entry
            .get("snapshot")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                drift(
                    url,
                    &format!("the entry for version {version} carries no `snapshot` string"),
                )
            })?;

        let snapshot = BlobId::from_hex(hex).map_err(|_| {
            drift(
                url,
                &format!("the `snapshot` for version {version} is not a hash this crate wrote"),
            )
        })?;

        out.push(Version { version, snapshot });
    }

    Ok(out)
}

/// A response this client cannot read.
///
/// Said as one sentence about the address, because the user is looking at a
/// command that printed nothing and needs to know which server to ask about.
fn drift(url: &str, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{url} answered with a version listing this client does not understand: {reason}. \
             Nothing is printed rather than an empty listing; `lepatch serve` documents the \
             shape in openapi/lepatch.json"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::{Version, kind_for, parse, refusal, said_by};
    use crate::storage::BlobId;

    /// The listing a server sends for a set holding two versions.
    const LISTING: &str = r#"{"versions":[
        {"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101"},
        {"version":2,"snapshot":"0202020202020202020202020202020202020202020202020202020202020202"}
    ]}"#;

    fn one() -> BlobId {
        BlobId::from_hex("0101010101010101010101010101010101010101010101010101010101010101")
            .unwrap()
    }

    fn two() -> BlobId {
        BlobId::from_hex("0202020202020202020202020202020202020202020202020202020202020202")
            .unwrap()
    }

    #[test]
    fn a_listing_is_read_in_order_with_both_fields() {
        // Field presence is asserted, not assumed: a parser that read `version`
        // and dropped `snapshot` would still produce a list, and this catches
        // it only if the value is compared.
        //
        // **`two()` for the second entry, not `one()` again.** Every entry
        // carrying the *first* entry's hash would satisfy a comparison that used
        // the same value twice, and that is exactly the parser this is meant to
        // reject: one that read `snapshot` off the first object and copied it
        // for the rest. The two entries differ here so that reading it wrongly
        // is visible.
        let listed = parse("http://s/v1/sets/docs/versions", LISTING.as_bytes()).unwrap();

        assert_eq!(
            listed,
            vec![
                Version {
                    version: 1,
                    snapshot: one()
                },
                Version {
                    version: 2,
                    snapshot: two()
                }
            ]
        );
    }

    #[test]
    fn an_empty_listing_is_an_empty_list() {
        // The one shape that legitimately says "nothing here": an array with
        // nothing in it. A set that holds nothing is a set that exists.
        let listed = parse("http://s/v1/sets/docs/versions", br#"{"versions":[]}"#).unwrap();

        assert_eq!(listed, Vec::new());
    }

    #[test]
    fn a_document_that_is_not_the_shape_is_an_error_not_an_empty_list() {
        // Each of these is a server that has changed what it sends. Printing an
        // empty listing for any of them would be a claim that the set is empty,
        // which it is not.
        for body in [
            &b"{}"[..],
            &br#"{"versions":{}}"#[..],
            &b"[]"[..],
            &b"not json"[..],
            &br#"{"versions":[{"version":1}]}"#[..],
            &br#"{"versions":[{"snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#[..],
            &br#"{"versions":[{"version":"1","snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#[..],
            &br#"{"versions":[{"version":1,"snapshot":10101}]}"#[..],
            &br#"{"versions":[{"version":1,"snapshot":"not a hash"}]}"#[..],
        ] {
            let error = parse("http://s/v1/sets/docs/versions", body).unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidData,
                "body {body:?} should not have parsed"
            );
            assert!(
                error.to_string().contains("http://s/v1/sets/docs/versions"),
                "the error should name the address: {error}"
            );
        }
    }

    #[test]
    fn a_number_too_large_for_a_version_is_drift_rather_than_a_truncation() {
        let body = br#"{"versions":[{"version":4294967296,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#;

        let error = parse("http://s/v1", body).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_four_oh_four_carries_the_servers_own_sentence() {
        // Not a paraphrase. The server's `detail` is what the command line
        // would have printed, and that is what the user reads.
        let body = br#"{"type":"about:blank","title":"no backup set at /srv/docs","status":404,"detail":"no backup set at /srv/docs (looked in /srv)"}"#;

        let error = refusal(404, "http://s/v1/sets/nope/versions", body);

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(
            error.to_string(),
            "no backup set at /srv/docs (looked in /srv)"
        );
    }

    #[test]
    fn a_four_oh_four_without_a_problem_document_still_names_the_server() {
        // A proxy in front, or a server that is not this one. The address is
        // then the only fact there is.
        let error = refusal(404, "http://s/v1/sets/nope/versions", b"<html>gone</html>");

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(error.to_string().contains("http://s/v1/sets/nope/versions"));
        assert!(error.to_string().contains("404"));
    }

    #[test]
    fn every_mapped_status_keeps_its_meaning_in_both_directions() {
        // The inverse of the server's `status_for`. Each row here is a status
        // that table produces from a kind, so a change to either side shows up.
        assert_eq!(kind_for(400), std::io::ErrorKind::InvalidInput);
        assert_eq!(kind_for(403), std::io::ErrorKind::PermissionDenied);
        assert_eq!(kind_for(404), std::io::ErrorKind::NotFound);
        assert_eq!(kind_for(409), std::io::ErrorKind::AlreadyExists);
        assert_eq!(kind_for(422), std::io::ErrorKind::InvalidData);
        assert_eq!(kind_for(500), std::io::ErrorKind::Other);
    }

    #[test]
    fn the_title_is_quoted_when_there_is_no_detail() {
        let body = br#"{"title":"the set is gone"}"#;

        assert_eq!(said_by(body).as_deref(), Some("the set is gone"));
        assert_eq!(said_by(b"{}"), None);
        assert_eq!(said_by(b""), None);
        assert_eq!(said_by(br#"{"detail":""}"#), None);
    }
}
