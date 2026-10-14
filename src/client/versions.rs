// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
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
//! on what a refusal means rather than each guessing. That table, the problem
//! reader and the strict field reader live in [`super::error`] and are shared by
//! every other module of this client, because all of them ask the same server
//! the same questions.

use std::io;

use super::{Remote, error::Document, set_path};
use crate::{metadata::Timestamp, storage::BlobId};

/// One version, as the server named it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    /// The set's own number for it.
    pub version: u32,
    /// The snapshot's hash.
    pub snapshot: BlobId,
    /// Time the version ran, in UTC seconds and nanos.
    pub taken_at: Timestamp,
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
    ///
    /// Returns an error if `set` is empty or is not one path component, because
    /// the name goes into the request path before anything is sent.
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
        let body = super::error::read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(super::error::refusal(status.as_u16(), &url, &body));
        }

        parse(&url, &body)
    }
}

/// The listing, read strictly.
///
/// See this module's documentation for why every field is checked rather than
/// taken if it is there. `url` is in every message, so a failure says which
/// server and which set it was about, and [`Document`] puts the entry a field
/// was missing in into the sentence.
fn parse(url: &str, body: &[u8]) -> io::Result<Vec<Version>> {
    let document = Document::read(url, "a version listing", body)?;

    let listed = document.len("versions")?;
    let mut out = Vec::with_capacity(listed);

    for at in 0..listed {
        let entry = document.element("versions", at)?;

        // The stamp parses through UTC text. A stamp the server wrote wrongly
        // is drift, and the error names the server it came from.
        let taken_at = entry.text("taken_at").and_then(|text| {
            Timestamp::parse_utc(text).map_err(|e| io::Error::new(e.kind(), format!("{url}: {e}")))
        })?;

        out.push(Version {
            version: entry.version("version")?,
            snapshot: entry.hash("snapshot")?,
            taken_at,
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{Timestamp, Version, parse};
    use crate::storage::BlobId;

    /// The listing a server sends for a set holding two versions.
    const LISTING: &str = r#"{"versions":[
        {"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101","taken_at":"2023-11-14T22:13:20Z"},
        {"version":2,"snapshot":"0202020202020202020202020202020202020202020202020202020202020202","taken_at":"2023-11-15T22:13:20Z"}
    ]}"#;

    fn one() -> BlobId {
        BlobId::from_hex("0101010101010101010101010101010101010101010101010101010101010101")
            .unwrap()
    }

    fn two() -> BlobId {
        BlobId::from_hex("0202020202020202020202020202020202020202020202020202020202020202")
            .unwrap()
    }

    fn first_time() -> Timestamp {
        Timestamp::parse_utc("2023-11-14T22:13:20Z").unwrap()
    }

    fn second_time() -> Timestamp {
        Timestamp::parse_utc("2023-11-15T22:13:20Z").unwrap()
    }

    #[test]
    fn a_listing_is_read_in_order_with_all_fields() {
        // Field presence is asserted, not assumed: a parser that read `version`
        // and dropped `snapshot` would still produce a list, and this catches
        // it only if the value is compared.
        //
        // **`two()` for the second entry, not `one()` again.** Every entry
        // carrying the *first* entry's hash would satisfy a comparison that used
        // the same value twice, and that is exactly the parser this is meant to
        // reject: one that read `snapshot` off the first object and copied it
        // for the rest. The two entries differ here so that reading it wrongly
        // is visible. The two stamps differ for the same reason.
        let listed = parse("http://s/v1/sets/docs/versions", LISTING.as_bytes()).unwrap();

        assert_eq!(
            listed,
            vec![
                Version {
                    version: 1,
                    snapshot: one(),
                    taken_at: first_time(),
                },
                Version {
                    version: 2,
                    snapshot: two(),
                    taken_at: second_time(),
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
            &br#"{"versions":[{"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#[..],
            &br#"{"versions":[{"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101","taken_at":"yesterday"}]}"#[..],
            &br#"{"versions":[{"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101","taken_at":1700000000}]}"#[..],
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
}
