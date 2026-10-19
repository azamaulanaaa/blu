//! `GET /v1/sets/{set}/history` — the versions where requested paths changed.
//!
//! ## The answer is what a local report gives
//!
//! The server diffs in one pass and answers the events, so this parses into
//! the same [`Event`](crate::command::Event) values a local report produces,
//! in the same order, and the caller prints them with the same code.
//!
//! ## Why the query carries the paths
//!
//! The request mirrors the command line: one `files` value per `--files`
//! value, percent-encoded by the same rule that encodes a set name. A history
//! is a read, so it is a `GET` rather than a job. The events are small lines,
//! so one response carries all of them.
//!
//! ## Why an unknown shape is an error
//!
//! **A response this client cannot read must not become an empty report.**
//! That is the one way a remote report could quietly lie. So every field is
//! asked for by name and its type checked, and the `status` and `detail` words
//! are read through the same vocabulary the local report writes. Anything else
//! is [`io::ErrorKind::InvalidData`] naming what was wrong, and nothing is
//! printed. An empty list is a thing the server can say (`"events": []`); it
//! is not a thing this client infers from silence.

use std::io;

use super::{Remote, error::Document, set_path};
use crate::{
    command::{Detail, Event, Status},
    metadata::Timestamp,
};

impl Remote {
    /// The versions of `set` where `files` changed, oldest first.
    ///
    /// **This future is `!Send`, and that is `awc`'s requirement rather than
    /// this crate's.** See [`Remote::versions`] for why a caller on a
    /// multi-threaded runtime enters a [`tokio::task::LocalSet`].
    ///
    /// # Errors
    ///
    /// Returns an error if no path was named, if the server cannot be reached,
    /// if it refuses the set, or if the answer is not the shape described in
    /// this module. Every one of those messages names the server, and none of
    /// them is an empty list.
    ///
    /// Returns an error if `set` is empty or is not one path component, because
    /// the name goes into the request path before anything is sent.
    pub async fn history(&self, set: &str, files: &[&str]) -> io::Result<Vec<Event>> {
        if files.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a history request names at least one path. Pass the paths `--files` holds",
            ));
        }
        tokio::task::LocalSet::new()
            .run_until(self.history_inner(set, files))
            .await
    }

    /// The request itself, for [`Remote::history`] to run on a
    /// [`tokio::task::LocalSet`].
    async fn history_inner(&self, set: &str, files: &[&str]) -> io::Result<Vec<Event>> {
        let mut url = self.url(&format!("/v1/sets/{}/history", set_path(set)?))?;
        for (at, file) in files.iter().enumerate() {
            url.push(if at == 0 { '?' } else { '&' });
            url.push_str("files=");
            url.push_str(&super::encode_component(file));
        }

        let mut response = self
            .authorize(self.client.get(&url))
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

/// The report, read strictly.
///
/// See this module's documentation for why every field is checked rather than
/// taken if it is there. `url` is in every message, so a failure says which
/// server and which set it was about.
fn parse(url: &str, body: &[u8]) -> io::Result<Vec<Event>> {
    let document = Document::read(url, "a file history", body)?;

    let count = document.len("events")?;
    let mut out = Vec::with_capacity(count);

    for at in 0..count {
        let entry = document.element("events", at)?;

        // The stamp parses through UTC text. A stamp the server wrote wrongly
        // is drift, and the error names the server it came from.
        let taken_at = entry.text("taken_at").and_then(|text| {
            Timestamp::parse_utc(text).map_err(|e| io::Error::new(e.kind(), format!("{url}: {e}")))
        })?;

        let status = entry.text("status").and_then(|word| {
            Status::parse(word).ok_or_else(|| {
                super::error::drift(
                    url,
                    "a file history",
                    &format!("`status` is {word:?}, and history events carry created, modified or deleted"),
                )
            })
        })?;

        let detail = entry.optional_text("detail").and_then(|word| {
            word.map(|found| {
                Detail::parse(found).ok_or_else(|| {
                    super::error::drift(
                        url,
                        "a file history",
                        &format!("`detail` is {found:?}, and history events carry content, type or metadata"),
                    )
                })
            })
            .transpose()
        })?;

        out.push(Event {
            version: entry.version("version")?,
            snapshot: entry.hash("snapshot")?,
            taken_at,
            path: std::path::PathBuf::from(entry.text("path")?),
            status,
            detail,
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::parse;
    use crate::{
        command::{Detail, Status},
        metadata::Timestamp,
        storage::BlobId,
    };

    /// The report a server sends for a path created once and changed once.
    const REPORT: &str = r#"{"events":[
        {"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101","taken_at":"2023-11-14T22:13:20Z","path":"src/main.rs","status":"created","detail":null},
        {"version":2,"snapshot":"0202020202020202020202020202020202020202020202020202020202020202","taken_at":"2023-11-15T22:13:20Z","path":"src/main.rs","status":"modified","detail":"content"}
    ]}"#;

    fn one() -> BlobId {
        BlobId::from_hex("0101010101010101010101010101010101010101010101010101010101010101")
            .unwrap()
    }

    #[test]
    fn a_report_reads_in_order_with_all_fields() {
        let events = parse("http://s/v1/sets/docs/history", REPORT.as_bytes()).unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].version, 1);
        assert_eq!(events[0].snapshot, one());
        assert_eq!(events[0].status, Status::Created);
        assert_eq!(events[0].detail, None);
        assert_eq!(events[0].path, std::path::PathBuf::from("src/main.rs"));
        assert_eq!(
            events[0].taken_at,
            Timestamp::parse_utc("2023-11-14T22:13:20Z").unwrap()
        );
        assert_eq!(events[1].status, Status::Modified);
        assert_eq!(events[1].detail, Some(Detail::Content));
    }

    #[test]
    fn an_empty_report_is_an_empty_list() {
        let events = parse("http://s/v1/sets/docs/history", br#"{"events":[]}"#).unwrap();

        assert_eq!(events, Vec::new());
    }

    #[test]
    fn a_document_that_is_not_the_shape_is_an_error_not_an_empty_list() {
        for body in [
            &b"{}"[..],
            &br#"{"events":{}}"#[..],
            &b"[]"[..],
            &b"not json"[..],
            &br#"{"events":[{"version":1}]}"#[..],
            &br#"{"events":[{"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101","taken_at":"2023-11-14T22:13:20Z","path":"a","status":"renamed","detail":null}]}"#[..],
            &br#"{"events":[{"version":1,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101","taken_at":"2023-11-14T22:13:20Z","path":"a","status":"modified","detail":"moved"}]}"#[..],
        ] {
            let error = parse("http://s/v1/sets/docs/history", body).unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidData,
                "body {body:?} should not have parsed"
            );
            assert!(
                error.to_string().contains("http://s/v1/sets/docs/history"),
                "the error should name the address: {error}"
            );
        }
    }
}
