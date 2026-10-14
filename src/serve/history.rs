// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! File history for requested paths, answered in one pass.
//!
//! One request names the paths and gets the events: the versions where they
//! changed, oldest first. The server loads each snapshot once and diffs in
//! memory, so a client learns the history without fetching a pack per version.
//!
//! ## The numbering is complete, and that is not the listing's answer
//!
//! [`Versions`](super::versions) lists what the index files name, which is a
//! lower bound: a version whose index was never written still exists and still
//! restores. History reads the numbering off the store's own commit records
//! instead, so every version reports and none is silently skipped.

use std::{io, path::PathBuf};

use crate::{
    command,
    metadata::Timestamp,
    storage::{BlobId, BlobStore, Compression, Read},
};

/// One reported change, ready to encode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Found {
    /// The set's own number for the version.
    pub version: u32,
    /// The snapshot that version names.
    pub snapshot: BlobId,
    /// The time that version ran.
    pub taken_at: Timestamp,
    /// The path relative to the backup root.
    pub path: String,
    /// `created`, `modified` or `deleted`.
    pub status: &'static str,
    /// `content`, `type` or `metadata`, set only for `modified`.
    pub detail: Option<&'static str>,
}

/// Reports the history of `files` in the set `store` reads, oldest first.
///
/// Only versions where at least one requested path changed appear.
///
/// # Errors
///
/// Returns an error if a `files` value is empty, absolute, or holds `..`.
/// Returns an error if a snapshot cannot be read, decoded, or fingerprinted.
pub(super) async fn report(
    store: &BlobStore<Read, Compression>,
    files: &[String],
) -> io::Result<Vec<Found>> {
    let raw: Vec<PathBuf> = files.iter().map(PathBuf::from).collect();

    let versions = store.versions()?;
    let mut named = Vec::with_capacity(versions.len());
    for version in versions {
        named.push((version, store.snapshot(version)?));
    }

    let events = command::report(store, &named, &raw).await?;

    Ok(events
        .into_iter()
        .map(|event| Found {
            version: event.version,
            snapshot: event.snapshot,
            taken_at: event.taken_at,
            path: event.path.to_string_lossy().into_owned(),
            status: event.status.word(),
            detail: event.detail.map(command::Detail::word),
        })
        .collect())
}

/// Reads the repeated `files` values out of a query string.
///
/// Values ride percent-encoded, by the same rule that encodes a set name. Any
/// other key is ignored: a client that sends more than history needs is a
/// client from the future, not a client that is wrong.
///
/// # Errors
///
/// Returns a `400` naming the value for a truncated encoding, a byte pair that
/// is not hexadecimal, or a value that is not UTF-8 once decoded. A `+` is a
/// literal plus: spaces travel as `%20`.
pub(super) fn parse_query(query: &str) -> Result<Vec<String>, super::ApiError> {
    let mut out = Vec::new();
    if query.is_empty() {
        return Ok(out);
    }

    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name != "files" {
            continue;
        }
        out.push(decode(value)?);
    }

    Ok(out)
}

/// One percent-encoded query value, decoded.
///
/// # Errors
///
/// Returns a `400` as [`parse_query`] describes.
fn decode(value: &str) -> Result<String, super::ApiError> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;

    while at < bytes.len() {
        if bytes[at] != b'%' {
            out.push(bytes[at]);
            at += 1;
            continue;
        }
        if at + 2 >= bytes.len() {
            return Err(super::ApiError::bad_request(format!(
                "{value:?} ends in an incomplete percent-encoding"
            )));
        }
        let hex = &value[at + 1..at + 3];
        let byte = u8::from_str_radix(hex, 16).map_err(|_| {
            super::ApiError::bad_request(format!(
                "{value:?} holds %{hex}, which is not percent-encoding"
            ))
        })?;
        out.push(byte);
        at += 3;
    }

    String::from_utf8(out)
        .map_err(|_| super::ApiError::bad_request(format!("{value:?} is not UTF-8 once decoded")))
}

#[cfg(test)]
mod tests {
    use super::parse_query;

    #[test]
    fn repeated_files_values_come_back_in_order() {
        let files = parse_query("files=src%2Fmain.rs&files=README.md").unwrap();

        assert_eq!(files, vec!["src/main.rs", "README.md"]);
    }

    #[test]
    fn other_keys_are_ignored_and_an_empty_query_is_empty() {
        assert_eq!(
            parse_query("since=3&files=a").unwrap(),
            vec!["a".to_string()]
        );
        assert_eq!(parse_query("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_truncated_or_foreign_encoding_is_a_bad_request() {
        for query in ["files=abc%", "files=abc%2", "files=%zz", "files=%FF%FE"] {
            assert!(
                parse_query(query).is_err(),
                "{query} should not have decoded"
            );
        }
    }
}
