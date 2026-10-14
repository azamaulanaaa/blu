// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A failure as an HTTP response.
//!
//! Every non-2xx this service sends is RFC 9457 `application/problem+json`. One
//! shape for every error, so a client parses one thing whatever went wrong.
//!
//! ## Why the message is the error's own text
//!
//! [`ApiError::title`] is the [`std::io::Error`]'s `Display`, verbatim. That is
//! not a shortcut; it is the point.
//!
//! This crate's errors are deliberately flat, complete sentences. That is a
//! documented position — see the long note in `command/mod.rs` about why there
//! is no crate-wide error enum and no `caused by:` walk. Those sentences were
//! written for a person reading a terminal, and they are good sentences: a
//! refusal that explains what was refused and why.
//!
//! So the server does not translate them and does not invent a second vocabulary
//! beside them. `lepatch merge` would have said *"refusing to clean your home
//! directory (/home/ann)"*, and the HTTP body says the same. A client shows the
//! `detail` and the user reads the error the tool would have printed, rather
//! than a paraphrase that lost the reason.
//!
//! ## Why the status comes from the kind
//!
//! [`io::ErrorKind`] already classifies what went wrong, and this crate has
//! already chosen its kinds. `NotFound` is a set that is not there.
//! `AlreadyExists` is a claim another run holds. `InvalidData` is a pack that
//! does not open. The mapping in [`status_for`] only *translates*; it does not
//! decide. A new error site that picks a kind gets the right status for free,
//! which is the property that stops a second opinion appearing in a handler.

use std::io;

use actix_web::{HttpResponse, ResponseError, http::StatusCode};

use super::Json;

/// An error with a status and a message a client can show.
///
/// Small on purpose. It is a status code and a sentence, because that is all a
/// failure is here: the work did not happen, and this is why.
#[derive(Debug)]
pub struct ApiError {
    /// The HTTP status to answer with.
    status: StatusCode,
    /// The error's own message, or a sentence written for this case.
    detail: String,
}

impl ApiError {
    /// A failure with an explicit status.
    #[must_use]
    pub fn new(status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
        }
    }

    /// `400`. The request was understood and cannot be acted on.
    #[must_use]
    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, detail)
    }

    /// `404`. The set, version or upload named is not here.
    #[must_use]
    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, detail)
    }

    /// `409`. Something else holds what this would need.
    #[must_use]
    pub fn conflict(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, detail)
    }

    /// `416`. A `Range` this file cannot answer.
    #[must_use]
    pub fn range_not_satisfiable(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::RANGE_NOT_SATISFIABLE, detail)
    }

    /// `422`. The bytes are well-formed HTTP and well-formed JSON, and are not
    /// what they claim to be.
    #[must_use]
    pub fn unprocessable(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, detail)
    }

    /// `500`. Something this crate does not have a word for.
    ///
    /// Reached through [`From<io::Error>`] rather than written by hand, so a
    /// fallible handler propagates with `?` and gets the right status from the
    /// kind it already chose.
    #[must_use]
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, detail)
    }

    /// The status this failure answers with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The problem document, as JSON.
    ///
    /// `type` is `about:blank`, which RFC 9457 defines as "a URI reference
    /// gives no further detail". Nothing here has one to give, and inventing a
    /// stable URL per error would be a taxonomy to keep in step for no gain.
    #[must_use]
    pub fn problem(&self) -> Json {
        Json::object(vec![
            ("type", Json::string("about:blank")),
            ("title", Json::string(self.detail.clone())),
            ("status", Json::int(u64::from(self.status.as_u16()))),
            ("detail", Json::string(self.detail.clone())),
        ])
    }
}

/// The status an [`io::ErrorKind`] answers with.
///
/// The table, in full, so it can be read as one thing rather than found a row
/// at a time.
///
/// | Kind | Status | What it means here |
/// |---|---|---|
/// | `NotFound` | 404 | the set, version or upload named is not there |
/// | `AlreadyExists` | 409 | another run holds the claim, or the job is already finished |
/// | `InvalidInput` | 400 | the request asks for something this refuses |
/// | `PermissionDenied` | 403 | |
/// | `InvalidData` | 422 | the bytes are not what they claim to be |
/// | `UnexpectedEof` | 422 | the file ends early |
/// | `InvalidFilename` | 400 | a set name that is not one path segment |
/// | everything else | 500 | |
///
/// **`InvalidInput` is 400 and not 422**, and the split is worth stating because
/// it is the one judgement in the table. Both kinds mean "refused". The
/// difference is where the fault is: `InvalidInput` is this crate refusing a
/// request it understood — a destination that holds the set being restored from,
/// a set name holding a separator — and the request can be fixed by changing it.
/// `InvalidData` is the *bytes* being wrong, and no change to the request will
/// fix a pack whose index does not decode. RFC 9457's `422` is "well-formed but
/// semantically invalid", which is the second of those.
///
/// Everything unmapped is `500`. That is a deliberate floor rather than an
/// omission: a kind this crate does not produce is a kind nobody has classified
/// here, and inventing a status for it would be a guess about what a client
/// ought to do about a failure nobody has seen.
#[must_use]
pub(super) fn status_for(kind: io::ErrorKind) -> StatusCode {
    match kind {
        io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        io::ErrorKind::AlreadyExists => StatusCode::CONFLICT,
        // `InvalidFilename` alongside `InvalidInput` rather than on its own line
        // below: this crate uses it for an identifier that is not one, and both
        // are a request this server can name and change. The table above says
        // which is which.
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidFilename => StatusCode::BAD_REQUEST,
        io::ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl From<io::Error> for ApiError {
    /// Translate by kind, and carry the message through unchanged.
    ///
    /// The message is `Display` and not a path into a chain. This crate has no
    /// error chain to walk — see `command/mod.rs` — so there is nothing else to
    /// read.
    fn from(error: io::Error) -> Self {
        Self {
            status: status_for(error.kind()),
            detail: error.to_string(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

/// So a handler can answer `Err(e)` with `?` and get a problem document.
///
/// `ResponseError` is actix's hook for this: an `Err` returned out of a handler is
/// rendered by calling the method below. Implementing it once here means no
/// handler writes an error response by hand, which is what would otherwise make
/// the shape a per-handler decision.
impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        self.status
    }

    /// The problem document, with the media type RFC 9457 names.
    ///
    /// `application/problem+json` rather than `application/json`. A client
    /// switches on the type, and a body that is an error under any other name
    /// is a body it has to guess at.
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status)
            .insert_header(("content-type", "application/problem+json"))
            .body(self.problem().to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use actix_web::{ResponseError, http::StatusCode};

    use super::{ApiError, status_for};

    /// An `io::Error` of a kind, carrying a message like the ones this crate
    /// writes.
    fn error(kind: io::ErrorKind) -> io::Error {
        io::Error::new(kind, "refusing to clean your home directory (/home/ann)")
    }

    #[test]
    fn a_kind_becomes_the_status_the_table_gives_it() {
        // Every row of the table, so a change to one of them is a visible change
        // rather than a silent one.
        assert_eq!(status_for(io::ErrorKind::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(
            status_for(io::ErrorKind::AlreadyExists),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(io::ErrorKind::InvalidInput),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(io::ErrorKind::PermissionDenied),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_for(io::ErrorKind::InvalidData),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            status_for(io::ErrorKind::UnexpectedEof),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            status_for(io::ErrorKind::InvalidFilename),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(io::ErrorKind::InvalidData),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[test]
    fn an_unmapped_kind_is_a_five_hundred() {
        // A floor, not a gap. Whatever this crate has not classified, a client
        // is told the service failed rather than that its request was wrong.
        assert_eq!(
            status_for(io::ErrorKind::BrokenPipe),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status_for(io::ErrorKind::OutOfMemory),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn an_io_error_carries_its_own_sentence_through() {
        // The property the whole module is for. A client reads the message the
        // command line would have printed, not a paraphrase of it.
        let api = ApiError::from(error(io::ErrorKind::InvalidInput));

        assert_eq!(api.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            api.detail,
            "refusing to clean your home directory (/home/ann)"
        );
    }

    #[test]
    fn a_problem_document_is_rfc_9457_shaped() {
        // `type`, `title`, `status`, `detail`. A client switches on the type,
        // so the four members have to be there and under those names.
        let document = ApiError::from(error(io::ErrorKind::NotFound)).problem();

        assert_eq!(
            document.to_string(),
            r#"{"type":"about:blank","title":"refusing to clean your home directory (/home/ann)","status":404,"detail":"refusing to clean your home directory (/home/ann)"}"#
        );
    }

    #[test]
    fn the_status_in_the_document_matches_the_status_of_the_response() {
        // Two copies of one number. They must agree, or a client that reads the
        // body and a client that reads the status line get different answers
        // about the same failure.
        let api = ApiError::unprocessable("the pack does not open");
        let rendered = api.problem().to_string();

        assert!(rendered.contains("\"status\":422"));
        assert_eq!(api.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn an_error_response_is_problem_json() {
        // The media type is the switch a client makes on, so it is part of the
        // contract rather than a detail.
        let response = ApiError::not_found("no backup set at docs").error_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/problem+json")
        );
    }

    #[test]
    fn a_range_failure_is_four_sixteen() {
        // Its own constructor, because `io::ErrorKind` has no kind that means
        // "the range you asked for is not in this file". It is not an error
        // about the file; it is an error about the request, and the file is
        // fine.
        let api = ApiError::range_not_satisfiable("the pack is 10 bytes long");

        assert_eq!(api.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[test]
    fn a_constructed_error_keeps_the_message_it_was_given() {
        // Not every failure comes from an `io::Error`. A request this crate
        // refuses before touching the disk has no `io::Error` behind it.
        let api = ApiError::bad_request("a set name may not hold a separator");

        assert_eq!(api.status(), StatusCode::BAD_REQUEST);
        assert_eq!(api.to_string(), "a set name may not hold a separator");
    }
}
