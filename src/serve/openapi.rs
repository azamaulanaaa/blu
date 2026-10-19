//! The `OpenAPI` document this server serves, and the schema names it must define.
//!
//! **The document is the source of truth, and it is checked in.** It is written
//! by hand, in `openapi/blu.json`, and `GET /openapi.json` serves it with
//! [`include_str!`]. Nothing generates it, and that is the decision.
//!
//! ## Why not `utoipa`
//!
//! Because it puts `#[derive(Serialize)]` and `#[derive(ToSchema)]` on **this
//! crate's own types** — on [`Job`](super::Job), on a set name, on a version.
//! Those types are the library's contract. `blu`'s other callers reason
//! about them, and a JSON concern on them is a second thing to keep in step with
//! the first, in a crate that has gone out of its way to have one encoding.
//!
//! And its anti-drift guarantee is weaker than it looks. A handler omitted from
//! `#[openapi(paths(…))]` still produces no schema — which is exactly the drift a
//! checked-in file can have, except that the checked-in file is a diff someone
//! read.
//!
//! A `build.rs` emitter would be worse still: the document would stop being
//! reviewable in a diff, and reviewable bytes are this repository's whole
//! documentation practice.
//!
//! ## Why the boundary is exempt from the no-serde rule
//!
//! `Cargo.toml` explains why `bincode` was removed: so that no upstream's release
//! cadence or security policy can decide whether a backup set is still
//! readable. **That argument is about the on-disk format.** A response body is
//! not read by a future version of this binary. It is read by a client, and it is
//! versioned by this document, which says what every field means and what every
//! status is for.
//!
//! So the boundary is exempt. The rule is kept anyway — the JSON is hand-encoded
//! in [`Json`](super::Json), for the reasons above. `serde_json` is a direct
//! dependency for *parsing request bodies*; it is also what `tests/openapi.rs`
//! uses to parse this document and fail when it stops being true.

/// The document, served verbatim.
///
/// `include_str!` rather than a file read so that a missing or renamed document is
/// a **compile error** rather than a `500` at runtime. A spec that a deploy can
/// lose is not a contract.
pub const DOCUMENT: &str = include_str!("../../openapi/blu.json");

/// Every schema name in `components.schemas`, mirrored from the code side.
///
/// The document is the source of truth, so this list does not define the API. It
/// restates the schema set so that `tests/openapi.rs` can compare the code with
/// the document and name the schema that drifted. The list holds request schemas
/// as well as response schemas. A handler reads `MergeRequest`, `PackRequest`
/// and `RestorePlan`, and writes `Job`.
///
/// `ROUTES` in the parent module mirrors the operation set in the same way.
///
/// Keep the list sorted. One added or one removed schema then reads as one line
/// in a diff, and a reviewer sees a missing entry instead of a reshuffled list.
pub const SCHEMAS: &[&str] = &[
    "Accepted",
    "Error",
    "HistoryEvent",
    "HistoryList",
    "Job",
    "JobKind",
    "JobList",
    "JobState",
    "MergeRequest",
    "MergedAlready",
    "MergedCommitted",
    "PackPlan",
    "PackRequest",
    "PackSummary",
    "Range",
    "RestorePlan",
    "ServerInfo",
    "SetList",
    "SetSummary",
    "Upload",
    "UploadCommit",
    "Version",
    "VersionInfo",
    "VersionList",
];

/// The media type a pack is sent and accepted as.
///
/// **Not `application/octet-stream`.** A client that uploads a pack and a client
/// that uploads a photograph are the same request without it, and the difference
/// is the one thing a client cannot check for itself. It is also what makes a
/// request log readable.
pub(super) const PACK_MEDIA_TYPE: &str = "application/x-blu-pack";

/// The media type errors are sent as.
///
/// RFC 9457 names it, and a client switches on it rather than guessing whether a
/// body is an error.
pub(super) const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
