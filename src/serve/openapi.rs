//! The `OpenAPI` document this server serves, and the schema names it must define.
//!
//! **The document is the source of truth, and it is checked in.** It is written
//! by hand, in `openapi/lepatch.json`, and `GET /openapi.json` serves it with
//! [`include_str!`]. Nothing generates it, and that is the decision.
//!
//! ## Why not `utoipa`
//!
//! Because it puts `#[derive(Serialize)]` and `#[derive(ToSchema)]` on **this
//! crate's own types** — on [`Job`](super::Job), on a set name, on a version.
//! Those types are the library's contract. `lepatch`'s other callers reason
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
//! in [`Json`](super::Json), for the reasons above — and `serde_json` appears once
//! as a **dev-dependency**, so `tests/openapi.rs` can parse this document and
//! fail when it stops being true.

/// The document, served verbatim.
///
/// `include_str!` rather than a file read so that a missing or renamed document is
/// a **compile error** rather than a `500` at runtime. A spec that a deploy can
/// lose is not a contract.
pub const DOCUMENT: &str = include_str!("../../openapi/lepatch.json");

/// Every schema name the handlers produce, and the document must define.
///
/// The other half of the drift check. `ROUTES` in the parent module says what the
/// router answers; this says what the bodies are. A schema that is defined and
/// never referenced is a leftover, and one that is referenced and never defined is
/// a dangling `$ref` — so `tests/openapi.rs` checks both directions.
pub const SCHEMAS: &[&str] = &[
    "Error",
    "Job",
    "JobList",
    "JobKind",
    "JobState",
    "MergeRequest",
    "MergedAlready",
    "MergedCommitted",
    "PackPlan",
    "PackRequest",
    "PackSummary",
    "Range",
    "SetSummary",
    "SetList",
    "Upload",
    "UploadCommit",
    "VersionInfo",
    "VersionList",
    "ServerInfo",
];

/// The media type a pack is sent and accepted as.
///
/// **Not `application/octet-stream`.** A client that uploads a pack and a client
/// that uploads a photograph are the same request without it, and the difference
/// is the one thing a client cannot check for itself. It is also what makes a
/// request log readable.
pub(super) const PACK_MEDIA_TYPE: &str = "application/x-lepatch-pack";

/// The media type errors are sent as.
///
/// RFC 9457 names it, and a client switches on it rather than guessing whether a
/// body is an error.
pub(super) const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
