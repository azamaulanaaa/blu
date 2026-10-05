//! The routes.
//!
//! Every handler is a **private function**. Two of this crate's lint rules make
//! that the right shape rather than a matter of taste:
//!
//! * `missing_docs = "deny"` applies to public items. A handler's signature is
//!   actix's, not ours — `HttpRequest`, `web::Path`, `HttpResponse` — so a doc
//!   comment on one describes machinery. The design that a handler *is* lives in
//!   this module's parent and in the `OpenAPI` document.
//! * `unreachable_pub = "deny"` rejects a `pub` handler. Actix's macros make the
//!   function reachable only through the router, so `pub` would be claiming a
//!   reachability the crate does not have.
//!
//! ## Reading a body
//!
//! Bodies are read whole, into a `Vec`, and then parsed. A `POST` here carries a
//! set of options — a version, a compression policy, a list of chunk hashes — and
//! not a file. **The one body that can be large is a pack**, and that arrives at
//! `PUT /v1/uploads/{id}`, which is a different endpoint for the reason
//! [`upload`](super::upload) gives.

use actix_web::{
    HttpRequest, HttpResponse, ResponseError as _,
    http::header::{CONTENT_RANGE, CONTENT_TYPE, RANGE},
    web,
};
use futures_util::StreamExt as _;
use std::io::{Read as _, Seek as _, SeekFrom};

use super::{
    ApiError, ByteRange, JobEvent, JobId, Json, Requested, Sets, Uploads, jobs::State, openapi,
    pack::Resolved, versions,
};

/// The media type of every non-error response that is JSON.
const JSON: &str = "application/json";

/// The largest pack this server will send in a single un-ranged response.
///
/// **Why there is a bound at all.** `GET /v1/packs/{id}` with no `Range` header is
/// the one response this service builds in memory: the pack's length is known
/// from its geometry, so `content-length` is right, but the body itself is a
/// `Vec` the size of the file (see [`read_range`]). This server has no
/// authentication and its bind address is chosen by whoever starts it, so "the
/// client asked for a big file" is not a limit anything can rely on — a handful
/// of concurrent whole-pack `GET`s against a large version is enough to take the
/// process out, and nothing in the request is the server's to refuse.
///
/// **Why this number.** It is a line an operator can reason about rather than a
/// property of anything: comfortably above every pack this crate's own suite
/// builds, and below the point where a `Vec` per in-flight request matters next
/// to the set itself. A version bigger than this is not served whole by design —
/// `POST /v1/packs/{id}/plan` is the documented way to pull one, it hands the
/// client the exact ranges it lacks, and it allocates nothing like this. So the
/// refusal is not a limitation the client works around; it is the supported flow
/// being pointed at.
const WHOLE_PACK_LIMIT: u64 = 64 * 1024 * 1024;

/// A `200` carrying a JSON document.
fn ok(document: &Json) -> HttpResponse {
    HttpResponse::Ok()
        .insert_header((CONTENT_TYPE, JSON))
        .body(document.to_string())
}

/// A `202`, for work that was accepted and is running.
///
/// The `Location` header is what a client follows to poll, and it is there so
/// that a client does not have to know this server's URL layout to find its job.
fn accepted(id: JobId) -> HttpResponse {
    HttpResponse::Accepted()
        .insert_header((CONTENT_TYPE, JSON))
        .insert_header(("location", format!("/v1/jobs/{id}")))
        .body(Json::object(vec![("job", Json::string(id.hex()))]).to_string())
}

/// A `204`, for a request that did something and has nothing to report.
fn no_content() -> HttpResponse {
    HttpResponse::NoContent().finish()
}

/// Build a JSON response from a fallible handler's error.
///
/// `?` in a handler propagates an [`ApiError`], and actix renders it through
/// [`ResponseError::error_response`], which is where the problem document is
/// written. This is here for the places that build a response by hand and would
/// otherwise have to remember to turn a status into a body.
fn error(failure: &ApiError) -> HttpResponse {
    failure.error_response()
}

/// Request bodies, read by hand.
///
/// A body here is a handful of fields. Deriving for them would put a
/// serialisation trait on types that are not the crate's, for eleven fields, and
/// the crate has no serialisation crate — see [`openapi`] for why the API
/// boundary is exempt from that rule but keeps its shape.
mod serde_lite {
    use super::Json;

    /// `POST /v1/sets/{set}/packs`.
    #[derive(Debug, Default)]
    pub(super) struct PackRequest {
        /// Which version, or `None` for the newest that resolves.
        pub version: Option<u32>,
        /// The version the pack assumes its reader holds, for a delta.
        pub base: Option<u32>,
        /// Always `keep`, and refused otherwise.
        pub compression: Option<String>,
    }

    /// `POST /v1/packs/{id}/plan`.
    #[derive(Debug, Default)]
    pub(super) struct PlanRequest {
        /// Chunk hashes the client already holds.
        pub have: Vec<String>,
    }

    /// `POST /v1/sets/{set}/merges`.
    #[derive(Debug, Default)]
    pub(super) struct MergeRequest {
        /// The upload to merge.
        pub upload: String,
        /// `keep`, or a level from 0 to 9.
        pub compression: Option<String>,
    }

    impl PackRequest {
        /// Read a body into this.
        ///
        /// # Errors
        ///
        /// Returns an error if the body is not a JSON object, or if a field is
        /// the wrong type. Both are `400`: the request was understood as a shape
        /// and does not fit it.
        pub(super) fn read(body: &str) -> Result<Self, super::ApiError> {
            let fields = super::parse_fields(body)?;

            Ok(Self {
                version: number(&fields, "version")?,
                base: number(&fields, "base")?,
                compression: text(&fields, "compression")?,
            })
        }
    }

    impl PlanRequest {
        /// Read a body into this.
        ///
        /// # Errors
        ///
        /// As [`PackRequest::read`].
        pub(super) fn read(body: &str) -> Result<Self, super::ApiError> {
            let fields = super::parse_fields(body)?;

            let have = match fields.iter().find(|(name, _)| name == "have") {
                None => Vec::new(),
                Some((_, Json::Array(items))) => items
                    .iter()
                    .map(|item| match item {
                        Json::Str(text) => Ok(text.clone()),
                        _ => Err(super::ApiError::bad_request(
                            "`have` must be a list of chunk hashes",
                        )),
                    })
                    .collect::<Result<Vec<String>, super::ApiError>>()?,
                Some(_) => {
                    return Err(super::ApiError::bad_request(
                        "`have` must be a list of chunk hashes",
                    ));
                }
            };

            Ok(Self { have })
        }
    }

    impl MergeRequest {
        /// Read a body into this.
        ///
        /// # Errors
        ///
        /// As [`PackRequest::read`], and also if `upload` is missing, since a
        /// merge with no pack to merge is not a request.
        pub(super) fn read(body: &str) -> Result<Self, super::ApiError> {
            let fields = super::parse_fields(body)?;

            Ok(Self {
                upload: text(&fields, "upload")?.ok_or_else(|| {
                    super::ApiError::bad_request("a merge must name an upload to merge")
                })?,
                compression: text(&fields, "compression")?,
            })
        }
    }

    fn number(fields: &[(String, Json)], name: &str) -> Result<Option<u32>, super::ApiError> {
        match fields.iter().find(|(field, _)| field == name) {
            None => Ok(None),
            Some((_, Json::Int(value))) => Ok(Some(u32::try_from(*value).map_err(|_| {
                super::ApiError::bad_request(format!("`{name}` is not a version number"))
            })?)),
            Some(_) => Err(super::ApiError::bad_request(format!(
                "`{name}` must be a version number"
            ))),
        }
    }

    fn text(fields: &[(String, Json)], name: &str) -> Result<Option<String>, super::ApiError> {
        match fields.iter().find(|(field, _)| field == name) {
            None => Ok(None),
            Some((_, Json::Str(value))) => Ok(Some(value.clone())),
            Some(_) => Err(super::ApiError::bad_request(format!(
                "`{name}` must be a string"
            ))),
        }
    }
}

/// The whole router, built from [`ROUTES`](super::ROUTES)' paths.
///
/// The macro list and `ROUTES` are written side by side and `tests/openapi.rs`
/// checks them against the document in both directions. A new route is one line
/// in three places, and forgetting any of them fails a test rather than
/// producing a server that answers something the spec does not mention.
pub(super) fn configure(
    config: &super::Config,
    jobs: &super::Jobs,
) -> std::io::Result<(actix_web::dev::Server, std::net::SocketAddr)> {
    // The shared state is built **once** and cloned into each worker. The `App`
    // itself is built inside the factory, because actix needs a fresh one per
    // worker and `App` is deliberately not `Clone` — sharing it would share the
    // router's state too, which is exactly what it must not do.
    let sets = web::Data::new(super::Sets::new(config.root.clone()));
    let uploads = web::Data::new(super::Uploads::new(config.scratch.clone()));
    let jobs = web::Data::new(jobs.clone());

    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(sets.clone())
            .app_data(uploads.clone())
            .app_data(jobs.clone())
            .service(openapi_document)
            .service(server_info)
            .service(list_sets)
            .service(list_versions)
            .service(show_version)
            .service(request_pack)
            .service(get_pack)
            .service(plan_pack)
            .service(begin_upload)
            .service(fill_upload)
            .service(commit_upload)
            .service(drop_upload)
            .service(merge_pack)
            .service(list_jobs)
            .service(show_job)
            .service(job_events)
            .service(cancel_job)
    })
    .bind(&config.bind)?;

    // Read before `run`, because `run` consumes the server and blocks.
    let address = server.addrs().first().copied().ok_or_else(|| {
        std::io::Error::other(format!("bound to {} but reported no address", config.bind))
    })?;

    Ok((server.run(), address))
}

/// `GET /openapi.json` — the document, verbatim.
///
/// Served as `include_str!` and not generated, so what a client reads is what is
/// in the repository and what `tests/openapi.rs` checked.
#[actix_web::get("/openapi.json")]
async fn openapi_document() -> HttpResponse {
    HttpResponse::Ok()
        .insert_header((CONTENT_TYPE, "application/json"))
        .body(openapi::DOCUMENT.to_string())
}

/// `GET /v1/version` — what this build is, and what it can read.
///
/// **A client cannot work out any of this from a pack fetch.** A pack this build
/// cannot read is refused with a message naming the version it wanted, and the
/// version it wants is not in the pack. So the answer has to come from somewhere
/// else, and this is it: a client asks, compares, and knows before it uploads
/// anything.
#[actix_web::get("/v1/version")]
async fn server_info() -> HttpResponse {
    ok(&Json::object(vec![
        ("version", Json::string(env!("CARGO_PKG_VERSION"))),
        (
            "snapshot_format",
            Json::int(u64::from(crate::metadata::FORMAT_VERSION)),
        ),
        (
            "record_format",
            Json::int(u64::from(crate::storage::FORMAT_VERSION)),
        ),
        (
            "pack_format",
            Json::int(u64::from(crate::pack::FORMAT_VERSION)),
        ),
    ]))
}

/// `GET /v1/sets` — the sets under the root.
#[actix_web::get("/v1/sets")]
async fn list_sets(sets: web::Data<Sets>) -> HttpResponse {
    match sets.list() {
        Ok(found) => {
            let listed: Vec<Json> = found
                .into_iter()
                .map(|(name, versions, segments)| {
                    Json::object(vec![
                        ("name", Json::string(name)),
                        ("versions", Json::int(versions as u64)),
                        ("segments", Json::int(segments as u64)),
                    ])
                })
                .collect();

            ok(&Json::object(vec![("sets", Json::Array(listed))]))
        }
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `GET /v1/sets/{set}/versions` — the versions a set's index files name.
///
/// A lower bound, and the response says so. See [`versions`] for why.
#[actix_web::get("/v1/sets/{set}/versions")]
async fn list_versions(sets: web::Data<Sets>, path: web::Path<String>) -> HttpResponse {
    let name = path.into_inner();

    match sets.base(&name).and_then(|base| versions::indexed(&base)) {
        Ok(listed) => {
            let store = match sets.reader(&name).await {
                Ok(store) => store,
                Err(e) => return error(&ApiError::from(e)),
            };

            // Every snapshot hash comes through the store rather than off the
            // index file, for the reason `versions::indexed` gives. A version
            // the store cannot serve is skipped rather than named.
            let mut out = Vec::new();
            for listed in listed {
                let Ok(snapshot) = store.snapshot_via_index(listed.version) else {
                    continue;
                };

                out.push(Json::object(vec![
                    ("version", Json::int(u64::from(listed.version))),
                    ("snapshot", Json::string(snapshot.to_hex().to_string())),
                ]));
            }

            ok(&Json::object(vec![("versions", Json::Array(out))]))
        }
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `GET /v1/sets/{set}/versions/{version}` — one version's figures.
#[actix_web::get("/v1/sets/{set}/versions/{version}")]
async fn show_version(sets: web::Data<Sets>, path: web::Path<(String, u32)>) -> HttpResponse {
    let (name, version) = path.into_inner();

    let result = async {
        let store = sets.reader(&name).await?;
        versions::resolve(&store, version).await
    }
    .await;

    match result {
        Ok(found) => {
            // The pack id is handed over rather than left for the client to
            // build. It encodes this set and this version, and a client that
            // assembled it from the URL rules would have to keep those rules in
            // step with this file.
            let requested = Requested {
                set: name,
                version: Some(found.version),
                base: None,
            };

            ok(&Json::object(vec![
                ("version", Json::int(u64::from(found.version))),
                (
                    "snapshot",
                    Json::string(found.snapshot.to_hex().to_string()),
                ),
                ("chunks", Json::int(found.chunks as u64)),
                ("content_bytes", Json::int(found.content_bytes)),
                ("pack", Json::opt(id_for(&requested).map(Json::string))),
            ]))
        }
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `POST /v1/sets/{set}/packs` — ask for a pack of a version.
///
/// A `202` with a job, because **answering this is work.** Even the `keep` case
/// has to walk the version and resolve every chunk against the store, and a
/// client that asked for a version of a large set would otherwise hold a
/// connection open for the length of that walk.
#[actix_web::post("/v1/sets/{set}/packs")]
async fn request_pack(
    sets: web::Data<Sets>,
    jobs: web::Data<super::Jobs>,
    path: web::Path<String>,
    body: web::Bytes,
) -> HttpResponse {
    let name = path.into_inner();
    let request = match serde_lite::PackRequest::read(&String::from_utf8_lossy(&body)) {
        Ok(request) => request,
        Err(e) => return error(&e),
    };

    if let Some(policy) = request.compression.as_deref()
        && !policy.eq_ignore_ascii_case("keep")
    {
        return error(&ApiError::unprocessable(format!(
            "compression {policy:?} cannot be served from a set: a re-encode is not in the set's \
             bytes. `keep` is the only policy a pack resource supports, and it is the \
             default on both `lepatch pack` and `lepatch merge`"
        )));
    }

    let sets_for_job = sets.clone();
    let requested = Requested {
        set: name.clone(),
        version: request.version,
        base: request.base,
    };

    // The job's span is entered by `Jobs::spawn`, which is where the id exists.
    // See `spawn`.
    let id = jobs.spawn("pack", move |_handle| async move {
        // `version: null` means "the newest one that resolves", which is what
        // the CLI does with no `--version`.
        let resolved = super::pack::resolve(&sets_for_job, &requested).await;

        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(e) => return Err(e.to_string()),
        };

        Ok(Json::object(vec![
            ("set", Json::string(requested.set)),
            ("version", Json::int(u64::from(resolved.version))),
            ("length", Json::int(resolved.pack.file_len())),
            ("payload_start", Json::int(resolved.pack.payload_start())),
            ("payload_bytes", Json::int(resolved.pack.payload_len())),
            ("chunks", Json::int(resolved.pack.chunks() as u64)),
            (
                "snapshot",
                Json::string(resolved.snapshot.to_hex().to_string()),
            ),
        ]))
    });

    accepted(id)
}

/// `GET /v1/packs/{id}` — the bytes, or a range of them.
///
/// The `id` names `(set, version, base)` and nothing is stored for it, so this is
/// a computation every time. See [`pack`](super::pack) for why that is the right
/// trade.
#[actix_web::get("/v1/packs/{id}")]
async fn get_pack(
    sets: web::Data<Sets>,
    request: HttpRequest,
    path: web::Path<String>,
) -> HttpResponse {
    let requested = match requested_from(&path.into_inner()) {
        Ok(requested) => requested,
        Err(e) => return error(&e),
    };

    // **The Range header is parsed before the set is resolved.** A malformed
    // range is a `400` whatever the set is, and saying so first means a client
    // that has the wrong syntax is told that rather than being sent to look for a
    // set that may or may not be there. It also keeps the refusal independent of
    // the server's state, which is what makes the two answers reproducible.
    let asked = match header_range(&request) {
        Some(header) => match ByteRange::parse(&header) {
            Ok(parsed) => Some(parsed),
            Err(e) => return error(&ApiError::from(e)),
        },
        None => None,
    };

    let resolved = match super::pack::resolve(&sets, &requested).await {
        Ok(resolved) => resolved,
        Err(e) => return error(&ApiError::from(e)),
    };

    serve_range(&resolved, asked)
}

/// `POST /v1/packs/{id}/plan` — what a client holding some chunks still needs.
///
/// The answer is [`VirtualPack::fetch_plan`](crate::pack::VirtualPack::fetch_plan):
/// the byte ranges of the chunks the client lacks, **plus** the header, snapshot,
/// index and trailer. The union matters — `needed` alone is a payload with no
/// trailer, and `Pack::open` reads the trailer first, so a client that fetched
/// only it would be refused on a file it had already paid for.
#[actix_web::post("/v1/packs/{id}/plan")]
async fn plan_pack(
    sets: web::Data<Sets>,
    path: web::Path<String>,
    body: web::Bytes,
) -> HttpResponse {
    let requested = match requested_from(&path.into_inner()) {
        Ok(requested) => requested,
        Err(e) => return error(&e),
    };

    let resolved = match super::pack::resolve(&sets, &requested).await {
        Ok(resolved) => resolved,
        Err(e) => return error(&ApiError::from(e)),
    };

    let plan = match serde_lite::PlanRequest::read(&String::from_utf8_lossy(&body)) {
        Ok(plan) => plan,
        Err(e) => return error(&e),
    };

    let ranges = match super::pack::fetch_plan(&resolved, &plan.have) {
        Ok(ranges) => ranges,
        Err(e) => return error(&ApiError::from(e)),
    };

    let listed: Vec<Json> = ranges
        .iter()
        .map(|range| {
            Json::object(vec![
                ("start", Json::int(range.start)),
                ("end", Json::int(range.end)),
            ])
        })
        .collect();

    let total = ranges
        .iter()
        .fold(0u64, |total, range| total.saturating_add(range.len()));

    ok(&Json::object(vec![
        ("length", Json::int(resolved.pack.file_len())),
        ("bytes", Json::int(total)),
        ("ranges", Json::Array(listed)),
    ]))
}

/// `POST /v1/uploads` — begin an upload.
#[actix_web::post("/v1/uploads")]
async fn begin_upload(uploads: web::Data<Uploads>) -> HttpResponse {
    let id = JobId::new().hex();

    match uploads.begin(&id).await {
        Ok(_) => HttpResponse::Created()
            .insert_header((CONTENT_TYPE, JSON))
            .insert_header(("location", format!("/v1/uploads/{id}")))
            .body(Json::object(vec![("upload", Json::string(id))]).to_string()),
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `PUT /v1/uploads/{id}` — the pack's bytes.
///
/// Checked with [`Pack::open`] before the `204`, so a client finds out here that
/// what it sent is not a pack, rather than one request later at commit. That
/// check reads the trailer and the index and no payload, so it costs nothing
/// beside the transfer.
///
/// **The body is streamed, not collected.** It used to be taken as `web::Bytes`,
/// which actix buffers subject to a default payload limit of 256 KiB — so a pack
/// larger than that was refused with a bare `413`. A backup tool that cannot
/// accept a 300 KiB pack cannot back anything up. See [`Uploads::put`].
#[actix_web::put("/v1/uploads/{id}")]
async fn fill_upload(
    uploads: web::Data<Uploads>,
    path: web::Path<String>,
    payload: web::Payload,
) -> HttpResponse {
    match uploads.put(&path.into_inner(), payload).await {
        Ok(_) => no_content(),
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `POST /v1/uploads/{id}/commit` — what arrived, and where it is.
#[actix_web::post("/v1/uploads/{id}/commit")]
async fn commit_upload(uploads: web::Data<Uploads>, path: web::Path<String>) -> HttpResponse {
    let id = path.into_inner();

    let pack = match uploads.open(&id) {
        Ok(pack) => pack,
        Err(e) => return error(&ApiError::from(e)),
    };

    let length = match pack.file_len() {
        Ok(length) => length,
        Err(e) => return error(&ApiError::from(e)),
    };

    ok(&Json::object(vec![
        ("upload", Json::string(id)),
        ("length", Json::int(length)),
        ("payload_bytes", Json::int(pack.payload_len())),
        ("chunks", Json::int(pack.len() as u64)),
        ("version", Json::int(u64::from(pack.version()))),
        (
            "snapshot",
            Json::string(pack.snapshot_id().to_hex().to_string()),
        ),
        (
            "base",
            Json::opt(pack.base().map(|b| Json::string(b.to_hex().to_string()))),
        ),
    ]))
}

/// `DELETE /v1/uploads/{id}` — drop an upload and its file.
#[actix_web::delete("/v1/uploads/{id}")]
async fn drop_upload(uploads: web::Data<Uploads>, path: web::Path<String>) -> HttpResponse {
    match uploads.discard(&path.into_inner()) {
        Ok(()) => no_content(),
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `POST /v1/sets/{set}/merges` — merge an uploaded pack into a set.
///
/// **A `202` with a job, because a merge of a large pack is measured in minutes.**
/// And a merge is where the write lock matters: the per-set lock is taken before
/// the store is opened, so two merges into one set cannot interleave records at
/// the same offset. See [`sets`](super::sets).
#[actix_web::post("/v1/sets/{set}/merges")]
async fn merge_pack(
    sets: web::Data<Sets>,
    uploads: web::Data<Uploads>,
    jobs: web::Data<super::Jobs>,
    path: web::Path<String>,
    body: web::Bytes,
) -> HttpResponse {
    let name = path.into_inner();
    let request = match serde_lite::MergeRequest::read(&String::from_utf8_lossy(&body)) {
        Ok(request) => request,
        Err(e) => return error(&e),
    };

    let policy = match request.compression.as_deref() {
        None => crate::command::MergeCompression::Keep,
        Some(policy) if policy.eq_ignore_ascii_case("keep") => {
            crate::command::MergeCompression::Keep
        }
        Some(other) => match other.parse::<u8>() {
            Ok(level) if u64::from(level) <= u64::from(crate::storage::MAX_COMPRESSION_LEVEL) => {
                crate::command::MergeCompression::Reencode
            }
            _ => {
                return error(&ApiError::bad_request(format!(
                    "{other:?} is not `keep` or a compression level from 0 to {}",
                    crate::storage::MAX_COMPRESSION_LEVEL
                )));
            }
        },
    };

    let upload = request.upload.clone();
    // Opened now so a client is told about a bad upload at the request that
    // names it, rather than by a job that fails afterwards.
    if let Err(e) = uploads.open(&upload) {
        return error(&ApiError::from(e));
    }

    let id = jobs.spawn("merge", move |_handle| async move {
        merge_into(&sets, &uploads, &name, &upload, policy).await
    });

    accepted(id)
}

/// The merge itself, as a job runs it.
///
/// Three steps in this order, and the order is not incidental.
///
/// 1. **Take the per-set write lock.** Held for the whole merge, including the
///    open. `BlobStore` takes its append reservation when it opens, so two
///    handles onto one set would each believe they owned the next offset.
/// 2. **Take the set's own file lock.** Inside the in-process one, because it is
///    the one that keeps other processes out — including a `lepatch merge` on the
///    same set, which this server knows nothing about.
/// 3. **Merge, then evict the read handle.** The eviction is not optional. A
///    cached reader's `contents` map was built before this merge, so without it
///    the next listing would omit the version this merge just published — a wrong
///    answer rather than a slow one.
async fn merge_into(
    sets: &Sets,
    uploads: &Uploads,
    name: &str,
    upload: &str,
    policy: crate::command::MergeCompression,
) -> Result<Json, String> {
    // Every `?` here is an `io::Error`; the job wants a sentence. One
    // conversion at the boundary rather than a `map_err` on each call.
    let attempt = sets
        .writing(name, |store| async move {
            let reason = |e: std::io::Error| e.to_string();

            let base_path = sets.base(name).map_err(reason)?;
            let set = crate::layout::IndexSet::new(&base_path);
            let _claim = set.lock().map_err(reason)?;

            let pack = uploads.open(upload).map_err(reason)?;
            crate::command::merge(&store, &pack, policy)
                .await
                .map_err(reason)
        })
        .await
        .map_err(|e: std::io::Error| e.to_string())?;

    // The closure carries the conversion, so there is exactly one place an
    // `io::Error` becomes the sentence a job reports.
    let merged = attempt?;

    // The cache is evicted here, outside the write lock and after the merge has
    // returned, so the eviction cannot race a merge that is still appending.
    sets.evict(name);

    Ok(match merged {
        crate::command::Merged::Committed { version, plan } => Json::object(vec![
            ("set", Json::string(name)),
            ("version", Json::int(u64::from(version))),
            ("already", Json::Bool(false)),
            ("snapshot", Json::string(plan.snapshot.to_hex().to_string())),
            (
                "base",
                Json::opt(plan.base.map(|b| Json::int(u64::from(b)))),
            ),
            ("chunks", Json::int(plan.chunks)),
            ("fresh", Json::int(plan.fresh)),
            ("held", Json::int(plan.held)),
            ("bytes", Json::int(plan.bytes)),
        ]),
        crate::command::Merged::Already { version } => Json::object(vec![
            ("set", Json::string(name)),
            ("version", Json::int(u64::from(version))),
            ("already", Json::Bool(true)),
        ]),
    })
}

/// `GET /v1/jobs` — every job this server is running or has run.
#[actix_web::get("/v1/jobs")]
async fn list_jobs(jobs: web::Data<super::Jobs>) -> HttpResponse {
    let listed: Vec<Json> = jobs.list().iter().map(super::Job::to_json).collect();

    ok(&Json::object(vec![("jobs", Json::Array(listed))]))
}

/// `GET /v1/jobs/{id}` — one job.
#[actix_web::get("/v1/jobs/{id}")]
async fn show_job(jobs: web::Data<super::Jobs>, path: web::Path<String>) -> HttpResponse {
    match JobId::parse(&path.into_inner()) {
        Ok(id) => match jobs.get(id) {
            Some(job) => ok(&job.to_json()),
            None => error(&ApiError::not_found(format!("no job {id}"))),
        },
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `DELETE /v1/jobs/{id}` — stop a job.
///
/// A `409` against a finished one, which is a definite answer rather than a `202`
/// for something that will not happen.
#[actix_web::delete("/v1/jobs/{id}")]
async fn cancel_job(jobs: web::Data<super::Jobs>, path: web::Path<String>) -> HttpResponse {
    match JobId::parse(&path.into_inner()) {
        Ok(id) => match jobs.cancel(id) {
            Ok(()) => accepted(id),
            Err(e) => error(&ApiError::from(e)),
        },
        Err(e) => error(&ApiError::from(e)),
    }
}

/// `GET /v1/jobs/{id}/events` — the same events, as a stream.
///
/// **Server-Sent Events rather than `WebSockets`.** This is one-directional, it is
/// text, and it reconnects on its own with a `Last-Event-ID`. A WebSocket would
/// buy a direction this endpoint does not have and cost a handshake, a frame
/// protocol and a second thing to keep working.
///
/// A client that does not want a stream polls `GET /v1/jobs/{id}` instead. Both
/// read the same [`JobEvent`]s, so the two agree.
#[actix_web::get("/v1/jobs/{id}/events")]
async fn job_events(jobs: web::Data<super::Jobs>, path: web::Path<String>) -> HttpResponse {
    let id = match JobId::parse(&path.into_inner()) {
        Ok(id) => id,
        Err(e) => return error(&ApiError::from(e)),
    };

    // **One call, one lock.** This used to be three: `subscribe(id)` as an
    // existence check whose value was discarded, then `receiver()`, then
    // `subscribe(id)` again for the history. Two locks and two clones for one
    // answer, and the receiver was taken *before* the history snapshot — so an
    // event broadcast in that window landed in both and a late subscriber
    // received it twice.
    let (receiver, history) = match jobs.subscribe(id) {
        Ok(pair) => pair,
        Err(e) => return error(&ApiError::from(e)),
    };

    // The replay first, then the live stream, chained. A client that connects late
    // is therefore told what it missed before it is told what is happening now,
    // rather than being told only about the future.
    let live = futures_util::stream::unfold(receiver, move |mut receiver| async move {
        // Each step yields a `Frame`, so this and the replay above are the same
        // stream type and `chain` has nothing to reconcile.
        // **One receive per step, not a loop.** `unfold` calls this closure again
        // for the next item, so a loop here would take several events per call and
        // the two would disagree about what one step is.
        match receiver.recv().await {
            Ok(event) => Some((Ok(render_event(&event)), receiver)),
            // A lagging or a closed stream ends it, and both answer the same
            // thing: there is nothing more to send. A lagging reader is dropped
            // rather than allowed to hold the broadcast open; it can poll
            // `GET /v1/jobs/{id}` for the state it missed, which is the whole
            // reason that endpoint exists.
            Err(
                tokio::sync::broadcast::error::RecvError::Lagged(_)
                | tokio::sync::broadcast::error::RecvError::Closed,
            ) => None,
        }
    });

    let replayed =
        futures_util::stream::iter(history).map(|event| -> Frame { Ok(render_event(&event)) });
    let stream = replayed.chain(live);

    HttpResponse::Ok()
        .insert_header((CONTENT_TYPE, "text/event-stream"))
        .insert_header(("cache-control", "no-cache"))
        .streaming(stream)
}

/// One event as an SSE frame.
///
/// Four event names rather than one undifferentiated `data:` line, so a client
/// can tell a state change from a counter tick without parsing JSON to find out.
/// `error` is the counterpart to `result`: a job that failed tells the stream why,
/// so a client that only reads events does not have to fall back to polling to
/// find out.
///
/// By reference, because the two halves of the stream — the replay and the live
/// receiver — hold the event between them and there is no reason to move it out.
fn render_event(event: &JobEvent) -> actix_web::web::Bytes {
    let (name, payload) = match &event {
        JobEvent::State(state) => ("state", Json::string(state_name(*state)).to_string()),
        JobEvent::Progress(counters) => (
            "progress",
            Json::Object(
                counters
                    .iter()
                    .map(|(name, value)| (name.clone(), Json::int(*value)))
                    .collect(),
            )
            .to_string(),
        ),
        JobEvent::Result(result) => ("result", result.to_string()),
        JobEvent::Error(reason) => ("error", Json::string(reason).to_string()),
    };

    actix_web::web::Bytes::from(format!("event: {name}\ndata: {payload}\n\n"))
}

/// Every event name the SSE renderer can put on the wire, as `JobEvent`'s
/// variants are matched.
///
/// **Next to the `match`, because the `match` is the thing that can fall out of
/// step with it.** An event variant added above renders a name this list does not
/// name, and nothing else here would notice. So the list is the renderer's own
/// vocabulary, published for `tests/openapi.rs` to check the `OpenAPI` document
/// against — the description of `GET /v1/jobs/{id}/events` promises a client a
/// closed set of names to switch on, and a promise that is one name short is
/// worse than no promise.
///
/// Public for the same reason [`ROUTES`](super::ROUTES) and
/// [`SCHEMAS`](super::SCHEMAS) are: the document is written by hand, so a test
/// has to be able to compare it against the code without reaching into a private
/// module. The renderer's own `match` is checked against this list by
/// `every_rendered_event_is_a_named_one` below.
pub const EVENT_NAMES: &[&str] = &["state", "progress", "result", "error"];

/// The word a state is reported under, matching the JSON in a job document.
fn state_name(state: State) -> &'static str {
    match state {
        State::Queued => "queued",
        State::Running => "running",
        State::Succeeded => "succeeded",
        State::Failed => "failed",
        State::Cancelled => "cancelled",
    }
}

/// Answer a `Range` request against a resolved pack, or the whole pack.
///
/// **The four responses**, and each says what happened:
///
/// * `206` with `Content-Range` — the bytes asked for.
/// * `200` with the whole pack — no `Range` header, which is the ordinary fetch,
///   for a pack up to [`WHOLE_PACK_LIMIT`].
/// * `416` with `Content-Range: bytes */length` — the offset is past the end, so
///   the client's idea of the file is wrong and it should hear about that rather
///   than receive an empty body it would write as a hole.
/// * `413` — a whole-pack `GET` above [`WHOLE_PACK_LIMIT`], naming the plan
///   endpoint and ranges as the way to fetch a pack that size.
fn serve_range(resolved: &Resolved, asked: Option<ByteRange>) -> HttpResponse {
    let length = resolved.pack.file_len();

    let Some(parsed) = asked else {
        if let Some(refusal) = whole_pack_refusal(length) {
            return error(&refusal);
        }

        let whole = ByteRange {
            start: 0,
            end: u64::MAX,
        }
        .clamp(length);

        let bytes = match whole.map_or_else(
            || {
                Err(std::io::Error::other(
                    "a zero-length pack cannot be served whole",
                ))
            },
            |range| read_range(resolved, range),
        ) {
            Ok(bytes) => bytes,
            Err(e) => return error(&ApiError::from(e)),
        };

        return HttpResponse::Ok()
            .insert_header((CONTENT_TYPE, openapi::PACK_MEDIA_TYPE))
            .insert_header(("accept-ranges", "bytes"))
            .insert_header(("content-length", length.to_string()))
            .body(bytes);
    };

    let Some(range) = parsed.clamp(length) else {
        return HttpResponse::build(StatusCode::RANGE_NOT_SATISFIABLE)
            .insert_header((CONTENT_RANGE, format!("bytes */{length}")))
            .insert_header((CONTENT_TYPE, openapi::PROBLEM_MEDIA_TYPE))
            .body(
                Json::object(vec![
                    ("type", Json::string("about:blank")),
                    (
                        "title",
                        Json::string(format!(
                            "bytes={}- is past the end of a {length}-byte pack",
                            parsed.start
                        )),
                    ),
                    ("status", Json::int(416)),
                    (
                        "detail",
                        Json::string(format!(
                            "bytes={}- is past the end of a {length}-byte pack",
                            parsed.start
                        )),
                    ),
                ])
                .to_string(),
            );
    };

    match read_range(resolved, range) {
        Ok(bytes) => {
            let sent = range.len();
            HttpResponse::PartialContent()
                .insert_header((CONTENT_TYPE, openapi::PACK_MEDIA_TYPE))
                .insert_header(("accept-ranges", "bytes"))
                .insert_header((
                    CONTENT_RANGE,
                    format!(
                        "bytes {}-{}/{}",
                        range.start,
                        range.end.saturating_sub(1),
                        length
                    ),
                ))
                .insert_header(("content-length", sent.to_string()))
                .body(bytes)
        }
        Err(e) => error(&ApiError::from(e)),
    }
}

/// The `413` for a whole-pack `GET` above [`WHOLE_PACK_LIMIT`], or `None`.
///
/// **Refused rather than streamed, and the length is why this is enough.** The
/// pack's length is already known before a byte is read, so there is no reason to
/// start building a body the server has already decided it cannot afford. Nothing
/// is read and nothing is allocated to produce this.
///
/// Its own function so the bound can be checked at its own edges without a
/// resolved pack behind it — the boundary is a property of the constant, and the
/// property is what a change to the constant has to break.
fn whole_pack_refusal(length: u64) -> Option<ApiError> {
    if length <= WHOLE_PACK_LIMIT {
        return None;
    }

    Some(ApiError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        format!(
            "refusing to send a {length}-byte pack in one response: an un-ranged pack fetch is \
             held in memory, and this server sends at most {WHOLE_PACK_LIMIT} bytes that way. \
             Fetch a pack this size with `POST /v1/packs/{{id}}/plan`, which returns the byte \
             ranges you lack, and request those ranges one at a time with a `Range` header"
        ),
    ))
}

/// Read `range` out of a resolved pack.
///
/// **Read into memory rather than streaming, and the bound is why.** A range is
/// one fetch plan entry: the client asked for the chunks it lacks plus the four
/// fixed regions, and it writes each response at an offset into a sparse file. The
/// plan merges touching ranges, so a response is a run of whole records or a fixed
/// region — not an arbitrary slice of a multi-gigabyte payload.
///
/// A whole-pack `GET` is the exception, and it is buffered because the alternative
/// is a body whose length is not known until it has been computed. **That case is
/// bounded now**, at [`WHOLE_PACK_LIMIT`] and by a `413` rather than by the
/// allocator, so the one allocation this endpoint can make is one whose size an
/// operator chose. See [`whole_pack_refusal`].
fn read_range(resolved: &Resolved, range: crate::pack::Range) -> std::io::Result<Vec<u8>> {
    let sources = resolved.source(range)?;

    let total = range.len();
    let mut out = Vec::with_capacity(usize::try_from(total).unwrap_or(0));

    for source in sources {
        match source {
            crate::pack::Source::Bytes(bytes) => out.extend_from_slice(&bytes),
            crate::pack::Source::Segment { path, offset, len } => {
                let mut file = std::fs::File::open(&path)?;
                file.seek(SeekFrom::Start(offset))?;

                let start = out.len();
                out.resize(start + usize::try_from(len).unwrap_or(0), 0);
                file.read_exact(&mut out[start..])?;
            }
        }
    }

    Ok(out)
}

/// The `Range` header, if the request carried one.
fn header_range(request: &HttpRequest) -> Option<String> {
    request
        .headers()
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// Decode a pack id into what it names.
///
/// An id is `(set, version, base)` in one string, so resolving one needs nothing
/// stored. The format is `set@version` or `set@version-base`, and a set name may
/// not hold an `@` because the registry refuses every name that is not one plain
/// path component.
fn requested_from(id: &str) -> Result<Requested, ApiError> {
    let (name, rest) = id.split_once('@').ok_or_else(|| {
        ApiError::bad_request(format!(
            "{id:?} is not a pack id; a pack id is `set@version` or `set@version-base`"
        ))
    })?;

    let (version, base) = match rest.split_once('-') {
        Some((version, base)) => (version, Some(base)),
        None => (rest, None),
    };

    let version: u32 = version
        .parse()
        .map_err(|_| ApiError::bad_request(format!("{version:?} is not a version number")))?;

    let base = match base {
        Some(text) => Some(
            text.parse::<u32>()
                .map_err(|_| ApiError::bad_request(format!("{text:?} is not a version number")))?,
        ),
        None => None,
    };

    Ok(Requested {
        set: name.to_string(),
        version: Some(version),
        base,
    })
}

/// Build a pack id from a request, for a client that wants one.
#[must_use]
///
/// A request with no version has no id, because an id has to name something: a
/// client asking for "the newest version" and a client asking for "version 3"
/// are different requests and resolve differently.
pub(super) fn id_for(request: &Requested) -> Option<String> {
    let version = request.version?;

    Some(match request.base {
        Some(base) => format!("{}@{version}-{base}", request.set),
        None => format!("{}@{version}", request.set),
    })
}

/// Split a JSON object body into its fields.
///
/// A body is read whole and handed to [`Json::checked`], which parses it with
/// `serde_json` and then validates the shape: a flat object of strings, whole
/// numbers, booleans, `null` and arrays of strings. Anything else is a `400`
/// naming the field and what it was, because a client that sent the wrong shape
/// needs to be told the shape rather than told it failed.
///
/// # Errors
///
/// Returns an error if `body` is not JSON, is not an object, or holds a value
/// this does not read.
fn parse_fields(body: &str) -> Result<Vec<(String, Json)>, ApiError> {
    match Json::checked(body).map_err(ApiError::bad_request)? {
        Json::Object(fields) => Ok(fields),
        // Unreachable: `checked` yields `Ok` only for an object. Stated rather
        // than `unreachable!()`, which this crate denies.
        _ => Err(ApiError::bad_request(
            "a request body must be a JSON object",
        )),
    }
}

/// A `StatusCode` re-export, so the handler module does not reach into actix for
/// one constant.
type StatusCode = actix_web::http::StatusCode;

/// One Server-Sent Events frame.
///
/// `actix_web::streaming` takes a `Stream` of `Result<Bytes, E>`, so each frame
/// is wrapped. `Infallible` because rendering an event cannot fail: every field of
/// one is either a string this module wrote or a number it already holds.
type Frame = Result<actix_web::web::Bytes, std::convert::Infallible>;

#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;

    use super::{EVENT_NAMES, WHOLE_PACK_LIMIT, render_event, whole_pack_refusal};
    use crate::serve::{JobEvent, Json, State};

    #[test]
    fn the_whole_pack_bound_is_inclusive_at_its_top() {
        // **The boundary, both sides of it.** `<=`, so the limit itself is
        // served and one byte more is not. A pack exactly at the line that was
        // refused would make the number a claim rather than a bound.
        assert!(
            whole_pack_refusal(WHOLE_PACK_LIMIT).is_none(),
            "a pack of exactly the limit must still be served whole"
        );
        assert!(whole_pack_refusal(WHOLE_PACK_LIMIT + 1).is_some());
    }

    #[test]
    fn an_ordinary_small_pack_is_served_whole() {
        // What a pull of a normal version does, and what must keep working: no
        // `Range` header, `200`, the whole file. The size here is far below the
        // bound and is the shape every other test in the suite fetches.
        assert!(whole_pack_refusal(0).is_none());
        assert!(whole_pack_refusal(2_073_438).is_none());
    }

    #[test]
    fn a_refused_whole_pack_get_says_what_to_do_instead() {
        // A `413` with no advice is a dead end. The client has two supported ways
        // to fetch a pack this size and neither is a guess.
        let refusal = whole_pack_refusal(WHOLE_PACK_LIMIT + 1).unwrap();

        assert_eq!(refusal.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let document = refusal.problem().to_string();
        assert!(
            document.contains("/v1/packs/{id}/plan"),
            "the refusal must name the plan endpoint: {document}"
        );
        assert!(
            document.contains("Range"),
            "the refusal must name ranged fetches: {document}"
        );
        assert!(
            document.contains(&WHOLE_PACK_LIMIT.to_string()),
            "the refusal must state the bound it refused against: {document}"
        );
    }

    /// One of each [`JobEvent`], so the renderer's whole vocabulary is covered.
    fn every_event() -> Vec<JobEvent> {
        vec![
            JobEvent::State(State::Running),
            JobEvent::Progress(vec![("chunks".to_string(), 3)]),
            JobEvent::Result(Json::Null),
            JobEvent::Error("a reason".to_string()),
        ]
    }

    #[test]
    fn every_rendered_event_is_a_named_one() {
        // **The chain that makes `EVENT_NAMES` worth exporting.** The renderer
        // matches on `JobEvent`, so a variant added there renders a name that no
        // other list knows about. This compares the two directions: every name the
        // renderer can produce is in the list, and every name in the list is
        // produced. `tests/openapi.rs` then holds the document's description to
        // that list, which is the last link.
        let rendered: Vec<String> = every_event()
            .iter()
            .map(|event| {
                let frame = String::from_utf8(render_event(event).to_vec())
                    .expect("a rendered frame is text");
                frame
                    .strip_prefix("event: ")
                    .and_then(|rest| rest.split_once('\n').map(|(name, _)| name))
                    .unwrap_or_else(|| panic!("a frame must lead with its name: {frame}"))
                    .to_string()
            })
            .collect();

        for name in &rendered {
            assert!(
                EVENT_NAMES.contains(&name.as_str()),
                "the renderer emits {name:?}, which EVENT_NAMES does not list"
            );
        }

        assert_eq!(
            rendered.len(),
            EVENT_NAMES.len(),
            "EVENT_NAMES lists {} names and the renderer produces {rendered:?}",
            EVENT_NAMES.len()
        );
    }
}
