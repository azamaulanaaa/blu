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

    let id = jobs.spawn("pack", move |handle| async move {
        // `version: null` means "the newest one that resolves", which is what
        // the CLI does with no `--version`.
        let resolved = super::pack::resolve(&sets_for_job, &requested).await;

        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(e) => {
                handle.fail(e.to_string());
                return Json::object(vec![("error", Json::string(e.to_string()))]);
            }
        };

        Json::object(vec![
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
        ])
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
#[actix_web::put("/v1/uploads/{id}")]
async fn fill_upload(
    uploads: web::Data<Uploads>,
    path: web::Path<String>,
    body: web::Bytes,
) -> HttpResponse {
    match uploads.put(&path.into_inner(), &body).await {
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

    let id = jobs.spawn("merge", move |handle| async move {
        let outcome = merge_into(&sets, &uploads, &name, &upload, policy).await;

        match outcome {
            Ok(merged) => merged,
            Err(e) => {
                handle.fail(e.to_string());
                Json::object(vec![("error", Json::string(e.to_string()))])
            }
        }
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
) -> Result<Json, std::io::Error> {
    let guard = sets.writing(name).await?;

    let base_path = sets.base(name)?;
    let set = crate::layout::IndexSet::new(&base_path);
    let _claim = set.lock()?;

    let pack = uploads.open(upload)?;
    let merged = crate::command::merge(guard.store(), &pack, policy).await?;

    // After the merge, and only after it. A merge that failed leaves no version,
    // and the handle is still accurate.
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

    // A late subscriber is given what it missed rather than an empty stream that
    // looks like a job with no progress.
    if let Err(e) = jobs.subscribe(id) {
        return error(&ApiError::from(e));
    }

    let receiver = jobs.receiver();
    let history = jobs.subscribe(id).unwrap_or_default();

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
/// Three event names rather than one undifferentiated `data:` line, so a client
/// can tell a state change from a counter tick without parsing JSON to find out.
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
    };

    actix_web::web::Bytes::from(format!("event: {name}\ndata: {payload}\n\n"))
}

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
/// **The three responses a range can produce**, and each says what happened:
///
/// * `206` with `Content-Range` — the bytes asked for.
/// * `200` with the whole pack — no `Range` header, which is the ordinary fetch.
/// * `416` with `Content-Range: bytes */length` — the offset is past the end, so
///   the client's idea of the file is wrong and it should hear about it rather
///   than receive an empty body it would write as a hole.
fn serve_range(resolved: &Resolved, asked: Option<ByteRange>) -> HttpResponse {
    let length = resolved.pack.file_len();

    let Some(parsed) = asked else {
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

/// Read `range` out of a resolved pack.
///
/// **Read into memory rather than streaming, and the bound is why.** A range is
/// one fetch plan entry: the client asked for the chunks it lacks plus the four
/// fixed regions, and it writes each response at an offset into a sparse file. The
/// plan merges touching ranges, so a response is a run of whole records or a fixed
/// region — not an arbitrary slice of a multi-gigabyte payload.
///
/// A whole-pack `GET` is the exception, and it is buffered because the alternative
/// is a body whose length is not known until it has been computed. That is the one
/// case where a very large allocation is possible, and it is a client asking for
/// the whole file.
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
/// A body is read whole and parsed here. **This is not a JSON parser and does not
/// pretend to be.** It handles exactly what these endpoints accept — a flat
/// object of strings, integers, arrays of strings and `null` — and anything else
/// is a `400` naming what it found. A general parser is a dependency and a
/// maintenance obligation, for three request shapes.
///
/// # Errors
///
/// Returns an error if `body` is not an object, or if it holds a value this does
/// not read.
fn parse_fields(body: &str) -> Result<Vec<(String, Json)>, ApiError> {
    match Json::parse_object(body).map_err(ApiError::bad_request)? {
        Json::Object(fields) => Ok(fields),
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
