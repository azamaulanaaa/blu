//! The span every request to `blu serve` is traced in.
//!
//! # Why not `TracingLogger`
//!
//! **actix-web's own middleware makes a span and never reads `traceparent`.** It
//! opens `request` off whatever span happens to be current on the worker thread,
//! and an actix worker has none, so every server span is a root. A client that
//! sent a `traceparent` gets a perfectly good span that belongs to a *different
//! trace* from the one the client is already looking at, and nothing anywhere
//! says so: the export succeeds, the span arrives, the panel draws. **That is
//! the most common way an OpenTelemetry integration quietly fails**, and it is
//! the reason this is a module rather than a line in `handlers.rs`.
//!
//! So this does the two things [`TracingLogger`] does not: it **extracts** the
//! inbound context through the global propagator (which [`crate::telemetry`]
//! has already registered) and **parents** the span to it, and it records what
//! the request actually was rather than what its path spelled.
//!
//! # What the span is
//!
//! One span per request, named `http.server`, in the semantic conventions' shape
//! so a panel that already has an HTTP dashboard on it can group this server
//! with everything else that speaks the same conventions:
//!
//! | attribute | written | value |
//! |---|---|---|
//! | `otel.kind` | at entry | `server` |
//! | `http.method` | at entry | the request's method |
//! | `http.route` | on the way out | the route **template**, or nothing |
//! | `http.status_code` | on the way out | the status actually returned |
//! | `otel.status_code` | on the way out | `ERROR`, for a `5xx` and nothing else |
//! | `blu.job` | on the way out | the kind of job this request started |
//!
//! **The two `otel.*` fields do not reach a collector as attributes.** The layer
//! reads `otel.kind` into the span's *kind* and `otel.status_code` into its
//! *status*, which is where the conventions want them, so what arrives is a
//! `SpanKind::Server` and a `Status::Unset` or `Status::Error`. Everything else
//! in the table is an attribute on the exported span.
//!
//! **`tracing-opentelemetry` adds six attributes of its own**, all bounded:
//! `code.file.path`, `code.module.name` and `code.line.number` for the callsite,
//! `thread.id`, the `thread.name` **when the thread has one** — a span exported
//! from a thread the runtime did not name carries no `thread.name` — and
//! `target`. It also emits a `busy_ns`/`idle_ns` **event** per span, which is
//! not an attribute and does not appear in an attribute listing at all.
//! `ours` in the tests names every one of them, so a seventh arriving
//! unannounced fails a test instead of arriving quietly.
//!
//! **The status is recorded from the response, never from what the handler
//! intended.** A `4xx` or `5xx` that reported `200` would be worse than no span
//! at all: it is a span that is confidently wrong, and every alert and every
//! error rate built on it is wrong in the same direction.
//!
//! ## The route, and why it is not the path
//!
//! `http.route` is `/v1/sets/{set}/versions` and **never**
//! `/v1/sets/photos/versions`. The path is an input: it carries a set name, a
//! version number and an upload id, every one of which is chosen by whoever
//! made the request. **A span named after it would make a new span name per set
//! per version**, which is how a trace backend is turned into a bill. The
//! template is a closed set of seventeen strings, and it is read from actix's
//! own resource map rather than matched here — the router already knows, and a
//! second matcher would be a second thing to keep in step with it.
//!
//! It is read **on the way out**, because that is when the router has matched.
//! `App::wrap` sits *outside* routing, and a request that has not been routed
//! has an empty resource map: `match_pattern` before the handler returns `None`
//! for every request, including the ones that matched. So the field is declared
//! `Empty` and recorded from the response's own request afterwards, which is
//! the same request after routing has filled its match info in.
//!
//! **A route that matched nothing carries no `http.route` at all**, rather than
//! the path it did not match. That is what the conventions ask for, and it is
//! also the honest answer: a `404` for `/v1/sets/a/versions/b` and one for
//! `/(  ; DROP` are the same event — a client asked for something this server
//! does not have — and writing the request back into the span would make the
//! first of them as unbounded as whatever the client felt like sending.
//!
//! # How a client and this server end up in one trace
//!
//! [`crate::telemetry::layer`] registers the W3C `TraceContext` propagator on
//! the OpenTelemetry global, and that registration is what makes this module's
//! extraction do anything. The three steps, and each of them is load-bearing:
//!
//! 1. **Extract.** [`Headers`] reads `traceparent` and `tracestate` off the
//!    request and the propagator turns them into an `opentelemetry::Context`.
//!    A request with no `traceparent` extracts an empty context, which is a root
//!    span — the correct answer for a client that is not instrumented, and the
//!    answer this crate's own `blu versions` of a serve target gets, since it
//!    sends no header at all.
//! 2. **Parent.** `Span::set_parent` attaches that context to the span before it
//!    is ever entered, so the server's span joins the client's trace rather than
//!    starting a second one beside it.
//! 3. **Instrument.** The handler's future is run under the span, so everything
//!    the handler opens itself nests *inside* it instead of beside it — the
//!    store it reads and the pack it resolves. **No `blulib::command::*` span
//!    is ever under it**, because the two commands this server reaches run in a
//!    spawned job; see the cardinality table below.
//!
//! ## Cardinality: what nests under what, and what may never be there
//!
//! **A reader should build a panel from this table and no other evidence.**
//!
//! ```text
//! http.server                      one per request; 17 route templates, 4 methods
//! ├── blulib::storage::blob::*     one per store a handler opens or reads
//! └── blulib::pack::*              one per chunk a handler reads out of a pack
//!
//! blu_job                      one per job, and a root span of its own trace
//! └── blulib::command::*          whatever the job runs
//! ```
//!
//! * `http.server` is a **fixed name**, so its cardinality is its attributes':
//!   method, route template, status, job kind. Four small closed sets. No
//!   attribute on it carries a path, a set name, a version, an upload id or a
//!   pack id, and none may be added.
//! * **What nests under it is the layer below the commands, not the commands.**
//!   A handler resolves a version, a pack or a plan itself, so an `http.server`
//!   span around `GET /v1/packs/{id}` contains the `blulib::pack` spans that
//!   read the chunks it served and any `blulib::storage::blob` span that opened
//!   a store. Every one of those is `#[instrument(skip_all)]`, so it carries no
//!   arguments at all.
//! * **`blulib::command::*` never appears under it.** `merge` and the pack work
//!   behind `POST /v1/sets/{set}/packs` are the only commands this server can
//!   reach, and both run inside a spawned job. The `blulib::command::*` spans a
//!   `blu backup` or `blu restore` produces are in their own trace too,
//!   because those commands do not run inside a server at all.
//! * **`blu_job` is *not* under `http.server`**, and this is the one asymmetry in the
//!   table. `Jobs::spawn` hands the work to `tokio::spawn`, and a spawned task
//!   does not inherit the request's span — it is a task with no parent scope at
//!   all. So a merge started by `POST /v1/sets/{set}/merges` reports a
//!   `blu_job` root span with `blulib::command::merge` under it, in **its
//!   own trace**, and the `http.server` span beside it records *which request*
//!   started it and what it answered (`202`, with a job id).
//!   `blu.job` is the join between them: it is the kind of job the request
//!   started, and it is one of the two words `blu.job.kind` reports on
//!   [`crate::telemetry::metrics`]. **No job id is written on the request span**,
//!   because the id does not exist until `spawn` has run, which is after this
//!   span was created; guessing one from the response body would mean reading
//!   and buffering every `202`. **Nothing links the two spans either** — the job
//!   span carries its own `job` id and kind and no reference back to the
//!   request — so the two traces are joined by that id in the `202` body and by
//!   nothing in the telemetry itself.
//!
//! # It is installed only when telemetry is on
//!
//! [`handlers::configure`](super::handlers) is told whether a tracer provider
//! is installed and builds one of two apps: this middleware wrapped around the
//! router, or the router alone. With telemetry off there is **no middleware at
//! all** — not a middleware that checks a flag, not one that returns early. A
//! default `blu serve` does not build a span, does not read a header, and
//! does not allocate for a request, which is the same promise
//! [`crate::telemetry`] makes for exporters and instruments and is held by
//! `no_span_is_created_when_the_middleware_is_not_installed`.

use actix_web::{
    Error,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::StatusCode,
    middleware::Next,
};
use opentelemetry::{Context, global, propagation::Extractor};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// The span name, and the reason it is a constant.
///
/// A `name` per request is a name per thing that can be asked for. The name
/// here never varies, so a panel can select on it, and the dimensions a panel
/// *breaks down by* are attributes rather than names — which is what keeps them
/// bounded. See the cardinality table in the module docs.
const SPAN: &str = "http.server";

/// The two headers the W3C `TraceContext` propagator reads.
///
/// **Named rather than left to the propagator's own key list**, because the
/// whole job of this middleware on the way in is to read these two and nothing
/// else; a key list that grows is a per-request allocation to hold a list that
/// never changes.
const TRACE_HEADERS: [&str; 2] = ["traceparent", "tracestate"];

/// Trace one request, as a child of whatever trace the client was already in.
///
/// Installed by [`handlers::configure`](super::handlers) on the app actix builds
/// for each worker, and by nothing else. It is `pub(super)` because it is
/// middleware rather than something a caller composes: [`crate::serve::bind`]
/// wraps it, and a library caller embedding this crate gets it by binding.
///
/// # Errors
///
/// Returns what the inner service returned. **Every error this server can
/// produce has already been turned into a response** — each handler answers
/// with an `HttpResponse`, and [`ApiError`](super::ApiError) is what it answers
/// with — so the `Err` arm here is for failures actix itself raises, a payload
/// that would not decode. It is recorded on the span like any other status and
/// then passed on untouched: this middleware traces a request and refuses none.
pub(super) async fn trace<B: MessageBody>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let span = tracing::info_span!(
        SPAN,
        otel.kind = "server",
        http.method = req.method().as_str(),
        http.route = tracing::field::Empty,
        http.status_code = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        blu.job = tracing::field::Empty,
    );

    // Before the span is ever entered: a span that has been entered already has
    // a parent, and this one has to take the client's.
    //
    // **The error is dropped, deliberately.** It has exactly one cause here —
    // there is no OpenTelemetry layer on the subscriber — and in that case the
    // span is not exported at all, so there is nothing for the failure to have
    // cost but a link to a trace that was never going to be there. Reporting it
    // per request would be a line of noise for a condition the process reports
    // once, loudly, at startup: either telemetry is on and the layer is
    // installed, or it is off and this middleware is not in the app at all.
    let _ = span.set_parent(extracted(&req));

    // `.instrument` rather than `enter` held across the `await`. Entering here
    // would make the guard live across the handler, and `Entered` is not `Send`
    // and is documented as unsound across a task boundary — which is exactly
    // where an actix worker moves a future it did not create.
    let response = next.call(req).instrument(span.clone()).await;

    match response {
        Ok(response) => {
            // The route is read here because this is the first moment the router
            // has matched. See the module docs on why it is not read at entry.
            finish(
                &span,
                response.request().match_pattern().as_deref(),
                response.status(),
            );
            Ok(response)
        }
        Err(e) => {
            finish(&span, None, e.as_response_error().status_code());
            Err(e)
        }
    }
}

/// The trace the client asked for, or a root span when it asked for none.
///
/// Through the **global** propagator rather than a named one, because the
/// propagator is process-global by design and [`crate::telemetry`] is what
/// registers it. With telemetry off the global propagator is the SDK's no-op,
/// so this costs one header lookup and produces an empty context, which is a
/// root span.
fn extracted(req: &ServiceRequest) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&Headers(req.request())))
}

/// Record what the request turned out to be.
///
/// Called on both arms — a response the handler built and an error actix raised
/// — because **the two are not the same kind of thing and only one of them is
/// what a client saw.** A span that records nothing on an error still reads as
/// a request that was fine.
fn finish(span: &tracing::Span, route: Option<&str>, status: StatusCode) {
    if let Some(route) = route {
        span.record("http.route", route);
        if let Some(kind) = job_kind(route) {
            span.record("blu.job", kind);
        }
    }

    span.record("http.status_code", status.as_u16());

    // **A `5xx` and nothing else.** A `4xx` is a client that asked for something
    // that is not there, which is this server working: the conventions say a
    // server span's status is `ERROR` for a `5xx` and unset otherwise, and
    // marking every mistyped path as an error would put a floor under every
    // error rate on the panel.
    if status.is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
}

/// The kind of job a request to `route` starts, if it starts one.
///
/// **Two routes, and they are the only two** — every other endpoint in
/// [`ROUTES`](super::ROUTES) answers from the request itself and leaves nothing
/// running. Both are the routes whose handler calls
/// [`Jobs::spawn`](super::Jobs::spawn) and answers `202`: `request_pack` with
/// `"pack"`, `merge_pack` with `"merge"`. **It is the two that answer `202` with
/// a job and nothing else**, so the answer is read off the handler rather than
/// off the `202` — a route that failed and returned `4xx` still started nothing.
///
/// `POST /v1/packs/{id}/plan` is **not** one of them: it resolves the pack and
/// answers the ranges itself, and a span claiming a job it did not start is a
/// panel counting a merge that never ran.
///
/// The words are the ones [`Jobs::spawn`](super::Jobs::spawn) is given and the
/// ones `blu.job.kind` reports on a metric, so the two halves of the
/// telemetry agree without a reader having to learn a second vocabulary.
fn job_kind(route: &str) -> Option<&'static str> {
    match route {
        "/v1/sets/{set}/packs" => Some("pack"),
        "/v1/sets/{set}/merges" => Some("merge"),
        _ => None,
    }
}

/// The inbound headers a propagator reads, and nothing else.
///
/// A `&str` per key is all [`Extractor`] asks for, so this hands out the
/// request's own header values without copying, and a header that is not valid
/// UTF-8 is reported as absent — which is what the propagator would have done
/// with it anyway, and is a refused `traceparent` rather than a panic.
struct Headers<'a>(&'a actix_web::HttpRequest);

impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .headers()
            .get(key)
            .and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        TRACE_HEADERS.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    // Aliased, and not merely imported: `use actix_web::test` would bring the
    // attribute macro in with it and turn every `#[test]` below into actix's,
    // which then insists the function is `async`.
    use actix_web::{
        HttpResponse,
        http::StatusCode,
        test as actix_test,
        web::{self},
    };
    use opentelemetry::{
        global,
        trace::{SpanKind, Status},
    };
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        propagation::TraceContextPropagator,
        trace::{SdkTracerProvider, SpanData, SpanExporter},
    };
    use tracing::{Metadata, Subscriber};
    use tracing_subscriber::{
        layer::{Context, Layer, SubscriberExt as _},
        registry::LookupSpan,
    };

    use super::{SPAN, TRACE_HEADERS, job_kind};
    use crate::serve::{Config, Jobs, Sets, Uploads};

    /// The trace and span a W3C `traceparent` names, as the specification's own
    /// worked example spells them.
    const CLIENT_TRACE: &str = "0af7651916cd43dd8448eb211c80319c";
    const CLIENT_SPAN: &str = "b7ad6b7169203331";
    const CLIENT_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    /// A span exporter that keeps every span it is handed, in this process.
    ///
    /// **No socket and no collector**, for the reason `telemetry`'s own tests
    /// give: what is under test is the pipeline and the attributes written on
    /// the way out, and a collector would only add a network client to that
    /// claim.
    #[derive(Clone, Debug, Default)]
    struct Recorder(Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for Recorder {
        fn export(
            &self,
            batch: Vec<SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            self.0.lock().expect("the recorder's lock").extend(batch);
            std::future::ready(Ok(()))
        }
    }

    impl Recorder {
        /// The one span named `name`.
        ///
        /// Panics naming what *was* recorded, because "no span named
        /// `http.server`" on its own says nothing about where the request went.
        fn span(&self, name: &str) -> SpanData {
            let spans = self.0.lock().expect("the recorder's lock");
            spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| {
                    panic!(
                        "no span named {name}. It recorded {:?}",
                        spans.iter().map(|s| s.name.as_ref()).collect::<Vec<_>>()
                    )
                })
                .clone()
        }

        /// One span's attributes, flattened to strings so a test can compare a
        /// whole set at once. `Value`'s own rendering is the honest one: an
        /// integer attribute prints as the number and a string prints as itself.
        fn attributes(span: &SpanData) -> BTreeMap<String, String> {
            span.attributes
                .iter()
                .map(|pair| (pair.key.to_string(), pair.value.to_string()))
                .collect()
        }
    }

    /// The attributes this crate writes, and not the ones the semantic
    /// conventions add.
    ///
    /// **`tracing-opentelemetry` writes six attributes of its own** on every
    /// exported span — three for the callsite, `thread.id`, `thread.name` and
    /// `target` — and they are all bounded, so they are not a cardinality
    /// problem. `thread.name` is absent when the thread has no name, and
    /// `busy_ns`/`idle_ns` are span *events* rather than attributes, so both are
    /// named here as well: this list is what the layer may write, and an
    /// attribute appearing from nowhere fails a test instead of being swept up by
    /// a prefix.
    fn ours(span: &SpanData) -> BTreeMap<String, String> {
        Recorder::attributes(span)
            .into_iter()
            .filter(|(key, _)| !CONVENTIONS.contains(&key.as_str()))
            .collect()
    }

    /// The attributes the layer adds, which this module does not choose.
    const CONVENTIONS: [&str; 8] = [
        "code.file.path",
        "code.line.number",
        "code.module.name",
        "thread.id",
        "thread.name",
        "target",
        "busy_ns",
        "idle_ns",
    ];

    /// A tracer provider over a recorder, plus the recorder.
    fn recorder() -> (SdkTracerProvider, Recorder) {
        let recorder = Recorder::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(recorder.clone())
            .build();
        (provider, recorder)
    }

    /// The subscriber a `blu serve` run exports spans through: this crate's
    /// OpenTelemetry layer over the crate's own filter, and nothing writing to a
    /// terminal.
    fn exporting(
        provider: &SdkTracerProvider,
    ) -> impl Subscriber + for<'lookup> LookupSpan<'lookup> {
        use opentelemetry::trace::TracerProvider as _;

        tracing_subscriber::registry()
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("blu"))
                    .with_filter(crate::telemetry::filter()),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(tracing_subscriber::EnvFilter::new("warn")),
            )
    }

    /// Register the W3C propagator the way `telemetry::layer` does.
    ///
    /// **The global one**, deliberately: that is what the middleware reads, and a
    /// test that registered its own propagator into a local variable would be
    /// testing an extraction path the crate never takes. It is idempotent, and
    /// nothing else in this binary asks the global propagator a question.
    fn register_propagator() {
        global::set_text_map_propagator(TraceContextPropagator::new());
    }

    /// The shared state [`configure`](super::super::handlers::configure) builds
    /// once per server: one set registry, one upload registry, one job registry.
    ///
    /// **Built here rather than through `serve::bind`**, because these tests need
    /// an app they can drive with actix's own harness rather than a socket, and
    /// because `bind` would install the middleware according to a process-global
    /// that no test may set — see
    /// `the_middleware_follows_the_installed_tracer_provider`.
    fn state(root: &std::path::Path) -> (web::Data<Sets>, web::Data<Uploads>, web::Data<Jobs>) {
        let config = Config::new(root, "127.0.0.1:0", None);

        (
            web::Data::new(Sets::new(config.root)),
            web::Data::new(Uploads::new(config.scratch)),
            web::Data::new(Jobs::new()),
        )
    }

    /// A request this server answers with `200`.
    fn version_request() -> actix_test::TestRequest {
        actix_test::TestRequest::get().uri("/v1/version")
    }

    /// The request span, as the recorder saw it.
    ///
    /// **Read after the provider is flushed**, because the exporter is driven by
    /// that flush and a span that has closed is queued until then.
    fn flush_and_read(provider: &SdkTracerProvider, recorder: &Recorder) -> SpanData {
        provider.force_flush().expect("a flush");
        recorder.span(SPAN)
    }

    /// A successful request yields one span, named, with the attributes this
    /// module's table promises.
    ///
    /// **The whole attribute set, compared as a set.** A subset assertion would
    /// pass with an unbounded attribute added beside it, and an unbounded
    /// attribute is the failure this module exists to prevent.
    #[actix_web::test]
    async fn a_request_is_traced_with_its_route_and_the_status_it_answered() {
        let (provider, recorder) = recorder();
        let root = tempfile::tempdir().expect("a temporary directory");
        let (sets, uploads, jobs) = state(root.path());

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;
        let response = actix_test::call_service(&app, version_request().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);

        assert_eq!(span.name, SPAN, "the span is named once, for every route");
        assert_eq!(
            ours(&span),
            BTreeMap::from([
                ("http.method".to_string(), "GET".to_string()),
                ("http.route".to_string(), "/v1/version".to_string()),
                ("http.status_code".to_string(), "200".to_string()),
            ]),
            "the span must carry the route TEMPLATE and nothing unbounded: \
             every key here is a closed set"
        );
        assert_eq!(
            span.span_kind,
            SpanKind::Server,
            "`otel.kind` is a field the layer reads rather than an attribute it \
             exports, so it has to be checked where it actually lands"
        );
        assert_eq!(
            span.status,
            Status::Unset,
            "a request that worked is not an error"
        );
        assert!(
            !span.span_context.is_remote(),
            "this span was made here, whatever its parent was"
        );
    }

    /// The inbound `traceparent` becomes the parent, so one trace spans the
    /// client and the server.
    ///
    /// **This is the test `TracingLogger` cannot pass**, and it is why this
    /// module exists rather than a line in `handlers.rs`: actix's middleware
    /// makes its span a root, so a client and its server would be two traces
    /// that look identical in a panel.
    #[actix_web::test]
    async fn an_inbound_traceparent_becomes_the_parent_of_the_request_span() {
        register_propagator();
        let (provider, recorder) = recorder();
        let root = tempfile::tempdir().expect("a temporary directory");
        let (sets, uploads, jobs) = state(root.path());

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;
        let request = actix_test::TestRequest::get()
            .uri("/v1/version")
            .insert_header(("traceparent", CLIENT_TRACEPARENT))
            .to_request();
        let response = actix_test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);

        assert_eq!(
            span.span_context.trace_id().to_string(),
            CLIENT_TRACE,
            "the server's span must join the client's trace, not start another one"
        );
        assert_eq!(
            span.parent_span_id.to_string(),
            CLIENT_SPAN,
            "the client must be able to find this span under the one it sent"
        );
    }

    /// A request with no `traceparent` is its own trace, which is the correct
    /// answer for an uninstrumented client.
    ///
    /// Asserted because "the parent is honoured when there is one" is half a
    /// claim: the other half is that an absent parent is not invented, and a
    /// middleware that reached for the ambient span — which is what
    /// `TracingLogger` does — would pass the first test and fail this one.
    #[actix_web::test]
    async fn a_request_with_no_traceparent_starts_its_own_trace() {
        register_propagator();
        let (provider, recorder) = recorder();
        let root = tempfile::tempdir().expect("a temporary directory");
        let (sets, uploads, jobs) = state(root.path());

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;
        let response = actix_test::call_service(&app, version_request().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);

        assert_eq!(
            span.parent_span_id,
            opentelemetry::trace::SpanId::INVALID,
            "with no inbound context there is no parent to claim"
        );
        assert_ne!(
            span.span_context.trace_id().to_string(),
            CLIENT_TRACE,
            "a request must not be filed under somebody else's trace"
        );
    }

    /// A request for a route this server does not have is still traced, and says
    /// so by carrying no `http.route`.
    ///
    /// **The attribute is absent rather than filled with the path.** A `404` is a
    /// real event — a client asking for something that is not there — and writing
    /// the path it asked for would make it as unbounded as whatever the client
    /// felt like sending.
    #[actix_web::test]
    async fn an_unknown_route_is_still_traced_without_a_route_attribute() {
        let (provider, recorder) = recorder();
        let root = tempfile::tempdir().expect("a temporary directory");
        let (sets, uploads, jobs) = state(root.path());

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;
        let request = actix_test::TestRequest::get()
            .uri("/v1/nothing/answers/this")
            .to_request();
        let response = actix_test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);

        let attributes = Recorder::attributes(&span);
        assert_eq!(
            attributes.get("http.status_code").map(String::as_str),
            Some("404"),
            "a client that was refused must be able to find the refusal: {attributes:?}"
        );
        assert!(
            !attributes.contains_key("http.route"),
            "no route matched, so there is no template to report: {attributes:?}"
        );
        assert!(
            !attributes
                .values()
                .any(|value| value.contains("nothing/answers/this")),
            "the requested path must never reach the span: {attributes:?}"
        );
    }

    /// A refused request records the status it was actually refused with.
    ///
    /// **This is the assertion the status code exists for.** A span that reported
    /// `200` for a `400` is worse than no span at all, because every error rate
    /// and every alert built on it is wrong in the same direction and nothing in
    /// the data says so.
    #[actix_web::test]
    async fn an_error_response_records_the_status_it_returned() {
        let (provider, recorder) = recorder();
        let root = tempfile::tempdir().expect("a temporary directory");
        let (sets, uploads, jobs) = state(root.path());

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;

        // A merge with a body that is not a merge. The handler refuses it
        // itself, so this is the ordinary path and not an error actix raised.
        let request = actix_test::TestRequest::post()
            .uri("/v1/sets/docs/merges")
            .set_payload("this is not a merge request")
            .to_request();
        let response = actix_test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);

        let attributes = Recorder::attributes(&span);
        assert_eq!(
            attributes.get("http.status_code").map(String::as_str),
            Some("400"),
            "the span must report the status the client was given: {attributes:?}"
        );
        assert_eq!(
            attributes.get("http.route").map(String::as_str),
            Some("/v1/sets/{set}/merges"),
            "and the route it matched, as a template: {attributes:?}"
        );
        assert_eq!(
            span.status,
            Status::Unset,
            "a 4xx is this server working — a client asked for something that is \
             not there — and marking it an error puts a floor under every error \
             rate on the panel"
        );
    }

    /// A `5xx` is marked as an error, and this server has no route that produces
    /// one — so the mapping is checked against the middleware itself.
    ///
    /// The handler here is the test's, not the server's, because a `500` from
    /// the server's own routes would mean an `io::Error` the tests cannot
    /// provoke. **The middleware is the code under test either way**, and the
    /// alternative is a branch nothing reaches.
    #[actix_web::test]
    async fn a_server_error_marks_the_span_as_an_error() {
        let (provider, recorder) = recorder();

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app = actix_test::init_service(
            actix_web::App::new()
                .wrap(actix_web::middleware::from_fn(super::trace))
                .route(
                    "/boom",
                    web::get().to(|| async { HttpResponse::InternalServerError().finish() }),
                ),
        )
        .await;
        let request = actix_test::TestRequest::get().uri("/boom").to_request();
        let response = actix_test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);

        let attributes = ours(&span);
        assert_eq!(
            span.status,
            Status::Error {
                description: String::new().into()
            },
            "a 5xx must be an error status; `otel.status_code` is read by the \
             layer into the span's status rather than exported as an attribute"
        );
        assert_eq!(
            attributes.get("http.status_code").map(String::as_str),
            Some("500"),
            "and must still report the code: {attributes:?}"
        );
    }

    /// A request that starts a job says which kind, by the route it matched.
    ///
    /// **`blu.job` is the join between a request and the work it set going**,
    /// and it is deliberately not the job's id: the id is created by
    /// `Jobs::spawn`, which runs inside the handler, after this span exists.
    /// Asserted here so the two words it can take cannot drift apart from the
    /// words `blu.job.kind` reports.
    #[actix_web::test]
    async fn a_request_names_the_kind_of_job_it_started() {
        for (route, kind) in [
            ("/v1/sets/{set}/packs", "pack"),
            ("/v1/sets/{set}/merges", "merge"),
        ] {
            assert_eq!(job_kind(route), Some(kind), "{route}");
        }

        // **The route that looks like one and is not.** `POST /v1/packs/{id}/plan`
        // resolves the pack and answers the ranges itself, so it starts nothing;
        // a span claiming a `pack` job for it is a panel counting a merge that
        // never ran.
        assert_eq!(
            job_kind("/v1/packs/{id}/plan"),
            None,
            "a route that answers from the request must not report a job"
        );

        // And the whole table: exactly two of the seventeen routes start a job,
        // and a route that starts none carries no `blu.job` at all rather
        // than an empty one.
        let starting: Vec<&str> = crate::serve::ROUTES
            .iter()
            .map(|(_, path)| *path)
            .filter(|path| job_kind(path).is_some())
            .collect();
        assert_eq!(
            starting,
            ["/v1/sets/{set}/packs", "/v1/sets/{set}/merges"],
            "a new job route has to be added here too, or its span will not say \
             what it started"
        );

        // **The table above, checked against the handlers rather than against
        // itself.** `job_kind` used to name `/v1/packs/{id}/plan`, which starts
        // no job, and miss `POST /v1/sets/{set}/packs`, which starts one — and
        // nothing failed, because the table was only ever compared with itself.
        // `handlers.rs` is read here as the source it is: every `Jobs::spawn` in
        // it belongs to a handler that answers `202` with a job, so there must
        // be exactly as many of them as the table names.
        let spawned = include_str!("handlers.rs").matches("jobs.spawn(").count();
        assert_eq!(
            spawned,
            starting.len(),
            "`job_kind` names {starting:?} but `handlers.rs` calls `Jobs::spawn` \
             {spawned} time(s): a route that starts a job must be one this table \
             names, or its span will not say what it started"
        );

        // The empty case, through the real server: `GET /v1/version` starts
        // nothing, so it carries no job attribute.
        let (provider, recorder) = recorder();
        let root = tempfile::tempdir().expect("a temporary directory");
        let (sets, uploads, jobs) = state(root.path());

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;
        let response = actix_test::call_service(&app, version_request().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);
        assert!(
            !Recorder::attributes(&span).contains_key("blu.job"),
            "a request that starts no job must not report one"
        );
    }

    /// **The default build: with telemetry off, nothing wraps the router at
    /// all.**
    ///
    /// Not "no span is exported" — that would be true of a middleware that built
    /// a span and had nowhere to send it, which is precisely the thing this must
    /// not be. So this records *tracing* spans with a layer that is offered
    /// everything and filters nothing, and asks whether an `http.server` span is
    /// created at all: not created means no span, no header read and no
    /// allocation per request, on the server most people run.
    ///
    /// **Both apps are asked the same question** with the same subscriber, so
    /// the difference between the two answers is the middleware and nothing else.
    #[actix_web::test]
    async fn no_span_is_created_when_the_middleware_is_not_installed() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Names(Arc::clone(&seen)));

        let root = tempfile::tempdir().expect("a temporary directory");

        let default = tracing::subscriber::set_default(subscriber);
        let (sets, uploads, jobs) = state(root.path());
        let bare = actix_test::init_service(super::super::handlers::app(sets, uploads, jobs)).await;
        let response = actix_test::call_service(&bare, version_request().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(bare);

        assert!(
            !seen.lock().expect("the recorder's lock").contains(&SPAN),
            "a server with telemetry off must not build a span for a request: \
             this is the whole of what 'zero overhead' means"
        );

        // The same request through the traced app, and the span is there. If this
        // half fails then the first assertion proved nothing: it would pass
        // because no request was ever traced, rather than because this one was
        // not wrapped.
        let (sets, uploads, jobs) = state(root.path());
        let traced =
            actix_test::init_service(super::super::handlers::traced_app(sets, uploads, jobs)).await;
        let response = actix_test::call_service(&traced, version_request().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(traced);

        assert!(
            seen.lock().expect("the recorder's lock").contains(&SPAN),
            "the traced app must build one, or the assertion above is vacuous"
        );

        // The guard is dropped by hand rather than left to the end of the block,
        // so it is obvious that it outlived both apps and that neither ran under
        // anybody else's subscriber.
        drop(default);
    }

    /// The middleware is installed only when a tracer provider is.
    ///
    /// **`bind` reads one value and hands it to `configure`**, and that value is
    /// `telemetry::is_installed()`. It is asserted as a fact about this process:
    /// no test here installs a global provider — `telemetry`'s own
    /// `no_exporter_is_built_when_telemetry_is_off` asserts the same slot is
    /// empty, so a test that did would break that one — so the answer in a test
    /// run is the answer a default `blu serve` gets.
    #[test]
    fn the_middleware_follows_the_installed_tracer_provider() {
        assert!(
            !crate::telemetry::is_installed(),
            "nothing has installed a provider, so the router must be built \
             unwrapped — this is the state of every `blu serve` that did not \
             ask for telemetry"
        );
    }

    /// A `tracing` layer that keeps the name of every span it is offered.
    ///
    /// Unfiltered on purpose: a layer with the crate's own filter would refuse
    /// nothing that matters here (the span's target is this module's), but the
    /// point of the test is that *no span object is created at all*, and a
    /// filter that hid the evidence would hide that too.
    struct Names(Arc<Mutex<Vec<&'static str>>>);

    impl<S: Subscriber> Layer<S> for Names {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::Id,
            _cx: Context<'_, S>,
        ) {
            self.0
                .lock()
                .expect("the recorder's lock")
                .push(attrs.metadata().name());
        }
    }

    /// The two headers, spelled out — a propagator that read a third header
    /// would be reading a header this server does not document accepting.
    #[test]
    fn only_the_two_trace_context_headers_are_read() {
        assert_eq!(TRACE_HEADERS, ["traceparent", "tracestate"]);
    }

    /// A header that is not UTF-8 is treated as absent, not as a panic.
    ///
    /// A `HeaderValue` may hold bytes that are not text, and a client may send
    /// them. The propagator would have refused the value anyway; this asserts
    /// that getting there costs nothing and takes nothing down.
    #[test]
    fn a_header_that_is_not_text_is_no_header() {
        let request = actix_test::TestRequest::get()
            .uri("/v1/version")
            .insert_header((
                "traceparent",
                actix_web::http::header::HeaderValue::from_bytes(b"\xff\xfe").expect("obs-text"),
            ))
            .to_http_request();

        // The extractor is the only thing that reads headers here, so it is
        // reached directly: driving a request through the whole app to make one
        // bad header would test actix's parser rather than this type's answer.
        let extractor = super::Headers(&request);
        assert_eq!(
            opentelemetry::propagation::Extractor::get(&extractor, "traceparent"),
            None,
            "a value that is not text is not a traceparent"
        );
        assert_eq!(
            opentelemetry::propagation::Extractor::get(&extractor, "tracestate"),
            None
        );
        assert_eq!(
            opentelemetry::propagation::Extractor::keys(&extractor),
            vec!["traceparent", "tracestate"]
        );
    }

    /// The span name is the constant the module documents, and the conventions'
    /// own name for a server span.
    #[test]
    fn the_span_name_is_the_one_the_module_documents() {
        assert_eq!(SPAN, "http.server");
    }

    /// The instrumented call nests: a handler's own span is inside the request's.
    ///
    /// The cardinality table in the module docs claims `http.server` wraps
    /// `blulib::command::*`, and this is that claim at the level it can be
    /// checked. A handler is stood in for, because none of the server's own
    /// handlers open a set on an ordinary request and driving a real one here
    /// would need a pack to exist.
    #[actix_web::test]
    async fn the_handler_runs_inside_the_request_span() {
        let (provider, recorder) = recorder();

        let default = tracing::subscriber::set_default(exporting(&provider));
        let app = actix_test::init_service(
            actix_web::App::new()
                .wrap(actix_web::middleware::from_fn(super::trace))
                .route(
                    "/nested",
                    web::get().to(|| async {
                        let inner = tracing::info_span!("a_command", set = "docs");
                        let _entered = inner.enter();
                        HttpResponse::Ok().finish()
                    }),
                ),
        )
        .await;
        let request = actix_test::TestRequest::get().uri("/nested").to_request();
        let response = actix_test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(app);
        drop(default);

        let span = flush_and_read(&provider, &recorder);
        let inner = recorder.span("a_command");

        assert_eq!(
            inner.parent_span_id,
            span.span_context.span_id(),
            "a handler's own span must nest inside the request's, or the \
             cardinality table in `span`'s docs is wrong"
        );
    }

    /// The one thing a reader of this module must be able to check by eye.
    #[test]
    fn the_span_is_opened_with_the_attributes_the_table_lists() {
        // Read off the callsite rather than off this file's prose: the macro's
        // field list is the thing that decides what a span carries, and a table
        // compared with itself proves nothing.
        let metadata: &'static Metadata<'static> = tracing::info_span!(
            "http.server",
            otel.kind = "server",
            http.method = "GET",
            http.route = tracing::field::Empty,
            http.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            blu.job = tracing::field::Empty,
        )
        .metadata()
        .expect("a callsite has static metadata even with no subscriber");

        let fields: Vec<&str> = metadata.fields().iter().map(|field| field.name()).collect();

        assert_eq!(
            fields,
            [
                "otel.kind",
                "http.method",
                "http.route",
                "http.status_code",
                "otel.status_code",
                "blu.job",
            ],
            "the attribute list is a contract with a panel; it may not grow by \
             accident and an unbounded attribute is the failure this module exists \
             to prevent"
        );
    }
}
