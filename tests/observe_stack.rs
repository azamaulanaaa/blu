//! The layer stack `blu serve` installs, and a client's view of a job through it.
//!
//! # What this file is for
//!
//! Every other test of job progress builds the subscriber it needs. **`serve`’s
//! own tests build one the library hands them** — the progress layer on its own,
//! or the progress layer and its filter — because that is the seam they are
//! testing. `src/cli/telemetry.rs::init_tracing` is a different seam: it is the *whole*
//! stack, on the process-global subscriber, and it is the only place in the
//! crate that builds one.
//!
//! And nothing was checking it.
//!
//! # The defect this file exists to catch
//!
//! Commit `9ab42a7`. The terminal’s `EnvFilter` was attached to the subscriber
//! rather than to the `fmt` layer, so it answered `never` to the
//! `blulib::progress` callsite, the event was never emitted, the progress layer
//! above was never offered it, and **a client polling `GET /v1/jobs/{id}` was
//! told a running merge had reported nothing.** No test failed, because every
//! other part of the stack worked perfectly on the events that were not filtered
//! out.
//!
//! That was a filter in the wrong place. **The same silence is reachable without
//! a filter at all**, which is why this file builds the stack rather than
//! mirroring it in an assertion:
//!
//! * A `Filtered` layer only ever sees the callsites **its own** filter enables.
//!   Moving `progress_filter()` off the progress layer, or onto the writer, or
//!   widening the stack so the layer sits somewhere it is not consulted, all
//!   drop a client’s counters with no error anywhere.
//! * **Adding a layer changes the stack’s type**, and can change which callsites
//!   reach which layer, in the same silent direction. The request middleware in
//!   `serve::span` is exactly that: a new layer of machinery added on top of a
//!   stack that was never re-tested once assembled.
//! * `serve::span` reads `telemetry::is_installed()` at bind time and builds
//!   **two different apps** depending on the answer, so the wrapped app is a
//!   service type the unwrapped one never has. Nothing else in the test suite
//!   drives a request through it.
//!
//! So: the stack is built **the way `init_tracing` builds it**, the request
//! middleware is **in it** (which means a tracer provider is installed, which is
//! the same predicate `bind` reads), and a **real merge** runs through the real
//! router over a real socket.
//!
//! # Why the stack is installed globally here
//!
//! Actix serves requests on its own worker threads, and a `tracing` subscriber
//! set with `set_default` is thread-local, so a request handled by a worker
//! would see no subscriber at all. `init_tracing` installs globally for exactly
//! this reason. The two tests here share one installation through a `OnceLock`,
//! which is also the only arrangement a global subscriber allows.
//!
//! # Why this is a file of its own rather than part of `tests/telemetry.rs`
//!
//! Because a global subscriber, a global tracer provider and a bound port are
//! **process-wide state that nothing else in that file may be surprised by**.
//! `tests/telemetry.rs` answers “does a span leave the process?” by running the
//! real binary against a socket, and its assertions are about bytes on the wire
//! from a process that builds its own stack. This file is about the stack this
//! process installs, and putting the two in one binary would couple a
//! `set_global_default` to a suite whose other half deliberately keeps its hands
//! off the subscriber. The name says what it holds: the stack `serve` builds for
//! the layer that turns progress into a client’s counters.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{fs, path::Path, sync::OnceLock, time::Duration};

use blulib::{
    metadata::Snapshot,
    pack::Pack,
    serve::{JobId, Jobs, Json},
    storage::{BlobStore, Plan, StorageGet},
    telemetry::Config,
};
use common::http::Request;
use tokio::io::AsyncReadExt as _;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::{Layer as _, SubscriberExt as _},
};

/// The media type of every pack this server accepts.
const PACK_MEDIA_TYPE: &str = "application/x-blu-pack";

/// The registry the progress layer forwards into.
///
/// One for the process, because the layer is installed once and holds a clone of
/// whatever it was given. Each test reads back the job it started by id, so a
/// shared registry costs nothing and a second installation could not happen.
fn jobs() -> &'static Jobs {
    static JOBS: OnceLock<Jobs> = OnceLock::new();
    JOBS.get_or_init(Jobs::new)
}

/// A runtime to build the OpenTelemetry pipeline on, and one that outlives it.
///
/// **Multi-threaded, and kept in a `OnceLock`.** `telemetry::layer` hands the OTLP
/// batch processor a `tokio` runtime and the processor spawns its worker with
/// `tokio::spawn`, so the stack cannot be built without a runtime in hand — the
/// reason `init_tracing` is called from `main` and not from a lazy initialiser.
/// Multi-threaded because only a multi-threaded runtime keeps executing a spawned
/// task once the `block_on` that spawned it has returned; a current-thread one
/// would leave the export worker parked for the life of the test binary. This is
/// the runtime `#[tokio::main]` gives a real `blu serve`, which is the same
/// arrangement.
fn telemetry_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a multi-thread runtime for the OpenTelemetry pipeline")
    })
}

/// A current-thread runtime, which is what actix hands the worker a job runs on.
///
/// **Each test builds its own.** Two tests sharing one runtime would mean two
/// threads calling `block_on` on the same scheduler at once, and neither test
/// here needs the other's runtime.
///
/// **Why current-thread is the faithful choice, and not a detail.** `Jobs::spawn`
/// enters the job's `blu_job` span and then `.await`s the work with that
/// guard still held, so the span is entered on whichever thread first polls the
/// task. Actix serves each worker on its own current-thread runtime, so a job is
/// polled on one thread for its whole life and the span is in scope for every
/// event it emits. On a **multi-threaded** runtime a task can migrate between
/// workers between polls, the guard is left behind on the thread that entered it,
/// and every event after the migration is emitted in no job span at all — so
/// `observe` drops them and **a client watching the job is told it reported
/// nothing, with no error anywhere**. This harness hit that before it was pinned
/// down: run on a multi-threaded runtime beside the server test, this job
/// reported `{}` in four runs out of six. See the report; `blu serve` is not
/// affected, because actix workers do not migrate tasks.
fn actix_like() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime, the kind actix gives a worker")
}

/// Install `init_tracing`’s stack, for a quiet server with telemetry on.
///
/// **Mirrored from `src/cli/telemetry.rs::init_tracing`, line for line and layer for
/// layer**, and the mirroring is the point: a stack re-derived from the same idea
/// rather than read off the same code proves only that the re-derivation agrees
/// with itself. The order, the three filters and the three layers are that
/// function’s:
///
/// ```text
/// registry()
///     .with(otel)      // telemetry::layer   — its own `blu=info` filter
///     .with(writer)    // fmt::layer         — the terminal’s EnvFilter, attached HERE
///     .with(progress)  // progress_layer     — its own `blulib::serve::progress_filter`
/// ```
///
/// `--quiet` is passed, so the terminal’s filter is at `warn` and **every event
/// under it is refused** — a progress event is `info`, so the writer’s refusal
/// covers all of them. That is the interesting configuration: if the terminal’s
/// filter were answering for the subscriber, this stack would report nothing for
/// any job, and it would be `9ab42a7` again.
///
/// The layer’s `S` is never written out, exactly as in `main`: the OpenTelemetry
/// layer sits **underneath** the writer so the subscriber type under each one is
/// the registry.
fn install_the_stack_a_server_installs() {
    static INSTALLED: OnceLock<()> = OnceLock::new();

    INSTALLED.get_or_init(|| {
        // Inside a runtime, because the OTLP batch processor spawns onto the
        // tokio runtime in hand when it is built — the reason `init_tracing` is
        // called from `main` and not from a lazy initialiser.
        telemetry_runtime().block_on(async {
            let telemetry = Config::new(true, Some(blulib::telemetry::DEFAULT_ENDPOINT));

            // `EnvFilter` exactly as `init_tracing` builds it, from the flags a
            // `--quiet` run passes: a `warn` default, still answerable from the
            // environment the way the real one is.
            let terminal = EnvFilter::builder()
                .with_default_directive(LevelFilter::WARN.into())
                .from_env_lossy();

            let writer = fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(terminal);

            let otel = blulib::telemetry::layer(&telemetry)
                .expect("the OTLP exporter builds with the default endpoint")
                .expect("telemetry was asked for, so a layer comes back");

            // The meters, because `init_tracing` installs them in the same breath
            // and under the same config. Not a layer, but part of what a run
            // starts up, and it is what a job's `blu.job.kind` is measured
            // on.
            blulib::telemetry::metrics::install(&telemetry).expect("the OTLP meter builds");

            let progress = blulib::serve::progress_layer(jobs().clone())
                .with_filter(blulib::serve::progress_filter());

            let subscriber = tracing_subscriber::registry()
                .with(otel)
                .with(writer)
                .with(progress);

            tracing::subscriber::set_global_default(subscriber)
                .expect("one subscriber per process, installed once");
        });
    });
}

/// `blu --quiet serve --telemetry`, in one process: a real merge, over a
/// real socket, through the real router **with the request middleware wrapped
/// around it**.
///
/// **Not a `#[tokio::test]`.** The stack is installed on the process-global
/// subscriber and the server is driven on a runtime this test owns, so both are
/// done by hand and in that order: the subscriber exists before any thread that
/// serves a request does.
#[test]
fn a_client_still_sees_a_running_job_through_the_whole_stack() {
    install_the_stack_a_server_installs();

    // The predicate `serve::bind` reads, asserted rather than assumed. It is
    // what decides whether the router is wrapped in the request middleware at
    // all, so if this is false the test below is not testing what it claims to.
    assert!(
        blulib::telemetry::is_installed(),
        "a provider was installed by the stack, so `bind` wraps the router in \
         `serve::span`'s middleware"
    );

    actix_like().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let pack = a_pack(dir.path()).await;

        let root = dir.path().join("served");
        fs::create_dir_all(&root).unwrap();

        // **The real bind, in this process.** With a provider installed this is
        // `handlers::traced_app`: the router with `serve::span` wrapped around
        // it. Everything else about it is what a `blu serve` run gets.
        let config = blulib::serve::Config::new(&root, "127.0.0.1:0", None);
        let bound = blulib::serve::bind(&config, jobs()).unwrap();
        let address = bound.addr();
        tokio::spawn(async move {
            bound
                .run()
                .await
                .expect("the server should run until it is dropped");
        });

        // A pack in, the way a client pushes one: create an upload, fill it,
        // commit it, and ask for the merge. **Nothing here is a fixture of
        // counters** — the job that runs is `command::merge`, in its own
        // `#[instrument]` span, writing real `blulib::progress` events.
        let begin = Request::new("POST", "/v1/uploads").send(address).await;
        assert_eq!(begin.status, 201, "body: {}", begin.text());
        let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();

        let filled = Request::bytes(
            "PUT",
            &format!("/v1/uploads/{upload}"),
            PACK_MEDIA_TYPE,
            pack,
        )
        .send(address)
        .await;
        assert_eq!(filled.status, 204, "body: {}", filled.text());

        let committed = Request::new("POST", &format!("/v1/uploads/{upload}/commit"))
            .send(address)
            .await;
        assert_eq!(committed.status, 200, "body: {}", committed.text());

        let accepted = Request::json(
            "POST",
            "/v1/sets/docs/merges",
            &serde_json::json!({ "upload": upload }),
        )
        .send(address)
        .await;
        assert_eq!(accepted.status, 202, "body: {}", accepted.text());
        let job = accepted
            .field("job")
            .expect("a 202 names the job it started")
            .as_str()
            .expect("a job id is a string")
            .to_string();

        let document = await_job(address, &job).await;

        // **What a client polls for.** This is the assertion `9ab42a7` broke: a
        // merge that stored chunks, reporting nothing, on a server whose terminal
        // had been turned down. An empty object here is the whole failure.
        let counters = document
            .get("progress")
            .expect("a merge reports progress, so the job document carries it");
        assert!(
            !counters
                .as_object()
                .expect("progress is an object")
                .is_empty(),
            "a merge that stored chunks must report progress with the terminal \
             silent, or every client is told the job reported nothing: {document}"
        );

        // **Real counters, from this crate's own list.** A server that invented a
        // key to look busy would pass an emptiness check.
        let names = blulib::progress::counter_names();
        for key in counters.as_object().expect("an object").keys() {
            assert!(
                names.contains(&key.as_str()),
                "the job document reports {key:?}, which is not a counter name: {names:?}"
            );
        }

        // **And the figure is the merge's own**, counted independently at the end
        // of the run and reported in its result. Two counts of one thing that
        // disagree are a wiring bug.
        assert_eq!(
            counters.get("chunks").and_then(serde_json::Value::as_u64),
            document
                .get("result")
                .and_then(|result| result.get("chunks"))
                .and_then(serde_json::Value::as_u64),
            "the reported chunk total must be the one the merge committed: {document}"
        );

        // **The same numbers in the registry the layer forwarded into.** The
        // document is what a client sees; this is what proves the counters got
        // *there* rather than being written into the response by the handler.
        let recorded: std::collections::HashMap<String, u64> = jobs()
            .get(JobId::parse(&job).unwrap())
            .expect("the job is in the registry")
            .progress
            .into_iter()
            .collect();
        assert_eq!(
            recorded.get("chunks").copied(),
            counters.get("chunks").and_then(serde_json::Value::as_u64),
            "the registry and the document are two views of one set of counters: \
             {recorded:?} against {counters}"
        );
    });
}

/// A job’s counters cross the span its command opens, and reach the registry.
///
/// **The nesting is the whole of this test.** `Jobs::spawn` enters the
/// `blu_job` span and the work runs inside it; `command::backup` is
/// `#[instrument(level = "info", …)]`, so the events the run emits carry
/// *that* span and not the job’s. `observe` therefore has to walk **outwards**
/// from the innermost span to find a job, and every span it walks through has to
/// be one its own filter admits — which is the crate-wide `blu=info`, not a
/// directive naming the three callsites.
///
/// A layer that matched only the innermost span would find no job at all, drop
/// every counter, and fail nothing else in this suite. So this runs the real
/// command rather than a hand-built event: the spans are the ones the crate
/// really declares, at the targets it really uses.
#[test]
fn a_job_reports_through_the_span_its_own_command_opens() {
    install_the_stack_a_server_installs();

    actix_like().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let source = common::source(dir.path());
        fs::write(source.join("a.bin"), common::noisy(200_000)).unwrap();
        fs::write(source.join("b.bin"), common::untagged_stream(3, 180_000)).unwrap();
        let blob = dir.path().join("local");

        // Spawned the way `serve` spawns, with the work running inside the job's
        // span — and the work is a real `blu` command.
        let (done, finished) = tokio::sync::oneshot::channel();
        let id = jobs().spawn("merge", move |_handle| async move {
            let storage = BlobStore::builder(&blob)
                .open_writer()
                .await
                .expect("a fresh set opens for writing");

            let committed = blulib::command::BackupBuilder::new(&source, storage)
                .latest(None)
                .run()
                .await
                .expect("the backup should commit");

            let _ = done.send(committed.version());
            Ok(Json::Null)
        });

        let version = finished.await.expect("the job should finish");
        assert_eq!(
            version, 1,
            "the backup should have committed a first version"
        );

        // **Read back the way `observe` forwards them**, and every one of them is
        // a counter the crate names.
        let recorded: std::collections::HashMap<String, u64> = jobs()
            .get(id)
            .expect("the job is in the registry")
            .progress
            .into_iter()
            .collect();

        assert_eq!(
            recorded.get("files").copied(),
            Some(2),
            "the run walked two files, and a client has to be able to see that: \
             {recorded:?}"
        );
        assert!(
            !recorded.is_empty(),
            "the run reported at least one line — `Progress::due` returns true \
             until it has — and not one counter crossed the command's own span: \
             {recorded:?}"
        );

        // **Only the counters the run had reported are asserted on.** The first
        // line is always due and carries `files`; everything after it is rate
        // limited to one line per reporting interval, so a run this short
        // legitimately reports `chunks` and `read` as zero. Asserting they are
        // non-zero would be asserting that the fixture is slow enough —
        // `tests/serve.rs::a_quiet_server_still_reports_progress_to_a_client`
        // builds a fixture for that, and gets the non-zero figures from a real
        // merge instead.
        assert!(
            recorded.values().any(|value| *value > 0),
            "something the run counted has to have arrived, or the counters are \
             crossing the stack as zeroes: {recorded:?}"
        );

        let names = blulib::progress::counter_names();
        for key in recorded.keys() {
            assert!(
                names.contains(&key.as_str()),
                "{key:?} reached the registry and is not a counter this crate \
                 knows the name of: {names:?}"
            );
        }
    });
}

/// How long [`await_job`] waits for a job to reach a terminal state.
///
/// **A deadline, not an iteration count.** `400` rounds of `10 ms` is 4 s of
/// sleeping *plus* 400 round-trips, so the real budget grew with per-request
/// latency on a loaded machine.
const JOB_BUDGET: Duration = Duration::from_secs(15);

/// Poll `GET /v1/jobs/{job}` until it reaches a terminal state, and hand back
/// the whole document.
///
/// **The document, not the `result`.** The counters are a field of the *job*, and
/// a helper that returned only the result would be asserting about something
/// else.
async fn await_job(address: std::net::SocketAddr, job: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + JOB_BUDGET;

    loop {
        let response = Request::new("GET", &format!("/v1/jobs/{job}"))
            .send(address)
            .await;
        assert_eq!(response.status, 200, "body: {}", response.text());

        let document: serde_json::Value =
            serde_json::from_slice(&response.body).expect("a job document is JSON");
        let state = document
            .get("state")
            .and_then(serde_json::Value::as_str)
            .expect("a job document carries a state");

        match state {
            "succeeded" => return document,
            "failed" | "cancelled" => panic!("job {job} {state}: {document}"),
            _ => {}
        }

        assert!(
            std::time::Instant::now() < deadline,
            "job {job} did not finish within {}s",
            JOB_BUDGET.as_secs()
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A small pack to push, made by this crate's own commands.
async fn a_pack(dir: &Path) -> Vec<u8> {
    let source = common::source(dir);
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("a.bin"), common::noisy(120_000)).unwrap();
    fs::write(
        source.join("nested/b.bin"),
        common::untagged_stream(7, 90_000),
    )
    .unwrap();

    let blob = dir.join("local");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    let (version, snapshot_id) = store.latest().unwrap();

    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut snapshot_bytes = Vec::new();
    reader.read_to_end(&mut snapshot_bytes).await.unwrap();
    let snapshot = Snapshot::try_from(snapshot_bytes.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();

    let at = dir.join("local.pack");
    Pack::write(&store, &plan, &snapshot, version, &at)
        .await
        .unwrap();

    fs::read(at).unwrap()
}
