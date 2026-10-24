//! The tests of the CLI itself, and there is one worth having.
//!
//! Everything a command does is tested through the library. **What is tested
//! here is the only thing the library cannot see: the layer stack.** A `tracing`
//! subscriber is process-global and this binary owns the only one there is, so
//! `src/cli/telemetry.rs::init_tracing` is the seam between what the crate
//! records and what anything can see — and a stack assembled here rather than
//! in the library is a stack nothing else exercises.
//!
//! # Why this regressed once already
//!
//! Commit `9ab42a7`. `--quiet` and `RUST_LOG` were attached to the *subscriber*
//! rather than to the `fmt` layer, so a `blu --quiet serve` answered
//! `never` to `blulib::progress`, the event was never emitted, the progress
//! layer above was never offered it, and **a client polling
//! `GET /v1/jobs/{id}` was told a running merge had reported nothing.** No test
//! failed, because every other part of the layer worked perfectly on the events
//! that were not filtered out.
//!
//! A filter is not the only way to do it. Adding a layer to the stack changes
//! the stack's type, changes which callsites reach which layer, and can break
//! `observe` in exactly the same way with no error anywhere — which is why the
//! test below installs **`init_tracing`'s own stack**, by calling
//! `init_tracing`, rather than a copy of it that a later edit would not reach.
//!
//! # The other end of the same question
//!
//! **`tests/observe_stack.rs`** asks what happens when this stack is
//! *assembled* rather than called: it spells the same three layers and three
//! filters out (it has to — `init_tracing` is private to this binary), installs
//! them on the process-global subscriber, binds a real server with
//! `serve::span`'s request middleware wrapped around the router, and pushes a
//! real pack through `POST /v1/sets/{set}/merges`. So the two ends are held at
//! once: this test says the function builds what it claims to build, and that
//! one says the arrangement still carries a client's progress.

use std::collections::HashMap;

use super::init_tracing;

use blulib::{
    progress::{Counter, Phase, Progress},
    serve::{JobId, Jobs},
};

/// Spawn a job, let it report one progress event, and wait for it to finish.
///
/// `Jobs::spawn` puts the work on a tokio task with a `blu_job` span
/// around it, which is the span `observe` matches on; the `Progress::report`
/// below writes the real `blulib::progress` event that layer reads. The
/// oneshot is how the test learns the work has run — `spawn` returns before
/// the task is first polled, so reading the registry immediately would be a
/// race the test would lose half the time.
fn report_progress(jobs: &Jobs) -> JobId {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime");

    let (done, finished) = tokio::sync::oneshot::channel();
    runtime.block_on(async {
        let id = jobs.spawn("merge", move |_handle| async move {
            let mut progress = Progress::new(Phase::Merge);
            progress.add(Counter::Chunks, 7);
            progress.add(Counter::Read, 4096);
            assert!(progress.report(), "a run's first report is always due");

            let _ = done.send(());
            Ok(blulib::serve::Json::Null)
        });

        let _ = finished.await;
        id
    })
}

/// The counters a job recorded, as a map to assert on.
fn counters(jobs: &Jobs, id: JobId) -> HashMap<String, u64> {
    jobs.get(id)
        .expect("the job is in the registry")
        .progress
        .into_iter()
        .collect()
}

/// **`init_tracing`'s full stack, for a quiet server, still carries a client's
/// progress into the registry.**
///
/// `init_tracing` is called rather than reconstructed: this is the stack a
/// `blu serve` run installs, the three layers and three filters in the
/// order `init_tracing` puts them, read off the same code. `--quiet` is on
/// deliberately — the terminal's filter is at `warn`, so every event under it
/// is refused, and the only question is whether that refusal stops where the
/// terminal's own layer says it should.
///
/// **The failure this test exists to catch** is `9ab42a7` again: move that
/// `EnvFilter` from the `fmt` layer onto the subscriber and the job's events
/// are never emitted, the progress layer is never offered them, and the
/// counters below are empty while every other test in this binary still
/// passes.
#[test]
fn the_stack_a_server_installs_still_carries_progress_to_a_client() {
    let jobs = Jobs::new();

    // Telemetry off, so nothing tries to build an exporter and the stack is
    // exactly the one a `blu serve` run that never asked for spans has.
    init_tracing(
        false,
        true,
        Some(&jobs),
        &blulib::telemetry::Config::new(false, None),
    );

    let id = report_progress(&jobs);
    let recorded = counters(&jobs, id);

    assert_eq!(
        recorded.get("chunks"),
        Some(&7),
        "a client's view of a running job must not move when the operator turns \
         the terminal down: {recorded:?}"
    );
    assert_eq!(
        recorded.get("read"),
        Some(&4096),
        "the byte counters must reach the job on the same terms: {recorded:?}"
    );
}

/// Check one store target.
///
/// Explicit staging keeps the test off the environment.
/// No cache directory is read.
/// Small strings only, no filesystem.
#[test]
fn check_store_target_with_set_and_staging() {
    let target = super::resolve_target(
        "backup",
        "file:///remote/docs",
        Some(std::path::Path::new("/stage")),
        None,
    )
    .unwrap();

    let super::Target::Store(target) = target else {
        panic!("a file:// target must resolve to a store");
    };
    assert_eq!(target.set, "docs");
    assert_eq!(target.url.to_string(), "file:///remote");
    assert!(target.staging.starts_with("/stage/blu/stores"));
    assert!(target.staging.ends_with("docs"));
}

/// Check local and serve targets.
///
/// A `TARGET` with no `://` is a local path.
/// An `http://` target splits into server and set.
/// `--staging` with either fails.
/// Small strings only.
#[test]
fn check_local_and_serve_targets() {
    let local = super::resolve_target("restore", "photos", None, None).unwrap();
    assert!(
        matches!(local, super::Target::Local(path) if path.as_os_str() == "photos"),
        "a bare name must resolve to a local path"
    );

    let super::Target::Serve {
        address,
        set,
        token,
    } = super::resolve_target("versions", "http://127.0.0.1:8480/docs", None, None).unwrap()
    else {
        panic!("an http:// target must resolve to a server and a set");
    };
    assert_eq!(address, "http://127.0.0.1:8480");
    assert_eq!(set, "docs");
    assert_eq!(token, None);

    let staged = super::resolve_target(
        "restore",
        "photos",
        Some(std::path::Path::new("/stage")),
        None,
    )
    .unwrap_err();
    assert_eq!(staged.kind(), std::io::ErrorKind::InvalidInput);

    let staged = super::resolve_target(
        "versions",
        "http://127.0.0.1:8480/docs",
        Some(std::path::Path::new("/stage")),
        None,
    )
    .unwrap_err();
    assert_eq!(staged.kind(), std::io::ErrorKind::InvalidInput);
}

/// Check token threading through targets.
///
/// The token travels beside the address, never inside it.
/// Small strings only.
#[test]
fn check_token_travels_beside_address() {
    let super::Target::Serve { token, .. } = super::resolve_target(
        "versions",
        "https://example.test/docs",
        None,
        Some("jwt-value"),
    )
    .unwrap() else {
        panic!("an https:// target must resolve to a server and a set");
    };
    assert_eq!(token.as_deref(), Some("jwt-value"));

    let super::Target::Serve { token, .. } =
        super::resolve_target("versions", "https://example.test/docs", None, None).unwrap()
    else {
        panic!("an https:// target must resolve to a server and a set");
    };
    assert_eq!(token, None);
}

/// Refuse targets without a set name.
///
/// A store `TARGET` without a set fails.
/// A bad URL fails in parsing.
/// Small strings only.
#[test]
fn refuse_target_without_set_name() {
    let missing = super::resolve_target(
        "backup",
        "s3://bucket/backups",
        Some(std::path::Path::new("/stage")),
        None,
    );
    // `s3://bucket/backups` names the store `s3://bucket` holding the set
    // `backups`: a set address always carries its set.
    assert!(missing.is_ok(), "the last segment is the set: {missing:?}");

    let bare = super::resolve_target(
        "backup",
        "s3://bucket",
        Some(std::path::Path::new("/stage")),
        None,
    )
    .unwrap_err();
    assert_eq!(bare.kind(), std::io::ErrorKind::InvalidInput);

    let bad_url = super::resolve_target(
        "versions",
        "gopher://host/backups",
        Some(std::path::Path::new("/stage")),
        None,
    )
    .unwrap_err();
    assert_eq!(bad_url.kind(), std::io::ErrorKind::InvalidInput);
}

/// Check local and network backends.
///
/// `file://` connects.
/// Other schemes read configuration.
/// No network runs here.
#[test]
fn check_local_and_network_backends() {
    let file = blulib::storage::RemoteUrl::parse("file:///remote").unwrap();
    assert!(super::connect_store(&file).is_ok());

    let s3 = blulib::storage::RemoteUrl::parse("s3://bucket/backups").unwrap();
    match super::connect_store(&s3) {
        Ok(_) => panic!("an s3 store must not connect in this test: no network"),
        Err(refused) => {
            assert_eq!(refused.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
}
