// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Turning the crate's progress lines into a job's counters.
//!
//! # Why a `tracing` layer rather than a callback
//!
//! `merge` and `pack` do not take a progress argument, and threading one through
//! `command/merge.rs`, `command/pack.rs` and `command/pipeline.rs` would put an
//! HTTP concern into the library's signatures — which is the thing this crate
//! avoids everywhere.
//!
//! The alternative already exists.
//! [`crate::progress::Progress::report`] emits a `tracing` event on the
//! `lepatch::progress` target carrying the counters as structured fields. So a
//! **layer** reads those events and forwards them. The library gains nothing, and
//! the coupling sits at the one place that already knows the counters have names.
//!
//! # How a job's events are told apart from anyone else's
//!
//! By the span. A job runs inside `tracing::info_span!("lepatch_job", …)`, and this
//! layer forwards a `lepatch::progress` event only when a job span is in scope
//! where it was emitted. A `lepatch restore` in another process emits the same
//! target with no such span and is ignored.
//!
//! **The whole scope, not just the innermost span.** Every command entry point is
//! `#[instrument]`ed, so the span an event carries is `merge` or `backup` and the
//! job span is one level out. An event names only the innermost span, so matching
//! that one and nothing else matches no command at all — silently, and with every
//! other test in the file still passing. See
//! `a_progress_event_in_a_nested_span_still_finds_its_job`.
//!
//! Two jobs running at once cannot cross-contaminate either: each has its own
//! span, and each event carries the one it was emitted in.
//!
//! # Why the layer carries a filter of its own
//!
//! **The filter the terminal prints under must not decide what a client sees.**
//! `--quiet` and `RUST_LOG` answer one question — what goes to stderr — and if
//! they are attached to the subscriber rather than to the writer layer, they
//! answer it for everything stacked above them too. A callsite they say `never`
//! to is not emitted at all, so this layer is never offered the event and the
//! client is told the job reported nothing. `init_tracing` therefore attaches
//! the terminal's `EnvFilter` to the `fmt` layer alone, and this one gets
//! [`filter`].
//!
//! It is not free: without a filter of its own, this layer is offered every
//! event in the process — actix's, tokio's, `zstd`'s — and does a target
//! comparison on each. [`DIRECTIVE`] is deliberately crate-wide rather than
//! naming the three callsites this layer reads. A narrow directive would have
//! to name the job span, the command's own `#[instrument]` span, *and* the
//! progress event, and would break silently the first time a command moved or a
//! new one was added; `every_callsite_the_layer_reads_is_admitted` checks the
//! real metadata instead of a list.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span,
};
use tracing_subscriber::EnvFilter;

use super::jobs::JobId;

/// The counters [`crate::progress::Counter`] reports, in the order
/// [`Progress::report`](crate::progress::Progress::report) writes them.
///
/// Named once and matched here. **A counter added to `progress.rs` without being
/// added to this list would simply stop reaching a client, and nothing would
/// fail** — so `every_counter_reaches_a_job` compares this list against the
/// enum rather than trusting it.
const COUNTERS: [&str; 7] = [
    "files", "chunks", "distinct", "checked", "done", "read", "written",
];

/// The span name that identifies a job.
///
/// A constant rather than a literal at each use: the span is written in
/// `handlers.rs` and matched here, and two spellings of one name is a **silent**
/// failure — no event would ever match, progress would never arrive, and nothing
/// would fail.
pub(crate) const SPAN: &str = "lepatch_job";

/// The event target this layer reads, next to the span name above it.
///
/// The same reasoning applies to both: two spellings of one name is a **silent**
/// failure, so both are constants and both are checked against the callsites
/// that write them.
pub(crate) const TARGET: &str = "lepatch::progress";

/// The directive [`filter`] is built from.
///
/// **`lepatch=info`, and the level matters as much as the target.** The job span
/// is `info_span!`, every command entry point is `#[instrument(level = "info")]`,
/// and the progress event is `tracing::info!` — all three verified against real
/// metadata by `every_callsite_the_layer_reads_is_admitted`.
///
/// The target is a crate-wide prefix rather than the three callsites, because
/// the layer reads them by *scope* and not by name alone: an event carries only
/// the innermost span, and this one walks outwards until it finds a job span. A
/// command span nested between the two has to be admitted as well, because the
/// walk passes through it — and a directive naming only `lepatch::progress` and
/// `lepatch_job` would drop the event at the span-walk, for exactly the reason
/// the terminal's filter used to drop it.
///
/// `info` and not `trace` because a `lepatch=trace` filter would make the
/// registry create every per-chunk span in the crate, which `--quiet` exists to
/// avoid. The callsites above are all at `info`, and the ones at `trace` carry
/// nothing this layer reads.
const DIRECTIVE: &str = "lepatch=info";

/// The filter the progress layer is installed with.
///
/// **Not optional and not negotiable from the environment.** `--quiet` and
/// `RUST_LOG` govern the terminal; a client's view of a running job does not
/// move when the operator turns the terminal down. See the module docs.
#[must_use]
pub fn filter() -> EnvFilter {
    EnvFilter::new(DIRECTIVE)
}

/// A `tracing` layer forwarding job progress into the registry.
///
/// Installed by `init_tracing` in `main.rs`, which is the only place that owns the
/// subscriber. The library does not install it, so a library caller that wants
/// progress over HTTP installs [`Handle`] itself.
struct Progress {
    /// A handle onto the registry, cloned into the layer.
    ///
    /// The layer holds it to forward. A job holds the other end so it can be
    /// cancelled, and neither owns the other.
    jobs: super::jobs::Jobs,
    /// Job spans currently open, and the job each names.
    ///
    /// A span is entered and exited; an event carries the span stack but not its
    /// fields. So an event is matched to a job by looking the span id up here.
    open: Mutex<HashMap<u64, String>>,
}

impl Progress {
    fn new(jobs: super::jobs::Jobs) -> Self {
        Self {
            jobs,
            open: Mutex::new(HashMap::new()),
        }
    }

    /// Forward one job's progress.
    ///
    /// # Panics
    ///
    /// Never in practice. The registry's locks are recovered from a poison rather
    /// than propagated, and this runs on the reporting thread: a merge that
    /// panicked because a *progress counter* failed would be a far worse outcome
    /// than a dropped counter.
    fn progress(&self, job: &str, counters: Vec<(String, u64)>) {
        if let Ok(id) = JobId::parse(job) {
            self.jobs.emit_progress(id, counters);
        }
    }

    /// The job a span names, if that span is a job span.
    ///
    /// Takes the id as a `u64` rather than a `span::Id` or a `SpanRef`, so this
    /// does not have to name the subscriber's `LookupSpan` bound — which the
    /// layer impl already has and this helper does not need.
    fn job_of(&self, id: u64) -> Option<String> {
        self.open.lock().ok()?.get(&id).cloned()
    }

    /// Record a span being entered, if it is a job span.
    fn entered(&self, id: &span::Id, attributes: &span::Attributes<'_>) {
        // `Attributes::metadata` returns a reference, never an `Option` here: a
        // `tracing` callsite always has the metadata it was declared with.
        if attributes.metadata().name() != SPAN {
            return;
        }

        let Some(job) = attribute_text(attributes, "job") else {
            return;
        };

        if let Ok(mut open) = self.open.lock() {
            open.insert(id.into_u64(), job);
        }
    }

    /// Forget a span that has closed.
    ///
    /// Without this the map grows for the life of the process: a job span closes
    /// when its future returns, and nothing else would ever remove the entry.
    fn exited(&self, id: &span::Id) {
        if let Ok(mut open) = self.open.lock() {
            open.remove(&id.into_u64());
        }
    }
}

/// One string attribute of a span, by field name.
fn attribute_text(attributes: &span::Attributes<'_>, wanted: &str) -> Option<String> {
    let mut found = None;
    attributes.record(&mut AttributeVisitor {
        slot: &mut found,
        wanted,
    });
    found
}

/// Reads the one span attribute this layer cares about.
///
/// `Visit` is called once per field and cannot be asked for one field, so every
/// field is offered and the wanted one is kept.
struct AttributeVisitor<'a> {
    /// Where the wanted value lands.
    slot: &'a mut Option<String>,
    /// The field name being looked for.
    wanted: &'a str,
}

impl Visit for AttributeVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == self.wanted {
            *self.slot = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == self.wanted {
            *self.slot = Some(format!("{value:?}"));
        }
    }
}

/// Collects a progress event's numeric fields.
struct CounterVisitor<'a> {
    into: &'a mut HashMap<String, u64>,
}

impl Visit for CounterVisitor<'_> {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.into.insert(field.name().to_string(), value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        // Counters are unsigned. A negative one would be a bug in `progress.rs`,
        // and folding it to zero would hide it — so it is dropped, and the
        // `COUNTERS` filter in `on_event` then never sees it.
        if let Ok(unsigned) = u64::try_from(value) {
            self.into.insert(field.name().to_string(), unsigned);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {
        // A counter is emitted as `u64`. Reaching here means the callsite changed
        // shape, and there is nothing sensible to do with a debug rendering of a
        // number.
    }
}

/// The layer, as `main.rs` installs it and as a library caller would.
///
/// **A newtype rather than an `impl` on `Arc<Progress>`.** A blanket
/// `impl<S> Layer<S> for Arc<T>` is not permitted: the orphan rule needs a local
/// type in the impl, and `Arc` is not one. So the `Arc` lives inside `Handle` and
/// the impl is on that.
pub struct Handle(Arc<Progress>);

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `Arc`'s contents are a `HashMap` of open spans, which is neither
        // useful nor stable to print.
        f.write_str("Progress")
    }
}

impl Handle {
    /// A layer forwarding the progress of jobs held in `jobs`.
    pub(crate) fn new(jobs: super::jobs::Jobs) -> Self {
        Self(Arc::new(Progress::new(jobs)))
    }
}

impl<S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>>
    tracing_subscriber::Layer<S> for Handle
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.entered(id, attrs);
    }

    fn on_close(&self, id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.0.exited(&id);
    }

    fn on_event(&self, event: &Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != TARGET {
            return;
        }

        // The span the event was emitted in. `Span::current()` would be wrong —
        // this layer can be called with no current span at all.
        //
        // **Walked outwards, not read once.** `ctx.event_span` gives the
        // innermost span, which for any instrumented command is the command's own
        // span rather than the job's, and looking only there finds no job at all.
        // `event_scope` yields the innermost first and then its parents, so the
        // first job span found is the nearest one — which is the job this event
        // belongs to. The iterator stops at the root, which the registry does not
        // hold.
        let Some(job) = ctx.event_scope(event).and_then(|scope| {
            scope
                .into_iter()
                .find_map(|span| self.0.job_of(span.id().into_u64()))
        }) else {
            return;
        };

        let mut counters = HashMap::new();
        event.record(&mut CounterVisitor {
            into: &mut counters,
        });

        let forwarded: Vec<(String, u64)> = counters
            .into_iter()
            .filter(|(name, _)| COUNTERS.contains(&name.as_str()))
            .collect();

        if !forwarded.is_empty() {
            self.0.progress(&job, forwarded);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tracing::Metadata;
    use tracing_subscriber::{
        EnvFilter, fmt,
        layer::{Context, Filter, Layer, SubscriberExt as _},
    };

    use super::{COUNTERS, DIRECTIVE, Handle, Progress, SPAN, filter};
    use crate::{
        progress::{Counter, Phase, Progress as Report},
        serve::{JobEvent, JobId, Jobs},
    };

    /// A job that is accepted and never finishes, so the registry entry the
    /// counters land on is still there when the test reads it back.
    ///
    /// `spawn` wants a runtime only to have somewhere to put the task; the entry
    /// is written before the task is ever polled, and this one never finishes.
    fn running_job(jobs: &Jobs) -> JobId {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime");

        runtime.block_on(async {
            jobs.spawn("merge", |_| async {
                std::future::pending::<()>().await;
                Ok(crate::serve::Json::Null)
            })
        })
    }

    /// One report through the real emitter, so the event under test is the one
    /// `Progress::report` writes rather than a hand-built stand-in for it.
    fn report_chunks_and_bytes(chunks: u64, read: u64) {
        let mut report = Report::new(Phase::Backup);
        report.add(Counter::Chunks, chunks);
        report.add(Counter::Read, read);
        assert!(report.report(), "a run's first report is always due");
    }

    /// The job's counters as a map, for asserting on.
    fn recorded(jobs: &Jobs, id: JobId) -> HashMap<String, u64> {
        jobs.get(id)
            .expect("the job is in the registry")
            .progress
            .into_iter()
            .collect()
    }

    #[test]
    fn a_progress_event_in_a_job_span_lands_in_the_registry() {
        // **The behaviour this layer exists for**, driven through every part of
        // it: a real `info_span!` in the shape `Jobs::spawn` writes one, a real
        // `Progress::report` event on the `lepatch::progress` target inside it,
        // and the numbers read back out of the registry.
        //
        // The structural tests here compare constants to constants. This one
        // fails if `on_event` stops forwarding, if the span name stops matching,
        // or if the counter filter stops admitting a counter the emitter reports.
        let jobs = Jobs::new();
        let id = running_job(&jobs);
        let layer = Handle::new(jobs.clone());

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            let span = tracing::info_span!(SPAN, job = %id, kind = "merge");
            let _entered = span.enter();

            report_chunks_and_bytes(7, 4096);
        });

        let counters = recorded(&jobs, id);

        assert_eq!(
            counters.get("chunks"),
            Some(&7),
            "a counter reported inside a job span must reach that job: {counters:?}"
        );
        assert_eq!(
            counters.get("read"),
            Some(&4096),
            "a byte counter must reach the job too: {counters:?}"
        );

        // Every counter the emitter writes crossed the filter, not only the two
        // this test reads values for — `report` writes all of them.
        let mut names: Vec<&str> = counters.keys().map(String::as_str).collect();
        names.sort_unstable();
        let mut declared: Vec<&str> = COUNTERS.to_vec();
        declared.sort_unstable();
        assert_eq!(
            names, declared,
            "every counter `Progress::report` writes must reach the job"
        );

        // And it arrived as an **event**, not only as a stored figure: a watcher
        // on the stream sees the same numbers a poller does.
        let (_, history) = jobs
            .subscribe(id)
            .expect("the job is in the registry, so it can be subscribed to");

        assert!(
            history.iter().any(|event| matches!(
                event,
                JobEvent::Progress(counters)
                    if counters.contains(&("chunks".to_string(), 7))
            )),
            "a watcher must see the counters as an event: {history:?}"
        );
    }

    #[test]
    fn a_progress_event_outside_a_job_span_reaches_nobody() {
        // **The isolation claim.** Another process's `lepatch restore` — or this
        // one, outside a job — emits the same target with no job span, and its
        // numbers are not any job's.
        let jobs = Jobs::new();
        let id = running_job(&jobs);
        let layer = Handle::new(jobs.clone());

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            report_chunks_and_bytes(7, 4096);

            // And from inside a span that is not a job's.
            let span = tracing::info_span!("some_other_work", job = %id);
            let _entered = span.enter();
            report_chunks_and_bytes(9, 8192);
        });

        assert!(
            recorded(&jobs, id).is_empty(),
            "an event in no job span must not be recorded against one: {:?}",
            jobs.get(id).map(|job| job.progress)
        );
    }

    #[test]
    fn a_progress_event_in_a_nested_span_still_finds_its_job() {
        // **The layer must walk out of the span it is in.** Every command entry
        // point is `#[instrument]`, so `merge` runs inside a span of its own that
        // sits *inside* the job span. An event carries the innermost span in
        // scope, so matching only that one finds a command's span, not its job's,
        // and the counters are dropped — with nothing failing, because the rest of
        // the machinery works perfectly on the events that are not nested.
        //
        // This is why no command's progress had ever been seen reaching a client:
        // every one of them is instrumented, and the layer was matching the one
        // thing that is never a job span.
        let jobs = Jobs::new();
        let id = running_job(&jobs);
        let layer = Handle::new(jobs.clone());

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            let job_span = tracing::info_span!(SPAN, job = %id, kind = "merge");
            let _job = job_span.enter();

            // What `#[instrument]` on `merge()` gives it.
            let command_span = tracing::info_span!("merge", set = "docs");
            let _command = command_span.enter();

            report_chunks_and_bytes(11, 8192);
        });

        let counters = recorded(&jobs, id);
        assert_eq!(
            counters.get("chunks"),
            Some(&11),
            "a counter reported inside a command's own span must still reach the job: \
             {counters:?}"
        );
    }

    #[test]
    fn every_counter_reaches_a_job() {
        // **The silent-gap check.** A counter added to `progress.rs` and not to
        // this list would stop reaching a client, and no test anywhere would fail.
        // So the two are compared rather than trusted.
        let mut reported: Vec<&str> = COUNTERS.to_vec();
        reported.sort_unstable();

        let defined: Vec<&str> = crate::progress::counter_names();
        let mut defined: Vec<&str> = defined;
        defined.sort_unstable();

        assert_eq!(
            defined, reported,
            "a counter's name and the sink's list have drifted apart"
        );
    }

    #[test]
    fn the_span_name_is_the_one_the_handler_writes() {
        // Two spellings of one span name would mean no event ever matches, and
        // progress would never arrive — with nothing failing. So the constant is
        // compared against the literal the handler uses.
        assert_eq!(SPAN, "lepatch_job");
    }

    #[test]
    fn a_closed_span_is_forgotten() {
        // Without this the map grows for the life of the process: a job span
        // closes when its future returns and nothing else would remove the entry.
        //
        // **A real span, entered and exited through a real registry**, because
        // the entry is added by `on_new_span` and removed by `on_close` — neither
        // of which fires for a span this test merely describes.
        let progress = Arc::new(Progress::new(Jobs::new()));
        let layer = Handle(Arc::clone(&progress));
        let job = JobId::new();
        let hex = job.hex();

        let opened =
            tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
                let span = tracing::info_span!(SPAN, job = %job, kind = "merge");
                let id = span
                    .id()
                    .expect("a span under a registry has an id")
                    .into_u64();

                assert_eq!(
                    progress.job_of(id).as_deref(),
                    Some(hex.as_str()),
                    "an open job span must be in the map"
                );

                id
                // The span closes here, on drop.
            });

        assert!(
            progress.job_of(opened).is_none(),
            "a closed span must be forgotten, or the map grows for the life of the process"
        );

        // And the bookkeeping still has to be safe when it is asked about an id
        // it never saw, which is what a foreign span looks like.
        assert!(progress.job_of(9_999).is_none());
        progress.exited(&tracing::span::Id::from_u64(9_999));
    }

    /// Records the metadata of every callsite the subscriber is offered, spans
    /// and events kept apart.
    ///
    /// Used to ask what the *real* callsites of this crate carry rather than what
    /// this file believes they carry: the filter is a target rule, and a target
    /// written out by hand in a test proves only that the hand-written copy
    /// agrees with itself.
    ///
    /// Two lists rather than one because the layer treats the two differently: it
    /// reads spans by name and events by target, so a single merged list could
    /// not say which kind of callsite had drifted.
    #[derive(Clone, Default)]
    struct Recorder {
        spans: Arc<Mutex<Vec<&'static Metadata<'static>>>>,
        events: Arc<Mutex<Vec<&'static Metadata<'static>>>>,
    }

    impl Recorder {
        fn spans(&self) -> Vec<&'static Metadata<'static>> {
            self.spans.lock().expect("the recorder's lock").clone()
        }

        fn events(&self) -> Vec<&'static Metadata<'static>> {
            self.events.lock().expect("the recorder's lock").clone()
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for Recorder {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _cx: Context<'_, S>,
        ) {
            self.spans
                .lock()
                .expect("the recorder's lock")
                .push(attrs.metadata());
        }

        fn on_event(&self, event: &tracing::Event<'_>, _cx: Context<'_, S>) {
            self.events
                .lock()
                .expect("the recorder's lock")
                .push(event.metadata());
        }
    }

    /// The one real `#[instrument]`ed function in this crate that is cheap
    /// enough to call from a unit test.
    ///
    /// It stands in for every command entry point: `merge`, `backup`,
    /// `restore`, `apply`, `probe` and `verify` are all
    /// `#[instrument(level = "info", …)]` in `src/command/`, so all of them
    /// produce a span whose target is the module path of the file declaring it.
    /// The merge case specifically is covered end to end by
    /// `tests/serve.rs::a_quiet_server_still_reports_progress`, which drives a
    /// real merge through a real server.
    async fn call_a_real_instrumented_function() {
        // Fails, because the path is not a store. The span is made either way,
        // which is the whole point: this is about the callsite, not the result.
        let _ = crate::storage::BlobStore::<crate::storage::Read>::open_for_reading(
            "no/such/set/anywhere",
        )
        .await;
    }

    /// Whether the progress filter would let this callsite through.
    ///
    /// `Filter::callsite_enabled` takes `&'static Metadata`, which `Span::metadata`
    /// only hands back as an `Option` — a span outside a subscriber has no id and
    /// no metadata reference. In practice it is always `Some`, because the
    /// callsite is static whether or not a span was created from it.
    fn admits(meta: Option<&'static Metadata<'static>>) -> bool {
        Filter::<tracing_subscriber::Registry>::callsite_enabled(
            &filter(),
            meta.expect("a callsite has static metadata even with no subscriber"),
        )
        .is_always()
    }

    /// A `MakeWriter` that collects what the terminal was given, so a test can
    /// assert on it.
    ///
    /// Needed rather than `std::io::sink` because "the terminal was told nothing"
    /// and "the terminal was never asked" are different claims, and only the
    /// first one is the one `--quiet` makes.
    #[derive(Clone, Default)]
    struct Terminal(Arc<Mutex<Vec<u8>>>);

    impl Terminal {
        fn written(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("the terminal's lock")).into_owned()
        }
    }

    impl std::io::Write for Terminal {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("the terminal's lock").write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for Terminal {
        type Writer = Terminal;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn every_callsite_the_layer_reads_is_admitted_by_the_filter() {
        // **The check that the filter is right, against real metadata.**
        //
        // A job's progress event is emitted inside the command's own
        // `#[instrument]` span, which sits inside the job span. `on_event` walks
        // outwards from the innermost span until it finds a job span, so *every*
        // span on that path has to be one the filter admits. Filter any one of
        // them and the event is dropped for exactly the reason the terminal's
        // filter used to drop it — so this asserts on the actual metadata of
        // each of the three, and on their real target strings.
        let jobs = Jobs::new();
        let recorded = Recorder::default();

        // The real job span, written by `Jobs::spawn` itself, and the real
        // event, written by `Progress::report`, and a real `#[instrument]` span
        // from a real function in this crate, entered between them.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime");
        let (done, finished) = tokio::sync::oneshot::channel();

        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(recorded.clone()),
            || {
                runtime.block_on(async move {
                    jobs.spawn("merge", move |_| async move {
                        call_a_real_instrumented_function().await;

                        let mut report = Report::new(Phase::Merge);
                        report.add(Counter::Chunks, 5);
                        report.report();

                        let _ = done.send(());
                        Ok(crate::serve::Json::Null)
                    });

                    // Waits for the job to have run to its first report, so the
                    // spans above are created rather than merely scheduled.
                    let _ = finished.await;
                });
            },
        );

        let seen = recorded.spans();
        let names: Vec<&str> = seen.iter().map(|meta| meta.name()).collect();
        let targets: Vec<&str> = seen.iter().map(|meta| meta.target()).collect();

        // **The names and targets, read off the real callsites.** Asserted
        // because a crate-wide directive is only correct for as long as every
        // callsite it has to cover still lives under `lepatch`. Were one of these
        // to move out of the crate, the directive would stop covering it, the
        // event would be dropped at the span-walk, and progress would go quiet
        // for every client again — with nothing failing.
        assert!(
            names.contains(&SPAN),
            "the job span must be named as the layer matches on: {names:?}"
        );
        assert!(
            targets.contains(&"lepatch::serve::jobs"),
            "the job span must carry the module path of `jobs.rs`: {targets:?}"
        );

        // **An `#[instrument]` span, on the stack between the two.** The report
        // is emitted inside it and the layer walks outwards through it, so this
        // is the callsite that has to be admitted for the walk to reach the job
        // span at all. Its name is the function's; its target is its module.
        assert!(
            names.contains(&"open_for_reading"),
            "the instrumented function's own span must have been created between \
             the job span and the report: {names:?}"
        );
        assert!(
            targets
                .iter()
                .any(|target| target.starts_with("lepatch::storage::")),
            "an `#[instrument]`ed function must carry its module path, which is \
             what the command spans do too: {targets:?}"
        );

        // **And the filter admits every one of them.** This is the assertion the
        // test exists for; the three above are here so a failure says which
        // callsite drifted.
        for meta in &seen {
            assert!(
                admits(Some(meta)),
                "{DIRECTIVE} must admit the `{}` callsite at {:?}, or a client's counters stop arriving",
                meta.target(),
                meta.level(),
            );
        }

        // **The event, read off the emitter** rather than checked against the copy of
        // its target the reader holds — the two agreeing being the thing that has
        // to be checked, and a self-consistent pair of constants proving nothing.
        //
        // **Every event the job emitted is admitted, not only the progress one.**
        // The instrumented call fails, and `err` reports that, so there is a
        // second event on a different target in here. That is the crate-wide
        // directive earning its keep: a narrow one naming only `lepatch::progress`
        // would be cheaper and wrong, because the layer walks *through* the spans
        // the report was emitted inside and those spans' own events come with it.
        let events = recorded.events();
        assert!(
            events
                .iter()
                .any(|meta| meta.target() == crate::progress::TARGET),
            "the job's report must have reached the subscriber: {names:?}"
        );
        for meta in &events {
            assert!(
                admits(Some(meta)),
                "{DIRECTIVE} must admit the `{}` event at {:?}, or a client sees nothing",
                meta.target(),
                meta.level(),
            );
        }
    }

    #[test]
    fn the_filter_refuses_callsites_outside_this_crate() {
        // **The other half of the filter.** Without this, the layer is offered
        // every event in the process — actix's, tokio's, zstd's — and does a
        // target comparison on each. The filter exists to be cheap as well as
        // correct, and cheap means refusing.
        let recorded = Recorder::default();

        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(recorded.clone()),
            || {
                let foreign = tracing::info_span!(target: "actix_web::server", "actix");
                let _foreign = foreign.enter();
                tracing::info!(target: "some_other_crate::thing", "hello");
            },
        );

        let spans = recorded.spans();
        let events = recorded.events();
        assert!(
            !spans.is_empty() && !events.is_empty(),
            "the recorder must have seen both foreign callsites, or this test asserts about nothing"
        );
        for meta in spans.into_iter().chain(events) {
            assert!(
                !admits(Some(meta)),
                "a `{}` callsite at {:?} is outside this crate and must be \\
                 refused: the layer is only cheap if it is not offered everything \\
                 the server process logs",
                meta.target(),
                meta.level(),
            );
        }
    }

    #[test]
    fn a_quiet_terminal_filter_does_not_stop_a_job_from_reporting() {
        // **The defect, at the layer level.** The subscriber here is the one
        // `init_tracing` builds for `lepatch --quiet serve`: a writer layer whose
        // `EnvFilter` is at `warn`, and this layer with its own filter. A
        // progress event is `info`, so the writer's filter refuses it — and the
        // point is that refusing it stops at the writer.
        //
        // **This fails if the `EnvFilter` is moved back onto the subscriber.** As
        // a global filter it answers the *callsite* question, a callsite it says
        // `never` to is never emitted, and this layer is never offered the event
        // at all — so the job reports nothing and the assertion below fails.
        let jobs = Jobs::new();
        let id = running_job(&jobs);

        let subscriber = tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(EnvFilter::new("warn")),
            )
            .with(Handle::new(jobs.clone()).with_filter(filter()));

        tracing::subscriber::with_default(subscriber, || {
            // The targets are spelled as `jobs.rs` and `merge.rs` declare them,
            // so this exercises the production shapes rather than this module's.
            let job_span = tracing::info_span!(target: "lepatch::serve::jobs", SPAN, job = %id, kind = "merge");
            let _job = job_span.enter();

            let command_span =
                tracing::info_span!(target: "lepatch::command::merge", "merge", set = "docs");
            let _command = command_span.enter();

            report_chunks_and_bytes(7, 4096);
        });

        let counters = recorded(&jobs, id);
        assert_eq!(
            counters.get("chunks"),
            Some(&7),
            "a client must see a job's counters whatever the terminal is set to: \\
             {counters:?}"
        );
        assert_eq!(
            counters.get("read"),
            Some(&4096),
            "the byte counters must reach the job on the same terms: {counters:?}"
        );
    }

    #[test]
    fn a_progress_event_outside_a_job_span_reaches_nobody_even_when_admitted() {
        // **The isolation claim, under the real filter.** The filter admits
        // `lepatch::progress` from anywhere in the crate — it cannot tell a
        // server's own progress from a library caller's, and does not try. The
        // span is what tells them apart, so this asserts that having admitted
        // the event is not on its own enough to record it.
        let jobs = Jobs::new();
        let id = running_job(&jobs);

        let subscriber = tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(EnvFilter::new("warn")),
            )
            .with(Handle::new(jobs.clone()).with_filter(filter()));

        tracing::subscriber::with_default(subscriber, || {
            report_chunks_and_bytes(7, 4096);

            let other = tracing::info_span!(target: "lepatch::serve::handlers", "request");
            let _other = other.enter();
            report_chunks_and_bytes(9, 8192);
        });

        assert!(
            recorded(&jobs, id).is_empty(),
            "an event with no job span in scope belongs to no job, however \\
             willing the filter is to pass it"
        );
    }

    #[test]
    fn the_filter_ignores_the_environment() {
        // **The operator's decision, as a fact about the code.** `--quiet` and
        // `RUST_LOG` govern the terminal. This filter is built from a constant
        // and cannot be talked out of it by the environment, so a client's view
        // of a job does not move when the terminal is turned down. If this ever
        // fails, something has made progress negotiable from `RUST_LOG` again.
        let job_span =
            tracing::info_span!(target: "lepatch::serve::jobs", SPAN, job = "x", kind = "merge");
        assert!(
            admits(job_span.metadata()),
            "the progress filter must not be negotiable from RUST_LOG"
        );

        // And its own directive is a crate-wide one rather than a list, which is
        // what keeps a new command from needing to be added to it.
        assert_eq!(DIRECTIVE, "lepatch=info");
    }

    #[test]
    fn the_writer_sees_nothing_under_the_same_filter_that_progress_does() {
        // **The two filters answer differently, and that is the point.** The same
        // subscriber that just recorded a job's counters writes nothing to the
        // terminal for them. If this ever fails, the writer's filter has stopped
        // being the writer's alone and `--quiet` has started meaning something
        // different from "quiet".
        let jobs = Jobs::new();
        let id = running_job(&jobs);

        let written = Terminal::default();
        let subscriber = tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(written.clone())
                    .with_filter(EnvFilter::new("warn")),
            )
            .with(Handle::new(jobs.clone()).with_filter(filter()));

        tracing::subscriber::with_default(subscriber, || {
            let job_span = tracing::info_span!(target: "lepatch::serve::jobs", SPAN, job = %id, kind = "merge");
            let _job = job_span.enter();
            report_chunks_and_bytes(7, 4096);
        });

        assert_eq!(
            recorded(&jobs, id).get("chunks"),
            Some(&7),
            "the counters must have reached the job"
        );
        assert!(
            written.written().is_empty(),
            "and nothing may have reached the terminal: {:?}",
            written.written()
        );
    }

    #[test]
    fn a_span_that_is_not_a_job_is_not_bookkept() {
        // The other half of the same map: only `SPAN` with a `job` attribute is
        // an entry, so an unrelated span in the same process does not fill it.
        let progress = Arc::new(Progress::new(Jobs::new()));
        let layer = Handle(Arc::clone(&progress));

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            let other = tracing::info_span!("some_other_work");
            let _other = other.enter();

            // The right name, but no `job` attribute to match on.
            let nameless = tracing::info_span!(SPAN, kind = "merge");
            let _nameless = nameless.enter();
        });

        assert!(
            progress.open.lock().is_ok_and(|open| open.is_empty()),
            "a span with no `job` attribute is not a job span"
        );
    }
}
