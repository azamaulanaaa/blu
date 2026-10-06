//! Metric instruments, and the one rule that names them.
//!
//! # Nothing is built until telemetry is on
//!
//! [`install`] returns before it constructs anything when [`Config`] is off: no
//! exporter, no reader, no thread, no socket, and no instrument handle of any
//! kind. Every instrument recording below asks [`installed`] first, and answers a
//! run of a default `lepatch` in a single atomic load; the one function that also
//! writes span attributes reaches `tracing` instead, and a subscriber with no
//! OpenTelemetry layer on it drops them. This is the same default-off promise
//! [`super::layer`] makes for spans, and it is held by
//! `nothing_is_built_and_nothing_is_recorded_when_telemetry_is_off`.
//!
//! # The phase goes in the name, and here is why that is not tidiness
//!
//! [`crate::progress::Counter`] has seven variants and [`crate::progress::Phase`] has
//! five, and
//! **the product of the two does not mean thirty-five things.** It means far
//! fewer, and two of them collide:
//!
//! | phase | `Counter::Distinct` means |
//! |---|---|
//! | `backup` | the **total** distinct chunks in the version just written — `progress.set` in `command/pipeline.rs`, once per committed chunk |
//! | `merge` | the chunks **this run stored** — `progress.add(Counter::Distinct, 1)` in `command/merge.rs`, once per new chunk |
//!
//! One is a per-version total, the other is an accumulating count, and an
//! instrument called `lepatch.distinct` would be right in whichever command
//! somebody had read and wrong in the other. So **every instrument carries its
//! phase**, and the two above are `lepatch.backup.distinct_chunks_in_version`
//! and `lepatch.merge.distinct_chunks_stored` — a reader can tell which is
//! which without opening the source.
//!
//! The wire names in [`crate::progress`] are a contract with `lepatch serve` and are
//! **not** renamed to match. `Counter::name` still answers `"distinct"` for both
//! phases; that is what `progress::counter_names` promises a client. Where an
//! instrument needs a longer word than the wire field, the instrument gets the
//! longer word.
//!
//! # Set or add: which numbers become instruments at all
//!
//! [`Progress::set`] and [`Progress::add`] are not two spellings of one call, so
//! the two kinds of total are not two kinds of metric:
//!
//! * **`add` totals accumulate during the run.** They are a `Counter`, added
//!   once per run by the run's final total. This is the part a dashboard sums
//!   over time — bytes read, chunks checked, files written.
//! * **`set` totals are known before the work starts** (a walk's file count, a
//!   plan's length, a version's chunk count) or describe one version rather than
//!   the run's work. Summing them over time says nothing useful, so they are
//!   **span attributes on the command span** and not instruments at all.
//!
//! `backup`'s `distinct` is a `set` and `merge`'s is an `add`, which is exactly
//! why one is an attribute and the other a counter. **No number appears in both
//! places**: a counter and an attribute carrying the same value would be two
//! copies of one fact that could drift, and a reader would have no way to tell
//! which to believe.
//!
//! ## The whole list, in one table
//!
//! **Every number this crate reports, and which kind it is.** A reader should be
//! able to build a panel from this table and no other evidence — the constants
//! below it agree with it, and `every_instrument_exists_with_the_documented_name_and_unit`
//! holds the counters and the histogram to the same list.
//!
//! | phase | span attributes (a `set` total) | counters (an `add` total) |
//! |---|---|---|
//! | `backup` | `lepatch.backup.files_walked` `{file}`, `lepatch.backup.distinct_chunks_in_version` `{chunk}` | `lepatch.backup.chunks_produced` `{chunk}`, `lepatch.backup.bytes_read` `By` |
//! | `merge` | `lepatch.merge.chunks_in_plan` `{chunk}` | `lepatch.merge.distinct_chunks_stored` `{chunk}` |
//! | `verify` | `lepatch.verify.chunks_in_version` `{chunk}` | `lepatch.verify.chunks_checked` `{chunk}`, `lepatch.verify.bytes_read` `By` |
//! | `restore` | `lepatch.restore.files_in_version` `{file}` | `lepatch.restore.files_written` `{file}`, `lepatch.restore.bytes_written` `By` |
//! | `apply` | `lepatch.apply.files_in_pack` `{file}` | `lepatch.apply.files_written` `{file}`, `lepatch.apply.bytes_written` `By` |
//!
//! Two instruments are not a phase's total and so are not in that table:
//! `lepatch.command.duration` (a histogram in `s`, carrying `lepatch.command`)
//! and `lepatch.job.count` (`{job}`, carrying `lepatch.job.kind` and
//! `lepatch.job.state`). Both are recorded through their own entry points — see
//! [`record_command_duration`] and [`record_job`].
//!
//! **The attributes are recorded on whatever span is current when the run
//! finishes**, which is the innermost span the run is inside: a command run on
//! the command line reports onto that command's own span, and a merge the
//! server started as a job reports onto its `lepatch::command::merge` span
//! inside the job's `lepatch_job` span — the job span is entered first and so is
//! the parent, never the one the attributes land on.
//!
//! # Semantic conventions
//!
//! Where they apply, they are followed: units are UCUM (`By` for bytes, `s` for
//! seconds, `{chunk}` / `{file}` / `{job}` for counts), names are dotted
//! lower-case under the `lepatch.` namespace, and counters deliberately **omit**
//! a `.total` suffix — the specification asks backends to add that where they
//! want it, and an instrument that already has it gets `_total_total`.
//!
//! Where they do not apply, this is said rather than pretended: the semantic
//! conventions describe HTTP, RPC, databases and runtimes, and none of them
//! describes content-defined chunking. `lepatch.command.duration` is not
//! `http.server.request.duration` and does not pretend to be — there is no HTTP
//! server in a `lepatch backup`, and borrowing the name would make a dashboard
//! group a local disk backup under web traffic. Nothing here claims a
//! `*_duration` semantic either; the only convention-shaped decision is the unit.
//!
//! # What is deliberately not an instrument
//!
//! * **`merge`'s bytes.** `command/merge.rs` does not report
//!   `Counter::Written`, and its module explains why: the only byte total a merge
//!   has is content *before* compression. A byte counter there would be a content
//!   rate wearing the name of a disk rate.
//! * **`backup`'s commit busy time.** It is a `Duration` in
//!   `command/pipeline.rs`, folded into a log field. It is a fraction of one run,
//!   and `lepatch.command.duration` already carries the whole.
//! * **Per-chunk updates.** Nothing is recorded from `Progress::add` or
//!   `Progress::report`. A backup calls `add` once per chunk, several thousand
//!   times a second; the instrument is added **once per run**, from the final
//!   totals, by [`record_run`]. A counter that moved per chunk would report the
//!   same information at a cost the hot loop should not pay.
//! * **`probe`'s work.** It has no [`Progress`] of its own — the five commands
//!   that do counted work are `backup`, `merge`, `verify`, `restore` and
//!   `apply` — so the probe an apply takes internally reports nothing beyond
//!   what that apply already reports.
//! * **A job's duration.** `lepatch.job.count` counts terminal states and there
//!   is no `lepatch.job.duration`. The span says how long the work took; a second
//!   instrument carrying the same number would be two copies of one fact.
//!
//! # Attributes
//!
//! `lepatch.command.duration` carries `lepatch.command`, and `lepatch.job.count`
//! carries `lepatch.job.kind` and `lepatch.job.state`. Nothing else carries
//! attributes: every other instrument is already qualified by name, and a set
//! name or a path would be unbounded cardinality on a tool that is given paths
//! on its command line.

use std::{
    io,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::{
    metrics::{SdkMeterProvider, periodic_reader_with_async_runtime::PeriodicReader},
    runtime,
};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::progress::{Counter as RunCounter, Phase, Progress};

use super::Config;

// ---------------------------------------------------------------------------
// Names.
//
// A constant each, and not a string at the callsite: the span-attribute name and
// the instrument name for one counter are spelled the same way here precisely so
// that a reader can check one against the other.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Units.
//
// UCUM, and named once each. A count of chunks and a count of files are the same
// sort of thing and are written the same way, and a byte total and a duration
// are each one spelling — so a unit is a constant rather than a literal at ten
// instrument constructions, and the tables below are assembled out of these.
// ---------------------------------------------------------------------------

/// A count of chunks. The semantic conventions' own annotation for "a number of
/// items like this one".
const CHUNKS: &str = "{chunk}";

/// A count of files.
const FILES: &str = "{file}";

/// A count of jobs.
const JOBS: &str = "{job}";

/// A byte total. UCUM's `By`, which is what the semantic conventions use.
const BYTES: &str = "By";

/// A duration. UCUM's `s`, for seconds.
const SECONDS: &str = "s";

/// How long one command took.
const COMMAND_DURATION: &str = "lepatch.command.duration";

/// The command a duration belongs to.
const COMMAND_ATTRIBUTE: &str = "lepatch.command";

/// Jobs that reached a terminal state.
const JOB_COUNT: &str = "lepatch.job.count";

/// What kind of job a count belongs to.
const JOB_KIND_ATTRIBUTE: &str = "lepatch.job.kind";

/// The terminal state a job reached.
const JOB_STATE_ATTRIBUTE: &str = "lepatch.job.state";

/// Chunks a backup cut from the tree. `add`.
const BACKUP_CHUNKS_PRODUCED: &str = "lepatch.backup.chunks_produced";

/// Bytes a backup read out of the tree. `add`.
const BACKUP_BYTES_READ: &str = "lepatch.backup.bytes_read";

/// Files a backup's walk found. `set`, so an attribute.
const BACKUP_FILES_WALKED: &str = "lepatch.backup.files_walked";

/// Distinct chunks in the version a backup committed. `set`, so an attribute.
const BACKUP_DISTINCT_CHUNKS_IN_VERSION: &str = "lepatch.backup.distinct_chunks_in_version";

/// Chunks a merge newly stored. `add`, and **not** the same measurement as the
/// one above — see the module docs.
const MERGE_DISTINCT_CHUNKS_STORED: &str = "lepatch.merge.distinct_chunks_stored";

/// Chunks the merged version reads, stored or already held. `set`, so an
/// attribute.
const MERGE_CHUNKS_IN_PLAN: &str = "lepatch.merge.chunks_in_plan";

/// Chunks a verify checked against their hashes. `add`.
const VERIFY_CHUNKS_CHECKED: &str = "lepatch.verify.chunks_checked";

/// Bytes a verify read back out of the store. `add`.
const VERIFY_BYTES_READ: &str = "lepatch.verify.bytes_read";

/// Chunks in the version a verify was asked about. `set`, so an attribute.
const VERIFY_CHUNKS_IN_VERSION: &str = "lepatch.verify.chunks_in_version";

/// Files a restore wrote. `add`.
const RESTORE_FILES_WRITTEN: &str = "lepatch.restore.files_written";

/// Bytes a restore wrote to the destination. `add`.
const RESTORE_BYTES_WRITTEN: &str = "lepatch.restore.bytes_written";

/// Files in the version a restore was asked for. `set`, so an attribute.
const RESTORE_FILES_IN_VERSION: &str = "lepatch.restore.files_in_version";

/// Files an apply wrote. `add`.
const APPLY_FILES_WRITTEN: &str = "lepatch.apply.files_written";

/// Bytes an apply wrote to the destination. `add`.
const APPLY_BYTES_WRITTEN: &str = "lepatch.apply.bytes_written";

/// Files in the pack's file table. `set`, so an attribute.
const APPLY_FILES_IN_PACK: &str = "lepatch.apply.files_in_pack";

/// Every counter this module records, with the unit it reports.
///
/// **The list, not a slice of the structs, so it can be asserted against what
/// the reader collected.** A counter added to [`Instruments`] without appearing
/// here is a counter nothing checks, and an instrument whose name is never
/// written down anywhere else in the crate is a name nobody is maintaining.
///
/// The histogram is in `HISTOGRAMS`, because a table of counters that quietly
/// contained a histogram would be a lie about what a counter is.
///
/// Test-only, and built out of the constants above — so it checks the units as
/// well as the names, and reordering a row here cannot silently relabel an
/// instrument.
#[cfg(test)]
const COUNTERS: [(&str, &str); 10] = [
    (JOB_COUNT, JOBS),
    (BACKUP_CHUNKS_PRODUCED, CHUNKS),
    (BACKUP_BYTES_READ, BYTES),
    (MERGE_DISTINCT_CHUNKS_STORED, CHUNKS),
    (VERIFY_CHUNKS_CHECKED, CHUNKS),
    (VERIFY_BYTES_READ, BYTES),
    (RESTORE_FILES_WRITTEN, FILES),
    (RESTORE_BYTES_WRITTEN, BYTES),
    (APPLY_FILES_WRITTEN, FILES),
    (APPLY_BYTES_WRITTEN, BYTES),
];

/// The one histogram, with the unit it reports.
///
/// Test-only, for the same reason [`COUNTERS`] is.
#[cfg(test)]
const HISTOGRAMS: [(&str, &str); 1] = [(COMMAND_DURATION, SECONDS)];

// ---------------------------------------------------------------------------
// The instruments.
// ---------------------------------------------------------------------------

/// Every instrument this process writes through.
///
/// Held in one struct rather than looked up by name at each call, because a
/// `OnceLock` read is cheap and an instrument constructor is not.
#[derive(Debug, Clone)]
struct Instruments {
    command_duration: Histogram<f64>,
    job_count: Counter<u64>,
    backup_chunks_produced: Counter<u64>,
    backup_bytes_read: Counter<u64>,
    merge_distinct_chunks_stored: Counter<u64>,
    verify_chunks_checked: Counter<u64>,
    verify_bytes_read: Counter<u64>,
    restore_files_written: Counter<u64>,
    restore_bytes_written: Counter<u64>,
    apply_files_written: Counter<u64>,
    apply_bytes_written: Counter<u64>,
}

impl Instruments {
    /// Build every instrument on one meter.
    ///
    /// Takes the meter rather than reading the global one, so a test can hand
    /// it a meter over a recorder and see exactly what was written.
    fn build(meter: &Meter) -> Self {
        Self {
            command_duration: meter
                .f64_histogram(COMMAND_DURATION)
                .with_unit(SECONDS)
                .build(),
            job_count: counter(meter, JOB_COUNT, JOBS),
            backup_chunks_produced: counter(meter, BACKUP_CHUNKS_PRODUCED, CHUNKS),
            backup_bytes_read: counter(meter, BACKUP_BYTES_READ, BYTES),
            merge_distinct_chunks_stored: counter(meter, MERGE_DISTINCT_CHUNKS_STORED, CHUNKS),
            verify_chunks_checked: counter(meter, VERIFY_CHUNKS_CHECKED, CHUNKS),
            verify_bytes_read: counter(meter, VERIFY_BYTES_READ, BYTES),
            restore_files_written: counter(meter, RESTORE_FILES_WRITTEN, FILES),
            restore_bytes_written: counter(meter, RESTORE_BYTES_WRITTEN, BYTES),
            apply_files_written: counter(meter, APPLY_FILES_WRITTEN, FILES),
            apply_bytes_written: counter(meter, APPLY_BYTES_WRITTEN, BYTES),
        }
    }

    /// The counter one `(phase, counter)` pair records to, if it records at all.
    ///
    /// **`None` is the common case and it is not an oversight.** A `set` total
    /// is an attribute, not an instrument, and a `(phase, counter)` pair a
    /// command never reports has nothing to record. Returning `None` for both is
    /// what keeps the mapping from growing a fall-through arm that guesses.
    fn instrument(&self, phase: Phase, counter: RunCounter) -> Option<&Counter<u64>> {
        let instrument = match (phase, counter) {
            (Phase::Backup, RunCounter::Chunks) => &self.backup_chunks_produced,
            (Phase::Backup, RunCounter::Read) => &self.backup_bytes_read,
            (Phase::Merge, RunCounter::Distinct) => &self.merge_distinct_chunks_stored,
            (Phase::Verify, RunCounter::Checked) => &self.verify_chunks_checked,
            (Phase::Verify, RunCounter::Read) => &self.verify_bytes_read,
            (Phase::Restore, RunCounter::Done) => &self.restore_files_written,
            (Phase::Restore, RunCounter::Written) => &self.restore_bytes_written,
            (Phase::Apply, RunCounter::Done) => &self.apply_files_written,
            (Phase::Apply, RunCounter::Written) => &self.apply_bytes_written,
            _ => return None,
        };
        Some(instrument)
    }

    /// Add a run's accumulating totals to their instruments.
    ///
    /// **Once per run, from [`Progress::totals`], never from `Progress::add`.**
    fn record_run(&self, phase: Phase, totals: &[(RunCounter, u64)]) {
        for (counter, total) in totals {
            if let Some(instrument) = self.instrument(phase, *counter) {
                instrument.add(*total, &[]);
            }
        }
    }
}

/// One counter, with its unit.
fn counter(meter: &Meter, name: &'static str, unit: &'static str) -> Counter<u64> {
    meter.u64_counter(name).with_unit(unit).build()
}

// ---------------------------------------------------------------------------
// Installation.
// ---------------------------------------------------------------------------

/// The instruments, built once and only when telemetry was asked for.
///
/// `None` forever on a default run, which is what
/// `nothing_is_built_and_nothing_is_recorded_when_telemetry_is_off` asserts.
static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

/// The meter provider [`super::shutdown`] flushes and stops.
///
/// Process-global like the tracer provider beside it, and kept for the same
/// reason: the SDK has no way to ask a global for its handle back. Installed and
/// taken out together with the tracer's, so a process that asked for telemetry
/// has both and a process that did not has neither.
static METER_PROVIDER: Mutex<Option<SdkMeterProvider>> = Mutex::new(None);

/// Build the instruments and start the reader, if telemetry is on.
///
/// Called once, from `init_tracing`, beside `super::layer` and under the same
/// [`Config`]. Returns having constructed **nothing at all** when telemetry is
/// off.
///
/// * **the OTLP metric exporter and a [`PeriodicReader`] over it.** The reader
///   spawns the task that collects and exports, and it is the reason the
///   interval is left at the SDK's default: a `lepatch backup` is over in under
///   a minute and its one export is the [`super::shutdown`] flush, exactly as
///   its spans are. **"Left at the default" is not "fixed"**: the reader reads
///   `OTEL_METRIC_EXPORT_INTERVAL`, so an operator exporting from a long-running
///   `serve` can have the numbers leave on a schedule instead of at exit.
///
///   The reader is the **runtime-aware** one, and that is not a detail. The
///   default reader collects on its own `std` thread and drives an asynchronous
///   OTLP client with `futures_executor::block_on`; that client is a `reqwest`
///   client underneath and wants a tokio reactor, which a thread the SDK made
///   does not have. The export then panics — "there is no reactor running" — and
///   every metric is lost with an exit code of 0 and nothing else said. This is
///   the metrics half of the argument `Cargo.toml` makes for the span
///   processor's feature, and the reason is the same.
/// * **the export timeout is `telemetry::EXPORT_TIMEOUT`**, not the reader's own
///   thirty seconds, for the same reason the span processor's is: this runs on
///   the exit path, with the command's result already decided, and a collector
///   that accepts the connection and then says nothing must not hold a backup
///   open behind it.
/// * **the meter provider's handle**, kept so `shutdown` can reach it — the
///   same reasoning as [`super::layer`], and losing it loses every metric
///   silently.
///
/// # Errors
///
/// Returns an error if the exporter cannot be built — a malformed endpoint, or
/// an HTTP client that could not be created — and an error if instruments have
/// already been installed, which [`install`] is called once per process by
/// `init_tracing` and a library caller could get wrong. Both are reported the
/// way `layer`'s is: a warning, and the command carries on.
pub fn install(config: &Config) -> io::Result<()> {
    if !config.enabled() {
        return Ok(());
    }

    let exporter = MetricExporter::builder()
        .with_http()
        .build()
        .map_err(|e| io::Error::other(format!("could not build the OTLP metrics exporter: {e}")))?;

    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_timeout(super::EXPORT_TIMEOUT)
        .build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(super::resource())
        .build();

    // Set before the instruments are built, so they are meters over the provider
    // that will actually export them rather than over a no-op global.
    opentelemetry::global::set_meter_provider(provider.clone());
    if let Ok(mut slot) = METER_PROVIDER.lock() {
        *slot = Some(provider);
    }

    INSTRUMENTS
        .set(Instruments::build(&opentelemetry::global::meter(
            super::crate_name(),
        )))
        .map_err(|_| io::Error::other("telemetry metrics were already installed"))?;

    Ok(())
}

/// Whether any instrument was ever built.
///
/// The answer to "is telemetry on?" *for metrics specifically*, and
/// [`super::is_installed`]'s counterpart. A provider that failed to build
/// leaves neither, which is why both are offered rather than one.
#[must_use]
pub fn installed() -> bool {
    INSTRUMENTS.get().is_some()
}

/// Flush the metrics and stop the reader, or say why it could not.
///
/// Called by [`super::shutdown`] on every way out of the process, for the same
/// reason that function exists: a metrics pipeline is batched, and a run that
/// exits without a flush discards everything it measured with no error and a
/// successful exit code.
///
/// # Errors
///
/// Returns an error if the SDK could not flush or could not stop. **Reported
/// and never changes the exit code**, exactly as in [`super::shutdown`].
pub(super) fn shutdown() -> io::Result<()> {
    let Some(provider) = METER_PROVIDER.lock().ok().and_then(|mut slot| slot.take()) else {
        return Ok(());
    };

    let flushed = provider
        .force_flush()
        .map_err(|e| super::otel_error("flush metrics", &e));
    let stopped = provider
        .shutdown()
        .map_err(|e| super::otel_error("stop metrics", &e));

    flushed.and(stopped)
}

// ---------------------------------------------------------------------------
// Recording.
// ---------------------------------------------------------------------------

/// Report a finished phase: its totals as span attributes, its accumulating
/// totals as counters.
///
/// Called once per run, from the command that owns the [`Progress`], after the
/// last thing the run counted. **It does nothing when telemetry is off** — the
/// span attribute goes through `tracing`, which drops it on a subscriber with
/// no OpenTelemetry layer, and the counter lookup is one `OnceLock` read that
/// answers `None`.
///
/// The span is [`tracing::Span::current`] — the instrumented command the run is
/// inside — and *not* the progress event's own target, so these attributes land
/// beside the span's own arguments rather than in a log line somebody has to
/// correlate by timestamp.
pub fn record_run(progress: &Progress) {
    let phase = progress.phase();
    let totals = progress.totals();

    let span = tracing::Span::current();
    for (counter, total) in &totals {
        if let Some(name) = span_attribute(phase, *counter) {
            // `Value` has no `u64`, and a total above `i64::MAX` is a counter
            // that has wrapped rather than a number to publish. Saturating, and
            // the same choice `Progress::add` makes.
            span.set_attribute(name, i64::try_from(*total).unwrap_or(i64::MAX));
        }
    }

    if let Some(instruments) = INSTRUMENTS.get() {
        instruments.record_run(phase, &totals);
    }
}

/// The span attribute one `(phase, counter)` pair reports as, if it does.
///
/// The `set` half of the mapping in the module docs. A pair that is not listed
/// has no attribute, and that is either an `add` total (a counter) or a pair no
/// command reports.
fn span_attribute(phase: Phase, counter: RunCounter) -> Option<&'static str> {
    let name = match (phase, counter) {
        (Phase::Backup, RunCounter::Files) => BACKUP_FILES_WALKED,
        (Phase::Backup, RunCounter::Distinct) => BACKUP_DISTINCT_CHUNKS_IN_VERSION,
        (Phase::Merge, RunCounter::Chunks) => MERGE_CHUNKS_IN_PLAN,
        (Phase::Verify, RunCounter::Chunks) => VERIFY_CHUNKS_IN_VERSION,
        (Phase::Restore, RunCounter::Files) => RESTORE_FILES_IN_VERSION,
        (Phase::Apply, RunCounter::Files) => APPLY_FILES_IN_PACK,
        _ => return None,
    };
    Some(name)
}

/// Record how long one command took.
///
/// Called by `main` once the command has returned, **on both the success and
/// the failure arm**, so a failed backup is on the histogram too. A command
/// that was interrupted is not: `run` abandons the dispatch future, and there
/// is no code after the `select!` that sees which command it was.
///
/// # Errors
///
/// None. A duration that cannot be exported is a telemetry problem and is
/// reported on the exit path, not here.
pub fn record_command_duration(command: &'static str, elapsed: Duration) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };

    instruments.command_duration.record(
        elapsed.as_secs_f64(),
        &[KeyValue::new(COMMAND_ATTRIBUTE, command)],
    );
}

/// Record a job that reached a terminal state.
///
/// Called from `Jobs`, at the two places a terminal state is *written* — so a
/// job is counted exactly once, and only if it earned the state. A cancel
/// against a job that had already finished records nothing, because the write
/// it is beside did not happen.
///
/// Only terminal states are counted. `queued` and `running` are not outcomes,
/// and a counter that moved on entry and again on completion would be a count
/// of transitions rather than of jobs.
///
/// # Errors
///
/// None. See [`record_command_duration`].
pub fn record_job(kind: &'static str, state: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };

    instruments.job_count.add(
        1,
        &[
            KeyValue::new(JOB_KIND_ATTRIBUTE, kind),
            KeyValue::new(JOB_STATE_ATTRIBUTE, state),
        ],
    );
}
#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        future::Future,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use opentelemetry::{KeyValue, Value, metrics::MeterProvider as _, trace::TracerProvider as _};
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        metrics::{
            PeriodicReader, SdkMeterProvider, Temporality,
            data::{AggregatedMetrics, MetricData, ResourceMetrics},
            exporter::PushMetricExporter,
        },
        trace::{SdkTracerProvider, SpanData, SpanExporter},
    };
    use tracing_subscriber::layer::{Layer as _, SubscriberExt as _};

    use super::{
        APPLY_BYTES_WRITTEN, APPLY_FILES_WRITTEN, BACKUP_BYTES_READ, BACKUP_CHUNKS_PRODUCED,
        BACKUP_DISTINCT_CHUNKS_IN_VERSION, BACKUP_FILES_WALKED, COMMAND_ATTRIBUTE,
        COMMAND_DURATION, COUNTERS, HISTOGRAMS, JOB_COUNT, JOB_KIND_ATTRIBUTE, JOB_STATE_ATTRIBUTE,
        MERGE_CHUNKS_IN_PLAN, MERGE_DISTINCT_CHUNKS_STORED, RESTORE_BYTES_WRITTEN,
        RESTORE_FILES_IN_VERSION, RESTORE_FILES_WRITTEN, VERIFY_BYTES_READ, VERIFY_CHUNKS_CHECKED,
        VERIFY_CHUNKS_IN_VERSION,
    };
    use crate::progress::{Counter as RunCounter, Phase, Progress};

    /// One collected measurement, flattened out of the SDK's data types.
    ///
    /// **Owned, because [`ResourceMetrics`] is neither `Clone` nor readable
    /// field-by-field.** So the exporter copies out the three things an assertion
    /// can be made about — the name, the unit, and the value with its attributes
    /// — at the moment it is handed the batch, and the SDK's own types never
    /// reach a test.
    #[derive(Debug, Clone, PartialEq)]
    struct Collected {
        name: String,
        unit: String,
        /// A counter's value, kept exact.
        ///
        /// **A `u64` and not a widened `f64`.** Every counter in this crate is
        /// a whole number of things, and rounding one to compare it would make
        /// the assertion weaker than the thing it checks.
        total: Option<u64>,
        /// A histogram's sum over the points collected so far.
        seconds: Option<f64>,
        attributes: BTreeMap<String, String>,
    }

    impl Collected {
        /// This point's counter value.
        fn total(&self) -> u64 {
            self.total
                .unwrap_or_else(|| panic!("{} is not a counter", self.name))
        }

        /// The value of this point's named attribute.
        fn attribute(&self, key: &str) -> Option<&str> {
            self.attributes.get(key).map(String::as_str)
        }
    }

    /// A metric exporter that keeps every point it is handed, in this process.
    ///
    /// **No socket and no collector, for the same reason the span recorder in
    /// `telemetry`'s own tests is a `Vec`.** What is under test is the pipeline —
    /// an instrument is built, a measurement is made, and the reader collects it
    /// — and a collector would only add a network client to that claim.
    #[derive(Clone, Debug, Default)]
    struct Recorder(Arc<Mutex<Vec<Collected>>>);

    impl Recorder {
        /// Every point collected so far, in a stable order.
        ///
        /// **Sorted, and the sort is the point.** The SDK hands back data points
        /// in whatever order its aggregation arrived in, so an assertion written
        /// against position would be testing the reader rather than the thing
        /// under test.
        fn points(&self) -> Vec<Collected> {
            let mut points = self.0.lock().expect("the recorder's lock").clone();
            points.sort_by(|a, b| (&a.name, &a.attributes).cmp(&(&b.name, &b.attributes)));
            points
        }

        /// Every collected point for one instrument name.
        fn named(&self, name: &str) -> Vec<Collected> {
            self.points()
                .into_iter()
                .filter(|point| point.name == name)
                .collect()
        }
    }

    impl PushMetricExporter for Recorder {
        fn export(&self, metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
            let mut collected = Vec::new();
            for scope in metrics.scope_metrics() {
                for metric in scope.metrics() {
                    match metric.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                collected.push(Collected {
                                    name: metric.name().to_string(),
                                    unit: metric.unit().to_string(),
                                    total: Some(point.value()),
                                    seconds: None,
                                    attributes: attributes(point.attributes()),
                                });
                            }
                        }
                        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                            for point in histogram.data_points() {
                                collected.push(Collected {
                                    name: metric.name().to_string(),
                                    unit: metric.unit().to_string(),
                                    total: None,
                                    seconds: Some(point.sum()),
                                    attributes: attributes(point.attributes()),
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }

            self.0
                .lock()
                .expect("the recorder's lock")
                .extend(collected);
            std::future::ready(Ok(()))
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    /// An attribute list as owned strings, sorted so two runs can be compared.
    fn attributes<'a>(points: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
        points
            .map(|pair| {
                (
                    pair.key.as_str().to_string(),
                    pair.value.as_str().to_string(),
                )
            })
            .collect()
    }

    /// A reader over a recorder, plus the instruments built on it.
    ///
    /// The meter provider is **local, never the global one**, so two tests
    /// running on parallel threads in one process cannot see each other's
    /// measurements. `force_flush` drives the collection synchronously through
    /// the reader's own thread, so a point is there when it returns.
    fn reader() -> (SdkMeterProvider, Recorder) {
        let recorder = Recorder::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(recorder.clone()).build())
            .build();
        (provider, recorder)
    }

    /// A progress report with every counter given a distinct value.
    ///
    /// The values are the counter's own number, so an assertion can say *which*
    /// measurement reached the reader and not only that something did.
    fn progress_with_every_counter(phase: Phase) -> Progress {
        let mut progress = Progress::new(phase);
        for (index, counter) in [
            RunCounter::Files,
            RunCounter::Chunks,
            RunCounter::Distinct,
            RunCounter::Checked,
            RunCounter::Done,
            RunCounter::Read,
            RunCounter::Written,
        ]
        .into_iter()
        .enumerate()
        {
            progress.add(counter, 100 + index as u64);
        }
        progress
    }

    /// Add a phase's totals to a recorder-backed meter and collect them.
    fn collect(phase: Phase) -> Vec<Collected> {
        let (provider, recorder) = reader();
        let instruments = super::Instruments::build(&provider.meter("lepatch"));
        instruments.record_run(phase, &progress_with_every_counter(phase).totals());
        provider.force_flush().expect("a flush of one run");
        recorder.points()
    }

    /// The instruments exist, with the names and units this module documents.
    ///
    /// **Both lists are compared exactly, in both directions.** A count of
    /// instruments would pass with one renamed; an equality check fails if an
    /// instrument is renamed *or* added without its name being written down.
    #[test]
    fn every_instrument_exists_with_the_documented_name_and_unit() {
        let mut collected: BTreeMap<String, String> = BTreeMap::new();
        for phase in [
            Phase::Backup,
            Phase::Merge,
            Phase::Verify,
            Phase::Restore,
            Phase::Apply,
        ] {
            for point in collect(phase) {
                collected.insert(point.name, point.unit);
            }
        }
        // The job counter and the duration histogram are recorded through their
        // own entry points rather than through a phase, so they are added here
        // rather than above.
        let (provider, recorder) = reader();
        let instruments = super::Instruments::build(&provider.meter("lepatch"));
        instruments
            .command_duration
            .record(2.5, &[KeyValue::new(COMMAND_ATTRIBUTE, "backup")]);
        instruments.job_count.add(
            1,
            &[
                KeyValue::new(JOB_KIND_ATTRIBUTE, "merge"),
                KeyValue::new(JOB_STATE_ATTRIBUTE, "succeeded"),
            ],
        );
        provider.force_flush().expect("a flush");
        for point in recorder.points() {
            collected.insert(point.name, point.unit);
        }

        let expected: BTreeMap<String, String> = COUNTERS
            .iter()
            .chain(HISTOGRAMS.iter())
            .map(|(name, unit)| ((*name).to_string(), (*unit).to_string()))
            .collect();

        assert_eq!(
            collected, expected,
            "the instruments the reader saw, and the ones this module names, must be the same set"
        );
    }

    /// A command's counters reach the reader, as the numbers it counted.
    ///
    /// **Values, not just names.** A pipeline that built the right instruments
    /// and recorded nothing into them would pass a test about names.
    #[test]
    fn a_command_records_values_that_reach_the_reader() {
        let points = collect(Phase::Restore);

        // `Files` was set to 100, `Done` and `Written` to 104 and 106.
        let written = recorder_of(&points, RESTORE_FILES_WRITTEN);
        assert_eq!(
            written
                .iter()
                .map(|point| point.total())
                .collect::<Vec<_>>(),
            vec![104],
            "files a restore wrote must reach the reader as that count"
        );
        assert_eq!(written[0].unit, "{file}");

        let bytes = recorder_of(&points, RESTORE_BYTES_WRITTEN);
        assert_eq!(
            bytes[0].total(),
            106,
            "bytes a restore wrote must reach the reader as that count"
        );
        assert_eq!(bytes[0].unit, "By");
    }

    /// The one point of every counter in the collected list.
    fn recorder_of<'a>(points: &'a [Collected], name: &str) -> Vec<&'a Collected> {
        let matching: Vec<&Collected> = points.iter().filter(|point| point.name == name).collect();
        assert!(
            !matching.is_empty(),
            "{name} must have been recorded at all: {:?}",
            points.iter().map(|p| p.name.as_str()).collect::<Vec<_>>()
        );
        matching
    }

    /// The two readings of `Counter::Distinct` are not confusable.
    ///
    /// **This is the assertion the whole mapping exists for.** A backup's
    /// `distinct` is the total distinct chunks in the version it committed; a
    /// merge's is the chunks that run stored. The two are named apart, and they
    /// are not even the same *kind* of instrument — one is a span attribute and
    /// one is a counter — so a reader cannot reach for the wrong one.
    ///
    /// The names are checked, and then the recordings are made and read back, so
    /// the assertion cannot pass on two constants that agree while the code
    /// behind them sends both numbers to the same place.
    #[test]
    fn the_two_readings_of_distinct_are_distinguishable_by_name() {
        assert_ne!(
            BACKUP_DISTINCT_CHUNKS_IN_VERSION, MERGE_DISTINCT_CHUNKS_STORED,
            "the same name for both readings would be a metric that lies in one of them"
        );
        for name in [
            BACKUP_DISTINCT_CHUNKS_IN_VERSION,
            MERGE_DISTINCT_CHUNKS_STORED,
        ] {
            assert!(
                name.starts_with("lepatch."),
                "an instrument belongs to this crate's namespace: {name}"
            );
            assert!(
                name.contains('.') && name.split('.').count() >= 3,
                "an instrument name carries its phase as a dotted segment: {name}"
            );
        }

        // A backup's distinct is not a counter at all: it reaches the span.
        let backup = collect(Phase::Backup);
        assert!(
            backup
                .iter()
                .all(|point| point.name != BACKUP_DISTINCT_CHUNKS_IN_VERSION),
            "a per-version total must not be a sum: summing two versions' distinct \
             counts is not the set's distinct count"
        );

        // A merge's distinct is, and it carries the merge's own number.
        let merge = collect(Phase::Merge);
        // `Distinct` was set to 102.
        assert_eq!(
            recorder_of(&merge, MERGE_DISTINCT_CHUNKS_STORED)[0].total(),
            102,
            "a merge's distinct must be the chunks that run stored"
        );
    }

    /// Every name in both tables carries its phase, and no name is unqualified.
    ///
    /// The rule is the lane's: a reader must be able to tell which command a
    /// number came from without reading the source, so the phase is a dotted
    /// segment of the name and not a field on the instrument.
    #[test]
    fn every_instrument_name_carries_its_phase() {
        for (name, unit) in COUNTERS.iter().chain(HISTOGRAMS.iter()) {
            assert!(
                name.starts_with("lepatch."),
                "every instrument is under this crate's namespace: {name}"
            );
            assert!(
                !unit.is_empty(),
                "every instrument reports a unit: {name} has none"
            );
        }

        // The phase-named ones, spelled out — the two table walks above say that
        // a segment exists, not that it is the right one.
        for name in [
            BACKUP_CHUNKS_PRODUCED,
            BACKUP_BYTES_READ,
            MERGE_DISTINCT_CHUNKS_STORED,
            VERIFY_CHUNKS_CHECKED,
            VERIFY_BYTES_READ,
            RESTORE_FILES_WRITTEN,
            RESTORE_BYTES_WRITTEN,
            APPLY_FILES_WRITTEN,
            APPLY_BYTES_WRITTEN,
        ] {
            let phase = name.strip_prefix("lepatch.").expect("namespaced");
            assert!(
                ["backup", "merge", "verify", "restore", "apply"]
                    .contains(&phase.split('.').next().unwrap_or_default()),
                "the first segment must name the phase that records it: {name}"
            );
        }
    }

    /// No number is reported in both places.
    ///
    /// The mapping splits the counters: an `add` total is a counter and a `set`
    /// total is a span attribute. A pair that was in both tables would be two
    /// copies of one fact that could drift apart, with nothing to say which to
    /// believe — so the two tables are compared against each other.
    #[test]
    fn no_counter_is_also_a_span_attribute() {
        // The global meter, deliberately: nothing here records through these
        // instruments, it only asks which pairs they answer for. A provider with
        // no reader would do the same and cost a thread this does not need.
        let instruments = super::Instruments::build(&opentelemetry::global::meter("lepatch"));

        for phase in [
            Phase::Backup,
            Phase::Merge,
            Phase::Verify,
            Phase::Restore,
            Phase::Apply,
        ] {
            for counter in [
                RunCounter::Files,
                RunCounter::Chunks,
                RunCounter::Distinct,
                RunCounter::Checked,
                RunCounter::Done,
                RunCounter::Read,
                RunCounter::Written,
            ] {
                assert!(
                    !(instruments.instrument(phase, counter).is_some()
                        && super::span_attribute(phase, counter).is_some()),
                    "{phase:?} {counter:?} is in both tables; one number is reported once"
                );
            }
        }
    }

    /// The on-wire counter names are not renamed to suit an instrument.
    ///
    /// `progress::counter_names` is a contract with `lepatch serve`, and an
    /// instrument name that reads better is not a reason to change it. The
    /// instrument names are longer and say which phase they belong to; these are
    /// the seven fields a client is told to expect, and they are unchanged.
    #[test]
    fn the_wire_names_are_the_ones_the_contract_promises() {
        assert_eq!(
            crate::progress::counter_names(),
            [
                "files", "chunks", "distinct", "checked", "done", "read", "written"
            ],
        );

        // And no instrument borrows one of them bare.
        for (name, _) in COUNTERS.iter().chain(HISTOGRAMS.iter()) {
            let leaf = name.rsplit('.').next().unwrap_or_default();
            assert!(
                !crate::progress::counter_names().contains(&leaf),
                "{name} would collide with the wire name `{leaf}`"
            );
        }
    }

    /// A duration reaches the reader, labelled with the command it measured.
    #[test]
    fn a_command_duration_is_recorded_per_command() {
        let (provider, recorder) = reader();
        let instruments = super::Instruments::build(&provider.meter("lepatch"));

        for command in ["backup", "merge"] {
            instruments.command_duration.record(
                Duration::from_millis(1500).as_secs_f64(),
                &[KeyValue::new(COMMAND_ATTRIBUTE, command)],
            );
        }
        provider.force_flush().expect("a flush");

        let points = recorder.named(COMMAND_DURATION);
        assert_eq!(points.len(), 2, "one measurement per command: {points:?}");
        assert_eq!(points[0].unit, "s", "a duration is in seconds");
        let measured = points[0]
            .seconds
            .unwrap_or_else(|| panic!("a histogram point must carry a sum: {points:?}"));
        assert!(
            (measured - 1.5).abs() < 1e-9,
            "a second and a half must be recorded as one and a half seconds, \
             not as milliseconds or nanoseconds: {measured}"
        );
        let commands: Vec<Option<&str>> = points
            .iter()
            .map(|point| point.attribute(COMMAND_ATTRIBUTE))
            .collect();
        assert_eq!(commands, [Some("backup"), Some("merge")]);
    }

    /// A job is counted once, by kind and by the state it reached.
    #[test]
    fn a_job_is_counted_by_kind_and_terminal_state() {
        let (provider, recorder) = reader();
        let instruments = super::Instruments::build(&provider.meter("lepatch"));

        for (kind, state) in [
            ("merge", "succeeded"),
            ("merge", "failed"),
            ("merge", "cancelled"),
            ("pack", "succeeded"),
        ] {
            instruments.job_count.add(
                1,
                &[
                    KeyValue::new(JOB_KIND_ATTRIBUTE, kind),
                    KeyValue::new(JOB_STATE_ATTRIBUTE, state),
                ],
            );
        }
        provider.force_flush().expect("a flush");

        let points = recorder.named(JOB_COUNT);
        assert_eq!(points.len(), 4, "one point per kind and state: {points:?}");
        assert_eq!(points[0].unit, "{job}");
        assert!(
            points.iter().all(|point| point.total() == 1),
            "a job is one job: {points:?}"
        );

        let series: Vec<(Option<&str>, Option<&str>)> = points
            .iter()
            .map(|point| {
                (
                    point.attribute(JOB_KIND_ATTRIBUTE),
                    point.attribute(JOB_STATE_ATTRIBUTE),
                )
            })
            .collect();
        assert_eq!(
            series,
            [
                (Some("merge"), Some("cancelled")),
                (Some("merge"), Some("failed")),
                (Some("merge"), Some("succeeded")),
                (Some("pack"), Some("succeeded")),
            ],
            "every terminal state must be its own time series"
        );
    }

    // -----------------------------------------------------------------------
    // Span attributes.
    // -----------------------------------------------------------------------

    /// A span exporter that keeps every span it is handed.
    #[derive(Clone, Debug, Default)]
    struct Spans(Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for Spans {
        fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
            self.0.lock().expect("the spans' lock").extend(batch);
            std::future::ready(Ok(()))
        }
    }

    impl Spans {
        /// The attributes of the one span named `name`.
        fn attributes_of(&self, name: &str) -> Vec<(String, Value)> {
            let spans = self.0.lock().expect("the spans' lock");
            let span = spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| {
                    panic!(
                        "no span named {name}; got {:?}",
                        spans.iter().map(|s| s.name.as_ref()).collect::<Vec<_>>()
                    )
                });
            span.attributes
                .iter()
                .map(|pair| (pair.key.to_string(), pair.value.clone()))
                .collect()
        }
    }

    /// Run `body` inside an instrumented span, and return that span's attributes.
    fn attributes_on_span(body: impl FnOnce()) -> Vec<(String, Value)> {
        let recorder = Spans::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(recorder.clone())
            .build();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("lepatch"))
            .with_filter(super::super::filter());

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            let span = tracing::info_span!(target: "lepatch::test", "a_backup");
            let _entered = span.enter();
            body();
        });
        provider.force_flush().expect("a flush");

        recorder.attributes_of("a_backup")
    }

    /// A finished phase's per-run totals land on the span as attributes.
    ///
    /// **And the accumulating ones do not.** `chunks` is an `add` total and so is
    /// a counter; putting it on the span as well would be the double-emit this
    /// crate's mapping exists to avoid.
    #[test]
    fn a_finished_phase_reports_its_totals_as_span_attributes() {
        let mut progress = Progress::new(Phase::Backup);
        progress.set(RunCounter::Files, 42);
        progress.set(RunCounter::Distinct, 4242);
        progress.add(RunCounter::Chunks, 7);
        progress.add(RunCounter::Read, 9999);

        let attributes = attributes_on_span(|| super::record_run(&progress));

        let find = |name: &str| {
            attributes
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        };

        assert_eq!(
            find(BACKUP_FILES_WALKED),
            Some(Value::I64(42)),
            "files the walk found is a per-run fact, and belongs on the span"
        );
        assert_eq!(
            find(BACKUP_DISTINCT_CHUNKS_IN_VERSION),
            Some(Value::I64(4242)),
            "and so is the version's distinct-chunk total"
        );
        assert_eq!(
            find(BACKUP_CHUNKS_PRODUCED),
            None,
            "an accumulating total is a counter; putting it here too would be two \
             copies of one number"
        );
        assert_eq!(find(BACKUP_BYTES_READ), None);
    }

    /// Every phase's `set` totals have an attribute name, and every `add` total
    /// has an instrument name — for the pairs the commands actually report.
    ///
    /// Spelled out rather than walked off the tables, because the tables are the
    /// thing being checked and a table that checks itself proves nothing.
    #[test]
    fn the_mapping_covers_every_pair_a_command_reports() {
        let instruments = super::Instruments::build(&opentelemetry::global::meter("lepatch"));

        for (phase, counter, attribute) in [
            (Phase::Backup, RunCounter::Files, Some(BACKUP_FILES_WALKED)),
            (
                Phase::Backup,
                RunCounter::Distinct,
                Some(BACKUP_DISTINCT_CHUNKS_IN_VERSION),
            ),
            (Phase::Backup, RunCounter::Chunks, None),
            (Phase::Backup, RunCounter::Read, None),
            (Phase::Merge, RunCounter::Chunks, Some(MERGE_CHUNKS_IN_PLAN)),
            (Phase::Merge, RunCounter::Distinct, None),
            (
                Phase::Verify,
                RunCounter::Chunks,
                Some(VERIFY_CHUNKS_IN_VERSION),
            ),
            (Phase::Verify, RunCounter::Checked, None),
            (Phase::Verify, RunCounter::Read, None),
            (
                Phase::Restore,
                RunCounter::Files,
                Some(RESTORE_FILES_IN_VERSION),
            ),
            (Phase::Restore, RunCounter::Done, None),
            (Phase::Restore, RunCounter::Written, None),
            (
                Phase::Apply,
                RunCounter::Files,
                Some("lepatch.apply.files_in_pack"),
            ),
            (Phase::Apply, RunCounter::Done, None),
            (Phase::Apply, RunCounter::Written, None),
        ] {
            assert_eq!(
                super::span_attribute(phase, counter),
                attribute,
                "{phase:?} {counter:?}"
            );
            assert_eq!(
                instruments.instrument(phase, counter).is_some(),
                attribute.is_none(),
                "{phase:?} {counter:?} must be in exactly one of the two tables"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Inertia.
    // -----------------------------------------------------------------------

    /// With telemetry off, nothing is built, no reader is started, and nothing
    /// is recorded.
    ///
    /// **The whole default-off promise, for metrics.** [`super::install`] returns
    /// before it constructs an exporter, so there is no reader thread, no socket
    /// and no instrument to look for — and the recording entry points are
    /// answered with one `OnceLock` read that says `None`.
    #[test]
    fn nothing_is_built_and_nothing_is_recorded_when_telemetry_is_off() {
        super::install(&super::super::Config::new(false, None))
            .expect("telemetry being off cannot fail");

        assert!(
            !super::installed(),
            "no instrument may be built when the operator asked for nothing"
        );

        // Every entry point, called the way a default run calls them.
        let mut progress = Progress::new(Phase::Backup);
        progress.set(RunCounter::Files, 1);
        progress.add(RunCounter::Chunks, 1);
        super::record_run(&progress);
        super::record_job("merge", "succeeded");
        super::record_command_duration("backup", Duration::from_secs(1));

        assert!(
            !super::installed(),
            "recording must not build an instrument on the way past"
        );
        assert!(
            super::super::shutdown().is_ok(),
            "there is nothing to flush, and that is not a failure"
        );
    }
}
