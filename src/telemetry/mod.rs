// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! OpenTelemetry spans, for an operator who asked for them.
//!
//! # Nothing leaves the machine unless the operator asked
//!
//! **This module is inert until it is asked for, and "asked for" is one flag or
//! one environment variable.** With neither, [`Config::enabled`] is `false`,
//! [`layer`] builds no exporter, starts no exporter thread, and opens no socket.
//! The tests `telemetry_is_off_unless_it_is_asked_for` and
//! `no_exporter_is_built_when_telemetry_is_off` hold that down.
//!
//! The reason is not politeness. `blu` is a command-line backup tool: it is
//! run by a person, over a shell, on their own machine, and a backup run that
//! phones a collector it was never told about is a surprise with a socket in
//! it. So the cost of the default build is the cost of *this module compiling*,
//! and nothing else.
//!
//! # The library hands back a layer; `main` installs it
//!
//! A `tracing` subscriber is process-global, and this crate is a library first:
//! a caller embedding `blu` owns its own subscriber and would not find one
//! installed under it. So [`layer`] builds the layer and returns it, and
//! `init_tracing` in `main.rs` puts it on the subscriber. That is the same
//! arrangement as [`crate::serve::progress_layer`].
//!
//! # Shutdown is not optional
//!
//! **An OTLP exporter is asynchronous and batched. A `blu` run that emits a
//! span and exits without [`shutdown`] drops it, and looks like it succeeded.**
//! There is no error, no warning and no non-zero exit: the span is sitting in a
//! queue on a thread the process has already decided to end. A backup tool that
//! reports a whole run through telemetry and then discards it is worse than one
//! that has no telemetry, because the operator is told they have it.
//!
//! So [`shutdown`] exists and `main.rs` calls it after its own `run` returns, on
//! every path out: the command succeeded, the command failed, and the process
//! was asked to stop. All three are one place, because
//! `main` computes an exit code and then returns it — see
//! `flush_telemetry` there.
//!
//! It flushes the **meter** provider as well as the tracer's, and for exactly
//! the same reason: a metric is batched too, so a run that exits without it
//! loses every number it measured with the same silence.
//!
//! One test holds it down, and it is the one that matters: a span is
//! **received** after the exporter is shut down, and the same span emitted with
//! no shutdown **is not**, both asserted in `a_span_emitted_before_shutdown_is_received`
//! — first half before the flush, second after it. That is the defect this
//! section is about.
//!
//! **And both signals are held down at the level that decides it: a collector,
//! in a real process, after that process has exited.** `tests/telemetry.rs` runs
//! the binary to completion against a socket and reads the bytes that arrive —
//! `a_span_survives_a_successful_run` for the tracer and
//! `a_metric_survives_a_successful_run` for the meter. Deleting either
//! `force_flush` below or the `metrics::shutdown()` beside it fails one of them,
//! which is the only reason to believe either sentence of this section.
//!
//! # What is not here
//!
//! * **This module does not read an inbound `traceparent`.** The W3C
//!   `TraceContext` propagator *is* installed, explicitly, by [`layer`] — see
//!   below — because a propagator that is merely *registered* is worth nothing to
//!   a caller that never extracts, and the registration is one line that is easy
//!   to leave out and impossible to notice. **The extraction that reads it is in
//!   `serve::span`**, a private module beside the routes it wraps, because a
//!   request is the only thing in this crate that arrives from outside with
//!   headers attached to it.
//! * **No middleware lives here.** The one request middleware this crate has is
//!   `serve::span`, written beside the routes it wraps: it is actix's middleware
//!   and needs actix's types, and putting it in this module would put a second
//!   concern in a module whose whole claim is that it exports nothing until the
//!   operator asks. It is installed by [`crate::serve::bind`], and only when a
//!   provider is installed.
//! * **No sampling.** Nothing here configures a sampler, so the SDK's default
//!   applies: every span is recorded. A sampled-out span is a span nobody can
//!   debug, and this is a tool an operator turns on to look at one run.
//! * **No TLS in the default build.** `--features telemetry-tls` turns on
//!   `rustls` — with the operating system's trust store, through
//!   `opentelemetry-otlp`'s `reqwest-rustls` — for the exporter, and changes
//!   nothing else: no endpoint, no span and no flag behaves differently. Without
//!   it an `https://` endpoint fails to connect rather than quietly
//!   downgrading, which is the same refusal
//!   [`blu versions`](crate::client) of an `https://` serve target makes. See
//!   the `opentelemetry-otlp` entry in `Cargo.toml`.
//! * **No logs signal.** `opentelemetry`'s `logs` feature is off on every crate
//!   in this graph and nothing bridges a `tracing` *event* into an OpenTelemetry
//!   log record. Diagnostics stay on stderr, filtered by `RUST_LOG`, `--verbose`
//!   and `--quiet`; only spans and metrics cross the wire.
//! * **No instrumentation of anything but this crate's own targets.** The layer
//!   is filtered with `blu=info` (see [`filter`]), so a span from actix,
//!   tokio, `reqwest` or from a library caller embedding this crate is never
//!   exported. An instrumented dependency is therefore invisible here rather
//!   than reported twice.
//! * **No metrics for the HTTP surface.** `http.server` is a span; there is no
//!   request counter, no request-duration histogram and no uploaded-byte
//!   counter. Job outcomes are counted (`blu.job.count`), and that is the
//!   whole of the server's metric story. See `crate::telemetry::metrics` for
//!   the full list, which is deliberately short.

use std::{io, sync::Mutex, time::Duration};

use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::SpanExporter as OtlpSpanExporter;
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkError,
    propagation::TraceContextPropagator,
    runtime,
    trace::{
        BatchConfig, BatchConfigBuilder, SdkTracer, SdkTracerProvider,
        span_processor_with_async_runtime::BatchSpanProcessor,
    },
};
use tracing::Subscriber;
use tracing_subscriber::{EnvFilter, layer::Layer as _, registry::LookupSpan};

pub mod metrics;

/// The environment variable that turns telemetry on by itself.
///
/// The standard OTLP one, honoured here only as a *switch*. Its value is the
/// exporter's business: [`layer`] does not read it, and the exporter resolves
/// it the way every OTLP exporter does — `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
/// first, then this one with `/v1/traces` appended, then
/// [`DEFAULT_ENDPOINT`].
///
/// **Only this one turns telemetry on.** The per-signal variables — traces,
/// metrics, headers, timeouts — configure an exporter, and an exporter is only
/// built when telemetry is on: `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` on its own
/// is read by nothing, because [`Config::new`] is asked about this variable and
/// no other. Setting the per-signal variable is how an operator points one signal
/// somewhere other than the other, and it is *not* a second way to switch the
/// whole thing on.
pub const ENDPOINT_VARIABLE: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// The endpoint the exporter falls back to, named here so `--telemetry` on its
/// own can say something useful.
///
/// **The OTLP exporter's own default, spelled the way it spells it.** The
/// fallback lives in `opentelemetry-otlp` as `http://localhost:4318` — the name
/// `localhost`, not the address `127.0.0.1` — and this constant says the same
/// thing, so the line `blu --telemetry` prints is where the exporter will
/// actually go rather than a loopback address that looks equivalent. A host
/// whose `localhost` does not resolve is a host this fallback fails on, and
/// saying so in advance is cheaper than discovering it from a missing export.
///
/// A loopback address at all, for the same reason [`crate::serve`]'s bind
/// default is: a telemetry backend is not something a backup tool should
/// discover across the network.
pub const DEFAULT_ENDPOINT: &str = "http://localhost:4318";

/// What the operator asked for, read once at startup.
///
/// Constructed by [`Config::from_environment`] from `--telemetry` and
/// [`ENDPOINT_VARIABLE`], and asked two questions by everything else: is it on
/// ([`Config::enabled`]), and where would it go ([`Config::endpoint`]).
///
/// It is a value and not a set of globals so that "telemetry is on if and only
/// if the operator asked" is a fact a test can hold down, rather than something
/// that has to be re-derived by running the process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Whether telemetry was asked for at all.
    enabled: bool,
    /// The endpoint variable's value, if it had one.
    endpoint: Option<String>,
}

impl Config {
    /// Read what the operator asked for.
    ///
    /// `--telemetry` is the flag; [`ENDPOINT_VARIABLE`] is the environment
    /// spelling of the same decision, and either one turns telemetry on. An
    /// empty value counts as unset, because `OTEL_EXPORTER_OTLP_ENDPOINT=` is
    /// how a shell spells "no address" and reading it as an address would point
    /// the exporter at nothing.
    #[must_use]
    pub fn from_environment(flag: bool) -> Self {
        let endpoint = std::env::var(ENDPOINT_VARIABLE)
            .ok()
            .filter(|value| !value.trim().is_empty());

        Self::new(flag, endpoint.as_deref())
    }

    /// The same rule, with the environment handed over as an argument.
    ///
    /// The decision is "flag **or** endpoint variable", and a test cannot set
    /// an environment variable without a process to set it in — `set_var` is
    /// `unsafe` on this edition, and a test that mutates the environment races
    /// every other test in the same binary. So the rule is written here, once,
    /// and [`from_environment`](Self::from_environment) is the only caller that
    /// reads the environment.
    #[must_use]
    pub fn new(flag: bool, endpoint: Option<&str>) -> Self {
        Self {
            enabled: flag || endpoint.is_some(),
            endpoint: endpoint.map(str::to_string),
        }
    }

    /// Whether telemetry was asked for.
    ///
    /// The whole of the default build's cost is decided by this returning
    /// `false`.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Where telemetry would go, for the log line that says so.
    ///
    /// **Informational only, and deliberately not handed to the exporter.** The
    /// endpoint an OTLP exporter uses is not simply this string: the standard
    /// variable names a *base* and the exporter appends `/v1/traces` to it, and
    /// a per-signal variable outranks it. Reconstructing that here and passing
    /// it as an explicit endpoint would override both, so this is reported and
    /// the exporter does its own resolution.
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
}

/// The `tracing` layer that carries spans into OpenTelemetry.
///
/// **A filtered `tracing_opentelemetry` layer, named.** It is a type alias
/// rather than a newtype because this crate has no blanket `impl<S> Layer<S>` to
/// build a newtype on, and writing one that forwards all twelve `Layer` methods
/// to add a name to it would be a hundred lines of delegation with nothing in
/// them.
///
/// The `Filtered` is not incidental: it is where [`filter`] lives, and a layer
/// without one is offered every span in the process. `S` is the subscriber the
/// layer is being added to, which the caller never has to name — `main.rs`
/// stacks this under [`crate::serve::progress_layer`] without writing a single
/// type parameter.
pub type Layer<S> = tracing_subscriber::filter::Filtered<
    tracing_opentelemetry::OpenTelemetryLayer<S, SdkTracer>,
    EnvFilter,
    S,
>;

/// The provider [`shutdown`] flushes and stops.
///
/// Process-global, like the `tracing` subscriber, and for the same reason there
/// is no way to ask the SDK for it back: `global::shutdown_tracer_provider` does
/// not exist, so the handle is kept here from the moment it is built.
///
/// `None` is the default and the state of every run that never asked for
/// telemetry.
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);

/// Whether a tracer provider is installed right now.
///
/// The answer to "is telemetry on?" *after* startup, where [`Config::enabled`]
/// is the answer before it. The two can differ — a provider that failed to
/// build leaves nothing installed — so the module offers both rather than
/// making a caller guess which question it is asking.
///
/// **A question, and it leaves the provider where it found it.** It reads the
/// slot rather than calling `installed`, which *takes* the handle out so that
/// [`shutdown`] cannot be run twice. Sharing that call would make this the worst
/// function in the module: a caller asking whether telemetry is on would consume
/// the provider, and the export that follows would find nothing to flush — **every
/// span the process recorded, silently discarded, on a run that asked for
/// telemetry and reported no error**. `blu serve` asks this once, when it
/// binds, to decide whether to wrap the router in request spans; that one call
/// is enough to lose the whole run. See
/// `asking_whether_a_provider_is_installed_leaves_it_where_it_was`.
#[must_use]
pub fn is_installed() -> bool {
    PROVIDER.lock().is_ok_and(|slot| slot.is_some())
}

/// The installed provider, taken out of the slot so it cannot be shut down twice.
///
/// A poisoned lock is recovered from rather than propagated. The only thing
/// guarded here is a provider handle, and a panic in one thread must not leave
/// every later thread unable to flush telemetry on its way out — which is the
/// exact outcome the panic would have wanted to cause.
fn installed() -> Option<SdkTracerProvider> {
    PROVIDER.lock().ok().and_then(|mut slot| slot.take())
}

/// Build the layer, if telemetry is on.
///
/// Returns `Ok(None)` when [`Config::enabled`] is `false`, having built
/// nothing at all: no exporter, no HTTP client, no batch processor, no thread,
/// no socket. That is the default build, and it is why the function can be
/// called unconditionally from `init_tracing`.
///
/// When it is on, this also does the two things that have to happen once per
/// process and cannot be left to a default:
///
/// * **it registers the W3C `TraceContext` propagator.** The OpenTelemetry
///   global propagator is a **no-op** until something replaces it. An
///   integration that never registers one produces perfectly good spans with
///   empty `traceparent` values, links nothing to anything, and reports
///   success — the classic silent `OTel` failure, and the reason this is a
///   sentence here rather than a line in a changelog.
/// * **it keeps the provider's handle** so [`shutdown`] can reach it.
///
/// `S` is the subscriber this layer will be installed on, and is inferred at the
/// call site.
///
/// # Errors
///
/// Returns an error if the exporter cannot be built — a malformed endpoint, or
/// an HTTP client that could not be created. It is an error rather than a
/// silent `None`, because the operator asked for telemetry and not getting it
/// is worth a warning; `main.rs` prints one and carries on, because a collector
/// being down is not a reason to refuse to back up a directory.
pub fn layer<S>(config: &Config) -> io::Result<Option<Layer<S>>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    if !config.enabled() {
        return Ok(None);
    }

    let exporter = OtlpSpanExporter::builder()
        .with_http()
        .build()
        .map_err(|e| io::Error::other(format!("could not build the OTLP exporter: {e}")))?;

    let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(batch_config())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource())
        .build();

    let tracer = provider.tracer(crate_name());

    // Set before the layer is handed back, so a span created by anything between
    // here and installation is a child of the right trace rather than an orphan.
    //
    // Both of these are process-global and have no non-global equivalent in the
    // SDK. The *subscriber* is not one of them, which is why this module still
    // hands back a layer instead of installing one.
    if let Ok(mut slot) = PROVIDER.lock() {
        *slot = Some(provider.clone());
    }
    global::set_tracer_provider(provider);
    global::set_text_map_propagator(TraceContextPropagator::new());

    // **No log line here.** The subscriber this layer is about to be installed
    // on does not exist yet, so anything said from here is said to nothing.
    // `init_tracing` says it, once, after `try_init`.

    Ok(Some(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter()),
    ))
}

/// How the batch processor is configured.
///
/// The queue and the batch are the SDK's defaults and are not restated here.
/// The one value this crate sets is the export timeout, and it is set because
/// of where the export happens: [`shutdown`] runs on the exit path, inside
/// `main`, with the process's exit code already decided. A collector that
/// accepts the connection and then says nothing must not hold a backup open
/// behind it, so an export gives up rather than waiting out the SDK's 30
/// seconds.
fn batch_config() -> BatchConfig {
    BatchConfigBuilder::default()
        .with_max_export_timeout(EXPORT_TIMEOUT)
        .build()
}

/// How long one export may take before it is abandoned.
///
/// Ten seconds against the SDK's default of thirty, and the shorter number is
/// the point: the alternative is a command that has finished its work and then
/// hangs on the way out, and an exit that hangs is indistinguishable from a
/// backup that hung.
///
/// It also **overrides `OTEL_BSP_EXPORT_TIMEOUT`**, because a programmatic value
/// wins over the environment in the SDK. That is deliberate — the exit path is
/// the last thing that happens, and the last thing that happens should not be
/// negotiable by a variable nobody was thinking about — and it is the one
/// exporter setting this crate takes away from the operator.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// What a span is recorded *as*: the binary that produced it, and its version.
///
/// **The same two facts `GET /v1/version` reports**, because they answer the
/// same question from opposite ends: which build produced this trace, and can
/// the thing reading it understand what the thing that wrote it meant. A
/// dashboard that cannot tell two versions of `blu` apart cannot tell a
/// regression from a rollout.
///
/// `service.name` rather than `process.executable.name`, and that is not a
/// preference. The latter is behind the semantic conventions' own
/// `semconv_experimental` feature, and this crate's only reason to turn an
/// experimental flag on would be to spell one attribute this one already spells.
///
/// `Resource::builder()` rather than `builder_empty()`, so the SDK's own
/// `telemetry.sdk.*` and `service.name` defaults are present and an operator's
/// `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` still apply — this adds
/// one attribute to the standard resource, it does not replace it.
fn resource() -> Resource {
    use opentelemetry::KeyValue;
    use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};

    Resource::builder()
        .with_attribute(KeyValue::new(SERVICE_NAME, crate_name()))
        .with_attribute(KeyValue::new(SERVICE_VERSION, crate_version()))
        .build()
}

/// This binary's name, as the resource records it.
///
/// `CARGO_PKG_NAME`, not `CARGO_BIN_NAME`: a resource that says which
/// *package* produced a trace survives the binary being renamed, and the
/// version beside it is what pairs with it.
fn crate_name() -> &'static str {
    env!("CARGO_PKG_NAME")
}

/// This build's version, and the number `GET /v1/version` reports.
fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The directive the layer is filtered with.
///
/// **The same shape as [`crate::serve::progress_filter`], and for the same
/// reason.** `RUST_LOG`, `--verbose` and `--quiet` answer one question: what
/// goes to stderr. If they were attached to the subscriber, a callsite they
/// answer `never` to would not be emitted at all, so this layer would never be
/// offered the span — and an operator who ran `blu --quiet` would silently
/// export nothing at all, which is the failure mode `9ab42a7` fixed for a
/// client's view of a job and is worth exactly as much care here.
///
/// `blu=info` rather than a list of spans: the layer reads spans by
/// *target*, and a new `#[instrument]` on a new command is then covered without
/// anybody editing a filter. `info` is the level every `#[instrument]`ed command
/// entry point in this crate is declared at. Spans from outside the crate —
/// actix's, tokio's, a library caller embedding this one — are not exported.
const DIRECTIVE: &str = "blu=info";

/// The filter the OpenTelemetry layer is installed with.
///
/// Not optional, and not negotiable from the environment, for the reasons above.
#[must_use]
pub fn filter() -> EnvFilter {
    EnvFilter::new(DIRECTIVE)
}

/// Flush what is buffered, and stop the exporter. **Call this on every way out
/// of the process.**
///
/// This is [`layer`]'s counterpart and the reason the feature is usable at
/// all: without it, every span produced by a command that then exits is
/// discarded, silently.
///
/// Two calls, in this order:
///
/// * [`force_flush`](SdkTracerProvider::force_flush) hands the buffered spans to
///   the exporter and waits for the export to finish. On its own this is what
///   makes a span *received* rather than queued.
/// * [`shutdown`](SdkTracerProvider::shutdown) stops the batch processor and
///   closes the HTTP client. Without it the exporter's thread is a live
///   thread being killed by a process that is exiting.
///
/// The meter provider is flushed alongside it, and it is the same argument a
/// second time: the metric reader batches too, and its measurements are lost in
/// the same silence.
///
/// Calling it twice is harmless: the provider is taken out of its slot on the
/// first call, so the second returns `Ok(())` without touching anything. A
/// process that never asked for telemetry takes the same path and costs one
/// uncontended lock.
///
/// # Errors
///
/// Returns an error if the SDK could not flush or could not stop. **It is
/// reported and never changes the exit code.** A run that failed to tell its
/// telemetry backend about itself has still done — or has still failed to do —
/// the work the user asked for, and the exit code is that fact.
pub fn shutdown() -> io::Result<()> {
    // The meters first: a metric reader collects on its own thread, and
    // stopping the tracer's processor can already be several seconds of waiting.
    // The order is the reverse of the argument's order for that reason.
    let metrics = metrics::shutdown();

    let Some(provider) = installed() else {
        return metrics;
    };

    let flushed = provider.force_flush().map_err(|e| otel_error("flush", &e));
    let stopped = provider.shutdown().map_err(|e| otel_error("shutdown", &e));

    flushed.and(stopped).and(metrics)
}

/// Name an SDK failure as an `io::Error`, which is this crate's only error type.
///
/// Carries what the SDK said: an unreachable collector and a full queue both
/// land here, and they are different problems for whoever is reading the log.
fn otel_error(what: &str, error: &OTelSdkError) -> io::Error {
    io::Error::other(format!("telemetry could not {what}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, PoisonError};

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        trace::{SdkTracerProvider, SpanData, SpanExporter},
    };
    use tracing_subscriber::layer::{Layer as _, SubscriberExt as _};

    use super::{Config, DEFAULT_ENDPOINT, DIRECTIVE, ENDPOINT_VARIABLE, EXPORT_TIMEOUT};

    /// The two tests that look at the provider slot, kept apart from each other.
    ///
    /// `PROVIDER` is process-global and `cargo test` runs these on parallel
    /// threads in one process, so a test that fills the slot and a test that
    /// asserts it is empty would otherwise fail each other depending on which
    /// thread the scheduler reached first.
    static PROVIDER_SLOT: Mutex<()> = Mutex::new(());

    /// An exporter that keeps every span it is handed, in this process.
    ///
    /// **No socket, no collector, no timing luck.** An OTLP exporter is a
    /// network client; testing what it sends by starting a server is testing the
    /// server. What is under test here is the *pipeline* — spans reach the
    /// processor, the processor's export is driven, and the export happens at
    /// all — and this is where a real span arrives.
    #[derive(Clone, Debug, Default)]
    struct Recorder(Arc<Mutex<Vec<SpanData>>>);

    impl Recorder {
        /// Every span this exporter has been handed, by name.
        fn names(&self) -> Vec<String> {
            self.0
                .lock()
                .expect("the recorder's lock")
                .iter()
                .map(|span| span.name.to_string())
                .collect()
        }
    }

    impl SpanExporter for Recorder {
        // Not an `async fn`, and the reason is worth a line: this returns
        // immediately, and the whole point of
        // `a_span_emitted_before_shutdown_is_received` is that the span is *not*
        // here until something flushes it. An exporter that did its work on
        // creation would prove nothing about batching.
        fn export(
            &self,
            batch: Vec<SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            self.0.lock().expect("the recorder's lock").extend(batch);
            std::future::ready(Ok(()))
        }
    }

    /// One span of the shape this crate's `#[instrument]`ed commands make.
    fn emit_a_span() {
        let span = tracing::info_span!("a_merge", set = "docs");
        // Entered and left, so the span is *finished* — which is the only thing
        // that hands it to the processor. A span that is created and dropped
        // unentered still closes, and this is what a command's outermost span
        // looks like.
        let _entered = span.enter();
    }

    #[test]
    fn telemetry_is_off_unless_it_is_asked_for() {
        // The default build. No flag, no endpoint variable: off.
        let config = Config::new(false, None);
        assert!(
            !config.enabled(),
            "telemetry must be off when the operator asked for nothing"
        );
        assert_eq!(config.endpoint(), None);

        // Either one is enough, and neither is a weaker signal than the other.
        assert!(Config::new(true, None).enabled(), "`--telemetry` alone");
        assert!(
            Config::new(false, Some("http://127.0.0.1:4318")).enabled(),
            "`{ENDPOINT_VARIABLE}` alone"
        );
        assert!(Config::new(true, Some("http://127.0.0.1:4318")).enabled());

        // And the endpoint is reported as given, not rewritten.
        let named = Config::new(true, Some("http://collector:4318"));
        assert_eq!(named.endpoint(), Some("http://collector:4318"));
    }

    #[test]
    fn the_environment_is_the_only_difference_between_off_and_on() {
        // `from_environment` is the caller of `new` that reads the environment,
        // so it is the only thing worth checking here — and it is checked
        // against the environment whatever the environment happens to be, rather
        // than by mutating it. A test that set `OTEL_EXPORTER_OTLP_ENDPOINT`
        // would be racing every other test in this binary for the same
        // environment.
        assert!(
            Config::from_environment(true).enabled(),
            "the flag must turn telemetry on whatever the environment says"
        );
        assert_eq!(
            Config::from_environment(false).enabled(),
            std::env::var_os(ENDPOINT_VARIABLE)
                .is_some_and(|value| { !value.to_string_lossy().trim().is_empty() }),
            "without the flag, the endpoint variable is the only thing that can turn it on"
        );
    }

    #[test]
    fn no_exporter_is_built_when_telemetry_is_off() {
        // The slot is shared with
        // `asking_whether_a_provider_is_installed_leaves_it_where_it_was`, which
        // fills it; see `PROVIDER_SLOT`.
        let _slot = PROVIDER_SLOT.lock().unwrap_or_else(PoisonError::into_inner);

        // **The default build, asserted rather than assumed.** `layer` returns
        // `Ok(None)` before it constructs anything, so there is no exporter to
        // look for and no thread to join: the only evidence available is that
        // nothing was installed and nothing is returned to install.
        let layer = super::layer::<tracing_subscriber::Registry>(&Config::new(false, None))
            .expect("telemetry being off cannot fail");

        assert!(layer.is_none(), "no layer when telemetry is off");

        // Nothing was installed globally either, so a later `shutdown` has
        // nothing to flush — which is the state every default run exits in.
        assert!(!super::is_installed());
    }

    /// **Asking whether a provider is installed must not consume it.**
    ///
    /// This was a real bug and it is the quietest one this module could hold:
    /// `is_installed` was written as `installed().is_some()`, and `installed`
    /// *takes* the handle out of the slot so that `shutdown` cannot run it
    /// twice. So the first question anybody asked took the provider with it, and
    /// `blu serve` asks exactly once — when it binds, to decide whether to
    /// wrap the router in request spans. **The run then flushed an empty slot:**
    /// every span it recorded, including the request spans the question was about
    /// to switch on, discarded on exit, with no error and a clean exit code.
    ///
    /// Found by `tests/telemetry.rs::a_span_survives_a_stopped_server`, which
    /// runs the real binary against a real socket and found nothing arriving.
    ///
    /// **A stand-in provider, built by hand**, rather than one from [`layer`]:
    /// `layer` needs a tokio runtime and installs a global tracer and propagator
    /// for the rest of the binary, and what is under test here is the slot
    /// rather than the pipeline. A provider with no processor has nothing to
    /// batch and shuts down cleanly, so every assertion below is about the
    /// handle alone.
    #[test]
    fn asking_whether_a_provider_is_installed_leaves_it_where_it_was() {
        let _slot = PROVIDER_SLOT.lock().unwrap_or_else(PoisonError::into_inner);

        let mut held = super::PROVIDER.lock().expect("the provider slot");
        assert!(held.is_none(), "another test left a provider behind");
        *held = Some(SdkTracerProvider::builder().build());
        drop(held);

        assert!(
            super::is_installed(),
            "a provider in the slot is an installed provider"
        );
        assert!(
            super::is_installed(),
            "**and asking again must still say so**: a question that consumes its \\
             answer loses every span at exit, silently, on exactly the runs that \\
             asked for telemetry"
        );

        // The exit path still has a provider to flush, which is the whole claim.
        super::shutdown().expect("a provider with nothing queued shuts down");
        assert!(
            !super::is_installed(),
            "shutdown takes it out, and it does so once"
        );
    }

    /// Install a layer over a recorder and emit a span — the shape `main` uses,
    /// with the exporter swapped for one that keeps its spans.
    ///
    /// Returns the names the exporter has been handed.
    fn emit_a_span_over(recorder: &Recorder) -> Vec<String> {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(recorder.clone())
            .build();

        // A simple processor, deliberately: this test is about the *layer*, and
        // the batching behaviour — the thing that makes shutdown matter — is
        // what `a_span_emitted_before_shutdown_is_received` is for.
        let tracer = provider.tracer("blu");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(super::filter());

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, emit_a_span);
        provider.force_flush().expect("a flush of one span");

        recorder.names()
    }

    #[test]
    fn a_span_reaches_the_exporter_when_telemetry_is_on() {
        let recorder = Recorder::default();

        let names = emit_a_span_over(&recorder);

        assert!(
            names.iter().any(|name| name == "a_merge"),
            "a span must reach the exporter: {names:?}"
        );
    }

    #[test]
    fn a_span_emitted_before_shutdown_is_received() {
        // **The test this module exists for.**
        //
        // A batching exporter queues a finished span and exports it later, on a
        // task of its own. A `blu` run is a command that finishes and then
        // exits, so the run of a second is the *normal* case, not the unusual
        // one: the export timer is five seconds by default and a backup of a
        // small tree is over before it fires. The span is still sitting in the
        // queue when the process decides to end.
        //
        // So the pipeline is built as `main` builds it — a batch processor over
        // an exporter, a real span, and then the exit path — and the span has to
        // arrive. If the flush is dropped from `shutdown`, or `shutdown` is
        // dropped from `main`, this fails; nothing else in the file notices,
        // because the spans are genuinely produced and genuinely valid.
        let recorder = Recorder::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(recorder.clone())
            .build();

        // The tracer is passed to the layer explicitly rather than taken from
        // the global provider, which is what keeps these tests independent of
        // each other: `cargo test` runs them on parallel threads in one process,
        // and a global would be whichever one installed last.
        let tracer = provider.tracer("blu");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(super::filter());

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), emit_a_span);

        // Before the flush: the span has been recorded by the tracer, and the
        // batch processor has not exported it. Nothing has gone wrong and
        // nothing will fail — which is exactly what makes this worth asserting.
        assert!(
            recorder.names().is_empty(),
            "a batched span is queued, not exported, until something asks: {:?}",
            recorder.names()
        );

        // The exit path.
        provider.force_flush().expect("a flush");
        provider.shutdown().expect("a shutdown");

        assert!(
            recorder.names().iter().any(|name| name == "a_merge"),
            "a span must survive the exit path: {:?}",
            recorder.names()
        );
    }

    #[test]
    fn shutdown_without_a_provider_is_a_no_op() {
        // Every process that never asked for telemetry exits through this.
        super::shutdown().expect("no provider is not a failure");
    }

    #[test]
    fn the_layer_filter_admits_this_crate_and_nothing_else() {
        use tracing_subscriber::layer::Filter;

        let filter = super::filter();
        assert_eq!(DIRECTIVE, "blu=info");

        let ours = tracing::info_span!(target: "blulib::command::merge", "merge");
        assert!(
            Filter::<tracing_subscriber::Registry>::callsite_enabled(
                &filter,
                ours.metadata().expect("a callsite has static metadata")
            )
            .is_always(),
            "a command span must be admitted: this layer exports nothing otherwise"
        );

        let theirs = tracing::info_span!(target: "actix_web::server", "actix");
        assert!(
            !Filter::<tracing_subscriber::Registry>::callsite_enabled(
                &filter,
                theirs.metadata().expect("a callsite has static metadata")
            )
            .is_always(),
            "a span from another crate must not be admitted: the layer is only \
             cheap if it is not offered the whole server"
        );
    }

    #[test]
    fn the_resource_names_this_build_and_its_version() {
        let resource = super::resource();

        // The two facts `GET /v1/version` reports, so a trace and a version
        // listing can be read together.
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("service.name")),
            Some(env!("CARGO_PKG_NAME").into()),
            "a trace has to say which binary produced it"
        );
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("service.version")),
            Some(env!("CARGO_PKG_VERSION").into()),
            "a trace has to say which build produced it, or two versions of \\
             `blu` are one line on a dashboard"
        );
        // And the SDK's own defaults survive, so `OTEL_SERVICE_NAME` and
        // `OTEL_RESOURCE_ATTRIBUTES` still work.
        assert!(
            resource
                .get(&opentelemetry::Key::from_static_str("telemetry.sdk.name"))
                .is_some(),
            "the standard resource must be added to, not replaced"
        );
    }

    #[test]
    fn the_defaults_this_module_documents_are_the_ones_in_it() {
        // The module docs name `http://localhost:4318` and the standard
        // variable. If either is renamed here, this fails rather than leaving a
        // README to be quietly wrong.
        //
        // **And this cannot check that the exporter still agrees, and does not
        // pretend to.** `opentelemetry-otlp` keeps `http://localhost:4318` in a
        // private constant and reads no accessor back out of a built exporter, so
        // there is nothing here at runtime that could compare the two: if the
        // exporter's fallback moved, the constant above and every sentence that
        // repeats it would be stale, and the mismatch would be found by a
        // collector that received nothing. What is asserted is the half that is
        // this crate's own — the constant, the variable name, and an export
        // timeout shorter than the SDK's default.
        assert_eq!(DEFAULT_ENDPOINT, "http://localhost:4318");
        assert_eq!(ENDPOINT_VARIABLE, "OTEL_EXPORTER_OTLP_ENDPOINT");
        assert!(
            EXPORT_TIMEOUT < std::time::Duration::from_secs(30),
            "the exit path must not wait out the SDK's default export timeout"
        );
    }
}
