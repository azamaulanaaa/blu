//! The tracing subscriber and the telemetry exporter.

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::{Layer as _, SubscriberExt as _},
    util::SubscriberInitExt as _,
};

/// Flush the telemetry exporter, and stop it, on the way out of the process.
///
/// **A failure here is reported and does not change the exit code.** A run that
/// could not tell its collector about itself has still done — or has still
/// failed to do — the work the user asked for, and the exit code is that fact.
/// Losing telemetry is worth a line on stderr and nothing more; a backup that
/// reports failure because a collector was down would be worse than the bug
/// this prevents.
///
/// Safe to call when telemetry was never turned on: the module holds no
/// provider in that case and this is one uncontended lock.
pub(crate) fn flush_telemetry() {
    if let Err(e) = blulib::telemetry::shutdown() {
        tracing::warn!(error = %e, "telemetry could not be flushed at exit");
    }
}

/// Build the subscriber, with the job-progress layer installed when serving and
/// the OpenTelemetry layer installed when telemetry was asked for.
///
/// **The OpenTelemetry layer is built here and installed here, and the library
/// builds it without installing it** (`blulib::telemetry::layer`), because the
/// subscriber is process-global and this binary owns the only one there is.
///
/// **When telemetry is off there is no layer at all**, not a layer that does
/// nothing: `blulib::telemetry::layer` returns `Ok(None)` before it constructs
/// an exporter, so the default build has no exporter, no exporter thread and no
/// socket. That is the whole of the cost of a build that carries the feature.
///
/// **A collector that cannot be reached is a warning, not a failure.** The
/// exporter is built at startup and never asked to deliver anything, so a
/// failure here is a malformed endpoint or an unusable client — a
/// configuration mistake worth saying out loud — and the right answer is to say
/// it and carry on. Refusing to back up a directory because telemetry is
/// misconfigured would make this feature into a way to lose backups.
///
/// The layer is stacked **underneath** the writer so that its subscriber type
/// is the registry: each layer above it can then be added without a single type
/// parameter being written out, which is what lets the two arms below differ
/// only in whether the progress layer is present.
///
/// **The progress layer is only installed for `serve`.** It reads
/// `blulib::progress` events out of the span a job runs in and forwards the
/// counters into the job registry, which only `serve` has. Installing it
/// elsewhere would cost every event a hash lookup for a registry nothing reads.
///
/// # What the filter is attached to, and why that is the whole fix
///
/// **`--verbose`, `--quiet` and `RUST_LOG` govern the terminal. They are attached
/// to the `fmt` layer, and to nothing else.**
///
/// They used to be attached to the subscriber, which is a global filter: a
/// callsite it answers `never` to is not emitted at all, so a decision made for
/// stderr was also a decision made for everything stacked above it. A
/// `blulib::progress` event is an `INFO` event, so `blu --quiet serve`
/// answered `never` to it, the event was never emitted, the progress layer above
/// was never offered it, and a client polling `GET /v1/jobs/{id}` was told the
/// merge had reported nothing. Adding `RUST_LOG=info` restored it, which is what
/// made this look like a filter bug rather than an architectural one.
///
/// A filter attached to the writer layer answers only the writer's question. The
/// progress layer carries its own — [`blulib::serve::progress_filter`] — so a
/// client's view of a running job does not move when the operator turns the
/// terminal down. The two are asked the same question and are allowed to answer
/// it differently, which is the point.
///
/// The other commands are unaffected because for them there is no second layer:
/// the `None` arm below installs one filtered layer over the registry, and a
/// filter on the only layer is indistinguishable from a filter on the subscriber.
/// `tests/cli.rs::quiet_suppresses_progress_and_verbose_adds_detail` is what
/// holds that still true.
pub(crate) fn init_tracing(
    verbose: bool,
    quiet: bool,
    jobs: Option<&blulib::serve::Jobs>,
    telemetry: &blulib::telemetry::Config,
) {
    let default = if verbose {
        LevelFilter::TRACE
    } else if quiet {
        LevelFilter::WARN
    } else {
        LevelFilter::INFO
    };

    let filter = EnvFilter::builder()
        .with_default_directive(default.into())
        .from_env_lossy();

    // Data on stdout, diagnostics on stderr. So every command is pipeable
    // without `--quiet`.
    let writer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(filter);

    let otel = match blulib::telemetry::layer(telemetry) {
        Ok(layer) => layer,
        Err(e) => {
            // `eprintln!` and not `tracing::warn!`: the subscriber does not exist
            // yet, so a tracing event would be said to nothing.
            eprintln!("warning: {e}");
            None
        }
    };
    let telemetry_is_on = otel.is_some();

    // Beside the layer above, under the same config, and built only if it was
    // asked for. Nothing here is constructed on a run that did not ask: the
    // function returns before the exporter, the reader or any instrument exists.
    //
    // **A failure is a warning and not a refusal**, exactly as it is for the span
    // exporter. A collector that cannot take metrics has not made the backup
    // wrong, and refusing to take the backup because of it would make this
    // feature into a way to lose backups.
    if let Err(e) = blulib::telemetry::metrics::install(telemetry) {
        eprintln!("warning: {e}");
    }

    match jobs {
        None => {
            // No second layer, so this is the plain case and it is spelled the
            // plain way.
            let _ = tracing_subscriber::registry()
                .with(otel)
                .with(writer)
                .try_init();
        }
        Some(jobs) => {
            // Each layer is asked separately, and each answers for itself. The
            // `fmt` layer's filter says what the terminal prints; the progress
            // layer's says which callsites carry a client's counters, and
            // refuses the rest of the process. The OpenTelemetry layer's filter
            // is a third such filter, and `RUST_LOG` does not reach it either.
            let layer = blulib::serve::progress_layer(jobs.clone())
                .with_filter(blulib::serve::progress_filter());

            let _ = tracing_subscriber::registry()
                .with(otel)
                .with(writer)
                .with(layer)
                .try_init();
        }
    }

    // Said after the subscriber is installed, because said before it is said to
    // nothing.
    if telemetry_is_on {
        tracing::info!(
            endpoint = telemetry
                .endpoint()
                .unwrap_or(blulib::telemetry::DEFAULT_ENDPOINT),
            "telemetry is on"
        );
    }
}
