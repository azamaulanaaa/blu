//! The `lepatch` command line interface.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use lepatch::{
    command::{
        ApplyOptions, BackupPlan, MergePlan, Merged, Options, PARTIAL_SUFFIX, PREVIOUS_SUFFIX,
        Planned, RestorePlan, apply, backup, backup_plan, load_snapshot, lock_destination, merge,
        merge_plan, restore, restore_plan, verify, version_holding,
    },
    layout::IndexSet,
    pack::Pack,
    reader, storage,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::{Layer as _, SubscriberExt as _},
    util::SubscriberInitExt as _,
};

#[derive(Debug, Clone, Parser)]
#[command(name = "lepatch", version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// Log everything, including per-chunk tracing.
    #[arg(long, short)]
    verbose: bool,

    /// Only report warnings and errors, on the terminal.
    ///
    /// "On the terminal" is the whole of the scope. A client watching a job over
    /// HTTP still sees its progress, because `--quiet` and `RUST_LOG` are asked
    /// what goes to stderr and nothing is asked them about the wire.
    #[arg(long, short, conflicts_with = "verbose")]
    quiet: bool,

    /// Send this run's OpenTelemetry spans to a collector.
    ///
    /// **Off unless you ask, and asking twice is the same as asking once:**
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` in the environment turns the same thing on
    /// without the flag, which is what a systemd unit or a container image sets.
    ///
    /// Off means *nothing leaves the machine*: no exporter is built, no thread
    /// is started for one, and no socket is opened. The spans this crate would
    /// send are the ones its `#[instrument]`ed commands already produce, so
    /// turning this on adds a collector to the picture and nothing else.
    ///
    /// Where it goes is the exporter's standard resolution:
    /// `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` with `/v1/traces` appended, then
    /// `http://127.0.0.1:4318`.
    ///
    /// **This build has no TLS backend unless it was compiled with
    /// `--features telemetry-tls`.** Without it, an `https://` endpoint fails to
    /// connect rather than being downgraded — the same refusal
    /// `versions --remote` makes. That matches `lepatch serve`, which offers no
    /// TLS either: a telemetry backend is expected to sit on the same trust
    /// boundary as the thing it is describing. A build that does speak `https`
    /// reads the operating system's trust store, not one compiled in.
    #[arg(long)]
    telemetry: bool,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    /// Snapshot a directory into a new version of a backup set.
    Backup {
        /// Directory to back up.
        source: std::path::PathBuf,
        /// Base path of the backup set, e.g. `photos` creates `photos.0000.bin`
        /// and `photos.lock`.
        name: std::path::PathBuf,
        /// How large a segment may get before a new one is started, e.g. `4GiB`.
        ///
        /// Only affects segments written from now on, and never rewrites what is
        /// already there: no record depends on where a boundary fell.
        #[arg(long, value_name = "SIZE", value_parser = parse_size)]
        segment_size: Option<u64>,
        /// How hard to compress record payloads: 0 stores them as they are, 1
        /// (default) is fastest, 9 is smallest.
        ///
        /// Only affects records written from now on, and never rewrites what is
        /// already there: every record names the codec it was written with, so a
        /// set written at one level reads back at any other. A payload that does
        /// not compress is stored raw at any level.
        #[arg(long, value_name = "LEVEL", value_parser = clap::value_parser!(u8).range(0..=9))]
        compression_level: Option<u8>,
        /// Which of the tuned chunk-size profiles to cut this tree with.
        ///
        /// `small` cuts finely, `large` coarsely, and `default` is what every
        /// backup so far used. Only affects chunks written from now on, and never
        /// rewrites what is already there: chunks are named by their contents, so
        /// a set backed up at more than one profile is still one store and every
        /// version restores — but a run stops deduplicating against chunks cut
        /// under a different profile, so a set changed mid-life wants re-taking.
        #[arg(long, value_name = "PROFILE")]
        chunk_profile: Option<reader::ChunkerProfile>,
        /// Say what this backup would store, and store none of it.
        ///
        /// Affects what is written and nothing else: the tree is still walked, cut
        /// into chunks and hashed, and every chunk still compared against what the
        /// set already holds — so a dry run costs what a backup costs and reports
        /// the numbers a backup would have printed. Nothing is created: no segment,
        /// no index file, no lock, and not the set's directory if it is not there
        /// yet. No claim is taken either, so a plan can be taken against a set a
        /// backup currently holds.
        #[arg(long)]
        dry_run: bool,
    },
    /// Apply a pack into a backup set, as a new version of it.
    ///
    /// The receiving half of `pack`, and the one a remote push would use: the set
    /// ends up holding the version the pack carries, and nothing else is consulted
    /// to do it — the pack carries its own file table.
    ///
    /// A full pack suits any set and creates one if there is none. A delta pack
    /// names a version its reader already holds and omits those chunks, so it is
    /// **refused unless some committed version of the set is that version** —
    /// nothing here can tell you were missing the chunks the pack left out.
    ///
    /// Atomic in the store's own sense: every chunk is stored, made durable, and
    /// then one commit record publishes the version. A run that fails before that
    /// leaves no version, and no rollback is attempted because there is nothing to
    /// undo — see `src/command/merge.rs` for why.
    ///
    /// Merging the same pack twice is not an error: the second run reports which
    /// version of this set already holds the pack's snapshot and writes nothing.
    /// A transfer may be retried, and the retry has to be answerable.
    ///
    /// A pack built from a version *behind* this set's tip is warned about on
    /// stderr and merged anyway — the two sets number their versions
    /// independently, so the pack's own number says nothing about where it lands.
    /// It lands on the set's next version, which is what `restore` with no
    /// `--version` returns, and that is what the warning says.
    Merge {
        /// The pack to merge.
        pack: std::path::PathBuf,
        /// Base path of the backup set to merge into, e.g. `photos` creates
        /// `photos.0000.bin` and `photos.lock`.
        name: std::path::PathBuf,
        /// How each chunk the pack carries is stored: `keep` (the default) writes
        /// the stored form the pack already has, `0` writes the content as it is,
        /// and 1 to 9 decode and store a zstd frame at that level.
        ///
        /// `keep` is what makes a merge of a compressed pack cheap: a chunk the
        /// source set deduplicated as a frame is written as that frame, rather than
        /// decoded and compressed again. It is not a promise to skip the work
        /// entirely — every chunk is still decoded and hashed on the way in, so a
        /// pack whose bytes do not match the names it carries is refused exactly as
        /// it is on any other setting. A frame that is not smaller than its
        /// content, or that names a codec this build cannot read, is stored raw
        /// instead, so a pack cannot grow the set.
        ///
        /// A level re-encodes everything, which is what a set with a compression
        /// policy of its own wants: `keep` would write the *sender's* frames into
        /// it. Nothing already in the set is rewritten either way — every record
        /// names the codec it was written with — so this is a policy for what
        /// arrives from now on, exactly as `backup --compression-level` is.
        ///
        /// Not named `--compression-level`, which `backup` already spends on a
        /// different vocabulary with a different default: one flag with two
        /// meanings turns a mistyped option into a confusing one rather than an
        /// obvious one.
        #[arg(long, value_name = "KEEP_OR_LEVEL", default_value = "keep",
              value_parser = parse_keep_or_level)]
        compression: KeepOrLevel,
        /// Say what this merge would store, and store none of it.
        ///
        /// Affects what is written and nothing else: the pack is still resolved
        /// against the set, and every chunk about to be stored is still read out
        /// of it and hashed, so a plan refuses exactly what a merge refuses and
        /// costs what one costs. Nothing is created — no segment, no index file,
        /// not the set's directory if it is not there yet — and no lock is taken,
        /// since the claim is a file. A delta is still refused for a set that
        /// does not hold its base.
        #[arg(long)]
        dry_run: bool,
    },
    /// Rebuild the per-version index files from the store's commit records.
    ///
    /// They are a cache: nothing is lost if they are missing or wrong.
    Reindex {
        /// Base path of the backup set to rebuild.
        name: std::path::PathBuf,
        /// Say how many index files would be written, and write none of them.
        ///
        /// Affects nothing but the writing: the set is still opened and scanned, so
        /// the version count is the store's own. A torn tail is *not* reclaimed —
        /// repairing one is the point of a real reindex, and a dry run repairs
        /// nothing, so a set left by a killed run is refused here and repaired by a
        /// real one — and no lock is taken, because taking one creates a file.
        #[arg(long)]
        dry_run: bool,
    },
    /// List the versions a backup set holds, oldest first.
    ///
    /// The count is a lower bound: a version whose index file was never written
    /// still exists and still restores, so this can under-report and never
    /// over-reports.
    ///
    /// **The same listing is printed whether the set is local or remote.** With
    /// `--remote` the set lives on a `lepatch serve` and nothing is read from
    /// disk, but the lines are the same lines: `{version}  {snapshot}`, or just
    /// `{version}` under `--numbers-only`. A script written against a local set
    /// works unchanged against a remote one.
    Versions {
        /// Base path of the backup set to inspect.
        ///
        /// With `--remote`, the *name* of the set on that server rather than a
        /// path to one: `docs`, not `backups/docs`.
        name: std::path::PathBuf,
        /// Print just the version numbers, one per line, and nothing else.
        #[arg(long)]
        numbers_only: bool,
        /// List a set held by a `lepatch serve`, at this address, over HTTP.
        ///
        /// The address is the one the server printed on its first line, which is
        /// why that line goes to stdout. `http://` only: this client has no TLS
        /// and `lepatch serve` offers none, so an `https://` address is refused
        /// rather than quietly downgraded.
        ///
        /// A set the server does not hold is an error carrying the server's own
        /// sentence, exactly as a mistyped local path is.
        #[arg(long, value_name = "URL")]
        remote: Option<String>,
    },
    /// Serve the backup sets under a directory over HTTP.
    ///
    /// **The server moves packs and nothing else.** It never takes a path on its
    /// own disk: there is no endpoint that snapshots a server-local tree and none
    /// that restores into one. A remote backup is a client running `lepatch
    /// backup` and `lepatch pack` locally, uploading the pack, and this server
    /// merging it. A remote restore is this server packing a version and the
    /// client fetching the chunks it lacks.
    ///
    /// That boundary is what keeps the server small. A server that snapshotted
    /// its own filesystem would need every rule the local commands have about
    /// claims, about `..` in recorded paths, and about refusing a destination
    /// that holds the set being restored from — all of it, at the one place where
    /// it is least wanted. None of it is needed here, because no tree is ever
    /// named.
    ///
    /// **There is no authentication, and there is no TLS.** Anyone who can reach
    /// the port can read every set under the root and can append versions to them.
    /// That is a decision, not an oversight. The bind address defaults to
    /// `127.0.0.1`, so the common case is a service on the same machine; `--bind`
    /// says otherwise out loud and the server does not check. Put it behind
    /// something that does, if it needs it.
    ///
    /// The API is described by `openapi/lepatch.json`, served at
    /// `/openapi.json`. `lepatch serve` is a `pack` on one end and a `merge` on
    /// the other, so anything that can run those can be a client.
    Serve {
        /// Directory holding the backup sets to serve.
        root: std::path::PathBuf,
        /// Address to bind, e.g. `127.0.0.1:8480`.
        ///
        /// Defaults to `127.0.0.1:8480`. There is no refusal for a non-loopback
        /// address, because there is no authentication to protect and a warning
        /// that cannot be acted on is noise.
        #[arg(long, value_name = "ADDR")]
        bind: Option<String>,
        /// Where uploaded packs are written.
        ///
        /// A merge reads a pack **twice** — once to check the file table and once
        /// per chunk — so an upload has to land on a seekable file rather than be
        /// streamed. Defaults to `.lepatch-serve` under the root, rather than a
        /// system temporary directory: a pack can be larger than one is sized for,
        /// and these files exist to move data the sets hold.
        #[arg(long, value_name = "DIR")]
        scratch: Option<std::path::PathBuf>,
    },
    /// Rebuild a directory from a backup set.
    Restore {
        /// Directory to restore into.
        destination: std::path::PathBuf,
        /// Base path of the backup set to restore from.
        name: std::path::PathBuf,
        /// Version to restore, defaulting to the most recent usable one.
        #[arg(long, short)]
        version: Option<u32>,
        /// Empty the destination before restoring, so it ends up holding exactly
        /// the snapshot and nothing else.
        ///
        /// Off by default; a merging restore says at the end that it may have
        /// left other files, and points at `--clean`.
        #[arg(long)]
        clean: bool,
        /// Say what this restore would write, and write none of it.
        ///
        /// Affects what is written and nothing else: the version is still read out
        /// of the set and every chunk still checked against the hash it is
        /// addressed by, so a dry run refuses exactly what a restore refuses and
        /// costs what one costs. Nothing is created and nothing is removed — no
        /// file, no directory, no attribute, no `.partial` beside the destination —
        /// and no claim is taken there, which is what lets a plan be taken against
        /// a destination another restore is using.
        #[arg(long)]
        dry_run: bool,
    },
    /// Check a snapshot's stored chunks against their recorded hashes.
    Verify {
        /// Base path of the backup set to check.
        name: std::path::PathBuf,
        /// Version to check, defaulting to the most recent usable one.
        #[arg(long, short)]
        version: Option<u32>,
    },
    /// Write one version's chunks to a single distributable file.
    ///
    /// The set's chunks concatenated with no framing between them, so a client
    /// fetching a new version gets one range rather than one per chunk. A pack is
    /// output, not a backup: it is regenerated from the set and its snapshot
    /// alone, and losing one loses nothing.
    ///
    /// ## What the pack records about where it came from
    ///
    /// A pack records the number of the version it was built from — *that set's*
    /// number, the one `lepatch versions` would print beside it. So `pack set
    /// v2.pack --version 2` writes a pack whose header says version 2.
    ///
    /// It buys one thing: `merge` can see that a pack arrived from behind and
    /// say so, so a stale sender is noticed at the moment a transfer lands rather
    /// than when a later restore returns an older version than was asked for. It
    /// changes nothing about what the pack *is* — the same chunks, the same file
    /// table, the same content hash — and nothing about whether it merges.
    ///
    /// **It is not a coordinate.** It says which version of the set the pack came
    /// from, not where it lands in the set you merge it into: a pack of origin
    /// version 2 merged into a set already on version 5 is published as version 6.
    /// The two sets numbered their versions independently and never met, so there
    /// is nothing for the 2 to map onto. What you restore is chosen with
    /// `restore --version N`, and with no `--version` you get the set's most
    /// recent version, which is not necessarily the one this pack was built from.
    Pack {
        /// Base path of the backup set to pack from.
        name: std::path::PathBuf,
        /// Where to write the pack. Overwritten if it is already there.
        out: std::path::PathBuf,
        /// Version to pack, defaulting to the most recent usable one.
        #[arg(long, short)]
        version: Option<u32>,
        /// Pack only what this version's holder is missing, instead of the whole
        /// version.
        ///
        /// `--from 7` writes a pack carrying version 8's file table and only the
        /// chunks version 7 does not already have. Smaller, and built once for
        /// everyone on version 7 — no per-client anything.
        ///
        /// A delta pack holds only what its holder already has *elsewhere*, so it is
        /// for a client known to be on that version. Anything else — version 4, or a
        /// half-applied tree — is missing chunks the pack omits, and `apply` fails
        /// partway rather than producing a wrong directory. Use `--full` for a pack
        /// that suits any client.
        #[arg(long, value_name = "VERSION")]
        from: Option<u32>,
        /// Pack the whole version, ignoring `--from`.
        ///
        /// The default when no `--from` is given. One artifact, one content hash,
        /// and correct for any client — which is what makes it the one worth caching.
        #[arg(long)]
        full: bool,
        /// How each chunk is written into the pack: `keep` (the default) writes
        /// the stored form the set already has, `0` writes the content as it is,
        /// and 1 to 9 decode and write a zstd frame at that level.
        ///
        /// `keep` is what makes packing a compressed set cheap: a chunk the set
        /// deduplicated as a frame is copied as that frame rather than decoded and
        /// compressed again. A level is for a pack built *for delivery* — a
        /// re-compressed pack of a set that stores raw, or a smaller one to send —
        /// and it changes only the bytes in the records. A chunk is named by the
        /// hash of its content, so every form decodes to the same content and
        /// everything downstream — `merge`, `apply`, a restore — sees the same
        /// version. It does not rewrite the set either, and neither does it rewrite
        /// a pack: this is a policy for what leaves the set now.
        ///
        /// The same vocabulary as `merge --compression`, parsed by the same parser,
        /// because it is the same question at the two ends of a transfer: what these
        /// records should look like on the wire.
        #[arg(long, value_name = "KEEP_OR_LEVEL", default_value = "keep",
              value_parser = parse_keep_or_level)]
        compression: KeepOrLevel,
        /// Say what this pack would hold, and write none of it.
        ///
        /// Affects the file and nothing else: the version is still resolved and
        /// every chunk still located, so the counts and the byte totals are the
        /// pack's own. `--out` is neither created nor overwritten. The size on disk
        /// and the content hash are not reported, because both are properties of a
        /// file this run does not write.
        ///
        /// Under a level the byte totals are the re-encoded ones, so the plan
        /// reports what that run would write rather than what the set holds: a dry
        /// run of `--compression 9` costs an encode per chunk, and that cost is
        /// what makes its figures the real run's.
        #[arg(long)]
        dry_run: bool,
    },
    /// Bring a directory up to the version a pack carries, writing only what is
    /// missing.
    ///
    /// The other half of `pack`. Nothing else is needed: a pack carries its own
    /// file table, so no backup set is consulted.
    ///
    /// A file with nothing to write is not opened, so a directory that is already
    /// the version keeps every inode and timestamp it had. Not atomic — a run that
    /// is interrupted leaves a directory that is neither version, and running it
    /// again finishes the job.
    Apply {
        /// The pack to apply.
        pack: std::path::PathBuf,
        /// Directory to bring up to the version.
        destination: std::path::PathBuf,
        /// Remove entries the version does not record.
        ///
        /// Off by default: a file here is as likely to be the user's own work as a
        /// leftover from an older version, and nothing in the version can tell those
        /// apart. Without this, such files are reported and left alone.
        #[arg(long)]
        prune: bool,
        /// Re-read every file, ignoring the record of the last apply.
        ///
        /// Each apply leaves a note beside the directory saying which files it
        /// wrote and what the filesystem said about them afterwards. The next one
        /// stats those files and skips the ones that have not moved, which is what
        /// makes a repeated apply cost nothing but a stat per file.
        ///
        /// With this, nothing is taken on that note's word. Slower — it reads the
        /// whole tree — and it is the mode to use if the directory has been changed
        /// by something that restores timestamps.
        #[arg(long)]
        recheck: bool,
        /// Say what this apply would change, and change none of it.
        ///
        /// Affects what is written and nothing else: the destination is still
        /// probed against the version, and every chunk about to be written is still
        /// read out of the pack and hashed, so a damaged pack is still refused and
        /// the figures are the ones an apply would have printed. Nothing is created
        /// and nothing is removed — no file is opened, no entry pruned, and no
        /// manifest recorded beside the destination — and no claim is taken there.
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    // The registry the progress layer forwards into. It has to exist before
    // `init_tracing`, because the layer is installed on the subscriber there.
    //
    // Built unconditionally: it is a broadcast channel and two mutexes, and a
    // conditional build made every command carry an `Option` that one arm of a
    // match could not use. Which commands install the layer is `init_tracing`'s
    // business, and it still installs it only for `serve`.
    let jobs = lepatch::serve::Jobs::new();
    let serving = matches!(args.command, Command::Serve { .. });

    // Telemetry is read once, here, and everything downstream asks this value
    // rather than the environment. See `lepatch::telemetry`.
    let telemetry = lepatch::telemetry::Config::from_environment(args.telemetry);

    init_tracing(
        args.verbose,
        args.quiet,
        serving.then_some(&jobs),
        &telemetry,
    );

    // **Every way out of this process goes through this line.**
    //
    // `run` returns a result for all three endings — the command succeeded, the
    // command failed, and `stop_signal` interrupted it — and the exit code is
    // decided from that result, so flushing here covers every one of them
    // rather than only the successful one. The stop path matters as much as the
    // others: a container that is sent `SIGTERM` is the most common way a
    // server-side run ends, and it is the one most likely to be the *only* span
    // anybody wanted.
    //
    // An OTLP exporter is asynchronous, so without this the spans of a run that
    // has already finished are still sitting in a queue on a thread the process
    // is about to stop using. That is a silent loss: no error, no warning, and
    // a run that looks exactly like a successful one.
    let code = match run(args.command, jobs).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The message, not the `Debug` form.
            //
            // There is no cause chain to walk. `lepatch` has one error type,
            // `io::Error`, at all ninety-odd sites where it fails. Each of those
            // messages is a complete sentence. Each one names the file, the
            // offset, and the expectation, because that is what a user has to act
            // on.
            //
            // This loop used to walk `source()` here and print `caused by:`
            // lines. It never printed anything, and could not have:
            //
            // * nothing in the crate implements `std::error::Error::source`, so
            //   the first `source()` call returned `None` at every one of them.
            // * and `io::Error` cannot carry the chain in the first place. Its
            //   `source()` returns the *source of* a wrapped custom error. It does
            //   not return the error itself. So `io::Error::new(kind, e)` drops
            //   `e` from the walk even when `e` has a cause. The one site that
            //   wraps
            //   another error loses it here too. That site is a snapshot that
            //   will not decode. Meanwhile `Display` still shows it, since
            //   `io::Error` delegates to the wrapped error's `Display`.
            //
            // So the code was a promise that the type system could not keep. A
            // contributor adding an error with a cause would reasonably assume
            // that the output was already wired up. If a future change
            // introduces a bespoke error type with a live chain, then this walk
            // comes back with it. Then it has something to print.
            eprintln!("error: {e}");

            // 130 for Ctrl-C, 143 for `kill`.
            let signal = STOP_SIGNAL.load(std::sync::atomic::Ordering::Relaxed);
            if signal == 0 {
                ExitCode::FAILURE
            } else {
                ExitCode::from(128 + signal)
            }
        }
    };

    flush_telemetry();

    code
}

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
fn flush_telemetry() {
    if let Err(e) = lepatch::telemetry::shutdown() {
        tracing::warn!(error = %e, "telemetry could not be flushed at exit");
    }
}

/// Build the subscriber, with the job-progress layer installed when serving and
/// the OpenTelemetry layer installed when telemetry was asked for.
///
/// **The OpenTelemetry layer is built here and installed here, and the library
/// builds it without installing it** (`lepatch::telemetry::layer`), because the
/// subscriber is process-global and this binary owns the only one there is.
///
/// **When telemetry is off there is no layer at all**, not a layer that does
/// nothing: `lepatch::telemetry::layer` returns `Ok(None)` before it constructs
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
/// `lepatch::progress` events out of the span a job runs in and forwards the
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
/// `lepatch::progress` event is an `INFO` event, so `lepatch --quiet serve`
/// answered `never` to it, the event was never emitted, the progress layer above
/// was never offered it, and a client polling `GET /v1/jobs/{id}` was told the
/// merge had reported nothing. Adding `RUST_LOG=info` restored it, which is what
/// made this look like a filter bug rather than an architectural one.
///
/// A filter attached to the writer layer answers only the writer's question. The
/// progress layer carries its own — [`lepatch::serve::progress_filter`] — so a
/// client's view of a running job does not move when the operator turns the
/// terminal down. The two are asked the same question and are allowed to answer
/// it differently, which is the point.
///
/// The other commands are unaffected because for them there is no second layer:
/// the `None` arm below installs one filtered layer over the registry, and a
/// filter on the only layer is indistinguishable from a filter on the subscriber.
/// `tests/cli.rs::quiet_suppresses_progress_and_verbose_adds_detail` is what
/// holds that still true.
fn init_tracing(
    verbose: bool,
    quiet: bool,
    jobs: Option<&lepatch::serve::Jobs>,
    telemetry: &lepatch::telemetry::Config,
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

    let otel = match lepatch::telemetry::layer(telemetry) {
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
    if let Err(e) = lepatch::telemetry::metrics::install(telemetry) {
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
            let layer = lepatch::serve::progress_layer(jobs.clone())
                .with_filter(lepatch::serve::progress_filter());

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
                .unwrap_or(lepatch::telemetry::DEFAULT_ENDPOINT),
            "telemetry is on"
        );
    }
}

/// Number of the signal that asked us to stop, or 0 if none did. `main` reads it
/// to report `128 + signal`.
static STOP_SIGNAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Print one line of a command's output to stdout, reporting whether it went.
///
/// `println!` cannot be used. `SIGPIPE` is ignored at startup. So a write to a
/// pipe whose reader has gone returns `EPIPE`, and the print macros panic on a
/// write error. That case is a *success*. The reader leaving is what `head` does
/// on purpose. So the macro returns `Ok(())`, and the command carries on to its
/// own result. Any other write failure is an error.
///
/// This deliberately does not decide the exit code. The macro used to call
/// `process::exit` directly. That meant a command could print its summary before
/// computing its result, then exit 0 with a result that it had not looked at
/// yet. `lepatch verify set | true` reported success on a store that had failed
/// verification, because the write failed before `into_result` was ever called.
/// The macro also skipped every `Drop` in scope. A `Drop` is how a `Lock` or a
/// `DestinationLock` releases its claim file. So any command that both held a
/// claim and printed would have leaked it.
macro_rules! say {
    ($($arg:tt)*) => {{
        use std::io::Write as _;

        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        match writeln!(out, $($arg)*) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
            Err(e) => Err(std::io::Error::other(format!("could not write output: {e}"))),
        }
    }};
}

/// Run a command, abandoning it if the process is asked to stop.
///
/// The claims a run holds are released by `Drop`, so stopping is a matter of
/// letting the work future drop rather than resuming it.
async fn run(command: Command, jobs: lepatch::serve::Jobs) -> std::io::Result<()> {
    tokio::select! {
        result = dispatch(command, jobs) => result,
        signal = stop_signal() => Err(signal),
    }
}

/// Parse a human size like `4GiB`, `512MiB`, `1G`, or a plain byte count.
///
/// Units are the powers of 1024 with an optional `i`. Anything else is
/// refused rather than guessed at. A byte count with no unit is taken literally.
fn parse_size(raw: &str) -> std::result::Result<u64, String> {
    let text = raw.trim();

    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);

    let value: u64 = digits
        .parse()
        .map_err(|_| format!("`{raw}` does not start with a number of bytes"))?;

    let multiplier: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024_u64 * 1024 * 1024 * 1024,
        other => {
            return Err(format!(
                "`{other}` is not a size lepatch knows; use B, KiB, MiB, GiB or TiB"
            ));
        }
    };

    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("`{raw}` is larger than can be addressed"))
}

/// Serve the sets under `root` until the process is asked to stop.
///
/// **The bound address goes to stdout**, and that is the same rule `pack` follows
/// when it prints a content hash. stdout is data; diagnostics are stderr. The
/// address is data a script needs — it wants to connect, and under `--bind
/// 127.0.0.1:0` it is the only way to learn the port. Diagnostics stay on
/// stderr, so `lepatch serve --bind 127.0.0.1:0 | head -1` is a working way to
/// get the address.
///
/// The server shuts down on the same [`stop_signal`] every other command uses, so
/// Ctrl-C and a container stop both stop it cleanly — and the `Drop` guard on the
/// scratch directory removes whatever uploads were still held.
async fn run_serve(
    root: &std::path::Path,
    bind: Option<&str>,
    scratch: Option<&std::path::Path>,
    jobs: &lepatch::serve::Jobs,
) -> std::io::Result<()> {
    let config = lepatch::serve::Config::new(root, bind.unwrap_or(DEFAULT_BIND), scratch);

    // Reported before the listener is up, and to stdout, so a caller reading one
    // line knows the port. The address is resolved by actix, which is what makes
    // `--bind 127.0.0.1:0` mean "pick one".
    tracing::info!(
        root = %config.root.display(),
        bind = %config.bind,
        scratch = %config.scratch.display(),
        "serving"
    );

    let running = lepatch::serve::bind(&config, jobs)?;
    say!("http://{}", running.addr())?;

    running.run().await
}

/// The address a server binds when `--bind` is not given.
///
/// Loopback, because there is no authentication and the two go together: a
/// service that grants every set on the machine to the network is a mistake, and
/// loopback is the default that prevents it by omission.
const DEFAULT_BIND: &str = "127.0.0.1:8480";

/// The word a command is reported and measured under.
///
/// **The name `lepatch --help` already prints**, so `lepatch.command.duration`
/// carries the same word the user typed. A second spelling would be a dashboard
/// split in two by nothing at all.
fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Backup { .. } => "backup",
        Command::Restore { .. } => "restore",
        Command::Verify { .. } => "verify",
        Command::Pack { .. } => "pack",
        Command::Apply { .. } => "apply",
        Command::Merge { .. } => "merge",
        Command::Reindex { .. } => "reindex",
        Command::Versions { .. } => "versions",
        Command::Serve { .. } => "serve",
    }
}

/// The work itself, without the interruption handling.
///
/// **Time the command and report it on both arms.** The measurement is around
/// [`dispatch_inner`] rather than inside it, so it covers the whole of the work
/// and *not* the `select!` — an interrupted run is not timed, because there is
/// no code after the `select!` in `run` that still knows which command it was.
/// `serve` is timed too: a server that runs for a day and is then told to stop
/// has a duration, and a histogram of one very large sample is a true answer
/// rather than a wrong one.
async fn dispatch(command: Command, jobs: lepatch::serve::Jobs) -> std::io::Result<()> {
    let name = command_name(&command);
    let started = std::time::Instant::now();
    let outcome = dispatch_inner(command, jobs).await;
    lepatch::telemetry::metrics::record_command_duration(name, started.elapsed());
    outcome
}

/// The commands themselves.
async fn dispatch_inner(command: Command, jobs: lepatch::serve::Jobs) -> std::io::Result<()> {
    match command {
        Command::Backup {
            source,
            name,
            segment_size,
            compression_level,
            chunk_profile,
            dry_run,
        } => {
            let compression = compression_policy(compression_level);
            let profile = chunk_profile.unwrap_or_default();
            run_backup(&source, &name, segment_size, compression, profile, dry_run).await
        }
        Command::Restore {
            destination,
            name,
            version,
            clean,
            dry_run,
        } => run_restore(&destination, &name, version, clean, dry_run).await,
        Command::Verify { name, version } => run_verify(&name, version).await,
        Command::Pack {
            name,
            out,
            version,
            from,
            full,
            compression,
            dry_run,
        } => {
            run_pack(
                &name,
                &out,
                version,
                effective_from(from, full),
                compression,
                dry_run,
            )
            .await
        }
        Command::Apply {
            pack,
            destination,
            prune,
            recheck,
            dry_run,
        } => run_apply(&pack, &destination, prune, recheck, dry_run).await,
        Command::Merge {
            pack,
            name,
            compression,
            dry_run,
        } => run_merge(&pack, &name, compression, dry_run).await,
        Command::Reindex { name, dry_run } => run_reindex(&name, dry_run).await,
        Command::Versions {
            name,
            numbers_only,
            remote,
        } => match remote {
            Some(address) => run_versions_remote(&name, numbers_only, &address).await,
            None => run_versions(&name, numbers_only).await,
        },
        Command::Serve {
            root,
            bind,
            scratch,
        } => run_serve(&root, bind.as_deref(), scratch.as_deref(), &jobs).await,
    }
}

/// Resolve when the process is asked to stop. `SIGTERM` as well as Ctrl-C.
/// A supervisor sends that, and so does a container stop or a closing laptop.
///
/// A second Ctrl-C is left to the default handler, so a user who wants out
/// immediately can still have it.
#[cfg(unix)]
async fn stop_signal() -> std::io::Error {
    use std::sync::atomic::Ordering;

    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(_e) => {
                // No `SIGTERM` on this platform. Ctrl-C is all there is.
                let _ = tokio::signal::ctrl_c().await;
                STOP_SIGNAL.store(2, Ordering::Relaxed);
                return interrupted();
            }
        };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            STOP_SIGNAL.store(2, Ordering::Relaxed);
        }
        _ = terminate.recv() => {
            STOP_SIGNAL.store(15, Ordering::Relaxed);
        }
    }

    interrupted()
}

/// Ctrl-C only: the unix build has `SIGTERM` as well.
#[cfg(not(unix))]
async fn stop_signal() -> std::io::Error {
    use std::sync::atomic::Ordering;

    let _ = tokio::signal::ctrl_c().await;
    STOP_SIGNAL.store(2, Ordering::Relaxed);
    interrupted()
}

/// The error a stopped run reports. Not a success, and not a silent exit. The
/// work was abandoned. What is guaranteed is that nothing was *published*. A
/// backup's index is filled in only on completion. A restore's claim is released.
///
/// `Interrupted` is worth the scrutiny, because the crate is wary of naming a
/// kind it does not mean. `NotFound` is what the operating system returns for a
/// file that has gone. So a caller cannot tell one of ours from a real one. That
/// ambiguity has cost a swallowed error before. `Interrupted` has no such
/// collision. `std` retries `EINTR` itself, so no filesystem call in this crate
/// ever produces it. Whoever gets this error knows it means a deliberate stop,
/// and nothing else. That is exactly what the message says.
fn interrupted() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "interrupted; any claim on a backup set or destination has been released",
    )
}

/// Resolve which version to operate on, or explain why there is none. A version
/// exists if and only if the store holds the commit record that published it.
fn select_version(
    set: &IndexSet,
    store: &storage::BlobStore<storage::Read>,
    requested: Option<u32>,
) -> std::io::Result<(u32, storage::BlobId)> {
    match requested {
        Some(version) => Ok((version, store.snapshot(version)?)),
        None => store.latest().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "no committed version found in {} (looked in {})",
                    set.base().display(),
                    set.directory().display()
                ),
            )
        }),
    }
}

/// Rebuild every per-version index file from the store's commit records.
///
/// Opened for writing, and that is what makes this a repair. A torn tail in the
/// last segment is reclaimed on the way. The lock is taken, so no run is
/// appending while the records are read.
///
/// A dry run takes neither. Both of them are writes. The lock is a file, and
/// reclaiming a tail is the repair. The version count that a dry run reports is
/// the same one either way. The count comes out of the commit records rather than
/// out of what the open did to the set.
///
/// Which leaves a consequence worth stating rather than leaving to be found. A
/// set left behind by a killed run has a torn tail. Its dry run fails. Its real
/// reindex succeeds and repairs the store. That is the right way round, because
/// deciding a tail is damage is itself a write.
async fn run_reindex(name: &std::path::Path, dry_run: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Before taking the lock and before opening. Opening for writing
    // *creates* a set that is not there. So a mistyped name would be repaired
    // and reported as a success.
    if set.segments()?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no backup set at {}: there is nothing to reindex (looked in {})",
                set.base().display(),
                set.directory().display()
            ),
        ));
    }

    let _lock = if dry_run { None } else { Some(set.lock()?) };

    if dry_run {
        let store = storage::BlobStore::open_for_reading(set.base()).await?;
        let versions = store.versions()?;

        // `write_index_files` writes one file per version that it can resolve a
        // snapshot for. The function skips the rest and returns the lower count.
        // So the plan applies the same test rather than counting the commit
        // records. A version whose snapshot does not resolve is not counted here.
        // A real reindex writes no index file for that version either. A plan
        // reports what the run does, and it does not write one. The index files
        // are a cache either way, so rewriting one is how they are brought back
        // to date.
        let writable = versions
            .iter()
            .filter(|version| store.snapshot(**version).is_ok())
            .count();
        let existing = set.indexed_versions()?.len();

        tracing::info!(name = %set.base().display(), "dry run: nothing was written");

        say!(
            "dry run: {} in {}",
            plural_versions(versions.len()),
            set.base().display()
        )?;
        say!(
            "dry run: {writable} index file(s) would be written, {existing} of them already there"
        )?;

        return Ok(());
    }

    let store = storage::BlobStore::open_for_writing(
        set.base(),
        storage::SegmentLimit::default(),
        storage::Compression::default(),
    )
    .await?;

    let versions = store.versions()?;
    let written = store.write_index_files()?;

    tracing::info!(
        name = %set.base().display(),
        versions = versions.len(),
        written,
        "rebuilt the version index"
    );

    Ok(())
}

/// "1 version" or "3 versions", for a line that reports a count.
fn plural_versions(count: usize) -> String {
    if count == 1 {
        "1 version".to_string()
    } else {
        format!("{count} versions")
    }
}

/// List the versions a backup set holds, oldest first, each with its snapshot hash.
///
/// The count is a **lower bound**. A version whose index file was never
/// written still exists and still restores. So this may report fewer than the set
/// holds, and never more. A set with no index files lists nothing rather than
/// failing. The command `lepatch reindex` rebuilds the index of such a set. A real
/// set with no committed version lists empty. A name that is not a set is an
/// error.
async fn run_versions(name: &std::path::Path, numbers_only: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // No segments at all is a mistyped name, not an empty set. Answering "no
    // versions" to a name that names nothing would be a failure that a script
    // acts on.
    if set.segments()?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no backup set at {} (looked in {})",
                set.base().display(),
                set.directory().display()
            ),
        ));
    }

    let versions = set.indexed_versions()?;

    // The hashes, which the file names cannot supply. Opened read-only, because
    // opening for writing truncates an unfinished run. A listing must not be the
    // thing that discards it. A set with every segment but no committed version
    // lists empty rather than erroring, and that falls out. The open succeeds.
    // The loop below has nothing to iterate, and nothing is printed.
    //
    // Which is why there is no arm for "no set here". This used to be
    // `Err(e) if e.kind() == NotFound => return Ok(())`, and the kind was doing
    // two jobs. The one it was meant for is unreachable. The check above already
    // established that the set has segments. That is the only condition which
    // makes the open report `NotFound`. So the arm never fired on the case it was
    // written for. The other job is the problem. `NotFound` is what the operating
    // system returns for a file that has gone. So a segment removed between that
    // check and the open would have been swallowed too. Then `lepatch versions`
    // would exit 0 having printed nothing about a set that was being deleted.
    let store = storage::BlobStore::open_for_reading(set.base()).await?;

    for version in versions {
        // Through the store, which checks the file against itself. An index that
        // is stale, foreign, or edited is a reason to fall back to a scan. It is
        // never a reason to name the version. One that does not resolve is
        // skipped rather than reported. A listing must not name a version that
        // the store cannot serve.
        //
        // Both modes go through `resolve` for that reason. `--numbers-only` used
        // to read the index file on its own. That path checksums one file and
        // never consults the store. So a leftover index in a set with fewer
        // versions would be named by `versions --numbers-only`. The same index
        // would be omitted by `versions`. Both commands ran from the same
        // directory at the same moment.
        let Ok(snapshot) = store.snapshot_via_index(version) else {
            continue;
        };

        print_version(version, &snapshot, numbers_only)?;
    }

    Ok(())
}

/// One line of a version listing, on whichever side the set is.
///
/// **One function, so the two listings cannot drift.** `--remote` prints through
/// this and prints nothing else, which is what makes a script that reads a local
/// set work unchanged against a remote one: same fields in the same order, the
/// same two-space separator, the same `say!` and so the same broken-pipe
/// behaviour. A second formatting path would be a difference nothing tests until
/// a script meets it.
fn print_version(
    version: u32,
    snapshot: &storage::BlobId,
    numbers_only: bool,
) -> std::io::Result<()> {
    if numbers_only {
        say!("{version}")
    } else {
        say!("{version}  {snapshot}")
    }
}

/// List a set held by a `lepatch serve`, printing exactly what a local listing
/// of the same set would.
///
/// The fetch is [`lepatch::client::Remote`]'s. Everything here is deciding which
/// text is a set name — the positional NAME is a name on the server, not a path
/// on this machine — and printing what came back.
async fn run_versions_remote(
    name: &std::path::Path,
    numbers_only: bool,
    address: &str,
) -> std::io::Result<()> {
    let Some(set) = name.to_str() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{} is not a name this client can send: a set on a server is named by text",
                name.display()
            ),
        ));
    };

    let remote = lepatch::client::Remote::new(address)?;

    for listed in remote.versions(set).await? {
        print_version(listed.version, &listed.snapshot, numbers_only)?;
    }

    Ok(())
}

/// The compression policy from `--compression-level`, where 0 means "as-is".
///
/// clap rejects anything outside 0 to 9 as a usage error rather than letting it
/// be clamped. So a level is never silently turned into a store that disagrees
/// with the command that wrote it.
fn compression_policy(level: Option<u8>) -> storage::Compression {
    match level {
        Some(level) => storage::Compression::from_level(level),
        None => storage::Compression::default(),
    }
}

/// What a `--compression` flag asked for, on `pack` or on `merge`.
///
/// **One type and one parser for both commands, deliberately.** The two flags ask
/// the same question at the two ends of a transfer. That question is what the
/// records of this chunk should look like on the wire. A user who learns `keep`
/// on one of them has learned it on the other. Two types would let the
/// vocabularies drift. One command could come to accept a bare number. One
/// command could spell the default differently. The drift would then show up as a
/// flag that is accepted somewhere it is not meant. This is a little repetition
/// of the *routing*, which is genuinely different. A merge re-encodes on the way
/// into a set, and it has to open that set under a policy. A pack re-encodes on
/// the way out of a set, and it only reads that set. None of the repetition is
/// the parsing or the vocabulary. The parsing and the vocabulary are the part a
/// user meets.
///
/// Two answers rather than a policy on its own, because a level is both for a
/// merge. The level says which write path the run takes *and* what the receiving
/// set is opened with. The two cannot be allowed to disagree. A merge told to
/// re-encode into a set opened at a different level would store frames at a level
/// that nobody asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeepOrLevel {
    /// Store what the pack carries, frame and all.
    Keep,
    /// Decode every chunk and store it at this zstd level, 0 meaning as-is.
    Level(u8),
}

impl KeepOrLevel {
    /// The policy the receiving set is opened with.
    ///
    /// The default under [`KeepOrLevel::Keep`], where it decides almost nothing.
    /// The verbatim path does not compress. So the only record that it writes
    /// through the store's own policy is the snapshot. This is still a real
    /// choice rather than `Off`. A set opened for writing has to be opened with
    /// something. The default is what every other command opens a set with.
    fn policy(self) -> storage::Compression {
        match self {
            Self::Keep => storage::Compression::default(),
            Self::Level(level) => storage::Compression::from_level(level),
        }
    }

    /// Which write path the merge takes per chunk.
    fn route(self) -> lepatch::command::MergeCompression {
        match self {
            Self::Keep => lepatch::command::MergeCompression::Keep,
            Self::Level(_) => lepatch::command::MergeCompression::Reencode,
        }
    }

    /// The policy a pack writes each record under, or `None` to keep the form
    /// the set already holds.
    ///
    /// The one place the flag becomes a route for this command. `keep` is the
    /// absence of a policy, because it is the absence of any work. Both
    /// non-default answers are a policy that the pack's records are encoded
    /// under.
    fn pack_policy(self) -> Option<storage::Compression> {
        match self {
            Self::Keep => None,
            Self::Level(level) => Some(storage::Compression::from_level(level)),
        }
    }
}

/// The `--compression` argument of `pack` and `merge`: `keep`, or a level from 0
/// to 9.
///
/// Its own vocabulary rather than a bare number, so the default is a word
/// that says what it does. `keep` is the only non-numeric answer. Anything else
/// that is not a number in range is a usage error naming both. That is the same
/// treatment `backup --compression-level` gives an out-of-range level. A value
/// that this parser does not recognise must never become a policy that looks
/// like one the user chose.
///
/// The name says `keep` rather than either command, because it is both commands'.
fn parse_keep_or_level(what: &str) -> Result<KeepOrLevel, String> {
    if what.eq_ignore_ascii_case("keep") {
        return Ok(KeepOrLevel::Keep);
    }

    let level: u8 = what
        .parse()
        .map_err(|_| format!("`{what}` is not `keep` or a compression level from 0 to 9"))?;

    if level > storage::MAX_COMPRESSION_LEVEL {
        return Err(format!(
            "`{what}` is not `keep` or a compression level from 0 to {}",
            storage::MAX_COMPRESSION_LEVEL
        ));
    }

    Ok(KeepOrLevel::Level(level))
}

/// Snapshot a tree into a new version of the set at `name`.
///
/// The chunk profile is the run's own policy, not the store's. Like
/// `--segment-size` and `--compression-level`, it is a write policy for what
/// comes next. What is already there is never rewritten. So the profile is
/// logged with the sizes it resolved to, not only as the name that was typed.
/// The name is a shorthand for three numbers that a user cannot see.
///
/// A dry run does the whole of that and writes none of it. It takes no lock. It
/// writes no segment and no index file. It does not create the set's directory
/// when there is not one already. The run opens the set for reading, and only if
/// it is there. Opening for writing to learn whether a set exists would create
/// it. That is the one thing a dry run must never do. So the first backup of a
/// location plans as an all-new version rather than failing.
async fn run_backup(
    source: &std::path::Path,
    name: &std::path::Path,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
    dry_run: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // The store is append-only with one commit point at the end of a run. So two
    // runs interleaving would publish versions over each other. A dry run takes
    // no lock, because the claim is a file and a file is a write.
    let _lock = if dry_run { None } else { Some(set.lock()?) };

    let limit = match segment_size {
        Some(bytes) => storage::SegmentLimit::new(bytes),
        None => storage::SegmentLimit::default(),
    };
    let lock_path = set.lock_path();

    let segments = set.segments()?;

    // The set and its lock live next to the tree being read. Both grow while
    // the tree is read. So neither may be swept into the backup.
    let mut exclude = vec![lock_path];
    for segment in &segments {
        exclude.push(segment.clone());
    }
    // A segment that this run has yet to create is named too. That is
    // segment 0 of a fresh set. It is also the segment that a roll-over opens
    // during the run.
    for segment in 0..u32::try_from(segments.len()).unwrap_or(0).saturating_add(4) {
        exclude.push(set.segment_path(segment));
    }

    let chunker = profile.config();

    // The chunker's sizes are the only thing about this run that a later run
    // cannot work out from the store. They are also the one thing whose
    // consequences are not visible on disk. A set whose runs disagree about
    // chunk sizes restores perfectly and deduplicates badly. Naming the profile
    // that cut this run is the only warning the user gets. So the profile goes
    // in the run's own log line rather than behind `--verbose`.
    tracing::info!(
        profile = %profile,
        min = chunker.min_size,
        avg = chunker.avg_size,
        max = chunker.max_size,
        dry_run,
        "chunking this tree"
    );

    let options = Options {
        exclude,
        chunker,
        ..Options::default()
    };

    if dry_run {
        let existing = if segments.is_empty() {
            None
        } else {
            Some(storage::BlobStore::open_for_reading(set.base()).await?)
        };
        let fresh = existing.is_none();

        let plan = backup_plan(source, options, existing).await?;
        return report_backup_plan(&set, &plan, fresh);
    }

    // Opening discards a torn tail in the last segment. A torn tail is an
    // unfinished run, not data.
    let store = storage::BlobStore::open_for_writing(set.base(), limit, compression).await?;

    if let Some(previous) = store.versions()?.last() {
        tracing::debug!(previous, name = %set.base().display(), "deduplicating against the store");
    } else {
        tracing::debug!(
            name = %set.base().display(),
            "no earlier version; this is the first backup of this location"
        );
    }

    let segments = store.segment_count();
    let committed = backup(source, options, store).await?;

    tracing::info!(
        name = %set.base().display(),
        version = committed.version,
        snapshot = %&committed.snapshot,
        store = %set.base().display(),
        segments,
        "backup complete"
    );

    Ok(())
}

/// Say what a backup would have stored, on stdout, and that it stored nothing.
///
/// On stdout rather than the log, because this is the whole output of the run.
/// A dry run produces no progress worth following. It produces no summary about
/// work that happened. What it produces is a report, and a report is data.
fn report_backup_plan(set: &IndexSet, plan: &BackupPlan, fresh: bool) -> std::io::Result<()> {
    tracing::info!(name = %set.base().display(), "dry run: nothing was written");

    say!(
        "dry run: version {} would be committed, snapshot {}",
        plan.version,
        plan.snapshot
    )?;
    say!(
        "dry run: {} files, {} dirs, {} links, {} chunks ({} new, {} already held), \
         {} bytes to store",
        plan.files,
        plan.dirs,
        plan.links,
        plan.chunks,
        plan.fresh,
        plan.held,
        plan.bytes
    )?;

    if fresh {
        say!(
            "dry run: there is no backup set at {} yet, so this would create it",
            set.base().display()
        )?;
    }

    Ok(())
}

/// Rebuild a tree from the set at `name` into `destination`.
///
/// A dry run reads the version and checks it exactly as a restore does. Then it
/// stops. Nothing is created. There is no file, no directory, and no attribute.
/// There is no `.partial` beside the destination. Nothing is removed, which
/// under `--clean` is the part worth being sure about. No claim is taken either,
/// so a plan can be taken against a destination that a restore is using now.
async fn run_restore(
    destination: &std::path::Path,
    name: &std::path::Path,
    version: Option<u32>,
    clean: bool,
    dry_run: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::open_for_reading(set.base()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        destination = %destination.display(),
        clean,
        dry_run,
        "restoring"
    );

    // Checked before the claim is taken. So a destination that can never be
    // cleaned reports why before anything is created for it. The removal itself
    // happens after, so two runs cannot both decide to empty it.
    if clean {
        check_clean_destination(destination, &set)?;
    }

    // Before the claim rather than instead of it. A dry run takes no claim,
    // and this is where the claim would be taken.
    if dry_run {
        return report_restore_plan(
            version,
            destination,
            clean,
            restore_plan(snapshot, store).await?,
        );
    }

    // Held for the whole run, so two restores into one destination cannot
    // interleave writes into the same tree.
    let claim = lock_destination(destination)?;

    if clean {
        restore_atomically(&claim, snapshot, store).await?;
    } else {
        let restored = restore(destination, snapshot, store).await?;
        report_restored(&restored);

        // A merging restore cannot say which extra files are there. The snapshot
        // is inside the library call by then. So the restore says the one thing
        // that is both true and actionable.
        tracing::info!(
            "the destination may also hold files that are not in this snapshot; \
             pass --clean to restore into an exact copy"
        );
    }

    Ok(())
}

/// Restore into a sibling directory, then swap it in. So a run that dies part-way
/// leaves the destination exactly as it was. The destination does not hold a
/// prefix of the snapshot. This costs the disk space for two trees at once.
///
/// The function takes the claim rather than the path. The claim resolves
/// "beside the destination", so the caller must ask the claim. This used to
/// re-derive the rule with its own `sibling`, which did not canonicalise. So a
/// restore through a symlink staged the tree beside the *link*. The claim's
/// cleanup removed things beside the *target*. A failed run then left a
/// half-written tree behind that the claim could not reach. There is now one
/// definition, and it is the claim's.
async fn restore_atomically<S: storage::StorageGet + 'static>(
    claim: &lepatch::command::DestinationLock,
    snapshot: storage::BlobId,
    store: S,
) -> std::io::Result<()> {
    let destination = claim.resolved();
    let partial = claim.scratch_dir(PARTIAL_SUFFIX);
    let previous = claim.scratch_dir(PREVIOUS_SUFFIX);

    // Either a half-written restore that never became the destination, or a
    // superseded tree. Both are safe to clear.
    for path in [&partial, &previous] {
        if path.exists() {
            tracing::warn!(
                path = %path.display(),
                "removing a directory left by an earlier restore"
            );
            storage::at(
                path,
                "remove the directory left by an earlier restore",
                std::fs::remove_dir_all(path),
            )?;
        }
    }

    let restored = match restore(&partial, snapshot, store).await {
        Ok(restored) => restored,
        Err(e) => {
            // The destination was never touched, so there is nothing to put back.
            let _ = std::fs::remove_dir_all(&partial);
            tracing::error!(
                destination = %destination.display(),
                "the restore failed; the destination was left as it was"
            );
            return Err(e);
        }
    };

    // Moved aside rather than deleted first, so a failure here can put it back.
    let had_previous = destination.exists();
    if had_previous {
        storage::at(
            &previous,
            "move the old tree aside",
            std::fs::rename(destination, &previous),
        )?;
    }

    if let Err(e) = std::fs::rename(&partial, destination) {
        if had_previous {
            // Best effort. The old tree is on disk either way, and saying where
            // is more use than a second failure.
            let restored_back = std::fs::rename(&previous, destination);
            tracing::error!(
                destination = %destination.display(),
                previous = %previous.display(),
                "could not move the restored tree into place ({e}) and \
                 could not put the previous one back ({restored_back:?})"
            );
        }
        return Err(e);
    }

    if had_previous {
        storage::at(
            &previous,
            "remove the superseded tree",
            std::fs::remove_dir_all(&previous),
        )?;
    }

    report_restored(&restored);
    Ok(())
}

/// Say what a restore would have written, on stdout, and that it wrote nothing.
fn report_restore_plan(
    version: u32,
    destination: &std::path::Path,
    clean: bool,
    plan: RestorePlan,
) -> std::io::Result<()> {
    tracing::info!(destination = %destination.display(), "dry run: nothing was written");

    say!("dry run: version {version} into {}", destination.display())?;
    say!(
        "dry run: {} files, {} dirs, {} links, {} chunks, {} bytes would be written",
        plan.files,
        plan.dirs,
        plan.links,
        plan.chunks,
        plan.bytes
    )?;

    // Both forms say what would happen to everything that the snapshot does not
    // record. Under `--clean` that is the whole of the destructive part. A plan
    // that did not mention it would be the least useful thing it could do.
    if clean {
        say!(
            "dry run: {} would be emptied first, and the tree assembled beside it",
            destination.display()
        )?;
    } else {
        say!(
            "dry run: anything already in {} would be left alone; pass --clean for an \
             exact copy",
            destination.display()
        )?;
    }

    if plan.unowned > 0 {
        // The figure is a bound, not a count. It is how many entries record an
        // owner at all. Only trying to set one says whether this process can.
        // Claiming either outcome would be a guess.
        eprintln!(
            "note: {} entr{} in this version record an owner, which a restore may not be \
             able to set without privilege",
            plan.unowned,
            if plan.unowned == 1 { "y" } else { "ies" },
        );
    }

    Ok(())
}

/// Log what a restore put into the destination.
fn report_restored(restored: &lepatch::command::Restored) {
    tracing::info!(
        files = restored.files,
        dirs = restored.dirs,
        links = restored.links,
        chunks = restored.chunks,
        "restore complete"
    );

    if restored.unowned > 0 {
        report_unowned(restored.unowned, "restore");
    }
}

/// Say that some entries could not be given the owner the version records.
///
/// On stderr, and unconditionally. The alternative is a tree that looks complete
/// and has the wrong owner on part of it. A restore of a system tree as an
/// unprivileged user always hits this. It is not an edge case to be phrased as
/// one. So the wording says what is true, and what would fix it. The files are
/// right, and the owners are not. The wording does not lead with a failure that
/// the run did not have.
fn report_unowned(count: u64, what: &str) {
    eprintln!(
        "note: {count} entr{} in this {what} recorded an owner this process could not set, \
         which takes privilege; their contents, permissions and times are correct. \
         Re-run as root to set them",
        if count == 1 { "y" } else { "ies" },
    );
}

/// Check that a destination is safe to empty, without touching it.
///
/// Paths are compared absolute. A relative store path never "starts with" a
/// relative destination, so the check would silently never fire. The check
/// refuses an empty path. It also refuses a destination that holds the set being
/// restored from, `$HOME`, the working directory, and the filesystem root.
fn check_clean_destination(destination: &std::path::Path, set: &IndexSet) -> std::io::Result<()> {
    if destination.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to clean an empty destination path",
        ));
    }

    let target = absolutise(destination);

    // Every segment, not just the first. A set that has rolled over holds the
    // rest of the backup in files that a clean restore would delete, while leaving
    // segment 0 behind.
    let segments = set.segments()?;
    for segment in &segments {
        let segment = absolutise(segment);
        if !segment.starts_with(&target) {
            continue;
        }

        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to clean {}: it holds the backup set being restored from ({})",
                target.display(),
                segment.display()
            ),
        ));
    }

    // The segment that a run would write next. It is not yet on disk. It is
    // part of the set, and a run creates it inside the destination.
    if absolutise(&set.segment_path(u32::try_from(segments.len()).unwrap_or(u32::MAX)))
        .starts_with(&target)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to clean {}: the backup set being restored from is written there \
                 ({} and following)",
                target.display(),
                set.base().display()
            ),
        ));
    }

    // None of these is ever a destination somebody meant, and all are
    // unrecoverable if emptied.
    if let Some(home) = std::env::var_os("HOME").map(std::path::PathBuf::from)
        && target == absolutise(&home)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to clean your home directory ({})",
                target.display()
            ),
        ));
    }

    if let Ok(cwd) = std::env::current_dir()
        && target == absolutise(&cwd)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to clean the working directory ({})",
                target.display()
            ),
        ));
    }

    // Checked before the claim is taken. Otherwise `/` would be refused for
    // having no sibling to put a claim beside.
    if target.parent().is_none() || target.components().count() <= 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to clean the filesystem root ({})",
                target.display()
            ),
        ));
    }

    Ok(())
}

/// Resolve a path to an absolute one, without requiring it to exist.
fn absolutise(path: &std::path::Path) -> std::path::PathBuf {
    if let Ok(resolved) = std::fs::canonicalize(path) {
        return resolved;
    }

    if path.is_absolute() {
        return path.to_path_buf();
    }

    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path.to_path_buf(),
    }
}

/// Check a version's chunks against their recorded hashes.
async fn run_verify(name: &std::path::Path, version: Option<u32>) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::open_for_reading(set.base()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(name = %set.base().display(), version, "verifying");

    let report = verify(snapshot, store).await?;

    for failure in &report.failures {
        eprintln!("error: {failure}");
    }

    say!(
        "checked {} chunks ({} bytes) in version {version}: {}",
        report.chunks,
        report.bytes,
        if report.is_ok() {
            "all chunks match their recorded hashes".to_string()
        } else {
            format!("{} chunks FAILED", report.failures.len())
        }
    )?;

    // After the summary, and not before it. The exit code is this line. A
    // summary that could terminate the process first would be able to answer
    // for it.
    report.into_result()
}

/// The base version a pack should omit, with `--full` overriding `--from`.
///
/// Two flags that contradict each other. The code resolves the conflict here
/// rather than in clap, so the message is ours. `--full` is the promise that the
/// artifact suits any client. Letting a stale `--from` in a script quietly narrow
/// that promise would break it without anyone noticing.
fn effective_from(from: Option<u32>, full: bool) -> Option<u32> {
    if full {
        if let Some(from) = from {
            say!("note: --full overrides --from {from}; packing the whole version").ok();
        }
        return None;
    }
    from
}

/// Write one version's chunks to a single file.
///
/// Opened for reading, and no lock taken. A pack does not touch the set. The
/// version is resolved before the plan is built. So a set that grows under the
/// run packs the version that was asked for, not whatever is newest when the run
/// finishes.
///
/// A dry run builds the same plan and stops before the file is touched. So the
/// run neither creates nor overwrites `--out`. The size on disk and the content
/// hash are absent from a plan rather than guessed. The size depends on the
/// header and the chunk index. The hash is the hash of the file. A file that
/// this run does not write is the only thing either figure could be about.
async fn run_pack(
    name: &std::path::Path,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::open_for_reading(set.base()).await?;
    let (version, snapshot_id) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        from,
        out = %out.display(),
        compression = ?compression,
        dry_run,
        "packing"
    );

    // The decoded snapshot is needed on both paths. The plan is resolved
    // from it rather than from the store's records. A dry run that counted
    // records would be counting the wrong thing.
    let snapshot = load_snapshot(&store, snapshot_id).await?;

    // A delta is the same pack with fewer chunks in it. The base that it
    // assumes is part of what has to be worked out before the contents are. The
    // base decides which chunks survive. It is also what an apply later checks
    // the destination against.
    let base = match from {
        None => None,
        Some(base_version) => {
            let (base_version, base_id) = select_version(&set, &store, Some(base_version))?;
            Some(lepatch::command::Base {
                version: base_version,
                id: base_id,
                snapshot: load_snapshot(&store, base_id).await?,
            })
        }
    };

    let packed = lepatch::command::pack(
        &store,
        &lepatch::command::Version {
            number: version,
            snapshot,
        },
        |id| store.locate(id),
        base,
        out,
        lepatch::command::PackOptions {
            dry_run,
            compression: compression.pack_policy(),
        },
    )
    .await?;

    // A plan reports and stops. It has read the version and every chunk that
    // it would write. So everything after this point on the real path would only
    // say what the plan has already worked out.
    if dry_run {
        report_pack(version, out, &packed)?;
        return Ok(());
    }

    // Matched rather than unwrapped. The pair belongs to a written pack,
    // and this line is only reachable for one.
    let (Some(bytes), Some(content)) = (packed.bytes, packed.content) else {
        return Err(std::io::Error::other(
            "the pack was written but reported neither a length nor a content hash",
        ));
    };

    tracing::info!(
        version,
        base = packed.base,
        chunks = packed.chunks,
        of = packed.of,
        bytes,
        snapshot = packed.table,
        content = %&content,
        "wrote pack"
    );

    // The content hash is the pack's name, so it goes to stdout for a script to
    // key a cache on.
    say!("{}  {}", &content, out.display())?;
    report_pack(version, out, &packed)?;

    Ok(())
}

/// Say what a pack holds, on stdout.
///
/// Both figures appear for a delta. The fraction depends on the fixture, and
/// the request count does not. The whole version is one range either way.
fn report_pack(
    version: u32,
    out: &std::path::Path,
    packed: &lepatch::command::Packed,
) -> std::io::Result<()> {
    let held = match packed.base {
        Some(base_version) => format!(
            "version {version} since {base_version}: {} of {} chunks",
            packed.chunks, packed.of
        ),
        None => format!("version {version}: {} chunks", packed.chunks),
    };

    match packed.bytes {
        // No file, so no size on disk. Saying "0 bytes on disk" would be a claim
        // about a file that does not exist. So the figure is left out. What did
        // happen is said instead.
        None => say!(
            "dry run: {held}, {} bytes of payload, {} bytes of file table; nothing written \
             to {}",
            packed.payload,
            packed.table,
            out.display()
        ),
        Some(bytes) => say!(
            "{held}, {} bytes of payload, {} bytes of file table, {bytes} bytes on disk",
            packed.payload,
            packed.table
        ),
    }
}

/// Apply `pack` into the set at `name` as a new version.
///
/// Opened for writing, which creates the set when there is not one already. A
/// full pack carries its snapshot and every chunk that it reads. So a set with
/// nothing behind it is not a reason to refuse. That set is the "receive the
/// first version into a new set" case. The lock is taken because the run appends
/// to the store. A store is append-only with one publish point, so two merges
/// interleaving would publish versions over each other. A dry run takes neither,
/// since the claim is a file and opening for writing creates a set. A plan has
/// to be able to run against a set that does not exist yet. The plan answers
/// "everything is new" rather than create the set in order to learn that.
///
/// `compression` is both the write path and the policy that the set is opened
/// with, from one value. See [`KeepOrLevel`]. **A dry run uses only the first**,
/// and reports the same figures either way. A dry run counts chunk content, and
/// content does not depend on how it is stored.
async fn run_merge(
    pack: &std::path::Path,
    name: &std::path::Path,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let pack = Pack::open(pack)?;

    tracing::info!(
        pack = %pack.path().display(),
        name = %set.base().display(),
        chunks = pack.len(),
        base = ?pack.base(),
        compression = ?compression,
        dry_run,
        "merging pack"
    );

    if dry_run {
        // Read-only, and only if the set is there. Opening for writing to find
        // out would create the set. That is the one thing a dry run must never
        // do. So a full pack into a name with nothing behind it plans as an
        // all-new version. The plan does not fail, and it does not create the
        // set in order to succeed.
        let existing = if set.segments()?.is_empty() {
            None
        } else {
            Some(storage::BlobStore::open_for_reading(set.base()).await?)
        };
        let fresh = existing.is_none();

        let planned = merge_plan(&pack, existing.as_ref()).await?;

        // Only when there is going to be a merge to warn about. A pack that the
        // set already holds publishes nothing. So telling the user what version
        // it would publish is telling them about a version that will not exist.
        // The report one line later is going to say so.
        if let (Some(store), Planned::Merge(_)) = (&existing, &planned) {
            warn_behind_tip(&set, &pack, store)?;
        }

        return report_merge_plan(&set, &planned, fresh);
    }

    // Checked before the lock and before the open for writing, because both of
    // those create. A delta that is refused for a set that does not hold its
    // base must leave the name exactly as it was. Opening for writing is how a
    // merge into a new set would create one. The check is repeated inside
    // `merge`, against the store that the run actually writes to. That store is
    // the one that counts, and the only one taken under the lock.
    //
    // So this check is for the name with nothing behind it. There is no store
    // to ask there, and the refusal has to come from here. `None` cannot hold
    // the base by anything. The open below would have made a set to refuse.
    // Where a set *is* there, asking it here would be a read open. A reader does
    // not repair. So a set left on a torn tail by a killed run would be refused
    // by the one operation that could put it right.
    if let Some(base) = pack.base()
        && set.segments()?.is_empty()
    {
        lepatch::command::establish_base::<storage::Write, storage::Compression>(
            None,
            base,
            pack.path(),
        )?;
    }

    let _lock = set.lock()?;
    let store = storage::BlobStore::open_for_writing(
        set.base(),
        storage::SegmentLimit::default(),
        compression.policy(),
    )
    .await?;

    // Before the already-merged answer below, because that answer is about
    // the set and this is about the pack. A pre-check that reports "already
    // merged" from a pack it has not verified is answering for a file that
    // neither `merge` nor `merge_plan` would accept. So a damaged pack whose
    // header names a snapshot that the set holds would succeed here. The dry run
    // would refuse it. The two paths must not disagree about one file.
    //
    // [`Pack::snapshot`] rather than a check of our own. It is the same
    // verification that `merge` and `merge_plan` start with. It hashes the
    // section against the hash that its header names. The repo would rather have
    // one answer to "does this file table hash to this snapshot" than two that
    // agree today.
    let _ = pack.snapshot()?;

    // Before the warning and before the merge, and before the merge on both
    // counts. The warning says what a merge would publish. A pack that the set
    // already holds publishes nothing. `merge` asks again, under the lock and
    // against the store that this run writes to. That is the check that counts.
    // This check is what lets the report be right.
    if let Some(version) = version_holding(&store, &pack.snapshot_id())? {
        tracing::info!(
            name = %set.base().display(),
            version,
            "merge: already merged, nothing to do"
        );

        return report_already_merged(&set, version);
    }

    // Before the merge and before the commit. So the tip is the one that the
    // merge publishes onto, not one that has already counted this version.
    // Taken under the lock with everything else that reads the set's commit
    // list.
    warn_behind_tip(&set, &pack, &store)?;

    let merged = merge(&store, &pack, compression.route()).await?;

    match merged {
        Merged::Committed { version, plan } => {
            tracing::info!(
                name = %set.base().display(),
                version,
                snapshot = %&plan.snapshot,
                base = plan.base,
                chunks = plan.chunks,
                fresh = plan.fresh,
                held = plan.held,
                bytes = plan.bytes,
                segments = store.segment_count(),
                "merge complete"
            );

            report_merge(&set, version, &plan, &pack)
        }
        Merged::Already { version } => {
            tracing::info!(
                name = %set.base().display(),
                version,
                "merge: already merged, nothing to do"
            );

            report_already_merged(&set, version)
        }
    }
}

/// Say on stderr that the pack was built from a version behind the set's tip.
///
/// The one thing the pack's recorded version is for. The two numbers use
/// different sets' numbering, and comparing them is exactly the point. The tip is
/// *this* set's most recent version. The pack's number is where its version sat
/// in the set that made it. So the message names both numbers, and it says what
/// the merge is about to do instead. "This pack is version 1" on its own reads
/// as a complaint about the pack. It does not read as a fact about a sender that
/// is behind.
///
/// A warning and not a refusal. The pack's chunks are the chunks that its own
/// snapshot names. Merging them produces a version that restores exactly what
/// the pack holds. What is stale is the sender's *view*, not the artifact.
/// Refusing would refuse a file that is not wrong.
///
/// On stderr because stdout is the report channel. A script reading the merge
/// report must not have to skip a line to find the line it wanted.
fn warn_behind_tip<W: storage::WriteMode, C: storage::Compressor>(
    set: &IndexSet,
    pack: &Pack,
    store: &storage::BlobStore<W, C>,
) -> std::io::Result<()> {
    let versions = store.versions()?;
    // Bounded by the call above, which is why the `try_from` cannot fail here.
    // `versions` returns an error rather than a count that it cannot represent.
    // So a store with more versions than `u32` has already been refused by the
    // `?` on the line above. The fallback is dead code kept for the type. It is
    // not a second answer to a case that it once handled. An uncountable set is
    // an error to propagate, not a set with a tip at the maximum.
    //
    // `try_from` rather than `as u32`. The lint `clippy::pedantic`'s
    // `cast_possible_truncation` denies `as u32`.
    let tip = u32::try_from(versions.len()).unwrap_or(u32::MAX);
    let from = pack.version();

    // Strictly below. A pack from the tip's own version means a sender that
    // is exactly up to date. That is the ordinary case, and it is not worth a
    // word.
    if from >= tip {
        return Ok(());
    }

    eprintln!(
        "warning: this pack is version {from} and {} is at version {tip}; merging publishes it \
         as version {}, and a restore with no --version will return that one",
        set.base().display(),
        tip.saturating_add(1),
    );

    Ok(())
}

/// Report a pack the set already holds, and that nothing was done.
///
/// On stdout, because it is the run's answer. A script that pushed a pack and
/// then checked the exit code is entitled to know why the set did not move. The
/// message names the version that the set holds the pack as. "Already merged" on
/// its own leaves the one question of a retried transfer unanswered. That
/// question is *which* version, so a client can ask for that one by number.
fn report_already_merged(set: &IndexSet, version: u32) -> std::io::Result<()> {
    say!(
        "already merged: {} holds this version as version {version}; nothing to do",
        set.base().display()
    )
}

/// Say what a merge committed, and what it left out.
///
/// Two lines: what went in, and the name that the new version is known by. The
/// name is the same hash that `pack` printed when it was written. A pack is a
/// function of the version that it carries, and both sides hash the same bytes.
///
/// Every number here is *this* set's. The version is the one that the commit
/// record published. The "since N" is the version of this set that holds the
/// pack's base. The code looks that version up by snapshot hash rather than
/// taking it from the pack. That is the only way it can be right. The pack's
/// base is a hash, and a hash says nothing about what this set calls it.
fn report_merge(
    set: &IndexSet,
    version: u32,
    plan: &MergePlan,
    pack: &Pack,
) -> std::io::Result<()> {
    let since = match plan.base {
        Some(base) => format!(" since {base}"),
        None => String::new(),
    };

    say!(
        "merged version {}{since}: {} chunks, {} new, {} already held, {} bytes stored from {}",
        version,
        plan.chunks,
        plan.fresh,
        plan.held,
        plan.bytes,
        pack.path().display()
    )?;
    say!("{}  {}", &plan.snapshot, set.base().display())
}

/// Say what a merge would have committed, on stdout, and that it committed
/// nothing.
///
/// The same wording as the real report, with the verbs changed. The reason is
/// the one that `report_applied` gives. The real run's line is what a script
/// parses. Interpolating "would" into that line would change bytes that nobody
/// asked to change.
///
/// A pack that the set already holds is reported exactly as the real run would
/// report it, with "dry run" nowhere in the line. There is nothing to plan. The
/// real run writes nothing either. So a dry-run-prefixed version of the answer
/// would be the more qualified of two statements about the same event. The
/// qualified one is the less true.
fn report_merge_plan(set: &IndexSet, planned: &Planned, fresh: bool) -> std::io::Result<()> {
    tracing::info!(name = %set.base().display(), "dry run: nothing was written");

    let plan = match planned {
        Planned::Already { version } => {
            return report_already_merged(set, *version);
        }
        Planned::Merge(plan) => plan,
    };

    let since = match plan.base {
        Some(base) => format!(" since {base}"),
        None => String::new(),
    };

    say!(
        "dry run: version {}{since}: {} chunks, {} new, {} already held, {} bytes to store, \
         nothing written to {}",
        plan.version,
        plan.chunks,
        plan.fresh,
        plan.held,
        plan.bytes,
        set.base().display()
    )?;
    say!("dry run: {} would be the snapshot", &plan.snapshot)?;

    if fresh {
        say!(
            "dry run: there is no backup set at {} yet, so this would create it",
            set.base().display()
        )?;
    }

    Ok(())
}

/// Bring `destination` up to the version `pack` carries.
///
/// The destination is claimed for the length of the run, on the same terms that
/// a restore claims one. Two applies into one directory would interleave. The
/// claim is a file *beside* the directory, so it never appears in the result.
///
/// A dry run takes no claim and writes nothing. It opens no file, prunes no
/// entry, and records no manifest. It has already probed the destination against
/// the version, and read back every chunk that it was about to write.
async fn run_apply(
    pack: &std::path::Path,
    destination: &std::path::Path,
    prune: bool,
    recheck: bool,
    dry_run: bool,
) -> std::io::Result<()> {
    let _claim = if dry_run {
        None
    } else {
        Some(lock_destination(destination)?)
    };

    let pack = Pack::open(pack)?;
    tracing::info!(
        pack = %pack.path().display(),
        destination = %destination.display(),
        chunks = pack.len(),
        dry_run,
        "applying pack"
    );

    let applied = apply(
        destination,
        &pack,
        ApplyOptions {
            prune,
            recheck,
            dry_run,
        },
    )
    .await?;

    tracing::info!(
        held = applied.held,
        files = applied.files,
        written = applied.written,
        bytes = applied.bytes,
        removed = applied.removed,
        dry_run,
        "applied pack"
    );

    report_applied(&applied, &pack, destination, dry_run)
}

/// Say what an apply changed, or what it would have changed.
///
/// Two wordings rather than one with the verbs filled in. The real run's
/// line is what a script parses. Interpolating into that line would change bytes
/// that nobody asked to change.
fn report_applied(
    applied: &lepatch::command::Applied,
    pack: &Pack,
    destination: &std::path::Path,
    dry_run: bool,
) -> std::io::Result<()> {
    // The bytes a fetch would have needed. That figure says whether this
    // was an update or a whole tree.
    if dry_run {
        say!(
            "dry run: would fetch {} of {} bytes in {} range(s); {} of {} chunks are already \
             held, {} chunks would be written into {} file(s)",
            applied.fetched,
            pack.payload_len(),
            applied.ranges,
            applied.held,
            applied.chunks,
            applied.written,
            applied.files,
        )?;
    } else {
        say!(
            "fetched {} of {} bytes in {} range(s); {} of {} chunks were already held, \
             {} chunks written into {} file(s)",
            applied.fetched,
            pack.payload_len(),
            applied.ranges,
            applied.held,
            applied.chunks,
            applied.written,
            applied.files,
        )?;
    }

    // Named on stderr, because stdout is data. Entries are left in place unless
    // `--prune`. The run says so either way, because silence here would read as
    // "nothing there".
    if applied.extra > 0 {
        let what = match (dry_run, applied.removed) {
            (true, 0) => "would be left alone; pass --prune to remove them",
            (true, _) => "would be removed by --prune",
            (false, _) => "left alone; pass --prune to remove them",
        };
        eprintln!(
            "note: {} unrecorded entr{} under {} {what}",
            applied.extra,
            if applied.extra == 1 { "y" } else { "ies" },
            destination.display(),
        );
    }

    if applied.removed > 0 {
        say!(
            "{} {} unrecorded entr{}",
            if dry_run { "would remove" } else { "removed" },
            applied.removed,
            if applied.removed == 1 { "y" } else { "ies" }
        )?;
    }

    if applied.unowned > 0 {
        if dry_run {
            // The figure is a bound, not a count. It is how many entries record an
            // owner at all. Only trying to set one says whether this process can.
            // Claiming either outcome would be a guess.
            eprintln!(
                "note: {} entr{} in this version record an owner, which an apply may not be \
                 able to set without privilege",
                applied.unowned,
                if applied.unowned == 1 { "y" } else { "ies" },
            );
        } else {
            report_unowned(applied.unowned, "apply");
        }
    }

    Ok(())
}
