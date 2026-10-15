// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The `blu` command line interface.

use std::process::ExitCode;

use blulib::{
    command::{
        ApplyBuilder, ApplyOptions, BackupBuilder, BackupPlan, BackupPlanBuilder, MergeBuilder,
        MergePlan, MergePlanBuilder, Merged, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, PackBuilder, Planned,
        RestoreBuilder, RestorePlan, RestorePlanBuilder, VerifyBuilder, lock_destination,
        version_holding,
    },
    layout::IndexSet,
    metadata::Timestamp,
    pack::Pack,
    reader, storage,
};
use clap::{Parser, Subcommand};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::{Layer as _, SubscriberExt as _},
    util::SubscriberInitExt as _,
};

#[derive(Debug, Clone, Parser)]
#[command(name = "blu", version, about, long_about = None)]
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

    /// Send this run's spans and metrics to an OTLP collector. Off by default.
    ///
    /// **Nothing leaves the machine unless one of the two asks.** Either this
    /// flag or `OTEL_EXPORTER_OTLP_ENDPOINT` in the environment turns the same
    /// thing on — asking twice is asking once — and the environment spelling is
    /// what a systemd unit or a container image sets. Without either, no
    /// exporter is built, no thread is started for one, no socket is opened, and
    /// `blu serve` does not even wrap its router in a request span.
    ///
    /// What is sent is what the crate already records: one span per `#[instrument]`ed
    /// command, one `http.server` span per request while `serve` is running, one
    /// `blu_job` span per server-side job, the run's totals as attributes and
    /// counters, and a duration per command. Turning this on adds a collector to
    /// the picture and nothing else.
    ///
    /// Where it goes is the exporter's standard resolution:
    /// `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` first, then
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` with `/v1/traces` (or `/v1/metrics`)
    /// appended, then `http://localhost:4318`. Only the base
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` switches telemetry on.
    /// The per-signal variables point an exporter somewhere.
    /// Nothing reads them until something has turned telemetry on.
    ///
    /// The exporter speaks plain HTTP and verified HTTPS.
    /// It verifies HTTPS against the system roots.
    /// It never downgrades HTTPS before plain HTTP.
    /// `blu serve` offers plain HTTP only.
    #[arg(long)]
    telemetry: bool,

    /// Bearer token a proxy guards serve targets with.
    ///
    /// The client forwards it as `Authorization: Bearer <token>`.
    /// The client never checks it.
    /// A proxy before the server checks it.
    /// `BLU_TOKEN` in the environment selects the same token.
    /// The flag wins when both name one.
    /// Leave both unset for a server with no proxy.
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    /// Snapshot a directory into a new version of a backup set.
    ///
    /// The run stamps the time. It refuses a stamp older than the newest
    /// committed version, so a clock behind the tip fails before writing.
    Backup(BackupArgs),
    /// Merge a pack into a backup set, as a new version of it.
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
    ///
    /// The run refuses a pack older than the tip rather than warning about it.
    /// Each version carries its run time, and a new version never predates the
    /// latest one. The run therefore refuses such a pack before writing a byte.
    /// Equal times pass. Two pushes at once race the tip, and the run refuses
    /// the loser with nothing published.
    Merge {
        /// The pack to merge.
        pack: std::path::PathBuf,
        /// The backup set to merge into: a local path (`photos`), a serve
        /// set (`http://127.0.0.1:8480/docs`), or a keyed store
        /// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). Remote targets hold no local copy.
        target: String,
        /// How each chunk the pack carries is stored. `keep` (the default) writes the stored
        /// form the pack already has. `0` writes the content as it is. 1 to 9 decode and store
        /// a zstd frame at that level.
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
        ///
        /// With an `http://` target the server does the merging and opens its
        /// set at this level, so a level sent over HTTP is the level those
        /// records are written at rather than the server's default.
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
        ///
        /// Refused with an `http://` target: the server has no plan-only
        /// merge, so a dry run here would report a plan about a set this run
        /// does not hold.
        #[arg(long)]
        dry_run: bool,
        /// Directory holding the staging copy for a store target.
        ///
        /// One subdirectory per store address; the set name selects the set
        /// inside it. Defaults to the system cache. Refused for local and
        /// `http://` targets: without a store there is no copy to stage.
        #[arg(long, value_name = "DIR")]
        staging: Option<std::path::PathBuf>,
    },
    /// Rebuild the per-version index files from the store's commit records.
    ///
    /// They are a cache: nothing is lost if they are missing or wrong.
    Reindex {
        /// The backup set to rebuild: a local path (`photos`) or a keyed
        /// store (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots), pulled into staging first and
        /// pushed back afterwards. An `http://` or `https://` target fails: the index
        /// lives on the server, where no endpoint rebuilds it.
        target: String,
        /// Say how many index files would be written, and write none of them.
        ///
        /// Affects nothing but the writing: the set is still opened and scanned, so
        /// the version count is the store's own. A torn tail is *not* reclaimed —
        /// repairing one is the point of a real reindex, and a dry run repairs
        /// nothing, so a set left by a killed run is refused here and repaired by a
        /// real one — and no lock is taken, because taking one creates a file.
        #[arg(long)]
        dry_run: bool,
        /// Directory holding the staging copy for a store target.
        ///
        /// One subdirectory per store address; the set name selects the set
        /// inside it. Defaults to the system cache. Refused for local targets:
        /// without a store there is no copy to stage.
        #[arg(long, value_name = "DIR")]
        staging: Option<std::path::PathBuf>,
    },
    /// List the versions a backup set holds, oldest first.
    ///
    /// The count is a lower bound: a version whose index file was never written
    /// still exists and still restores, so this can under-report and never
    /// over-reports.
    ///
    /// **The same listing is printed whether the set is local or remote.**
    /// The lines are the same lines: `{version}  {snapshot}  {taken_at}`, or just
    /// `{version}` under `--numbers-only`. The time is local time in RFC 3339 form.
    /// The store keeps UTC. A script written against a local set
    /// works unchanged against a remote one.
    ///
    /// With `--files`, this reports a different listing instead: the versions
    /// where the named paths changed, oldest first, one line per change as
    /// `{version}  {status}  {path}` where the status is `created`, `modified`
    /// or `deleted`. Versions where none of them changed are skipped.
    Versions {
        /// The backup set to inspect: a local path (`photos`), a serve set
        /// (`http://127.0.0.1:8480/docs`), or a keyed store
        /// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). Remote targets hold no local copy.
        target: String,
        /// Print just the version numbers, one per line, and nothing else.
        ///
        /// With `--files`, only the versions where a named path changed.
        #[arg(long)]
        numbers_only: bool,
        /// Only report the versions where these paths changed.
        ///
        /// Paths are relative to the backup root, as the snapshot records them
        /// (`src/main.rs`). A value holding `*`, `?`, `[`, `\` or `{` is a glob
        /// pattern: `*` spans one path level, `**` spans nested levels, and a
        /// backslash names the next byte literally. Quote patterns so the shell
        /// does not expand them first. A value naming a directory matches
        /// everything under it. A backslash escapes a literal metacharacter.
        #[arg(long, value_name = "PATH")]
        files: Vec<std::path::PathBuf>,
        /// Directory holding the staging copy for a store target.
        ///
        /// One subdirectory per store address; the set name selects the set
        /// inside it. Defaults to the system cache. Refused for local and
        /// `http://` targets: without a store there is no copy to stage.
        #[arg(long, value_name = "DIR")]
        staging: Option<std::path::PathBuf>,
    },
    /// Serve the backup sets under a directory over HTTP.
    ///
    /// **The server moves packs and nothing else.** It never takes a path on its
    /// own disk: there is no endpoint that snapshots a server-local tree and none
    /// that restores into one. A remote backup is a client running `blu
    /// backup` and `blu pack` locally, uploading the pack, and this server
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
    /// The API is described by `openapi/blu.json`, served at
    /// `/openapi.json`. `blu serve` is a `pack` on one end and a `merge` on
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
        /// A merge reads a pack **twice** — once to read the file table and once
        /// per chunk — so an upload has to land on a seekable file rather than be
        /// streamed. Defaults to `.blu-serve` under the root, rather than a
        /// system temporary directory: a pack can be larger than one is sized for,
        /// and these files exist to move data the sets hold.
        #[arg(long, value_name = "DIR")]
        scratch: Option<std::path::PathBuf>,
    },
    /// Rebuild a directory from a backup set.
    Restore {
        /// Directory to restore into.
        destination: std::path::PathBuf,
        /// The backup set to restore from: a local path (`photos`), a serve
        /// set (`http://127.0.0.1:8480/docs`), or a keyed store
        /// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). Remote targets hold no local copy:
        /// an `http://` restore fetches a pack over the socket and applies
        /// it, and a `file://` restore pulls into staging first. Reads never
        /// push back.
        target: String,
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
        /// of the set and every chunk still verified against the hash it is
        /// addressed by, so a dry run refuses exactly what a restore refuses and
        /// costs what one costs. Nothing is created and nothing is removed — no
        /// file, no directory, no attribute, no `.partial` beside the destination —
        /// and no claim is taken there, which is what lets a plan be taken against
        /// a destination another restore is using.
        ///
        /// With an `http://` target the pack is still fetched: the report has
        /// to come from the same probe an apply would take, and that probe
        /// reads the pack.
        #[arg(long)]
        dry_run: bool,
        /// Only restore these paths, leaving everything else alone.
        ///
        /// Paths are relative to the backup root, as the snapshot records them.
        /// The same dialect `versions --files` reads: exact paths, directory
        /// prefixes, and glob patterns with `*`, `**`, `?`, `[...]` and
        /// backslash escapes. Parent directories are created as scaffolding.
        /// Refused with `--clean`, which promises exactly the snapshot.
        #[arg(long, value_name = "PATH")]
        files: Vec<std::path::PathBuf>,
        /// Directory holding the staging copy for a store target.
        ///
        /// One subdirectory per store address; the set name selects the set
        /// inside it. Defaults to the system cache. Refused for local and
        /// `http://` targets: without a store there is no copy to stage.
        #[arg(long, value_name = "DIR")]
        staging: Option<std::path::PathBuf>,
    },
    /// Verify a snapshot's stored chunks against their recorded hashes.
    Verify {
        /// The backup set to verify: a local path (`photos`) or a keyed store
        /// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). An `http://` or `https://` target fails: the
        /// server exposes versions and packs, not chunk verification.
        target: String,
        /// Version to verify, defaulting to the most recent usable one.
        #[arg(long, short)]
        version: Option<u32>,
        /// Directory holding the staging copy for a store target.
        ///
        /// One subdirectory per store address; the set name selects the set
        /// inside it. Defaults to the system cache. Refused for local targets:
        /// without a store there is no copy to stage.
        #[arg(long, value_name = "DIR")]
        staging: Option<std::path::PathBuf>,
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
    /// number, the one `blu versions` would print beside it. So `pack set
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
        /// The backup set to pack from: a local path (`photos`), a serve set
        /// (`http://127.0.0.1:8480/docs`), or a keyed store
        /// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). Remote targets hold no local copy:
        /// a `file://` set is pulled into staging first, and an `http://`
        /// set is packed on the server and fetched.
        target: String,
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
        /// How each chunk is written into the pack. `keep` (the default) writes the stored
        /// form the set already has. `0` writes the content as it is. 1 to 9 decode and write
        /// a zstd frame at that level.
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
        /// pack's own. `<OUT>` is neither created nor overwritten. The size on disk
        /// and the content hash are not reported, because both are properties of a
        /// file this run does not write.
        ///
        /// Under a level the byte totals are the re-encoded ones, so the plan
        /// reports what that run would write rather than what the set holds: a dry
        /// run of `--compression 9` costs an encode per chunk, and that cost is
        /// what makes its figures the real run's.
        ///
        /// With an `http://` target only `keep` packs: the server serves the
        /// records it holds and re-encodes nothing, so any other `--compression`
        /// is refused rather than silently kept.
        #[arg(long)]
        dry_run: bool,
        /// Directory holding the staging copy for a store target.
        ///
        /// One subdirectory per store address; the set name selects the set
        /// inside it. Defaults to the system cache. Refused for local and
        /// `http://` targets: without a store there is no copy to stage.
        #[arg(long, value_name = "DIR")]
        staging: Option<std::path::PathBuf>,
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
        /// Each apply leaves a record beside the directory saying which files it
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
        /// Only bring these paths up to the version, writing only what is missing.
        ///
        /// Paths are relative to the backup root, as the pack records them.
        /// The same dialect `versions --files` reads: exact paths, directory
        /// prefixes, and glob patterns with `*`, `**`, `?`, `[...]`
        /// and backslash escapes. With `--prune`, only the entries the filter
        /// keeps are eligible for removal.
        #[arg(long, value_name = "PATH")]
        files: Vec<std::path::PathBuf>,
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
    let jobs = blulib::serve::Jobs::new();
    let serving = matches!(args.command, Command::Serve { .. });

    // Telemetry is read once, here, and everything downstream asks this value
    // rather than the environment. See `blulib::telemetry`.
    let telemetry = blulib::telemetry::Config::from_environment(args.telemetry);

    // The bearer token is read once, here, for the same reason.
    // The flag wins when both name one.
    // An empty environment value counts as unset.
    let token = args.token.or_else(|| {
        std::env::var("BLU_TOKEN")
            .ok()
            .filter(|value| !value.is_empty())
    });

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
    let code = match run(args.command, jobs, token).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The message, not the `Debug` form.
            //
            // There is no cause chain to walk. `blu` has one error type,
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
fn init_tracing(
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
/// yet. `blu verify set | true` reported success on a store that had failed
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
async fn run(
    command: Command,
    jobs: blulib::serve::Jobs,
    token: Option<String>,
) -> std::io::Result<()> {
    tokio::select! {
        result = dispatch(command, jobs, token) => result,
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
                "`{other}` is not a size blu knows. Use B, KiB, MiB, GiB or TiB"
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
/// stderr, so `blu serve --bind 127.0.0.1:0 | head -1` is a working way to
/// get the address.
///
/// The server shuts down on the same [`stop_signal`] every other command uses, so
/// Ctrl-C and a container stop both stop it cleanly — and the `Drop` guard on the
/// scratch directory removes whatever uploads were still held.
async fn run_serve(
    root: &std::path::Path,
    bind: Option<&str>,
    scratch: Option<&std::path::Path>,
    jobs: &blulib::serve::Jobs,
) -> std::io::Result<()> {
    let config = blulib::serve::Config::new(root, bind.unwrap_or(DEFAULT_BIND), scratch);

    // Reported before the listener is up, and to stdout, so a caller reading one
    // line knows the port. The address is resolved by actix, which is what makes
    // `--bind 127.0.0.1:0` mean "pick one".
    tracing::info!(
        root = %config.root.display(),
        bind = %config.bind,
        scratch = %config.scratch.display(),
        "serving"
    );

    let running = blulib::serve::bind(&config, jobs)?;
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
/// **The name `blu --help` already prints**, so `blu.command.duration`
/// carries the same word the user typed. A second spelling would be a dashboard
/// split in two by nothing at all.
fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Backup(..) => "backup",
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
async fn dispatch(
    command: Command,
    jobs: blulib::serve::Jobs,
    token: Option<String>,
) -> std::io::Result<()> {
    let name = command_name(&command);
    let started = std::time::Instant::now();
    let outcome = dispatch_inner(command, jobs, token).await;
    blulib::telemetry::metrics::record_command_duration(name, started.elapsed());
    outcome
}

/// One `backup` run: the tree, the set spellings, and the policies.
///
/// One struct because `dispatch_inner` routes commands, and a ten-flag
/// command routed as ten arguments pushes every callee past the argument
/// threshold (see `.clippy.toml`) toward bundling them back at each call
/// site. Clap fills it where the command lands (`#[command(flatten)]` keeps
/// every flag spelled where it was), and [`dispatch_inner`] runs it once.
#[derive(Debug, Clone, clap::Args)]
struct BackupArgs {
    /// Directory to back up.
    source: std::path::PathBuf,
    /// The backup set to write: a local path (`photos`, creating
    /// `photos.0000.bin` and `photos.lock`), a serve set
    /// (`http://127.0.0.1:8480/docs`), or a keyed store
    /// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). Remote targets hold no local copy: a
    /// `file://` set is pulled into staging, backed up there, and pushed
    /// back, and an `http://` set is backed up into temporary staging,
    /// packed, and merged on the server.
    target: String,
    /// How large a segment may get before a new one is started, e.g. `4GiB`.
    ///
    /// Only affects segments written from now on, and never rewrites what is
    /// already there: no record depends on where a boundary fell.
    #[arg(long, value_name = "SIZE", value_parser = parse_size)]
    segment_size: Option<u64>,
    /// How hard to compress record payloads. 0 stores them as they are. 1 (default)
    /// is fastest. 9 is smallest.
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
    /// backup currently holds. A `file://` target is pulled into staging
    /// first, so its plan is against the real set.
    ///
    /// Refused with an `http://` target: the plan would be computed against
    /// empty temporary staging rather than the server's set, so its figures
    /// would be wrong.
    #[arg(long)]
    dry_run: bool,
    /// Directory holding the staging copy for a store target.
    ///
    /// One subdirectory per store address; the set name selects the set
    /// inside it. Defaults to the system cache. Refused for local and
    /// `http://` targets: a local set needs no staging, and an `http://`
    /// backup stages in a temporary directory that is removed afterwards.
    #[arg(long, value_name = "DIR")]
    staging: Option<std::path::PathBuf>,
}

impl BackupArgs {
    /// Run the backup into a local path, a keyed store, or a server.
    async fn run(self, token: Option<String>) -> std::io::Result<()> {
        let compression = compression_policy(self.compression_level);
        let profile = self.chunk_profile.unwrap_or_default();

        match resolve_target(
            "backup",
            &self.target,
            self.staging.as_deref(),
            token.as_deref(),
        )? {
            Target::Local(path) => run_backup(
                &self.source,
                &path,
                self.segment_size,
                compression,
                profile,
                self.dry_run,
            )
            .await
            .map(|_| ()),
            Target::Store(target) => {
                run_backup_stored(
                    &self.source,
                    &target,
                    self.segment_size,
                    compression,
                    profile,
                    self.dry_run,
                )
                .await
            }
            Target::Serve {
                address,
                set,
                token,
            } => {
                if self.dry_run {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "`blu backup --dry-run` to an `http://` target plans against \
                         empty staging rather than the server's set, so its figures would \
                         be wrong. Plan against a local path or a `file://` store instead",
                    ));
                }
                run_backup_serve(
                    &self.source,
                    &address,
                    &set,
                    self.segment_size,
                    compression,
                    profile,
                    token.as_deref(),
                )
                .await
            }
        }
    }
}

/// The commands themselves.
async fn dispatch_inner(
    command: Command,
    jobs: blulib::serve::Jobs,
    token: Option<String>,
) -> std::io::Result<()> {
    match command {
        Command::Backup(args) => args.run(token).await,
        Command::Restore {
            destination,
            target,
            version,
            clean,
            dry_run,
            files,
            staging,
        } => {
            let target = resolve_target("restore", &target, staging.as_deref(), token.as_deref())?;
            dispatch_restore(&destination, target, version, clean, dry_run, files).await
        }
        Command::Verify {
            target,
            version,
            staging,
        } => {
            let target = resolve_target("verify", &target, staging.as_deref(), token.as_deref())?;
            dispatch_verify(target, version).await
        }
        Command::Pack {
            target,
            out,
            version,
            from,
            full,
            compression,
            dry_run,
            staging,
        } => {
            let from = effective_from(from, full);
            let target = resolve_target("pack", &target, staging.as_deref(), token.as_deref())?;
            dispatch_pack(target, &out, version, from, compression, dry_run).await
        }
        Command::Apply {
            pack,
            destination,
            prune,
            recheck,
            dry_run,
            files,
        } => run_apply(&pack, &destination, prune, recheck, dry_run, files).await,
        Command::Merge {
            pack,
            target,
            compression,
            dry_run,
            staging,
        } => {
            let target = resolve_target("merge", &target, staging.as_deref(), token.as_deref())?;
            dispatch_merge_command(pack, target, compression, dry_run).await
        }
        Command::Reindex {
            target,
            dry_run,
            staging,
        } => {
            let target = resolve_target("reindex", &target, staging.as_deref(), token.as_deref())?;
            dispatch_reindex(target, dry_run).await
        }
        Command::Versions {
            target,
            numbers_only,
            files,
            staging,
        } => {
            let target = resolve_target("versions", &target, staging.as_deref(), token.as_deref())?;
            dispatch_versions(target, numbers_only, files).await
        }
        Command::Serve {
            root,
            bind,
            scratch,
        } => run_serve(&root, bind.as_deref(), scratch.as_deref(), &jobs).await,
    }
}

/// Run `restore` against a local path, a keyed store, or a server.
///
/// Split out of [`dispatch_inner`] only to keep that match readable; it holds
/// no work of its own. A `--clean` restore promises exactly the snapshot, so
/// it cannot also restore only part of it: that combination is refused here,
/// before any target is touched.
async fn dispatch_restore(
    destination: &std::path::Path,
    target: Target,
    version: Option<u32>,
    clean: bool,
    dry_run: bool,
    files: Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    if clean && !files.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "`blu restore --clean` writes exactly the snapshot into the destination, and \
             `--files` restores only part of it. Run without `--clean` to merge the named \
             files in, or without `--files` for an exact copy",
        ));
    }
    match target {
        Target::Local(name) => {
            run_restore(destination, &name, version, clean, dry_run, &files).await
        }
        Target::Serve {
            address,
            set,
            token,
        } => {
            run_restore_remote(
                destination,
                &set,
                version,
                clean,
                dry_run,
                &files,
                &address,
                token.as_deref(),
            )
            .await
        }
        Target::Store(target) => {
            run_restore_stored(destination, &target, version, clean, dry_run, &files).await
        }
    }
}

/// Run `verify` against a local path or a keyed store.
///
/// An `http://` or `https://` target fails: the server exposes versions and packs,
/// not chunk verification, so there is nothing here to check one against.
async fn dispatch_verify(target: Target, version: Option<u32>) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_verify(&name, version).await,
        Target::Store(target) => run_verify_stored(&target, version).await,
        Target::Serve { .. } => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "`blu verify` takes a local path or a `file://` target: the server exposes \
             versions and packs, not chunk verification",
        )),
    }
}

/// Run `pack` against a local path, a keyed store, or a server.
async fn dispatch_pack(
    target: Target,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_pack(&name, out, version, from, compression, dry_run).await,
        Target::Store(target) => {
            run_pack_stored(&target, out, version, from, compression, dry_run).await
        }
        Target::Serve {
            address,
            set,
            token,
        } => {
            if compression != KeepOrLevel::Keep {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "an `http://` target packs `keep` only: the server serves the records it \
                     holds and re-encodes nothing",
                ));
            }
            run_pack_serve(
                &address,
                &set,
                out,
                version,
                from,
                dry_run,
                token.as_deref(),
            )
            .await
        }
    }
}

/// Run `merge` against a local path, a keyed store, or a server.
///
/// With an `http://` target the merge happens on the server: the pack is
/// uploaded, the server merges it holding that set's own write lock, and the
/// published version is numbered by the server's set. A dry run is refused
/// there: the server has no plan-only merge.
async fn dispatch_merge_command(
    pack: std::path::PathBuf,
    target: Target,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_merge(&pack, &name, compression, dry_run).await,
        Target::Store(target) => run_merge_stored(&pack, &target, compression, dry_run).await,
        Target::Serve {
            address,
            set,
            token,
        } => {
            if dry_run {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "`blu merge --dry-run` to an `http://` target plans against nothing \
                     this machine holds: the server has no plan-only merge. Plan against a \
                     local path or a `file://` store instead",
                ));
            }
            Push::open(&address, &set, token.as_deref())?
                .pack(&pack, compression)
                .await
        }
    }
}

/// Run `reindex` against a local path or a keyed store.
///
/// An `http://` or `https://` target fails: the index lives on the server, where no
/// endpoint rebuilds it.
async fn dispatch_reindex(target: Target, dry_run: bool) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_reindex(&name, dry_run).await,
        Target::Store(target) => run_reindex_stored(&target, dry_run).await,
        Target::Serve { .. } => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "`blu reindex` takes a local path or a `file://` target: the index lives on \
             the server, where no endpoint rebuilds it",
        )),
    }
}

/// Run `versions` against a local path, a keyed store, or a server.
///
/// Split out of [`dispatch_inner`] only to keep that match readable; the store
/// route pulls into staging first. With `files` this reports file history
/// rather than the version listing, on every target alike.
async fn dispatch_versions(
    target: Target,
    numbers_only: bool,
    files: Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    match target {
        Target::Local(name) => {
            if files.is_empty() {
                run_versions(&name, numbers_only).await
            } else {
                run_history(&name, &files, numbers_only).await
            }
        }
        Target::Serve {
            address,
            set,
            token,
        } => {
            if files.is_empty() {
                run_versions_remote(&set, numbers_only, &address, token.as_deref()).await
            } else {
                run_history_remote(&set, &files, numbers_only, &address, token.as_deref()).await
            }
        }
        Target::Store(target) => {
            if files.is_empty() {
                run_versions_stored(&target, numbers_only).await
            } else {
                run_history_stored(&target, &files, numbers_only).await
            }
        }
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
        "interrupted. This run released its claim on a backup set or destination",
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
        let store = storage::BlobStore::builder(set.base())
            .open_reader()
            .await?;
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

    let store = storage::BlobStore::builder(set.base())
        .open_writer()
        .await?;

    let versions = store.versions()?;
    let written = store.write_index_files().await?;

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

/// List the versions a backup set holds, oldest first, each with its snapshot hash
/// and the local time it ran.
///
/// The count is a **lower bound**. A version whose index file was never
/// written still exists and still restores. So this may report fewer than the set
/// holds, and never more. A set with no index files lists nothing rather than
/// failing. The command `blu reindex` rebuilds the index of such a set. A real
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
    // check and the open would have been swallowed too. Then `blu versions`
    // would exit 0 having printed nothing about a set that was being deleted.
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;

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
        let Ok(taken_at) = store.version_time(version).await else {
            continue;
        };

        print_version(version, &snapshot, taken_at, numbers_only)?;
    }

    Ok(())
}

/// One line of a version listing, on whichever side the set is.
///
/// **One function, so the two listings cannot drift.** A serve listing prints
/// this and prints nothing else, which is what makes a script that reads a local
/// set work unchanged against a remote one: same fields in the same order, the
/// same two-space separator, the same `say!` and so the same broken-pipe
/// behaviour. A second formatting path would be a difference nothing tests until
/// a script meets it.
///
/// The time is local time. The store keeps UTC. Conversion happens here, at the
/// point of display, and nowhere else.
fn print_version(
    version: u32,
    snapshot: &storage::BlobId,
    taken_at: Timestamp,
    numbers_only: bool,
) -> std::io::Result<()> {
    if numbers_only {
        say!("{version}")
    } else {
        say!("{version}  {snapshot}  {}", taken_at.to_local_string())
    }
}

/// List a set held by a `blu serve`, printing what a local listing prints.
///
/// The fetch is [`blulib::client::Remote`]'s.
/// Everything here prints what came back.
/// [`resolve_target`] decided what text is a set name.
/// The last segment of the serve target names the set.
/// Nothing here is a path on this machine.
/// The client forwards `token` when one is set.
async fn run_versions_remote(
    set: &str,
    numbers_only: bool,
    address: &str,
    token: Option<&str>,
) -> std::io::Result<()> {
    let remote = remote_for(address, token)?;

    for listed in remote.versions(set).await? {
        print_version(
            listed.version,
            &listed.snapshot,
            listed.taken_at,
            numbers_only,
        )?;
    }

    Ok(())
}

/// Report the versions where the requested paths changed, oldest first.
///
/// Reads the numbering off the store's own commit records rather than the
/// index files, so a version whose index was never written still reports. A
/// version where none of the paths changed is skipped.
async fn run_history(
    name: &std::path::Path,
    files: &[std::path::PathBuf],
    numbers_only: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // A mistyped name, not an empty set. Answering no events about a name that
    // names nothing would be a failure a script acts on.
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

    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
    let versions = store.versions()?;
    let mut named = Vec::with_capacity(versions.len());
    for version in versions {
        named.push((version, store.snapshot(version)?));
    }

    let events = blulib::command::HistoryBuilder::new(&store)
        .versions(&named)
        .files(files)
        .run()
        .await?;

    print_history(&events, numbers_only)
}

/// Report a keyed store's file history through staging.
///
/// Pulls the set into staging and reports with exactly the code a local run
/// uses, so the lines match a local report field for field. Reads never push
/// back.
async fn run_history_stored(
    target: &StoreTarget,
    files: &[std::path::PathBuf],
    numbers_only: bool,
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_history(&target.staging, files, numbers_only).await
}

/// Report a `blu serve` set's file history, printing what a local report of
/// the same set would.
///
/// The server diffs in one pass and answers the events. Everything here is
/// printing what came back; [`resolve_target`] decided what text is a set name.
async fn run_history_remote(
    set: &str,
    files: &[std::path::PathBuf],
    numbers_only: bool,
    address: &str,
    token: Option<&str>,
) -> std::io::Result<()> {
    let remote = remote_for(address, token)?;
    blulib::client::check_set(set)?;

    let mut named = Vec::with_capacity(files.len());
    for file in files {
        let Some(text) = file.to_str() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "{} is not valid UTF-8. A serve request carries paths as text, so such a \
                     path cannot travel. Report it against a local path or a `file://` store",
                    file.display()
                ),
            ));
        };
        named.push(text);
    }

    let events = remote.history(set, &named).await?;

    print_history(&events, numbers_only)
}

/// One file-history report, on whichever side the set is.
///
/// **One function, so the listings cannot drift.** A remote report prints this
/// and prints nothing else, which is what makes a script that reads a local
/// set work unchanged against a remote one. Under `--numbers-only` each
/// touching version is named once.
fn print_history(events: &[blulib::command::Event], numbers_only: bool) -> std::io::Result<()> {
    if numbers_only {
        let mut shown: Option<u32> = None;
        for event in events {
            if shown != Some(event.version) {
                say!("{}", event.version)?;
                shown = Some(event.version);
            }
        }
        return Ok(());
    }

    for event in events {
        say!(
            "{}  {}  {}",
            event.version,
            event.status.word(),
            event.path.display()
        )?;
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
    fn route(self) -> blulib::command::MergeCompression {
        match self {
            Self::Keep => blulib::command::MergeCompression::Keep,
            Self::Level(_) => blulib::command::MergeCompression::Reencode,
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
///
/// **Returns the version it committed, or `None` for a dry run.** The local path
/// ignores it, which is the whole of the local path's involvement with a remote
/// backup: the number is there for the push that follows, which packs *that*
/// version rather than whichever one is newest when the push gets there.
async fn run_backup(
    source: &std::path::Path,
    name: &std::path::Path,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
    dry_run: bool,
) -> std::io::Result<Option<u32>> {
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

    if dry_run {
        let existing = if segments.is_empty() {
            None
        } else {
            Some(
                storage::BlobStore::builder(set.base())
                    .open_reader()
                    .await?,
            )
        };
        let fresh = existing.is_none();

        let plan = BackupPlanBuilder::new(source)
            .exclude(exclude)
            .chunker(chunker)
            .store(existing)
            .run()
            .await?;
        report_backup_plan(&set, &plan, fresh)?;

        return Ok(None);
    }

    // Opening discards a torn tail in the last segment. A torn tail is an
    // unfinished run, not data.
    let store = storage::BlobStore::builder(set.base())
        .segment_limit(limit)
        .compression(compression)
        .open_writer()
        .await?;

    if let Some(previous) = store.versions()?.last() {
        tracing::debug!(previous, name = %set.base().display(), "deduplicating against the store");
    } else {
        tracing::debug!(
            name = %set.base().display(),
            "no earlier version; this is the first backup of this location"
        );
    }

    let segments = store.segment_count();

    // The stamp the new version must not predate. Read under the lock with
    // everything else that inspects the set's commit list.
    let latest = match store.latest() {
        None => None,
        Some((version, _)) => Some(store.version_time(version).await?),
    };

    let committed = BackupBuilder::new(source, store)
        .exclude(exclude)
        .chunker(chunker)
        .latest(latest)
        .run()
        .await?;

    tracing::info!(
        name = %set.base().display(),
        version = committed.version,
        snapshot = %&committed.snapshot,
        store = %set.base().display(),
        segments,
        "backup complete"
    );

    Ok(Some(committed.version))
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

/// Rebuild a tree from a set on a `blu serve`, into `destination`.
///
/// **The local path's work, unchanged, behind a download.** A local restore
/// reads a version out of a set and writes every chunk of it. This one asks the
/// server to pack the version, fetches the ranges it does not already have into
/// a sparse file, and hands that file to [`apply`] — which is the same call
/// `blu apply` makes, with the same options, and reports through the same
/// [`report_applied`]. There is no second applier here and no second report.
///
/// ## Where `have` comes from
///
/// **From the destination, and only from the destination.** A chunk in `have`
/// is a region this run will not fetch, so it has to be a chunk this machine can
/// read. The one place a pulled tree keeps the chunks of a version is the tree
/// itself, and the one record that says which version a tree is — and that its
/// files are still where that record left them — is the apply manifest beside
/// [`blulib::command::Manifest::path_for`]. So [`have_for`] is the whole of it: every chunk of the
/// version when the destination already is that version, and none of them
/// otherwise.
///
/// Anything less than certainty is the empty set, deliberately. An empty `have`
/// costs the whole payload and is always correct; a `have` naming a chunk the
/// destination cannot produce is a hole in the assembled pack, and the applier
/// would write zeroes for it.
///
/// ## Why the fixed regions are fetched before the plan is asked for
///
/// **`have` is a list of chunk hashes, and the hashes are in the version's file
/// table.** The table is inside the pack, so the first thing to know is which
/// chunks there are. Only the header, the snapshot, the index and the trailer are
/// needed for that, and a pack assembled out of those four opens — that is what
/// [`Pack::open`] reads — so the cost is a couple of small range requests in the
/// one case where it can pay for itself, and nothing at all in the case where
/// `have` was going to be empty regardless.
///
/// ## Why the assembled pack is a temporary file
///
/// **It is derived output, and a pack that failed to assemble is worse than no
/// pack.** It is named by this process and this version, it is removed whether
/// the run succeeded or failed, and nothing outside this run ever looks at it.
/// A pull that fails part-way leaves the destination as it was rather than a
/// tree of zeroes: the bytes are written into the file first and applied after,
/// and [`blulib::client::Pull::open`] refuses a file whose missing records are not the ones
/// [`have_for`] named.
#[expect(
    clippy::too_many_arguments,
    reason = "a restore's own arguments, plus the address"
)]
async fn run_restore_remote(
    destination: &std::path::Path,
    set: &str,
    version: Option<u32>,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
    address: &str,
    token: Option<&str>,
) -> std::io::Result<()> {
    use blulib::client::Outcome;

    let remote = remote_for(address, token)?;
    blulib::client::check_set(set)?;

    // The manifest, once. It is what `have` is built from, and reading it twice
    // would be two chances to answer differently.
    let manifest = blulib::command::Manifest::load(destination)?;

    let started = remote.start_pack(set, version, None).await?;

    let packed = remote
        .await_job_with(&started, |counters| {
            let shown = counters
                .iter()
                .map(|(name, value)| format!("{name} {value}"))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::info!("packing on {}: {shown}", remote.address());
        })
        .await?;

    let Some(Outcome::Packed(summary)) = &packed.result else {
        return Err(std::io::Error::other(format!(
            "job {} finished without saying what it packed",
            packed.id
        )));
    };

    tracing::info!(
        server = %remote.address(),
        set,
        version = summary.version,
        snapshot = %summary.snapshot,
        chunks = summary.chunks,
        length = summary.length,
        destination = %destination.display(),
        clean,
        dry_run,
        "packing a version to restore from"
    );

    let pack_path = std::env::temp_dir().join(format!(
        "blu-pull-{}-{}.pack",
        std::process::id(),
        &summary.snapshot.to_hex().to_string()[..16]
    ));

    // Everything from here on owns that file. Whether the run succeeded or
    // failed, it goes: a pack nobody named is not something to leave in a user's
    // directory, and a run that failed half-way has already said so.
    let restored = pull_and_apply(
        &remote,
        set,
        destination,
        manifest.as_ref(),
        summary,
        &pack_path,
        clean,
        dry_run,
        files,
    )
    .await;

    if let Err(e) = std::fs::remove_file(&pack_path) {
        tracing::debug!(
            pack = %pack_path.display(),
            error = %e,
            "could not remove the pack this pull assembled"
        );
    }

    restored
}

/// Fetch the pack a remote restore needs, apply it, and report.
///
/// Split out of [`run_restore_remote`] because that one is about the temporary
/// file and this one is about the work: what to ask for, what to leave out, and
/// what to do with the answer.
///
/// # Errors
///
/// Returns an error if the pack cannot be planned, fetched or opened, or if the
/// apply refuses the destination.
///
/// Returns an error if the server answers a plan whose length is not the length
/// its own pack summary named, because the file this run assembles would not be
/// the file the server described.
///
/// Returns an error if the destination claim cannot be taken, which includes
/// another run holding it.
#[expect(
    clippy::too_many_arguments,
    reason = "a restore's own arguments, plus the pack"
)]
async fn pull_and_apply(
    remote: &blulib::client::Remote,
    set: &str,
    destination: &std::path::Path,
    manifest: Option<&blulib::command::Manifest>,
    summary: &blulib::client::PackSummary,
    pack_path: &std::path::Path,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
) -> std::io::Result<()> {
    let fixed = fixed_regions(summary);
    let scratch = blulib::client::Pull::new(
        remote,
        set,
        summary.version,
        std::collections::HashSet::new(),
    );

    scratch.begin(summary.length, pack_path).await?;

    // The file table, and with it the chunk hashes `have` is built from — but
    // only where the destination's own record says there is something to save.
    let reads = manifest.filter(|m| m.version == summary.snapshot);
    let mut fetched = blulib::client::Fetched {
        ranges: 0,
        bytes: 0,
        total: 0,
    };
    let mut prefetched = Vec::new();

    let mut have = match reads {
        None => std::collections::HashSet::new(),
        Some(manifest) => {
            let table = scratch.write(&fixed, pack_path, |_| {}).await?;
            fetched.bytes = table.bytes;
            fetched.ranges = table.ranges;
            prefetched = fixed;

            have_for(destination, manifest, &Pack::open(pack_path)?.snapshot()?)
        }
    };

    // With `--files`, the chunks no selected file needs join `have`: the plan
    // then omits their ranges, and the assembled pack holds holes where they
    // would sit. The apply below reads only the selection through the same
    // filter, so those holes are never touched. Where no selection was named
    // this changes nothing.
    have.extend(
        skippable_chunks(
            &scratch,
            pack_path,
            summary,
            !prefetched.is_empty(),
            &mut fetched,
            files,
        )
        .await?,
    );

    let listed: Vec<blulib::storage::BlobId> = have.iter().copied().collect();
    tracing::info!(
        held = listed.len(),
        of = summary.chunks,
        "these chunks are not fetched: the destination holds them or no selected file needs them"
    );

    // Only the ranges actually written count as already there. Where the
    // destination gave no reason to read the file table first, nothing was
    // fetched and the plan has to bring all of it.
    let pull =
        blulib::client::Pull::new(remote, set, summary.version, have).already_fetched(prefetched);
    let plan = remote.plan_pack(set, summary.version, &listed).await?;

    if plan.length != summary.length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} planned a {}-byte pack and then described a {}-byte one. The file \
                 this run assembles would not be the file it fetched",
                remote.address(),
                summary.length,
                plan.length
            ),
        ));
    }

    let moved = pull
        .write(&plan.ranges, pack_path, |moved| {
            tracing::info!(
                server = %remote.address(),
                ranges = fetched.ranges + moved.ranges,
                bytes = fetched.bytes + moved.bytes,
                total = moved.total,
                "fetched"
            );
        })
        .await?;

    fetched.bytes = fetched.bytes.saturating_add(moved.bytes);
    fetched.ranges = fetched.ranges.saturating_add(moved.ranges);

    let pack = pull.open(&plan, pack_path)?;

    apply_pulled(
        destination,
        &pack,
        summary,
        set,
        remote.address(),
        &fetched,
        clean,
        dry_run,
        files,
    )
    .await
}

/// Applies the pack a pull assembled, with the restore's own options.
///
/// Reads only the selection, then reports what moved and what the applier
/// did. Split out of [`pull_and_apply`] so each half reads as one job:
/// fetching the bytes, then writing the tree.
///
/// # Errors
///
/// Returns an error if the destination claim cannot be taken, if the apply
/// refuses the destination, or if the report cannot be written.
#[expect(
    clippy::too_many_arguments,
    reason = "a restore's own arguments, plus the fetch figures"
)]
async fn apply_pulled(
    destination: &std::path::Path,
    pack: &Pack,
    summary: &blulib::client::PackSummary,
    set: &str,
    address: &str,
    fetched: &blulib::client::Fetched,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
) -> std::io::Result<()> {
    // Held for the whole run, as `blu apply` holds it, so two pulls into one
    // destination cannot interleave writes into the same tree.
    let _claim = if dry_run {
        None
    } else {
        Some(lock_destination(destination)?)
    };

    let applied = ApplyBuilder::new(destination, pack)
        .options(ApplyOptions {
            prune: clean,
            recheck: false,
            dry_run,
            files: files.to_vec(),
        })
        .run()
        .await?;

    tracing::info!(
        held = applied.held,
        files = applied.files,
        written = applied.written,
        bytes = applied.bytes,
        removed = applied.removed,
        fetched = fetched.bytes,
        ranges = fetched.ranges,
        dry_run,
        "applied the pack this pull fetched"
    );

    // What moved, and where it came from. The line after it is the applier's own,
    // unchanged, so a script reading an apply report reads this one the same way.
    say!(
        "version {} of {} on {}: fetched {} of {} bytes in {} range(s)",
        summary.version,
        set,
        address,
        fetched.bytes,
        summary.length,
        fetched.ranges,
    )?;

    report_applied(&applied, pack, destination, dry_run)
}

/// The chunks of the pack no selected file needs, for the fetch to skip.
///
/// Reads the file table out of the fixed regions — fetching them first where
/// the destination gave no reason to — and selects the same view the apply
/// below reads. Empty where no selection was named, so a full pull fetches as
/// before.
///
/// # Errors
///
/// Returns an error if the fixed regions cannot be fetched, if the pack does
/// not open, or if a `--files` value is empty, absolute, or holds `..`.
async fn skippable_chunks(
    scratch: &blulib::client::Pull<'_>,
    pack_path: &std::path::Path,
    summary: &blulib::client::PackSummary,
    table_present: bool,
    fetched: &mut blulib::client::Fetched,
    files: &[std::path::PathBuf],
) -> std::io::Result<std::collections::HashSet<blulib::storage::BlobId>> {
    let mut skipped = std::collections::HashSet::new();
    if files.is_empty() {
        return Ok(skipped);
    }

    if !table_present {
        let fixed = fixed_regions(summary);
        let table = scratch.write(&fixed, pack_path, |_| {}).await?;
        fetched.bytes = fetched.bytes.saturating_add(table.bytes);
        fetched.ranges = fetched.ranges.saturating_add(table.ranges);
    }

    let table = Pack::open(pack_path)?.snapshot()?;
    let filter = blulib::command::Filter::for_snapshot(files, &table)?;
    let view = filter.select(&table);
    let needed: std::collections::HashSet<blulib::storage::BlobId> =
        view.chunks.iter().copied().collect();
    skipped.extend(
        table
            .chunks
            .iter()
            .copied()
            .filter(|id| !needed.contains(id)),
    );
    Ok(skipped)
}

/// The chunks a destination holds of `snapshot`, or none of them.
///
/// **The apply manifest is a record of what a tree was, and this is the only
/// question it can answer.** It names one version, and `Manifest::untrusted` says
/// whether every file that version recorded is still where the record left it. A
/// tree that passes both *is* that version, which means it holds every chunk of
/// it — that is what a version is — and the server can be told not to send any of
/// them.
///
/// One file that has moved, one recorded with no modification time to compare,
/// and no manifest at all all give the empty set. So does a manifest for a
/// different version: that tree's chunks are a real thing the server could
/// leave out, but which of them *this* version also needs cannot be worked out
/// without reading the pack, and a wrong `have` is the one failure this protocol
/// cannot survive.
fn have_for(
    destination: &std::path::Path,
    manifest: &blulib::command::Manifest,
    snapshot: &blulib::metadata::Snapshot,
) -> std::collections::HashSet<blulib::storage::BlobId> {
    let moved = manifest.untrusted(destination, snapshot);

    if !moved.is_empty() {
        tracing::debug!(
            moved = moved.len(),
            "the destination has moved since its last apply, so the whole pack is fetched"
        );
        return std::collections::HashSet::new();
    }

    snapshot.chunks.iter().copied().collect()
}

/// The regions of a pack every client has to fetch.
///
/// **The header and the snapshot before the payload, the index and the trailer
/// after it.** A plan is these four plus the records a client lacks, so they are
/// what a client fetching nothing at all is sent — and they are enough on their
/// own to open the file, which is what a client that only wants the version's
/// file table needs.
fn fixed_regions(summary: &blulib::client::PackSummary) -> Vec<blulib::pack::Range> {
    use blulib::pack::Range;

    let head = Range {
        start: 0,
        end: summary.payload_start,
    };
    let tail = Range {
        start: summary.payload_start.saturating_add(summary.payload_bytes),
        end: summary.length,
    };

    [head, tail]
        .into_iter()
        .filter(|range| range.end > range.start)
        .collect()
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
    files: &[std::path::PathBuf],
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
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
            RestorePlanBuilder::new(snapshot, store)
                .files(files)
                .run()
                .await?,
        );
    }

    // Held for the whole run, so two restores into one destination cannot
    // interleave writes into the same tree.
    let claim = lock_destination(destination)?;

    if clean {
        restore_atomically(&claim, snapshot, store).await?;
    } else {
        let restored = RestoreBuilder::new(destination, snapshot, store)
            .files(files)
            .run()
            .await?;
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
    claim: &blulib::command::DestinationLock,
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

    let restored = match RestoreBuilder::new(&partial, snapshot, store).run().await {
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
            "dry run: {} would be emptied first. This run would assemble the tree beside it",
            destination.display()
        )?;
    } else {
        say!(
            "dry run: anything already in {} would be left alone. Pass --clean for an \
             exact copy",
            destination.display()
        )?;
    }

    if plan.unowned > 0 {
        // The figure is a bound, not a count. It is how many entries record an
        // owner at all. Only trying to set one says whether this process can.
        // Claiming either outcome would be a guess.
        eprintln!(
            "note: {} entr{} in this version record an owner. A restore may not be \
             able to set an owner without privilege",
            plan.unowned,
            if plan.unowned == 1 { "y" } else { "ies" },
        );
    }

    Ok(())
}

/// Log what a restore put into the destination.
fn report_restored(restored: &blulib::command::Restored) {
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
        "note: {count} entr{} in this {what} recorded an owner this process could not set. \
         Setting an owner takes privilege. Their contents, permissions and times are correct. \
         Run as root to set them",
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
                "refusing to clean {}. This path holds the backup set being restored \
                 from ({})",
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
                "refusing to clean {}. This run writes the backup set being restored \
                 from there ({} and following)",
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
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(name = %set.base().display(), version, "verifying");

    let report = VerifyBuilder::new(snapshot, store).run().await?;

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
            say!("note: --full overrides --from {from}. This run packs the whole version").ok();
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
/// run neither creates nor overwrites `<OUT>`. The size on disk and the content
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
    let (version, packed) = write_pack(name, out, version, from, compression, dry_run).await?;

    // A plan reports and stops, and it wrote no pack, so there is nothing to
    // push — which is the other half of the refusal above, stated where the
    // run actually stops rather than where the flag is read.
    if dry_run {
        report_pack(version, out, &packed)?;
        return Ok(());
    }

    // Matched rather than unwrapped. The pair belongs to a written pack,
    // and this line is only reachable for one.
    let (Some(bytes), Some(content)) = (packed.bytes, packed.content) else {
        return Err(std::io::Error::other(
            "this run wrote the pack, and the pack reported neither a length nor a content hash",
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
    packed: &blulib::command::Packed,
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
            "dry run: {held}, {} bytes of payload, {} bytes of file table. This run wrote \
             nothing to {}",
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

/// The set a command works on, and where it lives.
///
/// One `TARGET` names a local path (`photos`), a serve set
/// (`http://127.0.0.1:8480/docs` or `https://example.test/docs`), or a keyed store
/// (`file:///mnt/drive/docs` for local. `sftp://`, `ftp://` and `s3://` for network. `s3` accepts `http://` and `https://`. HTTPS verifies against system roots). Anything holding `://` is a remote address.
/// Anything else is a path on this machine.
/// Remote targets hold no local copy.
/// A store target works in staging.
/// An `http://` or `https://` target works on the server.
#[derive(Debug)]
enum Target {
    /// A path on this machine.
    Local(std::path::PathBuf),
    /// A set on a `blu serve`.
    Serve {
        /// The server's address, without the set.
        address: String,
        /// The set's name there.
        set: String,
        /// The bearer token a proxy guards with, if any.
        token: Option<String>,
    },
    /// A set in a keyed store, with its staging copy.
    Store(StoreTarget),
}

/// Resolve one `TARGET` into where the set lives.
///
/// `http://` and `https://` targets split into server and set.
/// Every other `://` target splits into store and set.
/// `--staging` selects the staging directory for a store target.
/// It fails otherwise.
/// Without a store there is no copy to stage.
/// An `http://` or `https://` backup stages in a temp directory.
/// The runner removes the temp directory afterwards.
/// The token travels beside the address, never inside it.
fn resolve_target(
    command: &str,
    raw: &str,
    staging: Option<&std::path::Path>,
    token: Option<&str>,
) -> std::io::Result<Target> {
    if raw.contains("://") {
        if raw.trim().to_ascii_lowercase().starts_with("http") {
            if staging.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "`blu {command} --staging` needs a store target. An `http://` or `https://` target works on the server and stages nothing here"
                    ),
                ));
            }
            let (address, set) = blulib::client::split_serve_target(raw)?;
            return Ok(Target::Serve {
                address,
                set,
                token: token.map(str::to_string),
            });
        }
        let (url, set) = storage::RemoteUrl::split_set(raw)?;
        let root = match staging {
            Some(dir) => dir.to_path_buf(),
            None => storage::default_cache_root().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no cache directory holds a staging copy on this machine: pass `--staging DIR`",
                )
            })?,
        };
        let staging = storage::staging_set_path(&root, &url, &set)?;
        return Ok(Target::Store(StoreTarget { url, set, staging }));
    }
    if staging.is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("`blu {command} --staging` needs a store target. A local path stages nothing"),
        ));
    }
    Ok(Target::Local(std::path::PathBuf::from(raw)))
}

/// Build a serve client for `address`, with a bearer token when one is set.
///
/// The token travels as `Authorization: Bearer <token>`.
/// The client never checks it.
/// A proxy before the server checks it.
///
/// # Errors
///
/// The call fails when `address` is not a server address.
/// The call fails when the token is empty or holds whitespace.
fn remote_for(address: &str, token: Option<&str>) -> std::io::Result<blulib::client::Remote> {
    match token {
        Some(value) => blulib::client::Remote::with_token(address, value),
        None => blulib::client::Remote::new(address),
    }
}

/// A set held by a keyed store, and the staging copy.
///
/// Resolution only.
/// Staged runners pull into staging.
/// They run the local command there.
/// They push back after a write.
/// Nothing else reads these fields.
#[derive(Debug)]
struct StoreTarget {
    /// The store holding the set, without the set.
    url: storage::RemoteUrl,
    /// The set name on that store.
    set: String,
    /// Staging base path on this machine.
    staging: std::path::PathBuf,
}

/// A connected keyed store.
///
/// One type holds both backends because staged runners are generic.
/// The local arm holds a directory.
/// The remote arm holds a network operator.
enum StoreBackend {
    /// A local directory through `file://`.
    Local(storage::LocalDirStore),
    /// An operator over the network.
    Remote(opendal::Operator),
}

impl storage::AsyncObjectStore for StoreBackend {
    async fn keys(&self) -> std::io::Result<Vec<String>> {
        match self {
            Self::Local(backend) => backend.keys().await,
            Self::Remote(operator) => operator.keys().await,
        }
    }

    async fn stat(&self, key: &str) -> std::io::Result<Option<u64>> {
        match self {
            Self::Local(backend) => backend.stat(key).await,
            // Fully qualified because `Operator` holds an inherent `stat`.
            // Method resolution prefers the inherent one.
            Self::Remote(operator) => storage::AsyncObjectStore::stat(operator, key).await,
        }
    }

    async fn read_range(&self, key: &str, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Local(backend) => backend.read_range(key, offset, len).await,
            Self::Remote(operator) => operator.read_range(key, offset, len).await,
        }
    }

    async fn write_bytes(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Local(backend) => backend.write_bytes(key, data).await,
            Self::Remote(operator) => operator.write_bytes(key, data).await,
        }
    }

    async fn import_file(&self, key: &str, path: &std::path::Path) -> std::io::Result<u64> {
        match self {
            Self::Local(backend) => backend.import_file(key, path).await,
            Self::Remote(operator) => operator.import_file(key, path).await,
        }
    }

    async fn export_file(&self, key: &str, path: &std::path::Path) -> std::io::Result<u64> {
        match self {
            Self::Local(backend) => backend.export_file(key, path).await,
            Self::Remote(operator) => operator.export_file(key, path).await,
        }
    }
}

/// Open the backend behind `url`.
///
/// `file://` opens a local directory.
/// Other schemes open a network operator.
/// `s3` accepts `http://` and `https://`.
/// HTTPS verifies against the system roots.
fn connect_store(url: &storage::RemoteUrl) -> std::io::Result<StoreBackend> {
    if url.scheme() == storage::RemoteScheme::File {
        return Ok(StoreBackend::Local(storage::LocalDirStore::new(
            url.location(),
        )));
    }
    connect_remote(url)
}

/// Open a network backend behind `url`.
fn connect_remote(url: &storage::RemoteUrl) -> std::io::Result<StoreBackend> {
    storage::connect(url).map(StoreBackend::Remote)
}

/// Refuse an empty staging copy with the store address.
///
/// A fresh pull from an empty remote leaves staging with no segments.
/// Local commands would then report the cache directory.
/// The user pointed at a URL and a set.
/// The error names those.
fn require_staged_set(staging: &IndexSet, target: &StoreTarget) -> std::io::Result<()> {
    if staging.segments()?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no set {} at {} (staged an empty copy)",
                target.set, target.url
            ),
        ));
    }
    Ok(())
}

/// Back up into a keyed store through staging.
///
/// Pulls the set into staging, backs the tree up there with exactly the code
/// a local run uses, and pushes the result back. A dry run pulls and plans
/// with that same code and pushes nothing back.
async fn run_backup_stored(
    source: &std::path::Path,
    target: &StoreTarget,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
    dry_run: bool,
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;

    let committed = run_backup(
        source,
        &target.staging,
        segment_size,
        compression,
        profile,
        dry_run,
    )
    .await?;
    if dry_run {
        return Ok(());
    }
    let Some(version) = committed else {
        return Err(std::io::Error::other(
            "a backup that stored nothing published no version",
        ));
    };

    let stats = storage::push_to_remote_async(&staging, &backend).await?;
    tracing::info!(
        set = %target.set,
        store = %target.url,
        version,
        pushed_files = stats.pushed_files,
        pushed_bytes = stats.pushed_bytes,
        "backup to store complete"
    );

    Ok(())
}

/// Counter keeping temporary work directories apart within one process.
static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A scratch directory for one pure-remote run, removed afterwards.
fn temp_work_dir(prefix: &str) -> std::path::PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("blu-{prefix}-{}-{n}", std::process::id()))
}

/// Back up into a `blu serve` set, holding no local copy.
///
/// Backs the tree up into temporary staging with exactly the code a local
/// run uses, packs the committed version, and has the server merge it. The
/// staging directory is removed however the run ends: a remote backup leaves
/// nothing behind here.
#[expect(
    clippy::too_many_arguments,
    reason = "a backup's own arguments, plus the address"
)]
async fn run_backup_serve(
    source: &std::path::Path,
    address: &str,
    set: &str,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
    token: Option<&str>,
) -> std::io::Result<()> {
    let push = Push::open(address, set, token)?;
    let work = temp_work_dir("backup");
    std::fs::create_dir_all(&work)?;
    let staging = work.join(set);
    let pack_path = work.join("push.pack");

    let result = async {
        let committed =
            run_backup(source, &staging, segment_size, compression, profile, false).await?;
        let Some(version) = committed else {
            return Err(std::io::Error::other(
                "a backup that stored nothing published no version",
            ));
        };
        // `keep`, and not a level. `backup --compression-level` is the policy
        // the temporary set is opened with — what this machine staged — and it
        // says nothing about how a server should store what arrives. The pack
        // already carries the staged form, and `keep` writes that form rather
        // than decoding and re-encoding every chunk a second time.
        write_pack(
            &staging,
            &pack_path,
            Some(version),
            None,
            KeepOrLevel::Keep,
            false,
        )
        .await?;
        push.pack(&pack_path, KeepOrLevel::Keep).await
    }
    .await;

    let _ = std::fs::remove_dir_all(&work);

    result
}

/// Restore from a keyed store through staging.
///
/// Pulls the set into staging and restores from there with exactly the code
/// a local run uses. Reads never push back.
async fn run_restore_stored(
    destination: &std::path::Path,
    target: &StoreTarget,
    version: Option<u32>,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_restore(destination, &target.staging, version, clean, dry_run, files).await
}

/// List a keyed store through staging.
///
/// Pulls the set into staging and lists it with exactly the code a local run
/// uses, so the lines match a local listing field for field.
async fn run_versions_stored(target: &StoreTarget, numbers_only: bool) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_versions(&target.staging, numbers_only).await
}

/// Pack a version of a keyed store into `out`.
///
/// Pulls the set into staging and packs from there with exactly the code a
/// local run uses. Reads never push back.
async fn run_pack_stored(
    target: &StoreTarget,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_pack(&target.staging, out, version, from, compression, dry_run).await
}

/// Pack a version of a `blu serve` set into `out`.
///
/// The pack is computed on the server and fetched whole: a pack is the one
/// artifact a client fetches in full, so the destination's record saves
/// nothing here.
///
/// A dry run reports the server's summary and fetches nothing: the counts
/// are the pack's own, and the size on disk and the content hash are left
/// out as properties of a file this run does not write.
#[expect(
    clippy::too_many_arguments,
    reason = "a pack's own arguments, plus the address"
)]
async fn run_pack_serve(
    address: &str,
    set: &str,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    dry_run: bool,
    token: Option<&str>,
) -> std::io::Result<()> {
    let remote = remote_for(address, token)?;
    blulib::client::check_set(set)?;

    let started = remote.start_pack(set, version, from).await?;
    let packed = remote
        .await_job_with(&started, |counters| {
            let shown = counters
                .iter()
                .map(|(name, value)| format!("{name} {value}"))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::info!("packing on {}: {shown}", remote.address());
        })
        .await?;

    let Some(blulib::client::Outcome::Packed(summary)) = &packed.result else {
        return Err(std::io::Error::other(format!(
            "job {} finished without saying what it packed",
            packed.id
        )));
    };

    tracing::info!(
        server = %remote.address(),
        set,
        version = summary.version,
        snapshot = %summary.snapshot,
        chunks = summary.chunks,
        length = summary.length,
        out = %out.display(),
        dry_run,
        "packing a version from the server"
    );

    if dry_run {
        let since = match from {
            Some(base) => format!(" since {base}"),
            None => format!("version {}", summary.version),
        };
        return say!(
            "dry run: {}: {} chunks, {} bytes of payload. This run wrote nothing to {}",
            since,
            summary.chunks,
            summary.payload_bytes,
            out.display(),
        );
    }

    fetch_pack_serve(&remote, set, out, summary, from).await
}

/// Fetch a server-packed version whole into `out`, and report what it holds.
///
/// Split out of [`run_pack_serve`] only to keep that function readable; the
/// plan is checked against the summary before a byte is trusted, exactly as
/// [`pull_and_apply`] checks its own.
async fn fetch_pack_serve(
    remote: &blulib::client::Remote,
    set: &str,
    out: &std::path::Path,
    summary: &blulib::client::PackSummary,
    from: Option<u32>,
) -> std::io::Result<()> {
    let pull = blulib::client::Pull::new(
        remote,
        set,
        summary.version,
        std::collections::HashSet::default(),
    );
    pull.begin(summary.length, out).await?;
    let plan = remote.plan_pack(set, summary.version, &[]).await?;
    if plan.length != summary.length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} planned a {}-byte pack and then described a {}-byte one. The file \
                 this run assembled would not be the file it fetched",
                remote.address(),
                summary.length,
                plan.length
            ),
        ));
    }
    pull.write(&plan.ranges, out, |moved| {
        tracing::info!(
            server = %remote.address(),
            ranges = moved.ranges,
            bytes = moved.bytes,
            total = moved.total,
            "fetched"
        );
    })
    .await?;
    let fetched = pull.open(&plan, out)?;
    drop(fetched);

    let pack = Pack::open(out)?;
    let content = blulib::pack::hash_file(out).await?;
    let packed = blulib::command::Packed {
        chunks: summary.chunks,
        of: summary.chunks,
        payload: pack.payload_len(),
        table: pack.snapshot_len(),
        base: from,
        bytes: Some(summary.length),
        content: Some(content),
    };
    tracing::info!(
        version = summary.version,
        chunks = summary.chunks,
        bytes = summary.length,
        snapshot = %summary.snapshot,
        content = %&content,
        "wrote pack"
    );
    say!("{}  {}", &content, out.display())?;
    report_pack(summary.version, out, &packed)
}

/// Apply `pack` into a keyed store as a new version.
///
/// Pulls the set into staging, merges there with exactly the code a local run
/// uses, and pushes the result back. A dry run pulls and plans with that same
/// code and pushes nothing back.
async fn run_merge_stored(
    pack: &std::path::Path,
    target: &StoreTarget,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;

    run_merge(pack, &target.staging, compression, dry_run).await?;
    if dry_run {
        return Ok(());
    }

    let stats = storage::push_to_remote_async(&staging, &backend).await?;
    tracing::info!(
        set = %target.set,
        store = %target.url,
        pushed_files = stats.pushed_files,
        pushed_bytes = stats.pushed_bytes,
        "merge to store complete"
    );

    Ok(())
}

/// Check a keyed store through staging.
///
/// Pulls the set into staging and checks it with exactly the code a local run
/// uses. Reads never push back.
async fn run_verify_stored(target: &StoreTarget, version: Option<u32>) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_verify(&target.staging, version).await
}

/// Rebuild a keyed store's index files through staging.
///
/// Pulls the set into staging, rebuilds there with exactly the code a local
/// run uses, and pushes the result back. A dry run pulls and plans with that
/// same code and pushes nothing back.
async fn run_reindex_stored(target: &StoreTarget, dry_run: bool) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_reindex(&target.staging, dry_run).await?;
    if dry_run {
        return Ok(());
    }

    let stats = storage::push_to_remote_async(&staging, &backend).await?;
    tracing::info!(
        set = %target.set,
        store = %target.url,
        pushed_files = stats.pushed_files,
        pushed_bytes = stats.pushed_bytes,
        "reindex to store complete"
    );

    Ok(())
}

/// A `blu serve` this run is pushing a pack to, and the set to put it in.
///
/// Upload, ask the server to merge it into one set, wait, and report. The
/// reporting lives here too, so a pushed pack and a merged one cannot say
/// different things about the same event.
///
/// ## Why the set name is asked for rather than derived
///
/// **A server holds many sets and this run cannot know which one was meant.**
/// The set arrives as the last segment of the `http://` target, so a target
/// with no set is refused rather than guessed at. The name is checked with
/// [`blulib::client::check_set`] before the first byte moves, so a name that
/// is really a path is refused here rather than after a transfer.
struct Push {
    /// The client, which has already refused an address it cannot speak to.
    remote: blulib::client::Remote,
    /// The set on that server, checked to be one path component.
    set: String,
}

impl Push {
    /// Open a push to `address`, into `set`.
    ///
    /// The client forwards `token` as a Bearer header when one is set.
    /// The client never checks the token.
    /// A proxy before the server checks it.
    ///
    /// # Errors
    ///
    /// The call fails when `address` is not a server address.
    /// The call fails when `set` is not one path component.
    /// The call fails when `token` is empty or holds whitespace.
    /// Both address and set are checked here, before any work.
    /// A mistyped value fails before a transfer.
    fn open(address: &str, set: &str, token: Option<&str>) -> std::io::Result<Self> {
        let remote = remote_for(address, token)?;

        blulib::client::check_set(set)?;

        Ok(Self {
            remote,
            set: set.to_string(),
        })
    }

    /// The merge policy this push asks the server for.
    ///
    /// **The command's own `--compression`, translated rather than reinterpreted.**
    /// `keep` means the same thing on both sides — store what the pack carries —
    /// and a level is a level. The two policies are different types because they
    /// answer different questions (what is on the wire versus what a set is
    /// opened with), and this is the one place the answer crosses.
    fn compression(compression: KeepOrLevel) -> blulib::client::MergeCompression {
        match compression {
            KeepOrLevel::Keep => blulib::client::MergeCompression::Keep,
            KeepOrLevel::Level(level) => blulib::client::MergeCompression::Reencode(level),
        }
    }

    /// Upload `pack`, ask the server to merge it, and report what it published.
    ///
    /// **The merge is the server's, not a local one followed by an upload.**
    /// `merge_pack` is one request sequence: begin the upload, send the bytes,
    /// commit, and start a merge into this set. The server opens the set, takes
    /// its own write lock, and commits a version in its own numbering. So the
    /// version this reports is the server's, and a pack pushed to a set that
    /// already holds it is answered with the version that holds it rather than
    /// an error.
    ///
    /// The job is awaited rather than handed back, so the exit code is about the
    /// merge: a job that failed or was cancelled is an error carrying the
    /// server's own sentence, and this prints nothing on stdout for it. There is
    /// no path here that reports a merge which did not happen.
    async fn pack(&self, pack: &std::path::Path, compression: KeepOrLevel) -> std::io::Result<()> {
        // Read before the upload rather than after: the merge request names an
        // upload, and an upload of bytes this machine does not have would fail at
        // the server with a sentence about the network rather than about the
        // file.
        let bytes = std::fs::read(pack)?;

        tracing::info!(
            server = %self.remote.address(),
            set = %self.set,
            pack = %pack.display(),
            bytes = bytes.len(),
            "pushing pack"
        );

        let job = self
            .remote
            .merge_pack(&self.set, bytes, Self::compression(compression))
            .await?;

        // Reported as it moves, on the same target and with the same counter
        // names a local merge uses. The merge is happening on the other machine,
        // so this is the only place a long push says anything at all.
        let finished = self
            .remote
            .await_job_with(&job, |counters| {
                let shown = counters
                    .iter()
                    .map(|(name, value)| format!("{name} {value}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                tracing::info!("merging on {}: {shown}", self.remote.address());
            })
            .await?;

        self.report(&finished, pack)
    }

    /// Say what the server's merge published, in the words a local merge uses.
    ///
    /// **One set of sentences for both sides of a transfer.** The figures are the
    /// server's — it read them out of the plan it committed — and the wording is
    /// `report_merge`'s and `report_already_merged`'s, so a script that reads a
    /// merge report does not have to learn a second one because the merge
    /// happened over HTTP. Only the two names differ, and they have to: the set is
    /// a name on a server rather than a path here, and the server is named because
    /// a push is a thing done *to* somewhere and a report that did not say where
    /// would be about a set on no machine in particular.
    fn report(&self, job: &blulib::client::Job, pack: &std::path::Path) -> std::io::Result<()> {
        use blulib::client::{Merged as RemoteMerged, Outcome};

        let Some(Outcome::Merged(merged)) = &job.result else {
            return Err(std::io::Error::other(format!(
                "job {} finished without saying what it merged",
                job.id
            )));
        };

        match merged {
            RemoteMerged::Committed(committed) => {
                tracing::info!(
                    server = %self.remote.address(),
                    set = %committed.set,
                    version = committed.version,
                    snapshot = %&committed.snapshot,
                    base = committed.base,
                    chunks = committed.chunks,
                    fresh = committed.fresh,
                    held = committed.held,
                    bytes = committed.bytes,
                    "merge complete"
                );

                let since = match committed.base {
                    Some(base) => format!(" since {base}"),
                    None => String::new(),
                };

                say!(
                    "merged version {}{since}: {} chunks, {} new, {} already held, {} bytes \
                     stored from {}",
                    committed.version,
                    committed.chunks,
                    committed.fresh,
                    committed.held,
                    committed.bytes,
                    pack.display(),
                )?;
                say!(
                    "{}  {} on {}",
                    &committed.snapshot,
                    committed.set,
                    self.remote.address(),
                )
            }
            RemoteMerged::Already(already) => {
                tracing::info!(
                    server = %self.remote.address(),
                    set = %already.set,
                    version = already.version,
                    "merge: already merged, nothing to do"
                );

                say!(
                    "already merged: {} on {} holds this version as version {}. Nothing to do",
                    already.set,
                    self.remote.address(),
                    already.version,
                )
            }
        }
    }
}

/// Pack a version of the set at `name` into `out`, and report what the pack holds.
///
/// **The one place a pack is written locally**, so `blu pack` and the pack
/// a backup to a serve target pushes are the same file built by the same code.
/// The run it is taken from does not matter: a serve backup hands the version
/// it just staged to this, and a `pack` hands the version its own `--version`
/// named, and both arrive here as one `Option<u32>`.
///
/// Returns the version it resolved, alongside what the pack holds. The caller
/// reports it, so `pack --version 2` says 2 and `pack` with no version says the
/// one the set resolved to — which is the number the report line is about, and
/// which a caller that only has the return value could not work out.
async fn write_pack(
    name: &std::path::Path,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<(u32, blulib::command::Packed)> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
    let (version, _) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        from,
        out = %out.display(),
        compression = ?compression,
        dry_run,
        "packing"
    );

    let mut packing = PackBuilder::new(&store, out).version(version);
    if let Some(base) = from {
        let (base, _) = select_version(&set, &store, Some(base))?;
        packing = packing.from(base);
    }
    if let Some(policy) = compression.pack_policy() {
        packing = packing.compression(policy);
    }
    if dry_run {
        packing = packing.dry_run();
    }
    let packed = packing.run().await?;

    Ok((version, packed))
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
            Some(
                storage::BlobStore::builder(set.base())
                    .open_reader()
                    .await?,
            )
        };
        let fresh = existing.is_none();

        let planned = MergePlanBuilder::new(&pack)
            .store(existing.as_ref())
            .run()
            .await?;

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
        blulib::command::establish_base::<storage::Write, storage::Compression>(
            None,
            base,
            pack.path(),
        )?;
    }

    let _lock = set.lock()?;
    let store = storage::BlobStore::builder(set.base())
        .compression(compression.policy())
        .open_writer()
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

    let merged = MergeBuilder::new(&store, &pack)
        .mode(compression.route())
        .run()
        .await?;

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
        "warning: this pack is version {from} and {} is at version {tip}. Merging \
         publishes it as version {}. A restore with no --version will return that one",
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
        "already merged: {} holds this version as version {version}. Nothing to do",
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
        "dry run: version {}{since}: {} chunks, {} new, {} already held, {} bytes to \
         store. This run wrote nothing to {}",
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
    files: Vec<std::path::PathBuf>,
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

    let applied = ApplyBuilder::new(destination, &pack)
        .options(ApplyOptions {
            prune,
            recheck,
            dry_run,
            files,
        })
        .run()
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
    applied: &blulib::command::Applied,
    pack: &Pack,
    destination: &std::path::Path,
    dry_run: bool,
) -> std::io::Result<()> {
    // The bytes a fetch would have needed. That figure says whether this
    // was an update or a whole tree.
    if dry_run {
        say!(
            "dry run: would fetch {} of {} bytes in {} range(s). {} of {} chunks are \
             already held. {} chunks would be written into {} file(s)",
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
            "fetched {} of {} bytes in {} range(s). {} of {} chunks were already \
             held. This run wrote {} chunks into {} file(s)",
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
            (true, 0) => "would be left alone. Pass --prune to remove them",
            (true, _) => "would be removed by --prune",
            (false, _) => "left alone. Pass --prune to remove them",
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
                "note: {} entr{} in this version record an owner. An apply may not be \
                 able to set an owner without privilege",
                applied.unowned,
                if applied.unowned == 1 { "y" } else { "ies" },
            );
        } else {
            report_unowned(applied.unowned, "apply");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    //! The tests of `main` itself, and there is one worth having.
    //!
    //! Everything a command does is tested through the library. **What is tested
    //! here is the only thing the library cannot see: the layer stack.** A `tracing`
    //! subscriber is process-global and this binary owns the only one there is, so
    //! `init_tracing` is the seam between what the crate records and what anything
    //! can see — and a stack assembled here rather than in the library is a stack
    //! nothing else exercises.
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

    use super::*;

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
}
