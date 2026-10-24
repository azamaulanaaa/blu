//! The `blu` command line interface.

use std::process::ExitCode;

use blulib::{
    command::{
        ApplyBuilder, ApplyOptions, BackedUp, BackupBuilder, BackupPlan, BackupPlanBuilder,
        MergeBuilder, MergePlan, MergePlanBuilder, Merged, PARTIAL_SUFFIX, PREVIOUS_SUFFIX,
        PackBuilder, Planned, Previous, RestoreBuilder, RestorePlan, RestorePlanBuilder,
        VerifyBuilder, load_snapshot, lock_destination, tip_with_same_tree, version_holding,
    },
    layout::IndexSet,
    metadata::Timestamp,
    pack::Pack,
    reader, storage,
    target::{StoreTarget, Target},
};
use clap::{Parser, Subcommand};

#[macro_use]
mod output;
mod apply;
mod backup;
mod compression;
mod merge;
mod pack;
mod push;
mod reindex;
mod restore;
mod serve;
mod signal;
mod store;
mod target;
mod telemetry;
mod temp;
mod verify;
mod versions;

pub(crate) use apply::report_applied;
pub(crate) use backup::BackupArgs;
pub(crate) use compression::{KeepOrLevel, compression_policy, parse_keep_or_level};
pub(crate) use output::{report_unattributed, report_unowned};
pub(crate) use pack::write_pack;
pub(crate) use push::Push;
pub(crate) use signal::stop_signal;
pub(crate) use store::{connect_store, require_staged_set, select_version};
pub(crate) use target::{remote_for, resolve_target};
pub(crate) use telemetry::{flush_telemetry, init_tracing};
pub(crate) use temp::remove_work_dir;

#[cfg(test)]
mod tests;

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

pub(crate) async fn entry() -> ExitCode {
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
            let signal = signal::STOP_SIGNAL.load(std::sync::atomic::Ordering::Relaxed);
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
            restore::dispatch_restore(&destination, target, version, clean, dry_run, files).await
        }
        Command::Verify {
            target,
            version,
            staging,
        } => {
            let target = resolve_target("verify", &target, staging.as_deref(), token.as_deref())?;
            verify::dispatch_verify(target, version).await
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
            let from = pack::effective_from(from, full);
            let target = resolve_target("pack", &target, staging.as_deref(), token.as_deref())?;
            pack::dispatch_pack(target, &out, version, from, compression, dry_run).await
        }
        Command::Apply {
            pack,
            destination,
            prune,
            recheck,
            dry_run,
            files,
        } => apply::run_apply(&pack, &destination, prune, recheck, dry_run, files).await,
        Command::Merge {
            pack,
            target,
            compression,
            dry_run,
            staging,
        } => {
            let target = resolve_target("merge", &target, staging.as_deref(), token.as_deref())?;
            merge::dispatch_merge_command(pack, target, compression, dry_run).await
        }
        Command::Reindex {
            target,
            dry_run,
            staging,
        } => {
            let target = resolve_target("reindex", &target, staging.as_deref(), token.as_deref())?;
            reindex::dispatch_reindex(target, dry_run).await
        }
        Command::Versions {
            target,
            numbers_only,
            files,
            staging,
        } => {
            let target = resolve_target("versions", &target, staging.as_deref(), token.as_deref())?;
            versions::dispatch_versions(target, numbers_only, files).await
        }
        Command::Serve {
            root,
            bind,
            scratch,
        } => serve::run_serve(&root, bind.as_deref(), scratch.as_deref(), &jobs).await,
    }
}
