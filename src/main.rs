//! The `lepatch` command line interface.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use lepatch::{
    command::{
        ApplyOptions, Options, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, apply, backup, load_snapshot,
        lock_destination, restore, verify,
    },
    layout::IndexSet,
    pack::Pack,
    reader, storage,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Clone, Parser)]
#[command(name = "lepatch", version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// Log everything, including per-chunk tracing.
    #[arg(long, short)]
    verbose: bool,

    /// Only report warnings and errors.
    #[arg(long, short, conflicts_with = "verbose")]
    quiet: bool,
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
    },
    /// Rebuild the per-version index files from the store's commit records.
    ///
    /// They are a cache: nothing is lost if they are missing or wrong.
    Reindex {
        /// Base path of the backup set to rebuild.
        name: std::path::PathBuf,
    },
    /// List the versions a backup set holds, oldest first.
    ///
    /// The count is a lower bound: a version whose index file was never written
    /// still exists and still restores, so this can under-report and never
    /// over-reports.
    Versions {
        /// Base path of the backup set to inspect.
        name: std::path::PathBuf,
        /// Print just the version numbers, one per line, and nothing else.
        #[arg(long)]
        numbers_only: bool,
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
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    init_tracing(args.verbose, args.quiet);

    match run(args.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The message, not the `Debug` form.
            //
            // There is no cause chain to walk. `lepatch` has one error type —
            // `io::Error` — at all ninety-odd sites where it fails, and each of
            // those messages is a complete sentence naming the file, the offset
            // and the expectation, because that is what a user has to act on.
            //
            // This loop used to walk `source()` here and print `caused by:`
            // lines. It never printed anything, and could not have:
            //
            // * nothing in the crate implements `std::error::Error::source`, so
            //   the first `source()` call returned `None` at every one of them;
            // * and `io::Error` cannot carry the chain in the first place. Its
            //   `source()` returns the *source of* a wrapped custom error, not
            //   the error itself, so `io::Error::new(kind, e)` drops `e` from
            //   the walk even when `e` has a cause. The one site that wraps
            //   another error — a snapshot that will not decode — loses it here
            //   too, while `Display` still shows it, since `io::Error` delegates
            //   to the wrapped error's `Display`.
            //
            // So the code was a promise the type system could not keep, and a
            // contributor adding an error with a cause would reasonably assume
            // the output was already wired up. If a future change introduces a
            // bespoke error type with a live chain, this walk comes back with it
            // — and then it has something to print.
            eprintln!("error: {e}");

            // 130 for Ctrl-C, 143 for `kill`.
            let signal = STOP_SIGNAL.load(std::sync::atomic::Ordering::Relaxed);
            if signal == 0 {
                ExitCode::FAILURE
            } else {
                ExitCode::from(128 + signal)
            }
        }
    }
}

fn init_tracing(verbose: bool, quiet: bool) {
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

    // Data on stdout, diagnostics on stderr, so every command is pipeable
    // without `--quiet`.
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Number of the signal that asked us to stop, or 0 if none did. `main` reads it
/// to report `128 + signal`.
static STOP_SIGNAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Print one line of a command's output to stdout, reporting whether it went.
///
/// `println!` cannot be used: `SIGPIPE` is ignored at startup, so a write to a
/// pipe whose reader has gone returns `EPIPE` and the print macros panic on a
/// write error. That case is a *success* — the reader leaving is what `head`
/// does on purpose — so it is `Ok(())` and the command carries on to its own
/// result. Any other write failure is an error.
///
/// This deliberately does not decide the exit code. It used to call
/// `process::exit` directly, which meant a command that printed its summary
/// before computing its result could exit 0 with a result it had not looked at
/// yet: `lepatch verify set | true` reported success on a store that had failed
/// verification, because the write failed before `into_result` was ever called.
/// It also skipped every `Drop` in scope, which is how a `Lock` or a
/// `DestinationLock` releases its claim file, so any command that both held a
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
async fn run(command: Command) -> std::io::Result<()> {
    tokio::select! {
        result = dispatch(command) => result,
        signal = stop_signal() => Err(signal),
    }
}

/// Parse a human size like `4GiB`, `512MiB`, `1G`, or a plain byte count.
///
/// Units are the powers of 1024 with an optional `i`; anything else is refused
/// rather than guessed at. A byte count with no unit is taken literally.
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

/// The work itself, without the interruption handling.
async fn dispatch(command: Command) -> std::io::Result<()> {
    match command {
        Command::Backup {
            source,
            name,
            segment_size,
            compression_level,
            chunk_profile,
        } => {
            let compression = compression_policy(compression_level);
            let profile = chunk_profile.unwrap_or_default();
            run_backup(&source, &name, segment_size, compression, profile).await
        }
        Command::Restore {
            destination,
            name,
            version,
            clean,
        } => run_restore(&destination, &name, version, clean).await,
        Command::Verify { name, version } => run_verify(&name, version).await,
        Command::Pack {
            name,
            out,
            version,
            from,
            full,
        } => run_pack(&name, &out, version, effective_from(from, full)).await,
        Command::Apply {
            pack,
            destination,
            prune,
            recheck,
        } => run_apply(&pack, &destination, prune, recheck).await,
        Command::Reindex { name } => run_reindex(&name).await,
        Command::Versions { name, numbers_only } => run_versions(&name, numbers_only).await,
    }
}

/// Resolve when the process is asked to stop: `SIGTERM` as well as Ctrl-C, since
/// that is what a supervisor, a container stop or a closing laptop sends.
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
                // No `SIGTERM` on this platform; Ctrl-C is all there is.
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

/// The error a stopped run reports. Not a success and not a silent exit: the work
/// was abandoned. What is guaranteed is that nothing was *published* — a backup's
/// index is filled in only on completion, and a restore's claim is released.
///
/// `Interrupted` is worth the scrutiny because the crate is wary of naming a kind
/// it does not mean: `NotFound` is what the operating system returns for a file
/// that has gone, so a caller cannot tell one of ours from a real one, and that
/// ambiguity has cost a swallowed error before. `Interrupted` has no such
/// collision — `std` retries `EINTR` itself, so no filesystem call in this crate
/// ever produces it. Whoever gets this error knows it means a deliberate stop and
/// nothing else, which is exactly what the message says.
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
/// Opened for writing, which is what makes this a repair: a torn tail in the last
/// segment is reclaimed on the way, and the lock is taken so no run is appending
/// while the records are read.
async fn run_reindex(name: &std::path::Path) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Before taking the lock and before opening: opening for writing *creates* a
    // set that is not there, so a mistyped name would be repaired and reported as
    // a success.
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

    let _lock = set.lock()?;

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

/// List the versions a backup set holds, oldest first, each with its snapshot hash.
///
/// The count is a **lower bound**: a version whose index file was never written
/// still exists and still restores, so this may report fewer than the set holds
/// and never more. A set with no index files lists nothing rather than failing,
/// and `lepatch reindex` brings it up to date. A real set with no committed
/// version lists empty; a name that is not a set is an error.
async fn run_versions(name: &std::path::Path, numbers_only: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // No segments at all is a mistyped name, not an empty set: answering "no
    // versions" to a name that names nothing would be a failure a script acts on.
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
    // opening for writing truncates an unfinished run and listing must not be the
    // thing that discards it. A set with every segment but no committed version
    // lists empty rather than erroring, and that falls out: the open succeeds, the
    // loop below has nothing to iterate, and nothing is printed.
    //
    // Which is why there is no arm for "no set here". This used to be
    // `Err(e) if e.kind() == NotFound => return Ok(())`, and the kind was doing
    // two jobs. The one it was meant for is unreachable — the check above already
    // established that the set has segments, and that is the only condition which
    // makes the open report `NotFound` — so it never fired on the case it was
    // written for. The other job is the problem: `NotFound` is what the operating
    // system returns for a file that has gone, so a segment removed between that
    // check and the open would have been swallowed too, and `lepatch versions`
    // would exit 0 having printed nothing about a set that was being deleted.
    let store = storage::BlobStore::open_for_reading(set.base()).await?;

    for version in versions {
        // Through the store, which checks the file against itself: an index that
        // is stale, foreign or edited is a reason to fall back to a scan, never a
        // reason to name the version. One that does not resolve is skipped rather
        // than reported, because a listing must not name a version the store
        // cannot serve.
        //
        // Both modes go through `resolve` for that reason. `--numbers-only` used
        // to read the index file on its own, which checksums one file and never
        // consults the store — so a leftover index in a set with fewer versions
        // would be named by `versions --numbers-only` and omitted by `versions`,
        // from the same directory, at the same moment.
        let Ok(snapshot) = store.snapshot_via_index(version) else {
            continue;
        };

        if numbers_only {
            say!("{version}")?;
        } else {
            say!("{version}  {}", &snapshot)?;
        }
    }

    Ok(())
}

/// The compression policy from `--compression-level`, where 0 means "as-is".
///
/// clap rejects anything outside 0–9 as a usage error rather than letting it be
/// clamped, so a level is never silently turned into a store that disagrees with
/// the command that wrote it.
fn compression_policy(level: Option<u8>) -> storage::Compression {
    match level {
        Some(level) => storage::Compression::from_level(level),
        None => storage::Compression::default(),
    }
}

/// Snapshot a tree into a new version of the set at `name`.
///
/// The chunk profile is the run's own policy rather than the store's, like
/// `--segment-size` and `--compression-level` it is a write policy for what
/// comes next, and what is already there is never rewritten — so it is logged
/// with the sizes it resolved to rather than only as the name that was typed,
/// since the name is a shorthand for three numbers a user cannot see.
async fn run_backup(
    source: &std::path::Path,
    name: &std::path::Path,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // The store is append-only with one commit point at the end of a run, so two
    // runs interleaving would publish versions over each other.
    let _lock = set.lock()?;

    let limit = match segment_size {
        Some(bytes) => storage::SegmentLimit::new(bytes),
        None => storage::SegmentLimit::default(),
    };
    let lock_path = set.lock_path();

    // The set and its lock live next to the tree being read and grow while it is
    // read, so neither may be swept into the backup.
    let mut exclude = vec![lock_path];
    for segment in set.segments()? {
        exclude.push(segment);
    }
    // A segment this run has yet to create is named too: segment 0 of a fresh set,
    // and the one a roll-over opens during the run.
    for segment in 0..u32::try_from(set.segments()?.len())
        .unwrap_or(0)
        .saturating_add(4)
    {
        exclude.push(set.segment_path(segment));
    }

    let chunker = profile.config();

    // The chunker's sizes are the only thing about this run that a later run will
    // not be able to work out from the store, and the one whose consequences are
    // not visible on disk: a set whose runs disagree about chunk sizes restores
    // perfectly and deduplicates badly. Saying which profile cut this run is the
    // only warning the user gets, so it goes in the run's own log line rather than
    // behind `--verbose`.
    tracing::info!(
        profile = %profile,
        min = chunker.min_size,
        avg = chunker.avg_size,
        max = chunker.max_size,
        "chunking this tree"
    );

    let options = Options {
        exclude,
        chunker,
        ..Options::default()
    };

    // Opening discards a torn tail in the last segment: an unfinished run, not data.
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

/// Rebuild a tree from the set at `name` into `destination`.
async fn run_restore(
    destination: &std::path::Path,
    name: &std::path::Path,
    version: Option<u32>,
    clean: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::open_for_reading(set.base()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        destination = %destination.display(),
        clean,
        "restoring"
    );

    // Checked before the claim is taken, so a destination that can never be
    // cleaned reports why before anything is created for it. The removal itself
    // happens after, so two runs cannot both decide to empty it.
    if clean {
        check_clean_destination(destination, &set)?;
    }

    // Held for the whole run, so two restores into one destination cannot
    // interleave writes into the same tree.
    let claim = lock_destination(destination)?;

    if clean {
        restore_atomically(&claim, snapshot, store).await?;
    } else {
        let restored = restore(destination, snapshot, store).await?;
        report_restored(&restored);

        // A merging restore cannot say which extra files are there — the snapshot
        // is inside the library call by then — so it says the one thing that is
        // both true and actionable.
        tracing::info!(
            "the destination may also hold files that are not in this snapshot; \
             pass --clean to restore into an exact copy"
        );
    }

    Ok(())
}

/// Restore into a sibling directory, then swap it in, so a run that dies part-way
/// leaves the destination exactly as it was rather than holding a prefix of the
/// snapshot. Costs the disk space for two trees at once.
///
/// Takes the claim rather than the path, because "beside the destination" is
/// resolved by the claim and must be asked of it. This used to re-derive the
/// rule with its own `sibling`, which did not canonicalise — so restoring through
/// a symlink staged the tree beside the *link* while the claim's cleanup removed
/// things beside the *target*, and a failed run left a half-written tree behind
/// that the claim could not reach. One definition, and it is the claim's.
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
            // Best effort: the old tree is on disk either way, and saying where
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

/// Log what a restore put into the destination.
fn report_restored(restored: &lepatch::command::Restored) {
    tracing::info!(
        files = restored.files,
        dirs = restored.dirs,
        links = restored.links,
        chunks = restored.chunks,
        "restore complete"
    );
}

/// Check that a destination is safe to empty, without touching it.
///
/// Paths are compared absolute, because a relative store path never "starts with"
/// a relative destination and the check would silently never fire. Refuses an
/// empty path, a destination holding the set being restored from, `$HOME`, the
/// working directory, and the filesystem root.
fn check_clean_destination(destination: &std::path::Path, set: &IndexSet) -> std::io::Result<()> {
    if destination.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to clean an empty destination path",
        ));
    }

    let target = absolutise(destination);

    // Every segment, not just the first: a set that has rolled over holds the
    // rest of the backup in files a clean restore would delete while leaving
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

    // The segment a run would write next: not yet on disk, but part of the set
    // and created inside the destination.
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

    // Checked before the claim is taken: `/` would otherwise be refused for
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

    // After the summary, and not before it: the exit code is this, and a summary
    // that could terminate the process first would be able to answer for it.
    report.into_result()
}

/// The base version a pack should omit, with `--full` overriding `--from`.
///
/// Two flags that contradict each other, and resolving it here rather than in clap
/// so the message is ours: `--full` is the promise that the artifact suits any
/// client, and letting a stale `--from` in a script quietly narrow it would break
/// that promise without anyone noticing.
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
/// Opened for reading and no lock taken: a pack does not touch the set. The
/// version is resolved before the plan is built, so a set that grows under the run
/// packs the version asked for rather than whatever is newest when it finishes.
async fn run_pack(
    name: &std::path::Path,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::open_for_reading(set.base()).await?;
    let (version, snapshot_id) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        from,
        out = %out.display(),
        "packing"
    );

    // A function of the tree, not of how the set laid its chunks out, so the pack
    // is reproducible. The decoded snapshot is handed to the writer as well as
    // resolved into a plan, because a pack carries its own file table and so is
    // the version rather than a reference to it.
    let snapshot = load_snapshot(&store, snapshot_id).await?;
    let whole = storage::Plan::from_snapshot(&snapshot, |id| store.locate(id))?;

    // A delta is the same pack with fewer chunks in it. No format change and no new
    // code path downstream: the snapshot still travels with it, so an apply can
    // still find every file's bytes, and the chunks that are not here are exactly
    // the ones a holder of `from` already has.
    let (base, plan) = match from {
        None => (None, whole.clone()),
        Some(base_version) => {
            let base_id = select_version(&set, &store, Some(base_version))?.1;
            let base_snapshot = load_snapshot(&store, base_id).await?;
            (
                Some((base_version, base_id)),
                whole.since(&snapshot, &base_snapshot),
            )
        }
    };

    let pack = match base {
        // `write_delta`, not `write` over a filtered plan, because the header has
        // to say what the delta assumes — that is what lets an apply refuse rather
        // than apply what it happens to hold.
        Some((_, base_id)) => {
            lepatch::pack::Pack::write_delta(&store, &plan, &snapshot, base_id, out).await?
        }
        None => lepatch::pack::Pack::write(&store, &plan, &snapshot, out).await?,
    };
    let file_len = pack.file_len()?;
    let content = lepatch::pack::hash_file(out).await?;

    tracing::info!(
        version,
        base = base.map(|(v, _)| v),
        chunks = pack.len(),
        of = whole.len(),
        bytes = file_len,
        snapshot = file_len.saturating_sub(pack.snapshot_len()),
        content = %&content,
        "wrote pack"
    );

    // The content hash is the pack's name, so it goes to stdout for a script to
    // key a cache on.
    say!("{}  {}", &content, out.display())?;
    match base {
        // Both figures, because the fraction is fixture-dependent and the
        // request count is not: the whole version is one range either way.
        Some((base_version, _)) => say!(
            "version {version} since {base_version}: {} of {} chunks, {} bytes of \
             payload, {} bytes of file table, {} bytes on disk",
            pack.len(),
            whole.len(),
            pack.payload_len(),
            pack.snapshot_len(),
            file_len
        )?,
        None => say!(
            "version {version}: {} chunks, {} bytes of payload, {} bytes of file table, \
             {} bytes on disk",
            pack.len(),
            pack.payload_len(),
            pack.snapshot_len(),
            file_len
        )?,
    }

    Ok(())
}

/// Bring `destination` up to the version `pack` carries.
///
/// The destination is claimed for the length of the run, on the same terms a
/// restore claims one: two applies into one directory would interleave, and the
/// claim is a file *beside* it so it never appears in the result.
async fn run_apply(
    pack: &std::path::Path,
    destination: &std::path::Path,
    prune: bool,
    recheck: bool,
) -> std::io::Result<()> {
    let _claim = lock_destination(destination)?;

    let pack = Pack::open(pack)?;
    tracing::info!(
        pack = %pack.path().display(),
        destination = %destination.display(),
        chunks = pack.len(),
        "applying pack"
    );

    let applied = apply(destination, &pack, ApplyOptions { prune, recheck }).await?;

    tracing::info!(
        held = applied.held,
        files = applied.files,
        written = applied.written,
        bytes = applied.bytes,
        removed = applied.removed,
        "applied pack"
    );

    // The bytes a fetch would have needed, which is the number that says whether
    // this was an update or a whole tree.
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

    // Named on stderr, because stdout is data. Left in place unless `--prune`, and
    // said so either way: silence here would read as "nothing there".
    if applied.extra > 0 {
        eprintln!(
            "note: {} unrecorded entr{} under {} left alone; pass --prune to remove them",
            applied.extra,
            if applied.extra == 1 { "y" } else { "ies" },
            destination.display(),
        );
    }

    if applied.removed > 0 {
        say!(
            "removed {} unrecorded entr{}",
            applied.removed,
            if applied.removed == 1 { "y" } else { "ies" }
        )?;
    }

    Ok(())
}
