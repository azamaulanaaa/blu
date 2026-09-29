//! The `lepatch` command line interface.

use std::{error::Error as _, process::ExitCode};

use clap::{Parser, Subcommand};
use lepatch::{
    command::{
        Options, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, backup, load_snapshot, lock_destination, restore,
        verify,
    },
    index::IndexSet,
    storage,
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
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    init_tracing(args.verbose, args.quiet);

    match run(args.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The message and its cause chain, not the `Debug` form.
            eprintln!("error: {e}");

            let mut cause = e.source();
            while let Some(error) = cause {
                eprintln!("  caused by: {error}");
                cause = error.source();
            }

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
        } => {
            let compression = compression_policy(compression_level);
            run_backup(&source, &name, segment_size, compression).await
        }
        Command::Restore {
            destination,
            name,
            version,
            clean,
        } => run_restore(&destination, &name, version, clean).await,
        Command::Verify { name, version } => run_verify(&name, version).await,
        Command::Pack { name, out, version } => run_pack(&name, &out, version).await,
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
    store: &storage::BlobStore<false>,
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

    let store = storage::BlobStore::<true>::open(
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
    // lists empty rather than erroring.
    let store = match storage::BlobStore::<false>::open(set.base()).await {
        Ok(store) => store,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

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
            say!("{version}  {}", storage::Hex(&snapshot))?;
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
async fn run_backup(
    source: &std::path::Path,
    name: &std::path::Path,
    segment_size: Option<u64>,
    compression: storage::Compression,
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
        exclude.push(set.blob_path(segment));
    }

    let options = Options {
        exclude,
        ..Options::default()
    };

    // Opening discards a torn tail in the last segment: an unfinished run, not data.
    let store = storage::BlobStore::<true>::open(set.base(), limit, compression).await?;

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
        snapshot = %storage::Hex(&committed.snapshot),
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
    let store = storage::BlobStore::<false>::open(set.base()).await?;
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
    let _claim = lock_destination(destination)?;

    if clean {
        restore_atomically(destination, snapshot, store).await?;
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
async fn restore_atomically<S: storage::StorageGet + 'static>(
    destination: &std::path::Path,
    snapshot: storage::BlobId,
    store: S,
) -> std::io::Result<()> {
    let partial = sibling(destination, PARTIAL_SUFFIX);
    let previous = sibling(destination, PREVIOUS_SUFFIX);

    // Either a half-written restore that never became the destination, or a
    // superseded tree. Both are safe to clear.
    for path in [&partial, &previous] {
        if path.exists() {
            tracing::warn!(
                path = %path.display(),
                "removing a directory left by an earlier restore"
            );
            std::fs::remove_dir_all(path)?;
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
        std::fs::rename(destination, &previous)?;
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
        std::fs::remove_dir_all(&previous)?;
    }

    report_restored(&restored);
    Ok(())
}

/// A path beside `path`, named for it and `suffix`.
fn sibling(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let name = path
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());

    path.with_file_name(format!("{name}.{suffix}"))
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
    if absolutise(&set.blob_path(u32::try_from(segments.len()).unwrap_or(u32::MAX)))
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
    let store = storage::BlobStore::<false>::open(set.base()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(name = %set.base().display(), version, "verifying");

    let report = verify(snapshot, store).await?;

    for failure in &report.failures {
        eprintln!("error: {}", failure.detail);
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

/// Write one version's chunks to a single file.
///
/// Opened for reading and no lock taken: a pack does not touch the set. The
/// version is resolved before the plan is built, so a set that grows under the run
/// packs the version asked for rather than whatever is newest when it finishes.
async fn run_pack(
    name: &std::path::Path,
    out: &std::path::Path,
    version: Option<u32>,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::<false>::open(set.base()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        out = %out.display(),
        "packing"
    );

    // A function of the snapshot, not of how the set laid its chunks out, so the
    // pack is reproducible.
    let decoded = load_snapshot(&store, snapshot).await?;
    let plan = storage::Plan::from_snapshot(&decoded, |id| store.locate(id))?;

    let pack = lepatch::pack::Pack::write(&store, &plan, &snapshot, out).await?;
    let file_len = pack.file_len()?;
    let content = lepatch::pack::hash_file(out).await?;

    tracing::info!(
        version,
        chunks = pack.len(),
        bytes = file_len,
        content = %storage::Hex(&content),
        "wrote pack"
    );

    // The content hash is the pack's name, so it goes to stdout for a script to
    // key a cache on.
    say!("{}  {}", storage::Hex(&content), out.display())?;
    say!(
        "version {version}: {} chunks, {} bytes of payload, {} bytes on disk",
        pack.len(),
        pack.payload_len(),
        file_len
    )?;

    Ok(())
}
