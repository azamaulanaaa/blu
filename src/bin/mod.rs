//! The `lepatch` command line interface.

use std::{error::Error as _, process::ExitCode};

use clap::{Parser, Subcommand};
use lepatch::{
    command::{
        Options, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, backup, lock_destination, restore, verify,
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
        /// Base path of the backup set, e.g. `photos` creates `photos.bin` and
        /// `photos.lock`.
        name: std::path::PathBuf,
    },
    /// List the versions a backup set holds, oldest first.
    Versions {
        /// Base path of the backup set to inspect.
        name: std::path::PathBuf,
        /// Print just the version numbers, one per line, and nothing else.
        ///
        /// For scripts counting or iterating over versions; the default output is
        /// for a person reading it.
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
        /// Off by default because merging is the safer default: a restore into a
        /// directory that turns out to be the wrong one should not delete it
        /// first. What survives a merge is reported at the end, so a stale file
        /// is visible rather than merely present.
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
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    init_tracing(args.verbose, args.quiet);

    match run(args.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Printing the `Debug` form of an `io::Error` is what made failures
            // unreadable; show the message and the chain instead.
            eprintln!("error: {e}");

            let mut cause = e.source();
            while let Some(error) = cause {
                eprintln!("  caused by: {error}");
                cause = error.source();
            }

            // `128 + signal` is what a shell expects from a signalled process, and
            // scripts already understand it: 130 for Ctrl-C, 143 for `kill`.
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

    let _ = fmt().with_env_filter(filter).try_init();
}

/// Number of the signal that asked us to stop, or 0 if none did.
///
/// Read by `main` to report `128 + signal`, which is what a shell expects: 130
/// for Ctrl-C, 143 for `kill`. A static rather than a return value because the
/// signal is the reason the run was abandoned, not the reason it failed.
static STOP_SIGNAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Run a command, abandoning it if the process is asked to stop.
///
/// Without this, Ctrl-C left a restore claim behind that had to be removed by
/// hand before the next restore into the same directory would run, and a backup
/// left an index reservation that had to be removed by hand the same way. The
/// claims are released by `Drop`, so all this has to do is stop unwinding — and
/// letting the work future drop is what does it.
async fn run(command: Command) -> std::io::Result<()> {
    tokio::select! {
        result = dispatch(command) => result,
        signal = stop_signal() => Err(signal),
    }
}

/// The work itself, without the interruption handling.
async fn dispatch(command: Command) -> std::io::Result<()> {
    match command {
        Command::Backup { source, name } => run_backup(&source, &name).await,
        Command::Restore {
            destination,
            name,
            version,
            clean,
        } => run_restore(&destination, &name, version, clean).await,
        Command::Verify { name, version } => run_verify(&name, version).await,
        Command::Versions { name, numbers_only } => run_versions(&name, numbers_only).await,
    }
}

/// Resolve when the process is asked to stop.
///
/// `SIGTERM` is handled as well as Ctrl-C because everything that stops a backup
/// unattended — a supervisor, a container stop, a laptop closing — uses it, and
/// it is the one that was leaving claims behind with no way to tell what had
/// happened.
///
/// A second Ctrl-C is left to the default handler, so a user who wants out
/// immediately can still have it rather than waiting for a run to notice.
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

#[cfg(not(unix))]
async fn stop_signal() -> std::io::Error {
    use std::sync::atomic::Ordering;

    let _ = tokio::signal::ctrl_c().await;
    STOP_SIGNAL.store(2, Ordering::Relaxed);
    interrupted()
}

/// The error a stopped run reports.
///
/// Deliberately not worded as a success or a silent exit: the work was abandoned,
/// and how much of it landed depends on the command. What is guaranteed is that
/// nothing was *published* — a backup's index is only filled in on completion,
/// and a restore's claim is released.
fn interrupted() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "interrupted; any claim on a backup set or destination has been released",
    )
}

/// Resolve which version to operate on, or explain why there is none.
///
/// Reads the store's commit records rather than a separate index, so the answer
/// cannot disagree with the bytes: a version exists here if and only if the store
/// holds the record that published it.
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
                    set.blob_path().display(),
                    set.directory().display()
                ),
            )
        }),
    }
}

/// List the versions a backup set holds, oldest first.
///
/// A set with no versions is not an error. "There are none" is the answer to the
/// question that was asked, and a script iterating over a location that has not
/// been backed up yet should see an empty list rather than a failure.
async fn run_versions(name: &std::path::Path, numbers_only: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Opened read-only: a store cannot be recovered and then left alone, because
    // recovery truncates. Listing what is there must not be the thing that
    // discards an unfinished run.
    let store = match storage::BlobStore::<false>::open(set.blob_path()).await {
        Ok(store) => store,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    for version in store.versions()? {
        if numbers_only {
            println!("{version}");
            continue;
        }

        let snapshot = store.snapshot(version)?;
        println!("{version}  {}", storage::hex(&snapshot));
    }

    Ok(())
}

async fn run_backup(source: &std::path::Path, name: &std::path::Path) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Refuse to run against a backup set another process is writing to. The
    // store is a single append-only file and the run holds one commit point at
    // its end, so two runs interleaving would publish versions over each other.
    let _lock = set.lock()?;

    let blob_path = set.blob_path();
    let lock_path = set.lock_path();

    // The store and the lock live next to the tree being read and grow while it
    // is being read, so they must not be swept into the backup.
    let options = Options {
        exclude: vec![blob_path.clone(), lock_path.clone()],
        ..Options::default()
    };

    // Opening recovers the store: anything past the last valid record is a run
    // that did not finish, and is discarded here rather than being read as data.
    let store = storage::BlobStore::<true>::open(&blob_path).await?;

    if let Some(previous) = store.versions()?.last() {
        tracing::debug!(previous, name = %set.base().display(), "deduplicating against the store");
    } else {
        tracing::debug!(
            name = %set.base().display(),
            "no earlier version; this is the first backup of this location"
        );
    }

    let committed = backup(source, options, store).await?;

    tracing::info!(
        name = %set.base().display(),
        version = committed.version,
        snapshot = %storage::hex(&committed.snapshot),
        store = %blob_path.display(),
        "backup complete"
    );

    Ok(())
}

async fn run_restore(
    destination: &std::path::Path,
    name: &std::path::Path,
    version: Option<u32>,
    clean: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::<false>::open(set.blob_path()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        destination = %destination.display(),
        clean,
        "restoring"
    );

    // Checked before the claim is taken, so that a destination which can never
    // be cleaned reports why before anything is created for it. The removal
    // itself happens after, so two runs cannot both decide to empty it.
    if clean {
        check_clean_destination(destination, &set.blob_path())?;
    }

    // Held for the whole run, so two restores into one destination cannot
    // interleave writes into the same tree.
    let _claim = lock_destination(destination)?;

    if clean {
        // Restored beside the destination and swapped in, rather than emptied
        // and refilled. A restore that dies part-way through then leaves the
        // destination exactly as it was, instead of holding some prefix of the
        // snapshot.
        restore_atomically(destination, snapshot, store).await?;
    } else {
        let restored = restore(destination, snapshot, store).await?;
        report_restored(&restored);

        // A merging restore leaves the destination holding more than the snapshot
        // accounts for, and it cannot say which: the snapshot is inside the
        // library call by then. So it says the one thing that is true and
        // actionable rather than guessing at a count.
        tracing::info!(
            "the destination may also hold files that are not in this snapshot; \
             pass --clean to restore into an exact copy"
        );
    }

    Ok(())
}

/// Restore into a directory beside the destination, then swap it in.
///
/// The two are siblings so the swap is a rename on one filesystem. Renaming a
/// directory over an existing one is refused by the OS, so the swap is two
/// renames and there is a window between them — but it is a window measured in
/// microseconds, against a restore that took minutes, and the new tree is
/// already complete on disk throughout it. The alternative is a window measured
/// in minutes.
///
/// Costs the disk space for two trees at once. That is the trade for a failure
/// leaving nothing behind.
async fn restore_atomically<S: storage::StorageGet + 'static>(
    destination: &std::path::Path,
    snapshot: storage::BlobId,
    store: S,
) -> std::io::Result<()> {
    let partial = sibling(destination, PARTIAL_SUFFIX);
    let previous = sibling(destination, PREVIOUS_SUFFIX);

    // Left behind by a killed run. Safe to clear: it is either a half-written
    // restore that never became the destination, or a superseded tree.
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
            // The destination has not been touched, so there is nothing to put
            // back — only the half-written tree to clear away.
            let _ = std::fs::remove_dir_all(&partial);
            tracing::error!(
                destination = %destination.display(),
                "the restore failed; the destination was left as it was"
            );
            return Err(e);
        }
    };

    // Move the old tree aside rather than deleting it first, so that a failure
    // here can put it back.
    let had_previous = destination.exists();
    if had_previous {
        std::fs::rename(destination, &previous)?;
    }

    if let Err(e) = std::fs::rename(&partial, destination) {
        if had_previous {
            // Best effort: the old tree is still on disk either way, and saying
            // where is more use than a second failure.
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

/// Check that a destination is safe to empty, and empty it.
///
/// This is the only operation here that destroys data, so it is deliberately
/// hard to talk into it: the paths are compared absolute, because a relative
/// blob path never "starts with" a relative destination and the check would
/// silently never fire; and the handful of directories nobody means to empty are
/// refused outright.
/// Check that a destination is safe to empty, without touching it.
///
/// This is the only operation here that destroys data, so it is deliberately
/// hard to talk into it: the paths are compared absolute, because a relative
/// blob path never "starts with" a relative destination and the check would
/// silently never fire; and the handful of directories nobody means to empty are
/// refused outright.
fn check_clean_destination(
    destination: &std::path::Path,
    blob: &std::path::Path,
) -> std::io::Result<()> {
    if destination.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to clean an empty destination path",
        ));
    }

    // Both sides absolute, or the comparison below is comparing two different
    // ways of naming the same place and quietly answering the wrong question.
    let target = absolutise(destination);
    let store = absolutise(blob);

    if store.starts_with(&target) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to clean {}: it holds the backup set being restored from ({})",
                target.display(),
                store.display()
            ),
        ));
    }

    // The directories a mistyped or truncated path lands on. None of these is
    // ever a restore destination somebody meant, and all of them are
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

    // Ahead of nothing in particular, but before the claim is taken: `/` would
    // otherwise be refused for having no sibling to put a claim beside, which is
    // a true but unhelpful reason for not emptying it.
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
async fn run_verify(name: &std::path::Path, version: Option<u32>) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::<false>::open(set.blob_path()).await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(name = %set.base().display(), version, "verifying");

    let report = verify(snapshot, store).await?;

    for failure in &report.failures {
        eprintln!("error: {}", failure.detail);
    }

    println!(
        "checked {} chunks ({} bytes) in version {version}: {}",
        report.chunks,
        report.bytes,
        if report.is_ok() {
            "all chunks match their recorded hashes".to_string()
        } else {
            format!("{} chunks FAILED", report.failures.len())
        }
    );

    report.into_result()
}
