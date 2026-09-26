//! The `lepatch` command line interface.

use std::{error::Error as _, process::ExitCode};

use clap::{Parser, Subcommand};
use lepatch::{
    command::{Options, backup, lock_destination, restore, verify},
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
        /// Base path of the backup set, e.g. `photos` creates `photos.bin`,
        /// `photos.001.idx` and so on.
        name: std::path::PathBuf,
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

            ExitCode::FAILURE
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

async fn run(command: Command) -> std::io::Result<()> {
    match command {
        Command::Backup { source, name } => run_backup(&source, &name).await,
        Command::Restore {
            destination,
            name,
            version,
            clean,
        } => run_restore(&destination, &name, version, clean).await,
        Command::Verify { name, version } => run_verify(&name, version).await,
    }
}

/// Resolve which version to operate on, or explain why there is none.
fn select_version(set: &IndexSet, requested: Option<u32>) -> std::io::Result<(u32, String)> {
    let versions = set.versions()?;

    match (requested, versions.latest) {
        (Some(version), _) => Ok((version, set.read_key(version)?)),
        (None, Some(found)) => Ok(found),
        (None, None) => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no usable snapshot found for {} (looked in {})",
                set.base().display(),
                set.directory().display()
            ),
        )),
    }
}

async fn run_backup(source: &std::path::Path, name: &std::path::Path) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Refuse to run against a backup set another process is writing to: the
    // blob store is append-only and shared, so interleaved writes from two
    // processes would corrupt both.
    let _lock = set.lock()?;

    let versions = set.versions()?;

    // Reuse the newest usable snapshot so unchanged chunks are not re-uploaded.
    let base_key = versions.latest.as_ref().map(|(_, key)| key.clone());
    if base_key.is_none() {
        tracing::debug!(name = %set.base().display(), "no earlier version to deduplicate against");
    }

    let reservation = set.reserve()?;
    let version = reservation.version();

    let blob_path = set.blob_path();
    let lock_path = set.lock_path();

    // The store and the lock live next to the tree being read and grow while it
    // is being read, so they must not be swept into the backup.
    let options = Options {
        base_key,
        exclude: vec![blob_path.clone(), lock_path.clone()],
        ..Options::default()
    };

    let store = storage::BlobFileStorage::<true>::new(&blob_path).await?;

    let key = match backup(source, options, store).await {
        Ok(key) => key,
        Err(e) => {
            tracing::error!(
                name = %set.base().display(),
                version,
                "backup failed; leaving no index behind"
            );
            return Err(e);
        }
    };

    reservation.commit(&key)?;

    tracing::info!(
        name = %set.base().display(),
        version,
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
    let (version, key) = select_version(&set, version)?;

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
        empty_destination(destination)?;
    }

    let store = storage::BlobFileStorage::<false>::new(set.blob_path()).await?;

    let restored = restore(destination, key, store).await?;

    tracing::info!(
        files = restored.files,
        dirs = restored.dirs,
        links = restored.links,
        chunks = restored.chunks,
        "restore complete"
    );

    // A merging restore leaves the destination holding more than the snapshot
    // accounts for, and it cannot say what: the snapshot is inside the library
    // call by then. So it says the one thing that is true and actionable rather
    // than guessing at a count.
    if !clean {
        tracing::info!(
            "the destination may also hold files that are not in this snapshot; \
             pass --clean to restore into an exact copy"
        );
    }

    Ok(())
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
    //
    // Nested rather than a `let` chain, which needs a compiler newer than this
    // crate's MSRV.
    if let Some(home) = std::env::var_os("HOME").map(std::path::PathBuf::from) {
        if target == absolutise(&home) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "refusing to clean your home directory ({})",
                    target.display()
                ),
            ));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        if target == absolutise(&cwd) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "refusing to clean the working directory ({})",
                    target.display()
                ),
            ));
        }
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

/// Empty a destination that has already been checked.
///
/// Separate from the check so the two can sit either side of taking the restore
/// claim: refuse first, then claim, then destroy. A failure here stops the run
/// rather than restoring into whatever is left, because at this point the
/// destination is known to have been emptied and a half-dealt one is worse than
/// an untouched one.
fn empty_destination(destination: &std::path::Path) -> std::io::Result<()> {
    let target = absolutise(destination);

    // A restore does not create the destination itself, so this only fires when
    // something is already there.
    match std::fs::read_dir(&target) {
        Ok(_) => {
            tracing::warn!(
                destination = %target.display(),
                "emptying the destination before restoring"
            );
            std::fs::remove_dir_all(&target)?;
            std::fs::create_dir_all(&target)?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
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
    let (version, key) = select_version(&set, version)?;

    tracing::info!(name = %set.base().display(), version, "verifying");

    let store = storage::BlobFileStorage::<false>::new(set.blob_path()).await?;
    let report = verify(key, store).await?;

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
