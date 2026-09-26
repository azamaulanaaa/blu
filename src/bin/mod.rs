use std::{error::Error as _, process::ExitCode};

use clap::{Parser, Subcommand};
use lepatch::{
    command::{Options, backup, restore},
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
        } => run_restore(&destination, &name, version).await,
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
) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let versions = set.versions()?;

    let version = match (version, &versions.latest) {
        (Some(version), _) => version,
        (None, Some((version, _))) => *version,
        (None, None) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "no usable snapshot found for {} (looked for {}",
                    set.base().display(),
                    set.directory().display()
                ),
            ));
        }
    };

    let key = set.read_key(version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        destination = %destination.display(),
        "restoring"
    );

    let store = storage::BlobFileStorage::<false>::new(set.blob_path()).await?;

    restore(destination, key, store).await?;

    tracing::info!("restore complete");

    Ok(())
}
