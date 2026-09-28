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
        /// Base path of the backup set, e.g. `photos` creates `photos.000.bin`
        /// and `photos.lock`.
        name: std::path::PathBuf,
        /// How large a segment may get before a new one is started, e.g. `4GiB`.
        ///
        /// Only affects segments written from now on. No record depends on where
        /// a boundary fell, so changing it mid-life is safe and never rewrites
        /// what is already there.
        #[arg(long, value_name = "SIZE", value_parser = parse_size)]
        segment_size: Option<u64>,
        /// How hard to compress record payloads: 0 stores them as they are, 1 is
        /// fastest, 9 is smallest.
        ///
        /// Only affects records written from now on. Every record names the codec
        /// it was written with, so a set written at one level reads back correctly
        /// whatever level it is read or written at later, and changing this never
        /// rewrites what is already there. A payload that does not compress —
        /// most already-compressed assets — is stored raw at any level.
        ///
        /// 1 is the default, and the levels are not close to each other: on a 4 MB
        /// executable level 1 is 2.7x smaller at 468 MB/s and level 9 is 3.3x at
        /// 65 MB/s. A backup runs once and is read many times, so write speed is
        /// the cost that matters. zstd's own levels go to 22, and the last
        /// thirteen of them buy a few percent for a hundred times the time.
        #[arg(long, value_name = "LEVEL", value_parser = clap::value_parser!(u8).range(0..=9))]
        compression_level: Option<u8>,
    },
    /// Rebuild the per-version index files from the store's commit records.
    ///
    /// The index files are a cache: they make listing a set cost a file name per
    /// version rather than a scan of every record in it. Nothing is lost if they
    /// are missing or wrong, and this puts them back. It is the operation the
    /// previous layout had no equivalent of, because there the index files were
    /// the only record of which versions existed and could not be derived from
    /// anything.
    Reindex {
        /// Base path of the backup set to rebuild.
        name: std::path::PathBuf,
    },
    /// List the versions a backup set holds, oldest first.
    ///
    /// Reads the per-version index files, so it costs a file name per version
    /// rather than a scan of the whole set. The count is a lower bound: a version
    /// whose index was never written still exists and still restores, so this can
    /// under-report and never over-reports.
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
    /// Write one version's chunks to a single distributable file.
    ///
    /// A pack is the same chunks a backup set holds, concatenated with no
    /// framing between them, so a client that needs a new version fetches one
    /// range rather than one per chunk. Measured on a synthetic tree, patching
    /// one 500 KB binary, that is 25,325 bytes in a single request against 25,325
    /// requests served from the set itself.
    ///
    /// A pack is output, not a backup: it is regenerated from the set and its
    /// snapshot alone, and the set's commit records remain the record of what
    /// versions exist. Losing one loses nothing.
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

    // Stderr, deliberately. `fmt()` writes to stdout unless told otherwise, which
    // put every log line in front of the commands' actual output: `lepatch pack`
    // prints a content hash meant to be captured by a script, and
    // `versions --numbers-only` prints a version per line for the same reason.
    // Both are only clean today because of which code path happens to log. Data on
    // stdout and diagnostics on stderr means every command is pipeable without
    // `--quiet`, which is what made this worth changing rather than working
    // around.
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Number of the signal that asked us to stop, or 0 if none did.
///
/// Read by `main` to report `128 + signal`, which is what a shell expects: 130
/// for Ctrl-C, 143 for `kill`. A static rather than a return value because the
/// signal is the reason the run was abandoned, not the reason it failed.
static STOP_SIGNAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Print one line of a command's output.
///
/// `println!` is not usable here. Rust sets `SIGPIPE` to ignored at startup, so a
/// write to a pipe whose reader has gone returns `EPIPE` rather than raising the
/// signal, and the print macros panic on a write error. So `lepatch versions set |
/// head -1` printed a version and then panicked with a backtrace, exiting 101.
///
/// A panic is the wrong answer three times over: the reader going away is what
/// `head` does on purpose, a backtrace is not an error report, and 101 is a
/// failure a script would act on. The usual fix is to restore the default
/// `SIGPIPE` disposition, which needs `unsafe` — and this crate forbids `unsafe`
/// outright rather than allowing it by exception, which is the right line for
/// something meant to be trusted with a copy of someone's data.
///
/// So the pipe is handled here instead: print until the reader goes, then stop and
/// exit as the run otherwise would. Any other write failure is still an error,
/// because a full disk on a redirected file is not the reader's fault.
macro_rules! say {
    ($($arg:tt)*) => {{
        use std::io::Write as _;

        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        match writeln!(out, $($arg)*) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => std::process::exit(0),
            Err(e) => {
                eprintln!("error: could not write output: {e}");
                std::process::exit(1);
            }
        }
    }};
}

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

/// Parse a human size like `4GiB`, `512MiB`, `1G`, or a plain byte count.
///
/// Deliberately small rather than a general parser: the units people reach for are
/// the powers of 1024 and an `iB`/`B` distinction, and a size that is ambiguous is
/// better refused than guessed at. A byte count with no unit is taken literally,
/// which is what a script passing `4294967296` means.
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
                    set.base().display(),
                    set.directory().display()
                ),
            )
        }),
    }
}

/// Rebuild every per-version index file from the store's commit records.
///
/// The store is opened for writing, which is what makes this a repair: it scans
/// every segment, so a torn tail in the last one is reclaimed on the way, and the
/// lock is taken so a run cannot be appending while the records are being read.
async fn run_reindex(name: &std::path::Path) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Refuse before taking the lock and before opening, because opening for
    // writing *creates* a set that is not there. Without this a mistyped name
    // left a new empty store behind and reported a successful repair of it —
    // which is the one thing a repair command must never do.
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

/// List the versions a backup set holds, oldest first.
///
/// Reads the index files, which is the point of them: this answers in proportion
/// to the number of versions rather than to the number of records in the set. Ten
/// thousand versions is ten thousand small files, not every chunk header in every
/// gigabyte.
///
/// The count printed is a **lower bound**, deliberately. A version whose index
/// write was abandoned still exists and still restores — the commit record is what
/// makes a version — so this may report fewer than the set holds. It never reports
/// more, because a file cannot invent one. A set with no index files lists nothing
/// rather than failing, and `lepatch reindex` brings it up to date.
///
/// A set with no versions is not an error. "There are none" is the answer to the
/// question that was asked, and a script iterating over a location that has not
/// been backed up yet should see an empty list rather than a failure.
async fn run_versions(name: &std::path::Path, numbers_only: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // No segments at all is not an empty set, it is a mistyped name. The store
    // reports both as `NotFound`, and answering "no versions" to a name that
    // names nothing is exactly the failure this command must not have: the set
    // read refuses it, and a listing of an empty but real set is still a listing.
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

    // The hashes, which the file names cannot supply. Opened once, read-only: a
    // store cannot be recovered and then left alone, because recovery truncates,
    // and listing what is there must not be the thing that discards an unfinished
    // run.
    //
    // A set whose segments are all here but which commits no version is a real
    // set with nothing in it, and lists as empty rather than as an error.
    let store = match storage::BlobStore::<false>::open(set.base()).await {
        Ok(store) => store,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    for version in versions {
        // Through the store, which checks the file rather than trusting it. One
        // that does not verify is skipped, not reported: it is a cache, and the
        // store answers the same question when asked.
        if numbers_only {
            let path = set.index_path(version);
            if storage::VersionIndex::read(&path).is_some() {
                say!("{version}");
            }
            continue;
        }

        let Ok(snapshot) = store.snapshot_via_index(version) else {
            continue;
        };

        say!("{version}  {}", storage::hex(&snapshot));
    }

    Ok(())
}

/// The compression policy from `--compression-level`, where 0 means "as-is".
///
/// One flag rather than a level and a switch, because the switch was a second
/// spelling of a value the flag already takes: `--no-compression` and
/// `--compression-level 0` mean the same thing, and two ways to say it is two ways
/// to forget which one is in force.
///
/// The range is checked by clap rather than clamped here, so a value outside 0–9 is
/// a usage error the user is shown rather than something silently corrected. That
/// is the difference between a tuning mistake and a mystery: asking for level 20
/// and quietly getting 9 would be a store on disk that does not match the command
/// that made it.
fn compression_policy(level: Option<u8>) -> storage::Compression {
    match level {
        Some(level) => storage::Compression::from_level(level),
        None => storage::Compression::default(),
    }
}

async fn run_backup(
    source: &std::path::Path,
    name: &std::path::Path,
    segment_size: Option<u64>,
    compression: storage::Compression,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Refuse to run against a backup set another process is writing to. The
    // store is append-only and the run holds one commit point at its end, so two
    // runs interleaving would publish versions over each other.
    let _lock = set.lock()?;

    let limit = match segment_size {
        Some(bytes) => storage::SegmentLimit::new(bytes),
        None => storage::SegmentLimit::default(),
    };
    let lock_path = set.lock_path();

    // The store's segments and the lock live next to the tree being read and grow
    // while it is being read, so they must not be swept into the backup. Every
    // segment, not just segment 0: a set that has rolled over has more than one,
    // and a segment left out of this list would be backed up while being written.
    let mut exclude = vec![lock_path];
    for segment in set.segments()? {
        exclude.push(segment);
    }
    // Segments written by this run may not exist yet — segment 0 of a fresh set,
    // and the one a roll-over creates during the run. A handful of names past the
    // last one on disk covers both: a run that rolls over more than once already
    // has more than a handful of segments, and those are on disk by the time the
    // exclusion is built.
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

    // Opening recovers the store: a tail past the last valid record in the last
    // segment is a run that did not finish, and is discarded here rather than
    // being read as data.
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
        snapshot = %storage::hex(&committed.snapshot),
        store = %set.base().display(),
        segments,
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
    let store = storage::BlobStore::<false>::open(set.base()).await?;
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
        check_clean_destination(destination, &set)?;
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

/// Check that a destination is safe to empty, without touching it.
///
/// `--clean` is the only operation here that destroys data, so it is deliberately
/// hard to talk into: the paths are compared absolute, because a relative blob
/// path never "starts with" a relative destination and the check would silently
/// never fire; and the handful of directories nobody means to empty are refused
/// outright.
fn check_clean_destination(destination: &std::path::Path, set: &IndexSet) -> std::io::Result<()> {
    if destination.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to clean an empty destination path",
        ));
    }

    // Both sides absolute, or the comparison below is comparing two different
    // ways of naming the same place and quietly answering the wrong question.
    let target = absolutise(destination);

    // Every segment, not just the first. A set that has rolled over holds the rest
    // of the backup in files the caller has probably never heard of, and a clean
    // restore would delete them while leaving segment 0 behind — which looks like
    // a successful restore of a set that is now two-thirds gone.
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

    // The segment a run would write next, which may not exist yet but is still
    // part of the set and would be created inside the destination.
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
    );

    report.into_result()
}

/// Write one version's chunks to a single file.
///
/// Opened for reading and no lock taken: a pack does not touch the set, and a
/// backup running concurrently appends segments this does not read. The version
/// is resolved before the plan is built, so a set that grows under the run packs
/// the version asked for rather than whatever is newest when it finishes.
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

    // The plan is a function of the snapshot, not of how the set happens to have
    // laid its chunks out, so this is the same list whatever order the set was
    // written in — which is what makes the pack reproducible.
    let decoded = load_snapshot(&store, snapshot).await?;
    let plan = storage::Plan::from_snapshot(&decoded, |id| store.locate(id))?;

    let pack = lepatch::pack::Pack::write(&store, &plan, &snapshot, out).await?;
    let file_len = pack.file_len()?;
    let content = lepatch::pack::hash_file(out).await?;

    tracing::info!(
        version,
        chunks = pack.len(),
        bytes = file_len,
        content = %storage::hex(&content),
        "wrote pack"
    );

    // The content hash is on stdout and not only in the log, because it is the
    // pack's name: the same tree packs to the same bytes, so this is a value a
    // cache can be keyed on and a client can verify with one comparison. Printing
    // the file's own size alongside it makes the two comparable.
    say!("{}  {}", storage::hex(&content), out.display());
    say!(
        "version {version}: {} chunks, {} bytes of payload, {} bytes on disk",
        pack.len(),
        pack.payload_len(),
        file_len
    );

    Ok(())
}
