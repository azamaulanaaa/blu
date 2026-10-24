//! The `backup` command: snapshot a tree into a new version of a set.

use super::{
    BackedUp, BackupBuilder, BackupPlan, BackupPlanBuilder, IndexSet, KeepOrLevel, Previous, Push,
    StoreTarget, Target, Timestamp, compression_policy, connect_store, load_snapshot, reader,
    remove_work_dir, resolve_target, storage, write_pack,
};

/// How one `backup` run excludes paths.
///
/// One struct because the four flags travel together from the command line to
/// the walk. Clap fills it where the command lands and `run_backup` reads it.
#[derive(Debug, Clone, clap::Args)]
struct IgnoreArgs {
    /// Exclude paths matching PATTERN, in gitignore line syntax. Repeatable.
    ///
    /// A line without a slash matches the name at any depth. A leading slash
    /// anchors it to the backup root. A trailing slash matches directories
    /// only. A leading `!` re-includes. Later lines win, and CLI lines win
    /// over `.bluignore` and `.gitignore` files.
    #[arg(long, value_name = "PATTERN")]
    exclude: Vec<String>,
    /// Read exclusion lines from FILE, one gitignore line per entry.
    /// Repeatable.
    ///
    /// Blank lines change nothing and `#` starts a comment. A missing file
    /// fails the run. Lines read as if from the backup root.
    #[arg(long, value_name = "FILE")]
    exclude_file: Vec<std::path::PathBuf>,
    /// Skip `.bluignore` files while walking.
    #[arg(long)]
    no_bluignore: bool,
    /// Skip `.gitignore` files while walking.
    #[arg(long)]
    no_gitignore: bool,
}

/// One `backup` run: the tree, the set spellings, and the policies.
///
/// One struct because `dispatch_inner` routes commands, and a ten-flag
/// command routed as ten arguments pushes every callee past the argument
/// threshold (see `.clippy.toml`) toward bundling them back at each call
/// site. Clap fills it where the command lands (`#[command(flatten)]` keeps
/// every flag spelled where it was), and `dispatch_inner` runs it once.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct BackupArgs {
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
    /// How this run excludes paths.
    #[command(flatten)]
    ignore: IgnoreArgs,
}

impl BackupArgs {
    /// Run the backup into a local path, a keyed store, or a server.
    pub(crate) async fn run(self, token: Option<String>) -> std::io::Result<()> {
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
                &self.ignore,
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
                    &self.ignore,
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
                    &self.ignore,
                    token.as_deref(),
                )
                .await
            }
        }
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
/// The tip version with its decoded tree, for the unchanged check.
///
/// Read under the lock with everything else that inspects the set.
/// A fresh set holds nothing.
/// A run passes this back as `previous`.
async fn backup_previous(
    store: &storage::BlobStore<storage::Write>,
) -> std::io::Result<Option<Previous>> {
    let Some((version, snapshot)) = store.latest() else {
        return Ok(None);
    };

    let tree = load_snapshot(store, snapshot).await?;

    Ok(Some(Previous {
        version,
        snapshot,
        tree,
    }))
}

#[expect(
    clippy::too_many_arguments,
    reason = "a backup's own arguments, threaded from the command line"
)]
async fn run_backup(
    source: &std::path::Path,
    name: &std::path::Path,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
    ignore: &IgnoreArgs,
    dry_run: bool,
) -> std::io::Result<Option<BackedUp>> {
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
            .exclude_patterns(ignore.exclude.iter().cloned())
            .exclude_files(ignore.exclude_file.iter().cloned())
            .read_bluignore(!ignore.no_bluignore)
            .read_gitignore(!ignore.no_gitignore)
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

    // The tip before the run, for the unchanged check.
    // Read under the lock with everything else that inspects the set.
    let previous = backup_previous(&store).await?;

    // The stamp the new version must not predate.
    // The tip tree carries it.
    let latest: Option<Timestamp> = previous.as_ref().map(|held| held.tree.taken_at);

    let outcome = BackupBuilder::new(source, store)
        .exclude(exclude)
        .exclude_patterns(ignore.exclude.iter().cloned())
        .exclude_files(ignore.exclude_file.iter().cloned())
        .read_bluignore(!ignore.no_bluignore)
        .read_gitignore(!ignore.no_gitignore)
        .chunker(chunker)
        .latest(latest)
        .previous(previous)
        .run()
        .await?;

    match &outcome {
        BackedUp::Committed(committed) => {
            tracing::info!(
                name = %set.base().display(),
                version = committed.version(),
                snapshot = %&committed.snapshot(),
                store = %set.base().display(),
                segments,
                "backup complete"
            );
        }
        BackedUp::Already { version, snapshot } => {
            tracing::info!(
                name = %set.base().display(),
                version,
                snapshot = %snapshot,
                "backup: the tree is unchanged, nothing to do"
            );

            say!("already at version {version}: the tree is unchanged. Nothing to do")?;
        }
    }

    Ok(Some(outcome))
}

/// Say what a backup would have stored, on stdout, and that it stored nothing.
///
/// On stdout rather than the log, because this is the whole output of the run.
/// A dry run produces no progress worth following. It produces no summary about
/// work that happened. What it produces is a report, and a report is data.
fn report_backup_plan(set: &IndexSet, plan: &BackupPlan, fresh: bool) -> std::io::Result<()> {
    tracing::info!(name = %set.base().display(), "dry run: nothing was written");

    if plan.unchanged {
        say!(
            "dry run: already at version {}, snapshot {}",
            plan.version,
            plan.snapshot
        )?;
    } else {
        say!(
            "dry run: version {} would be committed, snapshot {}",
            plan.version,
            plan.snapshot
        )?;
    }
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

/// Back up into a keyed store through staging.
///
/// Pulls the set into staging, backs the tree up there with exactly the code
/// a local run uses, and pushes the result back. A dry run pulls and plans
/// with that same code and pushes nothing back.
#[expect(
    clippy::too_many_arguments,
    reason = "a backup's own arguments, threaded from the command line"
)]
async fn run_backup_stored(
    source: &std::path::Path,
    target: &StoreTarget,
    segment_size: Option<u64>,
    compression: storage::Compression,
    profile: reader::ChunkerProfile,
    ignore: &IgnoreArgs,
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
        ignore,
        dry_run,
    )
    .await?;
    if dry_run {
        return Ok(());
    }
    let Some(outcome) = committed else {
        return Err(std::io::Error::other(
            "a backup that stored nothing published no version",
        ));
    };

    let version = match &outcome {
        BackedUp::Committed(committed) => committed.version(),
        BackedUp::Already { version, .. } => {
            tracing::info!(
                set = %target.set,
                store = %target.url,
                version,
                "backup: the tree is unchanged, nothing to push"
            );

            return Ok(());
        }
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
    ignore: &IgnoreArgs,
    token: Option<&str>,
) -> std::io::Result<()> {
    let push = Push::open(address, set, token)?;
    let mut work = tokio::task::spawn_blocking(tempfile::TempDir::new)
        .await
        .map_err(|join| {
            std::io::Error::other(format!("a temporary-directory task failed: {join}"))
        })??;
    let staging = work.path().join(set);
    let pack_path = work.path().join("push.pack");

    let outcome = async {
        let outcome = run_backup(
            source,
            &staging,
            segment_size,
            compression,
            profile,
            ignore,
            false,
        )
        .await?;
        let Some(outcome) = outcome else {
            return Err(std::io::Error::other(
                "a backup that stored nothing published no version",
            ));
        };
        let version = match &outcome {
            BackedUp::Committed(committed) => committed.version(),
            BackedUp::Already { version, .. } => {
                tracing::info!(
                    set,
                    version,
                    "backup: the tree is unchanged, nothing to push"
                );

                return Ok(());
            }
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

    // The backup ended. The directory that held the staging tree goes next.
    // The removal of that directory runs on the blocking pool.
    remove_work_dir(&mut work).await;

    outcome
}
