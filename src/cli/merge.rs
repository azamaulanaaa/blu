//! The `merge` command: merge a pack into a set as a new version.

use super::{
    IndexSet, KeepOrLevel, MergeBuilder, MergePlan, MergePlanBuilder, Merged, Pack, Planned, Push,
    StoreTarget, Target, connect_store, storage, tip_with_same_tree, version_holding,
};

/// Run `merge` against a local path, a keyed store, or a server.
///
/// With an `http://` target the merge happens on the server: the pack is
/// uploaded, the server merges it holding that set's own write lock, and the
/// published version is numbered by the server's set. A dry run is refused
/// there: the server has no plan-only merge.
pub(crate) async fn dispatch_merge_command(
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

    // A pack rebuilt from an unchanged tree carries a new stamp.
    // Its hash never matches, but its tree can.
    // Report the tip before warning about the pack version.
    // A same-tree pack publishes nothing, so the warning would be wrong.
    if let Ok(tree) = pack.snapshot()
        && let Some(version) = tip_with_same_tree(&store, &tree).await
    {
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
