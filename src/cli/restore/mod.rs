//! The `restore` command: rebuild a directory from a backup set.

use super::{
    IndexSet, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, RestoreBuilder, RestorePlan, RestorePlanBuilder,
    StoreTarget, Target, connect_store, lock_destination, report_unattributed, report_unowned,
    require_staged_set, select_version, storage,
};

mod remote;

/// Run `restore` against a local path, a keyed store, or a server.
///
/// Split out of `dispatch_inner` only to keep that match readable; it holds
/// no work of its own. A `--clean` restore promises exactly the snapshot, so
/// it cannot also restore only part of it: that combination is refused here,
/// before any target is touched.
pub(crate) async fn dispatch_restore(
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
            remote::run_restore_remote(
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

    if plan.unattributed > 0 {
        // The figure is a bound for the same reason as `unowned` above. Only
        // trying to set extended attributes says whether this filesystem
        // accepts them.
        eprintln!(
            "note: {} entr{} in this version record extended attributes. A restore \
             may not be able to set them on this filesystem",
            plan.unattributed,
            if plan.unattributed == 1 { "y" } else { "ies" },
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

    if restored.unattributed > 0 {
        report_unattributed(restored.unattributed, "restore");
    }
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
