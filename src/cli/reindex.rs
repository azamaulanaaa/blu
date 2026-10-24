//! The `reindex` command: rebuild the per-version index files.

use super::{IndexSet, StoreTarget, Target, connect_store, require_staged_set, storage};

/// Run `reindex` against a local path or a keyed store.
///
/// An `http://` or `https://` target fails: the index lives on the server, where no
/// endpoint rebuilds it.
pub(crate) async fn dispatch_reindex(target: Target, dry_run: bool) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_reindex(&name, dry_run).await,
        Target::Store(target) => run_reindex_stored(&target, dry_run).await,
        Target::Serve { .. } => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "`blu reindex` takes a local path or a `file://` target: the index lives on \
             the server, where no endpoint rebuilds it",
        )),
    }
}

/// Rebuild every per-version index file from the store's commit records.
///
/// Opened for writing, and that is what makes this a repair. A torn tail in the
/// last segment is reclaimed on the way. The lock is taken, so no run is
/// appending while the records are read.
///
/// A dry run takes neither. Both of them are writes. The lock is a file, and
/// reclaiming a tail is the repair. The version count that a dry run reports is
/// the same one either way. The count comes out of the commit records rather than
/// out of what the open did to the set.
///
/// Which leaves a consequence worth stating rather than leaving to be found. A
/// set left behind by a killed run has a torn tail. Its dry run fails. Its real
/// reindex succeeds and repairs the store. That is the right way round, because
/// deciding a tail is damage is itself a write.
async fn run_reindex(name: &std::path::Path, dry_run: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // Before taking the lock and before opening. Opening for writing
    // *creates* a set that is not there. So a mistyped name would be repaired
    // and reported as a success.
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

    let _lock = if dry_run { None } else { Some(set.lock()?) };

    if dry_run {
        let store = storage::BlobStore::builder(set.base())
            .open_reader()
            .await?;
        let versions = store.versions()?;

        // `write_index_files` writes one file per version that it can resolve a
        // snapshot for. The function skips the rest and returns the lower count.
        // So the plan applies the same test rather than counting the commit
        // records. A version whose snapshot does not resolve is not counted here.
        // A real reindex writes no index file for that version either. A plan
        // reports what the run does, and it does not write one. The index files
        // are a cache either way, so rewriting one is how they are brought back
        // to date.
        let writable = versions
            .iter()
            .filter(|version| store.snapshot(**version).is_ok())
            .count();
        let existing = set.indexed_versions()?.len();

        tracing::info!(name = %set.base().display(), "dry run: nothing was written");

        say!(
            "dry run: {} in {}",
            plural_versions(versions.len()),
            set.base().display()
        )?;
        say!(
            "dry run: {writable} index file(s) would be written, {existing} of them already there"
        )?;

        return Ok(());
    }

    let store = storage::BlobStore::builder(set.base())
        .open_writer()
        .await?;

    let versions = store.versions()?;
    let written = store.write_index_files().await?;

    tracing::info!(
        name = %set.base().display(),
        versions = versions.len(),
        written,
        "rebuilt the version index"
    );

    Ok(())
}

/// "1 version" or "3 versions", for a line that reports a count.
fn plural_versions(count: usize) -> String {
    if count == 1 {
        "1 version".to_string()
    } else {
        format!("{count} versions")
    }
}

/// Rebuild a keyed store's index files through staging.
///
/// Pulls the set into staging, rebuilds there with exactly the code a local
/// run uses, and pushes the result back. A dry run pulls and plans with that
/// same code and pushes nothing back.
async fn run_reindex_stored(target: &StoreTarget, dry_run: bool) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_reindex(&target.staging, dry_run).await?;
    if dry_run {
        return Ok(());
    }

    let stats = storage::push_to_remote_async(&staging, &backend).await?;
    tracing::info!(
        set = %target.set,
        store = %target.url,
        pushed_files = stats.pushed_files,
        pushed_bytes = stats.pushed_bytes,
        "reindex to store complete"
    );

    Ok(())
}
