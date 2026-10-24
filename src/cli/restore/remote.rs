//! Restoring from a `blu serve` set, through a fetched pack.

use crate::cli::{
    ApplyBuilder, ApplyOptions, Pack, lock_destination, remote_for, remove_work_dir, report_applied,
};

/// Rebuild a tree from a set on a `blu serve`, into `destination`.
///
/// **The local path's work, unchanged, behind a download.** A local restore
/// reads a version out of a set and writes every chunk of it. This one asks the
/// server to pack the version, fetches the ranges it does not already have into
/// a sparse file, and hands that file to [`blulib::command::ApplyBuilder`] — which is the same call
/// `blu apply` makes, with the same options, and reports through the same
/// [`report_applied`]. There is no second applier here and no second report.
///
/// ## Where `have` comes from
///
/// **From the destination, and only from the destination.** A chunk in `have`
/// is a region this run will not fetch, so it has to be a chunk this machine can
/// read. The one place a pulled tree keeps the chunks of a version is the tree
/// itself, and the one record that says which version a tree is — and that its
/// files are still where that record left them — is the apply manifest beside
/// [`blulib::command::Manifest::path_for`]. So [`have_for`] is the whole of it: every chunk of the
/// version when the destination already is that version, and none of them
/// otherwise.
///
/// Anything less than certainty is the empty set, deliberately. An empty `have`
/// costs the whole payload and is always correct; a `have` naming a chunk the
/// destination cannot produce is a hole in the assembled pack, and the applier
/// would write zeroes for it.
///
/// ## Why the fixed regions are fetched before the plan is asked for
///
/// **`have` is a list of chunk hashes, and the hashes are in the version's file
/// table.** The table is inside the pack, so the first thing to know is which
/// chunks there are. Only the header, the snapshot, the index and the trailer are
/// needed for that, and a pack assembled out of those four opens — that is what
/// [`Pack::open`] reads — so the cost is a couple of small range requests in the
/// one case where it can pay for itself, and nothing at all in the case where
/// `have` was going to be empty regardless.
///
/// ## Why the assembled pack is a temporary file
///
/// **It is derived output, and a pack that failed to assemble is worse than no
/// pack.** It is named by this process and this version, it is removed whether
/// the run succeeded or failed, and nothing outside this run ever looks at it.
/// A pull that fails part-way leaves the destination as it was rather than a
/// tree of zeroes: the bytes are written into the file first and applied after,
/// and [`blulib::client::Pull::open`] refuses a file whose missing records are not the ones
/// [`have_for`] named.
#[expect(
    clippy::too_many_arguments,
    reason = "a restore's own arguments, plus the address"
)]
pub(crate) async fn run_restore_remote(
    destination: &std::path::Path,
    set: &str,
    version: Option<u32>,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
    address: &str,
    token: Option<&str>,
) -> std::io::Result<()> {
    use blulib::client::Outcome;

    let remote = remote_for(address, token)?;
    blulib::client::check_set(set)?;

    // The manifest, once. It is what `have` is built from, and reading it twice
    // would be two chances to answer differently.
    let manifest = blulib::command::Manifest::load(destination)?;

    let started = remote.start_pack(set, version, None).await?;

    let packed = remote
        .await_job_with(&started, |counters| {
            let shown = counters
                .iter()
                .map(|(name, value)| format!("{name} {value}"))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::info!("packing on {}: {shown}", remote.address());
        })
        .await?;

    let Some(Outcome::Packed(summary)) = &packed.result else {
        return Err(std::io::Error::other(format!(
            "job {} finished without saying what it packed",
            packed.id
        )));
    };

    tracing::info!(
        server = %remote.address(),
        set,
        version = summary.version,
        snapshot = %summary.snapshot,
        chunks = summary.chunks,
        length = summary.length,
        destination = %destination.display(),
        clean,
        dry_run,
        "packing a version to restore from"
    );

    let mut work = tokio::task::spawn_blocking(tempfile::TempDir::new)
        .await
        .map_err(|join| {
            std::io::Error::other(format!("a temporary-directory task failed: {join}"))
        })??;
    let pack_path = work.path().join("pull.pack");

    // Everything from here on owns that file. Whether the run succeeded or
    // failed, it goes: a pack nobody named is not something to leave in a user's
    // directory, and a run that failed half-way has already said so.
    let applied = pull_and_apply(
        &remote,
        set,
        destination,
        manifest.as_ref(),
        summary,
        &pack_path,
        clean,
        dry_run,
        files,
    )
    .await;

    // The pull ended. The directory that held the pack goes next.
    // The removal of that directory runs on the blocking pool.
    remove_work_dir(&mut work).await;

    applied
}

/// Fetch the pack a remote restore needs, apply it, and report.
///
/// Split out of [`run_restore_remote`] because that one is about the temporary
/// file and this one is about the work: what to ask for, what to leave out, and
/// what to do with the answer.
///
/// # Errors
///
/// Returns an error if the pack cannot be planned, fetched or opened, or if the
/// apply refuses the destination.
///
/// Returns an error if the server answers a plan whose length is not the length
/// its own pack summary named, because the file this run assembles would not be
/// the file the server described.
///
/// Returns an error if the destination claim cannot be taken, which includes
/// another run holding it.
#[expect(
    clippy::too_many_arguments,
    reason = "a restore's own arguments, plus the pack"
)]
async fn pull_and_apply(
    remote: &blulib::client::Remote,
    set: &str,
    destination: &std::path::Path,
    manifest: Option<&blulib::command::Manifest>,
    summary: &blulib::client::PackSummary,
    pack_path: &std::path::Path,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
) -> std::io::Result<()> {
    let fixed = fixed_regions(summary);
    let scratch = blulib::client::Pull::new(
        remote,
        set,
        summary.version,
        std::collections::HashSet::new(),
    );

    scratch.begin(summary.length, pack_path).await?;

    // The file table, and with it the chunk hashes `have` is built from — but
    // only where the destination's own record says there is something to save.
    let reads = manifest.filter(|m| m.version == summary.snapshot);
    let mut fetched = blulib::client::Fetched {
        ranges: 0,
        bytes: 0,
        total: 0,
    };
    let mut prefetched = Vec::new();

    let mut have = match reads {
        None => std::collections::HashSet::new(),
        Some(manifest) => {
            let table = scratch.write(&fixed, pack_path, |_| {}).await?;
            fetched.bytes = table.bytes;
            fetched.ranges = table.ranges;
            prefetched = fixed;

            have_for(destination, manifest, &Pack::open(pack_path)?.snapshot()?)
        }
    };

    // With `--files`, the chunks no selected file needs join `have`: the plan
    // then omits their ranges, and the assembled pack holds holes where they
    // would sit. The apply below reads only the selection through the same
    // filter, so those holes are never touched. Where no selection was named
    // this changes nothing.
    have.extend(
        skippable_chunks(
            &scratch,
            pack_path,
            summary,
            !prefetched.is_empty(),
            &mut fetched,
            files,
        )
        .await?,
    );

    let listed: Vec<blulib::storage::BlobId> = have.iter().copied().collect();
    tracing::info!(
        held = listed.len(),
        of = summary.chunks,
        "these chunks are not fetched: the destination holds them or no selected file needs them"
    );

    // Only the ranges actually written count as already there. Where the
    // destination gave no reason to read the file table first, nothing was
    // fetched and the plan has to bring all of it.
    let pull =
        blulib::client::Pull::new(remote, set, summary.version, have).already_fetched(prefetched);
    let plan = remote.plan_pack(set, summary.version, &listed).await?;

    if plan.length != summary.length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} planned a {}-byte pack and then described a {}-byte one. The file \
                 this run assembles would not be the file it fetched",
                remote.address(),
                summary.length,
                plan.length
            ),
        ));
    }

    let moved = pull
        .write(&plan.ranges, pack_path, |moved| {
            tracing::info!(
                server = %remote.address(),
                ranges = fetched.ranges + moved.ranges,
                bytes = fetched.bytes + moved.bytes,
                total = moved.total,
                "fetched"
            );
        })
        .await?;

    fetched.bytes = fetched.bytes.saturating_add(moved.bytes);
    fetched.ranges = fetched.ranges.saturating_add(moved.ranges);

    let pack = pull.open(&plan, pack_path)?;

    apply_pulled(
        destination,
        &pack,
        summary,
        set,
        remote.address(),
        &fetched,
        clean,
        dry_run,
        files,
    )
    .await
}

/// Applies the pack a pull assembled, with the restore's own options.
///
/// Reads only the selection, then reports what moved and what the applier
/// did. Split out of [`pull_and_apply`] so each half reads as one job:
/// fetching the bytes, then writing the tree.
///
/// # Errors
///
/// Returns an error if the destination claim cannot be taken, if the apply
/// refuses the destination, or if the report cannot be written.
#[expect(
    clippy::too_many_arguments,
    reason = "a restore's own arguments, plus the fetch figures"
)]
async fn apply_pulled(
    destination: &std::path::Path,
    pack: &Pack,
    summary: &blulib::client::PackSummary,
    set: &str,
    address: &str,
    fetched: &blulib::client::Fetched,
    clean: bool,
    dry_run: bool,
    files: &[std::path::PathBuf],
) -> std::io::Result<()> {
    // Held for the whole run, as `blu apply` holds it, so two pulls into one
    // destination cannot interleave writes into the same tree.
    let _claim = if dry_run {
        None
    } else {
        Some(lock_destination(destination)?)
    };

    let applied = ApplyBuilder::new(destination, pack)
        .options(ApplyOptions {
            prune: clean,
            recheck: false,
            dry_run,
            files: files.to_vec(),
        })
        .run()
        .await?;

    tracing::info!(
        held = applied.held,
        files = applied.files,
        written = applied.written,
        bytes = applied.bytes,
        removed = applied.removed,
        fetched = fetched.bytes,
        ranges = fetched.ranges,
        dry_run,
        "applied the pack this pull fetched"
    );

    // What moved, and where it came from. The line after it is the applier's own,
    // unchanged, so a script reading an apply report reads this one the same way.
    say!(
        "version {} of {} on {}: fetched {} of {} bytes in {} range(s)",
        summary.version,
        set,
        address,
        fetched.bytes,
        summary.length,
        fetched.ranges,
    )?;

    report_applied(&applied, pack, destination, dry_run)
}

/// The chunks of the pack no selected file needs, for the fetch to skip.
///
/// Reads the file table out of the fixed regions — fetching them first where
/// the destination gave no reason to — and selects the same view the apply
/// below reads. Empty where no selection was named, so a full pull fetches as
/// before.
///
/// # Errors
///
/// Returns an error if the fixed regions cannot be fetched, if the pack does
/// not open, or if a `--files` value is empty, absolute, or holds `..`.
async fn skippable_chunks(
    scratch: &blulib::client::Pull<'_>,
    pack_path: &std::path::Path,
    summary: &blulib::client::PackSummary,
    table_present: bool,
    fetched: &mut blulib::client::Fetched,
    files: &[std::path::PathBuf],
) -> std::io::Result<std::collections::HashSet<blulib::storage::BlobId>> {
    let mut skipped = std::collections::HashSet::new();
    if files.is_empty() {
        return Ok(skipped);
    }

    if !table_present {
        let fixed = fixed_regions(summary);
        let table = scratch.write(&fixed, pack_path, |_| {}).await?;
        fetched.bytes = fetched.bytes.saturating_add(table.bytes);
        fetched.ranges = fetched.ranges.saturating_add(table.ranges);
    }

    let table = Pack::open(pack_path)?.snapshot()?;
    let filter = blulib::command::Filter::for_snapshot(files, &table)?;
    let view = filter.select(&table);
    let needed: std::collections::HashSet<blulib::storage::BlobId> =
        view.chunks.iter().copied().collect();
    skipped.extend(
        table
            .chunks
            .iter()
            .copied()
            .filter(|id| !needed.contains(id)),
    );
    Ok(skipped)
}

/// The chunks a destination holds of `snapshot`, or none of them.
///
/// **The apply manifest is a record of what a tree was, and this is the only
/// question it can answer.** It names one version, and `Manifest::untrusted` says
/// whether every file that version recorded is still where the record left it. A
/// tree that passes both *is* that version, which means it holds every chunk of
/// it — that is what a version is — and the server can be told not to send any of
/// them.
///
/// One file that has moved, one recorded with no modification time to compare,
/// and no manifest at all all give the empty set. So does a manifest for a
/// different version: that tree's chunks are a real thing the server could
/// leave out, but which of them *this* version also needs cannot be worked out
/// without reading the pack, and a wrong `have` is the one failure this protocol
/// cannot survive.
fn have_for(
    destination: &std::path::Path,
    manifest: &blulib::command::Manifest,
    snapshot: &blulib::metadata::Snapshot,
) -> std::collections::HashSet<blulib::storage::BlobId> {
    let moved = manifest.untrusted(destination, snapshot);

    if !moved.is_empty() {
        tracing::debug!(
            moved = moved.len(),
            "the destination has moved since its last apply, so the whole pack is fetched"
        );
        return std::collections::HashSet::new();
    }

    snapshot.chunks.iter().copied().collect()
}

/// The regions of a pack every client has to fetch.
///
/// **The header and the snapshot before the payload, the index and the trailer
/// after it.** A plan is these four plus the records a client lacks, so they are
/// what a client fetching nothing at all is sent — and they are enough on their
/// own to open the file, which is what a client that only wants the version's
/// file table needs.
fn fixed_regions(summary: &blulib::client::PackSummary) -> Vec<blulib::pack::Range> {
    use blulib::pack::Range;

    let head = Range {
        start: 0,
        end: summary.payload_start,
    };
    let tail = Range {
        start: summary.payload_start.saturating_add(summary.payload_bytes),
        end: summary.length,
    };

    [head, tail]
        .into_iter()
        .filter(|range| range.end > range.start)
        .collect()
}
