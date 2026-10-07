//! Making a directory match the version a pack carries.
//!
//! A restore writes every byte a version needs. Its bytes come out of a store one
//! chunk at a time, and it cannot know which of them the destination already has.
//! An apply is the same walk with one difference that changes everything. It can
//! know, and so it writes only the bytes that are missing.
//!
//! ## `held` is for planning, `mismatched` is for writing
//!
//! These are two different answers. Conflating them is a bug worth naming,
//! because both look like "what the directory already has".
//!
//! [`Probed::held`] is per *chunk*, and it exists to answer a question about the
//! transfer: [`Pack::needed`](crate::pack::Pack::needed) skips a range request for
//! every chunk in it.
//! A chunk is in it only if the directory holds it correctly everywhere the
//! version places it.
//!
//! [`Probed::mismatched`] is per *slice*, and it answers a question about the
//! disk: which recorded byte ranges are not currently the right bytes.
//! That is the set an apply writes, and it is not derivable from `held`.
//! A file that is missing entirely has every one of its slices reported
//! [`Unheld::Absent`].
//! This is true even where its chunks are shared with another file that is
//! perfectly correct.
//! Judge that file by `held` and those slices are skipped, and the file is created
//! empty.
//!
//! ## A corrupt pack must not reach the disk
//!
//! The probe compares the directory against the chunk's bytes *as the pack gives
//! them*.
//! A damaged pack makes everything look absent, and would otherwise be written
//! over a correct directory.
//! Every chunk about to be written is hashed and compared against the hash it
//! is addressed by, **before a byte is written**.
//! This check costs one pass over the delta, and a failure there leaves the
//! directory untouched.
//! The cost is reading the delta twice: once to check it and once to write it.
//! A restore makes the same trade, for the same reason, and on a delta it is a
//! delta-sized cost.
//!
//! ## What this does not do
//!
//! It is not atomic, and it cannot be without a copy.
//! A failure partway through leaves a directory that is neither the old version
//! nor the new one.
//! A probe is idempotent, so running the same apply again finishes the job.
//! That is the recovery, and it is why nothing here tries to be cleverer.
//!
//! Pruning is opt-in and last but one.
//! An entry the version does not record is as likely to be the user's own work as
//! a leftover from an older version.
//! It is reported by default and removed only when asked.

use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    task::JoinSet,
};
use tracing::instrument;

use crate::{
    metadata,
    pack::Pack,
    progress::{Counter, Phase, Progress},
    storage::{BlobId, StorageGet},
};

use super::probe::Probed;

/// Files to write at once, and the ceiling the machine can do.
const APPLY_CONCURRENCY: usize = 8;

fn apply_concurrency() -> usize {
    super::concurrency(APPLY_CONCURRENCY)
}

/// Knobs for a single [`apply`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Options {
    /// Remove entries the version does not record.
    ///
    /// Off by default, and the default is the decision.
    /// A file in the destination that this version does not mention is either
    /// something the user made or something an older version left behind.
    /// Nothing in the version can tell those apart.
    /// A tool that deleted the first to satisfy the second would destroy data on
    /// a guess.
    pub prune: bool,

    /// Re-read every file, ignoring any manifest beside the destination.
    ///
    /// Off by default. On, the run costs a full read of the tree.
    /// That is the honest cost of not believing a record of a directory that has
    /// been changing since it was written.
    pub recheck: bool,

    /// Work out what an apply would change, and change none of it.
    ///
    /// Off by default. A dry run here does the expensive half and skips the cheap
    /// half.
    /// It probes the destination against the version exactly as a real apply
    /// would.
    /// It also reads every chunk it is about to write back out of the pack.
    /// It checks each chunk's hash against the hash it is addressed by.
    /// A dry run does not open a file. It does not create a directory, set an
    /// attribute, prune an entry, or record the manifest an apply leaves beside the
    /// destination.
    ///
    /// So it costs as much as the apply it replaces and mutates nothing.
    /// That is the whole reason it exists.
    /// An apply prints its figure after it has rewritten the tree.
    /// A dry run reports that figure before.
    pub dry_run: bool,
}

/// What an [`apply`] run changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    /// Chunks the destination already held correctly, and did not have to be sent.
    pub held: u64,

    /// Distinct chunks the version needs.
    pub chunks: u64,

    /// Bytes of the pack a fetch would have needed to reach this version.
    ///
    /// Measured by the probe this run took, so it is reported here.
    /// A caller cannot probe again afterwards, because by then the destination
    /// holds the version by construction and would report a figure of nothing.
    /// This is the number that says whether a run was an update or a whole tree.
    pub fetched: u64,

    /// How many range requests [`Self::fetched`] would have been.
    ///
    /// Separate from the byte count because the *count* is the property packing
    /// exists for: one request, not one per chunk, however small the payload.
    pub ranges: usize,

    /// Files opened and written to.
    pub files: u64,

    /// Files this run took on a manifest's word and did not read.
    ///
    /// The measure of the fast path, and of whether it engaged.
    /// Zero means every file was read.
    /// That is what `--recheck` asks for, and what a first apply has to do.
    /// A caller that sees `held` at its maximum and `written` at zero is looking
    /// at a run that cost a stat per file.
    pub trusted: u64,

    /// Distinct chunks read out of the pack and written.
    pub written: u64,

    /// Bytes of file content written.
    pub bytes: u64,

    /// Entries removed, which is zero unless [`Options::prune`] was set.
    pub removed: u64,

    /// Entries the version does not record, whether or not they were removed.
    pub extra: u64,

    /// Entries that recorded an owner this process could not set.
    ///
    /// Every one of them, for an unprivileged caller applying to files it does not
    /// own. Their contents, modes and times are still correct. Only the owner is
    /// not what the version recorded.
    ///
    /// On a dry run this is a bound rather than a count.
    /// It is how many entries record an owner at all, which is the most that could
    /// fail to have it set.
    /// Whether an owner *can* be set is only knowable by setting it.
    /// Reporting zero would claim a tree could be fully owned on the strength of a
    /// run that touched nothing.
    pub unowned: u64,
}

/// Make `root` match the version `pack` carries, writing only what is missing.
///
/// The pack is read for its file table as well as its payload, so nothing else is
/// needed. No set is needed. No snapshot is fetched separately. There is no second
/// file to keep in step with the first.
///
/// # Errors
///
/// Returns an error if the pack's file table cannot be read or decoded.
/// Returns an error if the file table records a path that would land outside
/// `root`.
/// Returns an error if the recorded slice lengths do not account for each file's
/// length.
/// Returns an error if a chunk about to be written does not hash to the hash it is
/// addressed by.
/// Returns an error if the tree cannot be written.
/// Returns an error if the tree already at `root` cannot be read.
/// Every one of those is checked before the first byte is written.
/// It also returns an error if a link task or an attribute task stopped before
/// it finished.
/// A run that fails during verification leaves the destination exactly as it
/// found it.
/// A run that fails later may have written some of the delta.
/// Running the same run again finishes it.
#[instrument(level = "info", skip_all, err)]
pub async fn apply<P: AsRef<Path>>(root: P, pack: &Pack, options: Options) -> io::Result<Applied> {
    let root = root.as_ref();

    // The file table out of the pack, checked against the hash the header names
    // before it is parsed. See `Pack::snapshot`.
    let snapshot = pack.snapshot()?;

    // Both before anything is written, and for the reasons a restore checks.
    // A path that escapes the root cannot be laid down at all.
    // A length that does not account for its own slices cannot be laid down at
    // all.
    // Finding out afterwards leaves a directory that is neither the old one nor
    // the new one.
    super::restore::check_paths(&snapshot)?;
    snapshot.verify_file_lengths("does not match its own chunks")?;

    // What is already accounted for, and so need not be read.
    // Two sources, and they are independent.
    // A manifest covers *files* by stat, and a delta pack's base covers *chunks*
    // it left out.
    // See `readable_from_manifest` and `establish_base` for why neither is taken
    // on trust.
    let mut established = if options.recheck {
        super::probe::Established::default()
    } else {
        readable_from_manifest(root, pack.snapshot_id(), &snapshot)?
    };

    establish_base(root, pack, &snapshot, &mut established)?;

    let probed = super::probe::probe_skipping(root, &snapshot, pack.clone(), &established).await?;

    // One walk of the records, two figures from it.
    // Asked separately it was two walks of every record.
    // That is the whole index twice over for a pair of numbers that describe the
    // same ranges.
    let needed = pack.needed(&probed.held);
    let fetched = needed
        .iter()
        .fold(0u64, |total, range| total.saturating_add(range.len()));
    let ranges = needed.len();

    // Before the first byte. See the module docs.
    // A damaged pack would otherwise be written over a correct directory.
    // A probe against a damaged pack finds everything absent.
    verify_delta(&probed, pack).await?;

    // And everything after here is a write, or a consequence of one.
    // The counts the run would have reported are already in hand.
    // The probe found every slice that is not right.
    // `verify_delta` has read the bytes behind them.
    // The counts are derived below rather than by doing the work.
    if options.dry_run {
        let wanted = DryRun { fetched, ranges };
        return Ok(would_write(
            root,
            &snapshot,
            &probed,
            &established,
            options,
            &wanted,
        ));
    }

    let wanted: HashSet<(&Path, u64)> = probed
        .mismatched
        .iter()
        .map(|m| (m.path.as_path(), m.offset))
        .collect();

    // In this order, and the order is three sets of constraints rather than a
    // habit:
    //
    // * directories first, so every parent exists
    // * files before links, because a hard link's target has to exist
    // * prune before attributes, because removing an entry updates the
    //   modification time of the directory holding it. Setting times last is the
    //   only order in which a surviving directory keeps the time the version
    //   records for it.
    super::restore::restore_directories(root, &snapshot).await?;
    let written = write_files(root, &snapshot, pack, &wanted).await?;

    // Off the runtime thread, for the same reason a restore does it.
    // Creating links and setting attributes is plain synchronous filesystem work.
    // A tree of many links is where blocking a worker thread shows.
    let for_links = root.to_path_buf();
    let for_links_snapshot = snapshot.clone();
    tokio::task::spawn_blocking(move || {
        super::restore::restore_links(&for_links, &for_links_snapshot)
    })
    .await
    .map_err(|e| io::Error::other(format!("a link task failed: {e}")))??;

    let removed = if options.prune {
        prune(root, &probed.extra)?
    } else {
        0
    };

    let for_attrs = root.to_path_buf();
    let for_attrs_snapshot = snapshot.clone();
    let unowned = tokio::task::spawn_blocking(move || {
        super::restore::restore_attributes(&for_attrs, &for_attrs_snapshot)
    })
    .await
    .map_err(|e| io::Error::other(format!("an attribute task failed: {e}")))??;

    // Last, and only on a run that got this far.
    // A manifest describes what the tree is now.
    // Writing one before the attribute stage would record lengths and times this
    // apply is still about to change.
    //
    // A failure here is logged and not returned.
    // The tree is correct, and a caller told its apply failed would be told
    // something untrue.
    // The cost of losing the manifest is one full read next time.
    if let Err(e) = write_manifest(root, pack, &snapshot) {
        tracing::warn!(%e, "could not record what this apply left; the next one will read the tree");
    }

    Ok(Applied {
        held: u64::try_from(probed.held.len()).unwrap_or(0),
        chunks: u64::try_from(snapshot.chunks.len()).unwrap_or(0),
        fetched,
        ranges,
        trusted: u64::try_from(established.files.len()).unwrap_or(0),
        files: written.files,
        written: written.chunks.len().try_into().unwrap_or(0),
        bytes: written.bytes,
        removed,
        extra: probed.extra.len().try_into().unwrap_or(0),
        unowned,
    })
}

/// The two figures a pack would be fetched with, carried into the plan so it can
/// report them without asking the pack again.
struct DryRun {
    fetched: u64,
    ranges: usize,
}

/// What an apply would change, counted from the probe it already took.
///
/// Every figure is the one a real run reports, and each comes from the same place
/// the write stage takes it from.
/// [`Probed::mismatched`] is the set of recorded slices whose bytes are not
/// currently the right bytes.
/// The sum of their lengths is what `write_files` would write.
/// The set of their chunks is what it would put on disk.
/// The two counters above them are the ones a fetch would be sized by.
/// They come off the pack rather than off the destination.
///
/// The file count is the subtler one, because `write_files` opens a file for a
/// reason beyond having something to write.
/// A file with no mismatched slice but the wrong length is still opened, and
/// truncated to the version's length.
/// The same two conditions are applied here in the same order.
/// Otherwise a plan would report fewer files than the apply would touch.
fn would_write(
    root: &Path,
    snapshot: &metadata::Snapshot,
    probed: &Probed,
    established: &super::probe::Established<'_>,
    options: Options,
    transfer: &DryRun,
) -> Applied {
    let mut chunks: HashSet<BlobId> = HashSet::new();
    let mut bytes = 0u64;
    let mut mismatched_in: HashSet<&Path> = HashSet::new();

    for mismatch in &probed.mismatched {
        chunks.insert(mismatch.expected);
        bytes = bytes.saturating_add(mismatch.length);
        mismatched_in.insert(mismatch.path.as_path());
    }

    let mut files = 0u64;
    for file in &snapshot.files {
        // `write_files` skips a file with nothing to write and the right length
        // without opening it, and opens every other one. Same test, same order.
        if mismatched_in.contains(file.path.as_path()) || needs_touching(root, &file.path, file.len)
        {
            files = files.saturating_add(1);
        }
    }

    // Not derived from the mismatch set, and for a reason worth stating.
    // This is the one figure an apply reports about entries it does *not* touch.
    // There are none of those here.
    // It is the length of what a `--prune` would remove, which is what the same
    // run would report.
    let extra = u64::try_from(probed.extra.len()).unwrap_or(0);

    Applied {
        held: u64::try_from(probed.held.len()).unwrap_or(0),
        chunks: u64::try_from(snapshot.chunks.len()).unwrap_or(0),
        fetched: transfer.fetched,
        ranges: transfer.ranges,
        files,
        trusted: u64::try_from(established.files.len()).unwrap_or(0),
        written: u64::try_from(chunks.len()).unwrap_or(0),
        bytes,
        removed: if options.prune { extra } else { 0 },
        extra,
        unowned: owners(snapshot),
    }
}

/// How many entries a version records an owner for.
///
/// The most that could fail to have an owner set.
/// This is the reason [`Applied::unowned`] carries a bound rather than a count on
/// a dry run.
/// Whether an owner can be set is only knowable by setting it.
/// Every entry that would be tried is named here before any of them is.
///
/// Shared with [`super::restore::restore_plan`], which reports the same bound for
/// the same reason.
/// A restore that did not set the owner is not a failure.
/// A plan that could not report it would leave the figure out of the one run that
/// does.
pub(super) fn owners(snapshot: &metadata::Snapshot) -> u64 {
    let files = snapshot
        .files
        .iter()
        .filter(|file| file.attrs.owner.is_some())
        .count();
    let dirs = snapshot
        .dirs
        .iter()
        .filter(|dir| dir.attrs.owner.is_some())
        .count();

    u64::try_from(files.saturating_add(dirs)).unwrap_or(0)
}

/// Record what this apply left, beside the destination.
///
/// Built from the version rather than from the filesystem.
/// `file.len` and `file.attrs.mtime` are what the attribute stage has just
/// imposed, so they are the tree's state by the time this runs.
///
/// # Errors
///
/// Returns an error if the manifest's path cannot be formed or written.
/// Callers are expected to log and carry on. See the call site.
fn write_manifest(root: &Path, pack: &Pack, snapshot: &metadata::Snapshot) -> io::Result<PathBuf> {
    let manifest = super::manifest::Manifest::record(root, pack.snapshot_id(), snapshot)?;
    manifest.save(root)
}

/// What a manifest lets this run skip.
///
/// The interesting cases are the ones that make this *empty*, and none of them is
/// an error.
/// The cases are no manifest, a manifest naming another version, or one file having
/// been touched since.
/// Each leaves more to read, never a wrong answer.
///
/// Borrowed from the snapshot rather than from the manifest, so the set outlives
/// this function without an allocation per file.
/// The manifest's `untrusted` names recorded paths, so a version's file it does not
/// name is never skipped.
/// That is correct: a file the manifest never wrote about is read.
fn readable_from_manifest<'a>(
    root: &Path,
    version: BlobId,
    snapshot: &'a metadata::Snapshot,
) -> io::Result<super::probe::Established<'a>> {
    let empty = super::probe::Established::default();

    let Some(manifest) = super::manifest::Manifest::load(root)? else {
        return Ok(empty);
    };

    if manifest.version != version {
        // A manifest for another version describes chunk placement this one does
        // not share, so its records say nothing about this one. The files may well
        // be untouched, but nothing recorded here can tell, and the honest answer
        // is to read them.
        tracing::debug!(
            recorded = %manifest.version,
            applying = %version,
            "manifest is for another version; reading the tree instead"
        );
        return Ok(empty);
    }

    let untrusted = manifest.untrusted(root, snapshot);

    Ok(super::probe::Established {
        files: snapshot
            .files
            .iter()
            .map(|file| file.path.as_path())
            .filter(|path| !untrusted.iter().any(|one| one == path))
            .collect(),
        ..empty
    })
}

/// Account for the chunks a delta pack left out, or refuse the run.
///
/// **This is the only thing making a delta pack safe. It is a refusal.**
/// A delta holds the chunks its reader is asserted to have.
/// The bytes that would prove it has them are exactly the bytes the delta left
/// out, so nothing can be verified.
/// Reading them is what a probe does normally, and a delta exists to avoid it.
///
/// So the assertion has to come from somewhere else, and the only somewhere is the
/// reader.
/// A manifest naming exactly the base version says the directory *is* that version.
/// That is what the last apply left, and nothing since has moved a byte of it.
/// The run is refused when the manifest says otherwise.
/// The manifest says otherwise when there is no manifest, or a different version.
///
/// The alternative is to apply what the delta happens to contain and report
/// success.
/// That is the worst thing this crate can do.
/// The result is a directory that is neither version, with nothing in the output to
/// say so.
///
/// # Errors
///
/// Returns an error when the pack is a delta and the destination cannot account for
/// its base, before anything is read or written.
fn establish_base<'a>(
    root: &Path,
    pack: &Pack,
    snapshot: &'a metadata::Snapshot,
    established: &mut super::probe::Established<'a>,
) -> io::Result<()> {
    let Some(base) = pack.base() else {
        return Ok(());
    };

    let manifest = super::manifest::Manifest::load(root)?;

    if manifest.as_ref().is_none_or(|m| m.version != base) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to apply: this pack is a delta for a client already holding \
                 version {base}, and {} does not record holding it. A delta omits the \
                 chunks that version has, so nothing here could tell you were \
                 missing them — apply the full pack instead, or apply the base \
                 version first",
                manifest.as_ref().map_or_else(
                    || format!("{}", root.display()),
                    |m| format!("{} records version {}", root.display(), m.version),
                )
            ),
        ));
    }

    // The manifest says the directory is the base, so every chunk the delta left
    // out is in it.
    // The manifest itself does not list chunks. The base's snapshot does.
    // The delta carries that snapshot, and its own file table is the *target's*.
    // The omitted set is computed by difference against what the delta does hold.
    let held: HashSet<&BlobId> = pack.chunks().iter().map(|chunk| &chunk.id).collect();

    established.chunks = snapshot
        .chunks
        .iter()
        .filter(|id| !held.contains(id))
        .collect();

    tracing::debug!(
        base = %base,
        chunks = established.chunks.len(),
        "delta pack: the manifest accounts for the chunks it omits"
    );

    Ok(())
}

/// Hash every chunk about to be written and refuse the run if one does not match.
///
/// Whole chunks rather than the slices being written.
/// A slice can be part of a chunk, and the hash the chunk is addressed by covers
/// all of it.
/// A chunk the pack has damaged at one end is not trustworthy at either.
async fn verify_delta(probed: &Probed, pack: &Pack) -> io::Result<()> {
    let needed: HashSet<BlobId> = probed.mismatched.iter().map(|m| m.expected).collect();

    for id in &needed {
        let mut reader = pack.get(id).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        if blake3::hash(&buffer) != *id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "refusing to apply: the bytes this pack holds for chunk {id} do not \
                     hash to it"
                ),
            ));
        }
    }

    Ok(())
}

/// Bytes and chunks written across every file.
struct Written {
    files: u64,
    bytes: u64,
    chunks: HashSet<BlobId>,
}

/// Write the recorded slices that are not currently the right bytes.
///
/// A file with nothing to write is **not opened**.
/// That is the whole saving of a delta apply.
/// It is more than not re-sending bytes.
/// An unchanged file keeps its inode, its timestamps, and whatever the filesystem
/// has cached about it.
async fn write_files(
    root: &Path,
    snapshot: &metadata::Snapshot,
    pack: &Pack,
    wanted: &HashSet<(&Path, u64)>,
) -> io::Result<Written> {
    // Grouped by *index* into `file_chunks`, for the reason `restore_files` groups
    // the same way.
    // Copying the slices would cost a second full set on top of the version.
    // Cloning the version would cost an allocation per chunk.
    let mut slices: Vec<Vec<usize>> = vec![Vec::new(); snapshot.files.len()];
    for (position, slice) in snapshot.file_chunks.iter().enumerate() {
        let index = usize::try_from(slice.file_index)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "a slice names a file"))?;
        let bucket = slices.get_mut(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk slice references file {index}, which the version does not contain"),
            )
        })?;
        bucket.push(position);
    }

    let snapshot = Arc::new(snapshot.clone());
    let pack = Arc::new(pack.clone());

    let concurrency = apply_concurrency();
    let total = u64::try_from(snapshot.files.len()).unwrap_or(0);

    let mut progress = Progress::new(Phase::Apply);
    progress.set(Counter::Files, total);
    progress.report();

    let mut written = Written {
        files: 0,
        bytes: 0,
        chunks: HashSet::new(),
    };

    let mut writing = JoinSet::new();
    let mut next = 0usize;

    loop {
        while next < snapshot.files.len() && writing.len() < concurrency {
            let file = &snapshot.files[next];
            let recorded = file.path.clone();

            let to_write: Vec<usize> = slices[next]
                .iter()
                .copied()
                .filter(|position| {
                    let Some(slice) = snapshot.file_chunks.get(*position) else {
                        return false;
                    };
                    wanted.contains(&(recorded.as_path(), slice.file_offset))
                })
                .collect();

            next += 1;

            if to_write.is_empty() && !needs_touching(root, &recorded, file.len) {
                // Nothing to write and the right length: leave it entirely alone.
                progress.add(Counter::Done, 1);
                progress.report();
                continue;
            }

            let job = FileJob {
                path: super::restore::resolve(root, &recorded, "a file")?,
                len: file.len,
                slices: to_write,
                snapshot: Arc::clone(&snapshot),
                pack: Arc::clone(&pack),
            };

            writing.spawn(async move { write_file(job).await });
        }

        let Some(result) = writing.join_next().await else {
            break;
        };

        match result {
            Ok(Err(e)) => return Err(e),
            Err(join) => return Err(io::Error::other(format!("a write task failed: {join}"))),
            Ok(Ok(done)) => {
                progress.add(Counter::Done, 1);
                progress.add(Counter::Written, done.bytes);
                progress.report();

                written.files = written.files.saturating_add(1);
                written.bytes = written.bytes.saturating_add(done.bytes);
                written.chunks.extend(done.chunks);
            }
        }
    }

    crate::telemetry::metrics::record_run(&progress);

    Ok(written)
}

/// Whether a file with nothing to write still has to be opened.
///
/// Two cases.
/// The first is that the file is not there at all.
/// An empty file the version records but the destination lacks has no slices, and
/// so no mismatch.
/// The second case is that its length differs from the version's.
/// For a file whose slices are all correct, that means the file is too long.
/// Its tail is not the version's.
///
/// The length is read rather than assumed.
/// Opening a file for write to discover there is nothing to do is not free.
/// Touching it needlessly is exactly what this function exists to avoid.
fn needs_touching(root: &Path, recorded: &Path, len: u64) -> bool {
    match std::fs::metadata(root.join(recorded)) {
        Ok(meta) => meta.len() != len,
        Err(_) => true,
    }
}

/// One file's worth of work, owned so it can outlive the loop that queued it.
struct FileJob {
    /// Where the file is, already joined onto the root.
    path: PathBuf,
    /// The length the version records.
    len: u64,
    /// Positions in `snapshot.file_chunks` to write.
    slices: Vec<usize>,
    snapshot: Arc<metadata::Snapshot>,
    pack: Arc<Pack>,
}

/// What one file's slices came to.
struct FileWritten {
    bytes: u64,
    chunks: HashSet<BlobId>,
}

/// Write one file's missing slices.
///
/// **Opened, never truncated.**
/// A file being patched has to keep the bytes this is not rewriting.
/// That is the entire premise, so the length is set at the end rather than the
/// beginning.
/// A restore creates at the final length because it writes every byte and a short
/// file is never observable.
/// Here the file starts out correct, and shrinking it first would destroy bytes
/// this run has not yet replaced.
/// That is a strictly worse failure than a file that is briefly longer than the
/// version it is becoming.
async fn write_file(job: FileJob) -> io::Result<FileWritten> {
    let FileJob {
        path,
        len,
        slices,
        snapshot,
        pack,
    } = job;

    if let Some(parent) = path.parent() {
        tfs::create_dir_all(parent).await?;
    }

    let mut handle = tfs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await?;

    let mut out = FileWritten {
        bytes: 0,
        chunks: HashSet::with_capacity(slices.len()),
    };

    for position in slices {
        let Some(slice) = snapshot.file_chunks.get(position) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk slice {position} is not in the version"),
            ));
        };

        let Some(chunk) = snapshot.chunks.get(slice.chunk_index as usize).copied() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunk slice references chunk {}, which the version does not contain",
                    slice.chunk_index
                ),
            ));
        };

        let length = u64::from(slice.length);

        // Bounded on both sides: the source cannot read past this slice of the
        // chunk, and the sink cannot write past it. A `length` that came out of a
        // downloaded pack is not this crate's to trust with an unbounded copy.
        let mut source = pack.get(&chunk).await?;
        source
            .seek(io::SeekFrom::Start(u64::from(slice.chunk_offset)))
            .await?;
        let mut source = crate::slice::SliceAsyncReader::new(source, length);

        let mut buffer = vec![
            0u8;
            usize::try_from(length).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("a slice of {length} bytes cannot be addressed on this platform"),
                )
            })?
        ];
        source.read_exact(&mut buffer).await?;

        handle.seek(io::SeekFrom::Start(slice.file_offset)).await?;
        handle.write_all(&buffer).await?;

        out.bytes = out.bytes.saturating_add(length);
        out.chunks.insert(chunk);
    }

    // Closes a gap *between* slices, which would otherwise leave zeroes mid-file,
    // and truncates a file the version is shorter than.
    handle.set_len(len).await?;

    Ok(out)
}

/// Remove the entries the version does not record, deepest first.
///
/// Deepest first, because a directory cannot be removed while it still has
/// contents in it.
/// The entries inside it are also extras.
/// Sorting by component count is a blunt instrument.
/// It gets the *order* right, which is all that is needed.
/// Sorting lexicographically would get it wrong for `a-b/c` against `a/b`.
fn prune(root: &Path, extra: &[PathBuf]) -> io::Result<u64> {
    let mut ordered: Vec<&PathBuf> = extra.iter().collect();
    ordered.sort_by_key(|path| std::cmp::Reverse(path.components().count()));

    let mut removed = 0u64;
    for relative in ordered {
        super::restore::remove_existing(&root.join(relative))?;
        removed = removed.saturating_add(1);
    }

    Ok(removed)
}
