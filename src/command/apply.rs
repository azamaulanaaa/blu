//! Making a directory match the version a pack carries.
//!
//! A restore writes every byte a version needs, because its bytes come out of a
//! store one chunk at a time and it cannot know which of them the destination
//! already has. An apply is the same walk with one difference that changes
//! everything: it can know, and so it writes only the bytes that are missing.
//!
//! ## `held` is for planning, `mismatched` is for writing
//!
//! These are two different answers and conflating them is a bug worth naming,
//! because both look like "what the directory already has".
//!
//! [`Probed::held`] is per *chunk*, and it exists to answer a question about the
//! transfer: [`Pack::needed`](crate::pack::Pack::needed) skips a range request for
//! every chunk in it, so a chunk is in it only if the directory holds it
//! correctly everywhere the version places it.
//!
//! [`Probed::mismatched`] is per *slice*, and it answers a question about the
//! disk: which recorded byte ranges are not currently the right bytes. That is the
//! set an apply writes — and it is not derivable from `held`. A file that is
//! missing entirely has every one of its slices reported [`Unheld::Absent`], even
//! where its chunks are shared with another file that is perfectly correct. Judge
//! that file by `held` and those slices are skipped, and the file is created
//! empty.
//!
//! ## A corrupt pack must not reach the disk
//!
//! The probe compares the directory against the chunk's bytes *as the pack gives
//! them*, so a damaged pack makes everything look absent and would otherwise be
//! written over a correct directory. Every chunk about to be written is therefore
//! hashed and compared against the hash it is addressed by **before a byte is
//! written**, in one pass over the delta — and a failure there leaves the
//! directory untouched. The cost is reading the delta twice: once to check it and
//! once to write it. That is the same trade a restore makes, for the same reason,
//! and on a delta it is a delta-sized cost.
//!
//! ## What this does not do
//!
//! It is not atomic, and it cannot be without a copy. A failure partway through
//! leaves a directory that is neither the old version nor the new one — but
//! because a probe is idempotent, running the same apply again finishes the job.
//! That is the recovery, and it is why nothing here tries to be cleverer.
//!
//! Pruning is opt-in and last-but-one. An entry the version does not record is as
//! likely to be the user's own work as a leftover from an older version, so it is
//! reported by default and removed only when asked.

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
    /// Off by default, and the default is the decision. A file in the destination
    /// that this version does not mention is either something the user made or
    /// something an older version left behind, and nothing in the version can tell
    /// those apart. A tool that deleted the first to satisfy the second would be
    /// destroying data on a guess.
    pub prune: bool,
}

/// What an [`apply`] run changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    /// Chunks the destination already held correctly, and were not sent.
    pub held: u64,

    /// Files opened and written to.
    pub files: u64,

    /// Distinct chunks read out of the pack and written.
    pub chunks: u64,

    /// Bytes of file content written.
    pub bytes: u64,

    /// Entries removed, which is zero unless [`Options::prune`] was set.
    pub removed: u64,

    /// Entries the version does not record, whether or not they were removed.
    pub extra: u64,
}

/// Make `root` match the version `pack` carries, writing only what is missing.
///
/// The pack is read for its file table as well as its payload, so nothing else is
/// needed: no set, no snapshot fetched separately, no second file to keep in step
/// with the first.
///
/// # Errors
///
/// Returns an error if the pack's file table cannot be read or decoded, if it
/// records a path that would land outside `root`, if its recorded slice lengths do
/// not account for each file's length, if a chunk it is about to write does not
/// hash to the hash it is addressed by, or if the tree cannot be written. Every
/// one of those is checked before the first byte is written, so a run that fails
/// during verification leaves the destination exactly as it found it; a run that
/// fails later may have written some of the delta, and is finished by running
/// itself again.
#[instrument(level = "info", skip_all, err)]
pub async fn apply<P: AsRef<Path>>(root: P, pack: &Pack, options: Options) -> io::Result<Applied> {
    let root = root.as_ref();

    // The file table out of the pack, checked against the hash the header names
    // before it is parsed — see `Pack::snapshot`.
    let snapshot = pack.snapshot()?;

    // Both before anything is written, and for the reasons a restore checks: a
    // path that escapes the root, or a length that does not account for its own
    // slices, is a version that cannot be laid down at all, and finding out
    // afterwards leaves a directory that is neither the old one nor the new one.
    super::restore::check_paths(&snapshot)?;
    snapshot.verify_file_lengths("does not match its own chunks")?;

    let probed = super::probe::probe(root, &snapshot, pack.clone()).await?;

    // Before the first byte. See the module docs: a damaged pack would otherwise
    // be written over a correct directory, because a probe against a damaged pack
    // finds everything absent.
    verify_delta(&probed, pack).await?;

    let wanted: HashSet<(&Path, u64)> = probed
        .mismatched
        .iter()
        .map(|m| (m.path.as_path(), m.offset))
        .collect();

    // In this order, and the order is three sets of constraints rather than a
    // habit:
    //
    // * directories first, so every parent exists;
    // * files before links, because a hard link's target has to exist;
    // * prune before attributes, because removing an entry updates the
    //   modification time of the directory holding it — so setting times last is
    //   the only order in which a surviving directory keeps the time the version
    //   records for it.
    super::restore::restore_directories(root, &snapshot).await?;
    let written = write_files(root, &snapshot, pack, &wanted).await?;

    // Off the runtime thread, for the same reason a restore does it: creating links
    // and setting attributes is plain synchronous filesystem work, and a tree of
    // many links is where blocking a worker thread shows.
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
    tokio::task::spawn_blocking(move || {
        super::restore::restore_attributes(&for_attrs, &for_attrs_snapshot)
    })
    .await
    .map_err(|e| io::Error::other(format!("an attribute task failed: {e}")))??;

    Ok(Applied {
        held: u64::try_from(probed.held.len()).unwrap_or(0),
        files: written.files,
        chunks: written.chunks.len().try_into().unwrap_or(0),
        bytes: written.bytes,
        removed,
        extra: probed.extra.len().try_into().unwrap_or(0),
    })
}

/// Hash every chunk about to be written and refuse the run if one does not match.
///
/// Whole chunks rather than the slices being written, because a slice can be part
/// of a chunk and the hash the chunk is addressed by covers all of it — and because
/// a chunk the pack has damaged at one end is not trustworthy at either.
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
/// A file with nothing to write is **not opened**. That is the whole saving of a
/// delta apply, and it is more than not re-sending bytes: an unchanged file keeps
/// its inode, its timestamps and whatever the filesystem has cached about it.
async fn write_files(
    root: &Path,
    snapshot: &metadata::Snapshot,
    pack: &Pack,
    wanted: &HashSet<(&Path, u64)>,
) -> io::Result<Written> {
    // Grouped by *index* into `file_chunks`, for the reason `restore_files` groups
    // the same way: copying the slices would cost a second full set on top of the
    // version, and cloning it would cost an allocation per chunk.
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

    Ok(written)
}

/// Whether a file with nothing to write still has to be opened.
///
/// Two cases: it is not there at all — an empty file the version records but the
/// destination lacks, which has no slices and so no mismatch — or its length
/// differs from the version's, which for a file whose slices are all correct means
/// it is too long and the tail is not the version's.
///
/// The length is read rather than assumed because opening a file for write to
/// discover there is nothing to do is not free, and touching it needlessly is
/// exactly what this function exists to avoid.
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
/// **Opened, never truncated.** A file being patched has to keep the bytes this is
/// not rewriting — that is the entire premise — so the length is set at the end
/// rather than the beginning. A restore creates at the final length because it
/// writes every byte and a short file is never observable; here the file starts out
/// correct and shrinking it first would destroy bytes this run has not yet
/// replaced, which is a strictly worse failure than a file that is briefly longer
/// than the version it is becoming.
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
/// Deepest first because a directory cannot be removed while it still has contents
/// in it, and the entries inside it are also extras. Sorting by component count is
/// a blunt instrument — it gets the *order* right, which is all that is needed,
/// where sorting lexicographically would get it wrong for `a-b/c` against `a/b`.
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
