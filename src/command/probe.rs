//! Asking what a directory already holds.
//!
//! A pack is a delta waiting for a client. To know which ranges of it to fetch,
//! a client needs one thing: the set of chunks it already has, correctly. That is
//! what a probe produces, and the answer is per chunk and nothing coarser.
//!
//! ## Why not hash the tree and compare
//!
//! The obvious approach — walk the target, hash it, compare against the version —
//! cannot work here, for two separate reasons, and both matter.
//!
//! A version is not named by a hash of its contents. It is named by the hash of
//! the *snapshot blob*, which is the encoded file table: paths, lengths,
//! permissions and modification times alongside the chunk list. Two trees with
//! identical bytes and one differing `touch` are different versions. So there is
//! no "hash of version 6" for a directory to be compared against even in
//! principle.
//!
//! And re-chunking the target would not reproduce the version's chunks anyway.
//! Boundaries are content-defined over all the files read as one concatenated
//! stream, so they depend on the bytes that came before and on the order the walk
//! produced — a tree with one extra file in it chunks differently, and so does the
//! same tree read in a different order.
//!
//! ## So: verify the slices the version records, against the chunk's bytes
//!
//! The version says file `F`, from offset `O`, for `L` bytes, holds bytes
//! `C..C+L` of chunk `H`. That is checkable without a chunker, by reading both
//! sides and comparing them.
//!
//! **Comparing against the bytes rather than against the hash** is not a
//! preference, and getting it wrong is silent. A `FileChunk` records a
//! `chunk_offset`, so a slice is usually *part* of a chunk: the last chunk of a
//! file runs on into the next one. The bytes at `F[O..O+L]` are then not the whole
//! of `H`, and hashing them produces something that is never equal to `H` — so a
//! probe that hashes the file slice and compares to the chunk's hash condemns
//! every boundary-crossing slice in the tree, forever, and reports a target that is
//! byte-for-byte correct as holding nothing. The chunk's own bytes have to be read
//! to compare the two, which is why a probe is given a source and not only a
//! version.
//!
//! ## All-or-nothing, and that is load-bearing
//!
//! One chunk can be placed at several slices — repeated content, or one spanning a
//! file boundary — and `held` is what [`Pack::needed`](crate::pack::Pack::needed)
//! consults to decide a chunk needs no range request. A chunk recorded as held on
//! the strength of three correct slices out of four would be skipped by the fetch
//! and the fourth left wrong, and the client would believe it had applied the
//! version. So the question is not "is this chunk right somewhere" but "is this
//! chunk right everywhere it appears", and a probe that answered the first would
//! be worse than one that does not answer at all: it looks like an optimisation
//! and it is a silent corruption.
//!
//! That also makes a probe idempotent, which is what lets an interrupted apply be
//! resumed by running it again — and it is why no probe needs to name a base
//! version. A client can be holding v4, v6, or half of each, and the same probe
//! answers correctly for all three.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt},
    task::JoinSet,
};
use tracing::instrument;

use crate::{
    metadata::Snapshot,
    storage::{BlobId, StorageGet},
};

/// Chunks to check at once. Reading one is a read and then a comparison per slice
/// placing it, with nothing to overlap inside either, so this is all that keeps the
/// disk busy.
const PROBE_CONCURRENCY: usize = 8;

/// How many chunks to check at once.
fn probe_concurrency() -> usize {
    super::concurrency(PROBE_CONCURRENCY)
}

/// Why the target does not hold the bytes a version records.
///
/// Not named `Reason`, which [`super::verify`] already uses for why a chunk could
/// not be checked out. The two are the same shape and different questions, and a
/// caller holding a failure from either should be able to tell which they have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unheld {
    /// The file the version records is not there.
    Absent,

    /// The file ends before the slice does.
    Short,

    /// The file continues past the last slice the version records.
    ///
    /// The one difference no slice can see. Every recorded slice of a file that
    /// has grown can verify perfectly while a tail that is not the version's sits
    /// past the end of them — and a probe that could not see that would answer
    /// "holds this version" about a file with somebody else's data in it.
    Long,

    /// The bytes there hash to something else.
    ///
    /// Not necessarily corruption: this is also what a file the *user* edited
    /// looks like, which is the ordinary case for a client applying an update to a
    /// tree someone is using.
    Content,
}

/// One recorded slice the target did not hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// The recorded path, relative to the root.
    pub path: PathBuf,
    /// Where in the file the slice belongs.
    pub offset: u64,
    /// How many bytes the version says belong there.
    pub length: u64,
    /// The hash those bytes must have.
    pub expected: BlobId,
    /// Why the target does not hold them.
    pub reason: Unheld,
}

/// A file whose length is not the version's, for a reason no slice reported.
///
/// Reported only where a slice could not have. A file *shorter* than the version is
/// already reported slice by slice as [`Unheld::Short`], and saying it again once
/// per file would be one finding three times over; a file *longer* is reported here
/// and nowhere else, because there is no slice out past the end of one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mislength {
    /// The recorded path, relative to the root.
    pub path: PathBuf,
    /// The length the version records.
    pub expected: u64,
    /// The length on disk.
    pub actual: u64,
    /// Why it differs.
    pub reason: Unheld,
}

/// What a target directory holds of a version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Probed {
    /// Chunks the target holds correctly, at every slice the version places them.
    ///
    /// The answer a delta needs: pass it to
    /// [`Pack::needed`](crate::pack::Pack::needed) and the ranges it returns are
    /// exactly the bytes this directory is missing.
    pub held: HashSet<BlobId>,

    /// Slices that are not right, in the order the version records them.
    pub mismatched: Vec<Mismatch>,

    /// Files whose length is not the version's and whose slices could not say so.
    ///
    /// In practice: files that have *grown*. A file that has shrunk is reported
    /// slice by slice, and a file that has not changed is not here.
    pub mislengths: Vec<Mislength>,

    /// Entries the target has that the version does not record.
    ///
    /// Reported, never removed. A file here is as likely to be the user's own work
    /// as a leftover from an older version, and a tool that deleted it to make a
    /// directory match a file would be destroying data it cannot tell apart from
    /// data it should keep — so removing them is a decision the caller has to make
    /// out loud, and [`Probed`] only ever tells it what the decision would cost.
    pub extra: Vec<PathBuf>,
}

impl Probed {
    /// Whether the target *is* this version: every slice right, every length right.
    ///
    /// Content alone, in other words — it says nothing about
    /// [`Probed::extra`], because a directory with a stray file in it still holds
    /// the version even though it is not equal to it.
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.mismatched.is_empty() && self.mislengths.is_empty()
    }
}

/// Check what `root` holds of `snapshot`, without writing to it.
///
/// Reads every slice the version records and hashes it. Nothing is created,
/// truncated or removed, so this is safe to run against a tree in use — it may
/// cost a full read of it, which is the price of knowing.
///
/// # Errors
///
/// Returns an error if the version records a path that would land outside `root`,
/// if it names a file or chunk it does not itself contain, if a chunk it needs
/// cannot be read from `source`, or if a recorded file cannot be opened or read
/// for a reason that is not "it is not there". A missing file is a finding and
/// lands in [`Probed::mismatched`] as [`Unheld::Absent`] rather than failing the
/// probe: a client asking "what is missing" is asking precisely because some of it
/// is.
///
/// Generic over [`StorageGet`] because that is what a chunk's bytes come from — a
/// pack for a client updating from one, a store for a tool comparing a directory
/// against a set it can read. It needs somewhere to read chunks from, not a
/// particular kind of somewhere. Taken by value and shared, as a restore takes its
/// store, because each concurrent task holds one.
#[instrument(level = "info", skip_all, err)]
pub async fn probe<P, S>(root: P, snapshot: &Snapshot, source: S) -> std::io::Result<Probed>
where
    P: AsRef<Path>,
    S: StorageGet + 'static,
{
    let root = root.as_ref();
    let source = Arc::new(source);

    // Before a byte is read, and for the same reason a restore checks first: the
    // whole point is to find out about a tree before touching it, and a path that
    // escapes the root is a reason to stop rather than a reason to go and read it.
    super::restore::check_paths(snapshot)?;

    // One task per *chunk*, not per slice: a chunk is read once and compared
    // against every slice placing it, which is both the cheap way round and the
    // only one that can decide "right everywhere" rather than "right somewhere".
    let placed = placements(root, snapshot)?;

    let concurrency = probe_concurrency();
    let mut held = HashSet::with_capacity(placed.len());
    let mut mismatched = Vec::new();

    let mut checking = JoinSet::new();
    let mut next = 0usize;

    loop {
        while next < placed.len() && checking.len() < concurrency {
            let index = next;
            next += 1;

            let id = snapshot.chunks[index];
            let slices = placed[index].clone();
            checking.spawn({
                let source = Arc::clone(&source);
                async move { (index, check_chunk(id, slices, &*source).await) }
            });
        }

        let Some(joined) = checking.join_next().await else {
            break;
        };

        let (index, findings) =
            joined.map_err(|e| std::io::Error::other(format!("a probe task failed: {e}")))?;
        let findings = findings?;

        if findings.is_empty() {
            held.insert(snapshot.chunks[index]);
        } else {
            mismatched.extend(findings);
        }
    }

    // Sorted, so the report is the same whichever order the reads finished in —
    // and a report that reorders itself between two runs of the same tree is a
    // report nobody can diff.
    mismatched.sort_by(|a, b| (&a.path, a.offset).cmp(&(&b.path, b.offset)));

    let seen: HashSet<&Path> = mismatched.iter().map(|m| m.path.as_path()).collect();
    let mut mislengths = mislengths(root, snapshot, &seen)?;
    mislengths.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(Probed {
        held,
        mismatched,
        mislengths,
        extra: extra_entries(root, snapshot),
    })
}

/// Files whose length is not the version's, where no slice already said so.
///
/// `seen` is the files the slice pass reported, and it is what keeps this from
/// repeating a finding. The two directions are treated differently on purpose:
///
/// * **Too long** is only ever reported here. Every recorded slice can verify
///   perfectly while an extra tail sits past the end of them, so no slice can see
///   it, and a probe that missed it would report a directory holding ten kilobytes
///   of somebody's data as *this version*.
///
/// * **Too short** is skipped when a slice already reported it, because the slice
///   is the more specific finding — it says *which* bytes are missing, not just
///   that the file is the wrong size. A file with no slices at all has nothing to
///   report it, so it is reported here.
///
/// * **Missing** likewise, and only for a file with no slices: a file that exists
///   in no slice but not on disk is a finding nothing else would make.
fn mislengths(
    root: &Path,
    snapshot: &Snapshot,
    seen: &HashSet<&Path>,
) -> std::io::Result<Vec<Mislength>> {
    let mut out = Vec::new();

    for file in &snapshot.files {
        let actual = match std::fs::metadata(super::restore::resolve(root, &file.path, "a file")?) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if !seen.contains(file.path.as_path()) {
                    out.push(Mislength {
                        path: file.path.clone(),
                        expected: file.len,
                        actual: 0,
                        reason: Unheld::Absent,
                    });
                }
                continue;
            }
            Err(e) => return Err(e),
        };

        if actual == file.len {
            continue;
        }

        let reason = if actual > file.len {
            Unheld::Long
        } else if seen.contains(file.path.as_path()) {
            // Already reported, slice by slice, with more to say.
            continue;
        } else {
            Unheld::Short
        };

        out.push(Mislength {
            path: file.path.clone(),
            expected: file.len,
            actual,
            reason,
        });
    }

    Ok(out)
}

/// One recorded placement of one chunk, resolved against the root.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Placement {
    /// The path as the version records it, which is what a report names.
    path: PathBuf,
    /// Where that lands, already checked to be inside the root.
    resolved: PathBuf,
    /// Where in the chunk these bytes are.
    chunk_offset: u32,
    /// Where in the file they belong.
    file_offset: u64,
    /// How many of them.
    length: u32,
    /// The hash of the whole chunk, for the report.
    expected: BlobId,
}

/// Every slice of every chunk, grouped by the chunk it places.
///
/// A chunk with no slice is not here at all: the version lists it but reads it
/// nowhere, and a chunk nobody reads is not a thing a directory can be holding.
/// Counting those as missing would make every version report itself as incomplete.
fn placements(root: &Path, snapshot: &Snapshot) -> std::io::Result<Vec<Vec<Placement>>> {
    let mut out: Vec<Vec<Placement>> = vec![Vec::new(); snapshot.chunks.len()];

    for slice in &snapshot.file_chunks {
        let (Ok(chunk), Ok(file)) = (
            usize::try_from(slice.chunk_index),
            usize::try_from(slice.file_index),
        ) else {
            return Err(bad_index(snapshot, slice, "chunk"));
        };

        let file = snapshot
            .files
            .get(file)
            .ok_or_else(|| bad_index(snapshot, slice, "file"))?;
        let bucket = out
            .get_mut(chunk)
            .ok_or_else(|| bad_index(snapshot, slice, "chunk"))?;

        bucket.push(Placement {
            path: file.path.clone(),
            resolved: super::restore::resolve(root, &file.path, "a file")?,
            chunk_offset: slice.chunk_offset,
            file_offset: slice.file_offset,
            length: slice.length,
            expected: *snapshot
                .chunks
                .get(chunk)
                .ok_or_else(|| bad_index(snapshot, slice, "chunk"))?,
        });
    }

    Ok(out)
}

/// The error for a slice naming a file or chunk the version does not contain.
///
/// Names the index and which table it was checked against, because a caller who
/// gets this is holding a damaged version and the whole of it is suspect.
fn bad_index(
    snapshot: &Snapshot,
    slice: &crate::metadata::FileChunk,
    table: &str,
) -> std::io::Error {
    let index = if table == "file" {
        slice.file_index
    } else {
        slice.chunk_index
    };
    let size = if table == "file" {
        snapshot.files.len()
    } else {
        snapshot.chunks.len()
    };

    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "a chunk slice names {table} {index} of {size}, which the version does not contain"
        ),
    )
}

/// Whether every slice of `chunk` is right, and what is wrong with the ones that
/// are not.
///
/// The chunk is read once and buffered. It is bounded by the chunk profile's
/// maximum — 256 KiB at the largest — and buffering it is what lets one read
/// answer for every slice placing it, including the ones that hold only part of
/// it. Streaming instead would mean re-reading the chunk per slice, or a second
/// pass per slice to seek, and the win buys nothing: the caller is reading this
/// chunk from a store or a pack anyway and a restore buffers the same bytes.
async fn check_chunk<S: StorageGet + ?Sized>(
    id: BlobId,
    slices: Vec<Placement>,
    source: &S,
) -> std::io::Result<Vec<Mismatch>> {
    let mut reader = source.get(&id).await?;
    let mut chunk = Vec::new();
    reader.read_to_end(&mut chunk).await?;

    let mut findings = Vec::new();

    for slice in slices {
        let start = usize::try_from(slice.chunk_offset).unwrap_or(usize::MAX);
        let end = start.saturating_add(usize::try_from(slice.length).unwrap_or(usize::MAX));

        let expected = chunk.get(start..end).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "a version places bytes {start}..{end} of chunk {id}, which is {} \
                     bytes long — the version and its content disagree",
                    chunk.len()
                ),
            )
        })?;

        let mut local = match read_slice(&slice.resolved, slice.file_offset, slice.length).await {
            Ok(local) => local,
            Err(Unreadable::Absent) => {
                findings.push(finding(&slice, Unheld::Absent));
                continue;
            }
            Err(Unreadable::Short) => {
                findings.push(finding(&slice, Unheld::Short));
                continue;
            }
        };

        if local.as_slice() != expected {
            findings.push(finding(&slice, Unheld::Content));
        }

        local.clear();
    }

    Ok(findings)
}

fn finding(slice: &Placement, reason: Unheld) -> Mismatch {
    Mismatch {
        path: slice.path.clone(),
        offset: slice.file_offset,
        length: u64::from(slice.length),
        expected: slice.expected,
        reason,
    }
}

/// Why a recorded slice could not be read at all.
enum Unreadable {
    /// The file is not there.
    Absent,
    /// The file ends before the slice does.
    Short,
}

/// Read exactly `length` bytes at `offset`, or say why they could not be.
///
/// A short read is reported rather than returned, and the two are distinct
/// because they are distinct problems: "this file is not here" and "this file ends
/// early" send someone to two different places, and a caller that got only one of
/// the two would have to guess which it was.
async fn read_slice(path: &Path, offset: u64, length: u32) -> Result<Vec<u8>, Unreadable> {
    let wanted = u64::from(length);

    let mut file = match tfs::File::open(path).await {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(Unreadable::Absent),
        Err(_) => return Err(Unreadable::Short),
    };

    if file.seek(std::io::SeekFrom::Start(offset)).await.is_err() {
        return Err(Unreadable::Short);
    }

    // Bounded, so a length from a downloaded version cannot read past its own
    // slice, and read to exactly `wanted` so a file that ends early is visible as
    // a short read rather than as bytes that happen to be different.
    let mut reader = crate::slice::SliceAsyncReader::new(file, wanted);
    let mut out = Vec::new();
    if reader.read_to_end(&mut out).await.is_err() {
        return Err(Unreadable::Short);
    }

    if u64::try_from(out.len()).unwrap_or(u64::MAX) < wanted {
        return Err(Unreadable::Short);
    }

    Ok(out)
}

/// Entries under `root` that `snapshot` does not record.
///
/// Links are recorded but never followed, so a symlink pointing out of the tree
/// is one entry to report rather than a path to walk — following it would make
/// the answer depend on whatever is on the other end of it.
///
/// The root itself is never reported: it is the destination, and it exists before
/// any of this runs.
///
/// A walk that cannot be read is *skipped* rather than fatal. The entries it did
/// reach are still reported, and the alternative is a probe that refuses to answer
/// because one directory in a tree is unreadable — which turns "here is what is
/// wrong with your directory" into "here is nothing, and now go and find the
/// permission problem yourself". A caller that needs certainty can compare the
/// count against what the version records.
fn extra_entries(root: &Path, snapshot: &Snapshot) -> Vec<PathBuf> {
    use crate::metadata::Link;

    let mut recorded: HashSet<&Path> =
        HashSet::with_capacity(snapshot.files.len() + snapshot.dirs.len() + snapshot.links.len());
    recorded.extend(snapshot.files.iter().map(|f| f.path.as_path()));
    recorded.extend(snapshot.dirs.iter().map(|d| d.path.as_path()));
    recorded.extend(snapshot.links.iter().map(|link| match link {
        Link::Symbolic { path, .. } | Link::Hard { path, .. } => path.as_path(),
    }));

    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .min_depth(1)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
    {
        let Ok(relative) = entry.path().strip_prefix(root) else {
            continue;
        };

        if !recorded.contains(relative) {
            out.push(relative.to_path_buf());
        }
    }

    out.sort();
    out
}
