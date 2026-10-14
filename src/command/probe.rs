// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Asking what a directory already holds.
//!
//! A pack is a delta waiting for a client. To know which ranges of it to fetch,
//! a client needs one thing: the set of chunks it already has, correctly. That is
//! what a probe produces, and the answer is per chunk and nothing coarser.
//!
//! ## Why not hash the tree and compare
//!
//! The obvious approach cannot work here, for two separate reasons, and both
//! matter. That approach is to walk the target, hash it, and compare the result
//! against the version.
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
//! produced. A tree with one extra file in it chunks differently, and so does the
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
//! `chunk_offset`. So a slice is usually *part* of a chunk. The last chunk of a
//! file runs on into the next one. The bytes at `F[O..O+L]` are then not the whole
//! of `H`. Hashing them produces something that is never equal to `H`.
//! A probe that hashes the file slice and compares to the chunk's hash condemns
//! every boundary-crossing slice in the tree. It reports a target that is
//! byte-for-byte correct as holding nothing, forever. The chunk's own bytes have
//! to be read to compare the two. So a probe is given a source and not only a
//! version.
//!
//! ## All-or-nothing, and that is load-bearing
//!
//! One chunk can be placed at several slices.
//! That happens with repeated content, or with one chunk spanning a file
//! boundary.
//! [`Pack::needed`](crate::pack::Pack::needed) consults `held`.
//! It uses `held` to decide a chunk needs no range request.
//!
//! A chunk recorded as held on the strength of three correct slices out of four
//! would be skipped by the fetch. The fetch would leave the fourth slice wrong.
//! The client would believe it had applied the version.
//!
//! So the question is not "is this chunk right somewhere".
//! The question is "is this chunk right everywhere it appears".
//! A probe that answered the first would be worse than one that does not answer
//! at all. It looks like an optimisation, and it is a silent corruption.
//!
//! That also makes a probe idempotent.
//! Running the probe again resumes an interrupted apply.
//! It is also why no probe needs to name a base version.
//! A client can be holding v4, v6, or half of each.
//! The same probe answers correctly for all three.

use std::{
    collections::{BTreeSet, HashSet},
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

/// Chunks to check at once. Reading one chunk is a read and then a comparison per
/// slice placing it. There is nothing to overlap inside either.
/// So this is all that keeps the disk busy.
const PROBE_CONCURRENCY: usize = 8;

/// How many chunks to check at once.
fn probe_concurrency() -> usize {
    super::concurrency(PROBE_CONCURRENCY)
}

/// Why the target does not hold the bytes a version records.
///
/// Not named `Reason`, which [`super::verify`] already uses for why a chunk could
/// not be checked out. The two have the same shape and answer different questions.
/// A caller holding a failure from either should be able to tell which one it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unheld {
    /// The file the version records is not there.
    Absent,

    /// The file ends before the slice does.
    Short,

    /// The file continues past the last slice the version records.
    ///
    /// The one difference no slice can see. Every recorded slice of a file that
    /// has grown can verify perfectly. An extra tail then sits past the end of
    /// those slices. A probe that cannot see that tail would answer "holds this
    /// version" about a file with somebody else's data in it.
    Long,

    /// The bytes there hash to something else.
    ///
    /// Not necessarily corruption. This is also what a file the *user* edited
    /// looks like. That edit is the ordinary case for a client that applies an
    /// update to a tree someone is using.
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
/// already reported slice by slice as [`Unheld::Short`].
/// Saying it again once per file would repeat one finding three times over.
/// A file *longer* is reported here and nowhere else.
/// There is no slice out past the end of one.
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
    /// as a leftover from an older version. A tool that deletes it to make a
    /// directory match a file destroys data it cannot tell apart from data it
    /// should keep. So removing them is a decision the caller has to make out
    /// loud. [`Probed`] only ever tells the caller what the decision would cost.
    pub extra: Vec<PathBuf>,
}

impl Probed {
    /// Whether the target *is* this version: every slice right, every length right.
    ///
    /// Content alone, in other words. It says nothing about [`Probed::extra`].
    /// A directory with a stray file in it still holds the version.
    /// That directory is not equal to the version.
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.mismatched.is_empty() && self.mislengths.is_empty()
    }
}

/// Check what `root` holds of `snapshot`, without writing to it.
///
/// Reads every slice the version records and hashes it. Nothing is created,
/// truncated or removed, so this is safe to run against a tree in use.
/// It may cost a full read of that tree.
/// A full read is the price of knowing.
///
/// # Errors
///
/// This function returns an error if the version records a path that would land
/// outside `root`. It also returns an error if the version names a file or chunk
/// that it does not itself contain. It returns an error if a chunk it needs
/// cannot be read from `source`. It returns an error if a recorded file cannot
/// be opened or read.
/// It does not return that error when the reason is "it is not there".
///
/// A missing file is a finding, not a failure. It lands in [`Probed::mismatched`]
/// as [`Unheld::Absent`] rather than failing the probe. A client asks "what is
/// missing" precisely because some of it is missing.
///
/// This function is generic over [`StorageGet`] because a chunk's bytes come from
/// a store. A client that updates from a pack uses a pack. A tool that compares a
/// directory against a set it can read uses a store. The function needs somewhere
/// to read chunks from, not a particular kind of somewhere. It takes the source
/// by value and shares it, as a restore takes its store, because each concurrent
/// task holds one.
#[instrument(level = "info", skip_all, err)]
pub async fn probe<P, S>(root: P, snapshot: &Snapshot, source: S) -> std::io::Result<Probed>
where
    P: AsRef<Path>,
    S: StorageGet + 'static,
{
    probe_skipping(root, snapshot, source, &Established::default()).await
}

/// What a caller has already established about a directory, and so need not be read.
///
/// Both fields are claims this function does not check and could not: re-deriving
/// either is the work being skipped. So there is no way to fill one in by accident,
/// and the only way to get one wrong is to do so deliberately.
///
/// `BTreeSet` rather than `HashSet` because nothing here depends on order, so a
/// caller should not have to name a hasher to pass a set.
#[derive(Debug, Clone, Default)]
pub struct Established<'a> {
    /// Files whose bytes are known right, so their slices are neither read nor
    /// reported.
    ///
    /// From a record of a last apply plus a stat: the file is where it was left.
    pub files: BTreeSet<&'a Path>,

    /// Chunks the directory holds correctly that `source` cannot supply.
    ///
    /// For a delta pack: the ones it deliberately left out. It is not read from the
    /// pack *because* it is not in the pack. Nothing can be compared against it.
    /// Therefore the base version is the only thing that stands between a delta
    /// and a wrong answer. A caller may only fill this in having established the
    /// directory holds the base, and `apply` establishes that from a manifest or
    /// refuses.
    pub chunks: HashSet<&'a BlobId>,
}

/// [`probe`], reading only what `established` does not already account for.
///
/// A file in [`Established::files`] is neither read nor reported. A chunk in
/// [`Established::chunks`] is not read from `source`, because it may not be there.
/// Its placements are reported as held, because the caller has said the directory
/// has it.
///
/// Naming every file and no chunks is [`probe`], and is what [`probe`] does.
///
/// # Errors
///
/// As [`probe`].
#[instrument(level = "info", skip_all, err)]
pub async fn probe_skipping<P, S>(
    root: P,
    snapshot: &Snapshot,
    source: S,
    established: &Established<'_>,
) -> std::io::Result<Probed>
where
    P: AsRef<Path>,
    S: StorageGet + 'static,
{
    let root = root.as_ref();
    let source = Arc::new(source);

    // This check runs before a byte is read, for the same reason a restore checks
    // first. The whole point is to find out about a tree before touching it. A
    // path that escapes the root is a reason to stop, not a reason to read it.
    super::restore::check_paths(snapshot)?;

    // One task runs per *chunk*, not per slice. The task reads a chunk once and
    // compares it against every slice that places it. That order is the cheap way
    // round. It is also the only way to decide "right everywhere" rather than
    // "right somewhere".
    let placed = placements(root, snapshot, established)?;

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

            // A chunk the caller has already accounted for is neither read nor
            // reported. It is held, because the caller has said the directory has
            // it. A delta pack omits exactly these, and cannot be asked for them.
            // A `get` call would fail on a chunk that is not there by design, so
            // the check has to come before the read rather than be handled after
            // it.
            if established.chunks.contains(&id) {
                if !placed[index].is_empty() {
                    held.insert(id);
                }
                continue;
            }

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

    // Sorted, so the report is the same whichever order the reads finished in.
    // A report that reorders itself between two runs of the same tree is a report
    // nobody can diff.
    mismatched.sort_by(|a, b| (&a.path, a.offset).cmp(&(&b.path, b.offset)));

    let seen: HashSet<&Path> = mismatched.iter().map(|m| m.path.as_path()).collect();
    let mut mislengths = mislengths(root, snapshot, &seen, established)?;
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
///   perfectly while an extra tail sits past the end of them. No slice can see
///   that tail. A probe that missed it would report a directory holding ten
///   kilobytes of somebody's data as *this version*.
///
/// * **Too short** is skipped when a slice already reported it. The slice is the
///   more specific finding. It says *which* bytes are missing, not just that the
///   file is the wrong size. A file with no slices at all has nothing to report
///   it, so it is reported here.
///
/// * **Missing** likewise, and only for a file with no slices. A file that
///   exists in no slice but not on disk is a finding that nothing else makes.
fn mislengths(
    root: &Path,
    snapshot: &Snapshot,
    seen: &HashSet<&Path>,
    established: &Established<'_>,
) -> std::io::Result<Vec<Mislength>> {
    let mut out = Vec::new();

    for file in &snapshot.files {
        // A trusted file's length was in the stat that trusted it. Re-deriving it
        // here would be free but pointless, and a length that did not match would
        // contradict the trust rather than reveal anything.
        if established.files.contains(file.path.as_path()) {
            continue;
        }

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
    /// Whether a caller said this file is where its last apply left it.
    ///
    /// A trusted placement is not read. It is also not a finding. The caller has
    /// already established the file is unchanged by stat. Re-reading it would cost
    /// the very read the stat was meant to avoid. Nothing here trusts a file on
    /// its own account. `trusted` is empty unless a caller with a manifest
    /// supplied it, and a plain probe reads everything.
    trusted: bool,
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
/// A chunk with no slice is not here at all. The version lists it but reads it
/// nowhere. A directory cannot hold a chunk that nobody reads.
/// Counting those as missing would make every version report itself as
/// incomplete.
fn placements(
    root: &Path,
    snapshot: &Snapshot,
    established: &Established<'_>,
) -> std::io::Result<Vec<Vec<Placement>>> {
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
            trusted: established.files.contains(file.path.as_path()),
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
/// Names the index and which table it was checked against. A caller who gets
/// this error holds a damaged version, and the whole of it is suspect.
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
/// The chunk is read once and buffered. The buffer is bounded by the chunk
/// profile's maximum, which is 256 KiB at the largest. Buffering lets one read
/// answer for every slice placing the chunk, including a slice that holds only
/// part of it. Streaming instead would mean re-reading the chunk per slice, or a
/// second pass per slice to seek. The win buys nothing, because the caller reads
/// this chunk from a store or a pack anyway. A restore also buffers the same
/// bytes.
///
/// **A chunk whose every placement is trusted is not read at all.** That is the
/// whole point of the trusted set. The chunk's bytes are needed only to compare
/// against a file, and a file nobody has touched does not need comparing. The
/// function checks this before the read, rather than filtering the placements
/// afterwards. A `get` call on a delta pack would fail for a chunk that the pack
/// deliberately omitted. The failure happens even for a chunk that turns out
/// not to need reading.
async fn check_chunk<S: StorageGet + ?Sized>(
    id: BlobId,
    slices: Vec<Placement>,
    source: &S,
) -> std::io::Result<Vec<Mismatch>> {
    let mut findings = Vec::new();

    if slices.iter().all(|slice| slice.trusted) {
        return Ok(findings);
    }

    let mut reader = source.get(&id).await?;
    let mut chunk = Vec::new();
    reader.read_to_end(&mut chunk).await?;

    for slice in slices {
        // Trusted placements are neither read nor reported. See the type.
        if slice.trusted {
            continue;
        }

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
/// A short read is reported rather than returned. The two are distinct because
/// they are distinct problems. "This file is not here" and "this file ends early"
/// send someone to two different places. A caller that got only one of the two
/// would have to guess which one it was.
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

    // This reader is bounded, so a length from a downloaded version cannot read
    // past its own slice. It reads to exactly `wanted`, so a file that ends early
    // shows a short read. A short read is not the same as bytes that happen to be
    // different.
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
/// Links are recorded but never followed. A symlink that points out of the tree
/// is one entry to report. It is not a path to walk, because the answer would then
/// depend on whatever is on the other end of it.
///
/// The root itself is never reported: it is the destination, and it exists before
/// any of this runs.
///
/// A walk that cannot be read is *skipped* rather than fatal. The entries it did
/// reach are still reported. The alternative is a probe that refuses to answer
/// because one directory in a tree is unreadable. That refusal turns "here is
/// what is wrong with your directory" into "here is nothing". The caller then has
/// to find the permission problem alone. A caller that needs certainty can compare
/// the count against what the version records.
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
