// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Turning a snapshot into an ordered list of chunks in a store.
//!
//! A [`crate::metadata::Snapshot`] is a location-free recipe. It names the chunks
//! that exist. It names which file each slice of each chunk belongs in, and where
//! in that file. This module asks the store and produces the answer.
//!
//! **The order is the point.** Chunks reach a store in whatever order concurrent
//! uploads finish. A list taken from the store directly therefore differs between
//! runs, and anything built from it would differ too. [`Plan::from_snapshot`]
//! orders by the snapshot's own chunk table instead. That table is a function of
//! read order and nothing else. Two backups of one tree produce the same plan.
//!
//! That determinism is why a plan is a list of *chunks* and not a list of ranges.
//! A store range has to stop at every record header, so a range list taken against
//! a store coalesces almost never. A pack has no framing between its payloads, so
//! the same chunks end to end *are* contiguous. See [`crate::pack`].
//!
//! [`Plan::total_bytes`] is therefore the size of a pack. It is not the size of a
//! store byte range. A caller can compute it before reading a byte.

use std::collections::{HashMap, HashSet};
use std::io;

use crate::metadata::{FileChunk, Snapshot};
use crate::storage::{BlobId, Placement};

/// Where one distinct chunk lives, and how much of it a version actually uses.
///
/// `length` is what a restore reads for this chunk, not the chunk's whole length.
/// A chunk that spans two files is stored whole and used in part. A chunk that
/// repeated content spans may be used at several offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedChunk {
    /// The chunk's content hash, and its identity.
    pub id: BlobId,
    /// Where the chunk's payload sits in the store.
    pub placement: Placement,
    /// Bytes of the chunk that this version reads.
    pub length: u64,
}

/// Every chunk a snapshot needs, in a deterministic order, with its location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    chunks: Vec<PlannedChunk>,
}

impl Plan {
    /// The chunks, in order.
    ///
    /// [`Plan`] is also [`IntoIterator`] and [`AsRef<[PlannedChunk]>`]. Both
    /// `for chunk in &plan` and `plan.as_ref().len()` work. The double hop
    /// `plan.chunks().iter()` is not needed. Both spellings were in the tree, and
    /// the double hop reads like a mistake.
    pub fn iter(&self) -> std::slice::Iter<'_, PlannedChunk> {
        self.chunks.iter()
    }

    /// Resolve `snapshot` against the placements `locate` reports.
    ///
    /// `locate` receives a chunk hash. It returns where that chunk is, or
    /// [`io::ErrorKind::NotFound`] if the store does not hold it. The function
    /// takes a callback rather than a store, so this works for any backend.
    ///
    /// # Errors
    ///
    /// Returns an error if `locate` fails for any chunk that the snapshot names.
    /// Returns an error if a slice names a **chunk** index that does not exist.
    ///
    /// A slice that names a *file* index that does not exist is not refused here.
    /// An older version of this comment claimed otherwise. Nothing in a `Plan`
    /// consumes `file_index`. The tables are flattened to one entry per distinct
    /// chunk. A check on the file index would make this function read a table that
    /// it does not read. The index is checked where it is used, by
    /// [`Snapshot::verify_file_lengths`](crate::metadata::Snapshot::verify_file_lengths).
    /// A restore runs that check before it writes anything.
    pub fn from_snapshot<F>(snapshot: &Snapshot, mut locate: F) -> io::Result<Self>
    where
        F: FnMut(&BlobId) -> io::Result<Placement>,
    {
        // Slices per chunk, so a chunk used at several offsets is stored once in
        // the plan and its `length` is the furthest offset any slice reaches.
        let mut extent = vec![0u64; snapshot.chunks.len()];

        for slice in &snapshot.file_chunks {
            let end = reach(slice)?;
            let extent = extent
                .get_mut(
                    usize::try_from(slice.chunk_index).map_err(|_| bad_index(snapshot, slice))?,
                )
                .ok_or_else(|| bad_index(snapshot, slice))?;

            *extent = (*extent).max(end);
        }

        let mut chunks = Vec::with_capacity(snapshot.chunks.len());
        for (index, id) in snapshot.chunks.iter().enumerate() {
            let length = extent[index];
            if length == 0 {
                // Listed but never read, so there is nothing to read it for.
                continue;
            }

            let placement = locate(id)?;

            // Against the *raw* length. A snapshot records its slices in content
            // bytes. A compressed placement's `len` is the frame's size on disk.
            // Comparing content to storage would accept a chunk that is genuinely
            // too short, or reject a good one.
            let held = placement.raw_len;

            if length > held {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "a snapshot needs {length} bytes of chunk {id} but the store holds {held}"
                    ),
                ));
            }

            chunks.push(PlannedChunk {
                id: *id,
                placement,
                length,
            });
        }

        Ok(Self { chunks })
    }

    /// The chunks a holder of `base` does not already have, per *placement*.
    ///
    /// A delta: what `target` needs and a holder of `base` does not. The delta is
    /// deterministic. The surviving chunks keep `target`'s own chunk order instead
    /// of acquiring an order of their own. Two packs of the same delta are then
    /// the same bytes. A pack's content hash rests on that property.
    ///
    /// The function builds the answer in one pass over each table. One set first
    /// records every site that the base places. One pass over `target.file_chunks`
    /// then marks each target chunk index with a site that the base does not hold.
    /// A slice with a chunk index that does not convert to `usize`, or with a file
    /// index that names no file, is ignored. One pass over `target.chunks` records
    /// the first index of each id.
    ///
    /// For an id that appears more than once, only the first occurrence decides. An
    /// index with no resolvable slice drops its chunk, because nothing places it.
    /// An id that the target table does not name stays, because nothing can cover
    /// it. The delta keeps the target's chunk order.
    ///
    /// **Placements, not chunk sets.** The first version subtracted `base`'s chunk
    /// hashes. That is wrong in a way that only shows up with shared content. A
    /// chunk that the base holds *somewhere* is not thereby held *everywhere*. A
    /// tree where every file happens to have identical bytes produces such a chunk.
    /// So does a new file whose content repeats an existing one. That chunk is in
    /// the base and absent from a file that the base knows nothing about. Subtracting
    /// by hash drops it, and the delta then omits a file's only content. The client
    /// skips that file, never creates it, and reports success.
    ///
    /// A chunk therefore survives unless the base places *every* one of its slices
    /// identically. The placements must agree on path, file offset, chunk offset,
    /// and length. Only that can mean "the directory already has this". The
    /// directory holds bytes at paths, and a chunk is not a place.
    ///
    /// A reader must then know that a chunk absent from the delta is one in
    /// place at every one of the target's placements. That is what makes
    /// skipping it sound. It is a property of the *file*, not of the content.
    ///
    /// The earlier version scanned `target.chunks` and `target.file_chunks` once
    /// per planned chunk. The cost then grew as the product of the two table
    /// lengths, which is quadratic on large snapshots. This version scans each
    /// table once, so the cost grows as their sum.
    #[must_use]
    pub fn since(&self, target: &Snapshot, base: &Snapshot) -> Self {
        let mut covered: HashSet<Site<'_>> = HashSet::with_capacity(base.file_chunks.len());
        covered.extend(
            base.file_chunks
                .iter()
                .filter_map(|slice| placement_of(base, slice)),
        );

        let mut uncovered: HashSet<usize> = HashSet::new();
        for slice in &target.file_chunks {
            let Ok(index) = usize::try_from(slice.chunk_index) else {
                continue;
            };
            if uncovered.contains(&index) {
                continue;
            }
            let Some(site) = placement_of(target, slice) else {
                continue;
            };
            if !covered.contains(&site) {
                uncovered.insert(index);
            }
        }

        let mut first: HashMap<&BlobId, usize> = HashMap::with_capacity(target.chunks.len());
        for (index, id) in target.chunks.iter().enumerate() {
            first.entry(id).or_insert(index);
        }

        Self {
            chunks: self
                .chunks
                .iter()
                .copied()
                .filter(|chunk| match first.get(&chunk.id) {
                    None => true,
                    Some(index) => uncovered.contains(index),
                })
                .collect(),
        }
    }

    /// The chunks, in the snapshot's own chunk order.
    #[must_use]
    pub fn chunks(&self) -> &[PlannedChunk] {
        &self.chunks
    }

    /// How many distinct chunks this version needs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Whether the version has no file content at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Bytes a concatenation of every distinct chunk would occupy, counting each
    /// chunk once. This is the size of a *pack*, not of the tree.
    ///
    /// This is not a store byte range. A caller fetching from a store cannot use
    /// this number, because the chunks are separated by record headers.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.chunks
            .iter()
            .map(|c| c.length)
            .fold(0u64, u64::saturating_add)
    }
}

impl AsRef<[PlannedChunk]> for Plan {
    fn as_ref(&self) -> &[PlannedChunk] {
        &self.chunks
    }
}

impl<'a> IntoIterator for &'a Plan {
    type Item = &'a PlannedChunk;
    type IntoIter = std::slice::Iter<'a, PlannedChunk>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.iter()
    }
}

impl IntoIterator for Plan {
    type Item = PlannedChunk;
    type IntoIter = std::vec::IntoIter<PlannedChunk>;

    /// Takes the chunks rather than copying them, which is what a caller
    /// assembling its own index wants.
    fn into_iter(self) -> Self::IntoIter {
        self.chunks.into_iter()
    }
}

/// The furthest byte of a chunk that `slice` reads.
fn reach(slice: &FileChunk) -> io::Result<u64> {
    u64::from(slice.chunk_offset)
        .checked_add(u64::from(slice.length))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "a chunk slice reaches past the end of its chunk",
            )
        })
}

/// The error for a slice naming a chunk the snapshot does not contain.
///
/// Names only the chunk index, because that is the only one checked. See
/// [`Plan::from_snapshot`]. This error used to name the file index too, and to say
/// "cannot be resolved". That wording read as though both were being resolved. A
/// message that reports a check the function does not perform is worse than a
/// short one. Such a message tells a reader the snapshot was fully validated when
/// it was not.
fn bad_index(snapshot: &Snapshot, slice: &FileChunk) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "a snapshot slice names chunk {} of {}, which it does not contain",
            slice.chunk_index,
            snapshot.chunks.len(),
        ),
    )
}

/// Where a slice sits: the file, and the four numbers that make it the same bytes.
type Site<'a> = (&'a std::path::Path, u64, u32, u32);

/// A slice's placement, or `None` if it names a table the snapshot does not have.
///
/// Returning `None` rather than refusing is deliberate. It matches
/// [`Plan::from_snapshot`], which drops what it cannot resolve instead of failing.
/// A chunk whose only slice is unresolvable is never read anywhere. Where it would
/// have gone therefore cannot matter.
fn placement_of<'a>(snapshot: &'a Snapshot, slice: &FileChunk) -> Option<Site<'a>> {
    let file = usize::try_from(slice.file_index).ok()?;
    let path = snapshot.files.get(file)?.path.as_path();

    Some((path, slice.file_offset, slice.chunk_offset, slice.length))
}

#[cfg(test)]
mod tests {
    use super::Plan;
    use crate::metadata::{Attributes, File, FileChunk, Snapshot, Timestamp};
    use crate::storage::{BlobId, Codec, Placement};

    fn stamped() -> Timestamp {
        Timestamp { secs: 1, nanos: 0 }
    }

    fn chunk() -> BlobId {
        BlobId::from_bytes([7u8; 32])
    }

    fn at(offset: u64, len: u64) -> Placement {
        Placement {
            segment: 0,
            offset,
            stored_len: len,
            // These tests are about a plan's ordering and coverage, which do not
            // depend on how a payload is stored, so every placement here is raw
            // and its raw length is its length on disk.
            raw_len: len,
            codec: Codec::Raw,
        }
    }

    /// A snapshot of one file of 100 bytes made of two 50-byte chunks.
    fn two_chunks() -> Snapshot {
        let mut s = Snapshot::new(stamped());
        s.files.push(File {
            path: "a.bin".into(),
            len: 100,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: None,
            },
        });
        s.chunks.push(chunk());
        s.chunks.push(BlobId::from_bytes([8u8; 32]));
        s.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 0,
            length: 50,
        });
        s.file_chunks.push(FileChunk {
            chunk_index: 1,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 50,
            length: 50,
        });
        s
    }

    /// One file entry: a path, a length, and ordinary attributes.
    fn file(path: &str, len: u64) -> File {
        File {
            path: path.into(),
            len,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: None,
            },
        }
    }

    /// One slice of `chunk_index` in `file_index`, from the start of both.
    fn slice(chunk_index: u32, file_index: u32, file_offset: u64, length: u32) -> FileChunk {
        FileChunk {
            chunk_index,
            file_index,
            chunk_offset: 0,
            file_offset,
            length,
        }
    }

    #[test]
    fn a_plan_lists_every_chunk_the_snapshot_reads() {
        let plan = Plan::from_snapshot(&two_chunks(), |_| Ok(at(100, 50))).unwrap();

        assert_eq!(plan.len(), 2);
        assert!(!plan.is_empty());
        assert_eq!(plan.total_bytes(), 100);
    }

    #[test]
    fn a_chunk_used_at_several_offsets_is_listed_once() {
        let mut s = two_chunks();
        // The same chunk twice in one file, at two offsets.
        s.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 100,
            length: 50,
        });

        let plan = Plan::from_snapshot(&s, |_| Ok(at(0, 50))).unwrap();

        assert_eq!(plan.len(), 2, "the repeat should not add a second entry");
        assert_eq!(plan.total_bytes(), 100);
    }

    #[test]
    fn a_chunk_used_partly_is_counted_to_its_furthest_byte() {
        // One chunk, stored 1000 bytes long, of which the version reads 40.
        let mut s = Snapshot::new(stamped());
        s.files.push(File {
            path: "a.bin".into(),
            len: 40,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
                xattrs: Vec::new(),
                windows_acl: None,
            },
        });
        s.chunks.push(chunk());
        s.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 0,
            length: 40,
        });

        let plan = Plan::from_snapshot(&s, |_| Ok(at(0, 1000))).unwrap();

        assert_eq!(plan.chunks()[0].length, 40, "only 40 bytes are read");
        assert_eq!(plan.total_bytes(), 40);
    }

    #[test]
    fn a_chunk_the_snapshot_lists_but_never_reads_is_left_out() {
        let mut s = two_chunks();
        s.chunks.push(BlobId::from_bytes([9u8; 32]));

        let plan = Plan::from_snapshot(&s, |_| Ok(at(0, 50))).unwrap();

        assert_eq!(
            plan.len(),
            2,
            "an unreferenced chunk would be a zero-length range"
        );
    }

    #[test]
    fn a_chunk_the_store_does_not_hold_is_an_error() {
        let error = Plan::from_snapshot(&two_chunks(), |_| {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "absent"))
        })
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn a_chunk_shorter_than_the_snapshot_expects_is_an_error() {
        // The snapshot reads 50 bytes of a chunk the store holds 10 of.
        let error = Plan::from_snapshot(&two_chunks(), |_| Ok(at(0, 10))).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("but the store holds 10"),
            "the error should quantify the shortfall, got: {error}"
        );
    }

    #[test]
    fn a_slice_naming_a_chunk_that_does_not_exist_is_an_error() {
        let mut s = two_chunks();
        s.file_chunks.push(FileChunk {
            chunk_index: 9,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 200,
            length: 10,
        });

        let error = Plan::from_snapshot(&s, |_| Ok(at(0, 50))).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("names chunk 9 of 2"),
            "the error should name what it could not resolve, got: {error}"
        );
    }

    #[test]
    fn an_empty_snapshot_plans_to_nothing() {
        let plan = Plan::from_snapshot(&Snapshot::new(stamped()), |_| Ok(at(0, 0))).unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.total_bytes(), 0);
    }

    /// A version against itself sends nothing.
    ///
    /// The base places every slice where the target places it, so no placement
    /// is uncovered and the delta is empty.
    #[test]
    fn an_identical_base_drops_every_chunk() {
        let snapshot = two_chunks();
        let plan = Plan::from_snapshot(&snapshot, |_| Ok(at(0, 50))).unwrap();

        assert!(
            plan.since(&snapshot, &snapshot).is_empty(),
            "a holder of the base needs nothing sent"
        );
    }

    /// One uncovered placement keeps the whole chunk.
    ///
    /// The base places the shared chunk in `old.bin` and nowhere else. The
    /// target also reads it in `new.bin`. A holder of the base lacks that
    /// placement, so the chunk must travel. Without it the apply skips
    /// `new.bin`.
    #[test]
    fn one_uncovered_placement_keeps_the_chunk() {
        let shared = chunk();

        let mut base = Snapshot::new(stamped());
        base.files.push(file("old.bin", 50));
        base.file_chunks.push(slice(0, 0, 0, 50));

        let mut target = Snapshot::new(stamped());
        target.files.push(file("old.bin", 50));
        target.files.push(file("new.bin", 50));
        target.chunks.push(shared);
        target.file_chunks.push(slice(0, 0, 0, 50));
        target.file_chunks.push(slice(0, 1, 0, 50));

        let plan = Plan::from_snapshot(&target, |_| Ok(at(0, 50))).unwrap();
        let delta = plan.since(&target, &base);

        assert_eq!(delta.len(), 1, "the placement in new.bin is uncovered");
        assert_eq!(delta.chunks()[0].id, shared);
    }

    /// A chunk the target chunk table does not name stays in the delta.
    ///
    /// Nothing places an unnamed chunk, so nothing can cover it. The chunk the
    /// target does name here has no slice, so it drops instead. The test pins
    /// both sides of the rule.
    #[test]
    fn a_chunk_the_target_table_does_not_name_stays() {
        let plan = Plan::from_snapshot(&two_chunks(), |_| Ok(at(0, 50))).unwrap();

        let mut target = Snapshot::new(stamped());
        target.chunks.push(chunk());

        let delta = plan.since(&target, &Snapshot::new(stamped()));

        assert_eq!(delta.len(), 1, "the unnamed chunk cannot be covered");
        assert_eq!(delta.chunks()[0].id, BlobId::from_bytes([8u8; 32]));
    }

    /// A slice that names a file the target does not have is ignored.
    ///
    /// The chunk then has no placement at all, so the delta drops it. This
    /// matches `from_snapshot`, which also ignores the file index.
    #[test]
    fn slices_that_name_missing_files_are_ignored() {
        let mut target = Snapshot::new(stamped());
        target.chunks.push(chunk());
        target.file_chunks.push(slice(0, 9, 0, 50));

        let plan = Plan::from_snapshot(&target, |_| Ok(at(0, 50))).unwrap();
        let delta = plan.since(&target, &Snapshot::new(stamped()));

        assert!(
            delta.is_empty(),
            "a slice with no file has no placement to uncover"
        );
    }

    /// A chunk id that the target lists twice keeps its first occurrence.
    ///
    /// The first index of the id decides whether the chunk stays. The base
    /// covers the first placement here, so the chunk drops. The second
    /// placement is uncovered, and it does not rescue the chunk.
    #[test]
    fn a_duplicate_chunk_id_keeps_the_first_occurrence() {
        let shared = chunk();

        let mut base = Snapshot::new(stamped());
        base.files.push(file("a.bin", 50));
        base.file_chunks.push(slice(0, 0, 0, 50));

        let mut target = Snapshot::new(stamped());
        target.files.push(file("a.bin", 50));
        target.files.push(file("b.bin", 50));
        target.chunks.push(shared);
        target.chunks.push(shared);
        target.file_chunks.push(slice(0, 0, 0, 50));
        target.file_chunks.push(slice(1, 1, 0, 50));

        let plan = Plan::from_snapshot(&target, |_| Ok(at(0, 50))).unwrap();
        let delta = plan.since(&target, &base);

        assert!(
            delta.is_empty(),
            "the first occurrence of the id decides, and the base covers it"
        );
    }

    /// An uncovered first occurrence keeps a chunk that the target lists twice.
    ///
    /// The first index of the id decides, and here the base covers the second
    /// placement only. The chunk stays, because the first occurrence is the
    /// one that is uncovered.
    #[test]
    fn an_uncovered_first_occurrence_keeps_a_duplicate_chunk() {
        let shared = chunk();

        let mut base = Snapshot::new(stamped());
        base.files.push(file("b.bin", 50));
        base.file_chunks.push(slice(1, 0, 0, 50));

        let mut target = Snapshot::new(stamped());
        target.files.push(file("a.bin", 50));
        target.files.push(file("b.bin", 50));
        target.chunks.push(shared);
        target.chunks.push(shared);
        target.file_chunks.push(slice(0, 0, 0, 50));
        target.file_chunks.push(slice(1, 1, 0, 50));

        let plan = Plan::from_snapshot(&target, |_| Ok(at(0, 50))).unwrap();
        let delta = plan.since(&target, &base);

        assert_eq!(delta.len(), 2, "both entries of the id stay");
        assert_eq!(delta.chunks()[0].id, shared);
    }

    /// A slice that names a chunk index outside the table changes nothing.
    ///
    /// The index names no chunk, so it uncovers no chunk. The delta keeps
    /// the chunks that the rest of the target names. The test pins that the
    /// stray index neither adds a chunk nor drops one.
    #[test]
    fn a_slice_naming_a_chunk_index_out_of_range_changes_nothing() {
        let mut target = two_chunks();
        target.file_chunks.push(slice(9, 0, 100, 50));

        let plan = Plan::from_snapshot(&two_chunks(), |_| Ok(at(0, 50))).unwrap();
        let delta = plan.since(&target, &Snapshot::new(stamped()));

        assert_eq!(delta.len(), 2, "the two named chunks stay");
    }

    /// The survivors keep the target's own chunk order.
    ///
    /// A pack's bytes rest on that order. The base places the middle chunk, so
    /// the delta is the first and the last chunk, in that order.
    #[test]
    fn the_delta_keeps_the_targets_chunk_order() {
        let first = BlobId::from_bytes([1u8; 32]);
        let middle = BlobId::from_bytes([2u8; 32]);
        let last = BlobId::from_bytes([3u8; 32]);

        let mut target = Snapshot::new(stamped());
        target.files.push(file("a.bin", 150));
        target.chunks.extend([first, middle, last]);
        for (index, offset) in [0u64, 50, 100].into_iter().enumerate() {
            target
                .file_chunks
                .push(slice(u32::try_from(index).unwrap_or(0), 0, offset, 50));
        }

        // The base places the middle chunk, and only that one.
        let mut base = Snapshot::new(stamped());
        base.files.push(file("a.bin", 150));
        base.file_chunks.push(slice(1, 0, 50, 50));

        let plan = Plan::from_snapshot(&target, |_| Ok(at(0, 50))).unwrap();
        let delta = plan.since(&target, &base);

        let kept: Vec<BlobId> = delta.iter().map(|chunk| chunk.id).collect();
        assert_eq!(
            kept,
            vec![first, last],
            "the survivors keep the target order"
        );
    }
}
