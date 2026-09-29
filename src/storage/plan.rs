//! Turning a snapshot into an ordered list of chunks in a store.
//!
//! A [`crate::metadata::Snapshot`] is a location-free recipe: which chunks exist,
//! which file each slice of each chunk belongs in, and where in that file. This
//! module is the step that asks the store and produces the answer.
//!
//! **The order is the point.** Chunks reach a store in whatever order concurrent
//! uploads finish, so a list taken from the store directly differs between runs
//! and anything built from it would differ too. [`Plan::from_snapshot`] therefore
//! orders by the snapshot's own chunk table, a function of read order and nothing
//! else: two backups of one tree produce the same plan.
//!
//! That determinism is why a plan is a list of *chunks* and not a list of ranges.
//! A store range has to stop at every record header, so a range list taken against
//! a store coalesces almost never. A pack has no framing between its payloads, so
//! the same chunks end to end *are* contiguous — see [`crate::pack`].
//!
//! [`Plan::total_bytes`] is therefore the size of a pack, not of a store byte
//! range, and is computable before reading a byte.

use std::io;

use crate::metadata::{FileChunk, Snapshot};
use crate::storage::{BlobId, Placement};

/// Where one distinct chunk lives, and how much of it a version actually uses.
///
/// `length` is what a restore reads for this chunk, not the chunk's whole length:
/// one spanning two files is stored whole and used in part, and one spanned by
/// repeated content may be used at several offsets.
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
    /// [`Plan`] is also [`IntoIterator`] and [`AsRef<[PlannedChunk]>`], so
    /// `for chunk in &plan` and `plan.as_ref().len()` both work and the
    /// `plan.chunks().iter()` double hop is not needed. Both spellings were in
    /// the tree, and the double hop reads like a mistake.
    pub fn iter(&self) -> std::slice::Iter<'_, PlannedChunk> {
        self.chunks.iter()
    }

    /// Resolve `snapshot` against the placements `locate` reports.
    ///
    /// `locate` is given a chunk hash and returns where it is, or
    /// [`io::ErrorKind::NotFound`] if the store does not hold it. A callback
    /// rather than a store, so this works for any backend.
    ///
    /// # Errors
    ///
    /// Returns an error if `locate` fails for any chunk the snapshot names, or if
    /// a slice names a **chunk** index that does not exist.
    ///
    /// A slice naming a *file* index that does not exist is not refused here, and
    /// used to be claimed to be. Nothing in a `Plan` consumes `file_index` — the
    /// tables are flattened to one entry per distinct chunk — so checking it would
    /// be this function reaching into a table it does not read. It is checked
    /// where it is used, by
    /// [`Snapshot::verify_file_lengths`](crate::metadata::Snapshot::verify_file_lengths),
    /// which a restore runs before it writes anything.
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

            // Against the *raw* length: a snapshot records its slices in content
            // bytes, while a compressed placement's `len` is the frame's size on
            // disk. Comparing content to storage would accept a chunk that is
            // genuinely too short, or reject a good one.
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
    /// chunk once — the size of a *pack*, not of the tree.
    ///
    /// Not a store byte range: a caller fetching from a store cannot use this
    /// number, because the chunks are separated by record headers.
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
/// Names only the chunk index, because that is the only one checked — see
/// [`Plan::from_snapshot`]. It used to name the file index too, and to say
/// "cannot be resolved", which read as though both were being resolved. A message
/// that reports a check the function does not perform is worse than a short one:
/// it tells a reader the snapshot was fully validated when it was not.
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

#[cfg(test)]
mod tests {
    use super::Plan;
    use crate::metadata::{Attributes, File, FileChunk, Snapshot};
    use crate::storage::{BlobId, Codec, Placement};

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
        let mut s = Snapshot::new();
        s.files.push(File {
            path: "a.bin".into(),
            len: 100,
            attrs: Attributes {
                mode: Some(0o644),
                mtime: None,
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
        let mut s = Snapshot::new();
        s.files.push(File {
            path: "a.bin".into(),
            len: 40,
            attrs: Attributes {
                mode: Some(0o644),
                mtime: None,
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
        let plan = Plan::from_snapshot(&Snapshot::new(), |_| Ok(at(0, 0))).unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.total_bytes(), 0);
    }
}
