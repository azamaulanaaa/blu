//! Turning a snapshot into an ordered list of chunks in a store.
//!
//! A [`crate::metadata::Snapshot`] is a location-free recipe: which chunks exist,
//! which file each slice of each chunk belongs in, and where in that file. It
//! says nothing about where the bytes are. This module is the step that asks the
//! store and produces the answer — the one thing a pack writer needs and cannot
//! get for itself.
//!
//! The order is the point, and it is not incidental. Chunks are appended to a
//! store in whatever order concurrent uploads happen to finish, which is
//! dedup-driven and arbitrary: two backups of the same tree land their chunks in
//! different places. So a list taken from the store directly would differ between
//! runs, and anything built from it — a pack, a diff, a delta download — would
//! differ too. [`Plan::from_snapshot`] therefore orders by the snapshot's own
//! chunk table, which is a function of read order and nothing else. Two backups
//! of one tree produce the same plan.
//!
//! That determinism is what makes a plan the right input to a pack, and it is why
//! a plan is a list of *chunks* and not a list of ranges. A range in a store has
//! to stop at every record header, because the header of one chunk is not part of
//! the next, so a range list taken against a store coalesces almost never and
//! cannot be used to serve a delta download. A pack has no framing between its
//! payloads, so the same chunks laid end to end *are* contiguous — which is the
//! argument for packing being a separate artifact rather than a flag. See
//! [`crate::pack`].
//!
//! [`Plan::total_bytes`] is the size a concatenation of every distinct chunk
//! would occupy. It is a property of the plan alone, and is what makes the plan
//! worth computing before reading a byte: a caller can size a download, or decide
//! a transport, from it.

use std::io;

use crate::metadata::{FileChunk, Snapshot};
use crate::storage::{BlobId, Placement};

/// Where one distinct chunk lives, and how much of it a version actually uses.
///
/// `length` is the number of bytes a restore reads for this chunk, which is not
/// the chunk's whole length. A chunk that runs past the end of one file and into
/// the next is stored whole but only partly used, and a chunk entirely spanned by
/// repeated content may be used at several offsets and no more than once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Planned {
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
    chunks: Vec<Planned>,
}

impl Plan {
    /// Resolve `snapshot` against the placements `locate` reports.
    ///
    /// `locate` is given a chunk hash and returns where that chunk is, or
    /// [`io::ErrorKind::NotFound`] if the store does not hold it. It is a
    /// callback rather than a store so this works for any backend, including one
    /// that is not a single local file.
    ///
    /// # Errors
    ///
    /// Returns an error if `locate` fails for any chunk the snapshot names, or
    /// if the snapshot's own tables disagree — a slice naming a chunk index or a
    /// file index that does not exist. Both mean the snapshot and the store
    /// disagree about what this version is, and there is nothing to resolve.
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
                // Listed but never read: a chunk the snapshot carries and no file
                // slice refers to. Keeping it would put a zero-length range in a
                // range list, and there is nothing to read.
                continue;
            }

            let placement = locate(id)?;
            let stored = placement.len;

            if length > stored {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "a snapshot needs {} bytes of chunk {} but the store holds {stored}",
                        length,
                        crate::storage::hex(id)
                    ),
                ));
            }

            chunks.push(Planned {
                id: *id,
                placement,
                length,
            });
        }

        Ok(Self { chunks })
    }

    /// The chunks, in the snapshot's own chunk order.
    #[must_use]
    pub fn chunks(&self) -> &[Planned] {
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

    /// Bytes a concatenation of every distinct chunk would occupy.
    ///
    /// This counts each chunk once, so it is the size of a *pack*, not of the
    /// tree. It is less than the sum of the files' lengths whenever a chunk is
    /// used by more than one of them, which is the deduplication showing up as
    /// an arithmetic fact rather than as a time saving.
    ///
    /// A caller fetching out of a store cannot use this number, because the
    /// chunks are separated by the store's own record headers and are not
    /// contiguous: a range has to stop at every header. The same chunks laid end
    /// to end with the framing left out are contiguous, and that is a pack — see
    /// [`crate::pack`].
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.chunks
            .iter()
            .map(|c| c.length)
            .fold(0u64, u64::saturating_add)
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

/// A slice of the store this version needs, as a half-open byte range.
fn bad_index(snapshot: &Snapshot, slice: &FileChunk) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "a snapshot slice names chunk {} of {} and file {} of {}, which cannot be resolved",
            slice.chunk_index,
            snapshot.chunks.len(),
            slice.file_index,
            snapshot.files.len()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::Plan;
    use crate::metadata::{File, FileChunk, Snapshot};
    use crate::storage::{BlobId, Placement};

    fn chunk() -> BlobId {
        [7u8; 32]
    }

    fn at(offset: u64, len: u64) -> Placement {
        Placement {
            segment: 0,
            offset,
            len,
        }
    }

    fn at_in(segment: u32, offset: u64, len: u64) -> Placement {
        Placement {
            segment,
            offset,
            len,
        }
    }

    /// A snapshot of one file of 100 bytes made of two 50-byte chunks.
    fn two_chunks() -> Snapshot {
        let mut s = Snapshot::new();
        s.files.push(File {
            path: "a.bin".into(),
            len: 100,
            mode: 0o644,
            mtime: None,
        });
        s.chunks.push(chunk());
        s.chunks.push([8u8; 32]);
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
            mode: 0o644,
            mtime: None,
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
        s.chunks.push([9u8; 32]);

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
