//! A per-version index file: which segment a version's snapshot is in.
//!
//! ```text
//! version (4) | segment (4) | ordinal in segment (4) | snapshot hash (32) | check (8)
//! ```
//!
//! The whole of the file is a few dozen bytes and it exists to answer one
//! question without a scan. Without it, "where is version 7's snapshot" means
//! reading the header of every record in every segment of the set, and that scan
//! is on the critical path of every restore and every verify.
//!
//! ## This is a cache
//!
//! The store's commit records are the record of which versions exist. An index
//! file is derived from them, and a wrong or missing one costs a scan rather than
//! a backup. Nothing in this module is trusted without being checked against the
//! store, and nothing here can make a store report a version it does not hold —
//! see [`VersionIndex::resolve`].
//!
//! That is the inversion from the previous layout, where an index file *was* the
//! only place a version existed. There, publishing the index was the commit, and
//! it was a cross-file commit: the index named an offset in the store and nothing
//! could make the two land together. Here the commit record is the commit — one
//! append to one file — and this file is written afterwards, best-effort.
//!
//! ## Why the hash is in here at all
//!
//! Because it is the check. An index can be stale, or copied from another set, or
//! edited; the hash is what says which segment holds *this* snapshot. A resolve
//! that reads the wrong segment gets a snapshot whose bytes do not hash to what
//! the index claimed, and says so rather than returning it.

use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use crate::storage::BlobId;

/// Bytes of a version index: three counts, a hash, and a checksum.
const LEN: usize = 4 + 4 + 4 + 32 + 8;

/// Bytes the checksum covers: everything ahead of itself.
const UNCHECKED: usize = LEN - 8;

fn check(head: &[u8]) -> [u8; 8] {
    super::truncated_hash(head)
}

/// Where a version's snapshot is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionIndex {
    /// The version this file describes.
    pub version: u32,
    /// Which segment of the set the snapshot record is in.
    pub segment: u32,
    /// Which commit record *within that segment* the version is.
    ///
    /// This is what makes the file checkable from one segment. The versions a
    /// set holds are the concatenation of its segments' commit records, so a
    /// segment on its own cannot say which global version one of its commits is —
    /// that would need the segments before it scanned too, which is the work the
    /// file exists to avoid. A position *within* the segment is self-contained:
    /// read that one segment, take that commit, and the snapshot is the answer.
    pub ordinal: u32,
    /// The snapshot's content hash, and the check on everything above.
    pub snapshot: BlobId,
}

impl VersionIndex {
    /// The file's bytes.
    #[must_use]
    pub fn to_bytes(self) -> [u8; LEN] {
        let mut out = [0u8; LEN];
        out[..4].copy_from_slice(&self.version.to_le_bytes());
        out[4..8].copy_from_slice(&self.segment.to_le_bytes());
        out[8..12].copy_from_slice(&self.ordinal.to_le_bytes());
        out[12..44].copy_from_slice(&self.snapshot);

        // Computed before the final copy, because the checksum is over everything
        // ahead of itself and the borrow cannot overlap.
        let guard = check(&out[..UNCHECKED]);
        out[44..].copy_from_slice(&guard);

        out
    }

    /// Read an index file's bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is the wrong length, which is a file that is
    /// not one of these, or if the checksum does not verify, which is a torn write
    /// or something else entirely. Both are treated as "no index" by
    /// [`resolve`](super::BlobStore::snapshot): a cache that cannot be read is
    /// not a cache that can be believed.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "a version index is {LEN} bytes, and this one is {}",
                    bytes.len()
                ),
            ));
        }

        if bytes[UNCHECKED..] != check(&bytes[..UNCHECKED]) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a version index does not match its own checksum",
            ));
        }

        let mut snapshot = [0u8; 32];
        snapshot.copy_from_slice(&bytes[12..44]);

        Ok(Self {
            version: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            segment: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            ordinal: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            snapshot,
        })
    }

    /// Write this index to `path`, durably.
    ///
    /// The file is created with a temporary name and renamed into place, so a
    /// reader never sees a half-written index. That matters less than it would
    /// for the store — a torn index is simply ignored and the scan answers — but
    /// `rename` is free and the alternative is a window where a version's index
    /// exists and is wrong.
    ///
    /// The parent directory is synced afterwards, so the rename itself is durable
    /// and not just the bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written, renamed, or the directory
    /// synced.
    pub fn write_atomic(&self, path: &Path) -> io::Result<()> {
        let temporary = temporary_for(path);

        {
            let mut file = fs::File::create(&temporary)?;
            file.write_all(&self.to_bytes())?;
            file.sync_all()?;
        }

        fs::rename(&temporary, path)?;

        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::File::open(parent)?.sync_all()?;
        }

        Ok(())
    }

    /// Read the index at `path`, or `None` if it is not there or not readable.
    ///
    /// Never an error. Every way this can go wrong — absent, truncated, wrong
    /// checksum, unreadable directory — is the same thing to a caller that treats
    /// this as a cache: no usable index, so fall back to the scan. The only thing
    /// deliberately not swallowed is a directory that cannot be read at all, which
    /// is a problem with the set rather than with the cache.
    #[must_use]
    pub fn read(path: &Path) -> Option<Self> {
        let bytes = fs::read(path).ok()?;
        Self::from_bytes(&bytes).ok()
    }
}

/// A sibling path to write an index to first, before renaming it.
///
/// Deliberately in the same directory, because `rename` is only atomic within one
/// filesystem, and a temporary file in `/tmp` would be a copy rather than a move
/// on the wrong side of that boundary.
#[must_use]
fn temporary_for(path: &Path) -> std::path::PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);

    // A fixed suffix rather than a pid or a counter: two runs cannot be writing
    // the same set at once, because the lock says so.
    name.push(".tmp");

    path.with_file_name(name)
}
