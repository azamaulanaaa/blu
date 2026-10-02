//! A per-version index file: which segment a version's snapshot is in.
//!
//! ```text
//! version (4) | segment (4) | ordinal in segment (4) | snapshot hash (32) | check (8)
//! ```
//!
//! It answers "where is version 7's snapshot" without a scan of every record
//! header in the set.
//!
//! ## This is a cache
//!
//! The store's commit records are the record of which versions exist. An index
//! file is derived from them, and a wrong or missing one costs a scan rather than
//! a backup. The commit record is the commit, one append to one file; this is
//! written afterwards, best-effort. Nothing here is trusted without being checked
//! against the store, and nothing here can make a store report a version it does
//! not hold — see [`resolve`](super::resolve).
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
    super::hash::truncated_hash(head)
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
    /// A global version number would need every earlier segment scanned to
    /// resolve, which is the work this file exists to avoid. A position within
    /// the segment is self-contained: read that one segment, take that commit.
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
        out[12..44].copy_from_slice(self.snapshot.as_bytes());

        // Computed before the final copy, because the checksum is over everything
        // ahead of itself and the borrow cannot overlap.
        let guard = check(&out[..UNCHECKED]);
        out[44..].copy_from_slice(&guard);

        out
    }

    fn parse(bytes: &[u8]) -> io::Result<Self> {
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
            snapshot: BlobId::from_bytes(snapshot),
        })
    }

    /// Write this index to `path`, durably.
    ///
    /// Written to a sibling temporary file and renamed into place, so a reader
    /// never sees a half-written index. The parent directory is then synced, so
    /// the rename itself survives a crash and not just the bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written, renamed, or the directory
    /// synced.
    pub fn write_atomic(&self, path: &Path) -> io::Result<()> {
        let temporary = temporary_for(path);

        {
            let mut file = super::at(&temporary, "create", fs::File::create(&temporary))?;
            file.write_all(&self.to_bytes())?;
            file.sync_all()?;
        }

        super::at(path, "rename into place", fs::rename(&temporary, path))?;

        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            super::at(parent, "open to sync it", fs::File::open(parent))?.sync_all()?;
        }

        Ok(())
    }

    /// Read the index at `path`, or `None` if it is not there or not usable.
    ///
    /// Never an error: absent, truncated, wrong checksum and unreadable are all
    /// "no usable index" to a caller treating this as a cache, because an index
    /// that cannot be read is one that must be rebuilt, and rebuilding is always
    /// available. See [`crate::storage::resolve`] for what a caller does next.
    ///
    /// A directory at `path` is also `None`, not an error. It used to be called
    /// out here as the one thing not swallowed — "a directory that cannot be read
    /// at all is not swallowed, that is a problem with the set" — and the code
    /// underneath cannot tell that case from any other: `fs::read(path).ok()?`
    /// discards the `io::Error` whole, so a permission problem and a directory
    /// both arrive as `None`. The sentence described an intention the
    /// implementation never had.
    ///
    /// The distinction is not lost so much as not made here. A set whose directory
    /// cannot be read fails at `IndexSet::indexed_versions`, which lists the
    /// directory it could not read and says so, and the store's own scan refuses
    /// to open. This is the cache, and a cache that cannot be read is empty.
    #[must_use]
    pub fn read(path: &Path) -> Option<Self> {
        let bytes = fs::read(path).ok()?;
        Self::try_from(bytes.as_slice()).ok()
    }
}

impl TryFrom<&[u8]> for VersionIndex {
    type Error = io::Error;

    /// Parse an index file's bytes.
    ///
    /// `TryFrom` rather than an inherent `from_bytes`, so that "these bytes are a
    /// version index" is a conversion like any other and this type shares a
    /// vocabulary with everything else that parses a file. One implementation, not
    /// an inherent method beside a trait impl — a `from_bytes` that only called
    /// `try_from` would be a second spelling of one decision, and removing those
    /// is most of what this branch has been about.
    ///
    /// The whole file rather than a prefix: an index is exactly `LEN` bytes, so
    /// the length is a check and not a bound.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is the wrong length, which is a file that is
    /// not one of these, or if the checksum does not verify, which is a torn write
    /// or something else entirely. Both are treated as "no index" by
    /// [`resolve`](super::resolve): a cache that cannot be read is not a cache
    /// that can be believed.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::parse(bytes)
    }
}

/// A sibling path to write an index to first, before renaming it.
///
/// In the same directory, because `rename` is only atomic within one filesystem.
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
