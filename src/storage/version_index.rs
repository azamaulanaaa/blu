// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A per-version index file: which segment a version's snapshot is in, and when
//! the version ran.
//!
//! ```text
//! version (4) | segment (4) | ordinal in segment (4) | snapshot hash (32) |
//! UTC seconds (8) | nanos (4) | check (8)
//! ```
//!
//! It answers "where is version 7's snapshot" without a scan of every record
//! header in the set. It also answers "when did version 7 run" without
//! reading the snapshot blob.
//!
//! ## This is a cache
//!
//! The store's commit records are the record of which versions exist. An index
//! file is derived from them. A wrong or missing index file costs a scan, not a
//! backup. The commit record is the commit, one append to one file. This index
//! file is written afterwards, best-effort. Nothing here is trusted without a
//! check against the store. Nothing here can make a store report a version that
//! the store does not hold. See [`resolve`](super::resolve).
//!
//! ## Why the hash is in here at all
//!
//! The hash is the check. An index can be stale, or copied from another set, or
//! edited. The hash says which segment holds *this* snapshot. A resolve that reads
//! the wrong segment gets a snapshot whose bytes do not hash to what the index
//! claimed. The resolve says so instead of returning it.

use std::{fs, io, path::Path};

use crate::{metadata::Timestamp, storage::BlobId};

/// Bytes of a version index: three counts, a hash, a stamp, and a checksum.
const LEN: usize = 4 + 4 + 4 + 32 + 8 + 4 + 8;

/// Bytes the checksum covers: everything ahead of itself.
const UNCHECKED: usize = LEN - 8;

fn check(head: &[u8]) -> [u8; 8] {
    super::hash::truncated_hash(head)
}

/// Where a version's snapshot is, and when the version ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionIndex {
    /// The version this file describes.
    pub version: u32,
    /// Which segment of the set the snapshot record is in.
    pub segment: u32,
    /// Which commit record *within that segment* the version is.
    ///
    /// A global version number would need every earlier segment scanned to
    /// resolve. That scan is the work this file exists to avoid. A position within
    /// the segment is self-contained: read that one segment, take that commit.
    pub ordinal: u32,
    /// The snapshot's content hash, and the check on everything above.
    pub snapshot: BlobId,
    /// Time the version ran, in UTC seconds and nanos.
    ///
    /// The store keeps UTC. Display converts to local time.
    /// The value matches the stamp inside the snapshot blob.
    pub taken_at: Timestamp,
}

impl VersionIndex {
    /// Byte length of an encoded version index.
    ///
    /// This is the byte length of an encoded version index. A remote backend
    /// expects exactly this many bytes in the index object. A fetched object
    /// of any other length is no usable index. A local file of the wrong
    /// length gives the same result. [`VersionIndex::read`] returns `None`
    /// for it.
    pub const LEN: usize = LEN;

    /// The file's bytes.
    #[must_use]
    pub fn to_bytes(self) -> [u8; LEN] {
        let mut out = [0u8; LEN];
        out[..4].copy_from_slice(&self.version.to_le_bytes());
        out[4..8].copy_from_slice(&self.segment.to_le_bytes());
        out[8..12].copy_from_slice(&self.ordinal.to_le_bytes());
        out[12..44].copy_from_slice(self.snapshot.as_bytes());
        out[44..52].copy_from_slice(&self.taken_at.secs.to_le_bytes());
        out[52..56].copy_from_slice(&self.taken_at.nanos.to_le_bytes());

        // Computed before the final copy, because the checksum is over everything
        // ahead of itself and the borrow cannot overlap.
        let guard = check(&out[..UNCHECKED]);
        out[56..].copy_from_slice(&guard);

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
        let mut secs = [0u8; 8];
        secs.copy_from_slice(&bytes[44..52]);
        let mut nanos = [0u8; 4];
        nanos.copy_from_slice(&bytes[52..56]);

        Ok(Self {
            version: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            segment: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            ordinal: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            snapshot: BlobId::from_bytes(snapshot),
            taken_at: Timestamp {
                secs: i64::from_le_bytes(secs),
                nanos: u32::from_le_bytes(nanos),
            },
        })
    }

    /// Decode index bytes fetched from anywhere, or `None`.
    ///
    /// This is the remote counterpart to [`VersionIndex::read`]. A backend
    /// reads the index object with one GET. It sends the bytes to this
    /// function. These cases all mean no usable index:
    ///
    /// - Absent data.
    /// - Truncated data.
    /// - Data with a wrong checksum.
    /// - Unreadable data.
    ///
    /// The caller then falls back to the commit-record scan. The local path
    /// applies the same rule. The backend writes the bytes with
    /// [`VersionIndex::to_bytes`] and one PUT.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Self::try_from(bytes).ok()
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
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("could not create temporary in {}: {e}", dir.display()),
            )
        })?;

        {
            use std::io::Write as _;
            tmp.write_all(&self.to_bytes()).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("could not write {}: {e}", tmp.path().display()),
                )
            })?;
            tmp.as_file().sync_all().map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("could not sync {}: {e}", tmp.path().display()),
                )
            })?;
        }

        tmp.persist(path).map_err(|e| {
            io::Error::new(
                e.error.kind(),
                format!(
                    "could not rename temporary into {}: {}",
                    path.display(),
                    e.error
                ),
            )
        })?;

        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            super::at(parent, "open to sync it", fs::File::open(parent))?.sync_all()?;
        }

        Ok(())
    }

    /// Read the index at `path`, or `None` if it is not there or not usable.
    ///
    /// Never an error. Absent, truncated, wrong checksum and unreadable are all
    /// "no usable index" to a caller treating this as a cache. An index that
    /// cannot be read is an index that must be rebuilt. A rebuild is always
    /// available. See [`crate::storage::resolve`] for what a caller does next.
    ///
    /// A directory at `path` is also `None`, not an error. This doc comment used
    /// to call out that case as the one thing not swallowed. The old sentence said
    /// that "a directory that cannot be read at all is not swallowed, that is a
    /// problem with the set". The code underneath cannot tell that case from any
    /// other. `fs::read(path).ok()?` discards the `io::Error` whole, so a
    /// permission problem and a directory both arrive as `None`. The sentence
    /// described an intention the implementation never had.
    ///
    /// The distinction is not lost. It is not made here. A set whose directory
    /// cannot be read fails at `IndexSet::indexed_versions`. That function lists
    /// the directory it could not read and says so. The store's own scan refuses
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
    /// `TryFrom` rather than an inherent `from_bytes`. The choice makes "these
    /// bytes are a version index" a conversion like any other. This type then
    /// shares a vocabulary with everything else that parses a file. One
    /// implementation, not an inherent method beside a trait impl. A `from_bytes`
    /// that only called `try_from` would be a second spelling of one decision.
    /// Removing second spellings is most of what this branch has been about.
    ///
    /// The whole file rather than a prefix. An index is exactly `LEN` bytes, so
    /// the length is a check and not a bound.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is the wrong length. A file of the wrong
    /// length is not one of these. Returns an error if the checksum does not
    /// verify. A checksum failure means a torn write or something else entirely.
    /// [`resolve`](super::resolve) treats both errors as "no index". A cache that
    /// cannot be read is not a cache that can be believed.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::parse(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{Timestamp, VersionIndex};
    use crate::storage::BlobId;

    fn stamped() -> Timestamp {
        Timestamp {
            secs: 1_700_000_000,
            nanos: 42,
        }
    }

    /// Index bytes round-trip without the filesystem.
    ///
    /// A remote backend writes [`VersionIndex::to_bytes`] with one PUT. It
    /// reads the bytes with one GET into [`VersionIndex::decode`]. Wrong
    /// length and torn bytes mean no usable index. They never cause an error.
    /// The test uses a few dozen bytes in memory.
    #[test]
    fn index_bytes_round_trip_without_the_filesystem() {
        let index = VersionIndex {
            version: 7,
            segment: 2,
            ordinal: 3,
            snapshot: BlobId::from_bytes([5u8; 32]),
            taken_at: stamped(),
        };

        let bytes = index.to_bytes();
        assert_eq!(bytes.len(), VersionIndex::LEN);
        assert_eq!(VersionIndex::decode(&bytes), Some(index));
        assert_eq!(VersionIndex::decode(&bytes[..VersionIndex::LEN - 1]), None);

        let mut torn = bytes;
        torn[0] ^= 0xff;
        assert_eq!(VersionIndex::decode(&torn), None);
    }

    /// The stamp travels with the index and survives the round trip.
    ///
    /// A listing reads the stamp off the file rather than the blob.
    /// A stamp that the round trip drops would list every version wrong.
    #[test]
    fn index_records_taken_at() {
        let index = VersionIndex {
            version: 1,
            segment: 0,
            ordinal: 0,
            snapshot: BlobId::from_bytes([5u8; 32]),
            taken_at: stamped(),
        };

        let decoded = VersionIndex::try_from(&index.to_bytes()[..]).expect("decodes");

        assert_eq!(decoded.taken_at, stamped());
        assert_eq!(decoded.taken_at.to_utc_string(), stamped().to_utc_string());
    }
}
