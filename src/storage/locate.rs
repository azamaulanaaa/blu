//! Answering "where is version N's snapshot" without scanning the whole set.
//!
//! Every open scans, and the scan is proportional to the number of records in the
//! set. The per-version index files exist to avoid that, and this is the path that
//! uses them.
//!
//! # The rule: an index file is checked, never believed
//!
//! Three things can be wrong with one, and each is caught differently:
//!
//! * *stale* — the set has run since it was written, so a version has no index.
//!   That is not a failure, only the slow path.
//! * *from another set* — it names more versions than this set holds, or a
//!   segment that is not here. Refused, because an over-reaching index would
//!   otherwise answer about a version that does not exist.
//! * *edited* — it names a version, a segment and a hash that do not go together.
//!   The hash is the check: the commit records of the segment it named are read,
//!   and the version it claims must be among them with that hash.
//!
//! So a wrong index costs a scan and a forged one costs an error. Neither can
//! make a set report a version it does not hold: **the store's commit records
//! decide what versions exist, and this module only decides how much work it
//! takes to find out.**

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use crate::{
    index::IndexSet,
    storage::{BlobId, VersionIndex, record},
};

/// Bytes of a store header.
const STORE_HEADER: usize = record::STORE_HEADER;

/// What an index file claimed, and whether the store agrees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// The index named a segment, that segment holds the version, and the commit
    /// record in it names the snapshot the index claimed.
    Confirmed {
        /// Where the version's snapshot is.
        segment: u32,
        /// The snapshot's hash.
        snapshot: BlobId,
    },
    /// No usable index for this version. Fall back to the scan: correct, slower.
    Absent,
    /// An index exists and does not describe this set. Never used.
    Disagrees,
}

impl Resolved {
    /// The snapshot and its segment, if the index was confirmed.
    #[must_use]
    pub fn confirmed(self) -> Option<(u32, BlobId)> {
        match self {
            Self::Confirmed { segment, snapshot } => Some((segment, snapshot)),
            Self::Absent | Self::Disagrees => None,
        }
    }
}

/// Read the index for `version` and check it against the segments on disk.
///
/// `versions` is the number of versions the set is known to hold: an index naming
/// version 40 of a three-version set is describing something else, and refusing it
/// is faster than scanning for a version that is not there.
///
/// No I/O proportional to the size of the set: one small file, and the commit
/// records of the one segment it named.
#[must_use]
pub fn resolve(set: &IndexSet, version: u32, versions: u32) -> Resolved {
    // Absent, truncated, wrong checksum and unreadable are all the same to a
    // caller with a slow path: no usable index.
    let Some(index) = VersionIndex::read(&set.index_path(version)) else {
        return Resolved::Absent;
    };

    if index.version != version || version > versions {
        return Resolved::Disagrees;
    }

    let segment_path = set.segment_path(index.segment);
    if !segment_path.is_file() {
        return Resolved::Disagrees;
    }

    let Ok(commits) = read_commits(&segment_path, index.segment) else {
        return Resolved::Disagrees;
    };

    // The check. The position is *within* the segment, which is why the file
    // records one. An index copied from another set names a different snapshot, so
    // it fails here even when the segment exists.
    let held = usize::try_from(index.ordinal)
        .ok()
        .and_then(|ordinal| commits.get(ordinal));

    if held != Some(&index.snapshot) {
        return Resolved::Disagrees;
    }

    Resolved::Confirmed {
        segment: index.segment,
        snapshot: index.snapshot,
    }
}

/// Read one segment's commit records: headers only, so proportional to the
/// records in one segment rather than the whole set.
fn read_commits(path: &Path, segment: u32) -> std::io::Result<Vec<BlobId>> {
    let mut file = fs::File::open(path)?;

    let mut header = [0u8; STORE_HEADER];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;

    if record::decode_store_header(&header).ok() != Some(segment) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} is not segment {segment} of a store this build reads",
                path.display()
            ),
        ));
    }

    // Already a `Vec<BlobId>`, and the collect was re-allocating and re-copying
    // every hash to change nothing. Clippy's `useless_conversion` could not see
    // it while `BlobId` was an alias for `[u8; HASH]`, because the two types
    // were spelled differently and one of them was a synonym.
    record::commits_in(&mut file, segment)
}

impl Resolved {
    /// Why this answer was reached, for a message a person reads.
    ///
    /// Empty for a confirmed index, which needs no explanation.
    #[must_use]
    pub fn reason(self, version: u32, set: &IndexSet) -> String {
        match self {
            Self::Confirmed { .. } => String::new(),
            Self::Absent => format!(
                "version {version} of {} has no index file",
                set.base().display()
            ),
            Self::Disagrees => format!(
                "the index file for version {version} of {} does not describe this set, so it \
                 was not used",
                set.base().display()
            ),
        }
    }
}
