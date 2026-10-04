//! Answering "where is version N's snapshot" without scanning the whole set.
//!
//! Every open scans, and the scan is proportional to the number of records in the
//! set. The per-version index files exist to avoid that scan. This is the path that
//! uses them.
//!
//! # The rule: an index file is checked, never believed
//!
//! Three things can be wrong with one. Each is caught differently:
//!
//! * *stale*: the set has run since the index was written, so a version has no
//!   index. That is not a failure, only the slow path.
//! * *from another set*: it names more versions than this set holds, or a segment
//!   that is not here. The code refuses it, because an over-reaching index would
//!   otherwise answer about a version that does not exist.
//! * *edited*: it names a version, a segment and a hash that do not go together.
//!   The hash is the check. The code reads the commit records of the segment that
//!   the index named. The version it claims must be among them with that hash.
//!
//! A wrong index costs a scan. A forged one costs an error. Neither can make a
//! set report a version that the set does not hold. **The store's commit records
//! decide what versions exist. This module only decides how much work it takes
//! to find out.**

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use crate::{
    layout::IndexSet,
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
/// `versions` is the number of versions the set is known to hold. An index that
/// names version 40 of a three-version set describes something else. The code
/// refuses it, because refusing it is faster than scanning for a version that is
/// not there.
///
/// No I/O proportional to the size of the set: one small file, and the commit
/// records of the one segment it named.
///
/// **That last part is the expensive half.** A caller that already has the commit
/// log should not pay for it again. Reading a segment's commits walks every record
/// header in it. Resolving N versions against one segment reads that segment N
/// times. `lepatch versions` resolves one version at a time through this
/// function. [`BlobStore`](crate::storage::BlobStore) holds the whole log in
/// memory already, because its own open scans for it. That store uses
/// [`resolve_with`] and skips the file entirely.
#[must_use]
pub fn resolve(set: &IndexSet, version: u32, versions: u32) -> Resolved {
    resolve_with(set, version, versions, |segment| {
        read_commits(&set.segment_path(segment), segment).ok()
    })
}

/// [`resolve`], with the commit log supplied rather than read.
///
/// `commits` receives the segment number that the index named. It answers with
/// that segment's commit records in order, or with `None` if they cannot be had.
/// A missing segment, an unreadable one, or one whose header does not name the
/// number it was asked for all answer `None`. A caller has to check the first two
/// separately otherwise. The code checks them *here*, so a caller that supplies its
/// own log cannot accidentally skip the checks that make an index trustworthy.
///
/// The function takes a closure rather than a `&[BlobId]`. The log a caller
/// already has is usually a slice of a longer one. A store's commits are one
/// sequence across every segment. A slice parameter would make every caller do
/// the offset arithmetic that the ordinal is defined in terms of.
///
/// # Panics
///
/// Never. A hostile index naming an enormous `ordinal` cannot overflow anything
/// here: the position is used to index a slice, which returns `None` rather than
/// wrapping.
#[must_use]
pub fn resolve_with(
    set: &IndexSet,
    version: u32,
    versions: u32,
    commits: impl Fn(u32) -> Option<Vec<BlobId>>,
) -> Resolved {
    // Absent, truncated, wrong checksum and unreadable are all the same to a
    // caller with a slow path: no usable index.
    let Some(index) = VersionIndex::read(&set.index_path(version)) else {
        return Resolved::Absent;
    };

    if index.version != version || version > versions {
        return Resolved::Disagrees;
    }

    // The segment has to be there. A caller that supplies its own log has usually
    // already established that. However, "usually" is not a check, and this check
    // costs a `stat` against a file the caller is about to trust.
    if !set.segment_path(index.segment).is_file() {
        return Resolved::Disagrees;
    }

    let Some(commits) = commits(index.segment) else {
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

    // The value is already a `Vec<BlobId>`. The collect was re-allocating and
    // re-copying every hash to change nothing. Clippy's `useless_conversion` could
    // not see that while `BlobId` was an alias for `[u8; HASH]`. The two types
    // were spelled differently then, and one of them was a synonym.
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
