//! How large a segment gets before a new one is started.
//!
//! One value, and it is a write policy rather than part of the format. No record
//! says where a boundary fell. A set written under one limit reads back under any
//! other. This value has its own file rather than living in `compression`. The two
//! files decide different things. This one decides how the log is cut into files.
//! `compression` decides what goes inside a record. The one caller that wants both
//! takes them as two arguments, while a reader takes neither.

use super::record;

/// How large a store segment may get before a new one is started.
///
/// A write policy, not part of the format. A set written under one limit reads
/// back under any other, because no record depends on where a segment boundary
/// fell. A reader is given a limit only so one value can be passed to both
/// constructors. The reader ignores it.
///
/// The default is 4 GiB, under what FAT32 and several older archive formats
/// refuse, and small enough that no single file needs 64-bit offsets.
///
/// # Example
///
/// Runs, not `no_run`: `new` and `bytes` are pure, and an example that only
/// compiles is an example whose arithmetic can be wrong.
///
/// ```
/// # fn main() -> std::io::Result<()> {
/// use lepatch::storage::SegmentLimit;
///
/// assert_eq!(SegmentLimit::default().bytes(), 4 * 1024 * 1024 * 1024);
///
/// // A smaller limit is honoured: a set of smaller segments reads back the same,
/// // because where a boundary fell is not recorded anywhere.
/// assert_eq!(SegmentLimit::new(64 * 1024 * 1024).bytes(), 64 * 1024 * 1024);
///
/// // A limit too small to hold a store header and one record is raised to that
/// // floor rather than refused, because a segment that cannot hold a record
/// // cannot be a segment. The floor is a property of the format rather than of
/// // this policy, so `new` has no way to express anything below it.
/// assert!(SegmentLimit::new(1).bytes() > 1, "clamped up to a usable segment");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentLimit(u64);

impl SegmentLimit {
    /// A limit of `bytes`.
    ///
    /// Clamped to a floor of one store header plus one record, because a segment
    /// that cannot hold its own header cannot be a segment.
    #[must_use]
    pub fn new(bytes: u64) -> Self {
        let floor = u64::try_from(record::STORE_HEADER).unwrap_or(u64::MAX) + MIN_RECORD;
        Self(bytes.max(floor))
    }

    /// The limit in bytes.
    #[must_use]
    pub fn bytes(self) -> u64 {
        self.0
    }
}

impl Default for SegmentLimit {
    fn default() -> Self {
        Self(4 * 1024 * 1024 * 1024)
    }
}

/// The smallest record that can be written: a header and an empty payload.
///
/// This value comes from the record format's own constant rather than a
/// restatement of its parts. The constant was `6 + 2 + 32 + 8`. That value sits
/// one module away from the `PREFIX + 2 + HASH + CHECK` it copies. The `2` counts
/// two one-byte lengths of zero. The original documented that `2`, and this
/// comment does too. A header that grew a field would move `MIN_HEADER` and leave
/// this value behind. Only `SegmentLimit::new`'s floor would break, and it would
/// break silently, on the way to a store that refuses its own records.
const MIN_RECORD: u64 = record::MIN_HEADER as u64;
