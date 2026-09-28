//! The operations a backup tool performs: taking a snapshot, restoring one, and
//! checking that a stored snapshot is still intact.

use std::io;
mod backup;
mod pipeline;
mod restore;
mod verify;

pub use backup::{Committed, Options, backup};
pub use restore::{
    DestinationLock, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, Restored, lock_destination, restore,
};
pub use verify::{Failure, Report, verify};

/// How many things to work on at once, given a ceiling for what is worth doing.
///
/// The machine's parallelism, clamped into `1..=ceiling`. A single helper for
/// all three operations, because they had drifted: two clamped the low end and one
/// used `min`, so a platform reporting no usable parallelism would have had a
/// backup run with a concurrency of zero and a restore fall back to one.
pub(super) fn concurrency(ceiling: usize) -> usize {
    std::thread::available_parallelism().map_or(ceiling, |n| n.get().clamp(1, ceiling))
}

/// Read a version's snapshot out of a store and decode it.
///
/// Restore and verify both need this and had it written out twice. The decode
/// error is mapped to [`io::ErrorKind::InvalidData`] rather than surfaced as the
/// decoder's own type, because every caller here is already an `io::Result` and a
/// malformed snapshot is a damaged store either way.
pub(super) async fn load_snapshot<S: crate::storage::StorageGet>(
    storage: &S,
    snapshot_id: crate::storage::BlobId,
) -> io::Result<crate::metadata::Snapshot> {
    use tokio::io::AsyncReadExt as _;

    let mut reader = storage.get(&snapshot_id).await?;
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;

    crate::metadata::Snapshot::from_bytes(buffer.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
