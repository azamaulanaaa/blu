//! The operations a backup tool performs: taking a snapshot, restoring one, and
//! checking that a stored snapshot is still intact.

use std::io;

use crate::{metadata, storage};
mod backup;
mod pipeline;
mod restore;
mod verify;

pub use backup::{Committed, Options, backup};
pub use restore::{
    DestinationLock, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, Restored, lock_destination, restore,
};
pub use verify::{Failure, Reason, Report, verify};

/// How many things to work on at once: the machine's parallelism, clamped into
/// `1..=ceiling` so a platform reporting none cannot produce a concurrency of
/// zero.
pub(super) fn concurrency(ceiling: usize) -> usize {
    std::thread::available_parallelism().map_or(ceiling, |n| n.get().clamp(1, ceiling))
}

/// Read a version's snapshot out of a store and decode it.
///
/// Read whole, since a snapshot is held in memory while it is verified, restored
/// or packed. A decode failure is reported as [`io::ErrorKind::InvalidData`],
/// because every caller is already an `io::Result` and a malformed snapshot is a
/// damaged store either way.
///
/// # Errors
///
/// Returns an error if the store does not hold that snapshot, if reading it
/// fails, or if the bytes do not decode as a snapshot.
pub async fn load_snapshot<S: storage::StorageGet>(
    storage: &S,
    snapshot_id: storage::BlobId,
) -> io::Result<metadata::Snapshot> {
    use tokio::io::AsyncReadExt as _;

    let mut reader = storage.get(&snapshot_id).await?;
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;

    metadata::Snapshot::from_bytes(buffer.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
