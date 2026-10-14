// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The operations a backup tool performs: taking a snapshot, restoring one, and
//! checking that a stored snapshot is still intact.

use std::io;

use crate::{metadata, storage};
mod apply;
mod backup;
mod history;
mod manifest;
mod merge;
mod pack;
mod pipeline;
mod probe;
mod restore;
mod verify;

pub use apply::{Applied, Options as ApplyOptions, apply};
pub use backup::{BackupPlan, Committed, Options, backup, backup_plan};
pub use history::{Detail, Event, Status, check_query, report};
pub use manifest::{MANIFEST_SUFFIX, Manifest};
pub use merge::{
    Compression as MergeCompression, MergePlan, Merged, Planned, establish_base, merge, merge_plan,
    version_holding,
};
pub use pack::{Base, Options as PackOptions, Packed, Version, pack};
pub use probe::{Established, Mislength, Mismatch, Probed, Unheld, probe, probe_skipping};
pub use restore::{
    DestinationLock, PARTIAL_SUFFIX, PREVIOUS_SUFFIX, RestorePlan, Restored, lock_destination,
    restore, restore_plan,
};
pub use verify::{Failure, Reason, Report, verify};

/// How many things to work on at once: the machine's parallelism.
/// The value is clamped into `1..=ceiling`, so a platform reporting none
/// cannot produce a concurrency of zero.
pub(super) fn concurrency(ceiling: usize) -> usize {
    std::thread::available_parallelism().map_or(ceiling, |n| n.get().clamp(1, ceiling))
}

/// Read a version's snapshot out of a store and decode it.
///
/// Read whole, since a snapshot is held in memory while it is verified, restored
/// or packed. A decode failure is reported as [`io::ErrorKind::InvalidData`].
/// Every caller is already an `io::Result`, and a malformed snapshot is a
/// damaged store either way.
///
/// # Errors
///
/// Returns an error if the store does not hold that snapshot.
/// It returns an error if reading fails, or if the bytes do not decode as a
/// snapshot.
pub async fn load_snapshot<S: storage::StorageGet>(
    storage: &S,
    snapshot_id: storage::BlobId,
) -> io::Result<metadata::Snapshot> {
    use tokio::io::AsyncReadExt as _;

    let mut reader = storage.get(&snapshot_id).await?;
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;

    metadata::Snapshot::try_from(buffer.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    // `metadata::Error` has four variants, and all four are flattened to one
    // `ErrorKind` here. This looks like a loss. It is not, and this is where to
    // argue about it.
    //
    // The four variants are `Decode`, `BadMagic`, `UnsupportedVersion` and
    // `Truncated`.
    // `Decode` names a body that will not decode.
    // `BadMagic` names a blob that is not a snapshot.
    // `UnsupportedVersion` names a blob from a different layout.
    // `Truncated` names a blob that ends early.
    // All four keep their meaning.
    // The `Display` is the one a person reads, and it names which of the four
    // it is.
    // The strings are "not a blu snapshot: expected magic", "unsupported
    // snapshot format version N, this build supports M", and "snapshot is
    // truncated".
    // A caller that wants to *match* on them calls `Snapshot::try_from`, where
    // the enum is intact and public.
    // `tests/metadata.rs` does exactly that, and reaches none of this.
    //
    // The alternative is a crate-wide error enum with `From` in both
    // directions, so the ~40 passthrough `?`s keep working.
    // That alternative would add fifteen variants whose only consumer is
    // `main`, which prints them.
    // That is the same argument that retired `thiserror` and the `caused by:`
    // walk.
    // It has the same shape. This crate's errors are deliberately flat
    // complete sentences.
    // Nothing branches on them but one `AlreadyExists` check in
    // `lock_destination` and one `BrokenPipe` check in `say!`.
    // Both checks apply to the *underlying* OS error, not to a failure mode of
    // ours.
    //
    // There is a real distinction this does lose, and it is deliberately not
    // made here.
    // A corrupt snapshot and a store that cannot be read are both
    // `InvalidData`.
    // They are told apart by *where* the refusal happens.
    // `BlobStore::open_for_reading` refuses a store it cannot read whole,
    // before any snapshot is decoded.
    // `tests/verify.rs` pins that.
    // Moving the distinction into the kind would mean choosing between them
    // everywhere the two are produced.
}
