//! The operations a backup tool performs: taking a snapshot, restoring one, and
//! checking that a stored snapshot is still intact.

mod backup;
mod pipeline;
mod restore;
mod verify;

pub use backup::{Options, backup};
pub use restore::{DestinationLock, Restored, lock_destination, restore};
pub use verify::{Failure, Report, verify};
