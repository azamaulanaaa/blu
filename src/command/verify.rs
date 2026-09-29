//! Integrity checking of a snapshot's stored chunks.
//!
//! Run over the whole snapshot before a restore writes anything, and deliberately
//! a separate pass from the writing: folding the hash into the copy would mean
//! writing the first file before finding out the chunk for the last is corrupt.
//!
//! Chunks are checked several at a time, each hashed as it streams rather than
//! buffered whole.

use std::{io, sync::Arc};

use tokio::{io::AsyncReadExt, task::JoinSet};
use tracing::instrument;

use crate::{
    metadata::Snapshot,
    progress::{Counter, Phase, Progress},
    storage::{self, StorageGet},
};

/// Chunks to check at once. Reading one is a round trip and a hash with nothing to
/// overlap inside it, so this is all that keeps the store busy.
const VERIFY_CONCURRENCY: usize = 8;

/// Scratch size for hashing a chunk as it streams, so nothing here ever holds a
/// whole chunk and an unexpectedly large blob costs reads rather than allocation.
const SCRATCH: usize = 32 * 1024;

/// How many chunks to check at once.
fn verify_concurrency() -> usize {
    super::concurrency(VERIFY_CONCURRENCY)
}

/// A chunk that could not be read back, or whose contents did not match the
/// hash it is addressed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// Content hash of the offending chunk.
    pub location: storage::BlobId,
    /// Why it failed.
    pub detail: String,
}

/// The outcome of checking every chunk in a snapshot.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    /// How many distinct chunks were checked.
    pub chunks: usize,
    /// How many bytes of chunk data were read back.
    pub bytes: u64,
    /// Chunks that failed, in the order they appear in the snapshot.
    pub failures: Vec<Failure>,
}

impl Report {
    /// Whether every chunk checked out.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.failures.is_empty()
    }

    /// Turn a failed report into an error.
    ///
    /// # Errors
    ///
    /// Returns an error summarising the failures if any were recorded, and
    /// `Ok(())` otherwise. Up to five are named in the message, with a count of
    /// the remainder.
    pub fn into_result(self) -> io::Result<()> {
        if self.is_ok() {
            return Ok(());
        }

        let listed: Vec<String> = self
            .failures
            .iter()
            .take(5)
            .map(|f| f.detail.clone())
            .collect();

        let more = self.failures.len().saturating_sub(listed.len());

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} of {} chunks failed verification: {}{}",
                self.failures.len(),
                self.chunks,
                listed.join("; "),
                if more > 0 {
                    format!("; and {more} more")
                } else {
                    String::new()
                },
            ),
        ))
    }
}

/// Read back every distinct chunk in `snapshot` and check it against the hash it
/// is addressed by.
///
/// Every failure is collected rather than stopping at the first, so a damaged
/// store can be assessed in one pass. An unreadable store is reported as failures
/// of the chunks it holds rather than aborting the run.
///
/// # Errors
///
/// Returns an error only if the snapshot itself cannot be read or decoded.
/// Damaged chunk data is reported through [`Report::failures`] rather than as an
/// error, so a single call assesses the whole store.
#[instrument(level = "info", skip(storage), err)]
pub async fn verify<S: storage::StorageGet + 'static>(
    snapshot_id: storage::BlobId,
    storage: S,
) -> io::Result<Report> {
    let snapshot = super::load_snapshot(&storage, snapshot_id).await?;

    Ok(check(&snapshot, Arc::new(storage)).await)
}

/// What checking one chunk found.
struct Checked {
    /// Where in the snapshot's chunk list this came from, so the report can be
    /// put back in snapshot order however the checks interleaved.
    index: usize,
    /// Bytes of chunk data read.
    bytes: u64,
    /// Set if the chunk could not be read, or did not match its hash.
    failure: Option<Failure>,
}

/// Check every chunk in an already-loaded snapshot.
pub(crate) async fn check<S: StorageGet + 'static>(snapshot: &Snapshot, storage: Arc<S>) -> Report {
    let mut report = Report {
        chunks: snapshot.chunks.len(),
        ..Report::default()
    };

    let concurrency = verify_concurrency().min(snapshot.chunks.len().max(1));

    // Filled by index as the checks come back, so a failure is reported against
    // the chunk it belongs to rather than against whichever check finished
    // first. `None` until then, and still `None` if a check task died outright.
    let mut checked: Vec<Option<Checked>> = (0..snapshot.chunks.len()).map(|_| None).collect();

    let mut running = JoinSet::new();
    let mut next = 0usize;

    // The one long silent wait, and the one place a user is told nothing is being
    // written yet — so silence here is most likely to be read as a hang.
    let mut progress = Progress::new(Phase::Verify);
    progress.set(
        Counter::Chunks,
        u64::try_from(snapshot.chunks.len()).unwrap_or(0),
    );
    progress.report();

    loop {
        while next < snapshot.chunks.len() && running.len() < concurrency {
            let index = next;
            next += 1;

            running.spawn(check_chunk(
                index,
                snapshot.chunks[index],
                Arc::clone(&storage),
            ));
        }

        if running.is_empty() {
            break;
        }

        // A check that died is reported against its own chunk below, so one bad
        // task cannot lose the results of the others.
        if let Some(Ok(done)) = running.join_next().await
            && let Some(slot) = checked.get_mut(done.index)
        {
            // Read off `done` rather than the slot: the slot is still empty at
            // this point, and would report zero bytes for every chunk.
            progress.add(Counter::Checked, 1);
            progress.add(Counter::Read, done.bytes);
            progress.report();
            *slot = Some(done);
        }
    }

    for (index, chunk) in snapshot.chunks.iter().enumerate() {
        match checked[index].take() {
            Some(done) => {
                report.bytes += done.bytes;
                if let Some(failure) = done.failure {
                    report.failures.push(failure);
                }
            }
            None => report.failures.push(Failure {
                location: *chunk,
                detail: format!("chunk {chunk} was never checked"),
            }),
        }
    }

    report
}

/// Read one chunk back and check it against its recorded hash.
///
/// Owns its scratch buffer, so there is one per chunk in flight rather than one
/// per chunk checked.
async fn check_chunk<S: StorageGet>(
    index: usize,
    chunk: storage::BlobId,
    storage: Arc<S>,
) -> Checked {
    let mut scratch = vec![0u8; SCRATCH];

    let mut checked = Checked {
        index,
        bytes: 0,
        failure: None,
    };

    let mut reader = match storage.get(&chunk).await {
        Ok(reader) => reader,
        Err(e) => {
            checked.failure = Some(Failure {
                location: chunk,
                detail: format!("chunk {chunk} could not be read: {e}"),
            });
            return checked;
        }
    };

    // Hashed as it streams, so a chunk is never resident and a store that hands
    // back more than it stored costs reads rather than an allocation.
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;

    loop {
        let read = match reader.read(&mut scratch).await {
            Ok(read) => read,
            Err(e) => {
                checked.bytes = bytes;
                checked.failure = Some(Failure {
                    location: chunk,
                    detail: format!("chunk {chunk} could not be read: {e}"),
                });
                return checked;
            }
        };

        if read == 0 {
            break;
        }

        hasher.update(&scratch[..read]);
        bytes += u64::try_from(read).unwrap_or(u64::MAX);
    }

    checked.bytes = bytes;

    // A chunk is stored under the hash of its contents, so the address *is* the
    // expected value: a mismatch means the bytes under this name are not the bytes
    // that were named.
    let actual = hasher.finalize();
    if actual != chunk {
        checked.failure = Some(Failure {
            location: chunk,
            detail: format!(
                "the bytes stored as chunk {chunk} actually hash to {actual} ({bytes} bytes)"
            ),
        });
    }

    checked
}
