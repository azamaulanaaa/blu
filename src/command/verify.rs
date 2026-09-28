//! Integrity checking of a snapshot's stored chunks.
//!
//! A restore runs this over the whole snapshot before it writes anything, so it
//! is on the critical path of every restore and worth being fast: chunks are
//! checked several at a time, and each is hashed as it streams past rather than
//! being read into a buffer first.
//!
//! The pass is deliberately separate from the writing. Folding the hash into the
//! copy that fills each file would halve the bytes read, but it would also mean
//! writing the first file before finding out that the chunk for the last one is
//! corrupt — turning "refuses to restore a damaged store" into "leaves a
//! half-restored tree". Two passes is the price of that guarantee, and it is
//! worth paying.

use std::{io, sync::Arc};

use tokio::{io::AsyncReadExt, task::JoinSet};
use tracing::instrument;

use crate::{
    metadata::Snapshot,
    progress::Progress,
    storage::{self, StorageGet},
};

/// Chunks to check at once.
///
/// Reading a chunk is a round trip to the store and a hash, with nothing to
/// overlap inside it, so several at a time is the only way to keep the store
/// busy.
const VERIFY_CONCURRENCY: usize = 8;

/// Scratch size for hashing a chunk as it streams.
///
/// Not the whole chunk: chunks are capped at the chunker's maximum size, but
/// nothing here needs to hold one, and streaming means an unexpectedly large
/// blob costs a couple more reads rather than an unexpectedly large allocation.
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
/// store can be assessed in one pass. An unreadable store is reported as a
/// failure of the chunks it holds rather than aborting the run.
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

    // Checking a large store is a long silent wait otherwise, and it is the one
    // place a user is told "nothing is being written yet" — so it is exactly
    // where silence is most likely to be read as a hang.
    let mut progress = Progress::new("verify");
    progress.set("chunks", u64::try_from(snapshot.chunks.len()).unwrap_or(0));
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
            progress.add("checked", 1);
            progress.add("read", done.bytes);
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
                detail: format!("chunk {} was never checked", storage::hex(chunk)),
            }),
        }
    }

    report
}

/// Read one chunk back and check it against its recorded hash.
///
/// Owns its scratch buffer rather than sharing one, so there is one buffer per
/// chunk in flight rather than one per chunk checked.
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
                detail: format!("chunk {} could not be read: {e}", storage::hex(&chunk)),
            });
            return checked;
        }
    };

    // Hashed as it streams rather than read into a buffer first: the whole
    // chunk never needs to be resident, and a store that hands back more than
    // was written costs extra reads instead of an allocation the size of the
    // lie.
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;

    loop {
        let read = match reader.read(&mut scratch).await {
            Ok(read) => read,
            Err(e) => {
                checked.bytes = bytes;
                checked.failure = Some(Failure {
                    location: chunk,
                    detail: format!("chunk {} could not be read: {e}", storage::hex(&chunk)),
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

    // A chunk is addressed by the hash of its contents and is stored under that
    // hash, so the address *is* the expected value: a mismatch means the bytes
    // stored under this name are not the bytes that were named. The name is
    // therefore printed once, as the thing to look up, rather than twice as
    // "expected" and something else.
    let actual = hasher.finalize();
    if actual.as_bytes() != &chunk {
        checked.failure = Some(Failure {
            location: chunk,
            detail: format!(
                "the bytes stored as chunk {} actually hash to {} ({bytes} bytes)",
                storage::hex(&chunk),
                storage::hex(actual.as_bytes()),
            ),
        });
    }

    checked
}
