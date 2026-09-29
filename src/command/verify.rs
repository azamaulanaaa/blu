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
    pub reason: Reason,
}

/// Why a chunk failed to check out.
///
/// Was a `String`, formatted at each of the four sites that could produce one.
/// A caller could therefore match on *where* a failure was and nothing else —
/// whether a chunk was missing, truncated mid-read, or intact-but-corrupt were
/// three different diagnoses, and the only way to tell them apart was to
/// substring-match prose, which the tests did.
///
/// The two read failures were the sharpest loss. `storage.get` failing and a
/// read from the returned stream failing both rendered as "could not be read",
/// but they are not the same problem: the first means the store does not hold
/// the chunk, or cannot reach the segment that does; the second means it did
/// hand one over and the read then failed, which points at the device, the
/// connection, or damage within the chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    /// The store would not open the chunk at all.
    ///
    /// Not necessarily absent: the store refuses an unreadable or damaged segment
    /// too, and that refusal is reported against every chunk it should have held.
    Unavailable {
        /// What the store said went wrong.
        kind: io::ErrorKind,
        /// And the rest of it, verbatim.
        detail: String,
    },

    /// The chunk opened, and a read from it failed partway through.
    ///
    /// Bytes are what was read before the failure, which is less than the chunk
    /// recorded — so this is a chunk that exists and does not survive being read.
    Truncated {
        /// How much was read before the failure.
        bytes: u64,
        /// What the read said went wrong.
        kind: io::ErrorKind,
        /// And the rest of it, verbatim.
        detail: String,
    },

    /// The bytes came back whole and hash to something other than their address.
    Corrupt {
        /// What they actually hash to.
        actual: storage::BlobId,
        /// How many bytes they were, as read.
        bytes: u64,
    },

    /// A check that should have run did not, and left nothing behind.
    ///
    /// Not damage: every other variant is a statement about the store, and this
    /// one is a statement about the verifier. It is a separate variant rather
    /// than folded into "could not be read" so that it cannot be mistaken for a
    /// bad backup, and it is why [`verify`] documents what it does with a task
    /// that vanishes.
    Unchecked,
}

impl Reason {
    /// Whether this is the store's fault rather than the verifier's.
    ///
    /// Everything but [`Reason::Unchecked`], and spelled out so a caller that
    /// wants to distinguish "your backup is damaged" from "this program is
    /// broken" does not have to remember which variant is which.
    #[must_use]
    pub fn is_damage(&self) -> bool {
        !matches!(self, Self::Unchecked)
    }
}

impl std::fmt::Display for Failure {
    /// Exactly the text the `detail` string used to hold, so every line a user
    /// reads is unchanged.
    ///
    /// The match lives here rather than on [`Reason`] because the four messages
    /// put the location in two different places: three name the chunk first, and
    /// the corruption message puts it in the middle of a sentence. Splitting the
    /// rendering in two would have meant either fixing a wording a user has
    /// already read, or threading the location through a second formatting
    /// layer. `Reason` has no `Display` of its own, deliberately — it is meant
    /// to be matched on, not printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let chunk = self.location;
        match &self.reason {
            Reason::Unavailable { detail, .. } | Reason::Truncated { detail, .. } => {
                write!(f, "chunk {chunk} could not be read: {detail}")
            }
            Reason::Corrupt { actual, bytes } => write!(
                f,
                "the bytes stored as chunk {chunk} actually hash to {actual} ({bytes} bytes)"
            ),
            Reason::Unchecked => write!(f, "chunk {chunk} was never checked"),
        }
    }
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
            .map(ToString::to_string)
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
                reason: Reason::Unchecked,
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
                reason: Reason::Unavailable {
                    kind: e.kind(),
                    detail: e.to_string(),
                },
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
                    reason: Reason::Truncated {
                        bytes,
                        kind: e.kind(),
                        detail: e.to_string(),
                    },
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
            reason: Reason::Corrupt { actual, bytes },
        });
    }

    checked
}

#[cfg(test)]
mod tests {
    use super::{Failure, Reason};
    use crate::storage::BlobId;

    fn id(byte: u8) -> BlobId {
        BlobId::from_bytes([byte; 32])
    }

    /// Every failure a check can produce, rendered exactly.
    ///
    /// These are the lines a user reads, verbatim, on stderr and in the summary
    /// error. They were assembled by four `format!` calls that no test could
    /// reach, because the only thing to assert on was the string the test itself
    /// was checking. Pinning them here is what makes the rest of this change
    /// safe to make: the reason is a type, and the text is a promise.
    #[test]
    fn a_failure_renders_as_it_did_as_a_string() {
        // Full-width hex, not abbreviated. Worth being explicit about, because
        // this is the one place where the switch from a hand-rolled `Hex` to
        // `blake3::Hash`'s `Display` could have shortened a hash out of a
        // message a user reads. It did not: `Hex` was full width too.
        let one = id(1).to_string();
        let two = id(2).to_string();
        assert_eq!(one.len(), 64, "a hash is rendered in full");

        let cases: &[(Failure, String)] = &[
            (
                Failure {
                    location: id(1),
                    reason: Reason::Unavailable {
                        kind: std::io::ErrorKind::NotFound,
                        detail: "no such blob".into(),
                    },
                },
                format!("chunk {one} could not be read: no such blob"),
            ),
            (
                Failure {
                    location: id(1),
                    reason: Reason::Truncated {
                        bytes: 4096,
                        kind: std::io::ErrorKind::UnexpectedEof,
                        detail: "connection reset".into(),
                    },
                },
                format!("chunk {one} could not be read: connection reset"),
            ),
            (
                Failure {
                    location: id(1),
                    reason: Reason::Corrupt {
                        actual: id(2),
                        bytes: 8192,
                    },
                },
                format!("the bytes stored as chunk {one} actually hash to {two} (8192 bytes)"),
            ),
            (
                Failure {
                    location: id(1),
                    reason: Reason::Unchecked,
                },
                format!("chunk {one} was never checked"),
            ),
        ];

        for (failure, expected) in cases {
            assert_eq!(&failure.to_string(), expected);
        }
    }

    /// The two read failures render alike but are not the same diagnosis, which
    /// is the whole reason they are separate variants.
    ///
    /// They matched byte for byte as strings, so a caller could not tell "the
    /// store would not hand me this chunk" from "it did, and the read then
    /// failed" without matching on prose.
    #[test]
    fn the_two_read_failures_differ_only_in_what_they_carry() {
        let unavailable = Failure {
            location: id(1),
            reason: Reason::Unavailable {
                kind: std::io::ErrorKind::NotFound,
                detail: "gone".into(),
            },
        };
        let truncated = Failure {
            location: id(1),
            reason: Reason::Truncated {
                bytes: 1,
                kind: std::io::ErrorKind::UnexpectedEof,
                detail: "gone".into(),
            },
        };

        assert_eq!(unavailable.to_string(), truncated.to_string());
        assert_ne!(
            unavailable.reason, truncated.reason,
            "they are not the same"
        );
        assert!(unavailable.reason.is_damage() && truncated.reason.is_damage());
    }

    /// A check that never ran is this program's problem, not the backup's.
    #[test]
    fn only_an_unchecked_chunk_is_not_damage() {
        assert!(!Reason::Unchecked.is_damage());
        assert!(
            Reason::Unavailable {
                kind: std::io::ErrorKind::NotFound,
                detail: String::new(),
            }
            .is_damage()
        );
        assert!(
            Reason::Corrupt {
                actual: id(1),
                bytes: 0,
            }
            .is_damage()
        );
        assert!(
            Reason::Truncated {
                bytes: 0,
                kind: std::io::ErrorKind::UnexpectedEof,
                detail: String::new(),
            }
            .is_damage()
        );
    }
}
