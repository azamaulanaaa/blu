//! Integrity checking of a snapshot's stored chunks.

use std::io;

use tokio::io::AsyncReadExt;
use tracing::instrument;

use crate::{
    metadata::Snapshot,
    storage::{self, StorageGet, StreamReadSeeker},
};

/// A chunk that could not be read back, or whose contents did not match the
/// hash recorded when it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// Storage key of the offending chunk.
    pub location: String,
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

/// Read back every distinct chunk in `snapshot` and check it against the hash
/// recorded at backup time.
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
pub async fn verify<S: storage::StorageGet>(key: String, storage: S) -> io::Result<Report> {
    let snapshot = {
        let mut reader: StreamReadSeeker = storage.get(&key).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        Snapshot::from_bytes(buffer.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    };

    Ok(check(&snapshot, &storage).await)
}

/// Check every chunk in an already-loaded snapshot.
pub(crate) async fn check<S: StorageGet>(snapshot: &Snapshot, storage: &S) -> Report {
    let mut report = Report {
        chunks: snapshot.chunks.len(),
        ..Report::default()
    };

    for chunk in &snapshot.chunks {
        let mut reader = match storage.get(&chunk.location).await {
            Ok(reader) => reader,
            Err(e) => {
                report.failures.push(Failure {
                    location: chunk.location.clone(),
                    detail: format!("chunk at {} could not be read: {e}", chunk.location),
                });
                continue;
            }
        };

        let mut buffer = Vec::new();
        if let Err(e) = reader.read_to_end(&mut buffer).await {
            report.failures.push(Failure {
                location: chunk.location.clone(),
                detail: format!("chunk at {} could not be read: {e}", chunk.location),
            });
            continue;
        }

        report.bytes += buffer.len() as u64;

        let actual = blake3::hash(&buffer);
        if actual.as_bytes() != &chunk.hash {
            report.failures.push(Failure {
                location: chunk.location.clone(),
                detail: format!(
                    "chunk at {} is corrupt: expected hash {}, found {} ({} bytes)",
                    chunk.location,
                    hex(&chunk.hash),
                    hex(actual.as_bytes()),
                    buffer.len()
                ),
            });
        }
    }

    report
}

/// Lowercase hex, for reporting hashes in a form a human can compare.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}
