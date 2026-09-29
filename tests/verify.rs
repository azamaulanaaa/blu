//! Integrity verification of stored chunks.

mod common;

use std::fs;

use common::noisy;
use lepatch::{
    command::{Reason, Report, restore, verify},
    layout::IndexSet,
    storage::{BlobId, BlobStore},
};

#[tokio::test]
async fn an_intact_snapshot_verifies_clean() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    let storage = BlobStore::open_for_reading(&blob).await.unwrap();
    let report = verify(committed.snapshot, storage).await.unwrap();

    assert!(report.is_ok(), "unexpected failures: {:?}", report.failures);
    assert!(report.chunks > 1, "expected several distinct chunks");
    assert_eq!(report.bytes, 200_000);
}

#[tokio::test]
async fn a_flipped_byte_in_the_store_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;

    // Corrupt a byte in the middle of the chunk data, in the segment it is in.
    let segment = IndexSet::new(&blob).segment_path(0);
    let offset = fs::metadata(&segment).unwrap().len() / 2;
    let mut bytes = fs::read(&segment).unwrap();
    bytes[offset as usize] ^= 0xff;
    fs::write(&segment, &bytes).unwrap();

    let storage = BlobStore::open_for_reading(&blob).await.unwrap();
    let report = verify(committed.snapshot, storage).await.unwrap();

    assert!(!report.is_ok(), "corruption went undetected");

    // Matched, not substring-matched. This used to assert the message contains
    // "actually hash to", which is a claim about prose: reword the sentence and
    // the test fails, reword the *diagnosis* and it passes. The variant is the
    // diagnosis.
    assert!(
        matches!(report.failures[0].reason, Reason::Corrupt { .. }),
        "a flipped byte is corruption, not a failure to read: {:?}",
        report.failures[0].reason
    );

    // And the variant still carries the two facts a reader needs, which the
    // string used to carry: what the bytes really hashed to, and how many there
    // were.
    let Reason::Corrupt { actual, bytes } = report.failures[0].reason else {
        unreachable!("just asserted the variant");
    };
    assert_ne!(
        actual, report.failures[0].location,
        "it hashed to its own address"
    );
    assert!(
        bytes > 0,
        "some bytes were read, or the hash would be of nothing"
    );
}

/// A store cut short is reported as damaged, not read as a shorter store.
///
/// The single-file design had a version here: lopping off the tail destroyed the
/// commit record, so the set held no version and a restore said so. Segments make
/// that quieter rather than clearer -- the scan stops at the first record that does
/// not verify and the bytes after it are simply not there -- so the refusal moved
/// earlier, to the open. A set that cannot be read whole is refused whole.
#[tokio::test]
async fn a_store_truncated_before_its_snapshot_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();

    let blob = dir.path().join("store");
    common::run_backup(&source, &blob).await;

    let segment = IndexSet::new(&blob).segment_path(0);
    let bytes = fs::read(&segment).unwrap();
    fs::write(&segment, &bytes[..bytes.len() - 50_000]).unwrap();

    let error = BlobStore::open_for_reading(&blob).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("damaged"),
        "the error should say the set is damaged, got: {error}"
    );
}

#[tokio::test]
async fn restore_refuses_to_run_against_a_corrupt_store() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;

    let segment = IndexSet::new(&blob).segment_path(0);
    let offset = fs::metadata(&segment).unwrap().len() / 2;
    let mut bytes = fs::read(&segment).unwrap();
    bytes[offset as usize] ^= 0xff;
    fs::write(&segment, &bytes).unwrap();

    let destination = dir.path().join("restored");
    let storage = BlobStore::open_for_reading(&blob).await.unwrap();
    let error = restore(&destination, committed.snapshot, storage)
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("refusing to restore"),
        "the error should say the restore was refused, got: {error}"
    );

    // Nothing should have been written.
    assert!(
        !destination.join("a.bin").exists(),
        "restore wrote a file despite failing verification"
    );
}

#[test]
fn a_report_with_no_failures_converts_to_ok() {
    let report = Report {
        chunks: 3,
        bytes: 100,
        failures: Vec::new(),
    };

    assert!(report.is_ok());
    assert!(report.into_result().is_ok());
}

#[test]
fn a_report_with_failures_summarises_them() {
    let report = Report {
        chunks: 10,
        bytes: 100,
        failures: (0..7)
            .map(|i| lepatch::command::Failure {
                location: BlobId::from_bytes([u8::try_from(i).unwrap_or(0); 32]),
                reason: Reason::Corrupt {
                    actual: BlobId::from_bytes([u8::try_from(i + 1).unwrap_or(0); 32]),
                    bytes: u64::from(u8::try_from(i).unwrap_or(0)) * 10,
                },
            })
            .collect(),
    };

    let error = report.into_result().unwrap_err();

    assert!(error.to_string().contains("7 of 10 chunks"));
    assert!(error.to_string().contains("and 2 more"));
}
