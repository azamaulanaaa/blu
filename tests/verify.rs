//! Integrity verification of stored chunks.

mod common;

use std::fs;

use common::noisy;
use lepatch::{
    command::{Report, restore, verify},
    storage::BlobStore,
};

/// Non-uniform content, so the chunker finds real boundaries and the chunks
/// differ from one another. A run of identical bytes would be deduplicated
/// down to a single chunk.
fn varied(len: usize) -> Vec<u8> {
    noisy(len)
}

#[tokio::test]
async fn an_intact_snapshot_verifies_clean() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    let storage = BlobStore::<false>::open(&blob).await.unwrap();
    let report = verify(committed.snapshot, storage).await.unwrap();

    assert!(report.is_ok(), "unexpected failures: {:?}", report.failures);
    assert!(report.chunks > 1, "expected several distinct chunks");
    assert_eq!(report.bytes, 200_000);
}

#[tokio::test]
async fn a_flipped_byte_in_the_store_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    // Corrupt a byte in the middle of the chunk data.
    let offset = fs::metadata(&blob).unwrap().len() / 2;
    let mut bytes = fs::read(&blob).unwrap();
    bytes[offset as usize] ^= 0xff;
    fs::write(&blob, &bytes).unwrap();

    let storage = BlobStore::<false>::open(&blob).await.unwrap();
    let report = verify(committed.snapshot, storage).await.unwrap();

    assert!(!report.is_ok(), "corruption went undetected");
    assert!(
        report.failures[0].detail.contains("actually hash to"),
        "the failure should say what the bytes really hashed to: {}",
        report.failures[0].detail
    );
}

/// A store cut short leaves a version that cannot be read back.
///
/// The snapshot blob is written last, so lopping off the tail of the store
/// destroys it. The commit record naming it is gone with it, so the version
/// simply is not there — which is the correct report, and quite different from
/// the old behaviour of handing back a version whose bytes were missing.
#[tokio::test]
async fn a_store_truncated_before_its_snapshot_has_no_such_version() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    common::run_backup(&source, &blob).await;

    let bytes = fs::read(&blob).unwrap();
    fs::write(&blob, &bytes[..bytes.len() - 50_000]).unwrap();

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert!(
        store.latest().is_none(),
        "a store with no commit record holds no version, even though its snapshot \\
         record may still be partly there"
    );
}

#[tokio::test]
async fn restore_refuses_to_run_against_a_corrupt_store() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    let offset = fs::metadata(&blob).unwrap().len() / 2;
    let mut bytes = fs::read(&blob).unwrap();
    bytes[offset as usize] ^= 0xff;
    fs::write(&blob, &bytes).unwrap();

    let destination = dir.path().join("restored");
    let storage = BlobStore::<false>::open(&blob).await.unwrap();
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
                location: [u8::try_from(i).unwrap_or(0); 32],
                detail: format!("chunk {i} is corrupt"),
            })
            .collect(),
    };

    let error = report.into_result().unwrap_err();

    assert!(error.to_string().contains("7 of 10 chunks"));
    assert!(error.to_string().contains("and 2 more"));
}
