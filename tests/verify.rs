//! Integrity verification of stored chunks.

mod common;

use std::fs;

use lepatch::{
    command::{Report, restore, verify},
    storage::{BlobFileStorage, StorageGet},
};

async fn snapshot(blob: &std::path::Path, key: &str) -> lepatch::metadata::Snapshot {
    use tokio::io::AsyncReadExt;

    let storage = BlobFileStorage::<false>::new(blob).await.unwrap();
    let mut reader = storage.get(key).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    lepatch::metadata::Snapshot::from_bytes(&buffer).unwrap()
}

/// Non-uniform content, so the chunker finds real boundaries and the chunks
/// differ from one another. A run of identical bytes would be deduplicated
/// down to a single chunk.
fn varied(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

#[tokio::test]
async fn an_intact_snapshot_verifies_clean() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let key = common::run_backup(&source, &blob, None).await;

    let storage = BlobFileStorage::<false>::new(&blob).await.unwrap();
    let report = verify(key, storage).await.unwrap();

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
    let key = common::run_backup(&source, &blob, None).await;

    // Corrupt a byte in the middle of the chunk data.
    let offset = fs::metadata(&blob).unwrap().len() / 2;
    let mut bytes = fs::read(&blob).unwrap();
    bytes[offset as usize] ^= 0xff;
    fs::write(&blob, &bytes).unwrap();

    let storage = BlobFileStorage::<false>::new(&blob).await.unwrap();
    let report = verify(key, storage).await.unwrap();

    assert!(!report.is_ok(), "corruption went undetected");
    assert!(
        report.failures[0].detail.contains("corrupt"),
        "failure should say the chunk is corrupt: {}",
        report.failures[0].detail
    );
    assert!(report.failures[0].detail.contains("expected hash"));
}

#[tokio::test]
async fn a_store_truncated_before_its_snapshot_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let key = common::run_backup(&source, &blob, None).await;

    // The snapshot blob is written last, so lopping off the tail of the store
    // destroys it. That has to surface as an error, not as a clean report.
    let bytes = fs::read(&blob).unwrap();
    fs::write(&blob, &bytes[..bytes.len() - 50_000]).unwrap();

    let storage = BlobFileStorage::<false>::new(&blob).await.unwrap();
    let error = verify(key, storage).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("truncated"),
        "the error should name the cause, got: {error}"
    );
}

#[tokio::test]
async fn restore_refuses_to_run_against_a_corrupt_store() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), varied(200_000)).unwrap();

    let blob = dir.path().join("store.bin");
    let key = common::run_backup(&source, &blob, None).await;

    let offset = fs::metadata(&blob).unwrap().len() / 2;
    let mut bytes = fs::read(&blob).unwrap();
    bytes[offset as usize] ^= 0xff;
    fs::write(&blob, &bytes).unwrap();

    let destination = dir.path().join("restored");
    let storage = BlobFileStorage::<false>::new(&blob).await.unwrap();
    let error = restore(&destination, key, storage).await.unwrap_err();

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
                location: format!("loc{i}"),
                detail: format!("chunk loc{i} is corrupt"),
            })
            .collect(),
    };

    let error = report.into_result().unwrap_err();

    assert!(error.to_string().contains("7 of 10 chunks"));
    assert!(error.to_string().contains("and 2 more"));
}
