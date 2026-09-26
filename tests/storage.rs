//! The blob store: key encoding, length enforcement and bounds checking.

use std::io::Cursor;

use lepatch::storage::{BlobFileStorage, StorageGet, StoragePut};
use tokio::io::AsyncReadExt;

fn reader(bytes: Vec<u8>) -> lepatch::reader::StreamReadSeeker {
    Box::new(Cursor::new(bytes))
}

/// `StreamReadSeeker` is not `Debug`, so `unwrap_err` is not available on
/// `StorageGet::get`.
fn expect_get_error(
    error: Result<lepatch::reader::StreamReadSeeker, std::io::Error>,
) -> std::io::Error {
    match error {
        Ok(_) => panic!("expected get to fail, but it succeeded"),
        Err(e) => e,
    }
}

async fn read_all(store: &BlobFileStorage<false>, key: &str) -> std::io::Result<Vec<u8>> {
    let mut blob = store.get(key).await?;
    let mut buffer = Vec::new();
    blob.read_to_end(&mut buffer).await?;

    Ok(buffer)
}

#[tokio::test]
async fn a_stored_blob_reads_back_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 253) as u8).collect();

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();
    let key = store.put(reader(payload.clone()), 50_000).await.unwrap();

    let store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    assert_eq!(read_all(&store, &key).await.unwrap(), payload);
}

#[tokio::test]
async fn blobs_appended_in_sequence_do_not_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();

    let mut keys = Vec::new();
    for i in 0..8u8 {
        let payload = vec![i; 1_000 + usize::from(i)];
        let len = payload.len() as u64;
        keys.push((store.put(reader(payload), len).await.unwrap(), i));
    }

    let store = BlobFileStorage::<false>::new(&blob).await.unwrap();

    for (key, marker) in keys {
        let data = read_all(&store, &key).await.unwrap();
        assert!(data.iter().all(|b| *b == marker), "blob {key} came back wrong");
    }
}

#[tokio::test]
async fn an_empty_blob_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();
    let key = store.put(reader(Vec::new()), 0).await.unwrap();

    let store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    assert!(read_all(&store, &key).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_reader_shorter_than_the_declared_length_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();

    // Claimed 1000 bytes, only 10 available.
    let error = store.put(reader(vec![1u8; 10]), 1000).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(
        error.to_string().contains("declared 1000 bytes"),
        "the error should name both lengths, got: {error}"
    );
}

#[tokio::test]
async fn a_reader_longer_than_the_declared_length_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();

    // Claimed 10 bytes, 1000 available. Accepting this would leave the extra
    // 990 bytes in the store, misaligning every later blob's recorded offset.
    let error = store.put(reader(vec![1u8; 1000]), 10).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("more than the declared 10 bytes"),
        "the error should name the declared length, got: {error}"
    );
}

#[tokio::test]
async fn a_key_pointing_past_the_end_of_the_store_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();
    let key = store.put(reader(vec![7u8; 100]), 100).await.unwrap();

    // Chop the store in half.
    let bytes = std::fs::read(&blob).unwrap();
    std::fs::write(&blob, &bytes[..50]).unwrap();

    let store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    let error = expect_get_error(store.get(&key).await);

    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(
        error.to_string().contains("the store is only 50 bytes long"),
        "the error should quantify the shortfall, got: {error}"
    );
}

#[tokio::test]
async fn a_key_that_is_not_json_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobFileStorage::<false>::new(dir.path().join("store.bin"))
        .await
        .unwrap();

    let error = expect_get_error(store.get("not-a-key").await);

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("invalid blob key"),
        "got: {error}"
    );
}

#[tokio::test]
async fn a_key_from_an_incompatible_build_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");

    // A key claiming a future layout version.
    let key = r#"{"version":99,"offset":0,"length":0}"#;

    let store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    let error = expect_get_error(store.get(key).await);

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("version 99"),
        "the error should name the version it found, got: {error}"
    );
}

#[tokio::test]
async fn a_store_in_a_missing_directory_is_created() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("nested/deeper/store.bin");

    let store = BlobFileStorage::<true>::new(&blob).await.unwrap();
    let key = store.put(reader(vec![3u8; 10]), 10).await.unwrap();

    assert!(blob.is_file());

    let store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    assert_eq!(read_all(&store, &key).await.unwrap(), vec![3u8; 10]);
}
