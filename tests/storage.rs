//! The blob store: key encoding, length enforcement and bounds checking.

use std::io::Cursor;

use lepatch::storage::{BlobFileStorage, StorageGet, StoragePut};
use tokio::io::AsyncReadExt;

fn reader(bytes: Vec<u8>) -> lepatch::storage::StreamReadSeeker {
    Box::new(Cursor::new(bytes))
}

/// `StreamReadSeeker` is not `Debug`, so `unwrap_err` is not available on
/// `StorageGet::get`.
fn expect_get_error(
    error: Result<lepatch::storage::StreamReadSeeker, std::io::Error>,
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
        assert!(
            data.iter().all(|b| *b == marker),
            "blob {key} came back wrong"
        );
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
        error
            .to_string()
            .contains("more than the declared 10 bytes"),
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
        error
            .to_string()
            .contains("the store is only 50 bytes long"),
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

/// Concurrent uploads must not overlap.
///
/// The store claims a range for each blob and then copies into it without
/// holding anything, which is what lets uploads run in parallel. That is only
/// sound if every claim is distinct, and the way it goes wrong is quiet: two
/// writers agree on an offset, one clobbers the other, and both keys look
/// perfectly valid.
///
/// Every payload is filled with its own index, so an overlap shows up as a blob
/// that reads back with somebody else's number in it. Payloads are large and
/// uneven, so a writer cannot finish before its neighbours have started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_uploads_do_not_overlap() {
    const BLOBS: usize = 64;

    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");
    let store = std::sync::Arc::new(BlobFileStorage::<true>::new(&blob).await.unwrap());

    // Uneven, and big enough that the copies genuinely overlap in time.
    let payloads: Vec<Vec<u8>> = (0..BLOBS)
        .map(|i| {
            let len = 40_000 + (i * 977) % 90_000;
            vec![u8::try_from(i % 251).expect("in range") + 1; len]
        })
        .collect();

    let mut tasks = Vec::new();
    for (i, payload) in payloads.iter().cloned().enumerate() {
        let store = std::sync::Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            let key = store
                .put(reader(payload.clone()), payload.len() as u64)
                .await?;
            Ok::<_, std::io::Error>((i, key))
        }));
    }

    let mut keys = Vec::new();
    for task in tasks {
        keys.push(task.await.unwrap().unwrap());
    }

    // Reopened, not the same handle: this is what a restore or a verify does.
    let reader_store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    for (i, key) in &keys {
        let read = read_all(&reader_store, key).await.unwrap();
        assert_eq!(
            read.len(),
            payloads[*i].len(),
            "blob {i} came back the wrong length, so a range was claimed twice"
        );
        assert_eq!(
            read, payloads[*i],
            "blob {i} came back holding another blob's bytes"
        );
    }

    // Every byte written is accounted for: no gaps, so the claims were disjoint
    // *and* exhaustive.
    let total: usize = payloads.iter().map(Vec::len).sum();
    assert_eq!(
        std::fs::metadata(&blob).unwrap().len(),
        total as u64,
        "the store is not exactly the sum of its blobs"
    );
}

/// A failed upload must not disturb the ones that follow it.
///
/// A failed copy keeps the range it claimed: the bytes are never referenced by
/// any key, and giving them back would mean truncating the file, which a writer
/// that has already claimed the space past them has promised not to happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_upload_leaves_the_rest_of_the_store_readable() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store.bin");
    let store = std::sync::Arc::new(BlobFileStorage::<true>::new(&blob).await.unwrap());

    // 64 good blobs, and in the middle of them one that lies about its length.
    let mut tasks = Vec::new();
    for i in 0..64usize {
        let store = std::sync::Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            let payload = vec![u8::try_from(i % 251).expect("in range") + 1; 30_000 + i];
            let declared = if i == 32 {
                30_000
            } else {
                payload.len() as u64
            };
            let key = store.put(reader(payload), declared).await?;
            Ok::<_, std::io::Error>((i, key))
        }));
    }

    let mut keys = Vec::new();
    let mut failures = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok((i, key)) => keys.push((i, key)),
            Err(_) => failures += 1,
        }
    }

    assert_eq!(
        failures, 1,
        "the short reader should have been the only failure"
    );
    assert_eq!(keys.len(), 63);

    let reader_store = BlobFileStorage::<false>::new(&blob).await.unwrap();
    for (i, key) in &keys {
        let read = read_all(&reader_store, key).await.unwrap();
        assert_eq!(
            read,
            vec![u8::try_from(*i % 251).expect("in range") + 1; 30_000 + i],
            "blob {i} is wrong"
        );
    }
}
