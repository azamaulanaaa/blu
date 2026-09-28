//! The blob store: content addressing, length enforcement, deduplication, and
//! refusing to read anything the store does not hold.

// `.clippy.toml` exempts code inside a `#[test]` function; the helpers below are
// plain functions that tests call, so they need the exemption spelled out.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use lepatch::{
    index::IndexSet,
    storage::{BlobId, BlobStore, Compression, SegmentLimit, StorageGet, StoragePut},
};
use tokio::io::AsyncReadExt;

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

async fn read_all(store: &BlobStore<false>, id: &BlobId) -> std::io::Result<Vec<u8>> {
    let mut blob = store.get(id).await?;
    let mut buffer = Vec::new();
    blob.read_to_end(&mut buffer).await?;

    Ok(buffer)
}

#[tokio::test]
async fn a_stored_blob_reads_back_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 253) as u8).collect();

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(
            &common::id_of(&payload),
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert_eq!(
        read_all(&store, &common::id_of(&payload)).await.unwrap(),
        payload
    );
}

#[tokio::test]
async fn blobs_appended_in_sequence_do_not_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();

    let mut ids = Vec::new();
    for i in 0..8u8 {
        let payload = vec![i; 1_000 + usize::from(i)];
        store
            .put(
                &common::id_of(&payload),
                common::reader(payload.clone()),
                common::len_of(&payload),
            )
            .await
            .unwrap();
        ids.push(common::id_of(&payload));
    }

    let store = BlobStore::<false>::open(&blob).await.unwrap();

    for id in &ids {
        let data = read_all(&store, id).await.unwrap();
        assert!(
            data.iter().all(|b| *b == data[0]),
            "a blob came back holding a mixture of markers, so a range was reused"
        );
        assert_eq!(data.len(), 1_000 + usize::from(data[0]));
    }
}

#[tokio::test]
async fn an_empty_blob_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(&common::id_of(&[]), common::reader(Vec::new()), 0)
        .await
        .unwrap();

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert!(
        read_all(&store, &common::id_of(&[]))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_reader_shorter_than_the_declared_length_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();

    // Claimed 1000 bytes, only 10 available.
    let error = store
        .put(&common::id_of(&[]), common::reader(vec![1u8; 10]), 1000)
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(
        error.to_string().contains("declared 1000 bytes"),
        "the error should name both lengths, got: {error}"
    );
}

#[tokio::test]
async fn a_reader_longer_than_the_declared_length_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();

    // Claimed 10 bytes, 1000 available. Accepting this would leave the extra
    // 990 bytes in the store, past the end of the record that claims them.
    let error = store
        .put(
            &common::id_of(&[1u8; 10]),
            common::reader(vec![1u8; 1000]),
            10,
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("more than the declared 10 bytes"),
        "the error should name the declared length, got: {error}"
    );
}

/// Content that does not hash to the address it is offered under must be
/// refused.
///
/// Nothing else in the design would catch this: the bytes would be written,
/// indexed under a hash they do not have, and read back perfectly. The only
/// symptom would be a `verify` failure long after the run that caused it.
#[tokio::test]
async fn content_that_does_not_hash_to_its_address_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();

    let payload = vec![1u8; 100];
    // The address of something else entirely.
    let wrong = common::id_of(b"a different payload");

    let error = store
        .put(
            &wrong,
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("does not hash to"),
        "the error should say the hash did not match, got: {error}"
    );

    // And nothing was stored at all. `put` checks the content against the address
    // it was given *before* it reserves a place in the file or writes a byte, so
    // there is no record to abandon and no hole to punch: the set is exactly as
    // it was before the call.
    //
    // An earlier design wrote the record first and took its magic away on a
    // mismatch, which left the store reporting itself as damaged until something
    // repaired it. That worked, but it needed a window in which a record whose
    // bytes were not the ones its name promised existed on disk at all.
    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert_eq!(
        store.segment_count(),
        1,
        "the set should be intact and hold only its empty header"
    );
    assert!(
        store.locate(&wrong).is_err(),
        "content that does not hash to its address must not be indexed under it"
    );
}

/// A blob the store does not hold is reported as missing, not handed back as a
/// short read.
#[tokio::test]
async fn a_blob_the_store_does_not_hold_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    // A store that exists and is empty, so this is about the blob rather than
    // about the file being missing.
    let _ = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    let store = BlobStore::<false>::open(&blob).await.unwrap();

    let absent = common::id_of(b"never stored");
    let error = expect_get_error(store.get(&absent).await);

    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert!(
        error
            .to_string()
            .contains("does not hold a blob hashing to"),
        "the error should name what was asked for, got: {error}"
    );
}

/// A payload the store does not hold in full is not listed at all.
///
/// The recovery scan checks that each record's payload is entirely present before
/// it believes the header, so a chopped store reports the blob as missing rather
/// than handing back a reader that stops short — which, in a restore, would be a
/// quietly wrong file.
#[tokio::test]
async fn a_blob_cut_short_by_a_truncated_store_is_reported_missing() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payload = vec![7u8; 100];
    let id = common::id_of(&payload);
    let len = common::len_of(&payload);

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store.put(&id, common::reader(payload), len).await.unwrap();

    // Chop the last segment in half, which lands inside the one record it holds.
    let segment = IndexSet::new(&blob).blob_path(0);
    let bytes = std::fs::read(&segment).unwrap();
    std::fs::write(&segment, &bytes[..40]).unwrap();

    // A read refuses rather than repairing, so this surfaces as damage on the way
    // in rather than as a missing blob. Either is honest; what would not be is a
    // short read handed back as though it were the blob.
    let error = BlobStore::<false>::open(&blob).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("damaged"),
        "the error should say the segment is damaged, got: {error}"
    );
}

/// A file that is not a store is refused, and — the part that matters — is not
/// truncated on the way to being refused.
#[tokio::test]
async fn a_file_that_is_not_a_store_is_refused_without_being_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-a-store");

    // Somebody's data in the place a segment would go: long enough to look
    // plausible, and named so the set finds it and tries to read it.
    let segment = IndexSet::new(&path).blob_path(0);
    let contents = vec![0xabu8; 4_096];
    std::fs::write(&segment, &contents).unwrap();

    let error = BlobStore::<false>::open(&path).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not a backup store this build can read"),
        "the error should say what it found, got: {error}"
    );

    // Opening for writing refuses it too, and must leave it exactly as it was.
    let error = BlobStore::<true>::open(&path, SegmentLimit::default(), Compression::default())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not a backup store this build can read"),
        "the error should say what it found, got: {error}"
    );

    assert_eq!(
        std::fs::read(&segment).unwrap(),
        contents,
        "a file this build does not understand was modified"
    );
}

#[tokio::test]
async fn a_store_written_by_another_build_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("future");

    // A store header naming a layout this build does not know.
    let segment = IndexSet::new(&path).blob_path(0);
    let mut header = Vec::from(*b"LPST");
    header.extend_from_slice(&99u32.to_le_bytes());
    header.extend_from_slice(&[0u8; 64]);
    std::fs::write(&segment, &header).unwrap();

    let error = BlobStore::<false>::open(&path).await.unwrap_err();
    assert!(
        error.to_string().contains("not a backup store"),
        "the error should refuse a store it does not understand, got: {error}"
    );

    // Still untouched.
    assert_eq!(std::fs::read(&segment).unwrap(), header);
}

#[tokio::test]
async fn a_store_in_a_missing_directory_is_created() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("nested/deeper/store");

    let payload = vec![3u8; 10];
    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(
            &common::id_of(&payload),
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();

    assert!(
        IndexSet::new(&blob).blob_path(0).is_file(),
        "a set in a missing directory should create it and its first segment"
    );

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert_eq!(
        read_all(&store, &common::id_of(&payload)).await.unwrap(),
        payload
    );
}

/// Content already in the store is not written again.
///
/// This is what makes deduplication a property of the store rather than of the
/// caller: a run that has never seen the previous version can still hand over
/// every chunk it read and pay for only the ones that are new.
#[tokio::test]
async fn content_the_store_already_holds_is_not_written_again() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payload = common::noisy(64 * 1024);
    let id = common::id_of(&payload);

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(
            &id,
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();
    store.sync_data().await.unwrap();
    store.commit(&id).await.unwrap();

    let after_first = common::set_size(&blob);

    // The same content again, in a second run against the same store.
    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(
            &id,
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();
    store.sync_data().await.unwrap();

    // Nothing but the commit record was added, so the growth is the cost of
    // publishing a version and nothing else.
    let after_second = common::set_size(&blob);
    assert!(
        after_second - after_first < 128,
        "re-storing content the store already held grew it by {} bytes",
        after_second - after_first
    );

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert_eq!(read_all(&store, &id).await.unwrap(), payload);
}

/// Content the store does not hold, offered again in a later run, *is* written.
#[tokio::test]
async fn content_the_store_does_not_hold_is_written_in_a_later_run() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let first = common::noisy(48 * 1024);
    let second = common::noisy(49_999);
    assert_ne!(first, second, "the fixtures must differ");

    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(
            &common::id_of(&first),
            common::reader(first.clone()),
            common::len_of(&first),
        )
        .await
        .unwrap();
    store.sync_data().await.unwrap();
    store.commit(&common::id_of(&first)).await.unwrap();

    // Reopen, as a second run does, so the index is rebuilt from the file.
    let store = BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(
            &common::id_of(&second),
            common::reader(second.clone()),
            common::len_of(&second),
        )
        .await
        .unwrap();
    store.sync_data().await.unwrap();

    let reader_store = BlobStore::<false>::open(&blob).await.unwrap();
    assert_eq!(
        read_all(&reader_store, &common::id_of(&first))
            .await
            .unwrap(),
        first,
        "a blob from an earlier run should still be readable"
    );
    assert_eq!(
        read_all(&reader_store, &common::id_of(&second))
            .await
            .unwrap(),
        second
    );
}

/// Concurrent uploads must not overlap.
///
/// The store claims a range for each blob and then copies into it without
/// holding anything, which is what lets uploads run in parallel. That is only
/// sound if every claim is distinct, and the way it goes wrong is quiet: two
/// writers agree on an offset, one clobbers the other, and both records look
/// perfectly valid.
///
/// Every payload is filled with its own index, so an overlap shows up as a blob
/// that reads back with somebody else's number in it. Payloads are large and
/// uneven, so a writer cannot finish before its neighbours have started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_uploads_do_not_overlap() {
    const BLOBS: usize = 64;

    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = std::sync::Arc::new(
        BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
            .await
            .unwrap(),
    );

    // Uneven, and big enough that the copies genuinely overlap in time.
    //
    // Filled from a hash rather than a repeated byte, for two reasons. An overlap
    // would otherwise be masked: a run of one byte compresses to almost nothing,
    // so two writers whose ranges overlapped could still produce a store that
    // *looked* the right size. And the size assertion below only means "the
    // claimed ranges are disjoint" while the stored bytes are the bytes handed in,
    // which is a property of the payloads rather than of the store.
    let payloads: Vec<Vec<u8>> = (0..BLOBS)
        .map(|i| {
            let len = 40_000 + (i * 977) % 90_000;
            let mut out = Vec::with_capacity(len);
            let mut key = [0u8; 32];
            key[0] = u8::try_from(i).expect("in range");
            for counter in 0..u64::try_from(len.div_ceil(64)).expect("small") {
                let mut input = [0u8; 40];
                input[..32].copy_from_slice(&key);
                input[32..].copy_from_slice(&counter.to_le_bytes());
                out.extend_from_slice(blake3::hash(&input).as_bytes());
            }
            out.truncate(len);
            out
        })
        .collect();

    let mut tasks = Vec::new();
    for (i, payload) in payloads.iter().cloned().enumerate() {
        let store = std::sync::Arc::clone(&store);
        let id = common::id_of(&payload);
        let len = common::len_of(&payload);
        tasks.push(tokio::spawn(async move {
            store.put(&id, common::reader(payload), len).await?;
            Ok::<_, std::io::Error>((i, id))
        }));
    }

    let mut stored = Vec::new();
    for task in tasks {
        stored.push(task.await.unwrap().unwrap());
    }

    // Reopened, not the same handle: this is what a restore or a verify does.
    let reader_store = BlobStore::<false>::open(&blob).await.unwrap();
    for (i, id) in &stored {
        let read = read_all(&reader_store, id).await.unwrap();
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

    // The store is at least as large as the payloads it holds — the record
    // headers are the difference, and finding no gaps below is what makes the
    // claims disjoint rather than merely non-overlapping.
    let total: usize = payloads.iter().map(Vec::len).sum();
    assert!(
        common::set_size(&blob) >= total as u64,
        "the store is smaller than the sum of its blobs"
    );

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    assert_eq!(store.versions().unwrap().len(), 0, "nothing was committed");
}

/// A failed upload must not disturb the ones that follow it.
///
/// A failed record keeps the range it claimed: the bytes are never referenced by
/// any commit, and giving them back would mean truncating past a writer that has
/// already claimed the space after them. The next run's recovery reclaims the lot
/// of them, which is the other half of the guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_upload_leaves_the_rest_of_the_store_readable() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = std::sync::Arc::new(
        BlobStore::<true>::open(&blob, SegmentLimit::default(), Compression::default())
            .await
            .unwrap(),
    );

    // 64 blobs, and in the middle of them one that lies about its length.
    let mut tasks = Vec::new();
    for i in 0..64usize {
        let store = std::sync::Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            let payload = vec![u8::try_from(i % 251).expect("in range") + 1; 30_000 + i];
            let id = common::id_of(&payload);
            let declared = if i == 32 {
                30_000
            } else {
                common::len_of(&payload)
            };
            store.put(&id, common::reader(payload), declared).await?;
            Ok::<_, std::io::Error>((i, id))
        }));
    }

    let mut stored = Vec::new();
    let mut failures = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(entry) => stored.push(entry),
            Err(_) => failures += 1,
        }
    }

    assert_eq!(
        failures, 1,
        "the short reader should have been the only failure"
    );
    assert_eq!(stored.len(), 63);

    // The blobs that were written before the failure are still readable through
    // an index rebuilt from the file, so a half-finished record does not take
    // its neighbours down with it.
    let reader_store = BlobStore::<false>::open(&blob).await.unwrap();
    for (i, id) in &stored {
        let read = read_all(&reader_store, id).await.unwrap();
        assert_eq!(
            read,
            vec![u8::try_from(*i % 251).expect("in range") + 1; 30_000 + i],
            "blob {i} is wrong"
        );
    }
}
