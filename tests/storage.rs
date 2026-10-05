//! The blob store: content addressing, length enforcement, deduplication, and
//! refusing to read anything the store does not hold.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use lepatch::{
    command::load_snapshot,
    layout::IndexSet,
    pack::StorageStored,
    storage::{BlobId, BlobStore, Compression, Read, SegmentLimit, StorageGet, StoragePut},
};
use std::fs;
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

async fn read_all(store: &BlobStore<Read>, id: &BlobId) -> std::io::Result<Vec<u8>> {
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

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(
        read_all(&store, &common::id_of(&payload)).await.unwrap(),
        payload
    );
}

#[tokio::test]
async fn blobs_appended_in_sequence_do_not_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    let store = BlobStore::open_for_reading(&blob).await.unwrap();

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

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store
        .put(&common::id_of(&[]), common::reader(Vec::new()), 0)
        .await
        .unwrap();

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    // `assert_eq!(..., [])` no longer type-checks: `serde_json` is a
    // dev-dependency, so it is linked into this binary, and its `Value` has a
    // `PartialEq<u8>` impl that makes the empty array's element type ambiguous.
    assert_eq!(
        read_all(&store, &common::id_of(&[])).await.unwrap().len(),
        0
    );
}

#[tokio::test]
async fn a_reader_shorter_than_the_declared_length_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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
    // it was given. It does this *before* it reserves a place in the file or
    // writes a byte. The store therefore has no record to abandon and no hole to
    // punch. The set is exactly as it was before the call.
    //
    // An earlier design wrote the record first and took its magic away on a
    // mismatch. That design left the store reporting itself as damaged until
    // something repaired it. The design worked. It needed a window in which a
    // record existed on disk. In that window, the bytes on disk were not the ones
    // its name promised.
    let store = BlobStore::open_for_reading(&blob).await.unwrap();
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
    let _ = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    let store = BlobStore::open_for_reading(&blob).await.unwrap();

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
/// The recovery scan checks that a record's payload is entirely present. The scan
/// does this before it believes the header. A chopped store therefore reports the
/// blob as missing. It does not hand back a reader that stops short. In a restore,
/// such a reader would produce a quietly wrong file.
#[tokio::test]
async fn a_blob_cut_short_by_a_truncated_store_is_reported_missing() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payload = vec![7u8; 100];
    let id = common::id_of(&payload);
    let len = common::len_of(&payload);

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    store.put(&id, common::reader(payload), len).await.unwrap();

    // Chop the last segment in half, which lands inside the one record it holds.
    let segment = IndexSet::new(&blob).segment_path(0);
    let bytes = std::fs::read(&segment).unwrap();
    std::fs::write(&segment, &bytes[..40]).unwrap();

    // A read refuses rather than repairing. The damage therefore surfaces on the
    // way in rather than as a missing blob. Either result is honest. A short read
    // handed back as though it were the blob would not be honest.
    let error = BlobStore::open_for_reading(&blob).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("damaged"),
        "the error should say the segment is damaged, got: {error}"
    );
}

/// A segment that is shorter than its own index is refused, not read short.
///
/// `stored` is the path a merge takes when it keeps the frames it was sent. It
/// answers with bytes the pack handed over. It does not answer with bytes the
/// store already held. It bounds the read by the record's own stored length. The
/// bound stops a wrong index from pulling in a neighbour. The path refuses
/// outright when the segment is shorter than that length. A blob handed out now
/// has to be the blob that was indexed.
///
/// The bound had no test. It is the only thing between a hand-edited store and a
/// `put_stored` that writes a frame nobody hashed. The store builds the index
/// when it opens. The test therefore cuts the segment after the store opens. A
/// store opened *after* the cut is refused much earlier, as damage. The test above
/// pins that case.
#[tokio::test]
async fn a_segment_shorter_than_its_index_is_refused_when_stored() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    let payload = common::noisy(200_000);
    let id = common::id_of(&payload);
    store
        .put(
            &id,
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();
    store.sync_data().await.unwrap();
    drop(store);

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let placement = store.locate(&id).unwrap();

    // Cut the segment where the record begins, which leaves the index saying a
    // record is there and the file not having it.
    let segment = IndexSet::new(&blob).segment_path(placement.segment);
    let file = fs::OpenOptions::new().write(true).open(&segment).unwrap();
    file.set_len(placement.offset).unwrap();
    drop(file);

    let error = store
        .stored(&id)
        .await
        .expect_err("a record the segment no longer holds must be refused");

    assert!(
        error.to_string().contains("segment"),
        "the refusal should say which segment is short, got: {error}"
    );
}

/// A file that is not a store is refused. It is not truncated on the way to being
/// refused. That second part matters.
#[tokio::test]
async fn a_file_that_is_not_a_store_is_refused_without_being_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-a-store");

    // Somebody's data in the place a segment would go: long enough to look
    // plausible, and named so the set finds it and tries to read it.
    let segment = IndexSet::new(&path).segment_path(0);
    let contents = vec![0xabu8; 4_096];
    std::fs::write(&segment, &contents).unwrap();

    let error = BlobStore::open_for_reading(&path).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not a backup store this build can read"),
        "the error should say what it found, got: {error}"
    );

    // Opening for writing refuses it too, and must leave it exactly as it was.
    let error = BlobStore::open_for_writing(&path, SegmentLimit::default(), Compression::default())
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
    let segment = IndexSet::new(&path).segment_path(0);
    let mut header = Vec::from(*b"LPST");
    header.extend_from_slice(&99u32.to_le_bytes());
    header.extend_from_slice(&[0u8; 64]);
    std::fs::write(&segment, &header).unwrap();

    let error = BlobStore::open_for_reading(&path).await.unwrap_err();
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
    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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
        IndexSet::new(&blob).segment_path(0).is_file(),
        "a set in a missing directory should create it and its first segment"
    );

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(
        read_all(&store, &common::id_of(&payload)).await.unwrap(),
        payload
    );
}

/// Content already in the store is not written again.
///
/// This is what makes deduplication a property of the store and not of the caller.
/// A run that has never seen the previous version can still hand over every chunk
/// it read. That run pays only for the chunks that are new.
#[tokio::test]
async fn content_the_store_already_holds_is_not_written_again() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payload = common::noisy(64 * 1024);
    let id = common::id_of(&payload);

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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
    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
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

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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
    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    let reader_store = BlobStore::open_for_reading(&blob).await.unwrap();
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
/// The store claims a range for each blob. It then copies into that range without
/// holding anything. This is what lets uploads run in parallel. The method is
/// sound only if every claim is distinct. It goes wrong quietly. Two writers agree
/// on an offset. One writer clobbers the other. Both records look perfectly valid.
///
/// Every payload is filled with its own index. An overlap then shows up as a blob
/// that reads back with somebody else's number in it. The payloads are large and
/// uneven. A writer cannot finish before its neighbours have started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_uploads_do_not_overlap() {
    const BLOBS: usize = 64;

    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = std::sync::Arc::new(
        BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
            .await
            .unwrap(),
    );

    // Uneven, and big enough that the copies genuinely overlap in time.
    //
    // The code fills each payload from a hash rather than a repeated byte. Two
    // reasons explain that choice. Otherwise an overlap would be masked. A run of
    // one byte compresses to almost nothing. Two writers whose ranges overlapped
    // could then produce a store that *looked* the right size. The size assertion
    // below also only means "the claimed ranges are disjoint" while the stored
    // bytes are the bytes handed in. That property belongs to the payloads rather
    // than to the store.
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

    // Reopened, not the same handle. A restore or a verify does this.
    let reader_store = BlobStore::open_for_reading(&blob).await.unwrap();
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

    // The store is at least as large as the payloads it holds. The record headers
    // are the difference. Finding no gaps below is what makes the claims disjoint
    // rather than merely non-overlapping.
    let total: usize = payloads.iter().map(Vec::len).sum();
    assert!(
        common::set_size(&blob) >= total as u64,
        "the store is smaller than the sum of its blobs"
    );

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(store.versions().unwrap().len(), 0, "nothing was committed");
}

/// A failed upload must not disturb the ones that follow it.
///
/// A failed record keeps the range it claimed. No commit ever references those
/// bytes. Giving the range back would mean truncating past a writer. That writer
/// has already claimed the space after the range. The next run's recovery reclaims
/// the lot of them. That reclaim is the other half of the guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_upload_leaves_the_rest_of_the_store_readable() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = std::sync::Arc::new(
        BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
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

    // The blobs that were written before the failure are still readable. The
    // reader uses an index that is rebuilt from the file. A half-finished record
    // therefore does not take its neighbours down with it.
    let reader_store = BlobStore::open_for_reading(&blob).await.unwrap();
    for (i, id) in &stored {
        let read = read_all(&reader_store, id).await.unwrap();
        assert_eq!(
            read,
            vec![u8::try_from(*i % 251).expect("in range") + 1; 30_000 + i],
            "blob {i} is wrong"
        );
    }
}

/// A store's `Debug` says how much it holds, not what.
///
/// The derived version walked the whole in-memory index. A `{:?}` of a store with
/// a few hundred blobs was a few hundred hashes on one line. Nobody can read that
/// log line. The line appears at the moment something goes wrong. That moment is
/// the only time anyone asks for it. The counts are what a diagnostic actually
/// wants.
///
/// The test formats two stores of very different sizes and compares the *length*.
/// The assertion therefore covers an output that does not scale with the store. A
/// test that checked for one absence would pass on a store that is too small to
/// trip it.
#[tokio::test]
async fn a_store_does_not_print_its_index() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");

    let few = {
        fs::write(source.join("a.bin"), common::noisy(60_000)).unwrap();
        common::run_backup(&source, &blob).await;
        let store =
            BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
                .await
                .unwrap();
        format!("{store:?}")
    };

    // A lot more of it.
    for i in 0..40 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(i, 90_000),
        )
        .unwrap();
    }
    common::run_backup(&source, &blob).await;
    let many = {
        let store =
            BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
                .await
                .unwrap();
        format!("{store:?}")
    };

    assert!(
        many.contains("blobs:") && many.contains("commits:"),
        "a store should report what it holds: {many}"
    );

    // The point of the test is that the output does not grow with the store. A
    // derived `Debug` prints one entry per blob. A hash is 64 characters. Even
    // this small a store would therefore grow past a few hundred bytes.
    let held = many
        .split("blobs: ")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse::<usize>().ok())
        .unwrap_or(0);

    assert!(
        held > 20,
        "the fixture should hold a good many blobs for this to mean anything: {many}"
    );

    // Two stores are compared. Asserting only an upper bound would pass for a
    // `Debug` that printed nothing at all. The second store makes "does not grow"
    // a claim about growth rather than about brevity.
    assert!(
        many.len() < few.len() + 40,
        "a store's Debug grew with the store: {} bytes for one blob against {held} blobs at {} \
         bytes:\n{many}",
        few.len(),
        many.len(),
    );
}

/// A filesystem failure names the path and keeps its kind.
///
/// A bare `?` on `File::open` gives `No such file or directory (os error 2)`.
/// That message is true and useless. A person reading it has a machine with many
/// files. That person has no idea which one failed. The kind is the other half. A
/// crate that hands `io::Error` to callers has already made the kinds the
/// contract. A wrapper that flattened them to `Other` would be a quiet regression.
/// It would be the same kind of regression that the `NotFound` swallow was.
///
/// The test runs as root, so it cannot provoke a real permission error. A missing
/// file is a real one. A missing file needs no special anything.
#[test]
fn a_filesystem_failure_names_the_path_and_keeps_its_kind() {
    let missing = std::path::Path::new("/nonexistent-lepatch/store.0000.bin");

    // The bare form, for comparison. This is what the crate used to hand back.
    let bare = std::fs::File::open(missing).unwrap_err();
    assert_eq!(bare.kind(), std::io::ErrorKind::NotFound);
    assert!(
        !bare.to_string().contains("store.0000.bin"),
        "the bare message is what this is fixing: {bare}"
    );

    let wrapped = lepatch::storage::at(missing, "open", std::fs::File::open(missing)).unwrap_err();

    assert_eq!(
        wrapped.kind(),
        std::io::ErrorKind::NotFound,
        "the kind must survive the wrapper, or a caller branching on it is guessing"
    );
    assert!(
        wrapped.to_string().contains("store.0000.bin"),
        "the message must name the file: {wrapped}"
    );
    assert!(
        wrapped.to_string().contains("could not open"),
        "and say what was being done to it: {wrapped}"
    );
    assert!(
        wrapped.to_string().contains("No such file or directory"),
        "and keep what the operating system said, rather than replacing it: {wrapped}"
    );
}

/// The async wrapper is the same wrapper, awaited.
#[tokio::test]
async fn the_async_wrapper_behaves_the_same() {
    let missing = std::path::Path::new("/nonexistent-lepatch/segment.bin");

    let error = lepatch::storage::at_async(missing, "open", tokio::fs::File::open(missing))
        .await
        .expect_err("a missing file is still missing");

    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert!(
        error.to_string().contains("segment.bin"),
        "the message must name the file: {error}"
    );
}

/// A `StorageGet` future crosses a thread, and the traits are not object safe.
///
/// Two things about the return-position `impl Future` in [`StorageGet`] and
/// [`StoragePut`] deserve a test. Both are otherwise invisible.
///
/// The `+ Send` is not decoration. The code awaits these futures inside
/// `tokio::spawn`. Without the bound, the compiler cannot prove that a spawned
/// task is `Send`. A backup would then fail to build. Asserting that a store can
/// move into a task and work there proves the bound is real and not merely
/// declared.
///
/// These traits are also *not* object safe. That is a deliberate trade. No
/// `dyn StorageGet` appears anywhere. Every caller is generic and monomorphises.
/// `is_object_safe` is a nightly-only method. The check is therefore that naming
/// a trait object is a compile error. This test notes the absence. The test does
/// not use a `trybuild` case. Such a case would be a dependency for one line.
#[tokio::test]
async fn a_store_crosses_a_thread_boundary_and_back() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), common::noisy(80_000)).unwrap();
    let blob = dir.path().join("store");

    common::run_backup(&source, &blob).await;

    // A blob address the store must hold. Taken from the snapshot so the test
    // does not have to know how the chunker cut the file.
    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = load_snapshot(&store, store.snapshot(1).unwrap())
        .await
        .unwrap();
    let id = snapshot
        .chunks
        .first()
        .copied()
        .expect("the fixture has at least one chunk");

    // The store moves into a spawned task and is used there. `StorageGet::get`
    // returns a future. The code is therefore also the assertion that the future
    // is `Send`. That is the compiled form of the `+ Send` in the trait. A backup
    // does exactly this through `pipeline.rs`. A backup would not build without
    // it.
    let payload = tokio::spawn(async move {
        let mut reader = store.get(&id).await.expect("the store holds this chunk");

        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("reads to end");
        out
    })
    .await
    .expect("the task should not panic");

    assert!(!payload.is_empty(), "the chunk should have come back");
    assert_eq!(
        id, id,
        "the bytes read back must be the bytes the chunk is addressed by"
    );
}
