// The crate denies `unwrap_used` and `expect_used`, and `allow-expect-in-tests`
// only covers code inside a `#[test]` function.
// The helpers below are plain functions that a benchmark calls.
// They need the exemption spelled out.
// A benchmark that cannot build its fixture has nothing useful to measure.
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Benchmarks for the store: the layer between the chunking and the pack.
//!
//! `benches/hotpath.rs` measures the chunking and the hashing of a backup.
//! `benches/pack.rs` measures the pack layer.
//! This file measures the store in the middle: the blobs a backup writes and
//! the blobs a restore reads.
//!
//! # What each benchmark times
//!
//! ```text
//! store/put    writing the blobs through the store, at the concurrency a backup uses
//! store/open   opening a store for reading, and the blob map that the open builds
//! store/sync   making the dirty segments of a store durable
//! ```
//!
//! The run builds the fixture once, before the first measurement.
//! Criterion times the loop, so a fixture built inside the loop would measure
//! the fixture instead of the code.
//!
//! # The fixture
//!
//! The store holds 256 blobs of 64 KiB.
//! That is about the shape of a small backup.
//! The bytes come from the repository's own generator, so they hold no run of
//! equal bytes.
//! The compressor cannot shrink them, so the store keeps every blob as it is.
//!
//! # The open benchmark
//!
//! `store/open` guards the reservation that an open makes for its blob map.
//! A segment header carries no record count, so the only bound is the length
//! of the file over the smallest record.
//! A segment of 64 KiB records overcounts its own records by a factor of a
//! thousand.
//! So the store caps the reservation, and the cap has to hold on every open.
//! The benchmark opens the same store again and again, and the fixture holds a
//! known number of records.
//! A change to the reservation then shows up in the time and in the memory.
//!
//! Criterion is reliable for a ratio within one run.
//! Criterion is unreliable for a comparison against a previous run on a shared
//! machine.
//! `examples/ab.rs` documents why, with the measurements.

use std::{
    hint::black_box,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use blulib::{
    layout::IndexSet,
    storage::{BlobId, BlobStore, Compression, SegmentLimit, StoragePut, Write},
};
use criterion::{BenchmarkGroup, Criterion, Throughput, measurement::WallTime};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "fixture.rs"]
mod fixture;
use fixture::noisy;

/// Blobs the fixture store holds.
///
/// A few hundred blobs of 64 KiB is about the shape of a small backup.
const BLOBS: usize = 256;

/// Bytes each blob holds.
const BLOB_BYTES: usize = 64 * 1024;

/// The most writes the store holds at once.
///
/// A backup keeps this many writes in flight, and each write holds one chunk.
/// The default is the machine's parallelism capped at this number.
const CONCURRENCY_CEILING: usize = 8;

/// Bytes a segment holds before the store starts a new one.
///
/// The default is 4 GiB, so a small backup writes one segment.
/// This limit is smaller, so the sync fixture holds several segments and the
/// sync has more than one to cover.
const SEGMENT_BYTES: u64 = 4 * 1024 * 1024;

/// Segment files the put benchmark clears before a round.
///
/// The put fixture writes one segment, and the other numbers cover a store
/// that rolled over.
const SEGMENT_FILES: u32 = 8;

/// The blobs the benchmarks write.
#[derive(Debug)]
struct Payloads {
    /// The bytes of each blob.
    bytes: Vec<Arc<[u8]>>,

    /// The name of each blob.
    ///
    /// The store names a blob by the hash of its bytes, so the benchmark
    /// computes that hash before the timed region starts.
    ids: Vec<BlobId>,
}

/// The store the benchmarks measure.
#[derive(Debug)]
struct Fixture {
    /// The temporary directory that holds every file of the fixture.
    ///
    /// That directory owns the files, so it has to outlive the run.
    /// Nothing reads it, and the underscore says so.
    _dir: TempDir,

    /// The blobs the benchmarks write.
    payloads: Payloads,

    /// The path of the store the open benchmark reads.
    open_path: PathBuf,

    /// The path of the store the put benchmark writes into.
    put_path: PathBuf,

    /// The store the sync benchmark makes durable.
    sync_store: BlobStore<Write, Compression>,

    /// The segments the sync benchmark covers.
    segments: usize,
}

/// The writes the store holds at once.
///
/// A backup keeps `concurrency` writes in flight, and each write holds one
/// chunk.
/// The default is the machine's parallelism capped at [`CONCURRENCY_CEILING`].
fn concurrency() -> usize {
    std::thread::available_parallelism().map_or(CONCURRENCY_CEILING, |n| {
        n.get().clamp(1, CONCURRENCY_CEILING)
    })
}

/// Build the blobs the benchmarks write.
///
/// The bytes come from the repository's own generator, so they hold no run of
/// equal bytes.
fn payloads(count: usize) -> Payloads {
    let mut bytes = Vec::with_capacity(count);
    let mut ids = Vec::with_capacity(count);

    for index in 0..count {
        let payload: Arc<[u8]> = Arc::from(noisy(BLOB_BYTES, u64::try_from(index).unwrap_or(0)));
        ids.push(blake3::hash(&payload));
        bytes.push(payload);
    }

    Payloads { bytes, ids }
}

/// Write every payload into a new store at `path`.
///
/// The store keeps the writer open, so the caller can sync it later.
/// The caller passes the segment limit, so the fixture can hold more than one
/// segment.
async fn fill(
    path: &Path,
    payloads: &Payloads,
    limit: SegmentLimit,
) -> BlobStore<Write, Compression> {
    let store = BlobStore::builder(path)
        .segment_limit(limit)
        .open_writer()
        .await
        .expect("store");

    for (id, bytes) in payloads.ids.iter().zip(payloads.bytes.iter()) {
        let len = u64::try_from(BLOB_BYTES).unwrap_or(0);
        store
            .put(id, Box::new(Cursor::new(Arc::clone(bytes))), len)
            .await
            .expect("put");
    }

    store
}

/// Open a store that holds nothing.
///
/// The store refuses to write a blob it already holds. A round of writes
/// needs a store that holds none of them.
/// The files of the last round go first.
async fn empty_store(path: &Path) -> BlobStore<Write, Compression> {
    let set = IndexSet::new(path);

    for segment in 0..SEGMENT_FILES {
        // The file may not exist, and the error then carries no news.
        let _ = std::fs::remove_file(set.segment_path(segment));
        let _ = std::fs::remove_file(set.segment_cache_path(segment));
    }

    BlobStore::builder(path).open_writer().await.expect("store")
}

/// Write every payload through the store, with the writes in flight.
///
/// The store refuses to write a blob it already holds. The caller gives
/// this function a store that holds none of the payloads.
async fn put_all(store: Arc<BlobStore<Write, Compression>>, payloads: &Payloads) {
    let limit = concurrency();
    let mut next = 0usize;
    let mut in_flight = Vec::new();

    while next < payloads.ids.len() || !in_flight.is_empty() {
        while next < payloads.ids.len() && in_flight.len() < limit {
            let id = payloads.ids[next];
            let bytes = Arc::clone(&payloads.bytes[next]);
            let len = u64::try_from(BLOB_BYTES).unwrap_or(0);
            next += 1;

            in_flight.push(tokio::spawn({
                let store = Arc::clone(&store);
                async move { store.put(&id, Box::new(Cursor::new(bytes)), len).await }
            }));
        }

        // Wait for the oldest write, so the other writes stay in flight.
        in_flight.remove(0).await.expect("task").expect("put");
    }
}

/// Write one more blob into the store the sync benchmark uses.
///
/// The bytes carry the round number, so every round writes a blob the store
/// does not hold yet.
/// The write happens before the clock starts, so the segment has pages to
/// flush again.
async fn write_one(store: &BlobStore<Write, Compression>, round: u64) {
    let mut bytes = noisy(BLOB_BYTES, round);
    bytes[..8].copy_from_slice(&round.to_le_bytes());

    let id = blake3::hash(&bytes);
    let len = u64::try_from(BLOB_BYTES).unwrap_or(0);
    store
        .put(&id, Box::new(Cursor::new(bytes)), len)
        .await
        .expect("put");
}

/// Build the fixture the benchmarks measure.
///
/// The fixture holds three stores, because the three benchmarks need different
/// ones.
/// The open benchmark reads a store of one segment, so every open scans the
/// whole segment and builds the blob map from it.
/// The sync benchmark writes into a store of several segments, so the sync has
/// more than one to cover.
/// The put benchmark writes into a store that holds nothing.
async fn build() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let payloads = payloads(BLOBS);

    let open_path = dir.path().join("open");
    // The open benchmark opens the store itself, so the writer goes away here.
    let _open_store = fill(&open_path, &payloads, SegmentLimit::default()).await;

    let sync_path = dir.path().join("sync");
    let sync_store = fill(&sync_path, &payloads, SegmentLimit::new(SEGMENT_BYTES)).await;
    let segments = IndexSet::new(&sync_path)
        .segments()
        .expect("segments")
        .len();

    let put_path = dir.path().join("put");

    Fixture {
        _dir: dir,
        payloads,
        open_path,
        put_path,
        sync_store,
        segments,
    }
}

/// Writing the blobs through the store.
///
/// One round writes every blob of the fixture through a store that holds
/// nothing. The writes are in flight at the concurrency a backup uses.
/// The round removes the files of the last round and opens a new store
/// before it starts the clock. The store refuses to write a blob it
/// already holds.
/// The number is the cost of the write path alone: the hashing, the
/// compression, and the append to the segment.
fn put(group: &mut BenchmarkGroup<'_, WallTime>, runtime: &Runtime, fixture: &Fixture) {
    let total = u64::try_from(BLOBS * BLOB_BYTES).unwrap_or(0);
    group.throughput(Throughput::Bytes(total));

    group.bench_function("put", |b| {
        b.iter_custom(|rounds| {
            let mut measured = Duration::ZERO;

            for _ in 0..rounds {
                let store = runtime.block_on(empty_store(&fixture.put_path));

                let start = Instant::now();
                runtime.block_on(put_all(Arc::new(store), &fixture.payloads));
                measured += start.elapsed();
            }

            measured
        });
    });
}

/// Opening a store for reading, and the blob map that the open builds.
///
/// The fixture store holds a known number of records of 64 KiB.
/// The store sizes the blob map before it reads the first record. The
/// reservation is the part of the open that a change can move.
/// Criterion opens the store on every iteration, so the run pays the
/// reservation on every open.
/// The number is the cost of one open: the scan of the segment, and the map
/// of every record the segment holds.
fn open(group: &mut BenchmarkGroup<'_, WallTime>, runtime: &Runtime, fixture: &Fixture) {
    let records = u64::try_from(fixture.payloads.ids.len()).unwrap_or(0);
    group.throughput(Throughput::Elements(records));

    group.bench_function("open", |b| {
        b.iter(|| {
            let store = runtime
                .block_on(BlobStore::builder(&fixture.open_path).open_reader())
                .expect("open");
            black_box(&store);
        });
    });
}

/// Making the dirty segments of a store durable.
///
/// The fixture store holds several segments, and the run wrote to every one of
/// them.
/// The store keeps a segment on its dirty list after a sync, so every round
/// covers every segment the run wrote to.
/// Each round writes one more blob before the clock starts, so the last
/// segment has pages to flush again.
/// The number is the cost of the sync alone: the open of each dirty segment,
/// and the flush of its pages.
fn sync(group: &mut BenchmarkGroup<'_, WallTime>, runtime: &Runtime, fixture: &Fixture) {
    let segments = u64::try_from(fixture.segments).unwrap_or(0);
    group.throughput(Throughput::Elements(segments));

    group.bench_function("sync", |b| {
        b.iter_custom(|rounds| {
            let mut measured = Duration::ZERO;

            for round in 0..rounds {
                runtime.block_on(write_one(&fixture.sync_store, round));

                let start = Instant::now();
                runtime
                    .block_on(fixture.sync_store.sync_data())
                    .expect("sync");
                measured += start.elapsed();
            }

            measured
        });
    });
}

/// A runtime with a full thread pool, since the code under test spawns.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Guards the fixture against losing its records.
///
/// The open benchmark needs a store that holds a known number of records.
/// The test opens the fixture store and locates every blob of it.
#[test]
fn the_fixture_store_holds_every_blob() {
    let runtime = runtime();
    let fixture = runtime.block_on(build());

    let store = runtime
        .block_on(BlobStore::builder(&fixture.open_path).open_reader())
        .expect("open");

    for id in &fixture.payloads.ids {
        assert!(store.locate(id).is_ok(), "the store must hold every blob");
    }
}

/// Run every benchmark in this file.
///
/// Written out rather than using `criterion_main!`.
/// The expansion of that macro is an undocumented function, and the crate-level
/// `missing_docs` deny rejects it.
/// The macro ignores an attribute on its invocation.
/// Spelling the same four lines out keeps the lint switched on for the benchmarks
/// themselves.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    let runtime = runtime();
    let fixture = runtime.block_on(build());

    let mut group = criterion.benchmark_group("store");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    put(&mut group, &runtime, &fixture);
    open(&mut group, &runtime, &fixture);
    sync(&mut group, &runtime, &fixture);

    group.finish();
    criterion.final_summary();
}
