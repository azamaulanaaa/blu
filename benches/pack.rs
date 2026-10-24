// The crate denies `unwrap_used` and `expect_used`, and `allow-expect-in-tests`
// only covers code inside a `#[test]` function.
// The helpers below are plain functions that a benchmark calls.
// They need the exemption spelled out.
// A benchmark that cannot build its fixture has nothing useful to measure.
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Benchmarks for the pack layer: one version's chunks in one file.
//!
//! `benches/hotpath.rs` measures the chunking and the hashing of a backup.
//! `benches/throughput.rs` measures a whole backup and a whole restore.
//! This file measures the layer between them: the pack.
//!
//! A pack lays one version's chunks end to end.
//! A client then fetches a version with a few range requests rather than one
//! request per chunk.
//! Every part of that file sits on a path a user waits for.
//! The path runs through the header, the records, the snapshot, the index
//! and the trailer.
//!
//! # What each benchmark times
//!
//! ```text
//! pack/open      the index decode and the repeated-hash scan
//! pack/locate    repeated lookups through the opt-in table
//! pack/needed    the ranges a client is missing
//! pack/covering  the sweep over sorted ranges
//! pack/source    the cached regions of a served range
//! pack/header    a record header served out of the header store
//! pack/decode    the decode of one zstd frame
//! ```
//!
//! The run builds the fixture once, before the first measurement.
//! Criterion times the loop, so a fixture built inside the loop would measure
//! the fixture instead of the code.
//!
//! # The fixture
//!
//! The tree holds 96 files of 256 KiB, so the pack holds about 970 chunks.
//! That is enough for the index decode and the lookups to show a number.
//!
//! Every file shares half of its bytes with every other file.
//! The other half belongs to that file alone.
//! The shared half repeats a block, so the store keeps it as a zstd frame.
//! The unique half is noise, so the store keeps it as it is.
//! The pack therefore holds both kinds of record, which is what a real pack
//! holds.
//!
//! # Reading the numbers
//!
//! Criterion is reliable for a ratio within one run.
//! Criterion is unreliable for a comparison against a previous run on a shared
//! machine.
//! `examples/ab.rs` documents why, with the measurements.

use std::{collections::HashSet, hint::black_box, path::PathBuf, time::Duration};

use blulib::{
    command::{BackupBuilder, Options, load_snapshot},
    pack::{Pack, PackLookup, Range, VirtualPack},
    storage::{BlobId, BlobStore, Codec, Compression, Compressor, Decompressor, Plan, Zstd},
};

use bytes::Bytes;
use criterion::{BenchmarkGroup, Criterion, Throughput, measurement::WallTime};
use tempfile::TempDir;

#[path = "fixture.rs"]
mod fixture;
use fixture::noisy;

/// Files in the benchmark tree.
const FILES: usize = 96;

/// Bytes each file holds.
const FILE_BYTES: usize = 256 * 1024;

/// Bytes of each file that every file shares.
const SHARED_BYTES: usize = 64 * 1024;

/// Bytes of the block that the shared part repeats.
///
/// The shared part repeats a block so that it compresses.
/// A zstd frame of noise comes out larger than the noise.
/// A store keeps noise as it is, and never writes a frame of it.
const BLOCK: usize = 4 * 1024;

/// Bytes of content the decode fixture holds.
const DECODE_BYTES: usize = 256 * 1024;

/// The version the fixture records.
const VERSION: u32 = 1;

/// The narrowest a record header can be, in bytes.
///
/// A header holds a magic, a kind, a codec, two lengths, a hash and a check.
/// The two lengths take at least one byte each, so the narrowest header is
/// this long.
/// A range of this many bytes at the start of the payload region lies inside
/// the first record's header.
/// The two lengths do not change that.
const MIN_HEADER: u64 = 48;

/// The pack the benchmarks measure.
#[derive(Debug)]
struct Fixture {
    /// The temporary directory the files live in.
    ///
    /// That directory owns every file the fixture wrote, so it has to outlive
    /// the run.
    /// Nothing reads it, and the underscore says so.
    _dir: TempDir,

    /// The pack file on disk.
    path: PathBuf,

    /// The pack, opened once.
    pack: Pack,

    /// The same pack as the serve path computes it.
    virtual_pack: VirtualPack,

    /// The chunks a client already holds.
    have: HashSet<BlobId>,
}

/// One block of noise, repeated until it reaches `len` bytes.
///
/// The bytes come from the repository's own generator.
/// The repetition is what makes the result compress.
fn repeated(block: usize, len: usize, seed: u64) -> Vec<u8> {
    let pattern = noisy(block, seed);
    let mut out = Vec::with_capacity(len);

    while out.len() < len {
        out.extend_from_slice(&pattern);
    }
    out.truncate(len);
    out
}

/// The tree the pack comes from.
///
/// Every file shares half of its bytes with every other file.
/// The other half belongs to that file alone.
/// The shared half repeats a block, so the store keeps it as a zstd frame.
/// The unique half is noise, so the store keeps it as it is.
fn tree(dir: &TempDir) -> PathBuf {
    let root = dir.path().join("src");
    std::fs::create_dir_all(&root).expect("mkdir");

    let shared = repeated(BLOCK, SHARED_BYTES, 7);

    for i in 0..FILES {
        let mut bytes = shared.clone();
        let unique = FILE_BYTES - SHARED_BYTES;
        bytes.extend_from_slice(&noisy(unique, u64::try_from(i).unwrap_or(0)));
        std::fs::write(root.join(format!("f{i:04}.bin")), bytes).expect("write");
    }

    root
}

/// The chunks a client already holds: every other chunk of the pack.
///
/// Half the pack is the right shape for a client that holds some of it.
/// The chunks the client lacks then sit apart from each other, so the ranges
/// the pack answers with are many and narrow.
fn held(pack: &Pack) -> HashSet<BlobId> {
    pack.chunks()
        .iter()
        .enumerate()
        .filter(|(at, _)| at % 2 == 0)
        .map(|(_, chunk)| chunk.id)
        .collect()
}

/// Build the pack the benchmarks measure.
///
/// The store holds one version of the tree, and the pack holds that version.
async fn build() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let source = tree(&dir);
    let blob = dir.path().join("store");

    let storage = BlobStore::builder(&blob)
        .open_writer()
        .await
        .expect("store");
    let committed = BackupBuilder::new(&source, storage)
        .options(Options::default())
        .latest(None)
        .run()
        .await
        .expect("backup");

    let store = BlobStore::builder(&blob)
        .open_reader()
        .await
        .expect("store");
    let snapshot = load_snapshot(&store, committed.snapshot())
        .await
        .expect("snapshot");
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).expect("plan");

    let path = dir.path().join("v1.pack");
    Pack::write(&store, &plan, &snapshot, VERSION, &path)
        .await
        .expect("pack");

    // This bench opens the pack from its file, so every benchmark reads the
    // bytes a client would download.
    let pack = Pack::open(&path).expect("open");
    let virtual_pack = VirtualPack::new(&plan, &snapshot, VERSION, None, |segment| {
        dir.path().join(format!("segment{segment}.bin"))
    })
    .expect("virtual pack");

    let have = held(&pack);

    Fixture {
        _dir: dir,
        path,
        pack,
        virtual_pack,
        have,
    }
}

/// Opening a pack: the index decode and the repeated-hash scan.
///
/// One iteration reads the trailer and reads the index.
/// It decodes every entry, sorts the entries by hash, and walks the sorted
/// entries once to refuse a repeated hash.
fn open(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &Fixture) {
    let chunks = fixture.pack.len();
    group.throughput(Throughput::Elements(u64::try_from(chunks).unwrap_or(0)));

    group.bench_function("open", |b| {
        b.iter(|| {
            let pack = Pack::open(&fixture.path).expect("open");
            black_box(pack.len());
        });
    });
}

/// The opt-in lookup table, asked for every chunk of the pack.
///
/// The table is the fixture, so one iteration holds the lookups alone.
/// One iteration asks the table for every chunk the pack holds.
fn locate(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &Fixture) {
    let ids: Vec<BlobId> = fixture.pack.chunks().iter().map(|chunk| chunk.id).collect();
    let table = PackLookup::new(&fixture.pack);
    let count = u64::try_from(ids.len()).unwrap_or(0);
    group.throughput(Throughput::Elements(count));

    group.bench_function("locate", |b| {
        b.iter(|| {
            for id in &ids {
                black_box(table.locate(id));
            }
        });
    });
}

/// The ranges a client that holds half the pack still needs.
///
/// One iteration walks every record of the pack in payload order and merges
/// the runs of records the client lacks.
fn needed(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &Fixture) {
    let chunks = fixture.pack.len();
    group.throughput(Throughput::Elements(u64::try_from(chunks).unwrap_or(0)));

    group.bench_function("needed", |b| {
        b.iter(|| {
            let ranges = fixture.pack.needed(&fixture.have);
            black_box(ranges.len());
        });
    });
}

/// The sweep over sorted ranges.
///
/// The ranges come from the missing records, so there are many of them and
/// each one covers a single record.
/// One iteration sorts the ranges once and walks every record of the pack
/// once.
fn covering(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &Fixture) {
    let ranges = fixture.pack.needed(&fixture.have);
    let count = u64::try_from(ranges.len()).unwrap_or(0);
    group.throughput(Throughput::Elements(count));

    group.bench_function("covering", |b| {
        b.iter(|| {
            let covered = fixture.pack.chunks_covering(&ranges);
            black_box(covered.len());
        });
    });
}

/// The four regions the pack holds in memory.
fn cached_regions(pack: &VirtualPack) -> [Range; 4] {
    [
        pack.header_range(),
        pack.snapshot_range(),
        pack.index_range(),
        pack.trailer_range(),
    ]
}

/// The cached regions of a served range.
///
/// The pack holds the header, the snapshot, the index and the trailer in
/// memory.
/// One iteration asks for a range in each of those four regions.
/// Every answer shares the region's bytes rather than copying them.
fn source(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &Fixture) {
    let pack = &fixture.virtual_pack;
    let regions = cached_regions(pack);
    let bytes = regions.iter().map(Range::len).sum();
    group.throughput(Throughput::Bytes(bytes));

    group.bench_function("source", |b| {
        b.iter(|| {
            for region in regions {
                black_box(pack.source(region).expect("source"));
            }
        });
    });
}

/// The range that holds the first record's header.
fn first_header(pack: &VirtualPack) -> Range {
    let start = pack.payload_start();
    Range {
        start,
        end: start + MIN_HEADER,
    }
}

/// A record header served out of the header store.
///
/// The constructor encodes each record header once and keeps the bytes.
/// One iteration asks for a range inside the first record's header.
/// The answer comes from the store rather than from a second encoding.
fn header(group: &mut BenchmarkGroup<'_, WallTime>, fixture: &Fixture) {
    let pack = &fixture.virtual_pack;
    let region = first_header(pack);
    group.throughput(Throughput::Bytes(MIN_HEADER));

    group.bench_function("header", |b| {
        b.iter(|| {
            black_box(pack.source(region).expect("source"));
        });
    });
}

/// The zstd frame the decode fixture holds, and the length of its content.
fn compressed_frame() -> (Bytes, u64) {
    let content = repeated(BLOCK, DECODE_BYTES, 29);

    let (codec, stored) = Compressor::compress(&Compression::zstd(1), &content);
    assert_eq!(codec, Codec::Zstd, "the fixture must compress");

    let raw_len = u64::try_from(content.len()).unwrap_or(0);
    (Bytes::from(stored.into_owned()), raw_len)
}

/// The decode of one zstd frame.
///
/// The frame is the one the store writes for a payload it can shrink.
/// The decode runs on the blocking pool, so the number holds the hand-off to
/// that pool as well as the decode itself.
fn decode(group: &mut BenchmarkGroup<'_, WallTime>, runtime: &tokio::runtime::Runtime) {
    let (frame, raw_len) = compressed_frame();
    group.throughput(Throughput::Bytes(raw_len));

    group.bench_function("decode", |b| {
        b.iter(|| {
            let bytes = runtime.block_on(async {
                Decompressor::decompress(&Zstd, std::io::Cursor::new(frame.clone()), raw_len)
                    .await
                    .expect("decode")
            });
            black_box(bytes.len());
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

/// Guards the helper above against an empty tree.
#[test]
fn the_benchmark_tree_is_not_empty() {
    let dir = TempDir::new().expect("tempdir");
    let source = tree(&dir);

    let mut files = 0usize;
    for entry in std::fs::read_dir(&source).expect("readdir") {
        let entry = entry.expect("entry");
        let len = std::fs::metadata(entry.path()).expect("stat").len();
        assert_eq!(len, u64::try_from(FILE_BYTES).unwrap_or(0));
        files += 1;
    }

    assert_eq!(files, FILES);
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

    let mut group = criterion.benchmark_group("pack");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    open(&mut group, &fixture);
    locate(&mut group, &fixture);
    needed(&mut group, &fixture);
    covering(&mut group, &fixture);
    source(&mut group, &fixture);
    header(&mut group, &fixture);
    decode(&mut group, &runtime);

    group.finish();
    criterion.final_summary();
}
