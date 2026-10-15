// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
// The crate denies `unwrap_used` and `expect_used`, and `allow-expect-in-tests`
// only covers code inside a `#[test]` function.
// The helpers below are plain functions that a benchmark calls.
// They need the exemption spelled out.
// A benchmark that cannot build its fixture has nothing useful to measure.
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Benchmarks for the unit-level work a backup is made of.
//!
//! These are the numbers that decide where an optimisation is worth making, and
//! each covers one line of a backup's cost. Roughly, a backup spends its time
//! like this:
//!
//! ```text
//! read + chunk (CDC)   ~46%   chunking
//! blake3                ~27%   hashing
//! plumbing              ~28%   (channel and task handoffs — not here)
//! ```
//!
//! The "read + chunk" row is the `chunking/read_and_cut` benchmark.
//! That benchmark is a little more than its name.
//! One iteration there includes building the `Chunker`, which stats every file in
//! the tree before the first byte is read.
//! That cannot be timed separately within one iteration, because a `Chunker` is a
//! stream that can only be consumed once.
//! So `chunking/registering` measures that half on its own, and the difference is
//! the chunking.
//!
//! The split is indicative, and the third line is a plug rather than a
//! measurement.
//! The third line is the remainder once the other two are subtracted.
//! That is why the parenthesis says "not here".
//! These were taken on a 200 MiB tree, which this file does not reproduce.
//! This file's own fixture is 32 MiB.
//! So treat the proportions as a starting point for deciding what to measure.
//! Take the absolute figures from `benches/throughput.rs` or from the binary.
//!
//! Criterion is reliable for a ratio within one run.
//! Criterion is unreliable for a comparison against a previous run on a shared
//! machine.
//! `examples/ab.rs` documents why, with the measurements.

use std::{hint::black_box, io::Cursor, path::PathBuf};

use blulib::{
    metadata::{Attributes, File, FileChunk, Snapshot, Timestamp},
    reader::{Chunker, ChunkerConfig},
};
use criterion::{Criterion, Throughput};
use tempfile::TempDir;

#[path = "fixture.rs"]
mod fixture;
use fixture::noisy;

/// Bytes each generated file holds. Matches the CLI's default chunker's scale:
/// 8 KiB minimum, 16 KiB average, so a file is a handful of chunks.
const FILE_BYTES: usize = 512 * 1024;

/// How many files the tree benchmarks run over.
const FILES: usize = 64;

/// A tree on disk to chunk, built once per benchmark process.
fn tree(dir: &TempDir) -> Vec<PathBuf> {
    let root = dir.path().join("src");
    std::fs::create_dir_all(&root).expect("mkdir");

    let mut paths = Vec::with_capacity(FILES);
    for i in 0..FILES {
        let path = root.join(format!("f{i:04}.bin"));
        // Half shared between files, half unique, so the chunker sees a stream
        // that resembles a real tree rather than one enormous unique blob.
        let mut bytes = noisy(FILE_BYTES / 2, 7);
        bytes.extend_from_slice(&noisy(FILE_BYTES / 2, u64::try_from(i).unwrap_or(0)));
        std::fs::write(&path, &bytes).expect("write");
        paths.push(path);
    }

    paths
}

fn config() -> ChunkerConfig {
    ChunkerConfig {
        min_size: 8 * 1024,
        avg_size: 16 * 1024,
        max_size: 64 * 1024,
    }
}

/// A snapshot shaped like a real one, for the serialisation benchmarks.
fn sample_snapshot(chunks: usize) -> Snapshot {
    let mut snapshot = Snapshot::new(Timestamp { secs: 1, nanos: 0 });
    snapshot.files.push(File {
        path: PathBuf::from("dir/file.bin"),
        len: u64::try_from(FILE_BYTES).unwrap_or(0),
        attrs: Attributes {
            owner: None,
            mode: Some(0o644),
            mtime: None,
            xattrs: Vec::new(),
            windows_acl: None,
        },
    });

    for i in 0..chunks {
        // A chunk is the BLAKE3 of its contents, and that is also its address in the
        // store.
        // So a snapshot records no location of its own.
        snapshot
            .chunks
            .push(blake3::hash(&u64::try_from(i).unwrap_or(0).to_le_bytes()));
        snapshot.file_chunks.push(FileChunk {
            chunk_index: u32::try_from(i).unwrap_or(0),
            file_index: 0,
            chunk_offset: 0,
            file_offset: u64::try_from(i).unwrap_or(0) * 16384,
            length: 16384,
        });
    }

    snapshot
}

/// Opening the tree and cutting it into content-defined chunks, with no hashing
/// and no storage. This is the 46%: the one thread that cannot be parallelised,
/// because each boundary is derived from the bytes before it.
///
/// One iteration is one whole pass over the fixture, and it includes building the
/// `Chunker`.
/// Building the `Chunker` is `64` × `stat` and a `Vec<PathBuf>` clone, not
/// chunking.
/// That cannot be hoisted out of the timed region.
/// `Chunker::new` takes the paths by value, and a `Chunker` is a stream that can
/// only be consumed once.
/// So the registry and the reading are one indivisible unit of work.
///
/// Which is why `registering` below measures the registry on its own.
/// The difference between the two is the chunking.
/// The doc comment at the top of this file attributes a share of a *backup* to
/// chunking.
/// So the share of this number that is not chunking is worth being able to
/// subtract.
fn chunking(c: &mut Criterion) {
    let dir = TempDir::new().expect("tempdir");
    let paths = tree(&dir);
    let total = u64::try_from(FILE_BYTES * FILES).unwrap_or(0);

    let mut group = c.benchmark_group("chunking");
    group.throughput(Throughput::Bytes(total));
    group.sample_size(20);
    group.bench_function("read_and_cut", |b| {
        b.iter(|| {
            let chunker = Chunker::new(paths.clone(), config()).expect("chunker");
            let mut count = 0usize;
            for chunk in chunker {
                let chunk = chunk.expect("chunk");
                black_box(&chunk.data);
                count += 1;
            }
            count
        });
    });

    // The setup half, measured so the number above can be read as a difference
    // rather than taken whole.
    // `Chunker::new` stats every file before the first byte is read, so this is
    // not nothing.
    // A reader told "46% of a backup is chunking" deserves to know how much of the
    // measurement is bookkeeping.
    group.throughput(Throughput::Elements(u64::try_from(FILES).unwrap_or(0)));
    group.bench_function("registering", |b| {
        b.iter(|| {
            let chunker = Chunker::new(paths.clone(), config()).expect("chunker");
            black_box(&chunker);
        });
    });
    group.finish();
}

/// Hashing one chunk's bytes with blake3. This is the 27% that a change of mind
/// about *where* hashing happens can move off the critical thread.
fn hashing(c: &mut Criterion) {
    // A chunk's worth of data, at the chunker's default average.
    let chunk = noisy(16 * 1024, 11);
    let total = u64::try_from(chunk.len()).unwrap_or(0);

    let mut group = c.benchmark_group("hashing");
    group.throughput(Throughput::Bytes(total));
    group.bench_function("blake3_16KiB", |b| {
        b.iter(|| black_box(blake3::hash(black_box(&chunk))));
    });
    group.finish();
}

/// Hashing a whole file's worth at once, to show the per-call overhead that
/// 16 KiB chunks pay. Larger chunks are cheaper per byte, which is the other
/// half of the chunk-size trade-off.
fn hashing_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashing_by_size");

    for size in [16 * 1024usize, 64 * 1024, 256 * 1024, 1024 * 1024] {
        let data = noisy(size, 13);
        group.throughput(Throughput::Bytes(u64::try_from(size).unwrap_or(0)));
        group.bench_function(format!("blake3_{size}"), |b| {
            b.iter(|| black_box(blake3::hash(black_box(&data))));
        });
    }

    group.finish();
}

/// Encoding and decoding a snapshot. Every backup ends with one of these and
/// every restore begins with one, so it is worth knowing what it costs.
fn serialisation(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_serialisation");

    for chunks in [1usize, 100, 10_000] {
        let snapshot = sample_snapshot(chunks);
        let encoded = snapshot.to_bytes().expect("encodes");

        group.throughput(Throughput::Bytes(u64::try_from(encoded.len()).unwrap_or(0)));
        group.bench_function(format!("encode_{chunks}_chunks"), |b| {
            b.iter(|| black_box(snapshot.to_bytes()));
        });
        group.bench_function(format!("decode_{chunks}_chunks"), |b| {
            b.iter(|| black_box(Snapshot::try_from(black_box(&encoded[..])).expect("decode")));
        });
    }

    group.finish();
}

/// The bounded stream adapters, which every restored byte passes through twice:
/// once out of the store, once into the file.
fn adapters(c: &mut Criterion) {
    use blulib::slice::{SliceAsyncReader, SliceAsyncWriter};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let source = noisy(64 * 1024, 17);
    let total = u64::try_from(source.len()).unwrap_or(0);

    let mut group = c.benchmark_group("adapters");
    group.throughput(Throughput::Bytes(total));
    group.sample_size(30);

    group.bench_function("bounded_read", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut reader = SliceAsyncReader::new(Cursor::new(source.clone()), total);
                let mut out = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut out)
                    .await
                    .expect("read");
                black_box(&out);
            });
        });
    });

    group.bench_function("bounded_write", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut sink = SliceAsyncWriter::new(Vec::new(), total);
                let mut reader = Cursor::new(source.clone());
                let copied = tokio::io::copy(&mut reader, &mut sink).await.expect("copy");
                black_box(copied);
            });
        });
    });

    group.finish();
}

/// Guards the helper above against the tree silently not being built.
#[test]
fn the_benchmark_tree_is_not_empty() {
    let dir = TempDir::new().expect("tempdir");
    let paths = tree(&dir);
    assert_eq!(paths.len(), FILES);
    for path in &paths {
        assert_eq!(
            std::fs::metadata(path).expect("stat").len(),
            u64::try_from(FILE_BYTES).unwrap_or(0)
        );
    }
}

/// Run every benchmark in this file.
///
/// Written out rather than using `criterion_main!`.
/// The expansion of that macro is an undocumented function, and the crate-level
/// `missing_docs` deny rejects it.
/// An attribute on the macro invocation is ignored.
/// Spelling the same four lines out keeps the lint switched on for the benchmarks
/// themselves.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    chunking(&mut criterion);
    hashing(&mut criterion);
    hashing_sizes(&mut criterion);
    serialisation(&mut criterion);
    adapters(&mut criterion);
    criterion.final_summary();
}
