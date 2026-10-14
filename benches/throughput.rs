// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
// The crate denies `unwrap_used` and `expect_used`, and `allow-expect-in-tests`
// only covers code inside a `#[test]` function.
// The helpers below are plain functions that a benchmark calls.
// They need the exemption spelled out.
// A benchmark that cannot build its fixture has nothing useful to measure.
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! End-to-end throughput for a whole backup and a whole restore.
//!
//! These are the numbers to look at before and after a change.
//! Everything in `hotpath` is a component, and this is whether the components add
//! up to something a user would feel.
//!
//! # What these numbers can and cannot tell you
//!
//! **The `concurrency_*` rows are trustworthy.** They are measured inside one run
//! under identical conditions.
//! The ratio between them is then a ratio between observations that shared the
//! same machine state.
//!
//! **Comparing one of these rows against a previous run of criterion is not.**
//! Criterion's interval covers sampling noise within a run.
//! On a shared machine, sampling noise is not the dominant noise source.
//! An effect under about 20% is not measurable that way. `examples/ab.rs` documents this with the
//! measurements behind it, and is what a before/after claim should use.
//!
//! **The absolute throughput here is higher than the real binary's, and that gap
//! is unexplained.**
//! The *ratios* are sound.
//! Every variant is measured the same way.
//! For a number to quote about how fast a backup is, time the binary.
//!
//! Setup is deliberately outside the timed region: the tree is built once, and
//! each iteration starts from an empty store. A backup that deduped against its
//! own previous run would report a throughput no user would ever see.
//!
//! Backup is measured across a range of `Options::concurrency` rather than at one
//! value.
//! The interesting question is not "how fast is this" but "where does it stop
//! getting faster".
//! On a four-core box the curve is strongly rising to a default of 8.
//! The curve is still climbing at the top of the range.
//! CPU time falls as concurrency rises rather than staying flat.
//! That is why 8 is the default.
//!
//! **The 3.0x figure this used to quote was measured on a broken fixture.
//! It is gone.**
//! The byte generator was a function of `seed + position`.
//! So the 50 MiB tree was one 128 KiB stream repeated 200 times and deduplicated
//! away almost entirely.
//! The benchmark was timing the cost of storing 128 KiB.
//! The shape of the curve above was re-measured on the fixed fixture, but the
//! number is not reproduced here.
//! That number is a property of the machine.
//! This file's own header already says the absolute figures are not a number to
//! quote about how fast a backup is.
//! `benches/fixture.rs` records what was wrong with the old one, and
//! `tests/fixtures.rs` is what stops it coming back.

use std::{
    hint::black_box,
    path::PathBuf,
    time::{Duration, Instant},
};

use blulib::{
    command::{Options, backup, restore},
    layout::IndexSet,
    storage::{BlobStore, Compression, SegmentLimit},
};
use criterion::{Criterion, Throughput};
use tempfile::TempDir;

#[path = "fixture.rs"]
mod fixture;
use fixture::noisy;

/// Files in the benchmark tree. Enough that per-file costs are visible, small
/// enough that a full run stays in the low seconds.
const FILES: usize = 200;

/// Size of each file.
const FILE_BYTES: usize = 256 * 1024;

/// Build the benchmark tree, half of each file shared with every other.
fn tree(dir: &TempDir) -> PathBuf {
    let root = dir.path().join("src");
    std::fs::create_dir_all(&root).expect("mkdir");

    let shared = noisy(FILE_BYTES / 2, 7);
    for i in 0..FILES {
        let mut bytes = shared.clone();
        bytes.extend_from_slice(&noisy(FILE_BYTES / 2, u64::try_from(i).unwrap_or(0)));
        std::fs::write(root.join(format!("f{i:05}.bin")), bytes).expect("write");
    }

    root
}

/// A runtime with a full thread pool, since the code under test spawns.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn total_bytes() -> u64 {
    u64::try_from(FILE_BYTES * FILES).unwrap_or(0)
}

/// Back up the tree, once per iteration, into a store emptied beforehand.
fn bench_backup(c: &mut Criterion) {
    let dir = TempDir::new().expect("tempdir");
    let src = tree(&dir);
    let blob = dir.path().join("store");
    let rt = runtime();

    let mut group = c.benchmark_group("backup");

    for concurrency in [1usize, 2, 4, 8, 16] {
        group.throughput(Throughput::Bytes(total_bytes()));
        // A whole backup moves hundreds of megabytes, so few samples is the
        // honest setting: more would take minutes to say the same thing.
        group.sample_size(10);
        group.bench_function(format!("concurrency_{concurrency}"), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let mut elapsed = Duration::ZERO;

                    for _ in 0..iters {
                        // A fresh store each time, or the run would deduplicate
                        // against itself and report a throughput nothing else
                        // would ever see.
                        for segment in 0..64 {
                            let _ =
                                std::fs::remove_file(IndexSet::new(&blob).segment_path(segment));
                        }

                        let storage = BlobStore::open_for_writing(
                            &blob,
                            SegmentLimit::default(),
                            Compression::default(),
                        )
                        .await
                        .expect("store");
                        let options = Options {
                            concurrency,
                            ..Options::default()
                        };

                        let started = Instant::now();
                        let committed = backup(&src, options, storage, None).await.expect("backup");
                        elapsed += started.elapsed();

                        black_box(committed.version);
                    }

                    elapsed
                })
            });
        });
    }

    group.finish();
}

/// Back up once, then time restoring it over and over into a fresh destination.
fn bench_restore(c: &mut Criterion) {
    let dir = TempDir::new().expect("tempdir");
    let src = tree(&dir);
    let blob = dir.path().join("store");
    let rt = runtime();

    let committed = rt.block_on(async {
        let storage =
            BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
                .await
                .expect("store");
        backup(&src, Options::default(), storage, None)
            .await
            .expect("backup")
    });

    let mut group = c.benchmark_group("restore");
    group.throughput(Throughput::Bytes(total_bytes()));
    group.sample_size(10);
    group.bench_function("restore", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut elapsed = Duration::ZERO;

                for _ in 0..iters {
                    let destination = dir.path().join("out");
                    let _ = std::fs::remove_dir_all(&destination);

                    let storage = BlobStore::open_for_reading(&blob).await.expect("store");

                    let started = Instant::now();
                    restore(&destination, committed.snapshot, storage)
                        .await
                        .expect("restore");
                    elapsed += started.elapsed();
                }

                elapsed
            })
        });
    });
    group.finish();
}

/// Whether a restore actually reproduced the tree.
///
/// A throughput benchmark that measures a broken restore is worse than no
/// benchmark.
/// `restore` returning `Ok` says nothing about the bytes it wrote.
/// Checked once, outside the timed region.
#[test]
fn the_throughput_benchmark_restores_what_it_backed_up() {
    let dir = TempDir::new().expect("tempdir");
    let src = tree(&dir);
    let blob = dir.path().join("store");
    let out = dir.path().join("out");
    let rt = runtime();

    let committed = rt.block_on(async {
        let storage =
            BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
                .await
                .expect("store");
        backup(&src, Options::default(), storage, None)
            .await
            .expect("backup")
    });

    rt.block_on(async {
        let storage = BlobStore::open_for_reading(&blob).await.expect("store");
        restore(&out, committed.snapshot, storage)
            .await
            .expect("restore");
    });

    for i in 0..FILES {
        let name = format!("f{i:05}.bin");
        let original = std::fs::read(src.join(&name)).expect("read source");
        let restored = std::fs::read(out.join(&name)).expect("read restore");
        assert_eq!(original, restored, "{name} did not survive the round trip");
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
    bench_backup(&mut criterion);
    bench_restore(&mut criterion);
    criterion.final_summary();
}
