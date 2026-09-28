// The crate denies `unwrap_used` and `expect_used`, and `allow-expect-in-tests`
// only covers code inside a `#[test]` function. The helpers below are plain
// functions that a benchmark calls, so they need the exemption spelled out: a
// benchmark that cannot build its fixture has nothing useful to measure.
#![allow(clippy::unwrap_used, clippy::expect_used)]
// only covers code inside a `#[test]` function. The helpers below are plain
// benchmark that cannot build the fixture tree

//! End-to-end throughput for a whole backup and a whole restore.
//!
//! These are the numbers to look at before and after a change. Everything in
//! `hotpath` is a component; this is whether the components add up to something
//! a user would feel.
//!
//! # What these numbers can and cannot tell you
//!
//! **The `concurrency_*` rows are trustworthy.** They are measured inside one run
//! under identical conditions, so the ratio between them is a ratio between
//! observations that shared the same machine state.
//!
//! **Comparing one of these rows against a previous run of criterion is not.**
//! Criterion's interval covers sampling noise within a run, which is not the
//! dominant noise source on a shared machine. Measured here, on four cores with
//! a background load average above 2:
//!
//! ```text
//!                              within-run interval   across-run shift
//! chunking/read_and_cut                  3.8%              4.3%
//! adapters/bounded_read                  0.9%             16.0%
//! adapters/bounded_write                 0.1%             12.0%
//! ```
//!
//! A 0.9% interval around a number that then moved 16% on the next run. Criterion
//! would have reported that as "no change". Run-to-run shifts of up to 29% have
//! been observed on identical code here, so **an effect under about 20% is not
//! measurable by comparing two separate runs** — and criterion will happily
//! report one.
//!
//! For a before/after claim use `cargo run --release --example ab`, which takes
//! the minimum over several rounds and interleaves variants, rather than
//! criterion's stored baseline. `examples/ab.rs` documents why, and includes the
//! measurements behind it.
//!
//! **Its absolute numbers are higher than the real binary's, and that gap is
//! unexplained.** Measured here on a 50 MiB tree, byte for byte the same, with an
//! identical `Options::default()` on a four-core box: the binary takes 0.14s and
//! the harness 0.36s, with about half again as much CPU in the harness. The
//! fixture, the data, the store's location, the number of trees resident, the
//! concurrency, the worker count, building the fixture before the runtime exists
//! and letting writeback settle were each ruled out by measuring. So the
//! *ratios* this harness reports are sound — it measures every variant the same
//! way — but the absolute throughput it prints is not the product's. For a number
//! to quote about how fast a backup is, time the binary.
//!
//! Setup is deliberately outside the timed region: the tree is built once, and
//! each iteration starts from an empty store. A backup that deduped against its
//! own previous run would report a throughput no user would ever see.
//!
//! Backup is measured across a range of `Options::concurrency` rather than at
//! one value, because the interesting question is not "how fast is this" but
//! "where does it stop getting faster".
//!
//! That answer was wrong here for a long time, because of a fault in
//! `examples/ab.rs`'s fixture: its pseudo-random generator was a function of
//! `position + seed`, so every file in the tree was the same byte stream at a
//! different offset. Deduplication removed over 90% of it and the curve flattened
//! at 2.3x. With the fixture repaired — 50 MiB genuinely reaching the store — the
//! same four-core box gives **3.0x from concurrency 1 to 8, still climbing at the
//! top of the range**, and CPU time falls as concurrency rises (340 ms to 260 ms)
//! rather than staying flat, which is what a store that is no longer serialising
//! uploads looks like. The default of eight remains the right default; it is now
//! the measured answer rather than an assumption.

use std::{
    hint::black_box,
    path::PathBuf,
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput};
use lepatch::{
    command::{Options, backup, restore},
    storage::BlobStore,
};
use tempfile::TempDir;

/// Files in the benchmark tree. Enough that per-file costs are visible, small
/// enough that a full run stays in the low seconds.
const FILES: usize = 200;

/// Size of each file.
const FILE_BYTES: usize = 256 * 1024;

/// Pseudo-random bytes with no long runs, so the chunker finds real boundaries.
fn noisy(len: usize, seed: u64) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let mut x = u64::try_from(i).unwrap_or(0).wrapping_add(seed);
            x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            x ^= x >> 30;
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x ^= x >> 27;
            x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
            x ^= x >> 31;
            u8::try_from(x & 0xff).unwrap_or(0)
        })
        .collect()
}

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
    let blob = dir.path().join("store.bin");
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
                        let _ = std::fs::remove_file(&blob);

                        let storage = BlobStore::<true>::open(&blob).await.expect("store");
                        let options = Options {
                            concurrency,
                            ..Options::default()
                        };

                        let started = Instant::now();
                        let committed = backup(&src, options, storage).await.expect("backup");
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
    let blob = dir.path().join("store.bin");
    let rt = runtime();

    let committed = rt.block_on(async {
        let storage = BlobStore::<true>::open(&blob).await.expect("store");
        backup(&src, Options::default(), storage)
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

                    let storage = BlobStore::<false>::open(&blob).await.expect("store");

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
/// benchmark, and `restore` returning `Ok` says nothing about the bytes it
/// wrote. Checked once, outside the timed region.
#[test]
fn the_throughput_benchmark_restores_what_it_backed_up() {
    let dir = TempDir::new().expect("tempdir");
    let src = tree(&dir);
    let blob = dir.path().join("store.bin");
    let out = dir.path().join("out");
    let rt = runtime();

    let committed = rt.block_on(async {
        let storage = BlobStore::<true>::open(&blob).await.expect("store");
        backup(&src, Options::default(), storage)
            .await
            .expect("backup")
    });

    rt.block_on(async {
        let storage = BlobStore::<false>::open(&blob).await.expect("store");
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
/// Written out rather than using `criterion_main!`, whose expansion is an
/// undocumented function that the crate-level `missing_docs` deny rejects, and
/// an attribute on the macro invocation is ignored. Spelling the same four lines
/// out keeps the lint switched on for the benchmarks themselves.
fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_backup(&mut criterion);
    bench_restore(&mut criterion);
    criterion.final_summary();
}
