// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Throughput measurement that survives a shared machine.
//!
//! `cargo bench` (criterion) is the right tool for the component costs in
//! `benches/hotpath.rs`.
//! It is also the right tool for the `concurrency_*` ratios in
//! `benches/throughput.rs`, which are measured inside a single run under
//! identical conditions.
//! It is the wrong tool for answering "is this change faster?".
//! Its confidence interval only covers sampling noise within one run.
//!
//! Measured on this machine, four cores with a background load average above 2.
//!
//! ```text
//!                              within-run interval   across-run shift
//! chunking/read_and_cut                  3.8%              4.3%
//! hashing/blake3_16KiB                   1.2%              3.1%
//! adapters/bounded_read                  0.9%             16.0%
//! adapters/bounded_write                 0.1%             12.0%
//! ```
//!
//! So criterion reported a 0.9% interval around a number that then moved 16% on
//! the next run.
//! Criterion would have called that "no change".
//! Anything under about 20% is not measurable here by comparing two separate
//! runs.
//!
//! Two things fix that. Both are in this file.
//!
//! * **Minimum, not mean.** Interference can only ever make a run slower, never
//!   faster. The fastest of N runs is the closest observation to the
//!   uncontaminated cost. On the same measurements the minimum held steady
//!   across configurations (59.6 / 58.8 / 63.4 ms) where the mean swung 63.6 to
//!   84.4 ms.
//! * **Interleaving.** Variants alternate within one process. They are not
//!   measured in separate runs. A slow patch from a neighbour then lands on both
//!   of them, not on the one that happened to be running.
//!
//! And one thing does not work. It is recorded here so nobody repeats it.
//! **pinning the CPU with `taskset` makes it worse.** Constraining the process to
//! a subset of the cores forces sharing. Those specific cores then run the other
//! programs too.
//! The scheduler can drift onto a quiet core. That move is worth more than the
//! migration it costs.
//!
//! ```text
//!                 max/min   median/min
//! unpinned          1.18x      1.04x
//! taskset -c 0-3    1.35x      1.23x
//! taskset -c 0-1    1.79x      1.33x
//! ```
//!
//! # What this harness's own numbers are worth
//!
//! **The ratios, yes. The absolute throughput, no. It is well above the
//! real binary's. The gap is unexplained.** On a 50 MiB tree, byte for byte
//! the same, with an identical `Options::default()` on four cores: the binary
//! takes 0.14s. This takes 0.36s, with about half again as much CPU.
//! Ruled out by measurement, not by argument: the fixture, the data, the store's
//! location, and the number of trees left resident. Ruled out as well: the
//! concurrency, the worker count, building the fixture before any runtime exists,
//! and letting writeback settle.
//!
//! Every variant is measured the same way here, so a comparison between two
//! configurations is sound and is what this file is for. But the throughput it
//! prints is not the product's. Time the binary for that.
//!
//! # Use
//!
//! Compare configurations, interleaved, in one process.
//!
//! ```console
//! $ cargo run --release --example ab -- compare
//! ```
//!
//! Or measure one configuration and print a single number, for a shell loop
//! that alternates two builds of the source:
//!
//! ```console
//! $ cargo run --release --example ab -- report before 9
//! $ git stash && cargo build --release --example ab
//! $ cargo run --release --example ab -- report after 9
//! ```

#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::{fmt::Write as _, time::Instant};

use blulib::{
    command::{Options, backup},
    layout::IndexSet,
    storage::BlobStore,
};

/// Files in the tree. Enough for per-file costs to show, small enough that a
/// round of interleaved measurements finishes in seconds.
const FILES: usize = 200;

/// Size of each file.
const FILE_BYTES: usize = 256 * 1024;

/// Rounds per variant. Nine is the smallest count where the minimum has
/// stopped moving between rounds on this machine.
const ROUNDS: usize = 9;

/// Report an effect smaller than this as unmeasurable rather than as a number.
const NOISE_FLOOR: f64 = 0.20;

// The byte generator lives in its own file so `tests/fixtures.rs` can check the
// properties that guard it.
// An example is the first thing anyone deletes, and three property tests used to
// live here under `#[cfg(test)]`.
// `cargo test` does not run that target.
#[path = "../tests/fixtures/ab.rs"]
mod fixture;

use fixture::noisy;

/// Build a tree in which every file begins with the same `shared` bytes.
///
/// `shared` is a fraction, and it is the single most consequential thing about
/// this fixture, so it is a parameter rather than a constant.
///
/// It matters because deduplication moves work *out of the store*.
/// Half-shared files still have to be read and hashed in full, but only half
/// their bytes reach the store.
/// So the store is a smaller share of the run and changes to it look smaller than
/// they are.
/// Measured on this machine: removing the store's write lock was worth 2.5x on a
/// tree that shares nothing and 1.1x on one that shares half.
/// That is a real difference that a single fixture hides completely.
///
/// So the curve is measured on both, and the share is printed next to it.
fn build(dir: &std::path::Path, shared: usize) -> std::path::PathBuf {
    let src = dir.join(if shared > 0 {
        "src-shared"
    } else {
        "src-unique"
    });
    std::fs::create_dir_all(&src).expect("mkdir");

    let shared_bytes = noisy(shared, 7);

    for i in 0..FILES {
        let mut bytes = shared_bytes.clone();
        bytes.extend_from_slice(&noisy(
            FILE_BYTES - shared,
            u64::try_from(1000 + i).unwrap_or(0),
        ));
        std::fs::write(src.join(format!("f{i:05}.bin")), bytes).expect("write");
    }

    src
}

/// One configuration's timings.
struct Samples {
    label: String,
    times: Vec<f64>,
    cpu: Vec<f64>,
}

impl Samples {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_owned(),
            times: Vec::with_capacity(ROUNDS),
            cpu: Vec::with_capacity(ROUNDS),
        }
    }

    /// CPU seconds of the run that took least wall time.
    ///
    /// Reported next to the wall time because the two answer different questions.
    /// On a shared machine only one of them is about this program.
    /// A run that is short in wall time and long in CPU is compute-bound.
    /// A run that is long in both is doing too much work.
    /// A run that is long in wall and short in CPU is waiting for the neighbour.
    /// No amount of tuning changes that.
    fn cpu_min(&self) -> f64 {
        self.cpu.iter().copied().fold(f64::MAX, f64::min)
    }

    /// The fastest run: the least contaminated estimate of the real cost.
    fn min(&self) -> f64 {
        self.times.iter().copied().fold(f64::MAX, f64::min)
    }

    fn median(&self) -> f64 {
        let mut sorted = self.times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        sorted[sorted.len() / 2]
    }

    /// How much slower the median was than the minimum.
    ///
    /// A contamination indicator rather than a result.
    /// Near 1.0 means a quiet machine.
    /// Anything much above it means the numbers below deserve scepticism no matter
    /// how they compare.
    fn contamination(&self) -> f64 {
        self.median() / self.min()
    }

    fn push(&mut self, timed: &Timed) {
        self.times.push(timed.wall_ms);
        self.cpu.push(timed.cpu_ms);
    }

    fn report(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "  {:<22} min {:>7.1} ms   median {:>7.1} ms   median/min {:.2}x",
            self.label,
            self.min(),
            self.median(),
            self.contamination()
        );
    }
}

/// Time one backup into a store emptied beforehand, in wall and CPU seconds.
async fn time_backup(src: &std::path::Path, blob: &std::path::Path, options: Options) -> Timed {
    // A fresh store each time, or the run would deduplicate against itself and
    // report a throughput nothing else would ever see.
    for segment in 0..64 {
        let _ = std::fs::remove_file(store_segment(blob, segment));
    }

    let storage =
        BlobStore::builder(blob).open_writer().await
            .expect("store");
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    backup(src, options, storage, None).await.expect("backup");
    let wall = started.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu_before;

    Timed {
        wall_ms: wall * 1000.0,
        cpu_ms: cpu * 1000.0,
    }
}

/// The path of segment `segment` of the set at `base`.
///
/// A set is several files, and a benchmark that measures one backup wants a clean
/// one every round.
/// So it has to remove all of them, not just the first.
fn store_segment(base: &std::path::Path, segment: u32) -> std::path::PathBuf {
    IndexSet::new(base).segment_path(segment)
}

/// One measurement, in both clocks.
struct Timed {
    wall_ms: f64,
    cpu_ms: f64,
}

/// Compare concurrency settings, interleaved so drift lands on all of them.
async fn compare(dir: &std::path::Path, stores: &std::path::Path) {
    println!("\n=== nothing shared: every byte reaches the store ===");
    curve(stores, &dir.join("src-unique"), 0).await;
    println!("\n=== half of every file shared: half the bytes reach the store ===");
    curve(stores, &dir.join("src-shared"), FILE_BYTES / 2).await;
}

/// Bytes as MiB, split so no cast from `u64` is needed.
fn mib(bytes: u64) -> f64 {
    let whole = bytes / (1024 * 1024);
    let rest = bytes % (1024 * 1024);
    f64::from(u32::try_from(whole).unwrap_or(u32::MAX))
        + f64::from(u32::try_from(rest).unwrap_or(u32::MAX)) / (1024.0 * 1024.0)
}

/// Time one concurrency curve over one tree, and say how much work the store
/// actually got.
async fn curve(stores: &std::path::Path, src: &std::path::Path, shared: usize) {
    let settings = [1usize, 2, 4, 8];
    let mut samples: Vec<Samples> = settings
        .iter()
        .map(|c| Samples::new(&format!("concurrency {c}")))
        .collect();

    for _ in 0..ROUNDS {
        for (index, concurrency) in settings.iter().enumerate() {
            let blob = stores.join(format!("store{concurrency}"));
            let ms = time_backup(
                src,
                &blob,
                Options {
                    concurrency: *concurrency,
                    ..Options::default()
                },
            )
            .await;
            samples[index].push(&ms);
        }
    }

    let total = mib(u64::try_from(FILE_BYTES * FILES).expect("fits in u64"));
    let baseline = samples[0].min();

    // The size of the store the fastest setting produced.
    // That is the honest measure of how much work the store was handed.
    // Everything else in the tree was read and hashed but deduplicated away.
    let store_bytes = std::fs::metadata(stores.join("store1.bin")).map_or(0, |m| m.len());
    let on_disk = mib(store_bytes);

    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n  {FILES} files x {FILE_BYTES} bytes = {total:.0} MiB, {ROUNDS} interleaved rounds, \
         {:.0}% of each file shared",
        100.0 * f64::from(u32::try_from(shared).expect("fits in u32"))
            / f64::from(u32::try_from(FILE_BYTES).expect("fits in u32"))
    );
    let _ = writeln!(out, "  load average: {}", load_average());
    if on_disk >= total * 0.99 {
        let _ = writeln!(
            out,
            "  {on_disk:.0} MiB reached the store. Nothing was deduplicated away"
        );
    } else {
        let _ = writeln!(
            out,
            "  {on_disk:.0} MiB reached the store. The other {:.0} MiB was read and hashed, then \
             deduplicated away",
            total - on_disk
        );
    }
    for sample in &samples {
        sample.report(&mut out);
    }
    let _ = writeln!(out, "\n  throughput at the minimum:");
    for sample in &samples {
        let _ = writeln!(
            out,
            "  {:<22} {:>6.0} MiB/s   {:.2}x   cpu {:>6.0} ms",
            sample.label,
            total / (sample.min() / 1000.0),
            baseline / sample.min(),
            sample.cpu_min()
        );
    }
    let _ = writeln!(
        out,
        "\n  Differences below {:.0}% are inside this machine's noise and are not results.",
        NOISE_FLOOR * 100.0
    );
    print!("{out}");
}

/// Measure one configuration and print a single number, for external
/// interleaving of two builds of the source.
async fn report(stores: &std::path::Path, src: &std::path::Path, label: &str, rounds: usize) {
    let blob = stores.join("store");
    let mut samples = Samples::new(label);
    samples.times.reserve(rounds);

    for _ in 0..rounds {
        let ms = time_backup(src, &blob, Options::default()).await;
        samples.push(&ms);
    }

    // A single parseable line, so a shell loop can alternate two builds and
    // compare the two minima afterwards.
    println!(
        "RESULT label={label} min_ms={:.1} median_ms={:.1} contamination={:.2} stored_mib={:.0} \
         cpu_ms={:.0}",
        samples.min(),
        samples.median(),
        samples.contamination(),
        mib(std::fs::metadata(&blob).map_or(0, |m| m.len())),
        samples.cpu_min()
    );
}

/// First field of `/proc/loadavg`, or `unknown`.
fn load_average() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

/// CPU seconds this process has used, user plus system.
///
/// The counterpart to wall time, and the thing that says whether a run is doing
/// too much work or merely waiting.
/// Wall time on a shared box is mostly a measure of the neighbour.
/// CPU time is a measure of this program.
/// CPU time is stable enough to compare across runs even when wall time is not.
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // After the comm field, which may itself contain spaces.
    let Some(rest) = stat.rsplit_once(") ").map(|(_, r)| r) else {
        return 0.0;
    };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // utime is field 14 and stime field 15 of the whole line.
    // `rest` starts at field 3, so they are at indices 11 and 12.
    let ticks = |i: usize| -> f64 {
        fields
            .get(i)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    // _SC_CLK_TCK is 100 on every platform this runs on.
    // The alternative is an FFI dependency for a diagnostic.
    (ticks(11) + ticks(12)) / 100.0
}

/// Synchronous on purpose.
///
/// `build` writes tens of megabytes and takes a couple of hundred milliseconds.
/// Under `#[tokio::main]` that runs *on a runtime worker thread*.
/// That parks the rest of the runtime and leaves it in a state that costs the
/// measurement afterwards.
/// With the trees built this way the harness reported 2.6x the wall time and
/// 1.5x the CPU of the real binary. It used identical bytes, identical settings,
/// and an identical `Options::default()`.
/// No amount of waiting or of data explains that difference.
///
/// Building before a runtime exists is also simply the honest order: fixture
/// first, then measure.
fn main() {
    let tmp = std::env::temp_dir().join("blu-ab");
    let stores = std::env::temp_dir().join("blu-ab-stores");
    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::remove_dir_all(&stores);
    std::fs::create_dir_all(&tmp).expect("mkdir");
    std::fs::create_dir_all(&stores).expect("mkdir");

    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().cloned().unwrap_or_default();

    // Only the tree that is about to be measured gets built.
    // Building both leaves 100 MiB of tmpfs resident that nothing reads.
    // `/tmp` here is a 2 GiB tmpfs shared with whatever else the machine is
    // doing.
    match command.as_str() {
        "compare" => {
            build(&tmp, 0);
            build(&tmp, FILE_BYTES / 2);
        }
        "report" => {
            build(&tmp, 0);
        }
        "gen" => {
            // Build the trees and leave them, so the in-process harness can be
            // cross-checked against the real binary on the same bytes.
            // Worth having: the two disagreeing is how the broken generator was
            // found, and neither one alone would have shown it.
            let out = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| tmp.to_string_lossy().into_owned());
            let out = std::path::PathBuf::from(out);
            let _ = std::fs::remove_dir_all(&out);
            std::fs::create_dir_all(&out).expect("mkdir");
            let unique = build(&out, 0);
            let shared = build(&out, FILE_BYTES / 2);
            println!("{}", unique.display());
            println!("{}", shared.display());
            return;
        }
        _ => {
            println!("usage: ab compare | ab gen <dir> | ab report <label> [rounds]");
            return;
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async {
        if command == "compare" {
            compare(&tmp, &stores).await;
        } else {
            let label = args.get(1).cloned().unwrap_or_else(|| "run".to_owned());
            let rounds = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(ROUNDS);
            // The unique tree: a store change is worth about twice as much on a
            // tree that shares nothing.
            // `report` exists to compare two builds, so it should compare them
            // where the difference shows.
            report(&stores, &tmp.join("src-unique"), &label, rounds).await;
        }
    });

    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::remove_dir_all(&stores);
}
