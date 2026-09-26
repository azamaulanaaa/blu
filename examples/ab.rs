//! Throughput measurement that survives a shared machine.
//!
//! `cargo bench` (criterion) is the right tool for the component costs in
//! `benches/hotpath.rs`, and for the `concurrency_*` ratios in
//! `benches/throughput.rs`, which are measured inside a single run under
//! identical conditions. It is the wrong tool for answering "is this change
//! faster?", because its confidence interval only covers sampling noise within
//! one run.
//!
//! Measured on this machine, four cores with a background load average above 2:
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
//! the next run, and would have called that "no change". Anything under about
//! 20% is not measurable here by comparing two separate runs.
//!
//! Two things fix that, and both are in this file:
//!
//! * **Minimum, not mean.** Interference can only ever make a run slower, never
//!   faster, so the fastest of N runs is the closest observation to the
//!   uncontaminated cost. On the same measurements the minimum held steady
//!   across configurations (59.6 / 58.8 / 63.4 ms) where the mean swung 63.6 to
//!   84.4 ms.
//! * **Interleaving.** Variants alternate within one process rather than being
//!   measured in separate runs, so a slow patch from a neighbour lands on both
//!   of them instead of on whichever one happened to be running.
//!
//! And one thing that does not work, recorded here so nobody repeats it:
//! **pinning the CPU with `taskset` makes it worse.** Constraining the process to
//! a subset of the cores guarantees sharing those specific cores with whatever
//! else is on the box, and the scheduler's ability to drift onto a quiet core is
//! worth more than the migration it costs:
//!
//! ```text
//!                 max/min   median/min
//! unpinned          1.18x      1.04x
//! taskset -c 0-3    1.35x      1.23x
//! taskset -c 0-1    1.79x      1.33x
//! ```
//!
//! # Use
//!
//! Compare configurations, interleaved, in one process:
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

use lepatch::{
    command::{Options, backup},
    storage::BlobFileStorage,
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

/// Half of each file shared with every other, so deduplication has work to do
/// and the chunker sees a stream resembling a real tree.
fn build(dir: &std::path::Path) -> Vec<u8> {
    let src = dir.join("src");
    std::fs::create_dir_all(&src).expect("mkdir");

    let shared = noisy(FILE_BYTES / 2, 7);
    for i in 0..FILES {
        let mut bytes = shared.clone();
        bytes.extend_from_slice(&noisy(FILE_BYTES / 2, u64::try_from(1000 + i).unwrap_or(0)));
        std::fs::write(src.join(format!("f{i:05}.bin")), bytes).expect("write");
    }

    Vec::new()
}

/// One configuration's timings.
struct Samples {
    label: String,
    times: Vec<f64>,
}

impl Samples {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_owned(),
            times: Vec::with_capacity(ROUNDS),
        }
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
    /// A contamination indicator rather than a result: near 1.0 means a quiet
    /// machine, and anything much above it means the numbers below deserve
    /// scepticism no matter how they compare.
    fn contamination(&self) -> f64 {
        self.median() / self.min()
    }

    fn push(&mut self, ms: f64) {
        self.times.push(ms);
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

/// Time one backup into a store emptied beforehand.
async fn time_backup(src: &std::path::Path, blob: &std::path::Path, options: Options) -> f64 {
    // A fresh store each time, or the run would deduplicate against itself and
    // report a throughput nothing else would ever see.
    let _ = std::fs::remove_file(blob);

    let storage = BlobFileStorage::<true>::new(blob).await.expect("store");
    let started = Instant::now();
    backup(src, options, storage).await.expect("backup");
    started.elapsed().as_secs_f64() * 1000.0
}

/// Compare concurrency settings, interleaved so drift lands on all of them.
async fn compare(dir: &std::path::Path, src: &std::path::Path) {
    let settings = [1usize, 2, 4, 8];
    let mut samples: Vec<Samples> = settings
        .iter()
        .map(|c| Samples::new(&format!("concurrency {c}")))
        .collect();

    for _ in 0..ROUNDS {
        for (index, concurrency) in settings.iter().enumerate() {
            let blob = dir.join(format!("store{concurrency}.bin"));
            let ms = time_backup(
                src,
                &blob,
                Options {
                    concurrency: *concurrency,
                    ..Options::default()
                },
            )
            .await;
            samples[index].push(ms);
        }
    }

    let total = f64::from(u32::try_from(FILE_BYTES * FILES).unwrap_or(0)) / (1024.0 * 1024.0);
    let baseline = samples[0].min();

    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n  {FILES} files x {FILE_BYTES} bytes = {total:.0} MiB, {ROUNDS} interleaved rounds"
    );
    let _ = writeln!(out, "  load average: {}", load_average());
    for sample in &samples {
        sample.report(&mut out);
    }
    let _ = writeln!(out, "\n  throughput at the minimum:");
    for sample in &samples {
        let _ = writeln!(
            out,
            "  {:<22} {:>6.0} MiB/s   {:.2}x",
            sample.label,
            total / (sample.min() / 1000.0),
            baseline / sample.min()
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
async fn report(dir: &std::path::Path, src: &std::path::Path, label: &str, rounds: usize) {
    let blob = dir.join("store.bin");
    let mut samples = Samples::new(label);
    samples.times.reserve(rounds);

    for _ in 0..rounds {
        let ms = time_backup(src, &blob, Options::default()).await;
        samples.push(ms);
    }

    // A single parseable line, so a shell loop can alternate two builds and
    // compare the two minima afterwards.
    println!(
        "RESULT label={label} min_ms={:.1} median_ms={:.1} contamination={:.2}",
        samples.min(),
        samples.median(),
        samples.contamination()
    );
}

/// First field of `/proc/loadavg`, or `unknown`.
fn load_average() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

#[tokio::main]
async fn main() {
    let tmp = std::env::temp_dir().join("lepatch-ab");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).expect("mkdir");
    let _ = build(&tmp);
    let src = tmp.join("src");

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("compare") => compare(&tmp, &src).await,
        Some("report") => {
            let label = args.get(1).cloned().unwrap_or_else(|| "run".to_owned());
            let rounds = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(ROUNDS);
            report(&tmp, &src, &label, rounds).await;
        }
        _ => {
            println!("usage: ab compare | ab report <label> [rounds]");
        }
    }

    let _ = std::fs::remove_dir_all(&tmp);
}
