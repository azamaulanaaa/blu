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
//! # What this harness's own numbers are worth
//!
//! **The ratios, yes. The absolute throughput, no — it is well above the real
//! binary's, and the gap is unexplained.** On a 50 MiB tree, byte for byte the
//! same, with an identical `Options::default()` on four cores: the binary takes
//! 0.14s, this takes 0.36s, with about half again as much CPU. Ruled out by
//! measurement, not by argument: the fixture, the data, the store's location, the
//! number of trees left resident, the concurrency, the worker count, building
//! the fixture before any runtime exists, and letting writeback settle.
//!
//! Every variant is measured the same way here, so a comparison between two
//! configurations is sound and is what this file is for. But the throughput it
//! prints is not the product's. Time the binary for that.
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
///
/// xoshiro256\*\* rather than a counter, and that choice is load-bearing. Every
/// attempt at this that used a counter got the same wrong answer in a different
/// disguise: a counter's state is an *additive* function of the seed, so
/// `state == (seed + position) * K`, and any two `(seed, position)` pairs with
/// the same sum emit the same byte. The result is a tree whose files are all the
/// same stream at different offsets, which deduplicates away 90% of itself.
///
/// The failure is quiet in the worst way. The numbers look reasonable, the
/// backup round-trips perfectly, and the concurrency curve is smooth — it is
/// just a curve for a tree with almost nothing left to store. It survived a test
/// that compared two specific seeds, because the pairs that happen to misalign
/// are the ones a two-seed test looks at.
///
/// xoshiro's state is four words mixed by rotation and multiplication, so a seed
/// selects an unrelated point in the sequence rather than a position in it. The
/// test at the bottom of this file checks the property that actually matters —
/// that no chunk-sized run of the generated tree appears twice — rather than
/// checking a pair of seeds and hoping.
fn noisy(len: usize, seed: u64) -> Vec<u8> {
    let mut splitmix = seed;
    let mut word = move || {
        splitmix = splitmix.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = splitmix;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let [mut s0, mut s1, mut s2, mut s3] = [word(), word(), word(), word()];

    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let result = s1.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s1 << 17;

        s2 ^= s0;
        s3 ^= s1;
        s1 ^= s2;
        s0 ^= s3;
        s2 ^= t;
        s3 = s3.rotate_left(45);

        for byte in result.to_le_bytes() {
            if out.len() == len {
                break;
            }
            out.push(byte);
        }
    }
    out
}

/// Build a tree in which every file begins with the same `shared` bytes.
///
/// `shared` is a fraction, and it is the single most consequential thing about
/// this fixture, so it is a parameter rather than a constant.
///
/// It matters because deduplication moves work *out of the store*. Half-shared
/// files still have to be read and hashed in full, but only half their bytes
/// reach the store, so the store is a smaller share of the run and changes to it
/// look smaller than they are. Measured on this machine: removing the store's
/// write lock was worth 2.5x on a tree that shares nothing and 1.1x on one that
/// shares half — a real difference that a single fixture hides completely.
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
    /// Reported next to the wall time because the two answer different questions,
    /// and on a shared machine only one of them is about this program. A run that
    /// is short in wall time and long in CPU is compute-bound; a run that is long
    /// in both is doing too much work; a run that is long in wall and short in
    /// CPU is waiting for the neighbour, and no amount of tuning will show up.
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
    /// A contamination indicator rather than a result: near 1.0 means a quiet
    /// machine, and anything much above it means the numbers below deserve
    /// scepticism no matter how they compare.
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
    let _ = std::fs::remove_file(blob);

    let storage = BlobFileStorage::<true>::new(blob).await.expect("store");
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    backup(src, options, storage).await.expect("backup");
    let wall = started.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu_before;

    Timed {
        wall_ms: wall * 1000.0,
        cpu_ms: cpu * 1000.0,
    }
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
            let blob = stores.join(format!("store{concurrency}.bin"));
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

    // The size of the store the fastest setting produced, which is the honest
    // measure of how much work the store was handed. Everything else in the tree
    // was read and hashed but deduplicated away.
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
            "  {on_disk:.0} MiB reached the store; nothing was deduplicated away"
        );
    } else {
        let _ = writeln!(
            out,
            "  {on_disk:.0} MiB reached the store; the other {:.0} MiB was read and hashed, then \
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
    let blob = stores.join("store.bin");
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
/// too much work or merely waiting. Wall time on a shared box is mostly a
/// measure of the neighbour; CPU time is a measure of this program, and it is
/// stable enough to compare across runs even when wall time is not.
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // After the comm field, which may itself contain spaces.
    let Some(rest) = stat.rsplit_once(") ").map(|(_, r)| r) else {
        return 0.0;
    };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // utime is field 14 and stime field 15 of the whole line; `rest` starts at
    // field 3, so they are at indices 11 and 12.
    let ticks = |i: usize| -> f64 {
        fields
            .get(i)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    // _SC_CLK_TCK is 100 on every platform this runs on, and the alternative is
    // an FFI dependency for a diagnostic.
    (ticks(11) + ticks(12)) / 100.0
}

/// Synchronous on purpose.
///
/// `build` writes tens of megabytes and takes a couple of hundred milliseconds.
/// Under `#[tokio::main]` that runs *on a runtime worker thread*, which parks the
/// rest of the runtime and leaves it in a state that costs the measurement
/// afterwards: with the trees built this way the harness reported 2.6x the wall
/// time and 1.5x the CPU of the real binary, on identical bytes with identical
/// settings and an identical `Options::default()` — a difference no amount of
/// waiting or of data explains.
///
/// Building before a runtime exists is also simply the honest order: fixture
/// first, then measure.
fn main() {
    let tmp = std::env::temp_dir().join("lepatch-ab");
    let stores = std::env::temp_dir().join("lepatch-ab-stores");
    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::remove_dir_all(&stores);
    std::fs::create_dir_all(&tmp).expect("mkdir");
    std::fs::create_dir_all(&stores).expect("mkdir");

    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().cloned().unwrap_or_default();

    // Only the tree that is about to be measured gets built. Building both
    // leaves 100 MiB of tmpfs resident that nothing reads, and `/tmp` here is a
    // 2 GiB tmpfs shared with whatever else the machine is doing.
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
            // cross-checked against the real binary on the same bytes. Worth
            // having: the two disagreeing is how the broken generator was found,
            // and neither one alone would have shown it.
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
            // tree that shares nothing, and `report` exists to compare two
            // builds, so it should compare them where the difference shows.
            report(&stores, &tmp.join("src-unique"), &label, rounds).await;
        }
    });

    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::remove_dir_all(&stores);
}
#[cfg(test)]
mod fixture_tests {
    use super::*;

    /// The generated tree must not contain a run the chunker could deduplicate.
    ///
    /// This fixture was silently wrong for its whole life, three different ways.
    /// The generator was a function of `position + seed`, then of
    /// `(seed + position) * K`, and both make every file in the tree the same
    /// stream at a different offset. Deduplication threw away over 90% of the
    /// tree, and the benchmark reported a concurrency curve for a tree with
    /// almost nothing left to store.
    ///
    /// It survived a test comparing two specific seeds, because the seed pairs
    /// that happen to misalign are the ones such a test looks at. 200 files have
    /// 19,900 pairs and only two were checked. So this checks the property the
    /// pipeline actually depends on — is any chunk-sized run repeated anywhere in
    /// the tree — over every file the benchmark builds, and it does not care how
    /// the bytes are generated.
    #[test]
    fn no_chunk_sized_run_is_repeated_in_the_tree() {
        /// Comfortably above the chunker's minimum, so a real chunk would be
        /// collapsed and this catches it.
        const WINDOW: usize = 8 * 1024;

        let mut seen: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();
        let mut windows = 0usize;

        for i in 0..FILES {
            let bytes = noisy(FILE_BYTES, u64::try_from(1000 + i).expect("fits in u64"));
            for at in (0..bytes.len()).step_by(WINDOW) {
                let mut key = [0u8; 16];
                key.copy_from_slice(&bytes[at..at + 16]);
                windows += 1;
                assert!(
                    seen.insert(key),
                    "file {i} repeats a {WINDOW}-byte window at offset {at}; the generator is \
                     producing overlapping copies of one stream, so the tree deduplicates \
                     against itself and the benchmark measures a reader with no store work"
                );
            }
        }

        // A fixture that dedupes is not a slow fixture, it is a different one.
        assert_eq!(windows, FILES * (FILE_BYTES / WINDOW));
    }

    /// Two different seeds must not produce the same bytes at a different offset.
    ///
    /// Kept alongside the whole-tree check because it fails with a far more
    /// specific message, and because it is the cheap one to run while iterating
    /// on a generator.
    #[test]
    fn different_seeds_share_no_long_run() {
        let a = noisy(64 * 1024, 1000);
        let b = noisy(64 * 1024, 1001);

        // A content-defined chunker can only deduplicate runs of at least a
        // minimum chunk, so anything this long would be collapsed by the real
        // pipeline. If two seeds have no run this long, they will not deduplicate.
        const MIN_CHUNK: usize = 256;
        let longest = longest_common_run(&a, &b);
        assert!(
            longest < MIN_CHUNK,
            "seeds 1000 and 1001 share a {longest}-byte run; the generator is producing \
             shifted copies of one stream"
        );

        // And they should differ nearly everywhere, which a weak mixer would not.
        let differing = a.iter().zip(&b).filter(|(x, y)| x != y).count();
        let ratio = f64::from(u32::try_from(differing).expect("fits in u32"))
            / f64::from(u32::try_from(a.len()).expect("fits in u32"));
        assert!(
            (0.9..1.0).contains(&ratio),
            "seeds 1000 and 1001 differ in only {:.1}% of positions, which is not random",
            ratio * 100.0
        );
    }

    /// No two files may share a run at *any* offset.
    ///
    /// The aligned-window check above cannot see a shifted overlap: if file 1 is
    /// file 0 shifted by one byte, every 8 KiB window of one lines up with the
    /// middle of a window of the other and they all differ. That is precisely
    /// the case that got through — a generator whose state is additive in the
    /// seed produces files that are each other at an offset, and the whole tree
    /// deduplicated to a tenth of its size while every aligned window was unique.
    ///
    /// Smaller files than the benchmark uses, because this is a pairwise search
    /// and the failure mode it looks for is total rather than marginal. Anything
    /// the chunker could collapse is far longer than [`MIN_CHUNK`].
    #[test]
    fn no_two_files_overlap_at_any_offset() {
        const SIZE: usize = 16 * 1024;
        const MIN_CHUNK: usize = 256;

        let first = noisy(SIZE, 1000);
        for i in 1..FILES {
            let other = noisy(SIZE, u64::try_from(1000 + i).expect("fits in u64"));
            let longest = longest_common_run(&first, &other);
            assert!(
                longest < MIN_CHUNK,
                "file 0 and file {i} share a {longest}-byte run at some offset; the tree \
                 deduplicates against itself and the benchmark is measuring a reader with \
                 almost no store work to do"
            );
        }
    }

    /// Longest run of bytes appearing in both slices, at any alignment.
    ///
    /// Indexing `b` by a 16-byte gram and extending from each hit, so this is
    /// linear rather than quadratic. The quadratic version took 43 seconds, which
    /// is too slow to be a test anybody keeps.
    fn longest_common_run(a: &[u8], b: &[u8]) -> usize {
        const GRAM: usize = 16;
        /// Stop early: the assertion is about runs far below this.
        const CEILING: usize = 4096;

        if a.len() < GRAM || b.len() < GRAM {
            return 0;
        }

        let gram = |bytes: &[u8], at: usize| {
            let mut key = [0u8; GRAM];
            key.copy_from_slice(&bytes[at..at + GRAM]);
            key
        };

        let mut index: std::collections::HashMap<[u8; GRAM], Vec<usize>> =
            std::collections::HashMap::new();
        for at in 0..=b.len() - GRAM {
            index.entry(gram(b, at)).or_default().push(at);
        }

        let mut longest = 0;
        for at in 0..=a.len() - GRAM {
            let Some(hits) = index.get(&gram(a, at)) else {
                continue;
            };

            for &start in hits {
                let mut back = 0;
                while at > back && start > back && a[at - back - 1] == b[start - back - 1] {
                    back += 1;
                }
                let mut fwd = 0;
                while at + GRAM + fwd < a.len()
                    && start + GRAM + fwd < b.len()
                    && a[at + GRAM + fwd] == b[start + GRAM + fwd]
                {
                    fwd += 1;
                }

                longest = longest.max(back + GRAM + fwd);
                if longest >= CEILING {
                    return longest;
                }
            }
        }
        longest
    }
}
