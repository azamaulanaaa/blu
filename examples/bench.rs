//! Measure two binaries against each other.
//!
//! The question is "is this build faster than that one?".
//! `cargo bench` cannot answer it.
//! Criterion measures one build at a time.
//! A busy machine gives two runs of the same build numbers that differ by more
//! than the change under test.
//! `examples/ab.rs` records the numbers for this machine: a run that moves by
//! 16% between runs means anything under about 20% is not measurable here.
//!
//! So this example measures the two binaries together, in one run.
//! It interleaves them.
//! One round runs the base binary and then the head binary.
//! The next round runs the head binary first.
//! A slow patch from a neighbour then lands on both arms instead of on the one
//! that happened to be running.
//!
//! # Use
//!
//! ```console
//! $ cargo run --release --example bench -- \
//!     --base ./blu-base --head ./blu-head -- blu backup source store
//! ```
//!
//! The first word of the command names the program.
//! The example runs the base binary and the head binary in its place.
//! The command runs in the current directory.
//!
//! # What a run records
//!
//! Every run records the wall time, the CPU time and the peak resident memory
//! of the child.
//! The wall time comes from `std::time::Instant` around the child.
//! The other two come from the usage of the child itself.
//! `libc` has `wait4`, and `wait4` returns the usage of one child.
//! The usage of a child is gone the moment the parent waits for it, so
//! `/proc` cannot answer the question.
//! That is the only reason this example calls `libc`.
//!
//! # The one unsafe call
//!
//! `wait4` is an unsafe function.
//! This package sets `unsafe_code` to `deny`, so this example has to allow
//! unsafe code to call it.
//! Only this example allows it, and it makes one call.
//! The library and the binary allow nothing.
//!
//! # What a run prints
//!
//! A markdown table with one row per measurement.
//! The table is what goes into a commit message.
//! The host follows the table, so a reader can see whether the machine was
//! busy.
//! The full data goes into `reports/bench/`, which the repository ignores.
//!
//! # The verdict
//!
//! The verdict is one of four words: `head faster`, `head slower`,
//! `no difference` or `unmeasurable`.
//! The noise floor decides between the first three and the last one.
//! The sign count decides between the first three.
//! A run that is faster in four rounds and slower in five is a coin flip.
//!
//! # The noise floor
//!
//! The floor is the noise of the machine, measured in the same run.
//! Three rounds put the same binary in both arms, and the paired differences
//! of those rounds are the noise.
//! The floor is the largest of those differences.
//! An effect below the floor is not a result, because the machine reached
//! that difference on its own.
//! The noise rounds run inside the loop of the arms, so the floor comes from
//! the same conditions as the effect.

// This example calls one unsafe function: `libc::wait4`, which returns the
// usage of one child process.
// The measurement needs that call, and the package sets `unsafe_code` to
// `deny` so that this file has to say so in one place.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used, unsafe_code)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// The rounds per arm. Nine is the count `examples/ab.rs` settled on.
const ROUNDS: usize = 9;

/// The rounds of the noise floor. Three is enough to see the spread.
const FLOOR_ROUNDS: usize = 3;

/// The rounds the head arm must win by before the verdict names a direction.
const MARGIN: usize = 2;

/// The usage text of this example.
const USAGE: &str = "\
usage: bench --base PATH --head PATH [--rounds N] -- COMMAND [ARGUMENT...]
  --base PATH   the binary of the base arm.
  --head PATH   the binary of the head arm.
  --rounds N    the rounds per arm, 9 by default.
  -- COMMAND    the program and its arguments, the binary runs in its place.";

/// The three things a run records.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// The wall time of the run.
    Wall,
    /// The CPU time of the child.
    Cpu,
    /// The peak resident memory of the child.
    Rss,
}

impl Kind {
    /// The name of the measurement, with its unit.
    fn label(self) -> &'static str {
        match self {
            Kind::Wall => "wall time (ms)",
            Kind::Cpu => "cpu time (ms)",
            Kind::Rss => "peak resident memory (kB)",
        }
    }

    /// The value of this measurement in one run.
    fn value(self, sample: &Sample) -> Option<f64> {
        match self {
            Kind::Wall => Some(sample.wall_ms),
            Kind::Cpu => sample.cpu_ms,
            Kind::Rss => sample.rss_kb,
        }
    }

    /// Every kind, in the order the table lists them.
    fn all() -> [Kind; 3] {
        [Kind::Wall, Kind::Cpu, Kind::Rss]
    }
}

/// One run of one binary.
struct Sample {
    /// The wall time of the run, in milliseconds.
    wall_ms: f64,
    /// The CPU time of the child, in milliseconds.
    cpu_ms: Option<f64>,
    /// The peak resident memory of the child, in KiB.
    rss_kb: Option<f64>,
}

/// What the command line asked for.
struct Options {
    /// The binary of the base arm.
    base: PathBuf,
    /// The binary of the head arm.
    head: PathBuf,
    /// The rounds per arm.
    rounds: usize,
    /// The program and its arguments.
    command: Vec<String>,
}

/// What the operating system reports about a child that ran.
struct Usage {
    /// The CPU time of the child, in milliseconds.
    cpu_ms: Option<f64>,
    /// The peak resident memory of the child, in KiB.
    rss_kb: Option<f64>,
}

/// The paired comparison of one measurement.
struct Measurement {
    /// The measurement itself.
    kind: Kind,
    /// The values of the base arm, one per round.
    base: Vec<f64>,
    /// The values of the head arm, one per round.
    head: Vec<f64>,
    /// The paired difference of each round, as a percentage of the base.
    diffs: Vec<f64>,
    /// The median of the base arm.
    base_median: f64,
    /// The median of the head arm.
    head_median: f64,
    /// The minimum of the base arm.
    base_min: f64,
    /// The minimum of the head arm.
    head_min: f64,
    /// The median of the paired differences, as a percentage.
    median_diff: f64,
    /// The noise floor, as a percentage.
    floor: f64,
    /// The rounds the head arm won.
    head_wins: usize,
    /// The rounds the base arm won.
    base_wins: usize,
    /// The verdict of this measurement.
    verdict: &'static str,
}

/// The paired differences of the noise rounds, for one measurement.
struct Noise {
    /// The measurement itself.
    kind: Kind,
    /// The paired difference of each noise round, as a percentage.
    diffs: Vec<f64>,
}

/// The state of the machine around the run.
struct Host {
    /// The number of cores.
    cores: usize,
    /// The load average before the run.
    load_before: String,
    /// The load average after the run.
    load_after: String,
    /// The free memory, in KiB.
    free_memory_kb: Option<u64>,
    /// True when the machine had more work than cores.
    noisy: bool,
}

/// Everything the run measured, ready to print and to save.
struct Report {
    /// What the command line asked for.
    options: Options,
    /// The hash of the base binary.
    base_hash: String,
    /// The hash of the head binary.
    head_hash: String,
    /// The paired comparison of each measurement.
    measurements: Vec<Measurement>,
    /// The noise floor of each measurement.
    noise: Vec<Noise>,
    /// The state of the machine.
    host: Host,
}

fn main() {
    if let Err(message) = run() {
        eprintln!("{message}");
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
}

/// Do the whole run, from the command line to the report file.
fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let options = parse(&args)?;
    check_file(&options.base, "base")?;
    check_file(&options.head, "head")?;
    if same_file(&options.base, &options.head) {
        return Err("the base binary and the head binary are the same file".to_owned());
    }

    let base_hash = hash_of(&options.base)?;
    let head_hash = hash_of(&options.head)?;
    let load_before = load_average();

    let arms = run_arms(
        &options.base,
        &options.head,
        &options.command,
        options.rounds,
    )?;
    let noise = noise_of(&arms);
    let load_after = load_average();

    let report = Report {
        options,
        base_hash,
        head_hash,
        measurements: measurements(&arms.base, &arms.head, &noise),
        noise,
        host: Host::read(&load_before, &load_after),
    };

    print_report(&report);
    write_report(&report)?;
    Ok(())
}

/// Read the command line.
fn parse(args: &[String]) -> Result<Options, String> {
    let mut base: Option<PathBuf> = None;
    let mut head: Option<PathBuf> = None;
    let mut rounds = ROUNDS;
    let mut command: Vec<String> = Vec::new();

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].clone();
        index += 1;
        match arg.as_str() {
            "--base" => base = Some(PathBuf::from(take(args, &mut index, "--base")?)),
            "--head" => head = Some(PathBuf::from(take(args, &mut index, "--head")?)),
            "--rounds" => {
                let raw = take(args, &mut index, "--rounds")?;
                rounds = raw
                    .parse::<usize>()
                    .map_err(|_| format!("--rounds needs a count, not {raw}"))?;
                if rounds < 2 {
                    return Err("--rounds needs at least two rounds".to_owned());
                }
            }
            "--" => {
                command = args[index.min(args.len())..].to_vec();
                index = args.len();
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }

    let base = base.ok_or("--base needs the path of a binary")?;
    let head = head.ok_or("--head needs the path of a binary")?;
    if command.is_empty() {
        return Err("the command needs a program and its arguments".to_owned());
    }
    Ok(Options {
        base,
        head,
        rounds,
        command,
    })
}

/// Take the value that follows an option.
fn take(args: &[String], index: &mut usize, option: &str) -> Result<String, String> {
    let value = args
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{option} needs a value"))?;
    *index += 1;
    Ok(value)
}

/// Check that the path names a file that the example can run.
fn check_file(path: &Path, label: &str) -> Result<(), String> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        format!(
            "the {label} binary {} cannot be read: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "the {label} binary {} is not a file",
            path.display()
        ));
    }
    Ok(())
}

/// The hash of a file, as sixteen hex digits.
fn hash_of(path: &Path) -> Result<String, String> {
    let bytes =
        std::fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let hex = blake3::hash(&bytes).to_hex().to_string();
    Ok(hex[..16].to_owned())
}

/// True when the two paths name the same file.
fn same_file(left: &Path, right: &Path) -> bool {
    if let (Ok(a), Ok(b)) = (std::fs::canonicalize(left), std::fs::canonicalize(right))
        && a == b
    {
        return true;
    }
    same_inode(left, right)
}

/// True when the two paths name the same file on this platform.
#[cfg(unix)]
fn same_inode(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(left), std::fs::metadata(right)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

/// True when the two paths name the same file on this platform.
#[cfg(not(unix))]
fn same_inode(_left: &Path, _right: &Path) -> bool {
    false
}

/// The samples of every arm of one run.
struct Arms {
    /// The samples of the base arm.
    base: Vec<Sample>,
    /// The samples of the head arm.
    head: Vec<Sample>,
    /// The samples of the noise rounds, first arm.
    noise_left: Vec<Sample>,
    /// The samples of the noise rounds, second arm.
    noise_right: Vec<Sample>,
}

/// Run the arms, interleaved, and return the samples of each arm.
///
/// The noise rounds run inside the same loop as the arms, so the floor is
/// measured under the same conditions as the effect.
fn run_arms(base: &Path, head: &Path, command: &[String], rounds: usize) -> Result<Arms, String> {
    let mut base_samples = Vec::with_capacity(rounds);
    let mut head_samples = Vec::with_capacity(rounds);
    let mut noise_left: Vec<Sample> = Vec::with_capacity(FLOOR_ROUNDS);
    let mut noise_right: Vec<Sample> = Vec::with_capacity(FLOOR_ROUNDS);
    let every = (rounds / FLOOR_ROUNDS).max(1);
    for round in 0..rounds {
        // The same binary in both arms, so the difference is the noise of the
        // machine and not a difference between two programs.
        if round % every == 0 && noise_left.len() < FLOOR_ROUNDS {
            let first = measure(base, command)?;
            let second = measure(base, command)?;
            if round % 2 == 0 {
                noise_left.push(first);
                noise_right.push(second);
            } else {
                noise_left.push(second);
                noise_right.push(first);
            }
        }
        // The order flips every round, so a slow patch from a neighbour lands
        // on both arms instead of on the one that happens to be running.
        if round % 2 == 0 {
            base_samples.push(measure(base, command)?);
            head_samples.push(measure(head, command)?);
        } else {
            head_samples.push(measure(head, command)?);
            base_samples.push(measure(base, command)?);
        }
    }
    Ok(Arms {
        base: base_samples,
        head: head_samples,
        noise_left,
        noise_right,
    })
}

/// The paired differences of the noise rounds, for every measurement.
fn noise_of(arms: &Arms) -> Vec<Noise> {
    let mut noise = Vec::new();
    for kind in Kind::all() {
        let diffs = paired(&arms.noise_left, &arms.noise_right, kind);
        if diffs.is_empty() {
            continue;
        }
        noise.push(Noise { kind, diffs });
    }
    noise
}

/// Run the command against one binary, and record what the run used.
fn measure(binary: &Path, command: &[String]) -> Result<Sample, String> {
    let started = Instant::now();
    let mut child = Command::new(binary);
    child.args(&command[1..]);
    let child = child
        .spawn()
        .map_err(|error| format!("cannot run {}: {error}", binary.display()))?;
    let usage = wait_for(child)?;
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(Sample {
        wall_ms,
        cpu_ms: usage.cpu_ms,
        rss_kb: usage.rss_kb,
    })
}

/// Wait for the child, and read its own usage.
#[cfg(unix)]
fn wait_for(child: std::process::Child) -> Result<Usage, String> {
    let pid = libc::pid_t::try_from(child.id())
        .map_err(|_| "the child has a pid that does not fit in a pid".to_owned())?;
    let mut status: libc::c_int = 0;
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    loop {
        // `wait4` returns the usage of the child, and the usage of a child is
        // gone the moment the parent waits for it.
        let waited = unsafe { libc::wait4(pid, &raw mut status, 0, &raw mut usage) };
        if waited >= 0 {
            break;
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(format!("cannot wait for the child: {error}"));
    }
    // The parent waited for the child. The handle has nothing left to report.
    drop(child);
    Ok(Usage {
        cpu_ms: Some(cpu_ms(&usage)),
        rss_kb: Some(peak_rss_kb(&usage)),
    })
}

/// Wait for the child. This platform has no `wait4`, so it has no usage.
#[cfg(not(unix))]
fn wait_for(mut child: std::process::Child) -> Result<Usage, String> {
    child
        .wait()
        .map_err(|error| format!("cannot wait for the child: {error}"))?;
    drop(child);
    Ok(Usage {
        cpu_ms: None,
        rss_kb: None,
    })
}

/// The CPU time of a child, in milliseconds.
#[cfg(unix)]
fn cpu_ms(usage: &libc::rusage) -> f64 {
    let user = usage.ru_utime;
    let system = usage.ru_stime;
    let seconds = u32::try_from(user.tv_sec).unwrap_or(u32::MAX)
        + u32::try_from(system.tv_sec).unwrap_or(u32::MAX);
    let micros = u32::try_from(user.tv_usec).unwrap_or(u32::MAX)
        + u32::try_from(system.tv_usec).unwrap_or(u32::MAX);
    (f64::from(seconds) + f64::from(micros) / 1_000_000.0) * 1000.0
}

/// The peak resident memory of a child, in KiB.
#[cfg(unix)]
fn peak_rss_kb(usage: &libc::rusage) -> f64 {
    let raw = u32::try_from(usage.ru_maxrss).unwrap_or(u32::MAX);
    // Linux counts the peak in KiB, and macOS counts it in bytes.
    #[cfg(target_vendor = "apple")]
    let kibi = f64::from(raw) / 1024.0;
    #[cfg(not(target_vendor = "apple"))]
    let kibi = f64::from(raw);
    kibi
}

/// The paired difference of each round, as a percentage of the base.
fn paired(base: &[Sample], head: &[Sample], kind: Kind) -> Vec<f64> {
    base.iter()
        .zip(head.iter())
        .filter_map(|(base, head)| {
            let base_value = kind.value(base)?;
            let head_value = kind.value(head)?;
            if base_value <= 0.0 {
                return None;
            }
            Some((head_value - base_value) / base_value * 100.0)
        })
        .collect()
}

/// The paired comparison of every measurement.
fn measurements(base: &[Sample], head: &[Sample], noise: &[Noise]) -> Vec<Measurement> {
    let mut measurements = Vec::new();
    for kind in Kind::all() {
        let base_values = values(base, kind);
        let head_values = values(head, kind);
        if base_values.is_empty() || head_values.is_empty() {
            continue;
        }
        let diffs = paired(base, head, kind);
        let floor = floor_of(noise, kind);
        let median_diff = median(&diffs);
        let (head_wins, base_wins) = sign_counts(&diffs);
        measurements.push(Measurement {
            kind,
            base_median: median(&base_values),
            head_median: median(&head_values),
            base_min: minimum(&base_values),
            head_min: minimum(&head_values),
            median_diff,
            floor,
            head_wins,
            base_wins,
            verdict: verdict(median_diff, floor, head_wins, base_wins),
            base: base_values,
            head: head_values,
            diffs,
        });
    }
    measurements
}

/// The values of one measurement across the rounds of one arm.
fn values(samples: &[Sample], kind: Kind) -> Vec<f64> {
    samples
        .iter()
        .filter_map(|sample| kind.value(sample))
        .collect()
}

/// The noise floor of one measurement, as a percentage.
///
/// The floor is the largest difference the same binary produced against
/// itself. An effect below that is not a result, because the machine can
/// produce a difference that big on its own.
///
/// The largest difference, and not the median, because the floor has to hold
/// the tool back. The same binary must never come out as the faster one, and
/// a floor below the worst noise the machine reached cannot promise that.
/// The tool prints the spread of the differences next to the floor, so the
/// reader sees the whole picture.
fn floor_of(noise: &[Noise], kind: Kind) -> f64 {
    noise
        .iter()
        .find(|entry| entry.kind == kind)
        .map_or(0.0, |entry| {
            entry
                .diffs
                .iter()
                .map(|diff| diff.abs())
                .fold(0.0_f64, f64::max)
        })
}

/// How many rounds each arm won.
fn sign_counts(diffs: &[f64]) -> (usize, usize) {
    let mut head_wins = 0;
    let mut base_wins = 0;
    for diff in diffs {
        if *diff < 0.0 {
            head_wins += 1;
        } else if *diff > 0.0 {
            base_wins += 1;
        }
    }
    (head_wins, base_wins)
}

/// The verdict of one measurement.
fn verdict(median_diff: f64, floor: f64, head_wins: usize, base_wins: usize) -> &'static str {
    if median_diff.abs() < floor {
        return "unmeasurable";
    }
    if head_wins >= base_wins + MARGIN {
        return "head faster";
    }
    if base_wins >= head_wins + MARGIN {
        return "head slower";
    }
    "no difference"
}

/// The median of a list of values.
fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

/// The smallest of a list of values.
fn minimum(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::MAX, f64::min)
}

/// The spread of a list of values, from the smallest to the largest.
fn spread(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    maximum(values) - minimum(values)
}

/// The largest of a list of values.
fn maximum(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::MIN, f64::max)
}

/// The load average of the machine, as three numbers.
fn load_average() -> String {
    std::fs::read_to_string("/proc/loadavg").map_or_else(
        |_| "unknown".to_owned(),
        |text| {
            text.split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" ")
        },
    )
}

/// The first number of a load average.
fn load_value(text: &str) -> Option<f64> {
    text.split_whitespace().next()?.parse::<f64>().ok()
}

/// The free memory of the machine, in KiB.
fn free_memory_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            return rest.split_whitespace().next()?.parse::<u64>().ok();
        }
    }
    None
}

impl Host {
    /// Read the state of the machine.
    fn read(load_before: &str, load_after: &str) -> Self {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let limit = f64::from(u32::try_from(cores).unwrap_or(u32::MAX));
        let busy = |text: &str| load_value(text).is_some_and(|value| value > limit);
        Self {
            cores,
            load_before: load_before.to_owned(),
            load_after: load_after.to_owned(),
            free_memory_kb: free_memory_kb(),
            noisy: busy(load_before) || busy(load_after),
        }
    }
}

/// Print the report.
fn print_report(report: &Report) {
    let mut out = String::new();
    let _ = writeln!(out, "\n=== base against head ===");
    let _ = writeln!(
        out,
        "  base binary: {} (blake3 {})",
        report.options.base.display(),
        report.base_hash
    );
    let _ = writeln!(
        out,
        "  head binary: {} (blake3 {})",
        report.options.head.display(),
        report.head_hash
    );
    let _ = writeln!(out, "  command: {}", report.options.command.join(" "));
    let _ = writeln!(
        out,
        "  rounds: {}, noise floor rounds: {}",
        report.options.rounds, FLOOR_ROUNDS
    );
    let _ = writeln!(out, "  load average before: {}", report.host.load_before);
    let _ = writeln!(out, "  load average after: {}", report.host.load_after);
    let _ = writeln!(
        out,
        "  cores: {}, free memory: {} kB",
        report.host.cores,
        report
            .host
            .free_memory_kb
            .map_or_else(|| "unknown".to_owned(), |value| value.to_string())
    );
    if report.host.noisy {
        let _ = writeln!(out, "  the host is noisy, so this run has no verdict");
    }
    if !report
        .measurements
        .iter()
        .any(|item| item.kind == Kind::Cpu)
    {
        let _ = writeln!(
            out,
            "  this platform has no wait4 call, so this run records the wall time only"
        );
    }

    let _ = writeln!(out, "\n  arms:");
    for item in &report.measurements {
        let _ = writeln!(
            out,
            "    {:<26} base median {:>10.1} minimum {:>10.1}   head median {:>10.1} minimum {:>10.1}",
            item.kind.label(),
            item.base_median,
            item.base_min,
            item.head_median,
            item.head_min
        );
    }

    let _ = writeln!(out, "\n  sign counts, head wins then base wins:");
    for item in &report.measurements {
        let _ = writeln!(
            out,
            "    {:<26} {} then {}",
            item.kind.label(),
            item.head_wins,
            item.base_wins
        );
    }

    let _ = writeln!(out);
    print_table(report, &mut out);

    let _ = writeln!(out, "\n  noise floor, the same binary in both arms:");
    for entry in &report.noise {
        let diffs: Vec<String> = entry
            .diffs
            .iter()
            .map(|diff| format!("{diff:+.1}%"))
            .collect();
        let _ = writeln!(
            out,
            "    {:<26} {}   floor {:.1}%, spread {:.1}%",
            entry.kind.label(),
            diffs.join(" "),
            floor_of(&report.noise, entry.kind),
            spread(&entry.diffs)
        );
    }

    if report.host.noisy {
        let _ = writeln!(out, "\n  verdict: none, the host is noisy");
    } else {
        let _ = writeln!(out, "\n  verdict: {}", report.measurements[0].verdict);
    }
    print!("{out}");
}

/// Print the markdown table of the report.
fn print_table(report: &Report, out: &mut String) {
    let _ = writeln!(
        out,
        "  | measurement | base median | head median | paired median | floor | verdict |"
    );
    let _ = writeln!(out, "  | --- | --- | --- | --- | --- | --- |");
    for item in &report.measurements {
        let verdict = if report.host.noisy {
            "noisy"
        } else {
            item.verdict
        };
        let _ = writeln!(
            out,
            "  | {} | {:.1} | {:.1} | {:+.1}% | {:.1}% | {} |",
            item.kind.label(),
            item.base_median,
            item.head_median,
            item.median_diff,
            item.floor,
            verdict
        );
    }
}

/// The path of the report file of this run.
fn report_path(report: &Report) -> PathBuf {
    let base = report.options.base.file_stem().map_or_else(
        || "base".to_owned(),
        |stem| stem.to_string_lossy().into_owned(),
    );
    let head = report.options.head.file_stem().map_or_else(
        || "head".to_owned(),
        |stem| stem.to_string_lossy().into_owned(),
    );
    PathBuf::from("reports")
        .join("bench")
        .join(format!("{base}-vs-{head}.json"))
}

/// Write the full data of the run as JSON.
fn write_report(report: &Report) -> Result<(), String> {
    let path = report_path(report);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(&to_json(report))
        .map_err(|error| format!("cannot write the report: {error}"))?;
    std::fs::write(&path, text)
        .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    println!("\n  report: {}", path.display());
    Ok(())
}

/// The full data of the run, as JSON.
fn to_json(report: &Report) -> serde_json::Value {
    let measurements: Vec<serde_json::Value> = report
        .measurements
        .iter()
        .map(|item| {
            serde_json::json!({
                "measurement": item.kind.label(),
                "base": item.base,
                "head": item.head,
                "paired_percent": item.diffs,
                "base_median": item.base_median,
                "head_median": item.head_median,
                "base_minimum": item.base_min,
                "head_minimum": item.head_min,
                "paired_median_percent": item.median_diff,
                "floor_percent": item.floor,
                "head_wins": item.head_wins,
                "base_wins": item.base_wins,
                "verdict": item.verdict,
            })
        })
        .collect();
    let noise: Vec<serde_json::Value> = report
        .noise
        .iter()
        .map(|entry| {
            serde_json::json!({
                "measurement": entry.kind.label(),
                "paired_percent": entry.diffs,
                "floor_percent": floor_of(&report.noise, entry.kind),
                "spread_percent": spread(&entry.diffs),
            })
        })
        .collect();
    serde_json::json!({
        "generated": jiff::Timestamp::now().to_string(),
        "command": report.options.command,
        "rounds": report.options.rounds,
        "floor_rounds": FLOOR_ROUNDS,
        "base": {
            "path": report.options.base.display().to_string(),
            "blake3": report.base_hash,
        },
        "head": {
            "path": report.options.head.display().to_string(),
            "blake3": report.head_hash,
        },
        "measurements": measurements,
        "noise_floor": noise,
        "host": {
            "cores": report.host.cores,
            "load_before": report.host.load_before,
            "load_after": report.host.load_after,
            "free_memory_kb": report.host.free_memory_kb,
            "noisy": report.host.noisy,
        },
        "verdict": if report.host.noisy {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(report.measurements[0].verdict.to_owned())
        },
    })
}
