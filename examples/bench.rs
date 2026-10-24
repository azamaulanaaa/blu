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
//! # Two revisions
//!
//! `--base` and `--head` also take git revisions.
//! The example then builds the `blu` binary of each revision and measures them
//! against each other.
//!
//! ```console
//! $ cargo run --release --example bench -- \
//!     --base <revision> --head <revision> [--scenario NAME] [--diff FROM..TO]
//! ```
//!
//! The example makes one worktree per revision under the system temporary
//! directory.
//! Each worktree has its own target directory, so cargo cannot reuse the
//! artifact of the other revision.
//! The example removes the worktrees and their target directories when the run
//! ends.
//! `--keep-worktrees` leaves them in place.
//!
//! The example reads the scenarios from `benches/scenarios.toml`.
//! It prepares one fixture for each scenario and uses the same bytes for both
//! arms.
//! It gives every round a fresh store.
//! It writes the artifact into `reports/bench/`.
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

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use fs2::FileExt as _;

/// The rounds per arm. Nine is the count `examples/ab.rs` settled on.
const ROUNDS: usize = 9;

/// The rounds of the noise floor. Three is enough to see the spread.
const FLOOR_ROUNDS: usize = 3;

/// The rounds the head arm must win by before the verdict names a direction.
const MARGIN: usize = 2;

/// The usage text of this example.
const USAGE: &str = "\
usage: bench --base REV --head REV [--scenario NAME]... [--diff FROM..TO]
             [--rounds N] [--keep-worktrees]
       bench --base PATH --head PATH [--rounds N] -- COMMAND [ARGUMENT...]
  --base REV    the base revision or the base binary.
  --head REV    the head revision or the head binary.
  --scenario    run one scenario from benches/scenarios.toml, repeatable.
  --diff        run the scenarios that touch a path in FROM..TO.
  --rounds N    the rounds per arm, 9 by default.
  --keep-worktrees  leave the worktrees and the run directory.
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

    /// The name the scenario table uses for this measurement.
    fn name(self) -> &'static str {
        match self {
            Kind::Wall => "wall_ms",
            Kind::Cpu => "cpu_ms",
            Kind::Rss => "rss_kb",
        }
    }

    /// The measurement a scenario table names, or `None`.
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "wall_ms" => Some(Kind::Wall),
            "cpu_ms" => Some(Kind::Cpu),
            "rss_kb" => Some(Kind::Rss),
            _ => None,
        }
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

/// What the command line asked for in the two-revision mode.
#[derive(Clone)]
struct RevisionOptions {
    /// The base revision.
    base: String,
    /// The head revision.
    head: String,
    /// The rounds per arm.
    rounds: usize,
    /// The scenarios to run, by name.
    scenarios: Vec<String>,
    /// The revision range that selects the scenarios.
    diff: Option<String>,
    /// True when the worktrees and the run directory stay.
    keep: bool,
}

/// What the command line asked for.
enum Invocation {
    /// Two binaries and one command.
    Pair(Options),
    /// Two revisions and a scenario table.
    Revisions(RevisionOptions),
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
    match parse(&args)? {
        Invocation::Pair(options) => run_pair(options),
        Invocation::Revisions(options) => run_revisions(&options),
    }
}

/// Run the two binaries that the command line names.
fn run_pair(options: Options) -> Result<(), String> {
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
fn parse(args: &[String]) -> Result<Invocation, String> {
    let mut base: Option<String> = None;
    let mut head: Option<String> = None;
    let mut rounds = ROUNDS;
    let mut command: Vec<String> = Vec::new();
    let mut scenarios: Vec<String> = Vec::new();
    let mut diff: Option<String> = None;
    let mut keep = false;

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].clone();
        index += 1;
        match arg.as_str() {
            "--base" => base = Some(take(args, &mut index, "--base")?),
            "--head" => head = Some(take(args, &mut index, "--head")?),
            "--scenario" => scenarios.push(take(args, &mut index, "--scenario")?),
            "--diff" => diff = Some(take(args, &mut index, "--diff")?),
            "--keep-worktrees" => keep = true,
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

    let base = base.ok_or("--base needs a revision or the path of a binary")?;
    let head = head.ok_or("--head needs a revision or the path of a binary")?;
    if command.is_empty() {
        return Ok(Invocation::Revisions(RevisionOptions {
            base,
            head,
            rounds,
            scenarios,
            diff,
            keep,
        }));
    }

    if !scenarios.is_empty() || diff.is_some() || keep {
        return Err("--scenario, --diff and --keep-worktrees need two revisions".to_owned());
    }
    Ok(Invocation::Pair(Options {
        base: PathBuf::from(base),
        head: PathBuf::from(head),
        rounds,
        command,
    }))
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
    run_arms_with(base, head, rounds, |binary| measure(binary, &command[1..]))
}

/// Run the arms with one sample function, interleaved.
fn run_arms_with(
    base: &Path,
    head: &Path,
    rounds: usize,
    mut sample: impl FnMut(&Path) -> Result<Sample, String>,
) -> Result<Arms, String> {
    let mut base_samples = Vec::with_capacity(rounds);
    let mut head_samples = Vec::with_capacity(rounds);
    let mut noise_left: Vec<Sample> = Vec::with_capacity(FLOOR_ROUNDS);
    let mut noise_right: Vec<Sample> = Vec::with_capacity(FLOOR_ROUNDS);
    let every = (rounds / FLOOR_ROUNDS).max(1);
    for round in 0..rounds {
        // The same binary in both arms, so the difference is the noise of the
        // machine and not a difference between two programs.
        if round % every == 0 && noise_left.len() < FLOOR_ROUNDS {
            let first = sample(base)?;
            let second = sample(base)?;
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
            base_samples.push(sample(base)?);
            head_samples.push(sample(head)?);
        } else {
            head_samples.push(sample(head)?);
            base_samples.push(sample(base)?);
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
fn measure(binary: &Path, args: &[String]) -> Result<Sample, String> {
    measure_inner(binary, args, true)
}

/// Run the command against one binary, and discard its output.
fn measure_quiet(binary: &Path, args: &[String]) -> Result<Sample, String> {
    measure_inner(binary, args, false)
}

/// Run the command against one binary, and record what the run used.
fn measure_inner(binary: &Path, args: &[String], show: bool) -> Result<Sample, String> {
    let started = Instant::now();
    let mut command = Command::new(binary);
    command.args(args);
    if !show {
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null());
    }
    let child = command
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

/// Run one command without timing it, and refuse a failure.
fn run_command(binary: &Path, args: &[String]) -> Result<(), String> {
    let status = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("cannot run {}: {error}", binary.display()))?;
    if !status.success() {
        return Err(format!(
            "{} {} failed with {status}",
            binary.display(),
            args.join(" ")
        ));
    }
    Ok(())
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
    if !libc::WIFEXITED(status) {
        return Err(format!("the child ended with status {status}"));
    }
    let code = libc::WEXITSTATUS(status);
    if code != 0 {
        return Err(format!("the child failed with exit code {code}"));
    }
    Ok(Usage {
        cpu_ms: Some(cpu_ms(&usage)),
        rss_kb: Some(peak_rss_kb(&usage)),
    })
}

/// Wait for the child. This platform has no `wait4`, so it has no usage.
#[cfg(not(unix))]
fn wait_for(mut child: std::process::Child) -> Result<Usage, String> {
    let status = child
        .wait()
        .map_err(|error| format!("cannot wait for the child: {error}"))?;
    drop(child);
    if !status.success() {
        return Err(format!("the child failed with {status}"));
    }
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
    print_table(&report.measurements, report.host.noisy, &mut out);

    print_noise(&report.noise, &mut out);

    if report.host.noisy {
        let _ = writeln!(out, "\n  verdict: none, the host is noisy");
    } else if let Some(first) = report.measurements.first() {
        let _ = writeln!(out, "\n  verdict: {}", first.verdict);
    } else {
        let _ = writeln!(out, "\n  verdict: unmeasurable");
    }
    print!("{out}");
}

/// Print the noise floor of each measurement.
fn print_noise(noise: &[Noise], out: &mut String) {
    let _ = writeln!(out, "\n  noise floor, the same binary in both arms:");
    for entry in noise {
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
            floor_of(noise, entry.kind),
            spread(&entry.diffs)
        );
    }
}

/// Print the markdown table of one set of measurements.
fn print_table(measurements: &[Measurement], noisy: bool, out: &mut String) {
    let _ = writeln!(
        out,
        "  | measurement | base median | head median | paired median | floor | verdict |"
    );
    let _ = writeln!(out, "  | --- | --- | --- | --- | --- | --- |");
    for item in measurements {
        let verdict = if noisy { "noisy" } else { item.verdict };
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

/// The measurements of a run, as JSON.
fn measurements_json(measurements: &[Measurement]) -> Vec<serde_json::Value> {
    measurements
        .iter()
        .map(|item| {
            serde_json::json!({
                "measurement": item.kind.label(),
                "metric": item.kind.name(),
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
        .collect()
}

/// The noise floor of a run, as JSON.
fn noise_json(noise: &[Noise]) -> Vec<serde_json::Value> {
    noise
        .iter()
        .map(|entry| {
            serde_json::json!({
                "measurement": entry.kind.label(),
                "metric": entry.kind.name(),
                "paired_percent": entry.diffs,
                "floor_percent": floor_of(noise, entry.kind),
                "spread_percent": spread(&entry.diffs),
            })
        })
        .collect()
}

/// The full data of the run, as JSON.
fn to_json(report: &Report) -> serde_json::Value {
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
        "measurements": measurements_json(&report.measurements),
        "noise_floor": noise_json(&report.noise),
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
            serde_json::Value::String(
                report
                    .measurements
                    .first()
                    .map_or("unmeasurable", |item| item.verdict)
                    .to_owned(),
            )
        },
    })
}

/// The shape a scenario represents.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A shape that the product's defaults reach.
    Default,
    /// A shape that needs a setting the product does not use by default.
    Forced,
}

impl Shape {
    /// The name the scenario table uses for this shape.
    fn name(self) -> &'static str {
        match self {
            Shape::Default => "default",
            Shape::Forced => "forced",
        }
    }

    /// The shape a scenario table names, or `None`.
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "default" => Some(Shape::Default),
            "forced" => Some(Shape::Forced),
            _ => None,
        }
    }
}

/// The fixture that one scenario measures.
#[derive(Clone)]
struct Fixture {
    /// The shape `examples/mktree.rs` writes.
    kind: String,
    /// The number of files.
    files: u64,
    /// The bytes in each file.
    bytes: u64,
    /// The bytes that every file shares, for the `tree` shape.
    shared: Option<u64>,
}

impl Fixture {
    /// A key that names these bytes.
    fn key(&self) -> String {
        let shared = self
            .shared
            .map_or_else(|| "none".to_owned(), |value| value.to_string());
        format!("{}-{}-{}-{shared}", self.kind, self.files, self.bytes)
    }

    /// The arguments that `examples/mktree.rs` takes.
    fn mktree_args(&self, dir: &Path) -> Vec<String> {
        let dir = dir.display().to_string();
        if self.kind == "mutate" {
            return vec![self.kind.clone(), dir];
        }
        let mut args = vec![
            self.kind.clone(),
            dir,
            self.files.to_string(),
            self.bytes.to_string(),
        ];
        if let Some(shared) = self.shared {
            args.push(shared.to_string());
        }
        args
    }

    /// The fixture as JSON.
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "kind": self.kind,
            "files": self.files,
            "bytes": self.bytes,
            "shared": self.shared,
        })
    }
}

/// One row of `benches/scenarios.toml`.
#[derive(Clone)]
struct Scenario {
    /// The name of the scenario.
    name: String,
    /// The source files the scenario exercises.
    paths: Vec<String>,
    /// The shape the scenario represents.
    shape: Shape,
    /// The fixture the scenario reads.
    fixture: Fixture,
    /// The commands that fill the store, before the measured command.
    prepare: Vec<Vec<String>>,
    /// The measured command and its arguments.
    command: Vec<String>,
    /// The measurement the verdict uses.
    metric: Kind,
    /// The reason this scenario cannot run, when it cannot.
    unmeasurable: Option<String>,
}

/// The result of one scenario.
struct ScenarioResult {
    /// The scenario itself.
    scenario: Scenario,
    /// `ran`, `stale` or `unmeasurable`.
    status: &'static str,
    /// The reason the scenario did not run.
    reason: Option<String>,
    /// The fixture the scenario read.
    fixture: Option<PathBuf>,
    /// The paired comparison of each measurement.
    measurements: Vec<Measurement>,
    /// The noise floor of each measurement.
    noise: Vec<Noise>,
    /// The verdict of the metric the scenario names.
    verdict: Option<&'static str>,
}

/// Everything a two-revision run measured.
struct RevisionReport {
    /// What the command line asked for.
    options: RevisionOptions,
    /// The repository root.
    repo: PathBuf,
    /// The full hash of the base revision.
    base_revision: String,
    /// The full hash of the head revision.
    head_revision: String,
    /// The short hash of the base revision.
    base_short: String,
    /// The short hash of the head revision.
    head_short: String,
    /// The hash of the base binary.
    base_hash: String,
    /// The hash of the head binary.
    head_hash: String,
    /// The result of each selected scenario.
    scenarios: Vec<ScenarioResult>,
    /// The paths the diff changed.
    changed_paths: Vec<String>,
    /// The changed paths that no scenario covers.
    uncovered_paths: Vec<String>,
    /// The state of the machine.
    host: Host,
    /// The directory that holds the worktrees and the fixtures.
    run_dir: String,
}

/// A guard that removes a run directory when the run ends.
struct RunCleanup {
    /// The directory to remove.
    dir: PathBuf,
    /// True when the directory stays.
    keep: bool,
}

impl RunCleanup {
    /// Make a guard for one run directory.
    fn new(dir: PathBuf, keep: bool) -> Self {
        Self { dir, keep }
    }
}

impl Drop for RunCleanup {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// A borrowed worktree of one revision.
struct Worktree {
    /// The repository root.
    repo: PathBuf,
    /// The worktree directory.
    dir: PathBuf,
    /// True when the worktree stays.
    keep: bool,
}

impl Worktree {
    /// Add a detached worktree for one revision.
    fn add(
        repo: &Path,
        rev: &str,
        run_dir: &Path,
        label: &str,
        keep: bool,
    ) -> Result<Self, String> {
        let dir = run_dir.join(format!("worktree-{label}"));
        if dir.exists() {
            std::fs::remove_dir_all(&dir)
                .map_err(|error| format!("cannot remove {}: {error}", dir.display()))?;
        }
        let status = Command::new("git")
            .args(["worktree", "add", "--detach"])
            .arg(&dir)
            .arg(rev)
            .current_dir(repo)
            .status()
            .map_err(|error| format!("cannot run git worktree add: {error}"))?;
        if !status.success() {
            return Err(format!(
                "git worktree add {} {rev} failed with {status}",
                dir.display()
            ));
        }
        Ok(Self {
            repo: repo.to_path_buf(),
            dir,
            keep,
        })
    }

    /// The path of the worktree.
    fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for Worktree {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        let _ = Command::new("git")
            .args(["worktree", "remove", "--force"])
            .arg(&self.dir)
            .current_dir(&self.repo)
            .status();
        if self.dir.exists() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// The host-wide lock around one cargo call.
struct CargoLock {
    /// The open lock file.
    file: std::fs::File,
}

impl CargoLock {
    /// Take the lock, and wait for it when another cargo holds it.
    fn acquire() -> Result<Self, String> {
        let path = std::env::temp_dir().join("blu-bench-cargo.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
        file.lock_exclusive()
            .map_err(|error| format!("cannot lock {}: {error}", path.display()))?;
        Ok(Self { file })
    }
}

impl Drop for CargoLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// The worktrees and binaries of one two-revision run.
struct BuiltRevisions {
    /// The base worktree, kept alive until the run ends.
    _base_tree: Worktree,
    /// The head worktree, kept alive until the run ends.
    head_tree: Worktree,
    /// The base binary.
    base_bin: PathBuf,
    /// The head binary.
    head_bin: PathBuf,
    /// The hash of the base binary.
    base_hash: String,
    /// The hash of the head binary.
    head_hash: String,
    /// The full hash of the base revision.
    base_revision: String,
    /// The full hash of the head revision.
    head_revision: String,
    /// The short hash of the base revision.
    base_short: String,
    /// The short hash of the head revision.
    head_short: String,
}

/// Do a two-revision run, from the worktrees to the report file.
fn run_revisions(options: &RevisionOptions) -> Result<(), String> {
    let repo = repo_root()?;
    let table = repo.join("benches").join("scenarios.toml");
    let scenarios = load_scenarios(&table)?;
    check_names(&scenarios, &options.scenarios)?;

    let changed = match &options.diff {
        Some(spec) => changed_paths(&repo, spec)?,
        None => Vec::new(),
    };
    let selected = select_scenarios(&scenarios, options, &changed);
    if selected.is_empty() {
        return Err("no scenario matches the command line".to_owned());
    }

    let base_short = short_revision(&repo, &options.base)?;
    let head_short = short_revision(&repo, &options.head)?;
    let run_dir = make_run_dir(&base_short, &head_short)?;
    let _cleanup = RunCleanup::new(run_dir.clone(), options.keep);
    let built = build_revisions(&repo, &run_dir, options)?;
    let (results, host) = run_selected(
        &repo,
        &run_dir,
        &scenarios,
        &selected,
        &built,
        options.rounds,
    )?;
    let uncovered = uncovered_paths(&changed, &scenarios, built.head_tree.path());

    let report = RevisionReport {
        options: options.clone(),
        repo,
        base_revision: built.base_revision,
        head_revision: built.head_revision,
        base_short: built.base_short,
        head_short: built.head_short,
        base_hash: built.base_hash,
        head_hash: built.head_hash,
        scenarios: results,
        uncovered_paths: uncovered,
        changed_paths: changed,
        host,
        run_dir: run_dir.display().to_string(),
    };
    print_revision_report(&report);
    write_revision_report(&report)?;
    Ok(())
}

/// Make the worktrees and build the two binaries.
fn build_revisions(
    repo: &Path,
    run_dir: &Path,
    options: &RevisionOptions,
) -> Result<BuiltRevisions, String> {
    let base_revision = resolve_revision(repo, &options.base)?;
    let head_revision = resolve_revision(repo, &options.head)?;
    let base_short = short_revision(repo, &base_revision)?;
    let head_short = short_revision(repo, &head_revision)?;
    let base_tree = Worktree::add(repo, &base_revision, run_dir, "base", options.keep)?;
    let head_tree = Worktree::add(repo, &head_revision, run_dir, "head", options.keep)?;
    let base_built = build_binary(base_tree.path(), "base")?;
    let head_built = build_binary(head_tree.path(), "head")?;
    if same_file(&base_built, &head_built) {
        return Err("the base binary and the head binary are the same file".to_owned());
    }
    let base_bin = copy_binary(&base_built, run_dir, "base")?;
    let head_bin = copy_binary(&head_built, run_dir, "head")?;
    let base_hash = hash_of(&base_bin)?;
    let head_hash = hash_of(&head_bin)?;
    if base_hash == head_hash {
        eprintln!(
            "warning: the two revisions built the same binary, so this run measures the noise floor only"
        );
    }
    Ok(BuiltRevisions {
        _base_tree: base_tree,
        head_tree,
        base_bin,
        head_bin,
        base_hash,
        head_hash,
        base_revision,
        head_revision,
        base_short,
        head_short,
    })
}

/// Run the selected scenarios.
fn run_selected(
    repo: &Path,
    run_dir: &Path,
    scenarios: &[Scenario],
    selected: &[usize],
    built: &BuiltRevisions,
    rounds: usize,
) -> Result<(Vec<ScenarioResult>, Host), String> {
    let mut fixtures: HashMap<String, PathBuf> = HashMap::new();
    let mut results = Vec::new();
    let mut counter = 0usize;
    let load_before = load_average();
    for &index in selected {
        let scenario = &scenarios[index];
        if let Some(reason) = stale_reason(built.head_tree.path(), scenario) {
            results.push(ScenarioResult {
                scenario: scenario.clone(),
                status: "stale",
                reason: Some(reason),
                fixture: None,
                measurements: Vec::new(),
                noise: Vec::new(),
                verdict: None,
            });
            continue;
        }
        if let Some(reason) = &scenario.unmeasurable {
            results.push(ScenarioResult {
                scenario: scenario.clone(),
                status: "unmeasurable",
                reason: Some(reason.clone()),
                fixture: None,
                measurements: Vec::new(),
                noise: Vec::new(),
                verdict: None,
            });
            continue;
        }
        let fixture = prepare_fixture(repo, run_dir, &scenario.fixture, &mut fixtures)?;
        let run = ScenarioRun {
            scenario,
            base: &built.base_bin,
            head: &built.head_bin,
            fixture: &fixture,
            run_dir,
            rounds,
        };
        let (measurements, noise) = run_scenario(&run, &mut counter)?;
        results.push(ScenarioResult {
            scenario: scenario.clone(),
            status: "ran",
            reason: None,
            fixture: Some(fixture),
            measurements,
            noise,
            verdict: None,
        });
    }
    let load_after = load_average();
    let host = Host::read(&load_before, &load_after);
    for result in &mut results {
        if result.status == "ran" {
            result.verdict = if host.noisy {
                None
            } else {
                result
                    .measurements
                    .iter()
                    .find(|item| item.kind == result.scenario.metric)
                    .map(|item| item.verdict)
            };
        }
    }
    Ok((results, host))
}

/// Check that every name the command line asked for is in the table.
fn check_names(scenarios: &[Scenario], names: &[String]) -> Result<(), String> {
    for name in names {
        if !scenarios.iter().any(|scenario| &scenario.name == name) {
            return Err(format!("the table has no scenario {name}"));
        }
    }
    Ok(())
}

/// The scenarios the command line selects, as table indexes.
fn select_scenarios(
    scenarios: &[Scenario],
    options: &RevisionOptions,
    changed: &[String],
) -> Vec<usize> {
    if options.scenarios.is_empty() && options.diff.is_none() {
        return (0..scenarios.len()).collect();
    }
    let mut selected = Vec::new();
    for (index, scenario) in scenarios.iter().enumerate() {
        let named = options.scenarios.iter().any(|name| name == &scenario.name);
        let touched = options.diff.is_some()
            && scenario
                .paths
                .iter()
                .any(|path| changed.iter().any(|changed| covers(path, changed)));
        if named || touched {
            selected.push(index);
        }
    }
    selected
}

/// True when a scenario path covers a changed path.
fn covers(scenario_path: &str, changed: &str) -> bool {
    let path = scenario_path.trim_end_matches('/');
    changed == path
        || changed
            .strip_prefix(path)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// The changed paths that no scenario can measure.
fn uncovered_paths(changed: &[String], scenarios: &[Scenario], head: &Path) -> Vec<String> {
    changed
        .iter()
        .filter(|changed| {
            !scenarios.iter().any(|scenario| {
                scenario.unmeasurable.is_none()
                    && stale_reason(head, scenario).is_none()
                    && scenario.paths.iter().any(|path| covers(path, changed))
            })
        })
        .cloned()
        .collect()
}

/// The reason a scenario is stale, or `None`.
fn stale_reason(head: &Path, scenario: &Scenario) -> Option<String> {
    scenario.paths.iter().find_map(|path| {
        if head.join(path).exists() {
            None
        } else {
            Some(format!(
                "the path {path} does not exist at the head revision"
            ))
        }
    })
}

/// Read the scenario table.
fn load_scenarios(path: &Path) -> Result<Vec<Scenario>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let value: toml::Value = toml::from_str(&text)
        .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
    let rows = value
        .get("scenario")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| format!("{} holds no [[scenario]] row", path.display()))?;
    let mut scenarios = Vec::with_capacity(rows.len());
    for row in rows {
        scenarios.push(scenario_from_value(row)?);
    }
    for (index, scenario) in scenarios.iter().enumerate() {
        if scenarios[..index]
            .iter()
            .any(|other| other.name == scenario.name)
        {
            return Err(format!("the table names {} twice", scenario.name));
        }
    }
    Ok(scenarios)
}

/// Read one scenario row.
fn scenario_from_value(value: &toml::Value) -> Result<Scenario, String> {
    let table = value
        .as_table()
        .ok_or_else(|| "a scenario must be a table".to_owned())?;
    let name = required_string(table, "name")?;
    let paths = string_array(table, "paths")?;
    if paths.is_empty() {
        return Err(format!("scenario {name} needs at least one path"));
    }
    let shape_name = required_string(table, "shape")?;
    let shape = Shape::from_name(&shape_name)
        .ok_or_else(|| format!("scenario {name} has the shape {shape_name}"))?;
    let fixture_value = table
        .get("fixture")
        .ok_or_else(|| format!("scenario {name} needs a fixture"))?;
    let fixture = fixture_from_value(fixture_value)?;
    let prepare = match table.get("prepare") {
        Some(value) => command_array(value, "prepare")?,
        None => Vec::new(),
    };
    let command = table
        .get("command")
        .ok_or_else(|| format!("scenario {name} needs a command"))
        .and_then(|value| string_array_value(value, "command"))?;
    if command.is_empty() {
        return Err(format!("scenario {name} needs a command"));
    }
    let metric_name = required_string(table, "metric")?;
    let metric = Kind::from_name(&metric_name)
        .ok_or_else(|| format!("scenario {name} has the metric {metric_name}"))?;
    let unmeasurable = table
        .get("unmeasurable")
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("scenario {name} needs text in unmeasurable"))
        })
        .transpose()?;
    Ok(Scenario {
        name,
        paths,
        shape,
        fixture,
        prepare,
        command,
        metric,
        unmeasurable,
    })
}

/// Read one fixture table.
fn fixture_from_value(value: &toml::Value) -> Result<Fixture, String> {
    let table = value
        .as_table()
        .ok_or_else(|| "a fixture must be a table".to_owned())?;
    let kind = required_string(table, "kind")?;
    if kind == "mutate" {
        return Ok(Fixture {
            kind,
            files: 0,
            bytes: 0,
            shared: None,
        });
    }
    let files = required_count(table, "files")?;
    let bytes = required_count(table, "bytes")?;
    let shared = match table.get("shared") {
        Some(value) => Some(zero_count_value(value, "shared")?),
        None => None,
    };
    if kind == "tree" && shared.is_none() {
        return Err("the tree fixture needs shared".to_owned());
    }
    if let Some(shared) = shared
        && shared > bytes
    {
        return Err(format!(
            "the shared prefix of {shared} bytes does not fit in a file of {bytes} bytes"
        ));
    }
    Ok(Fixture {
        kind,
        files,
        bytes,
        shared,
    })
}

/// A required string field.
fn required_string(table: &toml::Table, field: &str) -> Result<String, String> {
    table
        .get(field)
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("the scenario needs a string {field}"))
}

/// A required count field.
fn required_count(table: &toml::Table, field: &str) -> Result<u64, String> {
    table
        .get(field)
        .ok_or_else(|| format!("the fixture needs {field}"))
        .and_then(|value| count_value(value, field))
}

/// One count value that must be positive.
fn count_value(value: &toml::Value, field: &str) -> Result<u64, String> {
    let number = zero_count_value(value, field)?;
    if number == 0 {
        return Err(format!("{field} needs a positive count"));
    }
    Ok(number)
}

/// One count value that can be zero.
fn zero_count_value(value: &toml::Value, field: &str) -> Result<u64, String> {
    value
        .as_integer()
        .and_then(|number| u64::try_from(number).ok())
        .ok_or_else(|| format!("{field} needs a count"))
}

/// A required array of strings.
fn string_array(table: &toml::Table, field: &str) -> Result<Vec<String>, String> {
    table
        .get(field)
        .ok_or_else(|| format!("the scenario needs {field}"))
        .and_then(|value| string_array_value(value, field))
}

/// An array of strings.
fn string_array_value(value: &toml::Value, field: &str) -> Result<Vec<String>, String> {
    let rows = value
        .as_array()
        .ok_or_else(|| format!("{field} needs an array"))?;
    rows.iter()
        .map(|row| {
            row.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{field} needs text"))
        })
        .collect()
}

/// An array of commands.
fn command_array(value: &toml::Value, field: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = value
        .as_array()
        .ok_or_else(|| format!("{field} needs an array"))?;
    rows.iter()
        .map(|row| string_array_value(row, field))
        .collect()
}

/// Make a directory for one run.
fn make_run_dir(base: &str, head: &str) -> Result<PathBuf, String> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    let dir = std::env::temp_dir().join(format!(
        "blu-bench-{}-{base}-{head}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    Ok(dir)
}

/// The repository root.
fn repo_root() -> Result<PathBuf, String> {
    let text = git_output(Path::new("."), &["rev-parse", "--show-toplevel"])?;
    let root = text.trim();
    if root.is_empty() {
        return Err("git did not report a repository root".to_owned());
    }
    Ok(PathBuf::from(root))
}

/// Run one git command and return its output.
fn git_output(repo: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .map_err(|error| format!("cannot run git {}: {error}", args.join(" ")))?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git {} failed: {}", args.join(" "), error.trim()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The full hash of a revision.
fn resolve_revision(repo: &Path, rev: &str) -> Result<String, String> {
    let text = git_output(
        repo,
        &["rev-parse", "--verify", &format!("{rev}^{{commit}}")],
    )?;
    Ok(text.trim().to_owned())
}

/// The short hash of a revision.
fn short_revision(repo: &Path, rev: &str) -> Result<String, String> {
    let text = git_output(repo, &["rev-parse", "--short", rev])?;
    Ok(text.trim().to_owned())
}

/// The paths that one revision range changed.
fn changed_paths(repo: &Path, spec: &str) -> Result<Vec<String>, String> {
    let (from, to) = spec
        .split_once("..")
        .ok_or_else(|| format!("{spec} is not FROM..TO"))?;
    if from.is_empty() || to.is_empty() {
        return Err(format!("{spec} is not FROM..TO"));
    }
    let text = git_output(repo, &["diff", "--name-only", &format!("{from}..{to}")])?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

/// Build the `blu` binary of one worktree.
fn build_binary(worktree: &Path, label: &str) -> Result<PathBuf, String> {
    let target = worktree.join("target");
    println!("building the {label} revision");
    cargo(
        &["build", "--release", "--bin", "blu", "-j", "1"],
        worktree,
        &target,
    )?;
    let binary = target.join("release").join("blu");
    check_file(&binary, label)?;
    Ok(binary)
}

/// Copy a binary into the run directory.
fn copy_binary(binary: &Path, run_dir: &Path, label: &str) -> Result<PathBuf, String> {
    let dir = run_dir.join("bin");
    std::fs::create_dir_all(&dir)
        .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    let copy = dir.join(format!("blu-{label}"));
    std::fs::copy(binary, &copy)
        .map_err(|error| format!("cannot copy {}: {error}", binary.display()))?;
    Ok(copy)
}

/// Run one cargo command under the host-wide lock.
fn cargo(args: &[&str], dir: &Path, target: &Path) -> Result<(), String> {
    let _lock = CargoLock::acquire()?;
    let status = Command::new("cargo")
        .args(args)
        .current_dir(dir)
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_PROFILE_RELEASE_DEBUG", "0")
        .stdin(Stdio::null())
        .status()
        .map_err(|error| format!("cannot run cargo: {error}"))?;
    if !status.success() {
        return Err(format!("cargo {} failed with {status}", args.join(" ")));
    }
    Ok(())
}

/// Prepare one fixture, and reuse it when the table names it twice.
fn prepare_fixture(
    repo: &Path,
    run_dir: &Path,
    fixture: &Fixture,
    cache: &mut HashMap<String, PathBuf>,
) -> Result<PathBuf, String> {
    let key = fixture.key();
    if let Some(path) = cache.get(&key) {
        return Ok(path.clone());
    }
    let dir = run_dir.join("fixtures").join(sanitize(&key));
    std::fs::create_dir_all(&dir)
        .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    let target = run_dir.join("fixture-target");
    let mut args: Vec<String> = vec![
        "run".to_owned(),
        "--release".to_owned(),
        "--example".to_owned(),
        "mktree".to_owned(),
        "-j".to_owned(),
        "1".to_owned(),
        "--".to_owned(),
    ];
    args.extend(fixture.mktree_args(&dir));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    cargo(&refs, repo, &target)?;
    cache.insert(key, dir.clone());
    Ok(dir)
}

/// Everything one scenario run needs.
struct ScenarioRun<'a> {
    /// The scenario.
    scenario: &'a Scenario,
    /// The base binary.
    base: &'a Path,
    /// The head binary.
    head: &'a Path,
    /// The fixture tree.
    fixture: &'a Path,
    /// The directory that holds the stores.
    run_dir: &'a Path,
    /// The rounds per arm.
    rounds: usize,
}

/// Run one scenario and return its measurements.
fn run_scenario(
    run: &ScenarioRun<'_>,
    counter: &mut usize,
) -> Result<(Vec<Measurement>, Vec<Noise>), String> {
    let root = run
        .run_dir
        .join("stores")
        .join(sanitize(&run.scenario.name));
    let mut sample = |binary: &Path| -> Result<Sample, String> {
        *counter += 1;
        let dir = root.join(format!("{counter}"));
        std::fs::create_dir_all(&dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
        let store = dir.join("store");
        let dest = dir.join("dest");
        let pack = dir.join("v1.pack");
        let result = (|| {
            for step in &run.scenario.prepare {
                let args = expand(step, run.fixture, &store, &dest, &pack);
                run_command(binary, &args)?;
            }
            let args = expand(&run.scenario.command, run.fixture, &store, &dest, &pack);
            measure_quiet(binary, &args)
        })();
        let _ = std::fs::remove_dir_all(&dir);
        result
    };
    let arms = run_arms_with(run.base, run.head, run.rounds, &mut sample)?;
    let noise = noise_of(&arms);
    let measurements = measurements(&arms.base, &arms.head, &noise);
    Ok((measurements, noise))
}

/// Replace the placeholders of a command.
fn expand(args: &[String], src: &Path, store: &Path, dest: &Path, pack: &Path) -> Vec<String> {
    let replacements = [
        ("{src}", src),
        ("{store}", store),
        ("{dest}", dest),
        ("{pack}", pack),
    ];
    args.iter()
        .map(|arg| {
            let mut text = arg.clone();
            for (token, path) in replacements {
                text = text.replace(token, &path.display().to_string());
            }
            text
        })
        .collect()
}

/// A name that a path can use.
fn sanitize(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

/// Print the two-revision report.
fn print_revision_report(report: &RevisionReport) {
    let mut out = String::new();
    let _ = writeln!(out, "\n=== base revision against head revision ===");
    let _ = writeln!(
        out,
        "  base revision: {} ({})",
        report.base_revision, report.base_short
    );
    let _ = writeln!(
        out,
        "  head revision: {} ({})",
        report.head_revision, report.head_short
    );
    let _ = writeln!(out, "  base binary: blake3 {}", report.base_hash);
    let _ = writeln!(out, "  head binary: blake3 {}", report.head_hash);
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
    for result in &report.scenarios {
        let _ = writeln!(
            out,
            "\n  scenario {} ({})",
            result.scenario.name,
            result.scenario.shape.name()
        );
        let _ = writeln!(out, "    paths: {}", result.scenario.paths.join(" "));
        let _ = writeln!(out, "    command: {}", result.scenario.command.join(" "));
        let _ = writeln!(out, "    status: {}", result.status);
        if let Some(reason) = &result.reason {
            let _ = writeln!(out, "    reason: {reason}");
        }
        if let Some(fixture) = &result.fixture {
            let _ = writeln!(out, "    fixture: {}", fixture.display());
        }
        if result.status == "ran" {
            let _ = writeln!(out, "    metric: {}", result.scenario.metric.name());
            print_table(&result.measurements, report.host.noisy, &mut out);
            print_noise(&result.noise, &mut out);
            match result.verdict {
                Some(verdict) => {
                    let _ = writeln!(out, "    verdict: {verdict}");
                }
                None if report.host.noisy => {
                    let _ = writeln!(out, "    verdict: none, the host is noisy");
                }
                None => {
                    let _ = writeln!(out, "    verdict: unmeasurable");
                }
            }
        }
    }
    if !report.changed_paths.is_empty() {
        let _ = writeln!(out, "\n  changed paths: {}", report.changed_paths.join(" "));
        if report.uncovered_paths.is_empty() {
            let _ = writeln!(out, "  every changed path has a scenario");
        } else {
            let _ = writeln!(
                out,
                "  changed paths with no scenario: {}",
                report.uncovered_paths.join(" ")
            );
        }
    }
    if report.options.keep {
        let _ = writeln!(out, "\n  worktrees kept in {}", report.run_dir);
    } else {
        let _ = writeln!(out, "\n  worktrees removed");
    }
    print!("{out}");
}

/// The path of the two-revision report file.
fn revision_report_path(report: &RevisionReport) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    report.repo.join("reports").join("bench").join(format!(
        "bench-{}-vs-{}-{stamp}.json",
        report.base_short, report.head_short
    ))
}

/// Write the full data of the two-revision run as JSON.
fn write_revision_report(report: &RevisionReport) -> Result<(), String> {
    let path = revision_report_path(report);
    write_json(&path, &revision_json(report))?;
    println!("\n  report: {}", path.display());
    Ok(())
}

/// Write one JSON value to a file.
fn write_json(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(value)
        .map_err(|error| format!("cannot write the report: {error}"))?;
    std::fs::write(path, text)
        .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    Ok(())
}

/// The full data of the two-revision run, as JSON.
fn revision_json(report: &RevisionReport) -> serde_json::Value {
    let scenarios: Vec<serde_json::Value> = report
        .scenarios
        .iter()
        .map(|result| {
            serde_json::json!({
                "name": result.scenario.name,
                "shape": result.scenario.shape.name(),
                "paths": result.scenario.paths,
                "fixture": result.scenario.fixture.json(),
                "prepare": result.scenario.prepare,
                "command": result.scenario.command,
                "metric": result.scenario.metric.name(),
                "status": result.status,
                "reason": result.reason,
                "measurements": measurements_json(&result.measurements),
                "noise_floor": noise_json(&result.noise),
                "verdict": result.verdict,
            })
        })
        .collect();
    let mut verdicts = serde_json::Map::new();
    for result in &report.scenarios {
        let verdict = result.verdict.map_or(serde_json::Value::Null, |verdict| {
            serde_json::Value::String(verdict.to_owned())
        });
        verdicts.insert(result.scenario.name.clone(), verdict);
    }
    serde_json::json!({
        "generated": jiff::Timestamp::now().to_string(),
        "base": {
            "revision": report.base_revision,
            "short": report.base_short,
            "blake3": report.base_hash,
        },
        "head": {
            "revision": report.head_revision,
            "short": report.head_short,
            "blake3": report.head_hash,
        },
        "rounds": report.options.rounds,
        "floor_rounds": FLOOR_ROUNDS,
        "scenarios": scenarios,
        "changed_paths": report.changed_paths,
        "uncovered_paths": report.uncovered_paths,
        "host": {
            "cores": report.host.cores,
            "load_before": report.host.load_before,
            "load_after": report.host.load_after,
            "free_memory_kb": report.host.free_memory_kb,
            "noisy": report.host.noisy,
        },
        "verdict": overall_verdict(report),
        "verdicts": verdicts,
        "run_dir": report.run_dir,
        "worktrees_kept": report.options.keep,
    })
}

/// The run verdict, when one scenario ran and the host was quiet.
fn overall_verdict(report: &RevisionReport) -> Option<&'static str> {
    if report.host.noisy {
        return None;
    }
    let ran: Vec<&ScenarioResult> = report
        .scenarios
        .iter()
        .filter(|result| result.status == "ran")
        .collect();
    match ran.as_slice() {
        [one] => one.verdict,
        _ => None,
    }
}
