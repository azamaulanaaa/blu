//! Saying how a run is going while it is going.
//!
//! There is no percentage to report. Content-defined chunking finds each
//! boundary from the bytes before it. The chunk count of a tree stays unknown
//! until the run reads the tree. The run reports bytes read or written
//! instead. It also reports counts, elapsed time, and a rate derived from
//! those. A timer that [`Progress::report`] checks sets the pace. So a fast run
//! stays quiet. So a slow run is not left silent for minutes.

use std::{
    fmt,
    time::{Duration, Instant},
};

/// How often a run says something. The interval is short enough that a slow run
/// is not left silent for long. It is long enough that a fast run does not fill
/// the terminal.
const INTERVAL: Duration = Duration::from_millis(300);

/// The `tracing` target a reported line is emitted on.
///
/// **A constant rather than a literal at the callsite, and not only for tidiness.**
/// It is half of the contract between this module and [`crate::serve::progress_layer`],
/// which reads events on this target and ignores every other one. A literal that
/// drifted from the reader's would mean no progress ever reaches a client, with
/// nothing failing — so the reader names this one, and
/// `every_callsite_the_layer_reads_is_admitted` checks the two against each other.
pub(crate) const TARGET: &str = "lepatch::progress";

/// What is being reported on.
///
/// A value rather than a `&'static str`, for the same reason the counters are.
/// These are a closed set, and a closed set is what an `enum` is for. The
/// counters were strings at ten call sites across three files. Only
/// `BYTE_COUNTERS` recorded that, and it was a two-element array. The array
/// names the two counters that are byte totals. A typo in `"written"` compiled.
/// It passed every test. It silently rendered a byte count as a raw integer
/// with no rate on it. That diagnostic looks deliberate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Reading a tree and writing what is new.
    Backup,
    /// Writing a tree back out.
    Restore,
    /// Writing only the bytes a destination is missing, from a pack.
    Apply,
    /// Reading every chunk back and hashing it.
    Verify,
    /// Adding a pack to a backup set.
    Merge,
}

impl Phase {
    /// The word it reports under, which is the command name.
    const fn name(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Apply => "apply",
            Self::Verify => "verify",
            Self::Merge => "merge",
        }
    }
}

/// One of the running totals a phase keeps.
///
/// Closed, and each variant says whether its total is a byte count. A caller
/// cannot misspell a counter. The renderer cannot disagree with its callers
/// about which totals need a size and a rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counter {
    /// Files walked.
    Files,
    /// Chunks produced.
    Chunks,
    /// Distinct chunks stored.
    Distinct,
    /// Chunks checked.
    Checked,
    /// Files written.
    Done,
    /// Bytes read from the tree.
    Read,
    /// Bytes written to the destination.
    Written,
}

impl Counter {
    /// Every counter, so [`Progress`] can size its table.
    const ALL: [Self; 7] = [
        Self::Files,
        Self::Chunks,
        Self::Distinct,
        Self::Checked,
        Self::Done,
        Self::Read,
        Self::Written,
    ];

    /// The name it reports under, byte-identical to the string it replaced.
    const fn name(self) -> &'static str {
        match self {
            Self::Files => "files",
            Self::Chunks => "chunks",
            Self::Distinct => "distinct",
            Self::Checked => "checked",
            Self::Done => "done",
            Self::Read => "read",
            Self::Written => "written",
        }
    }

    /// Whether its total is a byte count, and so worth a human size and a rate.
    ///
    /// A property of the variant, not a list that must stay in step with the
    /// callers. This is the whole point. Adding a counter forces the question.
    const fn is_bytes(self) -> bool {
        matches!(self, Self::Read | Self::Written)
    }

    /// The slot this counter's total lives in.
    const fn slot(self) -> usize {
        match self {
            Self::Files => 0,
            Self::Chunks => 1,
            Self::Distinct => 2,
            Self::Checked => 3,
            Self::Done => 4,
            Self::Read => 5,
            Self::Written => 6,
        }
    }
}

/// Every counter's wire name, for a reader that is not this module.
///
/// **This is the contract between the emitter and `lepatch serve`**, which
/// forwards these as `tracing` fields and matches on the names. A counter added to
/// [`Counter`] without appearing here reaches no client, and nothing fails — so
/// the sink compares the two lists rather than trusting either.
#[must_use]
pub fn counter_names() -> Vec<&'static str> {
    Counter::ALL.iter().map(|counter| counter.name()).collect()
}

/// Accumulates what a run has done and decides when to say so.
#[derive(Debug)]
pub struct Progress {
    what: Phase,
    started: Instant,
    /// When a line was last emitted. `None` until one has been, so the first
    /// tick always reports.
    last_reported: Option<Instant>,
    /// Running totals, in the order they are reported.
    ///
    /// A fixed table, not a `Vec` of pairs. The backup path calls `add` and `set`
    /// once per chunk. Each call was a linear scan of a `Vec` to find its entry.
    counters: [u64; Counter::ALL.len()],
    /// Which counters this run has touched, in report order.
    ///
    /// A counter that nothing has reported has no line. That is what the caller
    /// wants. A `restore` does not have a chunk count to show.
    seen: Vec<Counter>,
}

impl Progress {
    /// Start reporting on a run.
    #[must_use]
    pub fn new(what: Phase) -> Self {
        Self {
            what,
            started: Instant::now(),
            last_reported: None,
            counters: [0; Counter::ALL.len()],
            seen: Vec::new(),
        }
    }

    /// Which phase these totals belong to.
    ///
    /// A reader of the final totals needs it: the counters below do not mean
    /// one thing across phases, so a total is only a number once the phase is
    /// attached to it. See [`crate::telemetry::metrics`].
    pub(crate) const fn phase(&self) -> Phase {
        self.what
    }

    /// The totals, in the order they were first reported.
    ///
    /// **A copy of the running state, for the end of a run.** It is what
    /// [`crate::telemetry::metrics::record_run`] reads, and it is the same
    /// rendering path the lines use — a caller cannot get a total that was not
    /// reported.
    ///
    /// Ordered by when each counter was first reported rather than by the enum's
    /// own declaration, so the order matches what a client would have seen.
    #[must_use]
    pub fn totals(&self) -> Vec<(Counter, u64)> {
        self.seen
            .iter()
            .map(|counter| (*counter, self.counters[counter.slot()]))
            .collect()
    }

    /// Add to a running total, which is only read when a line is emitted.
    ///
    /// Saturating, so a total that would wrap reports a large wrong number
    /// rather than a small plausible one.
    pub fn add(&mut self, counter: Counter, amount: u64) {
        let slot = &mut self.counters[counter.slot()];
        *slot = slot.saturating_add(amount);
        self.note(counter);
    }

    /// Set a running total outright, for a value computed elsewhere.
    pub fn set(&mut self, counter: Counter, amount: u64) {
        self.counters[counter.slot()] = amount;
        self.note(counter);
    }

    fn note(&mut self, counter: Counter) {
        if !self.seen.contains(&counter) {
            self.seen.push(counter);
        }
    }

    /// The totals, in the order they were first reported.
    /// Whether enough time has passed to report.
    ///
    /// Always true for the first call, and false immediately after a report.
    ///
    /// Named for the mutation it performs, not as a query. It latches
    /// `last_reported`, so two calls in a row give two different answers. It is
    /// private. That privacy stops a caller writing `if p.due() { p.report() }`
    /// and reporting twice.
    fn due(&mut self) -> bool {
        let now = Instant::now();
        let due = self
            .last_reported
            .is_none_or(|last| now.duration_since(last) >= INTERVAL);

        if due {
            self.last_reported = Some(now);
        }

        due
    }

    /// Emit a line if one is due.
    ///
    /// The return value says whether anything was emitted. That answer distinguishes
    /// a quiet run from a stalled one.
    ///
    /// The line goes to the `lepatch::progress` target, not the default one. So a
    /// user who wants the summary lines without the running commentary can turn
    /// it off with `RUST_LOG=lepatch=info,lepatch::progress=warn`.
    pub fn report(&mut self) -> bool {
        if !self.due() {
            return false;
        }

        let elapsed = self.started.elapsed();

        // **The counters go on the event as structured fields as well as into the
        // message.** The message is for a person and its shape is free to change;
        // the fields are for a program, and `lepatch serve` needs the numbers
        // without parsing prose.
        //
        // The message is byte-identical to what it was before the fields were
        // added, so `RUST_LOG=lepatch=info,lepatch::progress=warn` keeps working
        // and every assertion on log text is unaffected. The fields are named
        // after `Counter::name`, which is what a client is told to expect.
        //
        // This costs nothing when progress is filtered out: `tracing` checks the
        // callsite before it evaluates arguments, and the fields are arguments.
        // The hot-path note below is why that matters here.
        tracing::info!(
            target: TARGET,
            phase = self.what.name(),
            files = self.counters[Counter::Files.slot()],
            chunks = self.counters[Counter::Chunks.slot()],
            distinct = self.counters[Counter::Distinct.slot()],
            checked = self.counters[Counter::Checked.slot()],
            done = self.counters[Counter::Done.slot()],
            read = self.counters[Counter::Read.slot()],
            written = self.counters[Counter::Written.slot()],
            "{} in progress: {}",
            self.what.name(),
            describe(&self.seen, &self.counters, elapsed)
        );

        true
    }
}

/// Render the counters, with byte totals turned into a size and a rate.
///
/// Takes the two arrays the counters live in, not a `Vec` of pairs. So building
/// the line allocates nothing but the line itself. The old code collected the
/// pairs into a fresh `Vec` on every tick. The code then iterated that `Vec`
/// once immediately afterwards. The plan called that "2 strings per counter per
/// tick". That count was three allocations per tick, because `totals` was one
/// of them.
///
/// A tick is [`INTERVAL`] apart. So this change removes about ten allocations a
/// second on a backup that does thousands of chunk operations a second. This
/// magnitude is worth stating once so that a reader does not pick it up again.
/// This is not a throughput change, and nothing here should be described as
/// one. This is a hot loop that used to allocate a diagnostic string. The loop
/// no longer allocates.
///
/// The rendering is not deferred to the subscriber, and it does not need to be.
/// `tracing` checks the callsite before it evaluates its arguments. So with
/// progress off, none of this runs at all. Progress is off for `--quiet`, and
/// for any subscriber that filters this target. The gate already exists, and
/// it is the right one. The allocations below only ever happened when the line
/// was going to be written.
fn describe(seen: &[Counter], counters: &[u64], elapsed: Duration) -> String {
    let mut out = String::new();
    for counter in seen {
        if !out.is_empty() {
            out.push(' ');
        }

        let total = counters[counter.slot()];
        let name = counter.name();
        if counter.is_bytes() {
            let (whole, tenth) = mib_per_second_tenths(total, elapsed);
            let _ = std::fmt::Write::write_fmt(
                &mut out,
                format_args!("{name}={} ({whole}.{tenth} MiB/s)", Bytes(total)),
            );
        } else {
            let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{name}={total}"));
        }
    }

    out
}

/// A byte count, rendered as a size.
///
/// A [`fmt::Display`] newtype, not a `String`. The caller writes it into a
/// formatter it already has. The caller does not build a temporary to hand
/// over. Three per second is not a throughput argument. The real argument is
/// that a function returning `String` from a `u64` is a heap allocation dressed
/// up as a conversion.
struct Bytes(u64);

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNITS: [(&str, u64); 4] = [
            ("TiB", 1 << 40),
            ("GiB", 1 << 30),
            ("MiB", 1 << 20),
            ("KiB", 1 << 10),
        ];

        for (unit, scale) in UNITS {
            if self.0 >= scale {
                // The remainder is under `scale`, so times ten cannot overflow.
                return write!(
                    f,
                    "{}.{} {unit}",
                    self.0 / scale,
                    (self.0 % scale) * 10 / scale
                );
            }
        }

        write!(f, "{} B", self.0)
    }
}

/// Bytes per second in tenths of a MiB/s, split into whole and tenths so it
/// prints without a float.
///
/// Integer throughout, with a `u128` intermediate so the multiply cannot
/// overflow.
fn mib_per_second_tenths(bytes: u64, elapsed: Duration) -> (u64, u64) {
    let millis = elapsed.as_millis();
    if millis == 0 {
        return (0, 0);
    }

    let tenths = (u128::from(bytes) * 10_000) / (millis * 1024 * 1024);
    let tenths = u64::try_from(tenths).unwrap_or(u64::MAX);

    (tenths / 10, tenths % 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counters and their totals as pairs, for asserting on.
    ///
    /// A thin alias for [`Progress::totals`], kept so the assertions below read
    /// as `totals(&progress)` rather than `progress.totals()`.
    fn totals(progress: &Progress) -> Vec<(Counter, u64)> {
        progress.totals()
    }

    /// The phase is part of what a total means, so it is readable.
    ///
    /// A caller that cannot tell a backup's `distinct` from a merge's has two
    /// numbers and one meaning, which is the confusion this crate's metrics are
    /// named to prevent.
    #[test]
    fn a_progress_knows_its_phase() {
        assert_eq!(Progress::new(Phase::Backup).phase(), Phase::Backup);
        assert_eq!(Progress::new(Phase::Merge).phase(), Phase::Merge);
    }

    #[test]
    fn the_counters_accumulate_into_one_total() {
        let mut progress = Progress::new(Phase::Backup);
        progress.add(Counter::Files, 2);
        progress.add(Counter::Files, 3);
        progress.set(Counter::Chunks, 10);
        assert_eq!(
            totals(&progress),
            vec![(Counter::Files, 5), (Counter::Chunks, 10)],
            "totals should be in the order they were first reported"
        );
    }

    #[test]
    fn a_counter_nothing_reported_has_no_line() {
        let mut progress = Progress::new(Phase::Restore);
        progress.add(Counter::Done, 1);
        let rendered = describe(&progress.seen, &progress.counters, Duration::from_secs(1));
        assert_eq!(
            rendered, "done=1",
            "a restore has no chunk count to report, and should not invent a line for one"
        );
    }

    #[test]
    fn a_byte_total_is_rendered_with_a_rate() {
        let seen = [Counter::Read];
        let mut counters = [0u64; Counter::ALL.len()];
        counters[Counter::Read.slot()] = 10 * 1024 * 1024;
        let rendered = describe(&seen, &counters, Duration::from_secs(2));
        assert!(rendered.contains("10.0 MiB"), "{rendered}");
        assert!(rendered.contains("5.0 MiB/s"), "{rendered}");
    }

    #[test]
    fn a_count_is_left_as_a_plain_number() {
        // A rate on a count of chunks or files would be meaningless.
        let seen = [Counter::Chunks];
        let mut counters = [0u64; Counter::ALL.len()];
        counters[Counter::Chunks.slot()] = 1234;
        let rendered = describe(&seen, &counters, Duration::from_secs(1));
        assert_eq!(rendered, "chunks=1234");
    }

    /// The unit a counter reports is a property of the counter. It is not a list
    /// somebody has to keep in step.
    ///
    /// This property was absent before. A typo in `"written"` silently downgraded a
    /// byte count to a raw integer. The only record of which counters are byte
    /// totals was a two-element string array. Nothing checked that array
    /// against the strings used at the call sites.
    #[test]
    fn only_byte_counters_get_a_size_and_a_rate() {
        // A second's worth of a mebibyte, so the rate is a number and not a
        // rounded zero. What is being checked is *which* counters get the
        // treatment, not the arithmetic. The test
        // `a_byte_total_is_rendered_with_a_rate` covers the arithmetic. So the
        // totals are chosen to keep that distinction legible.
        let (whole, tenth) = mib_per_second_tenths(1 << 20, Duration::from_secs(1));
        assert_eq!((whole, tenth), (1, 0), "one MiB in a second is one MiB/s");

        for counter in Counter::ALL {
            let mut counters = [0u64; Counter::ALL.len()];
            counters[counter.slot()] = 2048;
            let rendered = describe(
                std::slice::from_ref(&counter),
                &counters,
                Duration::from_secs(1),
            );
            if counter.is_bytes() {
                assert!(
                    rendered.contains(" MiB/s)"),
                    "a byte total should carry a rate: {rendered}"
                );
                assert!(
                    rendered.contains("2.0 KiB"),
                    "a byte total should carry a human size: {rendered}"
                );
            } else {
                assert_eq!(
                    rendered,
                    format!("{}=2048", counter.name()),
                    "a count should be left as a plain number, with no size and no rate"
                );
            }
        }
    }

    #[test]
    fn byte_units_scale() {
        assert_eq!(Bytes(512).to_string(), "512 B");
        assert_eq!(Bytes(2048).to_string(), "2.0 KiB");
        assert_eq!(Bytes(5 * 1024 * 1024).to_string(), "5.0 MiB");
    }

    #[test]
    fn the_first_tick_is_due_and_every_one_after_it_is_not() {
        let mut progress = Progress::new(Phase::Backup);
        assert!(progress.due());
        // And then it goes quiet.
        assert!(!progress.due());
        assert!(!progress.due());
    }

    /// Every counter has a distinct slot, or two counters share a total.
    #[test]
    fn every_counter_has_its_own_slot() {
        let mut slots = Counter::ALL.map(Counter::slot);
        slots.sort_unstable();
        assert_eq!(
            slots,
            [0, 1, 2, 3, 4, 5, 6],
            "slots should be a permutation"
        );
    }

    /// Every phase reports under its own command name.
    ///
    /// The `match` in `name` makes a forgotten variant a compile error. So what
    /// this test actually guards is the *string*. The string appears in every
    /// line the user sees. A rename there is a user-visible change with no other
    /// signal.
    #[test]
    fn every_phase_is_named() {
        // Every variant, spelled out. `Apply` was absent here and `Merge` would
        // have joined it: the list was written by hand, so a phase added above
        // without being added here was a phase whose name nothing checked.
        let names: Vec<&str> = [
            Phase::Backup,
            Phase::Restore,
            Phase::Apply,
            Phase::Verify,
            Phase::Merge,
        ]
        .iter()
        .map(|p| p.name())
        .collect();
        assert_eq!(names, ["backup", "restore", "apply", "verify", "merge"]);
    }
}
