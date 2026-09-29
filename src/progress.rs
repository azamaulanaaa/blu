//! Saying how a run is going while it is going.
//!
//! There is no percentage to report: content-defined chunking finds each boundary
//! from the bytes before it, so a tree's chunk count is unknown until it is read.
//! What is reported instead is bytes read or written, counts, elapsed time, and a
//! rate derived from those, on a timer that [`Progress::report`] checks — so a
//! fast run stays quiet and a slow one is not left silent for minutes.

use std::time::{Duration, Instant};

/// How often a run says something: short enough that a slow run is not left
/// silent for long, long enough that a fast one does not fill the terminal.
const INTERVAL: Duration = Duration::from_millis(300);

/// What is being reported on.
///
/// A value rather than a `&'static str` for the same reason the counters are:
/// these are a closed set, and a closed set is what an `enum` is for. The
/// counters were strings at ten call sites across three files, and
/// `BYTE_COUNTERS` — a two-element array naming the two that are byte totals —
/// was the only thing recording that. A typo in `"written"` compiled, passed
/// every test, and silently rendered a byte count as a raw integer with no rate
/// on it, which is a diagnostic that looks deliberate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Reading a tree and writing what is new.
    Backup,
    /// Writing a tree back out.
    Restore,
    /// Reading every chunk back and hashing it.
    Verify,
}

impl Phase {
    /// The word it reports under, which is the command name.
    const fn name(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Verify => "verify",
        }
    }
}

/// One of the running totals a phase keeps.
///
/// Closed, and each variant says whether its total is a byte count, so a counter
/// cannot be misspelled and the renderer cannot disagree with its callers about
/// which totals are worth showing as a size and a rate.
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
    /// A property of the variant rather than a list kept in step with the
    /// callers, which is the whole point: adding a counter forces the question.
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
    /// A fixed table rather than a `Vec` of pairs: `add` and `set` are called
    /// once per chunk on the backup path, and each was a linear scan of a
    /// `Vec` to find its own entry.
    counters: [u64; Counter::ALL.len()],
    /// Which counters this run has touched, in report order.
    ///
    /// A counter nothing has reported has no line, which is what the caller
    /// wants: a `restore` does not have a chunk count to show.
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
    fn totals(&self) -> Vec<(Counter, u64)> {
        self.seen
            .iter()
            .map(|c| (*c, self.counters[c.slot()]))
            .collect()
    }

    /// Whether enough time has passed to report.
    ///
    /// Always true for the first call, and false immediately after a report.
    ///
    /// Named for the mutation it performs rather than as a query: it latches
    /// `last_reported`, so calling it twice in a row gives two different
    /// answers. It is private, which is what stops a caller writing
    /// `if p.due() { p.report() }` and reporting twice.
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
    /// The return value says whether anything was emitted, which is what
    /// distinguishes a quiet run from a stalled one.
    ///
    /// Emitted on the `lepatch::progress` target rather than the default one, so
    /// a user who wants the summary lines without the running commentary can
    /// turn it off with `RUST_LOG=lepatch=info,lepatch::progress=warn`.
    pub fn report(&mut self) -> bool {
        if !self.due() {
            return false;
        }

        let elapsed = self.started.elapsed();
        tracing::info!(
            target: "lepatch::progress",
            "{} in progress: {}",
            self.what.name(),
            describe(&self.totals(), elapsed)
        );

        true
    }
}

/// Render the counters, with byte totals turned into a size and a rate.
fn describe(counters: &[(Counter, u64)], elapsed: Duration) -> String {
    let mut out = String::new();
    for (counter, total) in counters {
        if !out.is_empty() {
            out.push(' ');
        }

        let name = counter.name();
        if counter.is_bytes() {
            let (whole, tenth) = mib_per_second_tenths(*total, elapsed);
            let _ = std::fmt::Write::write_fmt(
                &mut out,
                format_args!("{name}={} ({whole}.{tenth} MiB/s)", human_bytes(*total)),
            );
        } else {
            let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{name}={total}"));
        }
    }

    out
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

/// Bytes as something readable, choosing the unit by magnitude.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
    ];

    for (unit, scale) in UNITS {
        if bytes >= scale {
            // The remainder is under `scale`, so times ten cannot overflow.
            return format!("{}.{} {unit}", bytes / scale, (bytes % scale) * 10 / scale);
        }
    }

    format!("{bytes} B")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate() {
        let mut progress = Progress::new(Phase::Backup);
        progress.add(Counter::Files, 2);
        progress.add(Counter::Files, 3);
        progress.set(Counter::Chunks, 10);
        assert_eq!(
            progress.totals(),
            vec![(Counter::Files, 5), (Counter::Chunks, 10)],
            "totals should be in the order they were first reported"
        );
    }

    #[test]
    fn a_counter_nothing_reported_has_no_line() {
        let mut progress = Progress::new(Phase::Restore);
        progress.add(Counter::Done, 1);
        let rendered = describe(&progress.totals(), Duration::from_secs(1));
        assert_eq!(
            rendered, "done=1",
            "a restore has no chunk count to report, and should not invent a line for one"
        );
    }

    #[test]
    fn a_byte_total_is_rendered_with_a_rate() {
        let rendered = describe(&[(Counter::Read, 10 * 1024 * 1024)], Duration::from_secs(2));
        assert!(rendered.contains("10.0 MiB"), "{rendered}");
        assert!(rendered.contains("5.0 MiB/s"), "{rendered}");
    }

    #[test]
    fn a_count_is_left_as_a_plain_number() {
        // A rate on a count of chunks or files would be meaningless.
        let rendered = describe(&[(Counter::Chunks, 1234)], Duration::from_secs(1));
        assert_eq!(rendered, "chunks=1234");
    }

    /// The unit a counter reports is a property of the counter, not a list
    /// somebody has to keep in step.
    ///
    /// This is the property whose absence made a typo in `"written"` silently
    /// downgrade a byte count to a raw integer — the only record of which
    /// counters are byte totals was a two-element string array, and nothing
    /// checked it against the strings used at the call sites.
    #[test]
    fn only_byte_counters_get_a_size_and_a_rate() {
        // A second's worth of a mebibyte, so the rate is a number rather than a
        // rounded zero. What is being checked is *which* counters get the
        // treatment, not the arithmetic — which `a_byte_total_is_rendered_with_a_rate`
        // covers — so the totals are chosen to keep that distinction legible.
        let (whole, tenth) = mib_per_second_tenths(1 << 20, Duration::from_secs(1));
        assert_eq!((whole, tenth), (1, 0), "one MiB in a second is one MiB/s");

        for counter in Counter::ALL {
            let rendered = describe(&[(counter, 2048)], Duration::from_secs(1));
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
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
    }

    #[test]
    fn the_first_tick_always_reports() {
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
    /// The `match` in `name` already makes a forgotten variant a compile error,
    /// so what this actually guards is the *string*: it appears in every line the
    /// user sees, and a rename there is a user-visible change with no other
    /// signal.
    #[test]
    fn every_phase_is_named() {
        let names: Vec<&str> = [Phase::Backup, Phase::Restore, Phase::Verify]
            .iter()
            .map(|p| p.name())
            .collect();
        assert_eq!(names, ["backup", "restore", "verify"]);
    }
}
