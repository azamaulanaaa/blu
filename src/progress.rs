//! Saying how a run is going while it is going.
//!
//! A backup of a real dataset takes minutes to hours, and until this existed the
//! only output was one line at the end. A user watching a silent terminal cannot
//! tell a working backup from a hung one, and the first thing they do about that
//! is kill it.
//!
//! There is no percentage to report, and that is worth being honest about: the
//! number of chunks a tree divides into is not known until it has been read,
//! because content-defined chunking finds each boundary from the bytes before it.
//! So this reports what *is* known — how much has been read or written, how many
//! files, and how long it has taken — and a rate derived from those.
//!
//! Reporting is on a timer rather than on a count, so a fast run stays quiet and
//! a slow one is not left silent for minutes at a stretch. [`Progress::report`]
//! decides whether a line is due.

use std::time::{Duration, Instant};

/// How often a run says something.
///
/// Short enough that a slow run does not leave the terminal silent for long
/// while staring, long enough that a fast one does not fill it.
const INTERVAL: Duration = Duration::from_millis(300);

/// Accumulates what a run has done and decides when to say so.
#[derive(Debug)]
pub struct Progress {
    /// What is being reported on, e.g. `"backup"`.
    what: &'static str,
    started: Instant,
    /// When a line was last emitted. `None` until one has been, so that the
    /// first tick always reports: a run that takes four seconds should still say
    /// something, and a run that takes four hours should say it has started.
    last_reported: Option<Instant>,
    /// Running totals, in the order they are reported.
    counters: Vec<(&'static str, u64)>,
}

impl Progress {
    /// Start reporting on a run.
    #[must_use]
    pub fn new(what: &'static str) -> Self {
        Self {
            what,
            started: Instant::now(),
            last_reported: None,
            counters: Vec::new(),
        }
    }

    /// Add to a running total, which is only read when a line is emitted.
    ///
    /// Counters are kept by name rather than as fields so a caller can add to one
    /// without this type having to know what every command measures. A name that
    /// is not already present is created, so a caller that never reports
    /// something simply has no line for it.
    pub fn add(&mut self, counter: &'static str, amount: u64) {
        if let Some((_, total)) = self.counters.iter_mut().find(|(name, _)| *name == counter) {
            *total = total.saturating_add(amount);
        } else {
            self.counters.push((counter, amount));
        }
    }

    /// Set a running total outright, for a value computed elsewhere.
    pub fn set(&mut self, counter: &'static str, amount: u64) {
        if let Some((_, total)) = self.counters.iter_mut().find(|(name, _)| *name == counter) {
            *total = amount;
        } else {
            self.counters.push((counter, amount));
        }
    }

    /// Whether enough time has passed to report.
    ///
    /// Always true for the first call, and false immediately after a report, so
    /// a run always announces itself and then goes quiet until it is worth
    /// speaking again.
    pub fn due(&mut self) -> bool {
        let now = Instant::now();
        let due = self
            .last_reported
            .is_none_or(|last| now.duration_since(last) >= INTERVAL);

        if due {
            self.last_reported = Some(now);
        }

        due
    }

    /// Emit a line if one is due. Returns whether it emitted anything, so a
    /// caller can tell a quiet run from a stalled one.
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
            self.what,
            describe(&self.counters, elapsed)
        );

        true
    }
}

/// Counters whose totals are byte counts, and so are worth showing as a human
/// size and a rate rather than as a raw integer.
const BYTE_COUNTERS: [&str; 2] = ["read", "written"];

/// Render the counters, with byte totals turned into a size and a rate.
fn describe(counters: &[(&'static str, u64)], elapsed: Duration) -> String {
    let mut out = String::new();
    for (name, total) in counters {
        if !out.is_empty() {
            out.push(' ');
        }

        if BYTE_COUNTERS.contains(name) {
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

/// Bytes per second as tenths of a mebibyte per second, split into whole and
/// tenths so it can be printed without a float.
///
/// Integer arithmetic throughout. The crate denies clippy's float-cast lints and
/// they are right to: a byte count is exactly the kind of value that has no
/// business losing precision on the way to a string, and the `u128` intermediate
/// means the multiplication cannot overflow for any run that fits on a disk.
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
        let mut progress = Progress::new("test");
        progress.add("files", 2);
        progress.add("files", 3);
        progress.set("chunks", 10);
        assert_eq!(progress.counters, vec![("files", 5), ("chunks", 10)]);
    }

    #[test]
    fn a_byte_total_is_rendered_with_a_rate() {
        let rendered = describe(&[("read", 10 * 1024 * 1024)], Duration::from_secs(2));
        assert!(rendered.contains("10.0 MiB"), "{rendered}");
        assert!(rendered.contains("5.0 MiB/s"), "{rendered}");
    }

    #[test]
    fn a_count_is_left_as_a_plain_number() {
        // A rate on a count of chunks or files would be meaningless.
        let rendered = describe(&[("chunks", 1234)], Duration::from_secs(1));
        assert_eq!(rendered, "chunks=1234");
    }

    #[test]
    fn byte_units_scale() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
    }

    #[test]
    fn the_first_tick_always_reports() {
        // A run that finishes quickly should still say something, and a run that
        // takes a long time should say it has started.
        let mut progress = Progress::new("test");
        assert!(progress.due());
        // And then it goes quiet, however many times it is asked.
        assert!(!progress.due());
        assert!(!progress.due());
    }
}
