//! Properties of the paired measurement statistics.
//!
//! The test includes `examples/bench.rs` by path, so the test and the runner
//! cannot drift apart.
//! `cargo test` does not run a test inside an example, so the test lives here.
//! The statistics are what a before/after claim rests on.
#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

// The test never calls the example's `main` or its process code.
#[allow(dead_code)]
#[path = "../examples/bench.rs"]
mod bench;

use bench::{Kind, Noise, Sample, floor_of, median, paired, sign_counts, verdict};

/// The tolerance of the float checks.
const TOLERANCE: f64 = 1e-6;

/// One wall-time run with no CPU or memory reading.
fn sample(wall_ms: f64) -> Sample {
    Sample {
        wall_ms,
        cpu_ms: None,
        rss_kb: None,
    }
}

/// The wall-time differences of two arms, as percentages of the base.
fn wall_diffs(base: &[f64], head: &[f64]) -> Vec<f64> {
    let base: Vec<Sample> = base.iter().copied().map(sample).collect();
    let head: Vec<Sample> = head.iter().copied().map(sample).collect();
    paired(&base, &head, Kind::Wall)
}

/// Check that a number is within the tolerance of its expected value.
fn check_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < TOLERANCE,
        "{actual} is not {expected}"
    );
}

/// The base arm runs 100, 200, 400 and 800 milliseconds.
/// The head arm runs 90, 250, 600 and 880 milliseconds.
/// The round differences are -10%, +25%, +50% and +10%.
/// The upper middle difference is +25%, so the paired median is +25%.
/// The medians of the arms are 400 and 600 milliseconds, so their difference
/// is +50%.
#[test]
fn paired_median_uses_the_round_differences() {
    let base = [100.0, 200.0, 400.0, 800.0];
    let head = [90.0, 250.0, 600.0, 880.0];
    let diffs = wall_diffs(&base, &head);
    assert_eq!(diffs.len(), 4);
    check_close(diffs[0], -10.0);
    check_close(diffs[1], 25.0);
    check_close(diffs[2], 50.0);
    check_close(diffs[3], 10.0);
    check_close(median(&diffs), 25.0);
    let base_median = median(&base);
    let head_median = median(&head);
    check_close(base_median, 400.0);
    check_close(head_median, 600.0);
    check_close((head_median - base_median) / base_median * 100.0, 50.0);
}

/// The head arm wins four rounds and the base arm wins five.
#[test]
fn sign_counts_count_the_round_wins() {
    let diffs = [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
    assert_eq!(sign_counts(&diffs), (4, 5));
}

/// A round with no difference is a win for neither arm.
#[test]
fn sign_counts_ignore_a_tie() {
    assert_eq!(sign_counts(&[-1.0, 0.0, 1.0]), (1, 1));
}

/// The head arm wins six rounds and the base arm wins three.
/// The paired median is negative, so the verdict names the head arm as faster.
#[test]
fn verdict_names_the_faster_arm() {
    assert_eq!(verdict(-2.0, 0.0, 6, 3), "head faster");
}

/// The base arm wins six rounds and the head arm wins three.
/// The paired median is positive, so the verdict names the head arm as slower.
#[test]
fn verdict_names_the_slower_arm() {
    assert_eq!(verdict(2.0, 0.0, 3, 6), "head slower");
}

/// The head arm wins four rounds and the base arm wins five, so the verdict is
/// no difference.
#[test]
fn verdict_calls_a_coin_flip_no_difference() {
    assert_eq!(verdict(1.0, 0.0, 4, 5), "no difference");
}

/// The floor is 1%.
/// An effect of 0.9% is below the floor, so the verdict is unmeasurable.
/// An effect of 1% is the size of the floor, so the verdict is unmeasurable.
/// An effect of 2% is above the floor, so the verdict names the slower arm.
#[test]
fn verdict_treats_the_floor_as_unmeasurable() {
    assert_eq!(verdict(0.9, 1.0, 3, 6), "unmeasurable");
    assert_eq!(verdict(1.0, 1.0, 3, 6), "unmeasurable");
    assert_eq!(verdict(2.0, 1.0, 3, 6), "head slower");
}

/// The noise rounds differ by -1.5%, -3% and +0.75%.
/// The floor is the largest magnitude, 3%.
#[test]
fn noise_floor_is_the_largest_paired_difference() {
    let noise = [Noise {
        kind: Kind::Wall,
        diffs: vec![-1.5, -3.0, 0.75],
    }];
    check_close(floor_of(&noise, Kind::Wall), 3.0);
}
