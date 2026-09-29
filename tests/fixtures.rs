//! Properties of the benchmark fixtures.
//!
//! These live in `tests/` rather than in either bench because a bench is its own
//! crate: nothing runs a bench target's `#[test]` functions as part of
//! `cargo test`, so a property asserted in one would only be checked by
//! whoever happened to run that bench. Here it is checked by every `cargo test`,
//! against the same generator both benches use — the file is included by path
//! rather than copied, so the test and the benchmarks cannot drift apart.
//!
//! The property exists because the failure it guards is silent. A generator
//! whose output is a function of `seed + position` produces a tree that is one
//! stream sampled at different offsets; it round-trips perfectly, the benchmark
//! numbers look reasonable, and the only thing wrong with them is that they
//! measure a tree with almost nothing left to store. `examples/ab.rs` documents
//! the discovery in full; this is the part of it that keeps it from coming
//! back.

#[path = "../benches/fixture.rs"]
mod fixture;

use std::collections::HashSet;

use fixture::noisy;

/// Bytes each generated file holds here. Smaller than either bench uses, so the
/// test is quick, but far larger than [`RUN`] — the property only bites when a
/// file has many offsets at which a run can start, because it is a collision
/// between a *seed* and an *offset* that has to be looked for.
const FILE_BYTES: usize = 16 * 1024;

/// Files in the tree under test.
///
/// Fewer than either bench uses, and deliberately overlapping [`OFFSETS`]: the
/// seeds under test run from 0, and the offsets within a file run from 0, so the
/// two ranges have to meet for the bug to be visible at all. A generator that is
/// a function of `seed + position` collides wherever `seed_i + at` equals
/// `seed_j + at'`, and it takes only a difference of up to `FILES` for that.
const FILES: usize = 24;

/// A run as long as a chunk is likely to be, which is the thing that must not
/// repeat.
///
/// Four KiB is under the default chunker's 8 KiB minimum, so a repeat of this
/// length cannot come from two different chunks being cut at the same place by
/// chance — if it is in the tree, it is in it twice. It is also short enough to
/// turn up by accident, which is what makes this a real test rather than a
/// formality.
const RUN: usize = 4 * 1024;

/// Offsets within each file that are checked.
///
/// `slice::windows` is *overlapping*: every byte offset in a 16 KiB file is a
/// start, so collecting them all is 12,289 four-kilobyte keys per file, about
/// a gigabyte of hashing for a test, and it gets worse in proportion to the
/// file rather than staying constant. Taking the first `OFFSETS` is enough and
/// is not a compromise: a collision between two seeds needs their difference to
/// equal a difference between two offsets, and consecutive offsets from zero
/// cover every seed difference up to `FILES - 1` without missing any.
const OFFSETS: usize = 64;

/// Every run of `RUN` bytes in the tree is unique.
///
/// This is the property that matters, and it is a property of the whole tree
/// rather than of a pair of seeds — which is how the original bug survived a
/// test that compared two specific seeds, since the pairs that happen to
/// misalign are the ones a two-seed test looks at.
#[test]
fn no_two_files_overlap_at_any_offset() {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();

    for i in 0..FILES {
        let file = noisy(FILE_BYTES, u64::try_from(i).unwrap_or(0));
        for (at, window) in file.windows(RUN).take(OFFSETS).enumerate() {
            assert!(
                seen.insert(window.to_vec()),
                "a {RUN}-byte run repeats: file {i} at offset {at} is a copy of something \
                 already in the tree, so this fixture would deduplicate away content it is \
                 meant to be storing"
            );
        }
    }
}

/// Two seeds share no long run, which is the property the two-seed test checked.
///
/// Kept because it is the check that used to exist, and it is the one that
/// passed while the tree was broken.
#[test]
fn different_seeds_share_no_long_run() {
    let a = noisy(FILE_BYTES, 0);
    let b = noisy(FILE_BYTES, 1);

    assert_ne!(a, b, "two seeds produced identical bytes");

    let runs: HashSet<&[u8]> = b.windows(RUN).take(OFFSETS).collect();
    for (at, window) in a.windows(RUN).take(OFFSETS).enumerate() {
        assert!(
            !runs.contains(window),
            "a run in file 0 at offset {at} also appears in file 1, at some other offset"
        );
    }
}

/// The shared prefix a fixture builds on purpose is genuinely shared.
///
/// Both benches deliberately give every file the same first half, so that
/// deduplication has something to do. That is a deliberate property, and this
/// asserts it survived the move to a shared generator — a generator that
/// accidentally made the unique halves identical would collapse this.
#[test]
fn the_shared_prefix_is_shared_and_the_unique_half_is_not() {
    let shared = noisy(FILE_BYTES, 7);

    let first: Vec<u8> = shared.iter().copied().chain(noisy(FILE_BYTES, 0)).collect();
    let second: Vec<u8> = shared.iter().copied().chain(noisy(FILE_BYTES, 1)).collect();

    assert_eq!(
        &first[..FILE_BYTES],
        &second[..FILE_BYTES],
        "the deliberate shared prefix should be identical"
    );
    assert_ne!(
        &first[FILE_BYTES..],
        &second[FILE_BYTES..],
        "the unique half of two files should differ"
    );
    assert_ne!(first, second, "two files should not be byte-identical");
}
