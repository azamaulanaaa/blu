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
#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

#[path = "../benches/fixture.rs"]
mod fixture;

// `ab.rs`'s generator, for the same reason. It is a *different* generator and
// that is deliberate: the bench fixture builds files that share a prefix, because
// deduplication moving work out of the store is what it measures, while this one
// builds files that share nothing, because a shared prefix would hide the store
// behind the reader. Neither is reusable for the other's job, and both have to
// satisfy the properties below.
#[path = "../examples/fixtures/ab.rs"]
mod ab_fixture;

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

/// The properties, run against both generators.
///
/// These were three `#[test]` functions inside `examples/ab.rs`, guarding that
/// example's own generator. An example is its own crate and `cargo test` does not
/// run its `#[cfg(test)]` items, so they were only checked by whoever happened to
/// run the example — and an example is the first thing anyone deletes as "just a
/// benchmark". They are here now, and they check *both* generators rather than
/// one, which is the other half of the win: the bench fixture had the same two
/// properties written out again, and a fourth copy was never going to happen.
///
/// Each takes the generator as a function, so the property is written once and the
/// two fixtures are checked against it. A generator that drifts back to a
/// counter fails whichever one it is, with the property's own message saying
/// what a chunk-sized repeated run would mean for the measurement.
mod both {
    use super::ab_fixture::noisy as ab;
    use super::fixture::noisy as bench;

    use std::collections::{HashMap, HashSet};

    /// Files per tree. Fewer than either benchmark uses, because the property is
    /// a pairwise search and the failure it looks for is total rather than
    /// marginal.
    const FILES: usize = 24;

    /// Comfortably above the chunker's minimum, so a real chunk would collapse
    /// and this catches it.
    const WINDOW: usize = 8 * 1024;

    /// A chunker can only deduplicate a run of at least a minimum chunk, so a run
    /// this long would be thrown away by the real pipeline.
    const MIN_CHUNK: usize = 256;

    const FILE_BYTES: usize = 16 * 1024;

    /// Longest run of bytes appearing in both slices, at any alignment.
    ///
    /// Indexed by a 16-byte gram and extended from each hit, so this is linear
    /// rather than quadratic. The quadratic version took 43 seconds, which is too
    /// slow to be a test anybody keeps.
    ///
    /// Checking one alignment is not enough, and that is the whole point of the
    /// gram: if one file is another shifted by a single byte, every aligned
    /// window of one lands in the middle of a window of the other and they all
    /// differ. A test that compares aligned windows passes on exactly the case
    /// that went wrong, which is the case a counter-seeded generator produces.
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

        let mut index: HashMap<[u8; GRAM], Vec<usize>> = HashMap::new();
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

    /// No two files may share a run at *any* offset.
    ///
    /// The property the pipeline actually depends on, checked over the whole
    /// tree. It is not a claim about how the bytes were made.
    fn no_overlap<F>(noisy: F)
    where
        F: Fn(usize, u64) -> Vec<u8>,
    {
        let first = noisy(FILE_BYTES, 1000);
        for i in 1..FILES {
            let other = noisy(FILE_BYTES, 1000 + u64::try_from(i).expect("fits in u64"));
            let longest = longest_common_run(&first, &other);
            assert!(
                longest < MIN_CHUNK,
                "file 0 and file {i} share a {longest}-byte run at some offset; the tree \
                 deduplicates against itself and the benchmark is measuring a reader with \
                 almost no store work to do"
            );
        }
    }

    /// Two different seeds must not produce the same bytes at a different offset.
    ///
    /// Kept because it fails with a far more specific message, and because it is
    /// the cheap one to run while iterating on a generator. It also checks that
    /// they differ nearly everywhere, which a weak mixer would not.
    fn seeds_are_unrelated<F>(noisy: F)
    where
        F: Fn(usize, u64) -> Vec<u8>,
    {
        let a = noisy(64 * 1024, 1000);
        let b = noisy(64 * 1024, 1001);

        let longest = longest_common_run(&a, &b);
        assert!(
            longest < MIN_CHUNK,
            "seeds 1000 and 1001 share a {longest}-byte run; the generator is producing \
             shifted copies of one stream"
        );

        let differing = a.iter().zip(&b).filter(|(x, y)| x != y).count();
        let ratio = f64::from(u32::try_from(differing).expect("fits in u32"))
            / f64::from(u32::try_from(a.len()).expect("fits in u32"));
        assert!(
            (0.9..1.0).contains(&ratio),
            "seeds 1000 and 1001 differ in only {:.1}% of positions, which is not random",
            ratio * 100.0
        );
    }

    /// No chunk-sized run may be repeated anywhere in the tree.
    ///
    /// The whole-tree form of the same property, and the one that survives: a
    /// two-seed check looks at one pair out of hundreds, and the pairs that
    /// happen to misalign are not the ones it picks.
    fn no_repeated_window<F>(noisy: F)
    where
        F: Fn(usize, u64) -> Vec<u8>,
    {
        let mut seen: HashSet<[u8; 16]> = HashSet::new();
        let mut windows = 0usize;

        for i in 0..FILES {
            let bytes = noisy(FILE_BYTES, 1000 + u64::try_from(i).expect("fits in u64"));
            for at in (0..bytes.len()).step_by(WINDOW) {
                let mut key = [0u8; 16];
                key.copy_from_slice(&bytes[at..at + 16]);
                windows += 1;
                assert!(
                    seen.insert(key),
                    "file {i} repeats a {WINDOW}-byte window at offset {at}; the generator is \
                     producing overlapping copies of one stream, so the tree deduplicates against \
                     itself and the benchmark measures a reader with no store work"
                );
            }
        }

        // A fixture that dedupes is not a slow fixture, it is a different one.
        assert_eq!(windows, FILES * (FILE_BYTES / WINDOW));
    }

    #[test]
    fn ab_no_two_files_overlap_at_any_offset() {
        no_overlap(ab);
    }

    #[test]
    fn ab_different_seeds_share_no_long_run() {
        seeds_are_unrelated(ab);
    }

    #[test]
    fn ab_no_chunk_sized_run_is_repeated_in_the_tree() {
        no_repeated_window(ab);
    }

    #[test]
    fn the_bench_fixture_overlaps_neither() {
        no_overlap(bench);
        seeds_are_unrelated(bench);
        no_repeated_window(bench);
    }
}
