//! `ab.rs`'s byte generator, on its own so the properties guarding it can be
//! tested.
//!
//! This file exists so that `tests/fixtures.rs` can check the property below
//! against this generator, and so the property does not die with the example. An
//! example is the first thing anyone deletes — it is "just a benchmark" — and
//! three property tests about the generator lived inside `ab.rs` for its whole
//! life, guarded by `#[cfg(test)]` in a target that `cargo test` does not run.
//! They were only checked by whoever happened to run the example.
//!
//! Included rather than exported, for the same reason `benches/fixture.rs` is:
//! an example target is its own crate, and there is nowhere for it to import
//! from that is not the library — and this is not part of the library's API. It is
//! a measurement fixture and nothing outside the example and the one test should
//! touch it.
//!
//! In a subdirectory rather than beside `ab.rs` because cargo builds every
//! `examples/*.rs` as its own target and would fail on the missing `main`. It
//! only looks for `examples/*/main.rs`, so one level down is not a target — the
//! same accident of layout that `benches/fixture.rs` relies on, arrived at from
//! the other direction. Declaring `[[example]]` entries to exclude it also works
//! and is worse: a partly-specified example target inherits the package metadata,
//! so cargo then warns that `edition` is unset and that setting it is deprecated.
//!
//! The two generators are separate on purpose. `benches/fixture.rs` builds a
//! tree with a deliberately shared prefix, because deduplication moving work out
//! of the store is the thing being measured. This one builds files that share
//! nothing, for the concurrency curve, where shared prefixes would hide the store
//! behind the reader. So the same properties are asserted against both, and
//! neither generator is reused for the other's job.

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
/// selects an unrelated point in the sequence rather than a position in it.
/// `tests/fixtures.rs` checks the property that actually matters — that no
/// chunk-sized run of the generated tree appears twice — over the whole tree,
/// rather than checking a pair of seeds and hoping.
#[must_use]
pub(crate) fn noisy(len: usize, seed: u64) -> Vec<u8> {
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
