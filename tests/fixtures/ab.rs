//! `ab.rs`'s byte generator, on its own so the properties guarding it can be
//! tested.
//!
//! This file exists so that `tests/fixtures.rs` can check the property below
//! against this generator.
//! This file also keeps the property from dying with the example.
//! An example is the first thing anyone deletes, because it is "just a
//! benchmark".
//! Three property tests about the generator lived inside `ab.rs` for its whole
//! life, guarded by `#[cfg(test)]` in a target that `cargo test` does not run.
//! Only whoever happened to run the example checked them.
//!
//! Included rather than exported, for the same reason `benches/fixture.rs` is.
//! An example target is its own crate, and there is nowhere for it to import
//! from other than the library.
//! This generator is not part of the library's API.
//! It is a measurement fixture and nothing outside the example and the one test
//! should touch it.
//!
//! It lives here, under `tests/`, because the property is what is durable and
//! the generator is only its witness.
//! The test suite is the guardian of the property.
//! So the thing being guarded belongs to the guardian.
//! A generator whose only claim on the tree is that it is the hardest to delete
//! quietly is a generator nobody will miss.
//! Nobody notices it until the numbers it corrupts are published.
//! Cargo builds every `tests/*.rs` as its own test target and only looks for
//! `tests/*/main.rs` below that.
//! So a file one level down is a module rather than a target.
//! `tests/common/mod.rs` relies on the same accident of layout.
//! Unlike the dodge this file replaces, this one says what the file is.
//!
//! The two generators are separate on purpose.
//! `benches/fixture.rs` builds a tree with a deliberately shared prefix, because
//! deduplication moving work out of the store is the thing being measured.
//! This one builds files that share nothing, for the concurrency curve.
//! Shared prefixes there would hide the store behind the reader.
//! The same properties are asserted against both, and neither generator is reused
//! for the other's job.

/// Pseudo-random bytes with no long runs, so the chunker finds real boundaries.
///
/// xoshiro256\*\* rather than a counter, and that choice matters.
/// Every attempt at this that used a counter got the same wrong answer in a
/// different disguise.
/// A counter's state is an *additive* function of the seed.
/// So `state == (seed + position) * K`, and any two `(seed, position)` pairs with
/// the same sum emit the same byte.
/// The result is a tree whose files are all the same stream at different
/// offsets, which deduplicates away 90% of itself.
///
/// The failure is quiet in the worst way.
/// The numbers look reasonable, and the backup round-trips perfectly.
/// The concurrency curve is smooth, but it is just a curve for a tree with
/// almost nothing left to store.
/// The bug survived a test that compared two specific seeds.
/// The pairs that happen to misalign are the ones a two-seed test looks at.
///
/// xoshiro's state is four words mixed by rotation and multiplication.
/// So a seed selects an unrelated point in the sequence rather than a position
/// in it.
/// `tests/fixtures.rs` checks the property that actually matters.
/// That property is that no chunk-sized run of the generated tree appears
/// twice.
/// The test checks it over the whole tree rather than checking a pair of seeds
/// and hoping.
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
