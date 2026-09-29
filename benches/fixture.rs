//! The benchmark fixtures' byte generator, shared by both benches.
//!
//! Included rather than exported, because a bench target is its own crate and
//! there is nowhere for a bench to import from that is not the library. The
//! property test that guards it lives in `tests/fixtures.rs`, which includes
//! this file by path, so the benches and the test cannot drift apart.
//!
//! # Why this is not the obvious thing
//!
//! Every attempt at this that used a counter got the same wrong answer in a
//! different disguise. A counter's state is an *additive* function of the seed,
//! so `state == mix(seed + position)`, and any two `(seed, position)` pairs
//! with the same sum emit the same byte. A tree built from those is not a tree
//! of distinct files: it is one stream, sampled at different offsets, which
//! deduplicates away almost all of itself.
//!
//! Measured on the generator this replaced, at the shape
//! `benches/throughput.rs` used — 200 files of 256 KiB, each half shared and
//! half unique:
//!
//! ```text
//! tree bytes                                 : 52 428 800
//! files containing a 64-byte run from file 0 : 199/199
//! ```
//!
//! Every file contained every other. The benchmark was measuring the cost of
//! storing 128 KiB, 200 times over, and reporting it as a 50 MB tree.
//!
//! The failure is quiet in the worst way. The numbers look reasonable, the
//! backup round-trips perfectly, and the concurrency curve is smooth — it is
//! just a curve for a tree with almost nothing left to store. It survived a test
//! that compared two specific seeds, because the pairs that happen to misalign
//! are the ones a two-seed test looks at.
//!
//! xoshiro256\*\* is used instead: its state is four words mixed by rotation
//! and multiplication, so a seed selects an unrelated point in the sequence
//! rather than a position in it. `no_two_files_overlap_at_any_offset` in
//! `tests/fixtures.rs` checks the property that actually matters, over the whole
//! tree, rather than checking a pair of seeds and hoping.

// `pub` so a bench can reach it, and `unreachable_pub` is silenced here because
// this file is a module of a bench crate rather than of the library: whether
// `noisy` is reachable depends on which target included this file, and all of
// them want it.
#![allow(dead_code, unreachable_pub)]

/// Pseudo-random bytes with no long runs, so the chunker finds real boundaries.
///
/// Content-defined chunking puts boundaries where the data changes, so a
/// constant or short-cycle stream would cut at the maximum size and measure
/// almost nothing.
pub fn noisy(len: usize, seed: u64) -> Vec<u8> {
    // splitmix64, to spread a seed across xoshiro's four words. A zero state is
    // the one thing xoshiro cannot recover from, and seed 0 would produce it.
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
