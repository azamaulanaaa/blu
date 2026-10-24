//! Write a benchmark fixture tree.
//!
//! The benchmark harness needs a tree of files with a known shape.
//! The audit of the perf commits wrote those trees with a Python script.
//! That put an interpreter and a second byte generator between the benchmark
//! and the repository.
//! This example writes the same trees with the repository's own generator.
//!
//! # The generator
//!
//! `benches/fixture.rs` holds the generator.
//! `tests/fixtures.rs` holds the property test that guards it.
//! A bench target is its own crate, so this example includes the generator by
//! path, the way the test does.
//! There is one generator and one property test.
//!
//! # The shapes
//!
//! - `tree` writes a shared prefix, then bytes that no other file holds.
//! - `identical` writes the same bytes in every file.
//! - `unique` writes bytes that no other file holds.
//! - `compressible` writes a pattern that repeats in every block.
//! - `mutate` writes new bytes into one file of an existing tree.
//!
//! # Use
//!
//! ```console
//! $ cargo run --release --example mktree -- tree DIR FILES BYTES SHARED
//! $ cargo run --release --example mktree -- identical DIR FILES BYTES
//! $ cargo run --release --example mktree -- unique DIR FILES BYTES
//! $ cargo run --release --example mktree -- compressible DIR FILES BYTES
//! $ cargo run --release --example mktree -- mutate DIR
//! ```
//!
//! # The bytes do not change
//!
//! The tree is the measurement, so two runs must write the same bytes.
//! Every seed comes from the shape and the parameters.
//! The clock never reaches the bytes.
//! The `mutate` shape takes its seed from the bytes it replaces.
//! So a second run on the same tree writes the same new bytes.
//! And a second mutation of the same file changes it again.
//!
//! # The directory
//!
//! The four shapes that build a tree empty the directory first.
//! So the tree holds only the files of one shape.
//! The `mutate` shape never removes anything.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../benches/fixture.rs"]
mod fixture;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use fixture::noisy;

/// The shapes this example writes, and the arguments each one takes.
const USAGE: &str = "\
usage: mktree SHAPE ...
  tree DIR FILES BYTES SHARED
  identical DIR FILES BYTES
  unique DIR FILES BYTES
  compressible DIR FILES BYTES
  mutate DIR";

/// The seed base of the `tree` shape.
const TREE: u64 = 1;

/// The seed base of the `identical` shape.
const IDENTICAL: u64 = 2;

/// The seed base of the `unique` shape.
const UNIQUE: u64 = 3;

/// The seed base of the `compressible` shape.
const COMPRESSIBLE: u64 = 4;

/// The seed base of the `mutate` shape.
const MUTATE: u64 = 5;

/// The bytes of the pattern that one block repeats.
const PATTERN_BYTES: usize = 1024;

/// The bytes of one block of a `compressible` file.
const BLOCK_BYTES: usize = 64 * 1024;

/// The seed of one part of one shape.
///
/// The shape and the parameters select the seed, so two runs write the same
/// bytes.
fn seed(base: u64, index: u64) -> u64 {
    base.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(index.wrapping_mul(0xBF58_476D_1CE4_E5B9))
}

/// The number of a file, as the number that the seed uses.
fn number(index: usize) -> u64 {
    u64::try_from(index).unwrap_or(0)
}

/// The name of one file of a tree.
fn file_name(index: usize) -> String {
    format!("f{index:05}.bin")
}

/// The bytes in a tree of `files` files of `bytes` bytes.
fn total_bytes(files: usize, bytes: usize) -> u64 {
    u64::try_from(files).unwrap_or(0) * u64::try_from(bytes).unwrap_or(0)
}

/// A count from the command line.
fn count(raw: &str) -> usize {
    raw.parse::<usize>().unwrap_or_else(|_| {
        eprintln!("{raw} is not a number");
        std::process::exit(2);
    })
}

/// Print how to use the example, and stop.
fn usage() -> ! {
    eprintln!("{USAGE}");
    std::process::exit(2);
}

/// The directory the files go to.
///
/// The example empties the directory first, so the tree holds only the files
/// of one shape.
fn prepare_dir(raw: &str) -> PathBuf {
    let dir = PathBuf::from(raw);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove the directory");
    }
    std::fs::create_dir_all(&dir).expect("create the directory");
    dir
}

/// Write one file of a tree.
fn write_file(dir: &Path, index: usize, content: &[u8]) {
    let path = dir.join(file_name(index));
    std::fs::write(&path, content).expect("write the file");
}

/// Write `files` files into `dir`, and count the different contents.
///
/// The closure holds the bytes of one file.
fn write_contents(dir: &Path, files: usize, content: impl Fn(usize) -> Vec<u8>) -> usize {
    let mut digests: HashSet<[u8; 32]> = HashSet::new();
    for index in 0..files {
        let bytes = content(index);
        digests.insert(*blake3::hash(&bytes).as_bytes());
        write_file(dir, index, &bytes);
    }
    digests.len()
}

/// The bytes of one block of a `compressible` file.
///
/// The block repeats one pattern, so the compressor finds the repeat and the
/// frame ends up smaller than the content.
fn compressible_block(file: usize, block: u64, size: usize) -> Vec<u8> {
    let pattern = noisy(PATTERN_BYTES, seed(COMPRESSIBLE, seed(number(file), block)));
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        let take = PATTERN_BYTES.min(size - out.len());
        out.extend_from_slice(&pattern[..take]);
    }
    out
}

/// The bytes of one `compressible` file.
///
/// Every block holds a different pattern, so the blocks stay distinct.
fn compressible_bytes(len: usize, file: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut block = 0u64;
    while out.len() < len {
        let size = BLOCK_BYTES.min(len - out.len());
        out.extend_from_slice(&compressible_block(file, block, size));
        block += 1;
    }
    out
}

/// The first eight bytes of the hash of some bytes, as a number.
fn digest_word(bytes: &[u8]) -> u64 {
    let digest = blake3::hash(bytes);
    let mut word = [0u8; 8];
    word.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(word)
}

/// New bytes of the same length as `old`.
///
/// The seed comes from the old bytes, so two runs on one tree write the same
/// new bytes, and a second mutation of the same file changes it again.
fn fresh_bytes(old: &[u8]) -> Vec<u8> {
    if old.is_empty() {
        return Vec::new();
    }

    let mut index = seed(MUTATE, digest_word(old));
    let mut fresh = noisy(old.len(), index);
    while fresh == old {
        index = index.wrapping_add(1);
        fresh = noisy(old.len(), index);
    }
    fresh
}

/// What one run wrote.
struct Written {
    /// The first line: the shape and the counts.
    header: String,
    /// The directory that holds the tree.
    dir: PathBuf,
    /// One line per fact about the tree.
    lines: Vec<String>,
}

impl Written {
    /// Print what the run wrote.
    fn print(&self) {
        let header = &self.header;
        println!("{header}");
        let dir = self.dir.display();
        println!("  directory: {dir}");
        for line in &self.lines {
            println!("  {line}");
        }
    }
}

/// Write the `tree` shape: a shared prefix, then bytes that no other file
/// holds.
fn write_tree(args: &[String]) -> Written {
    let [dir, files, bytes, shared] = args else {
        usage();
    };

    let dir = prepare_dir(dir);
    let files = count(files);
    let bytes = count(bytes);
    let shared = count(shared);
    if shared > bytes {
        eprintln!("the shared prefix of {shared} bytes does not fit in a file of {bytes} bytes");
        std::process::exit(2);
    }

    let prefix = noisy(shared, seed(TREE, 0));
    let distinct = write_contents(&dir, files, |index| {
        let mut content = prefix.clone();
        content.extend_from_slice(&noisy(bytes - shared, seed(TREE, number(index) + 1)));
        content
    });

    Written {
        header: format!(
            "tree: {files} files x {bytes} bytes, {} bytes in total",
            total_bytes(files, bytes)
        ),
        dir,
        lines: vec![
            format!("the first {shared} bytes are the same in every file"),
            format!(
                "the other {} bytes differ from file to file",
                bytes - shared
            ),
            format!("different file contents in the tree: {distinct}"),
        ],
    }
}

/// Write the `identical` shape: the same bytes in every file.
fn write_identical(args: &[String]) -> Written {
    let [dir, files, bytes] = args else {
        usage();
    };

    let dir = prepare_dir(dir);
    let files = count(files);
    let bytes = count(bytes);

    let content = noisy(bytes, seed(IDENTICAL, 0));
    let distinct = write_contents(&dir, files, |_| content.clone());

    Written {
        header: format!(
            "identical: {files} files x {bytes} bytes, {} bytes in total",
            total_bytes(files, bytes)
        ),
        dir,
        lines: vec![
            format!("every file holds the same {bytes} bytes"),
            format!("different file contents in the tree: {distinct}"),
        ],
    }
}

/// Write the `unique` shape: bytes that no other file holds.
fn write_unique(args: &[String]) -> Written {
    let [dir, files, bytes] = args else {
        usage();
    };

    let dir = prepare_dir(dir);
    let files = count(files);
    let bytes = count(bytes);

    let distinct = write_contents(&dir, files, |index| {
        noisy(bytes, seed(UNIQUE, number(index)))
    });

    Written {
        header: format!(
            "unique: {files} files x {bytes} bytes, {} bytes in total",
            total_bytes(files, bytes)
        ),
        dir,
        lines: vec![
            "no two files hold the same bytes".to_string(),
            format!("different file contents in the tree: {distinct}"),
        ],
    }
}

/// Write the `compressible` shape: a pattern that repeats in every block.
fn write_compressible(args: &[String]) -> Written {
    let [dir, files, bytes] = args else {
        usage();
    };

    let dir = prepare_dir(dir);
    let files = count(files);
    let bytes = count(bytes);

    let distinct = write_contents(&dir, files, |index| compressible_bytes(bytes, index));

    Written {
        header: format!(
            "compressible: {files} files x {bytes} bytes, {} bytes in total",
            total_bytes(files, bytes)
        ),
        dir,
        lines: vec![
            format!(
                "each {BLOCK_BYTES} byte block repeats a {PATTERN_BYTES} byte pattern, and each \
                 block has a different pattern"
            ),
            format!("different file contents in the tree: {distinct}"),
        ],
    }
}

/// Write the `mutate` shape: new bytes in one file of an existing tree.
fn write_mutate(args: &[String]) -> Written {
    let [dir] = args else {
        usage();
    };

    let dir = PathBuf::from(dir);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read the directory")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    if paths.is_empty() {
        eprintln!("the directory {} holds no files", dir.display());
        std::process::exit(2);
    }

    let path = &paths[paths.len() / 2];
    let old = std::fs::read(path).expect("read the file");
    let fresh = fresh_bytes(&old);
    std::fs::write(path, &fresh).expect("write the file");

    let files = paths.len();
    let bytes = old.len();
    Written {
        header: format!("mutate: {files} files in the tree, {bytes} bytes in the new file"),
        dir,
        lines: vec![format!(
            "rewrote {} with {bytes} bytes that differ from the old bytes",
            path.display()
        )],
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(shape) = args.first().map(String::as_str) else {
        usage();
    };

    let rest = &args[1..];
    let written = match shape {
        "tree" => write_tree(rest),
        "identical" => write_identical(rest),
        "unique" => write_unique(rest),
        "compressible" => write_compressible(rest),
        "mutate" => write_mutate(rest),
        _ => usage(),
    };

    written.print();
}
