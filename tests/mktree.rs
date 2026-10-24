//! Properties of the fixture trees that `examples/mktree.rs` writes.
//!
//! The benchmark harness reads those trees, so their shape decides what the
//! benchmark measures.
//! A tree that breaks a promise the example makes measures the wrong thing.
//!
//! The property lives in `tests/` rather than in the example for the same
//! reason it lives there for the bench fixtures.
//! `cargo test` does not run an example's `#[test]` functions.
//! A test in `tests/` runs for everyone on every `cargo test`.
//!
//! The test includes the example by path rather than copying it.
//! A copy is a second implementation, and a second implementation is a second
//! answer.
//!
//! The example writes every tree, so the test pins what the example writes
//! and not how the example writes it.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

#[path = "../examples/mktree.rs"]
#[allow(dead_code)]
mod mktree;

use std::path::{Path, PathBuf};

/// The number of files in a tree under test.
///
/// A property that compares each file with the others needs three files.
/// Four files catch a tree where one file alone differs.
const FILES: usize = 4;

/// The number of bytes in one file of a tree under test.
///
/// A `tree` file holds the shared prefix and its own tail, so it holds more
/// bytes than the prefix.
const BYTES: usize = 512;

/// The number of bytes that a `tree` shares between its files.
///
/// The shared prefix must be shorter than a file, or the file has no tail.
const SHARED: usize = 128;

/// The number of bytes in one file of a `compressible` tree under test.
///
/// The example repeats one pattern in each block of a file.
/// The file holds four copies of the pattern, so the repeat exists.
const COMPRESSIBLE_BYTES: usize = mktree::PATTERN_BYTES * 4;

/// The prefix of the line that holds the number of different file contents.
///
/// The example writes this line for every shape.
const DISTINCT_LINE: &str = "different file contents in the tree: ";

/// The path of a directory, as the example reads it.
fn dir_arg(dir: &Path) -> String {
    dir.display().to_string()
}

/// A count, as the example reads it.
fn count_arg(value: usize) -> String {
    value.to_string()
}

/// Write the `tree` shape into `dir`.
///
/// The example empties the directory first, so the tree holds only the files
/// of this shape.
fn tree(dir: &Path, files: usize, bytes: usize, shared: usize) {
    let _ = mktree::write_tree(&[
        dir_arg(dir),
        count_arg(files),
        count_arg(bytes),
        count_arg(shared),
    ]);
}

/// Write the `identical` shape into `dir`.
///
/// The example empties the directory first, so the tree holds only the files
/// of this shape.
fn identical(dir: &Path, files: usize, bytes: usize) {
    let _ = mktree::write_identical(&[dir_arg(dir), count_arg(files), count_arg(bytes)]);
}

/// Write the `unique` shape into `dir`.
///
/// The example empties the directory first, so the tree holds only the files
/// of this shape.
/// The helper returns the number of different file contents that the example
/// counted.
fn unique(dir: &Path, files: usize, bytes: usize) -> usize {
    let written = mktree::write_unique(&[dir_arg(dir), count_arg(files), count_arg(bytes)]);
    distinct(&written)
}

/// Write the `compressible` shape into `dir`.
///
/// The example empties the directory first, so the tree holds only the files
/// of this shape.
fn compressible(dir: &Path, files: usize, bytes: usize) {
    let _ = mktree::write_compressible(&[dir_arg(dir), count_arg(files), count_arg(bytes)]);
}

/// Write the `mutate` shape into `dir`.
///
/// The example never empties the directory, so the tree keeps every file it
/// had before the mutation.
fn mutate(dir: &Path) {
    let _ = mktree::write_mutate(&[dir_arg(dir)]);
}

/// The number of different file contents that a run reported.
fn distinct(written: &mktree::Written) -> usize {
    written
        .lines
        .iter()
        .find_map(|line| line.strip_prefix(DISTINCT_LINE))
        .and_then(|count| count.parse::<usize>().ok())
        .expect("the run reports the number of different file contents")
}

/// The paths of the files in a tree, in the order the names sort.
fn file_paths(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read the tree")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    paths
}

/// The bytes of every file in a tree, in the order the names sort.
fn contents(dir: &Path) -> Vec<Vec<u8>> {
    file_paths(dir)
        .iter()
        .map(|path| std::fs::read(path).expect("read the file"))
        .collect()
}

/// The indexes of the files whose bytes differ between two trees.
fn changed(before: &[Vec<u8>], after: &[Vec<u8>]) -> Vec<usize> {
    assert_eq!(
        before.len(),
        after.len(),
        "the two trees hold a different number of files"
    );
    (0..before.len())
        .filter(|&at| before[at] != after[at])
        .collect()
}

/// Copy the files of a tree into another directory.
///
/// The copy holds the same bytes, so a mutation of each tree must write the
/// same new bytes.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create the directory");
    for path in file_paths(from) {
        let name = path.file_name().expect("the file has a name");
        std::fs::copy(&path, to.join(name)).expect("copy the file");
    }
}

/// Build a shape twice and compare the two trees.
///
/// The shape and the parameters select the bytes, so the two trees must hold
/// the same bytes.
fn same_bytes(build: impl Fn(&Path)) {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    build(&first);
    build(&second);

    let one = contents(&first);
    let other = contents(&second);
    assert_eq!(
        one.len(),
        other.len(),
        "the two runs wrote a different number of files"
    );
    for (at, (a, b)) in one.iter().zip(other.iter()).enumerate() {
        assert!(a == b, "file {at} holds different bytes in the two runs");
    }
}

/// Every file of a `tree` starts with the same bytes.
///
/// The shape shares a prefix on purpose, so the store has something to
/// deduplicate.
#[test]
fn a_tree_gives_every_file_the_same_prefix() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("tree");
    tree(&dir, FILES, BYTES, SHARED);

    let files = contents(&dir);
    assert_eq!(
        files.len(),
        FILES,
        "the tree holds the wrong number of files"
    );
    for (at, file) in files.iter().enumerate().skip(1) {
        assert_eq!(
            &file[..SHARED],
            &files[0][..SHARED],
            "file {at} does not start with the shared prefix"
        );
    }
}

/// Every file of a `tree` has its own tail.
///
/// The bytes after the shared prefix must differ from file to file.
/// A tree whose tails match has almost nothing left to store.
#[test]
fn a_tree_gives_every_file_its_own_tail() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("tree");
    tree(&dir, FILES, BYTES, SHARED);

    let files = contents(&dir);
    assert_eq!(
        files.len(),
        FILES,
        "the tree holds the wrong number of files"
    );
    for at in 0..FILES {
        for other in at + 1..FILES {
            assert_ne!(
                &files[at][SHARED..],
                &files[other][SHARED..],
                "file {at} and file {other} hold the same tail"
            );
        }
    }
}

/// Every file of an `identical` tree holds the same bytes.
///
/// The shape measures a store that keeps one copy of the bytes.
#[test]
fn an_identical_tree_gives_every_file_the_same_bytes() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("identical");
    identical(&dir, FILES, BYTES);

    let files = contents(&dir);
    assert_eq!(
        files.len(),
        FILES,
        "the tree holds the wrong number of files"
    );
    for (at, file) in files.iter().enumerate().skip(1) {
        assert_eq!(file, &files[0], "file {at} holds different bytes");
    }
}

/// Every file of a `unique` tree holds different bytes.
///
/// The example counts the different file contents.
/// The count must equal the number of files.
#[test]
fn a_unique_tree_gives_every_file_different_bytes() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("unique");
    let count = unique(&dir, FILES, BYTES);

    let files = contents(&dir);
    assert_eq!(
        files.len(),
        FILES,
        "the tree holds the wrong number of files"
    );
    for at in 0..FILES {
        for other in at + 1..FILES {
            assert_ne!(
                files[at], files[other],
                "file {at} and file {other} hold the same bytes"
            );
        }
    }
    assert_eq!(
        count, FILES,
        "the example counted {count} different file contents"
    );
}

/// Every file of a `compressible` tree repeats one pattern.
///
/// The example repeats a pattern in each block of a file.
/// The bytes must be the pattern that the example says they are.
#[test]
fn a_compressible_file_holds_a_pattern_that_repeats() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("compressible");
    compressible(&dir, FILES, COMPRESSIBLE_BYTES);

    let files = contents(&dir);
    assert_eq!(
        files.len(),
        FILES,
        "the tree holds the wrong number of files"
    );
    for (at, file) in files.iter().enumerate() {
        assert_eq!(
            file.len(),
            COMPRESSIBLE_BYTES,
            "file {at} holds the wrong number of bytes"
        );
        let pattern = &file[..mktree::PATTERN_BYTES];
        let repeats = file
            .chunks(mktree::PATTERN_BYTES)
            .all(|chunk| chunk == pattern);
        assert!(repeats, "file {at} does not repeat the pattern");
    }
}

/// A frame makes every file of a `compressible` tree smaller.
///
/// The store writes a frame for a payload that shrinks.
/// A fixture that does not shrink measures the wrong work.
#[test]
fn a_compressible_file_gets_smaller_inside_a_frame() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("compressible");
    compressible(&dir, FILES, COMPRESSIBLE_BYTES);

    let files = contents(&dir);
    assert_eq!(
        files.len(),
        FILES,
        "the tree holds the wrong number of files"
    );
    for (at, file) in files.iter().enumerate() {
        let frame = zstd::bulk::compress(file, 1).expect("compress the file");
        assert!(
            frame.len() < file.len(),
            "file {at} does not get smaller inside a frame"
        );
    }
}

/// A mutation writes bytes that differ from the old bytes.
///
/// The example replaces one file of a tree.
/// The new bytes must not be the old bytes.
#[test]
fn a_mutation_changes_the_bytes_of_one_file() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("tree");
    unique(&dir, FILES, BYTES);
    let before = contents(&dir);

    mutate(&dir);

    let after = contents(&dir);
    let changed = changed(&before, &after);
    assert_eq!(
        changed.len(),
        1,
        "the mutation changed {} files",
        changed.len()
    );
    assert_ne!(
        before[changed[0]], after[changed[0]],
        "the mutation wrote the old bytes"
    );
}

/// Two mutations of one tree write the same bytes.
///
/// A tree that has not changed must measure the same on every run.
#[test]
fn two_mutations_of_one_tree_write_the_same_bytes() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    unique(&first, FILES, BYTES);
    copy_tree(&first, &second);

    mutate(&first);
    mutate(&second);

    let one = contents(&first);
    let other = contents(&second);
    assert_eq!(
        one.len(),
        other.len(),
        "the two trees hold a different number of files"
    );
    for (at, (a, b)) in one.iter().zip(other.iter()).enumerate() {
        assert!(
            a == b,
            "file {at} holds different bytes after the mutations"
        );
    }
}

/// A second mutation writes different bytes.
///
/// The first mutation must not freeze the file.
#[test]
fn a_second_mutation_writes_different_bytes() {
    let temp = tempfile::tempdir().expect("a temporary directory");
    let dir = temp.path().join("tree");
    unique(&dir, FILES, BYTES);
    let before = contents(&dir);

    mutate(&dir);
    let first = contents(&dir);

    mutate(&dir);
    let second = contents(&dir);

    let changed = changed(&before, &first);
    assert_eq!(
        changed.len(),
        1,
        "the first mutation changed {} files",
        changed.len()
    );
    assert_ne!(
        first[changed[0]], second[changed[0]],
        "the second mutation wrote the same bytes"
    );
}

/// Two runs with the same parameters write the same bytes.
///
/// The tree is the measurement, so a run must not change what the next run
/// measures.
#[test]
fn two_runs_write_the_same_bytes() {
    same_bytes(|dir| {
        tree(dir, FILES, BYTES, SHARED);
    });
    same_bytes(|dir| {
        identical(dir, FILES, BYTES);
    });
    same_bytes(|dir| {
        let _ = unique(dir, FILES, BYTES);
    });
    same_bytes(|dir| {
        compressible(dir, FILES, COMPRESSIBLE_BYTES);
    });
}
