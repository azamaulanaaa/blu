//! End-to-end tests that drive the `lepatch` binary as a subprocess.
//!
//! The library integration tests call `backup` and `restore` directly, which
//! left the binary itself at zero coverage: version selection, the
//! cross-process write lock, the exclude list, argument parsing, exit codes and
//! every error message the user sees were all untested. These tests go through
//! the real binary so those paths are exercised as a user would meet them.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use common::noisy;
use lepatch::{
    command::{PARTIAL_SUFFIX, load_snapshot},
    layout::IndexSet,
    storage::{BlobId, BlobStore, Codec, VersionIndex},
};

/// Run the binary in `dir` and return its output.
fn lepatch(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("failed to run the lepatch binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn ok(dir: &Path, args: &[&str]) -> String {
    let output = lepatch(dir, args);
    assert!(
        output.status.success(),
        "`lepatch {}` failed with {:?}\nstdout: {}\nstderr: {}",
        args.join(" "),
        output.status.code(),
        stdout(&output),
        stderr(&output)
    );
    stdout(&output)
}

/// Run the binary, require success, and return its **diagnostics**.
///
/// Progress, warnings and summaries go to stderr; stdout carries only the data a
/// command produces. Tests about what a run *said* want this, and tests about what
/// it *produced* want [`ok`].
fn ok_logs(dir: &Path, args: &[&str]) -> String {
    let output = lepatch(dir, args);
    assert!(
        output.status.success(),
        "`lepatch {}` failed with {:?}\nstdout: {}\nstderr: {}",
        args.join(" "),
        output.status.code(),
        stdout(&output),
        stderr(&output)
    );
    stderr(&output)
}

fn fails(dir: &Path, args: &[&str]) -> String {
    let output = lepatch(dir, args);
    assert!(
        !output.status.success(),
        "`lepatch {}` unexpectedly succeeded\nstdout: {}",
        args.join(" "),
        stdout(&output)
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a failure should exit 1, got {:?}\nstderr: {}",
        output.status.code(),
        stderr(&output)
    );
    stderr(&output)
}

/// A tree with the shapes a backup has to survive.
fn build_tree(dir: &Path) {
    let source = dir.join("src");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::create_dir_all(source.join("empty-dir")).unwrap();
    fs::write(source.join("large.bin"), noisy(120_000)).unwrap();
    fs::write(source.join("nested/small.txt"), b"hello\n").unwrap();
    fs::write(source.join("empty-file"), b"").unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("large.bin", source.join("link")).unwrap();
        fs::hard_link(source.join("large.bin"), source.join("hard")).unwrap();
    }
}

#[test]
fn backup_then_restore_reproduces_the_tree() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "snap"]);
    ok(dir.path(), &["restore", "out", "snap"]);

    let before = common::describe(&dir.path().join("src"));
    let after = common::describe(&dir.path().join("out"));

    assert_eq!(before, after, "the restored tree differs from the source");
    assert!(before.contains_key(Path::new("empty-dir")));
    assert!(before.contains_key(Path::new("empty-file")));
}

#[test]
fn each_backup_claims_the_next_version() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    for _ in 0..3 {
        ok(dir.path(), &["backup", "src", "snap"]);
    }

    // The versions are commit records inside the one store file, so three
    // backups leave one file and not three index files. Each version is
    // individually addressable, which is what proves they were all recorded.
    assert!(dir.path().join("snap.0000.bin").is_file());
    assert_eq!(
        count_entries(dir.path(), "snap"),
        3,
        "each backup should add exactly one commit record to the store"
    );

    // And the numbering a user asks for with --version resolves to each of
    // them, oldest first.
    for version in 1..=3 {
        ok(
            dir.path(),
            &[
                "restore",
                &format!("out{version}"),
                "snap",
                "--version",
                &version.to_string(),
            ],
        );
        assert_eq!(
            common::describe(&dir.path().join("src")),
            common::describe(&dir.path().join(format!("out{version}"))),
            "version {version} did not restore the tree it recorded"
        );
    }
}

/// How many commit records the store at `store` holds.
///
/// Read through the store itself rather than by counting files, which is the
/// point: a version is a record, so this is the only count that means anything.
fn count_entries(dir: &std::path::Path, set: &str) -> usize {
    let out = ok(dir, &["versions", set, "--numbers-only"]);
    out.lines().filter(|line| !line.trim().is_empty()).count()
}

/// The index files of the set named `set` in `dir`.
///
/// Spelled out here rather than reached for through the library, because a test
/// that deletes the cache should be deleting it the way a person would: by
/// whatever matches the name.
fn index_files(dir: &std::path::Path, set: &str) -> Vec<std::path::PathBuf> {
    let mut found: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "idx")
                && path
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .is_some_and(|stem| stem.starts_with(&format!("{set}.v")))
        })
        .collect();

    found.sort();
    found
}

#[test]
fn a_second_backup_of_an_unchanged_tree_uploads_only_its_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "snap"]);
    let after_first = fs::metadata(dir.path().join("snap.0000.bin"))
        .unwrap()
        .len();

    ok(dir.path(), &["backup", "src", "snap"]);
    let after_second = fs::metadata(dir.path().join("snap.0000.bin"))
        .unwrap()
        .len();

    let growth = after_second - after_first;
    assert!(
        growth < 8 * 1024,
        "the store grew by {growth} bytes, so chunk data was uploaded again"
    );
}

#[test]
fn restore_can_select_an_earlier_version() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "snap"]);

    // Change the tree, then take a second version.
    fs::write(dir.path().join("src/large.bin"), b"replaced").unwrap();
    ok(dir.path(), &["backup", "src", "snap"]);

    ok(
        dir.path(),
        &["restore", "from-v1", "snap", "--version", "1"],
    );
    assert_eq!(
        fs::read(dir.path().join("from-v1/large.bin")).unwrap(),
        noisy(120_000),
        "version 1 should hold the original contents"
    );

    ok(dir.path(), &["restore", "from-v2", "snap"]);
    assert_eq!(
        fs::read(dir.path().join("from-v2/large.bin")).unwrap(),
        b"replaced",
        "the latest version should hold the replacement"
    );
}

#[test]
fn the_backup_set_is_excluded_from_its_own_backup() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("inner");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("data.bin"), noisy(80_000)).unwrap();

    // Back up the directory the store is about to be written into.
    ok(&source, &["backup", ".", "self"]);

    let listing = fs::read_dir(&source)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    // A set is several files, and the exclusion has to cover all of them — a
    // segment left out of the list would be backed up while being written, which
    // is the failure this test exists for.
    assert!(
        listing.contains(&"self.0000.bin".to_string()),
        "the set should be there and excluded, got {listing:?}"
    );
    assert!(
        !listing.contains(&"self.lock".to_string()),
        "the lock is not content and must not be in the backup: {listing:?}"
    );

    // The store grew while it was being read, so if it had been chunked the
    // offsets would have shifted and the restore would not match.
    ok(dir.path(), &["restore", "out", "inner/self"]);
    assert_eq!(
        fs::read(dir.path().join("out/data.bin")).unwrap(),
        noisy(80_000)
    );
}

#[test]
fn verify_reports_a_clean_store() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    let report = ok(dir.path(), &["verify", "snap"]);

    assert!(
        report.contains("all chunks match their recorded hashes"),
        "unexpected verify output: {report}"
    );
}

/// A set with one byte flipped in the middle of its chunk data.
///
/// The three tests below each need this, and setting it up is six lines — enough
/// that repeating it three times is worse than a name for it. Which is why the
/// original was one test: it was written once, and then three unrelated claims
/// ended up sharing it.
fn damaged_store(dir: &std::path::Path) {
    build_tree(dir);
    ok(dir, &["backup", "src", "snap"]);

    // Flip a byte well inside the chunk data.
    let store = dir.join("snap.0000.bin");
    let mut bytes = fs::read(&store).unwrap();
    let target = bytes.len() / 2;
    bytes[target] ^= 0xff;
    fs::write(&store, &bytes).unwrap();
}

/// `verify` reports the damage on stdout and exits non-zero.
///
/// The per-chunk detail goes to stderr and the summary line to stdout, so a test
/// that reads only one stream reads half the answer — which is why these are
/// separate tests and each says which stream it is looking at.
#[test]
fn verify_exits_non_zero_and_says_so_on_stdout() {
    let dir = tempfile::tempdir().unwrap();
    damaged_store(dir.path());

    let verify = lepatch(dir.path(), &["verify", "snap"]);
    assert!(
        !verify.status.success(),
        "a damaged store must not verify: exit {:?}",
        verify.status.code()
    );

    let summary = stdout(&verify);
    assert!(
        summary.contains("FAILED"),
        "the summary should report the failure: {summary}"
    );
}

/// The detail names the chunk and what its bytes really hashed to.
///
/// A chunk is addressed by the hash of what it should contain, so a mismatch is a
/// report of two different hashes and which is which. The pair is found by
/// collecting the 64-character hex tokens rather than by splitting the message on
/// its wording: a test that parses prose fails when the prose is reworded, and
/// passes when the diagnosis is wrong.
#[test]
fn verify_names_the_chunk_and_what_it_really_hashed_to() {
    let dir = tempfile::tempdir().unwrap();
    damaged_store(dir.path());

    let detail = stderr(&lepatch(dir.path(), &["verify", "snap"]));

    let hashes: Vec<&str> = detail
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_ascii_hexdigit() && c != 'a'))
        .filter(|word| word.len() == 64 && word.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect();

    let mut distinct = hashes.clone();
    distinct.sort_unstable();
    distinct.dedup();

    assert!(
        hashes.len() >= 2 && distinct.len() >= 2,
        "the detail should name the chunk and the hash its bytes actually have, \
         and those must differ:\n{detail}"
    );
}

/// A restore from a damaged store is refused, and writes nothing.
///
/// The refusal matters as much as the absence: "nothing was written" is also what
/// a run that did nothing looks like. The message has to say it refused, and the
/// destination has to be untouched.
#[test]
fn a_restore_from_a_damaged_store_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    damaged_store(dir.path());

    let refused = fails(dir.path(), &["restore", "out", "snap"]);
    assert!(
        refused.contains("refusing to restore"),
        "restore should say it refused: {refused}"
    );
    assert!(
        !dir.path().join("out/large.bin").exists(),
        "nothing should have been written from a damaged store"
    );
}

/// A backup that fails must publish nothing.
///
/// There is no index file to leave behind and no version number to skip, which is
/// the easier half of the guarantee: the run's commit record is the only thing
/// that makes a version exist, and a run that never got there wrote none.
#[test]
fn a_failed_backup_publishes_no_version() {
    let dir = tempfile::tempdir().unwrap();

    let error = fails(dir.path(), &["backup", "no-such-directory", "snap"]);
    assert!(
        error.contains("no-such-directory"),
        "the error should name the path it could not read: {error}"
    );

    // Either nothing exists at all, or the store exists with no commit record in
    // it. Both mean the same thing: no version can be restored.
    assert_eq!(
        count_entries(dir.path(), "snap"),
        0,
        "a failed backup left behind a version"
    );

    assert!(
        fails(dir.path(), &["restore", "out", "snap"]).contains("no committed version"),
        "nothing should be restorable after a failed backup"
    );
}

/// A run killed part-way leaves the store exactly as the last good version.
///
/// This is the property the commit record buys, and it is what the old design
/// could not promise: the previous version's data records are already on the
/// device, and an unfinished run's records sit after them with no commit record
/// of their own. The next run reclaims the space and the version that was there
/// before is still restorable, and still the latest.
#[test]
fn an_unfinished_run_leaves_the_previous_version_intact() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "snap"]);
    assert_eq!(count_entries(dir.path(), "snap"), 1);

    // Append a blob record that is then cut short, which is what a torn write at
    // the tail of an interrupted run looks like: the header is there and the
    // payload is not all of it. No commit record follows it, because the run
    // never reached one.
    let mut bytes = fs::read(dir.path().join("snap.0000.bin")).unwrap();
    bytes.extend_from_slice(b"LPB1\x00\x80\x01");
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(b"half a record");
    fs::write(dir.path().join("snap.0000.bin"), &bytes).unwrap();

    // A read refuses rather than repairing: the bytes are there and unreadable,
    // and silently ignoring them would mean a restore that quietly omits whatever
    // they held. The next *run* is what repairs it, below.
    let refused = fails(dir.path(), &["restore", "out", "snap"]);
    assert!(
        refused.contains("damaged") && refused.contains("does not repair"),
        "a damaged segment should be reported rather than repaired, got: {refused}"
    );

    // The next run truncates the torn tail and the version is back.
    ok(dir.path(), &["backup", "src", "snap"]);
    assert_eq!(
        count_entries(dir.path(), "snap"),
        2,
        "the recovery should have discarded the torn tail rather than keeping it"
    );

    ok(dir.path(), &["restore", "out2", "snap"]);
    assert_eq!(
        common::describe(&dir.path().join("src")),
        common::describe(&dir.path().join("out2"))
    );
}

/// `lepatch versions` answers from the index files, and reindex puts them back.
///
/// The files exist so listing a set costs a file name per version rather than a
/// scan of every record in it. What matters for a user is that deleting them costs
/// nothing: the versions are still there, and one command brings the listing back.
#[test]
fn the_version_index_can_be_rebuilt_from_nothing() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    for _ in 0..3 {
        ok(dir.path(), &["backup", "src", "snap"]);
    }
    assert_eq!(count_entries(dir.path(), "snap"), 3);

    // Gone.
    for path in index_files(dir.path(), "snap") {
        fs::remove_file(path).unwrap();
    }
    assert_eq!(
        count_entries(dir.path(), "snap"),
        0,
        "with every index file removed there is nothing to list from"
    );

    // But the versions are still restorable, which is the whole claim.
    ok(dir.path(), &["restore", "out", "snap", "--version", "2"]);
    assert_eq!(
        common::describe(&dir.path().join("src")),
        common::describe(&dir.path().join("out"))
    );

    // And one command brings the listing back.
    ok(dir.path(), &["reindex", "snap"]);
    assert_eq!(
        count_entries(dir.path(), "snap"),
        3,
        "reindex should have written an index file for every version"
    );

    // Which resolve, not merely exist.
    ok(dir.path(), &["restore", "out2", "snap", "--version", "1"]);
    assert_eq!(
        common::describe(&dir.path().join("src")),
        common::describe(&dir.path().join("out2"))
    );
}

/// A store with a tail that does not parse is reported, not repaired or ignored.
///
/// The scan stops at the first record that does not verify, so bytes past that
/// point are unreadable. Silently ignoring them would mean a restore that
/// quietly omits whatever they held and reports success — so a read refuses, and
/// a write truncates only the last segment. This is the test for the read half.
#[test]
fn a_store_with_a_damaged_tail_is_reported_not_ignored() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    // Cut the store inside the records, so the last version's snapshot is not
    // wholly present.
    let store = dir.path().join("snap.0000.bin");
    let bytes = fs::read(&store).unwrap();
    fs::write(&store, &bytes[..bytes.len() - 40_000]).unwrap();

    for (args, what) in [
        (vec!["restore", "out", "snap"], "a restore"),
        (vec!["verify", "snap"], "a verify"),
        (vec!["versions", "snap"], "a listing"),
    ] {
        let error = fails(dir.path(), &args);
        assert!(
            error.contains("damaged"),
            "{what} should say the store is damaged rather than reporting a \
             partial answer: {error}"
        );
    }
}

#[test]
fn a_second_backup_is_refused_while_the_lock_is_held() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    fs::write(dir.path().join("snap.lock"), b"").unwrap();

    let error = fails(dir.path(), &["backup", "src", "snap"]);
    assert!(
        error.contains("locked by another lepatch process") && error.contains("remove it"),
        "the error should say how to recover: {error}"
    );
}

#[test]
fn restoring_a_name_with_no_snapshots_explains_itself() {
    let dir = tempfile::tempdir().unwrap();

    let error = fails(dir.path(), &["restore", "out", "ghost"]);
    assert!(
        error.contains("no backup store at ghost"),
        "the error should name what it looked for and where: {error}"
    );
}

#[test]
fn restoring_an_absent_version_names_the_version() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    let error = fails(dir.path(), &["restore", "out", "snap", "--version", "42"]);
    assert!(
        error.contains("42") && error.contains("does not exist"),
        "the error should name the version and why: {error}"
    );
}

#[test]
fn errors_are_reported_as_messages_not_debug_output() {
    let dir = tempfile::tempdir().unwrap();

    let error = fails(dir.path(), &["restore", "out", "ghost"]);

    // The old behaviour was `Error: Custom { kind: NotFound, error: "..." }`,
    // which says nothing about what went wrong.
    assert!(
        !error.contains("Custom {"),
        "error output should not be a Debug dump: {error}"
    );
    assert!(
        !error.contains("Os { code:"),
        "error output should not be a Debug dump: {error}"
    );
}

#[test]
fn a_backup_set_named_with_an_extension_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "photos.tar"]);

    assert!(dir.path().join("photos.tar.0000.bin").is_file());
    assert!(
        !dir.path().join("photos.0000.bin").exists(),
        "the base name's extension should not be replaced"
    );
}

#[test]
fn a_backup_set_can_live_in_a_subdirectory() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "data/deep/snap"]);
    ok(dir.path(), &["backup", "src", "data/deep/snap"]);
    ok(dir.path(), &["restore", "out", "data/deep/snap"]);

    assert!(dir.path().join("data/deep/snap.0000.bin").is_file());
    assert_eq!(count_entries(dir.path(), "data/deep/snap"), 2);
    assert_eq!(
        common::describe(&dir.path().join("src")),
        common::describe(&dir.path().join("out"))
    );
}

#[cfg(unix)]
#[test]
fn paths_that_are_not_valid_utf8_survive_a_round_trip() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::PathBuf};

    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("src");
    fs::create_dir_all(&source).unwrap();

    let raw = OsStr::from_bytes(b"bad-\xff\xfe-name.bin");
    fs::write(PathBuf::from(&source).join(raw), noisy(20_000)).unwrap();

    ok(dir.path(), &["backup", "src", "snap"]);
    ok(dir.path(), &["restore", "out", "snap"]);

    assert_eq!(
        common::describe(&source),
        common::describe(&dir.path().join("out"))
    );
}

#[test]
fn quiet_suppresses_progress_and_verbose_adds_detail() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    let quiet = ok_logs(dir.path(), &["--quiet", "backup", "src", "snap"]);
    assert!(
        quiet.is_empty(),
        "--quiet should print nothing on success, got: {quiet}"
    );

    let verbose = ok_logs(dir.path(), &["--verbose", "backup", "src", "snap"]);
    assert!(
        !verbose.is_empty(),
        "--verbose should report more than the default"
    );
}

#[test]
fn quiet_and_verbose_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let output = lepatch(
        dir.path(),
        &["--quiet", "--verbose", "backup", "src", "snap"],
    );
    assert!(!output.status.success());
}

#[test]
fn the_binary_reports_its_version_and_help() {
    let dir = tempfile::tempdir().unwrap();

    let version = ok(dir.path(), &["--version"]);
    assert!(
        version.contains("lepatch") && version.contains(env!("CARGO_PKG_VERSION")),
        "unexpected --version output: {version}"
    );

    let help = ok(dir.path(), &["--help"]);
    for command in ["backup", "restore", "verify"] {
        assert!(help.contains(command), "--help should list {command}");
    }
}

#[test]
fn an_unknown_subcommand_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let output = lepatch(dir.path(), &["frobnicate"]);
    assert!(!output.status.success());
}

/// `--compression-level` counts 0 to 9, and says so rather than guessing.
///
/// The library clamps an out-of-range level so a caller with a bad number gets a
/// working policy. A person typing a number should be told, not corrected: a
/// store quietly written at a level other than the one they asked for is a thing
/// they cannot see from the directory listing, and the whole point of the flag
/// being a write policy is that it is legible.
#[tokio::test]
async fn a_compression_level_outside_zero_to_nine_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.bin"), noisy(30_000)).unwrap();

    for level in ["10", "22", "255"] {
        let output = lepatch(
            dir.path(),
            &["backup", ".", "store", "--compression-level", level],
        );
        assert!(
            !output.status.success(),
            "--compression-level {level} should be refused"
        );
        // The range, and the number that was refused in it. Both, and only both.
        //
        // This was `contains("0..=9") || contains('0')`, and the second arm is
        // satisfied by almost any message with a zero in it — this one, a
        // timestamp, a byte count, the level the user typed. So the `||` made the
        // test pass whether or not the range was ever named, which is the one
        // thing it is checking. `clap` writes `11 is not in 0..=9`, so both
        // substrings are in one sentence and asserting both is no more brittle
        // than asserting one.
        let refused = stderr(&output);
        assert!(
            refused.contains("0..=9"),
            "the error should say the range, got: {refused}"
        );
        assert!(
            refused.contains(level),
            "and it should quote the level that was refused, got: {refused}"
        );
    }

    // And both ends of the range are accepted, 0 included.
    for level in ["0", "1", "9"] {
        ok(
            dir.path(),
            &[
                "backup",
                ".",
                &format!("store{level}"),
                "--compression-level",
                level,
            ],
        );
    }

    // 0 stored the payload raw, which is what it claims to mean.
    let store = BlobStore::open_for_reading(dir.path().join("store0"))
        .await
        .unwrap();
    let id = store.latest().unwrap().1;
    let placement = store.locate(&id).unwrap();
    assert_eq!(placement.codec, Codec::Raw);
}

/// A profile that does not exist is a usage error, and it says what does.
///
/// The same reasoning as `--compression-level`, one level up: the flag decides
/// how finely the tree is cut, a set quietly cut at a size nobody asked for is
/// invisible from the directory listing, and "unknown profile" on its own leaves
/// the user to run `--help` to find out what the choices are. So the error names
/// the profile that was refused *and* every profile that would have worked.
#[test]
fn a_chunk_profile_that_does_not_exist_is_refused() {
    let dir = tempfile::tempdir().unwrap();

    for name in ["fine", "big", "16k", "defaul"] {
        // Not `fails`, which insists on exit 1: a flag that does not parse is a
        // usage error, and clap exits 2 for those. The distinction is the one a
        // script sees — exit 1 is a run that was asked for and could not be had.
        let output = lepatch(
            dir.path(),
            &["backup", ".", "store", "--chunk-profile", name],
        );
        assert!(
            !output.status.success(),
            "--chunk-profile {name} should be refused"
        );
        assert_eq!(
            output.status.code(),
            Some(2),
            "a flag that does not parse should exit 2, got {:?}\nstderr: {}",
            output.status.code(),
            stderr(&output)
        );

        let refused = stderr(&output);
        assert!(
            refused.contains(&format!("`{name}` is not a chunk profile")),
            "the error should quote the name that was refused, got: {refused}"
        );
        for profile in ["small", "default", "large"] {
            assert!(
                refused.contains(profile),
                "the error should offer {profile}, got: {refused}"
            );
        }
    }

    // And none of that created a set: a refused flag is refused before the run,
    // not after it has written a version nobody asked for.
    assert!(
        !dir.path().join("store.0000.bin").exists(),
        "a refused --chunk-profile should not have written a store"
    );
}

/// The profile is what decides the chunking, and `default` decides exactly what
/// no flag at all decides.
///
/// The first half is the flag working: a tree cut at three profiles is three
/// different sets of chunks, so the counts have to differ and in the direction
/// the names promise. The second half is the property that makes the flag safe to
/// leave alone — `default` resolves to the configuration every backup so far
/// used, so a run that passes it has to produce the *same* run, down to the
/// snapshot's hash, and a store cannot come to depend on which spelling was used.
#[tokio::test]
async fn a_chunk_profile_is_the_chunking_and_default_is_the_flagless_run() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(128 * 1024)).unwrap();

    // Back up into a fresh set and report what the run committed. The snapshot's
    // hash is what says two runs cut the same tree the same way.
    let backup_of = |name: &str, profile: Option<&str>| {
        let mut args = vec!["backup", "source", name];
        if let Some(profile) = profile {
            args.push("--chunk-profile");
            args.push(profile);
        }
        ok(dir.path(), &args);
        ok(dir.path(), &["versions", name]).trim().to_string()
    };

    let bare = backup_of("bare", None);
    let default = backup_of("default", Some("default"));
    let small = backup_of("small", Some("small"));
    let large = backup_of("large", Some("large"));

    assert_eq!(
        bare, default,
        "--chunk-profile default should be the same run as no flag at all"
    );
    assert_ne!(
        small, large,
        "two profiles should not have produced the same snapshot"
    );

    let small_chunks = chunk_count(&dir.path().join("small")).await;
    let default_chunks = chunk_count(&dir.path().join("default")).await;
    let large_chunks = chunk_count(&dir.path().join("large")).await;

    assert!(
        small_chunks > default_chunks && default_chunks > large_chunks,
        "small should cut more chunks than default, and default more than large: \
         small={small_chunks}, default={default_chunks}, large={large_chunks}"
    );
}

/// How many chunks the newest version of the set at `base` names.
///
/// Through the snapshot rather than by counting records in the store: a chunk is
/// what the snapshot refers to, and a record is also the snapshot itself and every
/// commit, so the file holds more records than the version has chunks.
async fn chunk_count(base: &Path) -> usize {
    let store = BlobStore::open_for_reading(base).await.unwrap();
    let snapshot = load_snapshot(&store, store.latest().unwrap().1)
        .await
        .unwrap();
    snapshot.chunks.len()
}

/// A set whose runs were cut at different profiles is still one store.
///
/// This is what the flag's own promise is: chunks are named by their contents and
/// nothing about a profile is written into the format, so changing it mid-life
/// costs deduplication and nothing else. Every version still restores, byte for
/// byte, whichever profile cut it.
#[test]
fn a_set_survives_being_cut_at_more_than_one_profile() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(
        dir.path(),
        &["backup", "src", "snap", "--chunk-profile", "small"],
    );
    ok(
        dir.path(),
        &["backup", "src", "snap", "--chunk-profile", "large"],
    );

    let expected = common::describe(&dir.path().join("src"));
    for (version, profile) in [(1, "small"), (2, "large")] {
        ok(
            dir.path(),
            &[
                "restore",
                &format!("out{version}"),
                "snap",
                "--version",
                &version.to_string(),
            ],
        );
        assert_eq!(
            expected,
            common::describe(&dir.path().join(format!("out{version}"))),
            "version {version}, cut at the {profile} profile, did not restore the tree"
        );
    }
}

#[test]
fn a_run_says_that_it_is_making_progress() {
    // A backup of a real dataset takes minutes to hours. Until it reported
    // anything at all, a user watching a silent terminal could not tell a
    // working backup from a hung one, and the first thing they would do about
    // that is kill it.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();
    fs::write(source.join("b.bin"), noisy(200_000)).unwrap();

    let backup = ok_logs(dir.path(), &["backup", "source", "store"]);
    assert!(
        backup.contains("in progress"),
        "a backup said nothing while it ran:\n{backup}"
    );
    // The opening line is the file count, which the walk knows before any
    // content has been read — the only thing worth saying at that point.
    assert!(
        backup.contains("files=2"),
        "the opening progress line should report the file count:\n{backup}"
    );

    let restored = ok_logs(dir.path(), &["restore", "out", "store"]);
    assert!(
        restored.contains("in progress"),
        "a restore said nothing while it ran:\n{restored}"
    );

    let verified = ok_logs(dir.path(), &["verify", "store"]);
    assert!(
        verified.contains("in progress"),
        "a verify said nothing while it ran:\n{verified}"
    );
}

#[test]
fn progress_reporting_can_be_turned_off() {
    // Someone reading only the summary lines should be able to have them, and
    // the running commentary is opt-out rather than something to filter out of
    // the whole log.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(100_000)).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .env("RUST_LOG", "lepatch=info,lepatch::progress=warn")
        .args(["backup", "source", "store"])
        .output()
        .expect("failed to run the lepatch binary");

    let text = stderr(&output);
    assert!(
        !text.contains("in progress"),
        "progress was not silenced:\n{text}"
    );
    assert!(
        text.contains("backup complete"),
        "but the summary line should survive:\n{text}"
    );
}

#[test]
fn a_restore_merges_by_default_and_says_so() {
    // Merging is the safer default — a restore into a directory that turns out to
    // be the wrong one should not delete it first — but a user who expected the
    // destination to be exactly the snapshot should not have to diff two trees to
    // find out that it is not.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("kept.bin"), noisy(50_000)).unwrap();

    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("out");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("stale.bin"), b"from last week").unwrap();

    let merged = ok_logs(dir.path(), &["restore", "out", "store"]);
    assert!(
        destination.join("stale.bin").exists(),
        "a merging restore should not delete what it did not write"
    );
    assert!(destination.join("kept.bin").exists());
    assert!(
        merged.contains("--clean"),
        "a merging restore should mention how to get an exact copy:\n{merged}"
    );
}

#[test]
fn clean_gives_an_exact_copy() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("kept.bin"), noisy(50_000)).unwrap();

    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("out");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("stale.bin"), b"from last week").unwrap();
    fs::create_dir_all(destination.join("stale-dir")).unwrap();

    ok(dir.path(), &["restore", "out", "store", "--clean"]);

    assert!(
        !destination.join("stale.bin").exists(),
        "--clean left a stale file"
    );
    assert!(
        !destination.join("stale-dir").exists(),
        "--clean left a stale directory"
    );
    assert!(destination.join("kept.bin").exists());
}

#[test]
fn clean_refuses_a_destination_holding_the_backup_set() {
    // The one operation here that destroys data, so the obvious mistake has to
    // stop the run rather than empty the store it is about to read from.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(50_000)).unwrap();

    ok(dir.path(), &["backup", "source", "store"]);

    // The set is `store.bin` beside the source, so restoring it into its own
    // directory is the mistake.
    let output = lepatch(dir.path(), &["restore", ".", "store", "--clean"]);
    assert!(
        !output.status.success(),
        "cleaning a directory holding the backup set should fail"
    );
    assert!(
        stderr(&output).contains("backup set being restored from"),
        "unhelpful error: {}",
        stderr(&output)
    );
    assert!(
        dir.path().join("store.0000.bin").exists(),
        "the store was deleted"
    );
}

#[test]
fn a_second_restore_into_one_destination_is_refused() {
    // Two restores at once would interleave writes into the same tree, and
    // neither would end up with what it meant to produce.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();
    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("out");
    fs::create_dir_all(&destination).unwrap();

    let mut first = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["restore", "out", "store"])
        .spawn()
        .expect("failed to start the first restore");

    // Give it long enough to have taken the claim, then try a second.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let second = lepatch(dir.path(), &["restore", "out", "store"]);

    let _ = first.wait();

    // Whichever order they actually ran in, the second one must not have been
    // allowed to write. If the first finished before the second started there is
    // nothing to assert, so the check is on the lock file's existence instead.
    let lock_present = destination
        .with_file_name("out.lepatch-restore.lock")
        .exists();

    if lock_present {
        assert!(
            !second.status.success(),
            "a second restore into a claimed destination should fail"
        );
        assert!(
            stderr(&second).contains("another lepatch process"),
            "unhelpful error: {}",
            stderr(&second)
        );
    }

    // Either way the claim must not outlive the run.
    assert!(
        !lock_present,
        "the destination claim was left behind after the restore finished"
    );
}

#[test]
fn a_restore_creates_a_destination_whose_parent_is_missing() {
    // `mnt/new/photos` is an ordinary thing to want, and the restore itself
    // creates the missing directories. The destination claim is a *sibling* of
    // the destination, so it was the one part that could not cope with a path
    // that is not there yet — and it failed the run with a bare ENOENT before
    // any of that happened.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(50_000)).unwrap();
    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("brand").join("new").join("nested");
    assert!(!destination.exists(), "the fixture should not exist yet");

    ok(dir.path(), &["restore", "brand/new/nested", "store"]);

    assert_eq!(
        fs::read(destination.join("a.bin")).unwrap().len(),
        50_000,
        "the file did not land in the nested destination"
    );

    // And the claim must not be left inside the restored tree.
    assert!(
        !destination.join("nested.lepatch-restore.lock").exists(),
        "the restore claim ended up inside the restored output"
    );
}

/// The claim a restore takes sits beside the destination, named for it.
fn claim_path(destination: &Path) -> std::path::PathBuf {
    destination.with_file_name(format!(
        "{}.lepatch-restore.lock",
        destination
            .file_name()
            .expect("a destination name")
            .to_string_lossy()
    ))
}

#[test]
fn a_restore_claim_is_released_when_it_goes_out_of_scope() {
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("out");

    let claim = lepatch::command::lock_destination(&destination).expect("claim");
    let path = claim_path(&destination);
    assert!(path.exists(), "the claim file was not created");

    drop(claim);
    assert!(!path.exists(), "the claim outlived the guard that held it");
}

#[cfg(unix)]
#[test]
fn being_asked_to_stop_does_not_leave_a_claim_behind() {
    // Ctrl-C used to leave a claim that had to be removed by hand before the next
    // restore into the same directory would run.
    //
    // The signal goes out through a shell rather than a new dependency: this
    // crate's graph was just fought over to keep it small, and `kill` is a
    // coreutils builtin on every platform this test is compiled for. Signalled by
    // pid rather than by command-line pattern — matching the pattern races the
    // process installing its handler, and a signal arriving first takes the
    // default action and kills the run outright, which looks exactly like the
    // bug being tested for.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..12 {
        fs::write(source.join(format!("f{i}.bin")), noisy(2_000_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("out");

    for signal in ["INT", "TERM"] {
        let _ = fs::remove_dir_all(&destination);
        let _ = fs::remove_file(claim_path(&destination));

        let mut child = Command::new(env!("CARGO_BIN_EXE_lepatch"))
            .current_dir(dir.path())
            .args(["restore", "out", "store"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to start the restore");

        std::thread::sleep(std::time::Duration::from_millis(150));

        let _ = Command::new("sh")
            .arg("-c")
            .arg(format!("kill -{signal} {}", child.id()))
            .status();

        let status = child.wait().expect("failed to wait for the restore");
        let path = claim_path(&destination);

        // The claim must be gone whether the run was interrupted or finished:
        // both paths release it, and that is the property worth asserting.
        assert!(
            !path.exists(),
            "SIG{signal} left a claim at {}",
            path.display()
        );

        // When it was interrupted at all, it should report the way a shell
        // expects: 128 + signal.
        if let Some(code) = status.code()
            && code != 0
        {
            let expected = if signal == "INT" { 130 } else { 143 };
            assert_eq!(code, expected, "SIG{signal} produced exit code {code}");
        }
    }
}

#[test]
fn clean_restore_leaves_nothing_beside_the_destination() {
    // The swap works through a sibling directory. It must not survive a
    // successful run, or every subsequent restore has to clear it first.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(100_000)).unwrap();
    ok(dir.path(), &["backup", "source", "store"]);

    ok(dir.path(), &["restore", "out", "store", "--clean"]);

    let siblings: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("out."))
        .collect();

    assert!(
        siblings.is_empty(),
        "a finished --clean restore left {siblings:?} beside the destination"
    );
}

#[test]
fn a_failing_clean_restore_leaves_the_destination_alone() {
    // A damaged store is refused before anything is written, so a `--clean`
    // restore must not have emptied the destination on its way to finding out.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), noisy(200_000)).unwrap();
    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("out");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("precious.txt"), b"do not lose me").unwrap();

    // Flip a byte in the blob store so the verify gate refuses the run.
    let blob = dir.path().join("store.0000.bin");
    let mut bytes = fs::read(&blob).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    fs::write(&blob, &bytes).unwrap();

    let output = lepatch(dir.path(), &["restore", "out", "store", "--clean"]);
    assert!(
        !output.status.success(),
        "a damaged store should be refused"
    );

    assert_eq!(
        fs::read(destination.join("precious.txt")).unwrap(),
        b"do not lose me",
        "a refused --clean restore emptied the destination anyway"
    );
}

#[cfg(unix)]
#[test]
fn a_clean_restore_is_never_left_partly_written() {
    // The invariant, stated so it holds whatever the timing: after a `--clean`
    // restore is killed, the destination is either *entirely* the old contents or
    // *entirely* the snapshot. Never some mixture of the two, which is what a
    // restore that wrote in place would leave.
    //
    // This test previously slept a fixed 120 ms and then killed, which is a race
    // it cannot reliably win: on a machine where the restore of 20 MB finished
    // inside that window, the kill arrived *after* the swap and the destination
    // held the finished snapshot — a correct outcome the assertion rejected. The
    // comment claimed "both outcomes are acceptable, so this cannot be flaky in
    // the failing direction", and that was false: the assertion accepted one of
    // them. A test whose stated invariant is wider than what it checks is worse
    // than a tight assertion, because the comment is what a reader trusts.
    //
    // So two things are fixed rather than one. The kill is aimed by *waiting* for
    // the staging directory a `--clean` restore assembles into, which is the
    // window the atomicity claim is actually about, instead of guessing a duration
    // the machine decides. And the assertion states the invariant, so either side
    // of the race passes and only a genuinely mixed destination fails.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..10 {
        fs::write(source.join(format!("f{i}.bin")), noisy(2_000_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "store"]);

    let destination = dir.path().join("out");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("marker.txt"), b"the old contents").unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["restore", "out", "store", "--clean"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start the restore");

    // Wait for the restore to be *assembling*, rather than assuming how long that
    // takes. A `--clean` restore builds the whole tree in a sibling directory and
    // swaps it in, so the presence of that directory is the signal that the
    // destination has not been touched yet — the only state in which a kill says
    // anything about atomicity, and a fact about the restore rather than a
    // duration the machine gets to choose.
    let staging = dir.path().join(format!("out.{PARTIAL_SUFFIX}"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !staging.exists() && std::time::Instant::now() < deadline {
        if matches!(child.try_wait(), Ok(Some(_))) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let killed_while_assembling = staging.exists();

    if killed_while_assembling {
        let _ = Command::new("sh")
            .arg("-c")
            .arg(format!("kill -INT {}", child.id()))
            .status();
    }
    let _ = child.wait();

    let names: Vec<String> = fs::read_dir(&destination)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    let has_marker = names.iter().any(|n| n == "marker.txt");
    let restored_files = names.iter().filter(|n| n.starts_with('f')).count();

    // The invariant, not one side of it. Either exactly what it was — the marker,
    // and none of the restored files — or exactly the snapshot: every restored
    // file, and no marker. Anything else means the destination was written into,
    // which is the failure.
    let untouched = has_marker && restored_files == 0;
    let swapped = !has_marker && restored_files == 10;

    assert!(
        untouched || swapped,
        "the destination was left partly written: {has_marker} marker, \
         {restored_files} of 10 files (killed while assembling: \
         {killed_while_assembling})"
    );

    // And no half-written sibling was left to confuse the next run.
    let leftovers: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains("lepatch-restore-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "a killed --clean restore left {leftovers:?} behind"
    );
}

/// `lepatch pack` writes a file a client can fetch in one range.
///
/// The three properties that make a pack usable as a distributed artifact, checked
/// through the command line rather than the library, because the command is what
/// a user has:
/// it can be produced at all; the same version packs to the same bytes whatever
/// the set has done since; and a later version is a different artifact.
#[test]
fn a_pack_is_written_deterministically_and_per_version() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..6 {
        fs::write(
            dir.path().join(format!("f{i}.bin")),
            noisy(200_000 + i * 30_000),
        )
        .unwrap();
    }

    ok(dir.path(), &["backup", ".", "set"]);
    let first = ok(dir.path(), &["pack", "set", "v1.pack"]);

    // The content hash is on stdout, and it is the pack's name: the same tree
    // packs to the same bytes, so a cache can be keyed on it and a client can
    // verify with one comparison.
    let hash_of = |text: &str| {
        text.lines()
            .next()
            .expect("the pack command should print the content hash")
            .split_whitespace()
            .next()
            .expect("the hash should be the first field")
            .to_owned()
    };
    assert_eq!(hash_of(&first).len(), 64, "expected a 32-byte hex digest");

    // Same version, written again: identical bytes. A pack that varied would
    // defeat the caching the hash is for.
    ok(dir.path(), &["pack", "set", "v1-again.pack"]);
    assert_eq!(
        fs::read(dir.path().join("v1.pack")).unwrap(),
        fs::read(dir.path().join("v1-again.pack")).unwrap(),
        "packing the same version twice produced different bytes"
    );

    // Change a file and take a second version. That is a different artifact, and
    // the first is still reproducible from the set afterwards — which is the
    // property that makes a pack derived output rather than a second copy.
    fs::write(dir.path().join("f2.bin"), noisy(230_000)).unwrap();
    ok(dir.path(), &["backup", ".", "set"]);
    let second = ok(dir.path(), &["pack", "set", "v2.pack"]);

    assert_ne!(
        hash_of(&first),
        hash_of(&second),
        "a changed tree produced the same pack"
    );

    ok(
        dir.path(),
        &["pack", "set", "v1-later.pack", "--version", "1"],
    );
    assert_eq!(
        fs::read(dir.path().join("v1.pack")).unwrap(),
        fs::read(dir.path().join("v1-later.pack")).unwrap(),
        "version 1 should still pack to the same bytes after version 2 exists"
    );
}

/// A pack reports a version the set does not hold rather than packing the wrong
/// one.
///
/// `--version` is a number a user types, so it is exactly the input a stale script
/// or a typo produces, and the failure has to be a refusal.
#[test]
fn packing_a_version_that_does_not_exist_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.bin"), noisy(50_000)).unwrap();
    ok(dir.path(), &["backup", ".", "set"]);

    let output = lepatch(dir.path(), &["pack", "set", "out.pack", "--version", "9"]);
    assert!(
        !output.status.success(),
        "packing a version that does not exist should fail"
    );
    assert!(
        !dir.path().join("out.pack").exists(),
        "a refused pack should not leave a file behind"
    );
}

/// The whole round trip, through the command line and nothing else.
///
/// `pack` then `apply`, with no set consulted at apply time — which is the point
/// of a pack carrying its own file table. If a client needed the set as well as
/// the file, this would fail the moment the set was moved or deleted, which is the
/// situation sharing a patch is for.
#[test]
fn a_pack_applied_to_an_empty_directory_produces_the_tree() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..5 {
        fs::write(
            source.join(format!("f{i}.bin")),
            noisy(180_000 + i * 20_000),
        )
        .unwrap();
    }
    fs::create_dir_all(source.join("nested/deep")).unwrap();
    fs::write(source.join("nested/deep/leaf.bin"), noisy(40_000)).unwrap();

    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    // Move the set aside: the apply must not need it.
    fs::rename(
        dir.path().join("set.0000.bin"),
        dir.path().join("moved.bin"),
    )
    .unwrap();
    fs::remove_file(dir.path().join("set.lock")).ok();

    let target = dir.path().join("target");
    let report = ok(dir.path(), &["apply", "v1.pack", "target"]);

    assert!(
        report.contains("fetched"),
        "an apply should say what it fetched, got: {report}"
    );
    common::assert_same_tree(&source, &target);
}

/// Applying an update writes what changed and says how little that was.
///
/// This is the property the whole feature exists for, checked through the command
/// line: the report has to name a figure small enough to be a delta, because a
/// tool that quietly re-sent the tree would still produce a correct directory.
#[test]
fn an_update_fetches_a_delta_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..8 {
        fs::write(source.join(format!("f{i}.bin")), noisy(250_000)).unwrap();
    }

    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    // The client takes version 1.
    let target = dir.path().join("target");
    ok(dir.path(), &["apply", "v1.pack", "target"]);
    common::assert_same_tree(&source, &target);

    // Version 2: one file rewritten, one added, one dropped.
    fs::write(source.join("f5.bin"), noisy(280_000)).unwrap();
    fs::write(source.join("fresh.bin"), noisy(30_000)).unwrap();
    fs::remove_file(source.join("f7.bin")).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v2.pack"]);

    let report = ok(dir.path(), &["apply", "v2.pack", "target", "--prune"]);

    // The added and dropped files are only handled with pruning.
    assert!(
        target.join("fresh.bin").exists(),
        "a new file should have been written"
    );
    assert!(
        !target.join("f7.bin").exists(),
        "a dropped file should have been pruned"
    );
    common::assert_same_tree(&source, &target);

    let fetched = figure(&report, "fetched ").expect("the report should name a fetch size");
    let total = figure(&report, " of ").expect("and the size of the pack");
    assert!(
        fetched * 2 < total,
        "one rewritten file out of eight should need well under half the payload, \
         but the report says {fetched} of {total}"
    );
}

/// An unprivileged restore finishes, and says which owners it could not set.
///
/// The case that decides whether recording an owner is worth anything. Restoring a
/// tree containing files you do not own is ordinary — a home directory holds a few
/// and `/etc` is nothing but — and an unprivileged restore of one has to finish with
/// correct contents rather than stop partway.
///
/// It stopped partway. The file that failed was already on disk under the wrong
/// owner, so there was nothing to retry and nothing to say it was wrong.
#[cfg(unix)]
#[test]
fn an_unprivileged_restore_finishes_and_reports_its_owners() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("mine"), b"fine").unwrap();
    fs::write(source.join("not-mine"), b"owned by someone else").unwrap();

    if fs::metadata(source.join("not-mine")).unwrap().uid() != 0 {
        // Making a file this process cannot own needs privilege, so the test cannot
        // set up its own premise without it. Skipped rather than passed, because
        // "the tree matched" is true either way and would read as though the
        // unprivileged path had been exercised.
        eprintln!("skipped: the test needs root to make a file this process cannot own");
        return;
    }
    std::os::unix::fs::chown(source.join("not-mine"), Some(20_002), None).unwrap();

    ok(dir.path(), &["backup", "source", "set"]);

    // The store and the destination are readable and writable by anyone; nothing is
    // chownable by the caller, which is the whole situation.
    let dest = dir.path().join("out");
    fs::create_dir(&dest).unwrap();
    // The temp dir itself, because a restore's claim is a sibling of the
    // destination and so lands here.
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    for path in fs::read_dir(dir.path()).unwrap() {
        let path = path.unwrap().path();
        let mode = if path.is_dir() { 0o777 } else { 0o666 };
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    }

    let output = std::os::unix::process::CommandExt::uid(
        std::os::unix::process::CommandExt::gid(
            Command::new(env!("CARGO_BIN_EXE_lepatch"))
                .current_dir(dir.path())
                .args(["restore", "out", "set"]),
            65534,
        ),
        65534,
    )
    .output()
    .unwrap();

    assert!(
        output.status.success(),
        "an unprivileged restore should finish, got: {}",
        stderr(&output)
    );

    let said = stderr(&output);
    assert!(
        said.contains("recorded an owner this process could not set"),
        "and it should say what it could not do, got: {said}"
    );
    assert!(
        said.contains("Re-run as root"),
        "naming what would fix it, got: {said}"
    );

    // The tree is right; only the owner is not. That is the whole claim.
    assert_eq!(
        fs::read(dest.join("not-mine")).unwrap(),
        b"owned by someone else",
        "a file whose owner could not be set should still have the right contents"
    );
    assert_eq!(
        fs::metadata(dest.join("mine")).unwrap().mode() & 0o7777,
        fs::metadata(source.join("mine")).unwrap().mode() & 0o7777,
        "and every other file should be exactly right"
    );
}

/// The fetch size in a report, which is the first number after `marker`.
fn figure(report: &str, marker: &str) -> Option<u64> {
    let at = report.find(marker)? + marker.len();
    let digits: String = report[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// `pack --from N` writes a delta, and it is smaller and it still works.
///
/// The whole claim through the command line: a smaller artifact for a client known
/// to be on version 1, applied from nothing but that artifact and the directory
/// version 1 left behind.
#[test]
fn a_delta_pack_is_written_and_applied_through_the_command_line() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..6 {
        fs::write(source.join(format!("f{i}.bin")), noisy(200_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack", "--version", "1"]);

    // The client takes version 1, which is what leaves the record the delta will be
    // checked against.
    let target = dir.path().join("target");
    ok(dir.path(), &["apply", "v1.pack", "target"]);
    common::assert_same_tree(&source, &target);

    // One file changed, one added.
    fs::write(source.join("f2.bin"), noisy(240_000)).unwrap();
    fs::write(source.join("fresh.bin"), noisy(30_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);

    ok(dir.path(), &["pack", "set", "v2.pack", "--version", "2"]);
    let delta = ok(
        dir.path(),
        &["pack", "set", "v2.delta", "--version", "2", "--from", "1"],
    );

    assert!(
        delta.contains("since 1"),
        "a delta should say what it is a delta against, got: {delta}"
    );

    let delta_size = fs::metadata(dir.path().join("v2.delta")).unwrap().len();
    let whole_size = fs::metadata(dir.path().join("v2.pack")).unwrap().len();
    assert!(
        delta_size < whole_size,
        "one changed file out of six should be smaller: {delta_size} of {whole_size}"
    );

    ok(dir.path(), &["apply", "v2.delta", "target"]);
    common::assert_same_tree(&source, &target);
}

/// A delta pack is refused for a client that is not on the version it names.
///
/// Through the command line, because the refusal is a thing a user has to read. A
/// delta omits the chunks its assumed holder has, so applying one to the wrong
/// client would leave a directory that is neither version and report success.
#[test]
fn a_delta_pack_is_refused_for_a_client_on_another_version() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..5 {
        fs::write(source.join(format!("f{i}.bin")), noisy(180_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack", "--version", "1"]);

    // Move the client to version 2.
    fs::write(source.join("f0.bin"), noisy(220_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v2.pack", "--version", "2"]);
    let target = dir.path().join("target");
    ok(dir.path(), &["apply", "v2.pack", "target"]);

    // Version 3, and a delta for a client on version 1.
    fs::write(source.join("f1.bin"), noisy(230_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(
        dir.path(),
        &["pack", "set", "v3.delta", "--version", "3", "--from", "1"],
    );

    let output = lepatch(dir.path(), &["apply", "v3.delta", "target"]);
    assert!(
        !output.status.success(),
        "a delta for the wrong base version should be refused"
    );
    let said = stderr(&output);
    assert!(
        said.contains("is a delta") && said.contains("apply the full pack instead"),
        "and the refusal should say what to do instead, got: {said}"
    );

    // Nothing was written on the way to refusing.
    assert!(
        !target.join("fresh.bin").exists(),
        "the refused apply should not have created version 3's new file"
    );
}

/// `--full` wins over `--from`, and says so.
///
/// Two flags that contradict, in a script that has both. `--full` is the promise
/// that the artifact suits any client, so a stale `--from` narrowing it silently
/// would break that promise with nothing in the output to show.
#[test]
fn a_full_flag_overrides_a_from_and_says_it_did() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..4 {
        fs::write(source.join(format!("f{i}.bin")), noisy(150_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack", "--version", "1"]);

    fs::write(source.join("f0.bin"), noisy(170_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);

    let output = lepatch(
        dir.path(),
        &[
            "pack",
            "set",
            "v2.pack",
            "--version",
            "2",
            "--from",
            "1",
            "--full",
        ],
    );
    assert!(output.status.success(), "got: {}", stderr(&output));

    let report = stdout(&output);
    assert!(
        !report.contains("since 1"),
        "with --full the pack should not name a base, got: {report}"
    );

    // And it really is the whole thing: it applies to a directory holding nothing.
    ok(dir.path(), &["apply", "v2.pack", "fresh"]);
    common::assert_same_tree(&source, &dir.path().join("fresh"));
}

/// Packing against a version the set does not hold is refused.
#[test]
fn a_delta_from_a_version_that_does_not_exist_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), noisy(40_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);

    let output = lepatch(dir.path(), &["pack", "set", "out.delta", "--from", "9"]);
    assert!(
        !output.status.success(),
        "a delta against a version the set does not hold should fail"
    );
    assert!(
        !dir.path().join("out.delta").exists(),
        "and it should not leave a file behind"
    );
}

/// Re-applying a pack a directory already holds changes nothing.
#[test]
fn applying_a_pack_a_directory_already_holds_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..4 {
        fs::write(source.join(format!("f{i}.bin")), noisy(120_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    let target = dir.path().join("target");
    ok(dir.path(), &["apply", "v1.pack", "target"]);

    let second = ok(dir.path(), &["apply", "v1.pack", "target"]);
    assert_eq!(
        figure(&second, "fetched "),
        Some(0),
        "a directory already holding the version needs nothing, got: {second}"
    );
    common::assert_same_tree(&source, &target);
}

/// A pack that is not a pack is refused by every command that takes one.
///
/// `apply` writes to a directory, so accepting a file that is not a pack would mean
/// writing a version nobody verified. The refusal has to happen before any of it.
#[test]
fn applying_something_that_is_not_a_pack_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), noisy(50_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    fs::write(dir.path().join("garbage.pack"), b"not a pack at all").unwrap();

    let output = lepatch(dir.path(), &["apply", "garbage.pack", "target"]);
    assert!(
        !output.status.success(),
        "applying a file that is not a pack should fail"
    );
    assert!(
        !dir.path().join("target").exists(),
        "and it should have been refused before creating a destination"
    );
    assert!(
        stderr(&output).contains("not a pack"),
        "the refusal should say what is wrong, got: {}",
        stderr(&output)
    );
}

/// `probe` is not a subcommand.
///
/// It was one, reporting what a directory holds and which ranges a fetch would
/// need. That answer is the one an `apply` has to work out before it can write
/// anything, so it is not a separate thing a user does — it is a step of the apply,
/// and an `apply` that has just run has already reported it as its summary line. A
/// command worth only running when the user already knows what they want is not a
/// command.
///
/// The library keeps both halves. `probe` is what an apply uses to find the missing
/// slices and `Pack::needed` is how those become ranges; `tests/probe.rs` and
/// `tests/pack.rs` cover them.
#[test]
fn a_probe_is_not_a_subcommand() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), noisy(40_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    let output = lepatch(dir.path(), &["probe", "v1.pack", "target"]);
    assert!(
        !output.status.success(),
        "`probe` should no longer be a subcommand"
    );
    assert!(
        !dir.path().join("target").exists(),
        "and should not have touched the destination it named"
    );

    // The list a user reads should not offer it either.
    let help = lepatch(dir.path(), &["--help"]);
    assert!(help.status.success());
    assert!(
        !stdout(&help).contains("probe"),
        "and `--help` should not advertise it, got:\n{}",
        stdout(&help)
    );
}

/// Unrecorded entries are named and left alone unless pruning was asked for.
///
/// The default is the decision this command makes on the user's behalf, so it is
/// worth a test that says so from the outside: the note is on stderr and the file
/// is still there.
#[test]
fn an_unrecorded_entry_is_named_and_kept_until_prune_is_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), noisy(60_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    let target = dir.path().join("target");
    ok(dir.path(), &["apply", "v1.pack", "target"]);

    fs::write(target.join("scratch.txt"), b"mine").unwrap();

    let output = lepatch(dir.path(), &["apply", "v1.pack", "target"]);
    assert!(output.status.success());
    assert!(
        stderr(&output).contains("--prune"),
        "an apply should point at --prune when it left something behind, got: {}",
        stderr(&output)
    );
    assert!(
        target.join("scratch.txt").exists(),
        "an unrecorded file must not be deleted on a guess"
    );

    let pruned = lepatch(dir.path(), &["apply", "v1.pack", "target", "--prune"]);
    assert!(pruned.status.success());
    assert!(
        !target.join("scratch.txt").exists(),
        "--prune should have removed it"
    );
}

/// A mistyped set name is an error, not an empty answer.
///
/// The store's own rule is that reading a path which is not a store is an error
/// rather than an empty set, "so a mistyped name cannot come back as no
/// versions". Two commands were breaking it, in opposite ways.
#[test]
fn a_mistyped_set_name_is_an_error_not_an_empty_listing() {
    let dir = tempfile::tempdir().unwrap();

    let listed = lepatch(dir.path(), &["versions", "nosuchset"]);
    assert_eq!(
        listed.status.code(),
        Some(1),
        "`versions` on a set that is not there should fail, not list nothing\nstderr: {}",
        stderr(&listed)
    );
    assert_eq!(stdout(&listed), "", "a failed listing should print nothing");

    // The worse of the two: opening for writing *creates* a set, so a repair
    // command given a mistyped name used to manufacture an empty one and report a
    // successful repair of it.
    let reindexed = lepatch(dir.path(), &["reindex", "nosuchset"]);
    assert_eq!(
        reindexed.status.code(),
        Some(1),
        "`reindex` on a set that is not there should fail\nstderr: {}",
        stderr(&reindexed)
    );
    assert!(
        !dir.path().join("nosuchset.0000.bin").exists(),
        "`reindex` created a backup set instead of refusing to repair one"
    );
}

/// A set that exists but commits no version lists as empty, which is a different
/// thing from a name that names nothing.
///
/// The distinction is the whole reason for the check above: an empty set is a
/// real answer to a real question, and refusing it would make a freshly created
/// set unusable.
#[test]
fn a_set_with_no_committed_version_lists_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.bin"), noisy(40_000)).unwrap();

    // A backup that never commits: killed before its commit record.
    let mut child = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["backup", ".", "store"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to run the lepatch binary");
    let _ = child.kill();
    let _ = child.wait();

    // Whatever state the kill left, the store is either absent — in which case
    // there is nothing to assert — or present and listing as empty.
    if dir.path().join("store.0000.bin").exists() {
        let listed = lepatch(dir.path(), &["versions", "store"]);
        assert_eq!(
            listed.status.code(),
            Some(0),
            "a set that exists should list, even if empty\nstderr: {}",
            stderr(&listed)
        );
    }
}

/// A tree containing a named pipe is backed up without hanging.
///
/// Reading a FIFO blocks until a writer appears, so a tree containing one used to
/// hang the run forever — and, because the binary installs a SIGTERM handler to
/// release its claims, the signal could not kill it either. The test runs the
/// backup with a timeout so a regression fails rather than hanging the suite.
#[cfg(unix)]
#[test]
fn a_named_pipe_is_skipped_rather_than_read() {
    use std::os::unix::net::UnixListener;

    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.bin"), noisy(60_000)).unwrap();

    // A FIFO, and a socket: both are readable and neither terminates.
    assert!(
        std::process::Command::new("mkfifo")
            .arg(dir.path().join("pipe"))
            .status()
            .is_ok_and(|status| status.success()),
        "could not create a FIFO to test with"
    );
    let _listener = UnixListener::bind(dir.path().join("sock")).expect("bind a socket");

    let mut child = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["backup", ".", "store"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to run the lepatch binary");

    // Polled rather than slept on, so a slow machine does not fail the test and a
    // hung one does not hang it. Ten seconds is far longer than this backup takes
    // and far shorter than a CI timeout.
    let mut finished = None;
    for _ in 0..100 {
        match child.try_wait() {
            Ok(Some(status)) => {
                finished = Some(status);
                break;
            }
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(100)),
            Err(_) => break,
        }
    }
    let hung = finished.is_none();
    if hung {
        let _ = child.kill();
        let _ = child.wait();
    }

    assert!(
        !hung,
        "a backup of a tree containing a FIFO did not finish: reading one blocks \
         until a writer appears, so it must be skipped"
    );

    // And the rest of the tree was actually backed up, so "it finished" does not
    // just mean "it gave up early".
    let restored = dir.path().join("out");
    ok(dir.path(), &["restore", "out", "store"]);
    assert!(
        restored.join("a.bin").exists(),
        "the ordinary files should still have been backed up"
    );
}

/// Piping a command into something that stops reading is not a failure.
///
/// `lepatch versions set | head -1` used to print a line and then panic, because
/// Rust ignores `SIGPIPE` and the print macros turn the resulting `EPIPE` into a
/// panic. A reader that stops early is what `head` does on purpose, so it exits
/// as the run otherwise would, with nothing on stderr.
#[test]
fn a_command_piped_into_something_that_stops_early_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..4 {
        fs::write(
            dir.path().join(format!("f{i}.bin")),
            noisy(40_000 + i * 5_000),
        )
        .unwrap();
    }
    ok(dir.path(), &["backup", ".", "store"]);

    // Enough versions that `head` is guaranteed to close the pipe early.
    for i in 0..5 {
        fs::write(dir.path().join("a.bin"), noisy(40_000 + i * 3_000)).unwrap();
        ok(dir.path(), &["backup", ".", "store"]);
    }

    let child = Command::new("sh")
        .current_dir(dir.path())
        .arg("-c")
        .arg(format!(
            "{} versions store 2>pipe-err.log | head -1 > /dev/null; echo $?",
            env!("CARGO_BIN_EXE_lepatch")
        ))
        .output()
        .expect("failed to run the pipeline");

    assert!(
        child.status.success(),
        "the shell should not have reported a failure: {}",
        String::from_utf8_lossy(&child.stdout)
    );

    let reported = String::from_utf8_lossy(&child.stdout);
    let noise = fs::read_to_string(dir.path().join("pipe-err.log")).unwrap_or_default();
    assert!(
        !noise.contains("panicked") && !noise.contains("Broken pipe"),
        "a piped command panicked instead of stopping quietly:\n{noise}"
    );
    assert!(
        !reported.contains("101"),
        "the command should not have exited as a panic, got: {reported}"
    );
}

/// A failed `verify` reports the failure and exits 1.
///
/// The summary `verify` prints to stdout is not the verdict — the verdict is
/// `Report::into_result`, and it is computed *after* the summary is written.
/// `say!` used to be able to terminate the process from inside the write, which
/// meant the order of those two statements decided the exit code of a command
/// whose whole job is to report one. This pins the outcome, and the ordering is
/// now visible in the source rather than being an accident of it.
///
/// Note what this does *not* cover: the `EPIPE` path itself. A shell pipeline
/// cannot be made to close its read end deterministically — bash holds it open
/// for the life of the pipeline, and `dash` exposes no way to read a member's
/// status out of one — so a test that piped this would pass whether or not the
/// `say!` fix were present. The companion test above covers the reader-closed
/// case for a command that can only succeed.
#[test]
fn a_failed_verify_reports_and_exits_one() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..4 {
        fs::write(
            dir.path().join(format!("f{i}.bin")),
            noisy(40_000 + i * 5_000),
        )
        .unwrap();
    }
    ok(dir.path(), &["backup", ".", "store"]);

    // Damage a chunk's *payload*, so verification finds a hash mismatch rather
    // than a record it cannot parse. A damaged record is a different path: it
    // fails the run before the summary is ever reached and prints nothing to
    // stdout, which would make this test pass for the wrong reason.
    let segments = IndexSet::new(dir.path().join("store"));
    let paths = segments.segments().unwrap();
    assert!(
        !paths.is_empty(),
        "the fixture should have written a segment"
    );
    let mut bytes = fs::read(&paths[0]).unwrap();
    // Past the store header and the first record's, so this lands in a payload.
    let in_payload = 200;
    assert!(
        in_payload < bytes.len(),
        "the fixture's segment should be long enough to damage a payload"
    );
    bytes[in_payload] ^= 0xff;
    fs::write(&paths[0], &bytes).unwrap();

    let output = lepatch(dir.path(), &["verify", "store"]);
    let noise = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.code() == Some(1),
        "a verify that failed must exit 1, got {:?}\n{noise}",
        output.status.code()
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("FAILED"),
        "the summary on stdout should say the run failed:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        noise.contains("error:"),
        "the details should be on stderr, where a script will not capture them: {noise}"
    );
}

/// Both listing modes name the same versions.
///
/// `--numbers-only` used to read each index file on its own, which checksums one
/// file and never consults the store. A leftover index naming a version the set
/// no longer holds was therefore printed by one mode and omitted by the other,
/// from the same directory, in the same invocation pair. Both go through
/// `storage::resolve` now, which cross-checks the index against the store's own
/// commit log.
#[test]
fn both_listing_modes_name_the_same_versions() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..4 {
        fs::write(
            dir.path().join(format!("f{i}.bin")),
            noisy(40_000 + i * 5_000),
        )
        .unwrap();
    }
    ok(dir.path(), &["backup", ".", "store"]);
    for i in 0..2 {
        fs::write(dir.path().join("a.bin"), noisy(40_000 + i * 3_000)).unwrap();
        ok(dir.path(), &["backup", ".", "store"]);
    }

    let full = ok(dir.path(), &["versions", "store"]);
    let numbers = ok(dir.path(), &["versions", "store", "--numbers-only"]);

    let from_full: Vec<&str> = full
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    let from_numbers: Vec<&str> = numbers.lines().map(str::trim).collect();

    assert_eq!(
        from_full, from_numbers,
        "the two listing modes should agree on which versions exist"
    );

    // And with a stale index lying about a version the set no longer has.
    let set = IndexSet::new(dir.path().join("store"));
    let stale = set.index_path(999);
    fs::write(
        &stale,
        VersionIndex {
            version: 999,
            segment: 0,
            ordinal: 0,
            snapshot: BlobId::from_bytes([7u8; 32]),
        }
        .to_bytes(),
    )
    .unwrap();

    let full = ok(dir.path(), &["versions", "store"]);
    let numbers = ok(dir.path(), &["versions", "store", "--numbers-only"]);
    assert_eq!(
        full.lines().count(),
        numbers.lines().count(),
        "a stale index must not be named by one mode and omitted by the other:\n{full}\n{numbers}"
    );
    assert!(
        !numbers.contains("999"),
        "a version the store does not hold must not be listed: {numbers}"
    );
}

/// The claim cleans up beside the *resolved* destination, and only there.
///
/// `DestinationLock` resolves "beside the destination" by canonicalising, and its
/// `Drop` removes the two scratch paths built from that resolution. Its own doc
/// says "an interrupted restore leaves no half-written tree". The binary had a
/// second copy of the same rule and did not canonicalise, so a run staging
/// through a symlink put the tree somewhere the claim's cleanup never looked:
///
/// ```text
/// destination      /tmp/out/link  ->  /tmp/real
/// staging tree     /tmp/out/link.lepatch-restore-partial     (the binary's rule)
/// claim's cleanup  /tmp/real.lepatch-restore-partial          (the library's rule)
/// ```
///
/// Testing the cleanup through the CLI means arranging a restore that fails
/// *after* the staging directory exists, which is fiddly and mostly tests the
/// test. So this asks the claim directly: take it, put something at the scratch
/// paths it says it owns, drop it, and check they are gone. That is exactly the
/// promise, with nothing else in the way.
#[cfg(unix)]
#[test]
fn a_restore_claim_cleans_up_beside_the_resolved_destination() {
    use lepatch::command::{PARTIAL_SUFFIX, PREVIOUS_SUFFIX, lock_destination};

    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    fs::create_dir_all(&real).unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    let claim = lock_destination(&link).expect("taking the claim");

    // The claim resolved the link, so the destination it will swap into is the
    // real directory. Everything the run does is named after *this*, not after
    // the spelling the user typed.
    assert_eq!(
        std::fs::canonicalize(claim.resolved()).unwrap(),
        std::fs::canonicalize(&real).unwrap(),
        "a claim should name the directory the restore will actually reach"
    );

    // The scratch paths must be beside the resolved destination, never beside the
    // link. This is the line that was wrong.
    for suffix in [PARTIAL_SUFFIX, PREVIOUS_SUFFIX] {
        let scratch = claim.scratch_dir(suffix);
        assert_eq!(
            scratch.parent().unwrap(),
            dir.path(),
            "{suffix} should be a sibling of the real destination, not of the link"
        );
        assert!(
            !scratch.starts_with(&link),
            "{suffix} should not be named after the link"
        );

        // Stand in for the half-written tree a killed run would leave.
        fs::create_dir_all(&scratch).unwrap();
        fs::write(scratch.join("leftover"), b"half a file").unwrap();
    }

    // Dropping is the whole cleanup: the claim is released however the run ends,
    // including a kill.
    drop(claim);

    for suffix in [PARTIAL_SUFFIX, PREVIOUS_SUFFIX] {
        let name = format!("real.{suffix}");
        assert!(
            !dir.path().join(&name).exists(),
            "{name} should have been removed with the claim"
        );
    }
    assert!(
        !dir.path().join("link.lepatch-restore-partial").exists(),
        "and nothing should be left beside the link either"
    );
}

/// A clean restore through a symlink reaches the directory the link points at.
///
/// `restore_atomically` swaps by `rename`, and `rename` moves a symbolic link
/// itself rather than what it points at. So a version that took the destination as
/// written moved the *link* aside and put a plain directory in its place:
///
/// ```text
/// before: link -> /tmp/real,  real empty
/// $ lepatch restore link store --clean
///   restore complete  files=1
/// after:  link is a plain directory holding the snapshot,  real still empty
/// ```
///
/// Reported as success, into a path the user did not name, having replaced a
/// symlink with a directory. The destination is now whatever the resolve step
/// decided it is, so the swap is done against that and the link is left alone.
#[cfg(unix)]
#[test]
fn a_clean_restore_through_a_symlink_lands_in_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("src")).unwrap();
    for i in 0..4 {
        fs::write(
            dir.path().join("src").join(format!("f{i}.bin")),
            common::noisy(120_000 + usize::try_from(i).unwrap_or(0) * 5_000),
        )
        .unwrap();
    }
    ok(dir.path(), &["backup", "src", "store"]);

    let real = dir.path().join("real");
    fs::create_dir_all(&real).unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    ok(
        dir.path(),
        &["restore", link.to_str().unwrap(), "store", "--clean"],
    );

    for i in 0..4 {
        let name = format!("f{i}.bin");
        let restored = real.join(&name);
        assert!(restored.is_file(), "{name} should be in the destination");
        assert_eq!(
            fs::read(&restored).unwrap(),
            fs::read(dir.path().join("src").join(&name)).unwrap(),
            "{name} should have come back byte-identically",
        );
    }

    for name in [
        "link.lepatch-restore-partial",
        "real.lepatch-restore-partial",
        "link.lepatch-restore-previous",
        "real.lepatch-restore-previous",
        "link.lepatch-restore-lock",
    ] {
        assert!(
            !dir.path().join(name).exists(),
            "{name} should not survive a successful clean restore",
        );
    }

    // And the link is still a link. This is the half that used to change.
    assert!(
        std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink(),
        "a restore should not replace the destination's symbolic link with a directory"
    );
}

// ---------------------------------------------------------------------------
// Dry runs.
//
// Each of the five writing subcommands takes `--dry-run`, and each of these says
// two things: that a dry run reports the numbers the real run would print, and
// that it wrote nothing — not one byte, not one file, and not one directory that
// did not exist before it.
//
// The second is the one worth being paranoid about, because the failure is silent:
// a plan that quietly created the store, the lock or the destination it was
// describing would still print a correct-looking report, and the test that only
// checked the report would pass. So every test here compares a listing taken
// before with the same listing taken after, and the interesting cases are the ones
// where the artifact in question did not exist to begin with.
// ---------------------------------------------------------------------------

/// Every name in `dir`, sorted, as a listing a person would give.
///
/// Recursive, because the thing a dry run must not leave behind is not always
/// beside what it was asked about — a clean restore stages its tree next to the
/// destination, and an apply records a manifest next to it.
fn listing(root: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };

        let mut paths: Vec<std::path::PathBuf> =
            entries.filter_map(Result::ok).map(|e| e.path()).collect();
        paths.sort();

        for path in paths {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            out.push(relative.display().to_string());

            // `symlink_metadata`, so a link is not followed into whatever it
            // points at — a tree being backed up may well hold one.
            if fs::symlink_metadata(&path).is_ok_and(|meta| meta.is_dir()) {
                walk(root, &path, out);
            }
        }
    }

    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

/// The figure a report states, as the digits following `marker`.
fn after(report: &str, marker: &str) -> String {
    let at = report
        .find(marker)
        .unwrap_or_else(|| panic!("{marker:?} should be in the report:\n{report}"))
        + marker.len();

    report[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect()
}

/// Every number in a line, in order.
///
/// Both the pack and the apply say what they are doing in one sentence of figures,
/// and what a plan has to agree with is the whole sentence apart from the part it
/// cannot know — so comparing the numbers is simpler and stricter than matching on
/// wording, and it survives the two wordings being different sentences.
fn numbers(line: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut digits = String::new();

    for c in line.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() {
            digits.push(c);
        } else if !digits.is_empty() {
            out.push(digits.parse().unwrap_or(0));
            digits.clear();
        }
    }

    out
}

/// The hash of the snapshot a set's newest version committed.
fn latest_snapshot(dir: &Path, set: &str) -> String {
    let listed = ok(dir, &["versions", set]);
    listed
        .lines()
        .next_back()
        .expect("at least one version")
        .split_whitespace()
        .nth(1)
        .expect("a hash after the version number")
        .to_string()
}

/// The bytes of every regular file in `root`.
///
/// What a restore writes and — with nothing already held — what a backup stores,
/// since both are the tree's own content. Checked against a plan's figure so the
/// report is pinned to the tree rather than to itself.
fn content_bytes(root: &Path) -> u64 {
    common::describe(root)
        .values()
        .filter_map(|entry| match entry {
            common::Entry::File(bytes) => u64::try_from(bytes.len()).ok(),
            _ => None,
        })
        .sum()
}

/// The snapshot a backup plan says it would commit.
fn planned_snapshot(report: &str) -> String {
    report
        .lines()
        .next()
        .expect("a plan says something")
        .split_whitespace()
        .last()
        .expect("and the first line ends with the hash")
        .to_string()
}

/// A backup plan's figures: files, directories, links, chunks, and of those the
/// new, the already held and the bytes to store.
fn backup_figures(report: &str) -> Vec<u64> {
    numbers(
        report
            .lines()
            .find(|line| line.contains("chunks ("))
            .unwrap_or_else(|| panic!("a plan reports its counts:\n{report}")),
    )
}

#[test]
fn a_dry_backup_of_a_new_location_creates_nothing_and_plans_every_chunk() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    let before = listing(dir.path());

    let report = ok(dir.path(), &["backup", "src", "snap", "--dry-run"]);

    // Nothing at all. The set's directory does not exist and must not be made:
    // `open_for_writing` creates segment 0 for a set with none, so a dry run that
    // opened the store the way a backup does would leave an empty store behind and
    // report a first version in it.
    assert_eq!(
        listing(dir.path()),
        before,
        "a dry backup changed the directory"
    );
    assert!(
        !dir.path().join("snap.0000.bin").exists(),
        "a dry backup created a segment"
    );
    assert!(
        !dir.path().join("snap.lock").exists(),
        "a dry backup took the write lock"
    );

    // The plan is a plan: it says what it would commit, and it knows there is no set
    // behind the name yet rather than failing the way reading one would.
    assert!(
        report.contains("would create it"),
        "the first backup of a location should plan as an all-new version: {report}"
    );
    let planned = planned_snapshot(&report);
    assert_eq!(
        planned.len(),
        64,
        "a plan should name the snapshot it would commit, got: {report}"
    );

    // And it is true. The real run immediately afterwards commits exactly that
    // snapshot, which is what makes the hash worth printing at all.
    ok(dir.path(), &["backup", "src", "snap"]);
    assert_eq!(
        latest_snapshot(dir.path(), "snap"),
        planned,
        "the planned snapshot is not the one the next backup committed"
    );

    // Nothing was held before, so every chunk is new — and the byte total is the
    // tree's content, which is a figure a test can check against the tree itself.
    let figures = backup_figures(&report);
    let (files, _dirs, _links, chunks, fresh, held, stored) = (
        figures[0], figures[1], figures[2], figures[3], figures[4], figures[5], figures[6],
    );

    assert_eq!(fresh, chunks, "a first backup stores every chunk: {report}");
    assert_eq!(held, 0, "and there is nothing to hold: {report}");
    assert_ne!(chunks, 0, "a tree this size has chunks: {report}");
    assert_eq!(
        files, 3,
        "the fixture holds three regular files and two links: {report}"
    );

    let content = content_bytes(&dir.path().join("src"));
    assert_eq!(
        stored, content,
        "the bytes a plan would store are the tree's own, since nothing is held: {report}"
    );
}

#[test]
fn a_dry_backup_of_an_unchanged_set_reports_nothing_to_store_and_grows_nothing() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    ok(dir.path(), &["backup", "src", "snap"]);
    let set = common::set_size(&dir.path().join("snap"));
    let before = listing(dir.path());

    let report = ok(dir.path(), &["backup", "src", "snap", "--dry-run"]);

    assert_eq!(
        listing(dir.path()),
        before,
        "a dry backup changed the directory"
    );
    assert_eq!(
        common::set_size(&dir.path().join("snap")),
        set,
        "a dry backup appended to the store"
    );

    let figures = backup_figures(&report);
    assert_eq!(
        figures[4], 0,
        "an unchanged tree has nothing new to store: {report}"
    );
    assert_eq!(figures[6], 0, "and nothing to write: {report}");
    assert_eq!(
        figures[5], figures[3],
        "every chunk is one the set already holds: {report}"
    );
    assert!(figures[3] > 0, "a tree this size has chunks: {report}");

    // The version it would commit is the next one, and the tree is unchanged, so
    // the snapshot hash is the one the *first* backup committed — a second version
    // of the same tree is the same version.
    ok(dir.path(), &["backup", "src", "snap"]);
    assert_eq!(
        latest_snapshot(dir.path(), "snap"),
        planned_snapshot(&report),
        "the planned snapshot is not the one the next backup committed"
    );
    assert_eq!(
        count_entries(dir.path(), "snap"),
        2,
        "the dry run should not have published anything"
    );
}

#[test]
fn a_dry_restore_writes_no_file_no_directory_and_no_claim() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    let destination = dir.path().join("out");
    let report = ok(dir.path(), &["restore", "out", "snap", "--dry-run"]);

    assert!(
        !destination.exists(),
        "a dry restore created the destination"
    );
    assert!(
        !claim_path(&destination).exists(),
        "a dry restore took a claim beside the destination"
    );
    assert!(
        !dir.path().join("out.lepatch-restore-partial").exists(),
        "a dry restore staged a tree beside the destination"
    );

    // The counts are the ones the restore will go on to report, and the bytes are
    // the tree's — a restore writes each recorded file whole.
    let source_bytes = content_bytes(&dir.path().join("src"));
    let figures = numbers(report.lines().nth(1).expect("a plan reports its counts"));
    assert_eq!(
        figures[0], 3,
        "the fixture holds three regular files: {report}"
    );
    assert_eq!(figures[1], 2, "and two directories: {report}");
    assert_eq!(
        figures[4], source_bytes,
        "a restore writes every recorded file whole; the tree is {source_bytes} bytes: {report}"
    );
    assert_ne!(figures[3], 0, "the version names chunks: {report}");

    // And the one figure a real restore can only turn into a count afterwards: the
    // bound a plan gives, in the apply dry run's words, because whether this
    // process could set an owner is only knowable by setting it.
    let logs = ok_logs(dir.path(), &["restore", "out", "snap", "--dry-run"]);
    assert!(
        logs.contains(
            "note: 5 entries in this version record an owner, which a restore may not be \
             able to set without privilege"
        ),
        "a dry restore should bound what it might not be able to set: {logs}"
    );

    // And it is true: the real run writes that many bytes into that many files.
    ok(dir.path(), &["restore", "out", "snap"]);
    common::assert_same_tree(&dir.path().join("src"), &destination);
}

#[test]
fn a_dry_clean_restore_does_not_empty_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    let destination = dir.path().join("out");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("mine"), b"work in progress\n").unwrap();
    let before = common::describe(&destination);

    // `--clean` is the one operation here that destroys data, so a dry run that
    // did not empty the destination would be the single most surprising thing this
    // flag could do.
    let report = ok(
        dir.path(),
        &["restore", "out", "snap", "--clean", "--dry-run"],
    );
    assert!(
        report.contains("would be emptied first"),
        "a dry clean restore should say what it would empty: {report}"
    );

    assert_eq!(
        common::describe(&destination),
        before,
        "a dry clean restore changed the destination"
    );
    assert!(
        !dir.path().join("out.lepatch-restore-partial").exists(),
        "a dry clean restore staged a tree beside the destination"
    );
    assert!(
        !dir.path().join("out.lepatch-restore-previous").exists(),
        "a dry clean restore moved the destination aside"
    );
}

#[test]
fn a_dry_pack_writes_no_file_and_reports_what_it_would_hold() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    let plan = ok(dir.path(), &["pack", "snap", "v1.pack", "--dry-run"]);

    assert!(
        !dir.path().join("v1.pack").exists(),
        "a dry pack wrote the file it was asked about"
    );
    assert!(
        !plan.contains(&latest_snapshot(dir.path(), "snap")),
        "a dry pack has no content hash, and should not print one: {plan}"
    );
    assert!(
        plan.lines().count() == 1,
        "a dry pack prints a plan and not a hash: {plan}"
    );
    assert!(
        plan.contains("nothing written to v1.pack"),
        "a dry pack should say the file was not written: {plan}"
    );

    // The plan and the pack agree, chunk for chunk and byte for byte: the plan's
    // sentence is the pack's with the one figure a plan cannot know left off.
    let written = ok(dir.path(), &["pack", "snap", "v1.pack"]);
    let planned = numbers(
        plan.lines()
            .last()
            .expect("a summary line")
            .split("; nothing")
            .next()
            .unwrap_or_default(),
    );
    let actual = numbers(written.lines().last().expect("a summary line"));

    assert!(
        !planned.is_empty(),
        "the plan should report figures: {plan}"
    );
    assert_eq!(
        actual.get(..planned.len()),
        Some(planned.as_slice()),
        "a dry pack should report the figures the pack has:\n{plan}\n{written}"
    );
    assert!(
        written.lines().count() == 2,
        "a written pack prints its content hash and its summary: {written}"
    );
}

#[test]
fn a_dry_delta_pack_reports_the_chunks_it_would_leave_out() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0u8..5 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(i, 200_000),
        )
        .unwrap();
    }
    ok(dir.path(), &["backup", "source", "set"]);
    fs::write(source.join("f2.bin"), common::untagged_stream(9, 230_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);

    let plan = ok(
        dir.path(),
        &[
            "pack",
            "set",
            "v2.delta",
            "--version",
            "2",
            "--from",
            "1",
            "--dry-run",
        ],
    );
    assert!(
        !dir.path().join("v2.delta").exists(),
        "a dry delta pack wrote the file it was asked about"
    );
    assert!(
        plan.contains("since 1"),
        "a dry delta pack should still say what it is a delta against: {plan}"
    );

    let written = ok(
        dir.path(),
        &["pack", "set", "v2.delta", "--version", "2", "--from", "1"],
    );
    let planned = numbers(
        plan.lines()
            .last()
            .expect("a summary line")
            .split("; nothing")
            .next()
            .unwrap_or_default(),
    );
    let actual = numbers(written.lines().last().expect("a summary line"));

    assert_eq!(
        actual.get(..planned.len()),
        Some(planned.as_slice()),
        "a dry delta pack should report the figures the delta has:\n{plan}\n{written}"
    );
}

#[test]
fn a_dry_apply_writes_nothing_and_records_no_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    for i in 0..4 {
        fs::write(source.join(format!("f{i}.bin")), common::noisy(150_000)).unwrap();
    }
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);

    let target = dir.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let before = listing(dir.path());

    let plan = ok(dir.path(), &["apply", "v1.pack", "target", "--dry-run"]);

    assert_eq!(
        listing(dir.path()),
        before,
        "a dry apply changed the directory"
    );
    assert!(
        !claim_path(&target).exists(),
        "a dry apply took a claim beside the destination"
    );
    assert!(
        !listing(dir.path())
            .iter()
            .any(|name| name.contains(lepatch::command::MANIFEST_SUFFIX)),
        "a dry apply recorded a manifest beside the destination"
    );

    // Nothing is held yet, so every chunk would be written, and the target is
    // empty so every file would be opened.
    let figures = numbers(plan.lines().next().expect("a fetch line"));
    let (fetched, payload, _ranges, held, chunks, written, files) = (
        figures[0], figures[1], figures[2], figures[3], figures[4], figures[5], figures[6],
    );

    assert_eq!(held, 0, "an empty target holds nothing: {plan}");
    assert_eq!(fetched, payload, "so all of it would be fetched: {plan}");
    assert_eq!(chunks, written, "and every chunk would be written: {plan}");
    assert_eq!(files, 4, "one file per file in the tree: {plan}");

    // And the real run goes on to say exactly those numbers.
    let applied = ok(dir.path(), &["apply", "v1.pack", "target"]);
    assert_eq!(
        numbers(applied.lines().next().expect("a fetch line")),
        numbers(plan.lines().next().expect("a fetch line")),
        "a dry apply should report the figures a real apply does:\n{plan}\n{applied}"
    );
    common::assert_same_tree(&source, &target);
}

#[test]
fn a_dry_apply_with_prune_removes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("kept.bin"), common::noisy(120_000)).unwrap();
    ok(dir.path(), &["backup", "source", "set"]);
    ok(dir.path(), &["pack", "set", "v1.pack"]);
    ok(dir.path(), &["apply", "v1.pack", "target"]);

    // Somebody's own work, in a directory the version does not record.
    let stray = dir.path().join("target/notes.txt");
    fs::write(&stray, b"mine\n").unwrap();
    let before = common::describe(&dir.path().join("target"));

    let plan = ok(
        dir.path(),
        &["apply", "v1.pack", "target", "--prune", "--dry-run"],
    );

    assert_eq!(
        common::describe(&dir.path().join("target")),
        before,
        "a dry apply removed something"
    );
    assert!(
        plan.contains("would remove 1 unrecorded entry"),
        "a dry prune should say what it would remove: {plan}"
    );
    assert!(stray.is_file(), "the stray entry is gone after a dry run");
}

#[test]
fn a_dry_reindex_writes_no_index_file_and_takes_no_lock() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    for index in index_files(dir.path(), "snap") {
        fs::remove_file(index).unwrap();
    }
    let before = listing(dir.path());

    let plan = ok(dir.path(), &["reindex", "snap", "--dry-run"]);

    assert_eq!(
        listing(dir.path()),
        before,
        "a dry reindex changed the directory"
    );
    assert!(
        index_files(dir.path(), "snap").is_empty(),
        "a dry reindex wrote an index file"
    );
    assert!(
        !dir.path().join("snap.lock").exists(),
        "a dry reindex took the write lock"
    );
    assert_eq!(
        after(&plan, "index file(s) would be written, "),
        "0",
        "the index files were deleted before the dry run: {plan}"
    );
    assert_eq!(
        after(&plan, "dry run: "),
        "1",
        "and the set holds one version: {plan}"
    );

    // The figure a plan leads with is how many index files the real run writes,
    // which is a count of what it can resolve rather than of what it holds.
    let planned = numbers(plan.lines().nth(1).expect("a plan reports its counts"))[0];

    ok(dir.path(), &["reindex", "snap"]);
    assert_eq!(
        index_files(dir.path(), "snap").len(),
        1,
        "the real reindex should have written the index file"
    );
    assert_eq!(
        u64::try_from(index_files(dir.path(), "snap").len()).unwrap(),
        planned,
        "the plan should report the index files a real reindex writes: {plan}"
    );
}

/// A dry run is taken against a set or a destination something else holds.
///
/// Every write a real run makes around the work — the write lock on a set, the
/// claim beside a destination — is a file, and a plan that created one would be
/// making the very claim it must not. A listing taken after the run cannot see
/// this, because the file is removed on the way out again; the claim that *was*
/// taken is what another process would have collided with while the run was in
/// progress, so the way to see it is to be holding it.
///
/// Which is also the property worth having: the reason a dry run takes no claim
/// is that it reads nothing that a claim protects, so a plan can be taken against
/// a set a backup is holding or a destination a restore is writing into. The
/// refusals at the end are what make the successes meaningful — without them, a
/// lock nobody honours would prove nothing.
#[test]
fn a_dry_run_needs_no_claim_and_takes_none() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);
    ok(dir.path(), &["pack", "snap", "v1.pack"]);

    // Two claims somebody else is holding: the set's own lock, and the one a
    // restore or an apply takes beside a destination.
    let set_lock = dir.path().join("snap.lock");
    fs::write(&set_lock, b"").unwrap();
    let destination = dir.path().join("out");
    let destination_claim = claim_path(&destination);
    fs::write(&destination_claim, b"").unwrap();

    ok(dir.path(), &["backup", "src", "snap", "--dry-run"]);
    ok(dir.path(), &["reindex", "snap", "--dry-run"]);
    ok(dir.path(), &["restore", "out", "snap", "--dry-run"]);
    ok(dir.path(), &["apply", "v1.pack", "out", "--dry-run"]);

    // And the same commands are refused while the claims are held, so the four
    // successes above are about the dry run rather than about a lock nobody takes.
    fails(dir.path(), &["backup", "src", "snap"]);
    fails(dir.path(), &["reindex", "snap"]);
    fails(dir.path(), &["restore", "out", "snap"]);
    fails(dir.path(), &["apply", "v1.pack", "out"]);

    // Neither claim was disturbed: a plan that took one and dropped it on the way
    // out would have removed somebody else's.
    assert!(set_lock.is_file(), "the write lock should still be there");
    assert!(
        destination_claim.is_file(),
        "the destination's claim should still be there"
    );
}

#[test]
fn versions_and_verify_refuse_a_dry_run_rather_than_pretending_to_have_one() {
    // They are already read-only, so a flag that did nothing there would be noise
    // in `--help`. Refusal is the honest answer, and it is what clap gives.
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    for args in [
        &["versions", "snap", "--dry-run"][..],
        &["verify", "snap", "--dry-run"][..],
    ] {
        let output = lepatch(dir.path(), args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "`lepatch {}` is a usage error, not a failure of the command: {args:?}",
            args.join(" ")
        );
        assert!(
            stderr(&output).contains("unexpected argument '--dry-run'"),
            "and it should say the argument is not one this command takes: {}",
            stderr(&output)
        );
    }
}

#[test]
fn every_writing_subcommand_documents_its_dry_run() {
    // The help is where somebody looks before trusting the flag, and the neighbours
    // say both what it affects and what it does not — so this checks both are there
    // rather than just the word.
    let dir = tempfile::tempdir().unwrap();

    for (command, affects, does_not) in [
        (
            "backup",
            "the tree is still walked",
            "not the set's directory",
        ),
        ("restore", "still checked", "no claim is taken"),
        ("pack", "still located", "`--out` is neither created"),
        ("apply", "hashed", "no manifest recorded"),
        (
            "reindex",
            "version count is the store's own",
            "no lock is taken",
        ),
        ("merge", "still resolved", "no lock is taken"),
    ] {
        let help = ok(dir.path(), &[command, "--help"]);
        assert!(
            help.contains("--dry-run"),
            "`lepatch {command} --help` should offer --dry-run"
        );
        assert!(
            help.contains(affects),
            "`lepatch {command} --help` should say what a dry run still does: {help}"
        );
        assert!(
            help.contains(does_not),
            "`lepatch {command} --help` should say what a dry run does not do: {help}"
        );
    }
}

// ---------------------------------------------------------------------------
// `lepatch merge` — the receiving half of a pack.
// ---------------------------------------------------------------------------

/// A source tree with several files, big enough that a delta is a real fraction
/// of it rather than one chunk.
fn merge_tree(dir: &Path) -> std::path::PathBuf {
    let source = dir.join("source");
    fs::create_dir_all(source.join("nested")).unwrap();
    for i in 0..6 {
        fs::write(
            source.join(format!("f{i}.bin")),
            noisy(200_000 + i * 20_000),
        )
        .unwrap();
    }
    fs::write(source.join("nested/leaf.txt"), b"a leaf\n").unwrap();
    source
}

/// Every entry of a restored tree, by path, as `describe` sees it.
fn set_contents(
    dir: &Path,
    name: &str,
    version: u32,
) -> std::collections::BTreeMap<PathBuf, common::Entry> {
    let out = dir.join(format!("out-{name}-{version}"));
    ok(
        dir,
        &[
            "restore",
            out.to_str().unwrap(),
            name,
            "--version",
            &version.to_string(),
            "--clean",
        ],
    );
    common::describe(&out)
}

/// `lepatch merge` is the other half of `pack`, and the two must compose.
///
/// The property that decides whether merge is a real operation rather than a
/// write that happens to succeed: a set built by merging a pack restores to the
/// same tree as one built by `backup`, byte for byte. Compared as trees rather
/// than by exit code, because a merge that dropped a file, truncated one or
/// stored the wrong chunk at the wrong offset would still exit 0.
///
/// What would falsify it: the merge storing chunks under the wrong hashes, or
/// committing a snapshot that does not match the chunks beside it, or the pack
/// being read at the wrong offsets. Any of those leaves a set that restores to
/// something else, which `assert_same_tree` catches and an exit code does not.
#[test]
fn a_merged_set_restores_exactly_as_a_backed_up_one() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());

    ok(
        dir.path(),
        &["backup", source.to_str().unwrap(), "backed-up"],
    );
    ok(dir.path(), &["pack", "backed-up", "v1.pack"]);
    ok(dir.path(), &["merge", "v1.pack", "merged"]);

    let backed = set_contents(dir.path(), "backed-up", 1);
    let merged = set_contents(dir.path(), "merged", 1);

    assert!(
        !backed.is_empty(),
        "the fixture should have restored something to compare"
    );
    assert_eq!(
        backed, merged,
        "a set built by merging a pack restored differently from one built by backup"
    );
    // And both against the tree itself, so a difference in `describe` cannot
    // make two wrong sets look equal.
    common::assert_same_tree(&source, &dir.path().join("out-merged-1"));
}

/// A full pack into a set that is not there creates it, which is the case a
/// remote push is for: a new machine receiving its first version.
///
/// Falsified by a merge that refused a name with nothing behind it, or that
/// created the set and then failed to leave a version in it.
#[test]
fn a_full_pack_creates_a_set_that_is_not_there() {
    let dir = tempfile::tempdir().unwrap();
    merge_tree(dir.path());

    ok(dir.path(), &["backup", "source", "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    assert!(
        !dir.path().join("received.0000.bin").exists(),
        "the fixture should start with no set at the receiving name"
    );

    let report = ok(dir.path(), &["merge", "v1.pack", "received"]);

    assert!(
        report.contains("version 1"),
        "the first version into a new set should be version 1: {report}"
    );
    let versions = ok(dir.path(), &["versions", "received", "--numbers-only"]);
    assert_eq!(
        versions.trim(),
        "1",
        "the new set should hold exactly the version that was merged: {versions}"
    );
    set_contents(dir.path(), "received", 1);
    common::assert_same_tree(
        &dir.path().join("source"),
        &dir.path().join("out-received-1"),
    );
}

/// The same pack into a set that already holds versions.
///
/// The falsifiable part is that the chunks already held are *not* written again
/// and the set still grows a version: a merge that deduplicated without
/// committing would lose the version, and one that committed without
/// deduplicating would double the store.
#[test]
fn a_full_pack_merges_into_a_set_that_already_has_versions() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());

    // Version 1 of the receiving set, from a different tree.
    fs::create_dir_all(dir.path().join("other")).unwrap();
    fs::write(dir.path().join("other/other.bin"), noisy(50_000)).unwrap();
    ok(dir.path(), &["backup", "other", "received"]);

    // Then the pack of a version made somewhere else, which shares nothing.
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    let report = ok(dir.path(), &["merge", "v1.pack", "received"]);

    assert!(
        report.contains("version 2"),
        "the merged version should follow the one already there: {report}"
    );
    assert!(
        figure(&report, "chunks, ").is_some(),
        "the report should count the chunks: {report}"
    );
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        2,
        "the set should now hold both versions"
    );
    // The first version is still restorable, which is the half a re-merge could
    // quietly break.
    set_contents(dir.path(), "received", 1);
    common::assert_same_tree(
        &dir.path().join("other"),
        &dir.path().join("out-received-1"),
    );
    set_contents(dir.path(), "received", 2);
    common::assert_same_tree(&source, &dir.path().join("out-received-2"));
}

/// Merging the same pack twice makes a second version and stores no chunk bytes.
///
/// The decision this pins: allowed rather than refused. A store is a log, and a
/// transfer may be retried, so a second arrival has to be answerable. What would
/// falsify the test is either half of the opposite choice — a refusal, or a
/// second version whose report claims chunks were written when nothing was.
#[test]
fn merging_the_same_pack_twice_adds_a_version_and_stores_nothing() {
    let dir = tempfile::tempdir().unwrap();
    merge_tree(dir.path());
    ok(dir.path(), &["backup", "source", "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    ok(dir.path(), &["merge", "v1.pack", "received"]);
    let first = fs::metadata(dir.path().join("received.0000.bin"))
        .unwrap()
        .len();

    let second = ok(dir.path(), &["merge", "v1.pack", "received"]);

    assert!(
        second.contains("version 2"),
        "the second merge should be version 2: {second}"
    );
    assert!(
        second.contains(", 0 new, "),
        "every chunk was already held, so nothing should have been stored: {second}"
    );
    let said = ok_logs(dir.path(), &["merge", "v1.pack", "received", "--dry-run"]);
    assert!(
        said.contains("already holds this version as version 1"),
        "a user has to be able to tell a repeat from a first merge: {said}"
    );

    // Both versions are real, and both restore the same tree — which is what
    // makes the second one harmless rather than a mistake.
    // The only growth is the second commit record: `put` deduplicated every
    // chunk, so no payload was written, and a repeat that re-sent the version
    // would grow the segment by the whole pack.
    let grown = fs::metadata(dir.path().join("received.0000.bin"))
        .unwrap()
        .len();
    assert!(
        grown - first < 1024,
        "a repeat merge should add a commit record and nothing else: {first} to {grown}"
    );
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        2
    );
    set_contents(dir.path(), "received", 1);
    set_contents(dir.path(), "received", 2);
    common::assert_same_tree(
        &dir.path().join("source"),
        &dir.path().join("out-received-1"),
    );
    common::assert_same_tree(
        &dir.path().join("source"),
        &dir.path().join("out-received-2"),
    );
}

/// A delta merges into a set that holds its base, and does not re-send what the
/// base already has.
///
/// The falsifiable part is the count: if the delta's chunks were all written, or
/// if the base's chunks were fetched from nowhere, the "already held" figure
/// would not be a delta-sized one.
#[test]
fn a_delta_merges_into_a_set_holding_its_base() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    // The receiving set takes version 1, then the set moves on.
    ok(dir.path(), &["merge", "v1.pack", "received"]);

    // Version 2 of the origin: one file rewritten, one added.
    fs::write(source.join("f3.bin"), noisy(260_000)).unwrap();
    fs::write(source.join("fresh.bin"), noisy(30_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(
        dir.path(),
        &[
            "pack",
            "origin",
            "v2.delta",
            "--version",
            "2",
            "--from",
            "1",
        ],
    );

    let report = ok(dir.path(), &["merge", "v2.delta", "received"]);

    assert!(
        report.contains("since 1"),
        "a merged delta should name the base version it was against: {report}"
    );
    assert!(
        report.contains("version 2"),
        "the delta should become the set's next version: {report}"
    );
    let fresh = figure(&report, ", ").and_then(|_| figure(&report, " new, "));
    let whole = figure(&report, ": ").unwrap_or(0);
    assert!(
        fresh.unwrap_or(0) < whole,
        "a delta of one rewritten file out of seven should not store every chunk: \
         {fresh:?} of {whole:?} in {report}"
    );

    set_contents(dir.path(), "received", 2);
    common::assert_same_tree(&source, &dir.path().join("out-received-2"));
    // And version 1 of the receiving set is untouched by the merge, which is the
    // half a delta merge could quietly break: the base's own chunks had to be
    // found in the set rather than fetched from the pack.
    set_contents(dir.path(), "received", 1);
    common::assert_same_tree(
        &merge_tree_at_v1(dir.path()),
        &dir.path().join("out-received-1"),
    );
}

/// A delta merges into a set left with a torn tail, and the merge repairs it.
///
/// A merge killed mid-`put` leaves bytes past the last valid record, and nothing
/// ever claims them, so the set is not damaged — it is unfinished. The run that
/// comes next is what reclaims that space, and this pins that the run is a
/// *merge* that does it: refusing the tail instead would leave a delta
/// permanently unmergeable into its own set, with no command left that could fix
/// it.
///
/// What would falsify it: a merge that refuses the torn set the way a read does,
/// or one that succeeds and leaves the torn bytes where they were.
#[test]
fn a_delta_merges_into_a_set_left_with_a_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    // The receiving set takes version 1, and a run then dies part-way through
    // the next one: a record header with its payload cut short, and no commit
    // record behind it.
    ok(dir.path(), &["merge", "v1.pack", "received"]);
    let segment = dir.path().join("received.0000.bin");
    let mut bytes = fs::read(&segment).unwrap();
    bytes.extend_from_slice(b"LPB1\x00\x80\x01");
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(b"half a record");
    fs::write(&segment, &bytes).unwrap();

    // The set really is torn, which is what the merge is about: a read refuses
    // it, because a reader does not repair.
    let refused = fails(dir.path(), &["restore", "out", "received"]);
    assert!(
        refused.contains("does not repair"),
        "the fixture should have left a set a reader refuses, got: {refused}"
    );
    assert!(
        !dir.path().join("out").exists(),
        "the refused restore should not have produced a tree"
    );

    // The delta of version 2, which can only be merged by a set holding its
    // base — and version 1 is there, commit record and all.
    fs::write(source.join("f3.bin"), noisy(260_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(
        dir.path(),
        &[
            "pack",
            "origin",
            "v2.delta",
            "--version",
            "2",
            "--from",
            "1",
        ],
    );

    let report = ok(dir.path(), &["merge", "v2.delta", "received"]);

    assert!(
        report.contains("version 2"),
        "the delta should have merged into the torn set: {report}"
    );
    // The merge is the run that repaired the set: the same read that refused it
    // a moment ago now works, and restores the tree the delta merged.
    set_contents(dir.path(), "received", 2);
    common::assert_same_tree(&source, &dir.path().join("out-received-2"));
    assert!(
        !fs::read(&segment)
            .unwrap()
            .windows(b"half a record".len())
            .any(|w| w == b"half a record"),
        "the merge should have truncated the torn tail rather than kept it"
    );
    set_contents(dir.path(), "received", 1);
    common::assert_same_tree(
        &merge_tree_at_v1(dir.path()),
        &dir.path().join("out-received-1"),
    );
}

/// The tree as it was at version 1 of `origin`, restored.
///
/// Version 1 is gone from `source` by the time the delta test looks at it, and
/// this is how the earlier version is compared without assuming anything about
/// what a later version changed.
fn merge_tree_at_v1(dir: &Path) -> std::path::PathBuf {
    let copy = dir.join("at-v1");
    let _ = fs::remove_dir_all(&copy);
    ok(
        dir,
        &[
            "restore",
            copy.to_str().unwrap(),
            "origin",
            "--version",
            "1",
            "--clean",
        ],
    );
    copy
}

/// A delta is refused for a set that does not hold its base, and refuses to
/// create one.
///
/// The whole reason the base check exists, and the half that is easy to get
/// wrong: a refused merge must leave the name exactly as it was, or the refusal
/// has created the set it just declined to use. Falsified by an exit code of 0,
/// by a segment appearing, or by a version in the set afterwards.
#[test]
fn a_delta_is_refused_for_a_set_that_does_not_hold_its_base() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    fs::write(source.join("f3.bin"), noisy(260_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(
        dir.path(),
        &[
            "pack",
            "origin",
            "v2.delta",
            "--version",
            "2",
            "--from",
            "1",
        ],
    );

    // Into a name with no set behind it.
    let output = lepatch(dir.path(), &["merge", "v2.delta", "absent"]);
    assert!(
        !output.status.success(),
        "a delta into a set that does not exist should be refused"
    );
    let said = stderr(&output);
    assert!(
        said.contains("is a delta against") && said.contains("merge the full pack instead"),
        "and the refusal should say what to do instead: {said}"
    );
    assert!(
        !dir.path().join("absent.0000.bin").exists(),
        "the refused merge should not have created the set: {:?}",
        fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
}

/// A delta is refused for a set that holds *other* versions but not this one.
///
/// The case a "is the set on the base version?" check gets backwards. A set at a
/// later version still holds every chunk of an earlier one, so the only thing
/// that matters is whether the base is among its committed versions — and here it
/// is not, however many versions there are.
#[test]
fn a_delta_is_refused_for_a_set_holding_only_other_versions() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    // Version 2 and then 3 exist at the origin, so a delta against version 2 can
    // be packed for version 3.
    fs::write(source.join("f4.bin"), noisy(240_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    fs::write(source.join("f5.bin"), noisy(255_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(
        dir.path(),
        &[
            "pack",
            "origin",
            "v3.delta",
            "--version",
            "3",
            "--from",
            "2",
        ],
    );

    // The receiving set holds version 1 only.
    ok(dir.path(), &["merge", "v1.pack", "received"]);
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        1
    );

    let refused = fails(dir.path(), &["merge", "v3.delta", "received"]);
    assert!(
        refused.contains("is a delta against"),
        "the refusal should name the pack as a delta: {refused}"
    );
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        1,
        "the refused delta should have published nothing"
    );

    // The full pack of the same version is accepted into the same set, which is
    // the alternative the refusal points at.
    ok(dir.path(), &["pack", "origin", "v3.pack", "--version", "3"]);
    ok(dir.path(), &["merge", "v3.pack", "received"]);
    set_contents(dir.path(), "received", 2);
    common::assert_same_tree(&source, &dir.path().join("out-received-2"));
}

/// A delta is merged into a set that is *ahead* of its base.
///
/// The other half of the case above, and the one that justifies the containment
/// check: the set holds version 3 while the delta assumes version 1, and the
/// merge must succeed because every chunk of version 1 is still there. A check
/// ported from `apply` — which establishes a base against a *directory* that may
/// hold extra files — would refuse this, and refusing it would be wrong.
#[test]
fn a_delta_is_merged_into_a_set_that_is_ahead_of_its_base() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    // The receiving set takes version 1 and then version 2.
    ok(dir.path(), &["merge", "v1.pack", "received"]);

    fs::write(source.join("f0.bin"), noisy(230_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v2.pack", "--version", "2"]);
    ok(dir.path(), &["merge", "v2.pack", "received"]);

    // A delta assuming version 1, arriving at a set now on version 2.
    fs::write(source.join("f1.bin"), noisy(250_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(
        dir.path(),
        &[
            "pack",
            "origin",
            "v3.delta",
            "--version",
            "3",
            "--from",
            "1",
        ],
    );

    let report = ok(dir.path(), &["merge", "v3.delta", "received"]);
    assert!(
        report.contains("since 1"),
        "the delta should have been merged against the version it names: {report}"
    );
    set_contents(dir.path(), "received", 3);
    common::assert_same_tree(&source, &dir.path().join("out-received-3"));
    // And the versions the delta did not concern are still there, which is what
    // makes a set ahead of its base safe to merge into.
    set_contents(dir.path(), "received", 1);
    set_contents(dir.path(), "received", 2);
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        3,
        "the merged delta should have left every earlier version in place"
    );
}

/// A pack whose payload has been damaged is refused, and publishes nothing.
///
/// Falsified by a merge that succeeds: the chunk is addressed by a hash of its
/// contents, and `put` checks what it is given against it, so damaged bytes
/// cannot be stored under a chunk's name. What is *not* asserted is that nothing
/// was written to the segment — records no version references are wasted space,
/// not corruption, and the store's own recovery reclaims them.
#[test]
fn a_damaged_pack_is_refused_and_publishes_no_version() {
    let dir = tempfile::tempdir().unwrap();
    merge_tree(dir.path());
    ok(dir.path(), &["backup", "source", "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    // One byte of payload flipped. The header and index still agree with each
    // other, so `Pack::open` accepts this file; only the bytes are wrong.
    let bytes = fs::read(dir.path().join("v1.pack")).unwrap();
    let mut damaged = bytes.clone();
    let at = 200;
    damaged[at] ^= 0xff;
    fs::write(dir.path().join("damaged.pack"), &damaged).unwrap();

    let refused = fails(dir.path(), &["merge", "damaged.pack", "received"]);
    assert!(
        refused.contains("does not hash to") || refused.contains("not a pack"),
        "the refusal should name what is wrong with the pack: {refused}"
    );

    // Nothing was published. The segment may exist — the open created it, and the
    // records it holds are unreferenced — but there is no version.
    let output = lepatch(dir.path(), &["versions", "received"]);
    assert!(
        output.status.success(),
        "the set exists, so listing it should work: {}",
        stderr(&output)
    );
    assert!(
        stdout(&output).trim().is_empty(),
        "a refused merge should have published no version, got: {}",
        stdout(&output)
    );

    // And a truncated pack is refused too, by the reader rather than by a hash.
    fs::write(dir.path().join("cut.pack"), &bytes[..bytes.len() / 2]).unwrap();
    let cut = fails(dir.path(), &["merge", "cut.pack", "other"]);
    assert!(
        cut.contains("not a pack"),
        "a truncated pack should be refused as one, got: {cut}"
    );
}

/// `--dry-run` writes nothing and says what it would have merged.
///
/// The falsifiable part is the *nothing*: not just no new version, but no
/// segment, no index file, no lock, and not the set's directory for a name that
/// had none. A dry run that created a set in order to report on it would be the
/// one thing the flag promises it never does.
#[test]
fn a_dry_merge_writes_nothing_and_reports_what_it_would_merge() {
    let dir = tempfile::tempdir().unwrap();
    merge_tree(dir.path());
    ok(dir.path(), &["backup", "source", "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    // Into a name with no set behind it.
    let plan = ok(dir.path(), &["merge", "v1.pack", "absent", "--dry-run"]);
    assert!(
        plan.contains("would create it"),
        "a plan for a set that is not there should say it would create one: {plan}"
    );
    assert!(
        plan.contains("nothing written"),
        "and should say it wrote nothing: {plan}"
    );
    assert!(
        !dir.path().join("absent.0000.bin").exists(),
        "a dry merge created the set: {:?}",
        fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
    assert!(
        !dir.path().join("absent.lock").exists(),
        "a dry merge took the lock"
    );

    // Against a set that does exist: the figures are the merge's own, and still
    // nothing moves.
    ok(dir.path(), &["merge", "v1.pack", "received"]);
    let before = fs::read(dir.path().join("received.0000.bin")).unwrap();

    let again = ok(dir.path(), &["merge", "v1.pack", "received", "--dry-run"]);
    assert!(
        again.contains("already held"),
        "a dry merge into a set that holds the version should say so: {again}"
    );
    assert_eq!(
        fs::read(dir.path().join("received.0000.bin")).unwrap(),
        before,
        "a dry merge grew the segment"
    );
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        1,
        "a dry merge published a version"
    );

    // And the real run then does exactly what the plan said.
    let real = ok(dir.path(), &["merge", "v1.pack", "received"]);
    assert!(
        real.contains("version 2"),
        "the real merge should commit what the plan reported: {real}"
    );
}

/// A dry merge refuses a delta for a set that does not hold its base, like a
/// real one.
///
/// A plan that waved a delta through would send somebody to run a command that
/// cannot work, and the report would have been the last place the problem
/// surfaced.
#[test]
fn a_dry_merge_refuses_a_delta_whose_base_the_set_lacks() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    fs::write(source.join("f2.bin"), noisy(240_000)).unwrap();
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(
        dir.path(),
        &[
            "pack",
            "origin",
            "v2.delta",
            "--version",
            "2",
            "--from",
            "1",
        ],
    );

    let refused = fails(dir.path(), &["merge", "v2.delta", "absent", "--dry-run"]);
    assert!(
        refused.contains("is a delta against"),
        "a dry merge should refuse a delta for a set that lacks its base: {refused}"
    );
    assert!(
        !dir.path().join("absent.0000.bin").exists(),
        "and should not have created the set to find out"
    );

    // With the base merged first, the plan is happy and reports a delta-sized
    // figure — which is the difference the refusal is about.
    ok(dir.path(), &["merge", "v1.pack", "received"]);
    let plan = ok(dir.path(), &["merge", "v2.delta", "received", "--dry-run"]);
    assert!(plan.contains("since 1"), "{plan}");
    assert!(
        plan.contains("nothing written"),
        "a dry merge should say it wrote nothing: {plan}"
    );
    assert_eq!(
        ok(dir.path(), &["versions", "received", "--numbers-only"])
            .lines()
            .count(),
        1,
        "the plan published a version"
    );
}

/// A dry merge refuses a damaged pack, like a real one.
///
/// The plan resolves the pack and hashes every chunk the merge would store, so a
/// pack it waves through is a pack a real merge accepts. A plan that only counted
/// records would report success for a file that cannot be merged.
#[test]
fn a_dry_merge_refuses_a_damaged_pack() {
    let dir = tempfile::tempdir().unwrap();
    merge_tree(dir.path());
    ok(dir.path(), &["backup", "source", "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);

    let bytes = fs::read(dir.path().join("v1.pack")).unwrap();
    let mut damaged = bytes.clone();
    damaged[200] ^= 0xff;
    fs::write(dir.path().join("damaged.pack"), &damaged).unwrap();

    let refused = fails(
        dir.path(),
        &["merge", "damaged.pack", "received", "--dry-run"],
    );
    assert!(
        refused.contains("do not hash to"),
        "a dry merge should refuse damaged bytes by hashing them: {refused}"
    );
    assert!(
        !dir.path().join("received.0000.bin").exists(),
        "and should not have created the set"
    );
}

/// A merge holds the set's claim for its run, like any other write.
///
/// Two merges into one set at once would interleave their commit records, since
/// the store has one publish point per run.
#[test]
fn a_second_merge_is_refused_while_the_lock_is_held() {
    let dir = tempfile::tempdir().unwrap();
    merge_tree(dir.path());
    ok(dir.path(), &["backup", "source", "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);
    ok(dir.path(), &["merge", "v1.pack", "received"]);

    fs::write(dir.path().join("received.lock"), b"").unwrap();
    let refused = fails(dir.path(), &["merge", "v1.pack", "received"]);
    assert!(
        refused.contains("locked by another lepatch process"),
        "a second merge should be refused while the set is claimed: {refused}"
    );
    fs::remove_file(dir.path().join("received.lock")).unwrap();
}

/// A merged version verifies, so it is as sound as a backed-up one.
///
/// The property is about the store rather than the tree: the chunks a merge
/// wrote must hash to what they are addressed by, which is the same check a
/// backup's own store passes.
#[test]
fn a_merged_version_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let source = merge_tree(dir.path());
    ok(dir.path(), &["backup", source.to_str().unwrap(), "origin"]);
    ok(dir.path(), &["pack", "origin", "v1.pack"]);
    ok(dir.path(), &["merge", "v1.pack", "received"]);

    let report = ok(dir.path(), &["verify", "received"]);
    assert!(
        report.contains("all chunks match"),
        "a merged version should verify: {report}"
    );
}
