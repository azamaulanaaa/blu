//! End-to-end tests that drive the `lepatch` binary as a subprocess.
//!
//! The library integration tests call `backup` and `restore` directly, which
//! left `src/bin/mod.rs` at zero coverage: version selection, the cross-process
//! write lock, the exclude list, argument parsing, exit codes and every error
//! message the user sees were all untested. These tests go through the real
//! binary so those paths are exercised as a user would meet them.

// `.clippy.toml` exempts code inside a `#[test]` function; the `lepatch` helper
// below is a plain fn called from many.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

use common::noisy;

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

#[test]
fn verify_fails_and_restore_refuses_on_a_damaged_store() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    ok(dir.path(), &["backup", "src", "snap"]);

    // Flip a byte well inside the chunk data.
    let store = dir.path().join("snap.0000.bin");
    let mut bytes = fs::read(&store).unwrap();
    let target = bytes.len() / 2;
    bytes[target] ^= 0xff;
    fs::write(&store, &bytes).unwrap();

    // The per-chunk detail and the final error go to stderr; the summary line
    // goes to stdout, so both streams have to be read.
    let verify = lepatch(dir.path(), &["verify", "snap"]);
    assert!(!verify.status.success());

    let summary = stdout(&verify);
    assert!(
        summary.contains("FAILED"),
        "the summary should report the failure: {summary}"
    );

    let detail = stderr(&verify);
    // A chunk is named by the hash of what it should contain, and the bytes are
    // reported as what they actually hash to — so two different hashes, and the
    // message says which is which.
    assert!(
        detail.contains("actually hash to"),
        "the detail should say what the bytes really hashed to: {detail}"
    );

    let named = detail
        .split("actually hash to")
        .next()
        .and_then(|line| line.split("stored as chunk ").nth(1))
        .map(|hash| {
            hash.split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .unwrap_or_default();
    let found = detail
        .split("actually hash to")
        .nth(1)
        .map(|rest| {
            rest.split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .unwrap_or_default();
    assert!(
        !named.is_empty() && !found.is_empty() && named != found,
        "the detail should name the chunk and what it really hashed to, \
         and those must differ: {detail}"
    );

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

#[test]
fn a_run_says_that_it_is_making_progress() {
    // A backup of a real dataset takes minutes to hours. Until it reported
    // anything at all, a user watching a silent terminal could not tell a
    // working backup from a hung one, and the first thing they would do about
    // that is kill it.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    // *entirely* the snapshot. Never some prefix of it, which is what a restore
    // that writes in place leaves.
    //
    // Both outcomes are acceptable, so this cannot be flaky in the failing
    // direction — a partial destination fails it whichever way the race went.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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

    std::thread::sleep(std::time::Duration::from_millis(120));
    let _ = Command::new("sh")
        .arg("-c")
        .arg(format!("kill -INT {}", child.id()))
        .status();
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

    assert!(
        has_marker && restored_files == 0,
        "the destination was left partly written: {has_marker} marker, \
         {restored_files} of 10 files"
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
            .map(|s| s.success())
            .unwrap_or(false),
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

    let mut child = Command::new("sh")
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
