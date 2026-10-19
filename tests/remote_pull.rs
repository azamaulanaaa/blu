//! `blu restore` of a serve set: a tree that went up over a socket comes
//! back down one, byte for byte.
//!
//! The push half of a remote backup is [`tests/client_rest.rs`], and the wire
//! itself is [`tests/serve.rs`]. What this file holds is the direction nobody
//! had driven end to end before it: **the server packs a version, this client
//! assembles a pack out of the ranges it is missing, and the tree that lands in
//! the destination is the tree that was backed up.** Both ends of it go through
//! the binary as a user runs it, over a socket to a real `blu serve` — so
//! what is asserted here is what a user gets, not what a function returns.
//!
//! ## What each test is for
//!
//! * **`a_tree_backed_up_remotely_restores_remotely_byte_for_byte`** is the whole
//!   point of the branch. It walks both trees and compares **every path and
//!   every byte** — not a file count, and not the snapshot hash. A count passes
//!   when a restore writes three truncated files instead of three whole ones;
//!   the snapshot hash is what the *server* stores, so asserting on it would be
//!   asserting that the push worked while the pull did nothing at all. The only
//!   thing that distinguishes "the tree came back" from "a tree came back" is
//!   the tree itself.
//! * **`the_second_pull_fetches_fewer_bytes`** is what holds the `have` list to
//!   its claim. A client that always sends an empty `have` is always *correct*
//!   and always wasteful, so nothing about a correct restore can tell the two
//!   apart — only the byte count can, and the test reads it off the run's own
//!   report line rather than off a counter it computed itself.
//! * **`a_pull_restores_the_version_it_was_asked_for`** is what stops "the
//!   server returned something" from passing as "the server returned that".
//! * **`a_pull_of_a_set_the_server_does_not_hold_is_a_failure_that_says_which`**
//!   and **`a_pull_from_an_address_nothing_is_listening_on_is_a_failure_that_says_which`**
//!   are the two refusals a user meets first, and both are also *hang* tests.
//! * **the removed flags** stay removed: `--remote` and `--set` named the
//!   server and the set as two arguments, and one `TARGET` names both, so
//!   either flag is a usage error that writes nothing.
//!
//! ## Why every run here is bounded by a deadline
//!
//! A pull asks a server to pack a version and then **polls a job every 200 ms
//! until it reaches a terminal state**. There is no overall deadline on that
//! loop: what stops it is the per-request timeout in `client::Remote` firing on
//! the poll that cannot be answered. So a failure case that never got as far as
//! a request — an address nothing is listening on, or a server that accepts and
//! then says nothing — is exactly the case where a test would hang rather than
//! fail, and a hung test is a suite that never finishes.
//!
//! Hence [`blu_within`]: every run in this file is killed at a deadline and
//! **the test fails** if the binary is still going. A pull that cannot finish is
//! a finding, not a slow test.
//!
//! ## The port
//!
//! Never a fixed one. [`Server`] binds `127.0.0.1:0` and prints the address it
//! got, so nothing here can reach a server left running by an earlier build —
//! which matters more than it sounds: a stale `blu serve` answers a large
//! range request with a bare `413` and looks exactly like a bug in the client.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

use common::{Entry, server::Server};

/// How long a run that is doing real work may take before it is killed.
const RUN: Duration = Duration::from_secs(180);

/// How long a run that is *supposed* to fail may take before it is killed.
///
/// Half of [`RUN`], because a failure case has nothing to fetch: if it has not
/// finished by now it is not going to, and a test that waits three minutes to
/// learn that a refused connection is slow is a test nobody runs.
const REFUSAL: Duration = Duration::from_secs(60);

/// Names the log files, one pair per run, inside the test's own temp dir.
static RUNS: AtomicU64 = AtomicU64::new(0);

/// How many pulls
/// [`every_pull_leaves_the_whole_file_in_place_before_it_is_opened`] has going
/// at once, and how many each of its threads makes.
///
/// **A count, not a timeout.** The race that test holds is between two threads
/// in one process, so it is the number of runs on the machine at once that
/// decides whether it is caught at all — see that test.
const CONCURRENT_PULLS: u32 = 8;

/// Pulls each of [`CONCURRENT_PULLS`] makes: thirty-two in all.
const PULLS_EACH: u32 = 4;

/// The address a server printed: the server half of a serve TARGET.
fn remote_of(server: &Server) -> String {
    format!("http://{}", server.address())
}

/// The serve set `docs` on the server at `address`, as one TARGET.
fn serve_target(address: &str) -> String {
    format!("{address}/docs")
}

/// Run the binary in `dir`, and hand back what it produced.
///
/// [`RUN`] is the deadline, and the run is `--quiet`: stdout carries the report
/// a script reads, and `error:` lines reach stderr whatever the filter is, so
/// nothing asserted on here is something a log line was saying instead.
fn blu(dir: &Path, args: &[&str]) -> Output {
    blu_within(dir, args, RUN)
}

/// Run the binary in `dir`, and kill it if it has not finished in `limit`.
///
/// ## Why files rather than pipes
///
/// The obvious shape is `Command::output`, which drains both pipes on its own
/// threads and so cannot deadlock on a full buffer — but it also cannot be
/// interrupted, because it is blocked inside `wait`. Writing stdout and stderr
/// to files under the test's own directory takes that problem away entirely: the
/// child never blocks on a reader, and the parent is free to notice a deadline.
/// The files sit beside the tree being compared rather than inside it, so they
/// are not part of any comparison either.
///
/// # Panics
///
/// Panics if the binary cannot be started, if a log file cannot be written or
/// read back, or **if the run is still going at `limit`** — in which case it is
/// killed first, so a hung run fails the test instead of holding the suite.
fn blu_within(dir: &Path, args: &[&str], limit: Duration) -> Output {
    let run = RUNS.fetch_add(1, Ordering::Relaxed);
    let out = dir.join(format!("run-{run}.out"));
    let err = dir.join(format!("run-{run}.err"));

    let mut child = Command::new(env!("CARGO_BIN_EXE_blu"))
        .current_dir(dir)
        .arg("--quiet")
        .args(args)
        .stdout(Stdio::from(File::create(&out).unwrap()))
        .stderr(Stdio::from(File::create(&err).unwrap()))
        .spawn()
        .expect("failed to run the blu binary");

    let deadline = Instant::now() + limit;
    let status = loop {
        match child.try_wait().expect("the child should still be ours") {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "`blu {}` was still running {limit:?} later and was killed; a pull that \
                     does not finish is a hang, not a slow run",
                    args.join(" ")
                );
            }
            None => thread::sleep(Duration::from_millis(20)),
        }
    };

    Output {
        status,
        stdout: fs::read(&out).unwrap(),
        stderr: fs::read(&err).unwrap(),
    }
}

/// The stdout of a run that succeeded.
fn ok(dir: &Path, args: &[&str]) -> String {
    let output = blu(dir, args);

    assert!(
        output.status.success(),
        "`blu {}` failed with {:?}\nstdout: {}\nstderr: {}",
        args.join(" "),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The stderr of a run that failed, with the run asserted to have exited 1.
///
/// Exit 1 rather than a panic: a refusal is a thing the tool reports, and a test
/// that let a panic through would pass a run that crashed on the way to saying
/// nothing useful.
fn fails(dir: &Path, args: &[&str], limit: Duration) -> String {
    let output = blu_within(dir, args, limit);

    assert!(
        !output.status.success(),
        "`blu {}` unexpectedly succeeded\nstdout: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a refusal should exit 1, not panic\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The bytes a pull's own report says it fetched, and how long the pack is.
///
/// Read off the line the run printed rather than off anything the test counted,
/// so what is asserted is what a script reading this command's output would
/// read. The line is the one that names the version it restored:
///
/// ```text
/// version 1 of docs on http://127.0.0.1:39001: fetched 121072 of 121072 bytes in 1 range(s)
/// ```
///
/// The applier's own report below it starts with `fetched` too and is a
/// different sentence about a different quantity — bytes written into the tree,
/// not bytes moved over the socket — so the leading `version` is what tells the
/// two apart.
fn fetched(stdout: &str) -> (u64, u64) {
    let line = stdout
        .lines()
        .find(|line| line.starts_with("version ") && line.contains(": fetched "))
        .unwrap_or_else(|| {
            panic!("no line saying what this pull fetched:\n{stdout}");
        });

    let mut numbers = line
        .split_once(": fetched ")
        .expect("the line names what it fetched")
        .1
        .split_whitespace();
    let fetched = numbers.next().expect("a byte count").parse().unwrap();
    assert_eq!(numbers.next(), Some("of"), "of what: {line}");
    let length = numbers.next().expect("a length").parse().unwrap();

    (fetched, length)
}

/// A tree worth backing up, and worth comparing afterwards.
///
/// **A shape a count cannot stand in for.** An empty file and an empty directory
/// are both "zero bytes", so a restore that wrote neither is invisible to
/// anything but a walk; a symlink and a hard link are both "a path that exists",
/// and `diff -r` follows one and ignores the other. So the fixture holds all of
/// them, and the comparison is [`common::describe`] rather than a byte count.
fn tree(dir: &Path) -> PathBuf {
    let source = dir.join("src");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::create_dir_all(source.join("empty-dir")).unwrap();
    fs::write(source.join("large.bin"), common::noisy(120_000)).unwrap();
    fs::write(source.join("notes.txt"), b"the first state\n").unwrap();
    fs::write(source.join("nested/small.txt"), b"hello\n").unwrap();
    fs::write(source.join("empty-file"), b"").unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("large.bin", source.join("link")).unwrap();
        fs::hard_link(source.join("large.bin"), source.join("hard")).unwrap();
    }
    source
}

/// A served root, empty, under `dir`.
fn served_root(dir: &Path) -> PathBuf {
    let root = dir.join("served");
    fs::create_dir_all(&root).unwrap();
    root
}

/// Push the tree at `src` to the serve set `docs` at `address`.
///
/// **Through one TARGET, not around it.** The half of this branch that goes
/// up is only worth testing if it goes up the way a user sends it: the tree
/// backed up straight into the serve set, packed there, the pack uploaded,
/// the merge run on the server. A fixture that built the server's set with
/// the library would leave the push untested while the pull looked fine.
fn push(dir: &Path, src: &str, address: &str) -> String {
    ok(dir, &["backup", src, &serve_target(address)])
}

/// Compare two trees the only way that can fail: every path, every byte.
///
/// The same walk and the same verdict as `common::assert_same_tree`, taking two
/// described trees rather than two paths, because one of the tests below has to
/// compare against a version that was overwritten before the pull ran.
fn assert_identical(left: &BTreeMap<PathBuf, Entry>, right: &BTreeMap<PathBuf, Entry>) {
    let mut problems = Vec::new();

    for (path, expected) in left {
        match right.get(path) {
            Some(actual) if actual == expected => {}
            Some(actual) => problems.push(format!(
                "{}: expected {expected:?}, got {actual:?}",
                path.display()
            )),
            None => problems.push(format!(
                "{}: missing from the restored tree ({expected:?})",
                path.display()
            )),
        }
    }

    for path in right.keys() {
        if !left.contains_key(path) {
            problems.push(format!(
                "{}: present in the restored tree and in no version",
                path.display()
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "the tree that came back over the socket is not the tree that went up:\n  {}",
        problems.join("\n  ")
    );
}

#[test]
fn a_tree_backed_up_remotely_restores_remotely_byte_for_byte() {
    // **The claim this branch exists to make.** A tree goes up through
    // `backup --remote`, comes down through `restore --remote`, and what is on
    // disk afterwards is the same tree — every path, every byte, every kind of
    // entry — rather than something of the same shape.
    //
    // The comparison is the whole test. Not the file count, which a restore of
    // three truncated files passes; not the snapshot hash, which is what the
    // *server* stores and would be identical whether this pull wrote the
    // destination or never touched it.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);

    // Into a directory of its own, rather than one this test made earlier: a
    // destination holding anything at all is a destination with no manifest, so
    // this is the case where the whole pack has to come down.
    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");
    let stdout = ok(
        dir.path(),
        &["restore", out.to_str().unwrap(), &serve_target(&address)],
    );

    // The tree that went up is worth comparing: an empty fixture would make the
    // equality below true without pulling a byte.
    let before = common::describe(&source);
    assert!(
        before.len() >= 7,
        "the fixture should hold a nested file, an empty file, an empty directory and a large \
         binary; it holds {} entries",
        before.len()
    );

    // And the run says it moved the whole pack, because it had to.
    let (fetched, length) = fetched(&stdout);
    assert_eq!(
        fetched, length,
        "a destination holding nothing needs every byte of the pack:\n{stdout}"
    );

    assert_identical(&before, &common::describe(&out));
}

/// Every pull assembles a file and then reads it back through a different
/// descriptor, and the bytes have to be in that file by then.
///
/// **The defect this holds.** A write on the handle a pull assembles into is a
/// copy into that handle's own buffer; the `write(2)` behind it runs on a
/// blocking task, and it has not necessarily run when the copy is acknowledged.
/// A pull that dropped the handle at that point handed `Pack::open` a file whose
/// trailer was still unwritten — a hole at the end of the file, read through a
/// fresh descriptor that saw zeroes where the magic should be, reported as
/// `it does not end with a pack's magic, so it is truncated or is not one`. The
/// pack was never at fault, and neither was the server.
///
/// **Why these pulls run at the same time.** The window between the last write
/// being acknowledged and the blocking task issuing it is a scheduling race
/// between two threads in one process, and it is won by the main thread far more
/// often when nothing else is running: sixteen pulls of this fixture, one after
/// another, caught the defect in none of them. With [`CONCURRENT_PULLS`] runs on
/// the machine at once it is one pull in five. So the count here is chosen for
/// the miss probability rather than for the clock — thirty-two pulls at one in
/// five is a miss in about one run in ten thousand — and every one of them has
/// to restore the whole tree, or the test fails.
#[test]
fn every_pull_leaves_the_whole_file_in_place_before_it_is_opened() {
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);

    let before = common::describe(&source);
    assert!(
        before.len() >= 7,
        "the fixture should hold a nested file, an empty file, an empty directory and a large \
         binary; it holds {} entries",
        before.len()
    );

    let pulls: Vec<_> = (0..CONCURRENT_PULLS)
        .map(|pull| {
            let source = source.clone();
            let address = address.clone();
            let dir = dir.path().to_path_buf();

            thread::spawn(move || {
                // Described per thread, because the walk is over the tree that
                // went up and every thread compares against that and not
                // against what another thread restored.
                let before = common::describe(&source);

                // **A destination of its own each time.** A second pull into a
                // destination that already holds the version reads its manifest
                // and fetches only the pack's fixed regions, which is a
                // different write pattern from the one this is about: one
                // range, the whole pack.
                for round in 0..PULLS_EACH {
                    let destination = tempfile::tempdir().unwrap();
                    let out = destination.path().join("out");
                    let stdout = ok(
                        &dir,
                        &["restore", out.to_str().unwrap(), &serve_target(&address)],
                    );

                    let (fetched, length) = fetched(&stdout);
                    assert_eq!(
                        fetched, length,
                        "pull {pull} round {round} did not fetch the whole pack:\n{stdout}"
                    );
                    assert_identical(&before, &common::describe(&out));
                }
            })
        })
        .collect();

    // A failure inside a pull is a failure of this test, and `join` is what says
    // so: a panicked thread nobody joined would not fail anything.
    for pull in pulls {
        pull.join()
            .expect("every pull should restore the whole tree");
    }
}

#[test]
fn the_second_pull_fetches_fewer_bytes() {
    // **What proves the `have` list is real.** An empty `have` is always correct
    // — every pack opens, every tree restores — so a test that only watched the
    // destination could not tell a delta pull from a full one. Only the byte
    // count can, and it has to *drop*, not merely be small: a client that sent
    // the whole snapshot as `have` would fetch nothing at all and then restore
    // zeroes over a destination it had just filled in.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);

    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");
    let restore = |dir: &Path| {
        ok(
            dir,
            &["restore", out.to_str().unwrap(), &serve_target(&address)],
        )
    };

    let first_report = restore(dir.path());
    let (first, length) = fetched(&first_report);
    assert_eq!(
        first, length,
        "the first pull has nothing to save, so it takes the whole pack:\n{first_report}"
    );

    // **The same destination**, which is the whole point: the run reads the
    // record its own last apply left beside the destination, and every file is
    // still where that record left it.
    let second_report = restore(dir.path());
    let (second, second_length) = fetched(&second_report);

    assert_eq!(
        length, second_length,
        "both pulls were of the same pack, so they are measured against the same length"
    );
    assert!(
        second < first,
        "a destination already holding this version should not be sent it again: the second \
         pull fetched {second} bytes against the first pull's {first}, of a {length}-byte pack\n\
         first:\n{first_report}\nsecond:\n{second_report}"
    );

    // Not a free lunch: the fixed regions still come down, because a plan is
    // those plus what the client lacks. A second pull of zero bytes would mean
    // the run had stopped trusting the file it assembles.
    assert!(
        second > 0,
        "the pack's header, snapshot, index and trailer are fetched however much the destination \
         holds:\n{second_report}"
    );

    // And the tree is still exactly what it was, so the saving was not made by
    // writing less of it.
    assert_identical(&common::describe(&source), &common::describe(&out));
}

#[test]
fn a_pull_restores_the_version_it_was_asked_for() {
    // **The version is an argument, and it has to be the one that comes back.**
    // A server that answered every request with its tip would restore a tree
    // that looked plausible and was the wrong one, and nothing in the flow
    // checks it — so the test pushes twice and asks for the first.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);
    let first_tree = common::describe(&source);

    // A second version that shares most of its chunks with the first, so the two
    // trees are genuinely different rather than trivially so.
    fs::write(source.join("notes.txt"), b"the second state\n").unwrap();
    fs::write(source.join("extra.bin"), common::untagged_stream(7, 40_000)).unwrap();
    fs::remove_file(source.join("nested/small.txt")).unwrap();
    push(dir.path(), "src", &address);
    let second_tree = common::describe(&source);

    assert_ne!(
        first_tree, second_tree,
        "the fixture's two versions have to differ, or this test proves nothing"
    );

    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");
    let stdout = ok(
        dir.path(),
        &[
            "restore",
            out.to_str().unwrap(),
            &serve_target(&address),
            "--version",
            "1",
        ],
    );

    assert!(
        stdout.contains("version 1 of docs"),
        "the report names the version it restored:\n{stdout}"
    );

    let restored = common::describe(&out);
    assert_identical(&first_tree, &restored);
    assert_ne!(
        second_tree, restored,
        "the newer version came back instead of the one that was asked for"
    );
}

#[test]
fn a_pull_of_a_set_the_server_does_not_hold_is_a_failure_that_says_which() {
    // **The first refusal a user meets**, and the one where a hang is most
    // likely: it is the server's own `404`, carried out of its problem
    // document. So the message names the set that is not there, and the
    // destination is not created — a pull that fails before it fetches anything
    // has nothing to clean up and must not leave a directory behind.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");

    let stderr = fails(
        dir.path(),
        &[
            "restore",
            out.to_str().unwrap(),
            &format!("{address}/nosuch"),
        ],
        REFUSAL,
    );

    assert!(
        stderr.contains("nosuch"),
        "the refusal names the set it could not find: {stderr}"
    );
    assert!(
        !out.exists(),
        "a refused pull leaves the destination as it was, and a directory is not what it was"
    );
}

#[test]
fn a_pull_from_an_address_nothing_is_listening_on_is_a_failure_that_says_which() {
    // **A server that is not running**, which is the failure a user hits first
    // in practice, and the one where the client has no sentence of the server's
    // to carry: it never spoke. So the message names the address it tried, which
    // is the one thing the user can act on — and it must arrive at all, so the
    // run is bounded by [`REFUSAL`] and fails rather than hanging if it does not.
    let dir = tempfile::tempdir().unwrap();

    // **A port that was bound and released**, rather than a number chosen here:
    // this cannot be a port something else is already listening on, so the only
    // thing the run can meet is a refused connection.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);

    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");

    let stderr = fails(
        dir.path(),
        &["restore", out.to_str().unwrap(), &serve_target(&address)],
        REFUSAL,
    );

    assert!(
        stderr.contains(&address),
        "the refusal names the address it tried: {stderr}"
    );
    assert!(!out.exists(), "nothing was fetched, so nothing was written");
}

/// One `TARGET` names the set, however it is spelled on disk or on the wire.
///
/// Two pulls of the same serve target restore the same tree as the tree that
/// went up — every path and every byte, the same walk every other test here
/// uses. Two destinations of their own, because a second pull into a
/// destination that already holds the version reads the manifest and fetches
/// only the pack's fixed regions, which would make the two runs differ for a
/// reason that has nothing to do with the address.
#[test]
fn a_serve_target_restores_the_tree_that_went_up_twice_over() {
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);

    let first_dir = tempfile::tempdir().unwrap();
    let first = first_dir.path().join("out");
    ok(
        dir.path(),
        &["restore", first.to_str().unwrap(), &serve_target(&address)],
    );

    let second_dir = tempfile::tempdir().unwrap();
    let second = second_dir.path().join("out");
    ok(
        dir.path(),
        &["restore", second.to_str().unwrap(), &serve_target(&address)],
    );

    let before = common::describe(&source);
    assert_identical(&before, &common::describe(&first));
    assert_identical(&before, &common::describe(&second));
    assert_identical(&common::describe(&first), &common::describe(&second));
}

/// The removed flags stay removed.
///
/// `--remote` and `--set` named the server and the set as two arguments. One
/// `TARGET` names both, so either flag is a usage error now — exit 2 from
/// clap, before any request is made — and a refused pull writes nothing.
///
/// **Exit 2, not 1**: this is clap refusing an argument before any of the
/// command runs, which is what a script distinguishes from a run that failed
/// while working.
#[test]
fn the_removed_remote_and_set_flags_are_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);

    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");
    let target = serve_target(&address);

    for args in [
        vec![
            "restore",
            out.to_str().unwrap(),
            target.as_str(),
            "--set",
            "other",
        ],
        vec![
            "restore",
            out.to_str().unwrap(),
            "docs",
            "--remote",
            &address,
        ],
    ] {
        let output = blu(dir.path(), &args);
        let said = String::from_utf8_lossy(&output.stderr).into_owned();

        assert!(
            !output.status.success(),
            "a removed flag should be refused\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(
            output.status.code(),
            Some(2),
            "an argument clap refuses is a usage error\nstderr: {said}"
        );
        assert!(
            said.contains("unexpected argument"),
            "the refusal names the mistake: {said}"
        );
    }
    assert!(!out.exists(), "nothing was fetched, so nothing was written");
}

/// A remote restore with `--files` fetches the selection and writes the selection.
///
/// The pull skips the chunks no selected file needs, so the byte count on the
/// report line sits below the whole pack — and the destination holds the named
/// file at the version's bytes and nothing else. A fetch that brought
/// everything would still restore correctly, so only the count and the tree
/// together prove the holes were never touched.
#[test]
fn a_remote_restore_with_files_fetches_and_writes_only_the_selection() {
    let dir = tempfile::tempdir().unwrap();
    tree(dir.path());
    let server = Server::start(&served_root(dir.path()));
    let address = remote_of(&server);

    push(dir.path(), "src", &address);

    let destination = tempfile::tempdir().unwrap();
    let out = destination.path().join("out");
    let stdout = ok(
        dir.path(),
        &[
            "restore",
            out.to_str().unwrap(),
            &serve_target(&address),
            "--files",
            "notes.txt",
        ],
    );

    let (fetched, length) = fetched(&stdout);
    assert!(
        fetched < length,
        "a selection must cost fewer bytes than the whole pack:\n{stdout}"
    );

    assert_eq!(
        fs::read(out.join("notes.txt")).unwrap(),
        b"the first state\n"
    );
    assert!(
        !out.join("large.bin").exists(),
        "a file outside the selection must not be written"
    );
}
