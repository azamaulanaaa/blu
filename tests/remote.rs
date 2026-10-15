// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! `blu versions` of a serve set: the same listing a local set gives, from a
//! server somewhere else.
//!
//! The claim this file exists to hold is one line long — **a script written
//! against a local set works unchanged against a remote one** — and it is a
//! claim about bytes on stdout, so it is tested as bytes on stdout. Every test
//! here runs the real binary twice against the same set and compares
//! [`Output::stdout`] with `==`, in both listing modes. Not "the same versions",
//! not "the same lines after trimming": the same bytes, or the test fails.
//!
//! What that comparison cannot catch, the rest of the file covers, and the two
//! halves are the failure modes the feature actually has:
//!
//! * **A refusal that is a different refusal.** A set the server does not hold
//!   has to arrive as the server's own sentence, not as a client's paraphrase
//!   and not as an empty listing. `a_set_the_server_does_not_hold_carries_its_own_sentence`
//!   reads the problem document over HTTP and asserts the terminal said exactly
//!   that, so the two ends are compared rather than each checked against a
//!   guess.
//! * **An answer the client does not understand.** `awc` will happily return
//!   `200 OK` and some bytes, and a parser that read permissively would print
//!   nothing and exit 0 — which a script reads as "the set is empty", a claim,
//!   and the wrong one. The [`Stub`] cases drive a server that has changed its
//!   shape and assert the command *fails, loudly, naming the address*.
//!
//! ## Why a subprocess and a real server
//!
//! Same reasoning as `tests/serve.rs`, and the same helpers: a real
//! `blu serve` started as a user starts it, reached over a socket. The
//! byte comparison is only worth anything if both sides of it are the bytes a
//! user gets.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use common::server::{Server, Stub};

/// The address a server printed: the server half of a serve TARGET.
fn remote_of(server: &Server) -> String {
    format!("http://{}", server.address())
}

/// The serve set `docs` on the server at `address`, as one TARGET.
fn serve_target(address: &str) -> String {
    format!("{address}/docs")
}

/// Run the binary and hand back its output.
fn blu(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_blu"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("failed to run the blu binary")
}

/// The stdout of a run that succeeded.
fn ok(dir: &Path, args: &[&str]) -> Vec<u8> {
    let output = blu(dir, args);

    assert!(
        output.status.success(),
        "`blu {}` failed with {:?}\nstdout: {}\nstderr: {}",
        args.join(" "),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    output.stdout
}

/// The stderr of a run that failed, with the run asserted to have exited 1.
fn fails(dir: &Path, args: &[&str]) -> String {
    let output = blu(dir, args);

    assert!(
        !output.status.success(),
        "`blu {}` unexpectedly succeeded\nstdout: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a failure should exit 1, not panic\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    output
        .stderr
        .into_iter()
        .map(|b| b as char)
        .collect::<String>()
}

/// A tree worth backing up: several files, and enough noise in them that a
/// second backup of a changed tree deduplicates against the first rather than
/// replacing it.
fn tree(dir: &Path) -> PathBuf {
    let source = common::source(dir);
    fs::write(source.join("large.bin"), common::noisy(120_000)).unwrap();
    fs::write(source.join("notes.txt"), b"the first state\n").unwrap();
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("nested/small.txt"), b"hello\n").unwrap();
    source
}

/// A served root holding a set named `docs` with three versions in it.
///
/// **Three, and not two.** One version is a listing that cannot be reordered;
/// two is a listing whose order a sort could get right by accident. Three is
/// where "oldest first, from the same index files" is a claim with content.
async fn served_three_versions(dir: &Path) -> (PathBuf, PathBuf) {
    let source = tree(dir);
    let root = dir.join("served");
    fs::create_dir_all(&root).unwrap();
    let set = root.join("docs");

    for edit in 0..3 {
        if edit == 1 {
            // Enough of `large.bin` changed to move chunk boundaries, which is
            // what makes version 2 a delta rather than a copy of version 1.
            fs::write(
                source.join("large.bin"),
                common::untagged_stream(9, 120_000),
            )
            .unwrap();
        }
        if edit == 2 {
            fs::write(source.join("notes.txt"), b"the third state\n").unwrap();
            fs::write(source.join("extra.bin"), common::untagged_stream(3, 40_000)).unwrap();
        }

        common::run_backup(&source, &set).await;
    }

    (root, set)
}

/// The two listings of one set, compared as bytes.
///
/// The shared body of the equality tests. Both modes run here rather than one
/// per test so that "normal mode matches" cannot be true while "numbers-only
/// matches" is quietly untested — the two modes print through the same function
/// with one branch between them, and it is that branch this is holding.
fn assert_both_modes_match_the_local_listing(root: &Path, set: &Path, address: &str) {
    let local_path = set.display().to_string();
    let remote = ok(root, &["versions", local_path.as_str()]);

    assert!(
        !remote.is_empty(),
        "the local listing is empty, so this test would compare two empty listings and prove \
         nothing: the fixture should hold three versions"
    );

    assert_eq!(
        remote,
        ok(root, &["versions", &serve_target(address)]),
        "the remote listing is not the local one, byte for byte"
    );

    let numbers = ok(root, &["versions", local_path.as_str(), "--numbers-only"]);

    assert_eq!(
        numbers,
        ok(
            root,
            &["versions", &serve_target(address), "--numbers-only"]
        ),
        "the remote --numbers-only listing is not the local one, byte for byte"
    );

    // And the mode really is a different listing, not the same bytes twice —
    // otherwise the comparison above would be passing for the wrong reason.
    assert_ne!(remote, numbers);
}

#[tokio::test]
async fn a_remote_listing_is_the_local_listing_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let (root, set) = served_three_versions(dir.path()).await;
    let server = Server::start(&root);

    assert_both_modes_match_the_local_listing(&root, &set, &remote_of(&server));
}

#[tokio::test]
async fn a_trailing_slash_on_the_address_is_the_same_server() {
    // The address a user copies out of a terminal, which is the one the server
    // printed and which has no trailing slash — and the one a config file
    // holds, which does. Both have to work, and they have to be the *same*
    // server: a join that produced `http://host:port//v1/...` would 404.
    let dir = tempfile::tempdir().unwrap();
    let (root, set) = served_three_versions(dir.path()).await;
    let server = Server::start(&root);
    let with_slash = format!("{}/", remote_of(&server));

    assert_both_modes_match_the_local_listing(&root, &set, &with_slash);
}

#[tokio::test]
async fn a_set_name_holding_a_space_is_the_same_set() {
    // **A space sent raw ends the path**, and the server reads what came before
    // it as a different set's name — which for a set called `photos 2024` is a
    // set called `photos`, and the answer to listing a set that does not exist
    // is a 404 rather than a listing of the wrong thing. So the client
    // percent-encodes, and this is what proves it against the server rather
    // than against the encoder's own unit test.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path());
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let set = root.join("photos 2024");

    common::run_backup(&source, &set).await;

    let server = Server::start(&root);
    let address = remote_of(&server);
    let local_path = set.display().to_string();

    assert_eq!(
        ok(dir.path(), &["versions", local_path.as_str()]),
        ok(
            dir.path(),
            &["versions", &format!("{address}/photos%202024")]
        ),
        "a set name holding a space should name the same set on both sides"
    );
}

#[tokio::test]
async fn a_set_the_server_does_not_hold_carries_its_own_sentence() {
    // **The server's words, not a paraphrase of them.** Read the problem
    // document over HTTP and compare: `detail` is the sentence `blu versions`
    // on the server's own disk would have printed, so a remote miss reads like
    // the local one and there is no second vocabulary to learn. Asserting the
    // string contains "no backup set" would pass for any paraphrase of it.
    let dir = tempfile::tempdir().unwrap();
    let (root, _set) = served_three_versions(dir.path()).await;
    let server = Server::start(&root);

    let document = common::http::Request::new("GET", "/v1/sets/nosuch/versions")
        .send(server.address())
        .await;

    assert_eq!(document.status, 404);
    assert!(
        document.is_type("application/problem+json"),
        "a refusal should be a problem document, got {:?}",
        document.header("content-type")
    );

    let said = document
        .field("detail")
        .and_then(|value| value.as_str().map(str::to_string))
        .expect("a problem document carries a detail");

    let stderr = fails(
        &root,
        &["versions", &format!("{}/nosuch", remote_of(&server))],
    );

    assert!(
        stderr.contains(&said),
        "the client's message should be the server's own sentence.\n  server said: {said}\n  \
         client said: {stderr}"
    );
    assert!(
        !stderr.contains("could not reach"),
        "a 404 is an answer, not a connection failure: {stderr}"
    );
}

#[tokio::test]
async fn a_set_the_server_does_not_hold_prints_nothing() {
    // The half of the miss that a `404` and an empty listing share on stdout.
    // Both print nothing, so the exit code and the message are the only things
    // telling them apart — which is exactly why this is separate from the
    // sentence test.
    let dir = tempfile::tempdir().unwrap();
    let (root, _set) = served_three_versions(dir.path()).await;
    let server = Server::start(&root);

    let output = blu(
        &root,
        &["versions", &format!("{}/nosuch", remote_of(&server))],
    );

    assert_eq!(
        output.stdout,
        Vec::<u8>::new(),
        "a 404 prints nothing on stdout: an empty listing and a refusal look identical there"
    );
}

#[tokio::test]
async fn an_unreachable_address_names_the_address_it_tried() {
    // **A connection failure has a cause the user can act on** — the server is
    // not running, the port is wrong, the name does not resolve — and
    // "could not read the response" throws all of it away. So the message
    // carries the URL it tried, which is the one fact a reader cannot get
    // anywhere else.
    let dir = tempfile::tempdir().unwrap();

    // A loopback port that was bound and released, rather than one guessed. The
    // kernel has just confirmed the number is free, so nothing is listening on
    // it and the connection is refused immediately — no DNS, no timeout, and
    // nothing that could make this flaky on a slow machine.
    let closed = closed_port();

    let address = format!("http://{closed}");

    let stderr = fails(dir.path(), &["versions", &serve_target(&address)]);

    assert!(
        stderr.contains(&address),
        "the error should name the address it tried.\n  tried: {address}\n  said: {stderr}"
    );
    assert!(
        stderr.contains("could not reach"),
        "the error should say what failed to happen: {stderr}"
    );
}

/// A loopback address with nothing on it.
///
/// Bound to find a free port and released again. There is an unavoidable window
/// between the release and the connection in which another process could take
/// it; on loopback, for a test binary's own milliseconds, nothing does.
fn closed_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

#[tokio::test]
async fn refuse_bad_address_before_transfer() {
    let dir = tempfile::tempdir().unwrap();

    // HTTPS dials now.
    // A bad host fails as a connection failure.
    // It no longer fails as a refusal.
    let tls = fails(dir.path(), &["versions", "https://example.test/docs"]);
    assert!(
        tls.contains("could not reach"),
        "an https target dials before it fails: {tls}"
    );
    assert!(
        !tls.contains("needs TLS"),
        "an https target no longer names a missing backend: {tls}"
    );

    // An unknown store scheme names the schemes it could use.
    let scheme = fails(dir.path(), &["versions", "gopher://host/docs"]);
    assert!(
        scheme.contains("not a store scheme"),
        "an unknown scheme names the valid schemes: {scheme}"
    );

    // A known network scheme dials now.
    // It fails in the backend, not at the feature gate.
    let network = fails(dir.path(), &["versions", "ftp://host/docs"]);
    assert!(
        !network.contains("--features remote"),
        "a network scheme no longer names a feature: {network}"
    );

    // Anything else without `://` is a local path.
    // A local path that is not a set gives the local miss.
    for target in ["127.0.0.1:8480", "//127.0.0.1:8480", ""] {
        let stderr = fails(dir.path(), &["versions", target]);

        assert!(
            stderr.contains("no backup set"),
            "{target:?} is a local path and gets the local miss: {stderr}"
        );
    }
}

#[tokio::test]
async fn a_path_rather_than_a_name_is_refused_before_anything_is_sent() {
    // A set on a server is named by one path component. `backups/docs` is a path
    // on *this* machine, and sending it would ask the server about a set whose
    // name contains a slash.
    let dir = tempfile::tempdir().unwrap();
    let address = remote_of(&Server::start(dir.path()));

    let stderr = fails(
        dir.path(),
        &["versions", &format!("{address}/backups/docs")],
    );

    assert!(
        stderr.contains("is a path"),
        "the refusal should say which half of the name is wrong: {stderr}"
    );
}

#[tokio::test]
async fn a_response_this_client_cannot_read_fails_loudly() {
    // **The one way a remote listing can quietly lie.** Each of these is a
    // server that has changed what it sends: a renamed field, a changed entry
    // shape, something that is not JSON at all. Read permissively, any of them
    // prints nothing and exits 0, and a script reading that believes the set is
    // empty — which is a claim, and the wrong one. So each has to be a failure
    // whose message names the address, and stdout has to be empty rather than
    // an empty listing.
    let dir = tempfile::tempdir().unwrap();

    for body in [
        // The field is gone.
        "{}",
        // The field is there and is no longer an array.
        r#"{"versions":{}}"#,
        // An entry lost its `snapshot`.
        r#"{"versions":[{"version":1}]}"#,
        // An entry lost its `version`.
        r#"{"versions":[{"snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#,
        // A version is a string now.
        r#"{"versions":[{"version":"1","snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#,
        // A snapshot is not a string.
        r#"{"versions":[{"version":1,"snapshot":10101}]}"#,
        // A snapshot is not a hash.
        r#"{"versions":[{"version":1,"snapshot":"not a hash"}]}"#,
        // A version past what a `u32` can hold.
        r#"{"versions":[{"version":4294967296,"snapshot":"0101010101010101010101010101010101010101010101010101010101010101"}]}"#,
        // Not JSON.
        "not json at all",
        // JSON, but not an object.
        "[]",
    ] {
        let stub = Stub::answering_200(body);
        let address = format!("http://{}", stub.address());

        let output = blu(dir.path(), &["versions", &serve_target(&address)]);

        assert_eq!(
            output.status.code(),
            Some(1),
            "a listing this client cannot read should exit 1, not 0.\n  body: {body}\n  stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(
            output.stdout,
            Vec::<u8>::new(),
            "nothing may be printed for a body this client cannot read, or the empty output \
             reads as an empty set.\n  body: {body}"
        );

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(&address),
            "the failure should name the server whose answer it could not read.\n  body: {body}\n\
             address: {address}\n  stderr: {stderr}"
        );
        assert!(
            stderr.contains("does not understand"),
            "the failure should say the answer was unreadable rather than naming an empty set: \
             {stderr}"
        );
    }
}

#[tokio::test]
async fn an_empty_listing_is_an_empty_list_and_not_a_failure() {
    // The counterpart to the case above, and the reason the parser's strictness
    // is not strictness for its own sake. `{"versions":[]}` is a thing a server
    // can say, it means what it says, and the command prints nothing and exits
    // 0. A parser that refused this would be refusing a correct answer.
    let dir = tempfile::tempdir().unwrap();
    let stub = Stub::answering_200(r#"{"versions":[]}"#);
    let address = format!("http://{}", stub.address());

    let output = blu(dir.path(), &["versions", &serve_target(&address)]);

    assert!(
        output.status.success(),
        "an empty listing is an answer, not a failure\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        output.stdout,
        Vec::<u8>::new(),
        "an empty listing prints no lines, and prints them to no other stream either"
    );
}

#[tokio::test]
async fn a_listing_of_a_set_that_exists_holds_every_version_the_server_names() {
    // The equality tests compare against a local listing, which is only
    // meaningful if the local listing is itself the whole set. So this asserts
    // the fixture is what it claims to be: three versions, each naming the
    // snapshot the commit record holds, and no fourth.
    //
    // Otherwise `a_remote_listing_is_the_local_listing_byte_for_byte` would
    // still pass if the server were omitting a version *and* the local command
    // were omitting the same one — which is a real possibility, both go through
    // the same `snapshot_via_index` skip.
    let dir = tempfile::tempdir().unwrap();
    let (root, set) = served_three_versions(dir.path()).await;

    let storage = blulib::storage::BlobStore::open_for_reading(&set)
        .await
        .unwrap();

    let versions = storage.versions().unwrap();
    assert_eq!(versions.len(), 3, "the fixture should hold three versions");

    let listing_path = set.display().to_string();
    let listing = String::from_utf8(ok(&root, &["versions", listing_path.as_str()])).unwrap();
    let mut named = 0;
    for version in versions {
        let snapshot = storage.snapshot(version).unwrap();
        let head = format!("{version}  {snapshot}");

        assert!(
            listing.lines().any(|line| line.starts_with(&head)),
            "the local listing should name version {version} and its snapshot:\n{listing}"
        );
        named += 1;
    }

    assert_eq!(named, 3);
    assert_eq!(
        listing.lines().count(),
        3,
        "and should name nothing else:\n{listing}"
    );
}

/// One `TARGET` lists what the local path lists, and the removed flags fail.
///
/// A serve target lists the same versions as the served path, byte for byte —
/// the claim this whole file exists to hold. `--remote` and `--set` named the
/// server and the set as two arguments, and one `TARGET` names both, so either
/// flag is a usage error now: clap exits 2 before any request is made, and a
/// refused listing prints nothing on stdout, as any other refusal does.
#[tokio::test]
async fn a_serve_target_lists_what_the_local_path_lists_and_old_flags_fail() {
    let dir = tempfile::tempdir().unwrap();
    let (root, set) = served_three_versions(dir.path()).await;
    let server = Server::start(&root);
    let address = remote_of(&server);

    let listed = ok(&root, &["versions", &serve_target(&address)]);

    assert!(
        !listed.is_empty(),
        "the fixture should hold three versions, so this compares two non-empty listings"
    );
    assert_eq!(
        listed,
        ok(&root, &["versions", set.display().to_string().as_str()]),
        "the serve target should list what the local path lists, byte for byte"
    );

    for args in [
        vec!["versions", "docs", "--remote", address.as_str()],
        vec!["versions", "--remote", address.as_str(), "--set", "docs"],
    ] {
        let output = blu(&root, &args);
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
        assert_eq!(
            output.stdout,
            Vec::<u8>::new(),
            "a refused listing prints nothing on stdout, as any other refusal does"
        );
    }
}
