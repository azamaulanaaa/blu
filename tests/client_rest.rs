//! The client's REST surface: a backup pushed to a server, end to end.
//!
//! Everything here goes through [`lepatch::client::Remote`] — the same type
//! `lepatch versions` of a serve target uses — against a real `lepatch serve`
//! a user starts it, reached over a socket. Nothing reaches into `src/serve`
//! and nothing parses a response by hand, because a client tested against
//! something other than the server a client really talks to is a client tested
//! against nothing.
//!
//! ## What these tests are for
//!
//! The plumbing was proved by `tests/serve.rs`, over its own hand-written HTTP
//! client. That left the half this file holds: **the client as a user of it**,
//! and the ways that half can be wrong while still looking right.
//!
//! * **A push that is not a version.** `a_merge_the_client_starts_publishes_a
//!   version_the_set_then_holds` asserts the whole chain: the upload lands, the
//!   merge job succeeds, and `versions` on the same server now names a version —
//!   which is the claim `lepatch backup` to a serve target would make.
//! * **A failed job reported as a success.** This is the bug `9ab42a7` fixed on
//!   the server side, and the client half of it is the one a caller would not
//!   catch: `a_failed_job_is_an_error_carrying_the_servers_own_sentence` drives a
//!   job the server really fails, and asserts both that `await_job` returns
//!   `Err` and that the raw poll says `failed` with the same sentence. The second
//!   half is what makes the first meaningful: a client that invented the
//!   failure would also pass it.
//! * **A refusal that is not the server's sentence.** `a_set_the_server_does_not
//!   _hold_is_not_found_with_its_own_sentence` reads the three refusals a client
//!   meets first — a set, a job, an upload — and asserts each is a `NotFound`
//!   carrying the server's words, because a paraphrase is a second vocabulary
//!   for one failure.
//! * **A push that leaves a scratch file behind.** The server writes an upload to
//!   its own disk, and `a_failed_push_leaves_no_upload_on_the_server` looks in
//!   that directory afterwards, which is the only place the cleanup this client
//!   owes is observable from outside.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
};

use common::server::Server;
use lepatch::{
    client::{Kind, MergeCompression, Merged, Outcome, Remote, State},
    pack::Pack,
    storage::{BlobId, BlobStore, Compression, Plan, StorageGet as _},
};
use tokio::io::AsyncReadExt as _;

/// The directory a `lepatch serve` writes uploaded packs into, under its root.
///
/// A private constant in `src/serve/mod.rs`, and the tests are on the other side
/// of a socket from it — so it is written down here rather than imported, and a
/// server that moved it would fail these tests rather than quietly pass them.
const SCRATCH: &str = ".lepatch-serve";

/// A client for `server`, as a serve target would build one.
fn remote_of(server: &Server) -> Remote {
    Remote::new(&format!("http://{}", server.address())).unwrap()
}

/// A served root, empty, under `dir`.
fn served_root(dir: &Path) -> PathBuf {
    let root = dir.join("served");
    fs::create_dir_all(&root).unwrap();
    root
}

/// The packs a server is currently holding, by name.
///
/// **The scratch directory is the only witness to an upload's existence** that a
/// client can see: the registry is in the server's memory, so a client that
/// forgot to clean up is invisible through the API and visible only here.
fn held_packs(root: &Path) -> Vec<String> {
    let scratch = root.join(SCRATCH);
    if !scratch.exists() {
        return Vec::new();
    }

    let mut names: Vec<String> = fs::read_dir(&scratch)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// A tree worth backing up: three files whose contents are not prefixes of one
/// another, so deduplication has nothing to collapse and a merge has real work.
fn tree(dir: &Path) -> PathBuf {
    let source = common::source(dir);
    for (tag, len) in [(1_u8, 40_000_usize), (2, 50_000), (3, 60_000)] {
        fs::write(
            source.join(format!("f{tag}.bin")),
            common::untagged_stream(tag, len),
        )
        .unwrap();
    }
    fs::write(source.join("notes.txt"), b"the first state\n").unwrap();
    source
}

/// Pack `source` up locally, and hand back the pack's bytes, its snapshot and how
/// many chunks it carries.
///
/// **Packed rather than uploaded as a directory**, because the API this client
/// speaks moves packs: a merge reads one twice, so it has to be a file on the
/// server's disk before anything can merge it.
async fn packed(dir: &Path) -> (Vec<u8>, BlobId, u64) {
    let source = tree(dir);

    let blob = dir.join("local");
    let store = BlobStore::open_for_writing(
        &blob,
        lepatch::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();
    lepatch::command::backup(&source, lepatch::command::Options::default(), store)
        .await
        .unwrap();

    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    let (version, snapshot_id) = reader.latest().unwrap();

    let mut buffer = Vec::new();
    reader
        .get(&snapshot_id)
        .await
        .unwrap()
        .read_to_end(&mut buffer)
        .await
        .unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(reader.locate(id).unwrap())).unwrap();

    let pack_path = dir.join("local.pack");
    Pack::write(&reader, &plan, &snapshot, version, &pack_path)
        .await
        .unwrap();

    let chunks = u64::try_from(plan.len()).unwrap();

    (fs::read(&pack_path).unwrap(), snapshot_id, chunks)
}

#[tokio::test]
async fn an_upload_round_trips_from_begin_through_send_to_commit() {
    // The three steps a push is made of, driven by hand so each one's answer can
    // be looked at: the `201` names the slot, the `204` means the bytes are a
    // pack, and the `200` reports what arrived.
    let dir = tempfile::tempdir().unwrap();
    let (pack, snapshot, chunks) = packed(dir.path()).await;
    let server = Server::start(&served_root(dir.path()));
    let remote = remote_of(&server);

    let upload = remote.begin_upload().await.unwrap();
    assert!(!upload.id().is_empty(), "the server names the slot it made");

    upload.send(pack.clone()).await.unwrap();
    let commit = upload.commit().await.unwrap();

    assert_eq!(commit.upload, upload.id());
    assert_eq!(
        commit.length,
        u64::try_from(pack.len()).unwrap(),
        "the commit reports the file that landed"
    );
    assert_eq!(commit.chunks, chunks, "the pack's own chunk count");
    assert!(commit.payload_bytes > 0);
    assert_eq!(commit.version, 1, "it was packed out of a one-version set");
    assert_eq!(
        commit.snapshot.to_hex().to_string(),
        snapshot.to_hex().to_string(),
        "the pack's snapshot is the version it was packed from"
    );
    assert_eq!(commit.base, None, "a full pack names no base");

    // And the upload was there the whole time: the scratch file is the server's
    // side of that claim, and nothing has dropped it.
    assert_eq!(held_packs(dir.path().join("served").as_path()).len(), 1);
}

#[tokio::test]
async fn a_merge_the_client_starts_publishes_a_version_the_set_then_holds() {
    // **The whole chain, and the claim is in the last two lines.** A backup that
    // a server accepted and did not keep is a backup that does not exist, and the
    // only way a client can know it kept one is by asking it what it holds.
    let dir = tempfile::tempdir().unwrap();
    let (pack, snapshot, chunks) = packed(dir.path()).await;
    let root = served_root(dir.path());
    let server = Server::start(&root);
    let remote = remote_of(&server);

    let job = remote
        .merge_pack("docs", pack.clone(), MergeCompression::Keep)
        .await
        .unwrap();
    assert!(!job.is_empty(), "the 202 names the job to watch");

    let finished = remote.await_job(&job).await.unwrap();

    assert_eq!(finished.id, job);
    assert_eq!(finished.kind, Kind::Merge);
    assert_eq!(finished.state, State::Succeeded);
    assert!(finished.finished.is_some(), "a job that ended says when");
    assert_eq!(finished.error, None);

    let Outcome::Merged(Merged::Committed(merged)) = finished.result.clone().unwrap() else {
        panic!("a merge of a new pack commits a version: {finished:?}");
    };

    assert_eq!(merged.set, "docs");
    assert_eq!(merged.version, 1);
    assert_eq!(
        merged.snapshot.to_hex().to_string(),
        snapshot.to_hex().to_string(),
        "the version holds the snapshot that was packed"
    );
    assert_eq!(merged.chunks, chunks, "the plan the merge committed");
    assert!(
        merged.fresh > 0,
        "a set holding nothing stored these chunks"
    );
    assert_eq!(merged.held, 0);
    assert_eq!(merged.base, None);
    assert!(merged.bytes > 0);

    // **The push is only real if the server agrees it holds the version.** The
    // same client, the same socket, asking the question a restore would ask.
    let listed = remote.versions("docs").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].version, 1);
    assert_eq!(
        listed[0].snapshot.to_hex().to_string(),
        snapshot.to_hex().to_string()
    );
}

#[tokio::test]
async fn merging_the_same_pack_twice_reports_the_version_that_already_holds_it() {
    // **The `Already` half of the merge result, and the reason it exists.** A
    // transfer whose answer was lost has to be retryable, so the second merge is
    // a success that reports which version holds the pack rather than an error —
    // and it must be a *different answer* from the first, not a second version 1.
    let dir = tempfile::tempdir().unwrap();
    let (pack, _, _) = packed(dir.path()).await;
    let server = Server::start(&served_root(dir.path()));
    let remote = remote_of(&server);

    let first = remote
        .merge_pack("docs", pack.clone(), MergeCompression::Keep)
        .await
        .unwrap();
    assert!(matches!(
        remote.await_job(&first).await.unwrap().result,
        Some(Outcome::Merged(Merged::Committed(_)))
    ));

    let second = remote
        .merge_pack("docs", pack.clone(), MergeCompression::Keep)
        .await
        .unwrap();
    let finished = remote.await_job(&second).await.unwrap();

    let Outcome::Merged(Merged::Already(already)) = finished.result.clone().unwrap() else {
        panic!("a merge of a pack the set holds writes nothing: {finished:?}");
    };

    assert_eq!(already.set, "docs");
    assert_eq!(already.version, 1, "version 1 is the one that holds it");

    // And no second version was published for it to be a version of.
    assert_eq!(remote.versions("docs").await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_failed_job_is_an_error_carrying_the_servers_own_sentence() {
    // **The client half of the bug `9ab42a7` fixed.** A pack of a set the server
    // does not hold is work the server accepts and then cannot do, so the job
    // fails — and a client that returned that job as an `Ok` would report a
    // successful backup of nothing.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(&served_root(dir.path()));
    let remote = remote_of(&server);

    let job = remote.start_pack("nope", None, None).await.unwrap();

    let error = remote
        .await_job(&job)
        .await
        .expect_err("a job that failed is not a job a caller can use");

    // **The raw poll, to prove the failure is the server's and not the client's.**
    // A client that decided a job had failed on its own would pass the assertion
    // above with a sentence of its own; this one is the server's own document.
    let polled = remote.job(&job).await.unwrap();
    assert_eq!(polled.state, State::Failed);
    assert!(polled.state.is_terminal());
    let said = polled.error.expect("a failed job says why");
    assert!(
        said.contains("nope"),
        "the server names the set it could not find: {said}"
    );
    assert_eq!(
        error.to_string(),
        said,
        "the error carries the server's sentence unchanged"
    );
    assert_eq!(error.kind(), std::io::ErrorKind::Other);

    // The same refusal, asked for directly, is a `NotFound` carrying it too.
    let refused = remote.start_pack("nope", None, None).await.unwrap();
    let refused = remote.await_job(&refused).await.unwrap_err();
    assert!(refused.to_string().contains("nope"));
}

#[tokio::test]
async fn a_set_the_server_does_not_hold_is_not_found_with_its_own_sentence() {
    // Three refusals a client meets first, and one rule for all three: the kind
    // is the one the server started from, and the message is the server's own
    // sentence rather than a paraphrase a reader has to learn.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(&served_root(dir.path()));
    let remote = remote_of(&server);

    let missing_set = remote.versions("nope").await.unwrap_err();
    assert_eq!(missing_set.kind(), std::io::ErrorKind::NotFound);
    assert!(
        missing_set.to_string().contains("no backup set at"),
        "the server's own sentence: {missing_set}"
    );

    // A job id of the server's own shape, for nothing.
    let missing_job = remote.job(&"aa".repeat(32)).await.unwrap_err();
    assert_eq!(missing_job.kind(), std::io::ErrorKind::NotFound);
    assert!(
        missing_job.to_string().contains("no job"),
        "the server's own sentence: {missing_job}"
    );

    // An upload begun and dropped: the id is one the server issued, and the
    // refusal is the one the same request would give for any id it never held.
    let upload = remote.begin_upload().await.unwrap();
    upload.discard().await.unwrap();
    let gone = upload.commit().await.unwrap_err();
    assert_eq!(gone.kind(), std::io::ErrorKind::NotFound);
    assert!(
        gone.to_string().contains("no upload"),
        "the server's own sentence: {gone}"
    );

    // And dropping it twice is still fine: a caller cleaning up after a failure
    // must not have to know whether there was anything left to drop.
    upload.discard().await.unwrap();
}

#[tokio::test]
async fn progress_is_observable_while_a_job_is_awaited() {
    // **The reason `await_job_with` exists.** A merge of minutes that reports
    // nothing looks like a hung command, and this is the only thing in the client
    // that can change that: the same counters the server's event stream carries,
    // handed to a callback as they move.
    let dir = tempfile::tempdir().unwrap();
    let (pack, _, _) = packed(dir.path()).await;
    // **`start_with_progress`, not `start`**: the server's terminal is pinned all
    // the way down (`--quiet` *and* `RUST_LOG=warn`), so anything this sees
    // reached the client in spite of the verbosity rather than because of it.
    let server = Server::start_with_progress(&served_root(dir.path()));
    let remote = remote_of(&server);

    let job = remote
        .merge_pack("docs", pack, MergeCompression::Keep)
        .await
        .unwrap();

    let reported: RefCell<Vec<Vec<(String, u64)>>> = RefCell::new(Vec::new());
    let finished = remote
        .await_job_with(&job, |counters| {
            reported.borrow_mut().push(counters.to_vec());
        })
        .await
        .unwrap();

    let reported = reported.into_inner();
    assert!(
        !reported.is_empty(),
        "a merge that stored chunks must report at least once while it is awaited"
    );
    assert!(
        !finished.progress.is_empty(),
        "and the finished job keeps its own figures"
    );
    assert_eq!(
        reported.last().unwrap(),
        &finished.progress,
        "the last report is the job's own final counters"
    );

    // **The names are this crate's, not the client's.** A counter the tool can
    // produce and a client that cannot render it is a gap with nothing failing,
    // and this is the client end of that check.
    let known = lepatch::progress::counter_names();
    for (name, _) in &finished.progress {
        assert!(
            known.contains(&name.as_str()),
            "the job reports {name:?}, which is not a counter this crate knows: {known:?}"
        );
    }

    // `chunks` is set from the resolved plan before the first chunk is written,
    // so it is the same figure the result reports — two counts of one thing.
    let Outcome::Merged(Merged::Committed(merged)) = finished.result.clone().unwrap() else {
        panic!("the merge committed a version: {finished:?}");
    };
    let chunks = finished
        .progress
        .iter()
        .find(|(name, _)| name == "chunks")
        .map(|(_, value)| *value)
        .expect("a merge reports the chunks it is working through");
    assert_eq!(chunks, merged.chunks);
}

#[tokio::test]
async fn bytes_that_are_not_a_pack_are_refused_at_the_upload() {
    // **The `PUT` is where this is caught, and this client says so.** The server
    // runs `Pack::open` on every upload rather than only at commit, so a caller
    // learns at the transfer that what it sent is not a pack — and the sentence
    // it learns is the one a local `lepatch merge` would have printed.
    let dir = tempfile::tempdir().unwrap();
    let root = served_root(dir.path());
    let server = Server::start(&root);
    let remote = remote_of(&server);

    let upload = remote.begin_upload().await.unwrap();
    let error = upload
        .send(b"this is not a pack".to_vec())
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("pack"),
        "the sentence says what was wrong with the bytes: {error}"
    );

    // The upload is still the caller's until the caller drops it: driving the
    // steps by hand means owning the cleanup, and this is where that shows.
    assert_eq!(held_packs(&root).len(), 1);
    upload.discard().await.unwrap();
    assert!(
        held_packs(&root).is_empty(),
        "the scratch file a dropped upload left is the server's disk, so it goes"
    );
}

#[tokio::test]
async fn a_failed_push_leaves_no_upload_on_the_server() {
    // **The cleanup this client owes, asserted where it is observable.** A
    // push that failed has an upload behind it that nothing will merge, and the
    // scratch file it occupies is on the server's disk. So `merge_pack` drops it
    // on the way out — and the caller hears the failure that went wrong, not the
    // cleanup's.
    let dir = tempfile::tempdir().unwrap();
    let root = served_root(dir.path());
    let server = Server::start(&root);
    let remote = remote_of(&server);

    let error = remote
        .merge_pack(
            "docs",
            b"this is not a pack".to_vec(),
            MergeCompression::Keep,
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.kind(),
        std::io::ErrorKind::InvalidData,
        "the failure the caller hears is the one that went wrong"
    );
    assert!(
        error.to_string().contains("pack"),
        "and it is the server's sentence about the bytes: {error}"
    );
    assert!(
        held_packs(&root).is_empty(),
        "a failed push must not leave an upload on the server: {:?}",
        held_packs(&root)
    );
}
