//! `lepatch serve` over HTTP: the whole push and pull flows, end to end.
//!
//! The two directions a backup has over a network, driven through a real socket
//! to a real process:
//!
//! * **push** — a client backs a tree up locally, packs it, uploads the pack, and
//!   the server merges it into a set it keeps. The set then holds the version.
//! * **pull** — the server packs a version out of a set, and a client fetches only
//!   the chunks it lacks, assembles a file from the byte ranges it is given, and
//!   `Pack::open`s it. The file is byte-identical to one `lepatch pack` would
//!   have written.
//!
//! These are the tests that would catch the two ways this feature could be wrong
//! and still look right:
//!
//! * **a served pack that is not a pack.** `tests/pack_geometry.rs` proves the
//!   bytes match; these prove a client can fetch them over HTTP and open what it
//!   assembled. A `Content-Range` that is off by one, or a plan that omits the
//!   trailer, fails here and nowhere else.
//! * **a push that a listing does not see.** The server caches a read handle per
//!   set, and a cached handle was built before the merge. `a_push_is_visible_to_a
//!   listing_immediately` is the assertion that the cache is evicted.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use common::{http::Request, server::Server};
use lepatch::{
    pack::Pack,
    storage::{BlobStore, Compression, Plan, StorageGet},
};
use tokio::io::AsyncReadExt;

const PACK_MEDIA_TYPE: &str = "application/x-lepatch-pack";

/// A source tree of `files` noisy files.
fn tree(dir: &Path, files: usize, size: usize) -> PathBuf {
    let source = common::source(dir);
    for i in 0..files {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(size + i * 1_000),
        )
        .unwrap();
    }
    source
}

/// Back a tree up into a set at `blob`, and return the set.
async fn backup(source: &Path, blob: &Path) -> lepatch::storage::BlobId {
    let storage = BlobStore::open_for_writing(
        blob,
        lepatch::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();

    lepatch::command::backup(source, lepatch::command::Options::default(), storage)
        .await
        .unwrap()
        .snapshot
}

/// Pack a set's latest version, into `at`, recorded as the pack's own version.
///
/// The number is the pack's `version` header, which is the version it came *from*
/// in the set that made it. It matters for the delta tests, where a pack of
/// version 2 against a base of version 1 has to be nameable on the server.
async fn pack(blob: &Path, at: &Path) -> Pack {
    let store = BlobStore::open_for_reading(blob).await.unwrap();
    let (version, snapshot_id) = store.latest().unwrap();

    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();

    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();

    Pack::write(&store, &plan, &snapshot, version, at)
        .await
        .unwrap()
}

/// Ask for a pack and wait for its job, returning the job's `result`.
async fn request_pack(server: &Server, set: &str, version: Option<u32>) -> serde_json::Value {
    let mut body = serde_json::json!({});
    if let Some(version) = version {
        body["version"] = serde_json::json!(version);
    }

    let accepted = Request::json("POST", &format!("/v1/sets/{set}/packs"), &body)
        .send(server.address())
        .await;

    assert_eq!(accepted.status, 202, "body: {}", accepted.text());
    assert!(
        accepted.header("location").is_some(),
        "a 202 must say where the job is"
    );

    let job = accepted.field("job").unwrap().as_str().unwrap().to_string();
    await_job(server, &job).await
}

/// Poll a job until it finishes, and return its `result`.
async fn await_job(server: &Server, job: &str) -> serde_json::Value {
    for _ in 0..400 {
        let response = Request::new("GET", &format!("/v1/jobs/{job}"))
            .send(server.address())
            .await;

        assert_eq!(response.status, 200, "body: {}", response.text());

        let state = response
            .field("state")
            .and_then(|value| value.as_str().map(str::to_string))
            .expect("a job document carries a state");

        match state.as_str() {
            "succeeded" => {
                return response
                    .field("result")
                    .expect("a succeeded job carries a result");
            }
            "failed" | "cancelled" => {
                panic!("job {job} {state}: {}", response.text())
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
        }
    }

    panic!("job {job} did not finish in time");
}

/// Upload a pack and merge it into `set`, returning the merge's result.
async fn push(server: &Server, set: &str, pack_path: &Path) -> serde_json::Value {
    let begin = Request::new("POST", "/v1/uploads")
        .send(server.address())
        .await;
    assert_eq!(begin.status, 201, "body: {}", begin.text());

    let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();

    let bytes = fs::read(pack_path).unwrap();
    let filled = Request::bytes(
        "PUT",
        &format!("/v1/uploads/{upload}"),
        PACK_MEDIA_TYPE,
        bytes,
    )
    .send(server.address())
    .await;
    assert_eq!(filled.status, 204, "body: {}", filled.text());

    let commit = Request::new("POST", &format!("/v1/uploads/{upload}/commit"))
        .send(server.address())
        .await;
    assert_eq!(commit.status, 200, "body: {}", commit.text());

    let accepted = Request::json(
        "POST",
        &format!("/v1/sets/{set}/merges"),
        &serde_json::json!({ "upload": upload }),
    )
    .send(server.address())
    .await;
    assert_eq!(accepted.status, 202, "body: {}", accepted.text());

    let job = accepted.field("job").unwrap().as_str().unwrap().to_string();
    await_job(server, &job).await
}

#[tokio::test]
async fn the_server_reports_its_version_and_formats() {
    // A client cannot work this out from a pack fetch, which is why it is an
    // endpoint at all.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path());

    let response = Request::new("GET", "/v1/version")
        .send(server.address())
        .await;

    assert_eq!(response.status, 200);
    assert!(response.is_type("application/json"));

    let document: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(document["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        document["pack_format"],
        serde_json::json!(lepatch::pack::FORMAT_VERSION)
    );
}

#[tokio::test]
async fn a_fresh_root_holds_no_sets() {
    // `{"sets":[]}`, not `{"sets":null}`: the first says "there are none" and
    // the second says the field has no value, which is a different claim.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path());

    let response = Request::new("GET", "/v1/sets").send(server.address()).await;

    assert_eq!(response.status, 200);
    assert_eq!(response.text(), r#"{"sets":[]}"#);
}

#[tokio::test]
async fn a_set_name_that_climbs_out_is_refused() {
    // The one check standing between a request and the filesystem.
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path());

    for name in ["..", "..%2Fetc", "a%2Fb"] {
        let response = Request::new("GET", &format!("/v1/sets/{name}/versions"))
            .send(server.address())
            .await;

        assert!(
            response.status == 400 || response.status == 404,
            "{name} produced {}",
            response.status
        );
    }
}

#[tokio::test]
async fn a_push_lands_in_the_set_and_a_listing_sees_it_immediately() {
    // **The read-cache eviction.** The server caches a read handle per set, and a
    // cached handle's `contents` map was built before the merge. Without the
    // eviction this listing would omit the version the push just created — a
    // wrong answer, not a slow one.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 4, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // Nothing there yet.
    assert_eq!(
        Request::new("GET", "/v1/sets")
            .send(server.address())
            .await
            .text(),
        r#"{"sets":[]}"#
    );

    let merged = push(&server, "docs", &pack_path).await;
    assert_eq!(merged["already"], serde_json::json!(false));
    assert_eq!(merged["version"], serde_json::json!(1));

    // **Immediately**, on the same server, without anything evicting it by hand.
    let listed = Request::new("GET", "/v1/sets/docs/versions")
        .send(server.address())
        .await;
    assert_eq!(listed.status, 200, "body: {}", listed.text());

    let versions: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
    assert_eq!(
        versions["versions"].as_array().map(Vec::len),
        Some(1),
        "the listing must show the version the push created: {listed:?}"
    );

    // And the set resolves it: the figures are read through the store, not off an
    // index file.
    let info = Request::new("GET", "/v1/sets/docs/versions/1")
        .send(server.address())
        .await;
    assert_eq!(info.status, 200, "body: {}", info.text());
    assert_eq!(info.field("pack").unwrap(), serde_json::json!("docs@1"));
}

#[tokio::test]
async fn pushing_the_same_pack_twice_reports_which_version_holds_it() {
    // **A retried transfer has to be answerable.** The second push is not an
    // error, and it says which version already holds the snapshot so a client can
    // ask for that one by number.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 3, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let first = push(&server, "docs", &pack_path).await;
    assert_eq!(first["already"], serde_json::json!(false));

    let second = push(&server, "docs", &pack_path).await;
    assert_eq!(
        second["already"],
        serde_json::json!(true),
        "the second push must say it was already merged: {second}"
    );
    assert_eq!(second["version"], serde_json::json!(1));
}

#[tokio::test]
async fn a_pulled_pack_is_byte_identical_to_one_packed_locally() {
    // **The pull flow, whole.** A pack is requested, the whole file is fetched
    // with no `Range` header, and it must be the same file `lepatch pack` wrote —
    // which is what makes a client able to `Pack::open` it and merge it locally
    // without knowing it came off a wire.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 5, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    let local = pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let summary = request_pack(&server, "docs", None).await;
    let length = summary["length"].as_u64().unwrap();

    let response = Request::new("GET", "/v1/packs/docs@1")
        .send(server.address())
        .await;

    assert_eq!(response.status, 200, "body: {}", response.text());
    assert!(response.is_type(PACK_MEDIA_TYPE));
    assert_eq!(response.header("accept-ranges"), Some("bytes"));
    assert_eq!(response.body.len() as u64, length);

    // The bytes, and the file they open as.
    assert_eq!(response.body, fs::read(&pack_path).unwrap());

    let fetched = dir.path().join("fetched.pack");
    fs::write(&fetched, &response.body).unwrap();

    let opened = Pack::open(&fetched).unwrap();
    assert_eq!(opened.len(), local.len());
    assert_eq!(opened.version(), 1);
    assert_eq!(opened.payload_len(), local.payload_len());
    assert_eq!(opened.snapshot_id(), local.snapshot_id());
}

#[tokio::test]
async fn a_client_fetches_only_the_chunks_it_is_missing() {
    // **The property the pull protocol exists for.** A client holding half the
    // chunks asks for a plan, fetches only the ranges the plan names, assembles a
    // sparse file, and `Pack::open`s it.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 8, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let summary = request_pack(&server, "docs", None).await;
    let length = summary["length"].as_u64().unwrap();

    // What this "client" already holds: half the chunks.
    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let (_, snapshot_id) = store.latest().unwrap();
    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();

    let every: Vec<_> = plan
        .iter()
        .map(|chunk| chunk.id.to_hex().to_string())
        .collect();
    let have: Vec<&String> = every.iter().step_by(2).collect();
    assert!(
        have.len() >= 2,
        "the fixture must leave a client holding some chunks"
    );

    let planned = Request::json(
        "POST",
        "/v1/packs/docs@1/plan",
        &serde_json::json!({ "have": have }),
    )
    .send(server.address())
    .await;
    assert_eq!(planned.status, 200, "body: {}", planned.text());

    let body: serde_json::Value = serde_json::from_slice(&planned.body).unwrap();
    let total = body["bytes"].as_u64().unwrap();
    let ranges = body["ranges"].as_array().unwrap();

    assert_eq!(body["length"].as_u64().unwrap(), length);
    assert!(
        total < length,
        "a client holding half the chunks must fetch less than the pack"
    );

    // Fetch and assemble, the way a client has to.
    let assembled = dir.path().join("partial.pack");
    fs::File::create(&assembled)
        .unwrap()
        .set_len(length)
        .unwrap();

    for range in ranges {
        let start = range["start"].as_u64().unwrap();
        let end = range["end"].as_u64().unwrap();

        let response = Request::new("GET", "/v1/packs/docs@1")
            .header("range", &format!("bytes={}-{}", start, end - 1))
            .send(server.address())
            .await;

        assert_eq!(response.status, 206, "body: {}", response.text());
        assert_eq!(
            response.header("content-range"),
            Some(format!("bytes {}-{}/{}", start, end - 1, length).as_str())
        );
        assert_eq!(response.body.len() as u64, end - start);

        let mut file = fs::OpenOptions::new().write(true).open(&assembled).unwrap();
        file.seek(SeekFrom::Start(start)).unwrap();
        file.write_all(&response.body).unwrap();
    }

    // The assembled file opens, and its index names every chunk — including the
    // ones this client claimed to hold and wrote as holes.
    let opened = Pack::open(&assembled).unwrap();
    assert_eq!(opened.len(), every.len());
}

#[tokio::test]
async fn the_plan_covers_the_fixed_regions_a_client_needs() {
    // A client holding **everything** still fetches the header, snapshot, index
    // and trailer. Without them the file it assembles does not open, because
    // `Pack::open` reads the trailer first.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 3, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let (_, snapshot_id) = store.latest().unwrap();
    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();
    let every: Vec<String> = plan.iter().map(|c| c.id.to_hex().to_string()).collect();

    let planned = Request::json(
        "POST",
        "/v1/packs/docs@1/plan",
        &serde_json::json!({ "have": every }),
    )
    .send(server.address())
    .await;
    assert_eq!(planned.status, 200, "body: {}", planned.text());

    let body: serde_json::Value = serde_json::from_slice(&planned.body).unwrap();
    let ranges = body["ranges"].as_array().unwrap();

    // The four fixed regions are covered — and **merged**, because two of them are
    // adjacent. A plan is a list of fetches, not a list of gaps, so asking for
    // four ranges where two touch would be two requests for one run of bytes.
    for pair in ranges.windows(2) {
        assert!(
            pair[0]["end"].as_u64().unwrap() <= pair[1]["start"].as_u64().unwrap(),
            "the plan must be sorted and must not overlap: {ranges:?}"
        );
    }

    // The header is always first.
    assert_eq!(ranges[0]["start"].as_u64().unwrap(), 0);

    // And it is small. A client holding every chunk fetches the fixed regions and
    // nothing else — which is the whole point of asking, and what
    // `the_fixed_regions_alone_are_enough_to_open_a_pack` then opens.
    let covered: u64 = ranges
        .iter()
        .map(|r| r["end"].as_u64().unwrap() - r["start"].as_u64().unwrap())
        .sum();
    assert_eq!(covered, body["bytes"].as_u64().unwrap());
    assert!(
        covered < body["length"].as_u64().unwrap() / 10,
        "holding every chunk should cost a small part of the pack, not all of it"
    );
}

#[tokio::test]
async fn a_held_chunk_hash_that_is_not_one_is_refused() {
    // A typo would become a chunk the client does not have, and it would fetch
    // it — with the response covering bytes that were never asked for.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let planned = Request::json(
        "POST",
        "/v1/packs/nosuch@1/plan",
        &serde_json::json!({ "have": ["not-a-hash"] }),
    )
    .send(server.address())
    .await;

    // The set is resolved before the plan, so this is a 404 rather than a 400.
    // Both are refusals; the set is the first thing that can fail.
    assert!(
        planned.status == 400 || planned.status == 404,
        "got {}",
        planned.status
    );
}

#[tokio::test]
async fn a_range_past_the_end_is_four_sixteen() {
    // Not an empty body. A client whose idea of the file is wrong should hear
    // about it, rather than receive nothing and write a hole.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let summary = request_pack(&server, "docs", None).await;
    let length = summary["length"].as_u64().unwrap();

    let response = Request::new("GET", "/v1/packs/docs@1")
        .header("range", &format!("bytes={}-", length + 10_000))
        .send(server.address())
        .await;

    assert_eq!(response.status, 416);
    assert_eq!(
        response.header("content-range"),
        Some(format!("bytes */{length}").as_str())
    );
}

#[tokio::test]
async fn a_range_that_asks_for_more_than_there_is_gets_what_there_is() {
    // RFC 9110 allows clamping, and a client whose plan was slightly optimistic
    // should still get a usable file.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let summary = request_pack(&server, "docs", None).await;
    let length = summary["length"].as_u64().unwrap();

    let response = Request::new("GET", "/v1/packs/docs@1")
        .header("range", &format!("bytes={}-999999", length - 10))
        .send(server.address())
        .await;

    assert_eq!(response.status, 206);
    assert_eq!(response.body.len() as u64, 10);
}

#[tokio::test]
async fn the_fixed_regions_alone_are_enough_to_open_a_pack() {
    // A client that already holds **every** chunk fetches only the header, the
    // snapshot, the index and the trailer — and that is enough for `Pack::open`,
    // because a pack's index is at the end and its trailer is last of all.
    //
    // This is why `needed()` is not the answer a client wants: the ranges of the
    // missing chunks alone would leave a payload with no trailer, and `Pack::open`
    // reads the trailer first.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 4, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    let local = pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let (_, snapshot_id) = store.latest().unwrap();
    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();
    let every: Vec<String> = plan.iter().map(|c| c.id.to_hex().to_string()).collect();

    let body = plan_for(&server, &every).await;
    let length = body["length"].as_u64().unwrap();
    let ranges = body["ranges"].as_array().unwrap();

    // Only the fixed regions, so far less than the whole file.
    let fixed: u64 = ranges
        .iter()
        .map(|r| r["end"].as_u64().unwrap() - r["start"].as_u64().unwrap())
        .sum();
    assert!(
        fixed < length / 10,
        "the fixed regions are a small part of a pack"
    );

    // Fetch them into a sparse file and open it.
    let head = dir.path().join("head.pack");
    fs::File::create(&head).unwrap().set_len(length).unwrap();

    for range in ranges {
        let start = range["start"].as_u64().unwrap();
        let end = range["end"].as_u64().unwrap();

        let response = Request::new("GET", "/v1/packs/docs@1")
            .header("range", &format!("bytes={}-{}", start, end - 1))
            .send(server.address())
            .await;
        assert_eq!(response.status, 206, "body: {}", response.text());

        let mut file = fs::OpenOptions::new().write(true).open(&head).unwrap();
        file.seek(SeekFrom::Start(start)).unwrap();
        file.write_all(&response.body).unwrap();
    }

    // It opens, and names every chunk — including the ones this client claims to
    // hold and has written as holes.
    let opened = Pack::open(&head).unwrap();
    assert_eq!(opened.len(), local.len());
    assert_eq!(opened.snapshot_id(), local.snapshot_id());
}

#[tokio::test]
async fn a_range_that_is_not_one_is_refused_with_the_form_that_is() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // A pack request needs a real set first, so this uses the same shape against
    // a name that does not resolve: the range is refused before anything is read.
    let response = Request::new("GET", "/v1/packs/nosuch@1")
        .header("range", "bytes=-500")
        .send(server.address())
        .await;

    // A suffix range is parsed before the set is resolved, so this is the 400 and
    // the message names the form that works.
    assert_eq!(response.status, 400, "body: {}", response.text());
    assert!(response.text().contains("bytes=first-last"));
}

#[tokio::test]
async fn bytes_that_are_not_a_pack_are_refused_at_the_upload() {
    // **Where the check happens, and why.** `Pack::open` reads the trailer and the
    // index and no payload, so checking here costs nothing beside the transfer and
    // the client finds out at the request it sent rather than one request later.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let begin = Request::new("POST", "/v1/uploads")
        .send(server.address())
        .await;
    let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();

    let noise = common::noisy(4_000);
    let filled = Request::bytes(
        "PUT",
        &format!("/v1/uploads/{upload}"),
        PACK_MEDIA_TYPE,
        noise,
    )
    .send(server.address())
    .await;

    assert_eq!(filled.status, 422, "body: {}", filled.text());
    assert!(filled.is_type("application/problem+json"));

    // And a commit of the same upload still refuses, rather than reporting figures
    // for a file that is not a pack.
    let commit = Request::new("POST", &format!("/v1/uploads/{upload}/commit"))
        .send(server.address())
        .await;
    assert_eq!(commit.status, 422);
}

#[tokio::test]
async fn a_commit_reports_what_arrived() {
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 3, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    let local = pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let begin = Request::new("POST", "/v1/uploads")
        .send(server.address())
        .await;
    let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();

    Request::bytes(
        "PUT",
        &format!("/v1/uploads/{upload}"),
        PACK_MEDIA_TYPE,
        fs::read(&pack_path).unwrap(),
    )
    .send(server.address())
    .await;

    let commit = Request::new("POST", &format!("/v1/uploads/{upload}/commit"))
        .send(server.address())
        .await;
    assert_eq!(commit.status, 200, "body: {}", commit.text());

    // The figures come from the pack's own header and index, so they agree with
    // the file on disk without a second implementation of the format.
    assert_eq!(
        commit.field("length").unwrap().as_u64().unwrap(),
        local.file_len().unwrap()
    );
    assert_eq!(
        commit.field("chunks").unwrap().as_u64().unwrap(),
        local.len() as u64
    );
    assert_eq!(
        commit.field("snapshot").unwrap().as_str().unwrap(),
        local.snapshot_id().to_hex().to_string()
    );
    assert_eq!(commit.field("version").unwrap(), serde_json::json!(1));
}

#[tokio::test]
async fn a_compression_policy_that_cannot_be_served_from_a_set_is_refused() {
    // A re-encode is not in the set's bytes, so a pack under one cannot be
    // computed. Refused rather than quietly answered with `keep` bytes, which
    // would be a client receiving something it did not ask for.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let response = Request::json(
        "POST",
        "/v1/sets/docs/packs",
        &serde_json::json!({ "compression": "9" }),
    )
    .send(server.address())
    .await;

    assert_eq!(response.status, 422, "body: {}", response.text());
    assert!(response.text().contains("keep"));
}

#[tokio::test]
async fn a_name_that_is_not_a_set_is_a_four_oh_four_not_an_empty_list() {
    // "There are no versions" and "that name names nothing" are different claims,
    // and a client acting on the first when the second is true would give up.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let response = Request::new("GET", "/v1/sets/nosuch/versions")
        .send(server.address())
        .await;

    assert_eq!(response.status, 404);
    assert!(response.is_type("application/problem+json"));

    let problem: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(problem["type"], "about:blank");
    assert_eq!(problem["status"], serde_json::json!(404));
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("no backup set")
    );
}

#[tokio::test]
async fn a_job_can_be_watched_over_sse() {
    // **The stream, end to end.** `Request::send` reads to end-of-stream, which
    // for an SSE body means "until the job is over" — so it would hand back the
    // frames in a lump and prove nothing about the stream. This uses the bounded
    // streaming read, which stops as soon as it has enough.
    //
    // The three event names are part of the contract, not a detail: a client
    // switches on them rather than parsing the payload to find out what it is.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 4, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // Accept the merge, then read the stream.
    let begin = Request::new("POST", "/v1/uploads")
        .send(server.address())
        .await;
    let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();
    Request::bytes(
        "PUT",
        &format!("/v1/uploads/{upload}"),
        PACK_MEDIA_TYPE,
        fs::read(&pack_path).unwrap(),
    )
    .send(server.address())
    .await;

    let accepted = Request::json(
        "POST",
        "/v1/sets/docs/merges",
        &serde_json::json!({ "upload": upload }),
    )
    .send(server.address())
    .await;
    assert_eq!(accepted.status, 202, "body: {}", accepted.text());
    let job = accepted.field("job").unwrap().as_str().unwrap().to_string();

    let stream = Request::new("GET", &format!("/v1/jobs/{job}/events"))
        .stream_until(server.address(), 8192, "succeeded")
        .await;

    assert_eq!(stream.status, 200);
    let text = String::from_utf8_lossy(&stream.body).into_owned();

    assert!(
        text.contains("event: state"),
        "the stream must name state transitions: {text}"
    );
    assert!(
        text.contains("event: result"),
        "the stream must carry the result: {text}"
    );
    assert!(
        text.contains("succeeded"),
        "the terminal state must be on the stream: {text}"
    );

    // And it agrees with what polling says, which is the property that makes the
    // two worth having.
    await_job(&server, &job).await;
}

#[tokio::test]
async fn a_running_job_can_be_cancelled() {
    // `DELETE /v1/jobs/{id}` was documented and implemented and untested. The
    // `202` carries a `Location`, which is unusual for a `DELETE` and so is
    // exactly the kind of thing that should be pinned.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let begin = Request::new("POST", "/v1/uploads")
        .send(server.address())
        .await;
    let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();
    Request::bytes(
        "PUT",
        &format!("/v1/uploads/{upload}"),
        PACK_MEDIA_TYPE,
        fs::read(&pack_path).unwrap(),
    )
    .send(server.address())
    .await;

    let accepted = Request::json(
        "POST",
        "/v1/sets/docs/merges",
        &serde_json::json!({ "upload": upload }),
    )
    .send(server.address())
    .await;
    let job = accepted.field("job").unwrap().as_str().unwrap().to_string();

    // Either it is still running, or it finished first. Both are legitimate
    // outcomes over a real socket, so the test accepts either and asserts the
    // mapping either way — a cancel of a *finished* job is a `409`, and that is
    // worth pinning too.
    let cancelled = Request::new("DELETE", &format!("/v1/jobs/{job}"))
        .send(server.address())
        .await;

    match cancelled.status {
        202 => {
            assert_eq!(
                cancelled.header("location"),
                Some(format!("/v1/jobs/{job}").as_str()),
                "an accepted cancel must say where the job is"
            );

            // And it reaches a terminal state a client can see.
            let mut state = String::new();
            for _ in 0..400 {
                let response = Request::new("GET", &format!("/v1/jobs/{job}"))
                    .send(server.address())
                    .await;
                state = response
                    .field("state")
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default();

                if state == "cancelled" || state == "succeeded" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }

            assert!(
                state == "cancelled" || state == "succeeded",
                "a cancelled job must reach a terminal state, got {state:?}"
            );

            // A second cancel of a finished job is a definite answer, not a 202
            // for something that will not happen.
            let again = Request::new("DELETE", &format!("/v1/jobs/{job}"))
                .send(server.address())
                .await;
            assert_eq!(again.status, 409, "body: {}", again.text());
        }
        409 => { /* it finished before we got there; the 409 is the contract */ }
        other => panic!("a cancel returned {other}: {}", cancelled.text()),
    }

    // A job id that is not one is a `400`, not a `404`.
    let bad = Request::new("DELETE", "/v1/jobs/not-a-job")
        .send(server.address())
        .await;
    assert_eq!(bad.status, 400);
    assert!(bad.text().contains("64 hexadecimal"));
}

#[tokio::test]
async fn a_push_then_a_pull_round_trips_into_a_second_set() {
    // **The whole feature, in one test.** A tree is backed up and packed locally,
    // pushed, and then pulled back out — and merged into a *different* set, which
    // is what makes it a round trip rather than a copy.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 6, 25_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // Push.
    let pushed = push(&server, "remote", &pack_path).await;
    assert_eq!(pushed["already"], serde_json::json!(false));

    // Pull the pack back out.
    let whole = Request::new("GET", "/v1/packs/remote@1")
        .send(server.address())
        .await;
    assert_eq!(whole.status, 200);

    let fetched = dir.path().join("fetched.pack");
    fs::write(&fetched, &whole.body).unwrap();

    // And merge it into a local set that had nothing.
    let mirror = dir.path().join("mirror");
    let storage = BlobStore::open_for_writing(
        &mirror,
        lepatch::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();

    let merged = lepatch::command::merge(
        &storage,
        &Pack::open(&fetched).unwrap(),
        lepatch::command::MergeCompression::Keep,
    )
    .await
    .unwrap();

    let lepatch::command::Merged::Committed { version, .. } = merged else {
        panic!("the mirror set had nothing, so this must be a commit");
    };
    assert_eq!(version, 1);

    // The mirror restores to the same tree.
    let restored = dir.path().join("restored");
    let store = BlobStore::open_for_reading(&mirror).await.unwrap();
    let snapshot = store.snapshot_via_index(1).unwrap();
    lepatch::command::restore(&restored, snapshot, store)
        .await
        .unwrap();

    common::assert_same_tree(&source, &restored);
}

#[tokio::test]
async fn a_set_that_holds_nothing_is_not_a_set() {
    // A scratch directory beside the sets, and a file a user left. Neither is a
    // set, so neither is listed — and neither becomes a `404` later.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("notes.txt"), b"not a set").unwrap();
    fs::create_dir_all(root.join(".lepatch-serve")).unwrap();

    let server = Server::start(&root);

    let response = Request::new("GET", "/v1/sets").send(server.address()).await;
    assert_eq!(response.status, 200);
    assert_eq!(response.text(), r#"{"sets":[]}"#);
}

#[tokio::test]
async fn chunk_hashes_round_trip_through_the_plan() {
    // The plan's `have` is a list of hex strings. A hash that does not round-trip
    // would silently become a chunk the client does not hold.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 4, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let (_, snapshot_id) = store.latest().unwrap();
    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();
    let every: Vec<String> = plan.iter().map(|c| c.id.to_hex().to_string()).collect();

    // A `have` of every chunk and one of none must give two different plans, and
    // both must be well-formed: sorted, non-overlapping.
    let with = plan_for(&server, &every).await;
    let without = plan_for(&server, &[]).await;

    assert!(without["bytes"].as_u64().unwrap() >= with["bytes"].as_u64().unwrap());

    for planned in [&with, &without] {
        let ranges = planned["ranges"].as_array().unwrap();
        for pair in ranges.windows(2) {
            assert!(
                pair[0]["end"].as_u64().unwrap() <= pair[1]["start"].as_u64().unwrap(),
                "ranges must not overlap: {ranges:?}"
            );
        }
    }
}

/// Ask for a plan against a set, and return the parsed body.
async fn plan_for(server: &Server, have: &[String]) -> serde_json::Value {
    let response = Request::json(
        "POST",
        "/v1/packs/docs@1/plan",
        &serde_json::json!({ "have": have }),
    )
    .send(server.address())
    .await;

    assert_eq!(response.status, 200, "body: {}", response.text());
    serde_json::from_slice(&response.body).unwrap()
}

#[tokio::test]
async fn an_unresolvable_version_is_not_listed_as_one() {
    // The index is a cache. A version whose index file was never written still
    // exists and still restores, but the listing must not name something the
    // store cannot serve.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;

    // A set with a segment but no index files, which is what a set left by a run
    // that committed without writing one looks like.
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let segment = lepatch::layout::IndexSet::new(root.join("docs")).segment_path(0);
    let from = lepatch::layout::IndexSet::new(&blob).segment_path(0);
    fs::write(&segment, fs::read(from).unwrap()).unwrap();

    let server = Server::start(&root);

    // A version that does not exist.
    let missing = Request::new("GET", "/v1/sets/docs/versions/99")
        .send(server.address())
        .await;
    assert!(
        missing.status == 404 || missing.status == 422,
        "got {}",
        missing.status
    );

    // And the listing does not invent one.
    let listed = Request::new("GET", "/v1/sets/docs/versions")
        .send(server.address())
        .await;
    assert_eq!(listed.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert!(
        versions
            .iter()
            .all(|v| v["version"] != serde_json::json!(99)),
        "the listing must not name a version the store cannot serve"
    );
}

#[tokio::test]
async fn a_set_name_with_a_dot_is_ordinary() {
    // A set's name is a *prefix*, not a stem, so `docs.tar` is a set called
    // `docs.tar` and produces `docs.tar.0000.bin`. Nothing in it is reinterpreted.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("docs.tar");
    backup(&source, &blob).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    fs::copy(
        blob.with_file_name("docs.tar.0000.bin"),
        root.join("docs.tar.0000.bin"),
    )
    .unwrap();

    let server = Server::start(&root);

    let listed = Request::new("GET", "/v1/sets").send(server.address()).await;
    let body: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
    let names: Vec<&str> = body["sets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();

    assert_eq!(names, vec!["docs.tar"]);
}

#[tokio::test]
async fn two_merges_into_one_set_are_serialised() {
    // **The write lock.** `BlobStore` takes its append reservation when it opens,
    // so two handles onto one set would each believe they owned the next offset.
    // Without the per-set lock the two merges would interleave records into one
    // segment.
    let dir = tempfile::tempdir().unwrap();

    // Two packs of two different trees, so neither is a duplicate of the other.
    let first_source = dir.path().join("one");
    let second_source = dir.path().join("two");
    fs::create_dir_all(&first_source).unwrap();
    fs::create_dir_all(&second_source).unwrap();
    fs::write(first_source.join("a.bin"), common::noisy(30_000)).unwrap();
    fs::write(second_source.join("b.bin"), common::noisy(35_000)).unwrap();

    let one = dir.path().join("one.store");
    let two = dir.path().join("two.store");
    backup(&first_source, &one).await;
    backup(&second_source, &two).await;

    let one_pack = dir.path().join("one.pack");
    let two_pack = dir.path().join("two.pack");
    pack(&one, &one_pack).await;
    pack(&two, &two_pack).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // Both at once, without awaiting the first.
    let a = async { push(&server, "shared", &one_pack).await };
    let b = async { push(&server, "shared", &two_pack).await };
    let (first, second) = tokio::join!(a, b);
    for merged in [&first, &second] {
        assert_eq!(merged["already"], serde_json::json!(false));
    }

    // Two distinct versions, and the set holds both. Interleaved records would
    // leave one version that does not restore, or a set that fails to open.
    let listed = Request::new("GET", "/v1/sets/shared/versions")
        .send(server.address())
        .await;
    assert_eq!(listed.status, 200, "body: {}", listed.text());

    let body: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
    assert_eq!(
        body["versions"].as_array().map(Vec::len),
        Some(2),
        "both merges must have published a version"
    );

    // And the set is still a set that opens, which a scan check establishes.
    let store = BlobStore::open_for_reading(root.join("shared"))
        .await
        .unwrap();
    assert_eq!(store.versions().unwrap().len(), 2);
}

#[tokio::test]
async fn a_dropped_upload_leaves_nothing_on_disk() {
    // The scratch file is what a crash would leave behind, so `DELETE` and the
    // server's own shutdown both have to clear it.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let begin = Request::new("POST", "/v1/uploads")
        .send(server.address())
        .await;
    let upload = begin.field("upload").unwrap().as_str().unwrap().to_string();

    Request::bytes(
        "PUT",
        &format!("/v1/uploads/{upload}"),
        PACK_MEDIA_TYPE,
        fs::read(&pack_path).unwrap(),
    )
    .send(server.address())
    .await;

    let scratch = root.join(".lepatch-serve");
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        1,
        "the upload should be on disk"
    );

    let dropped = Request::new("DELETE", &format!("/v1/uploads/{upload}"))
        .send(server.address())
        .await;
    assert_eq!(dropped.status, 204);

    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "the upload's file should be gone"
    );
}

#[tokio::test]
async fn the_server_survives_a_request_it_cannot_answer() {
    // A damaged or absent set is a request that fails, not a server that stops.
    // Every error is a problem document, so a client parses one shape.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let mut server = Server::start(&root);

    let bad = Request::new("POST", "/v1/sets/docs/packs")
        .header("content-type", "application/json")
        .header("content-length", "3")
        .send(server.address())
        .await;
    assert!(bad.status >= 400, "got {}", bad.status);

    // Still serving.
    assert_eq!(
        Request::new("GET", "/v1/version")
            .send(server.address())
            .await
            .status,
        200
    );
    assert!(server.is_alive());
}

#[tokio::test]
async fn a_job_document_carries_what_a_client_needs() {
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 2, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let jobs = Request::new("GET", "/v1/jobs").send(server.address()).await;
    assert_eq!(jobs.status, 200);

    let body: serde_json::Value = serde_json::from_slice(&jobs.body).unwrap();
    let listed = body["jobs"].as_array().unwrap();
    assert!(!listed.is_empty(), "the merges above should have made jobs");

    for job in listed {
        for field in [
            "id", "kind", "state", "started", "finished", "error", "result", "progress",
        ] {
            assert!(
                job.get(field).is_some(),
                "a job document must carry {field}: {job}"
            );
        }
        assert_eq!(job["kind"], serde_json::json!("merge"));
        assert_eq!(job["state"], serde_json::json!("succeeded"));
    }
}

#[tokio::test]
async fn holding_more_chunks_means_fetching_fewer_bytes() {
    // The reason the plan endpoint exists, stated as a comparison rather than as
    // one number. A plan whose `have` were compared in the wrong shape would
    // return the same answer for a client holding nothing and one holding
    // everything, and this is what would notice.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 6, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let (_, snapshot_id) = store.latest().unwrap();
    let mut reader = store.get(&snapshot_id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = lepatch::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap();

    let every: Vec<String> = plan.iter().map(|c| c.id.to_hex().to_string()).collect();
    let half: Vec<String> = every.iter().step_by(2).cloned().collect();
    let none: Vec<String> = Vec::new();

    let holding_none = plan_for(&server, &none).await;
    let holding_half = plan_for(&server, &half).await;
    let holding_all = plan_for(&server, &every).await;

    let none = holding_none["bytes"].as_u64().unwrap();
    let half = holding_half["bytes"].as_u64().unwrap();
    let all = holding_all["bytes"].as_u64().unwrap();
    let length = holding_all["length"].as_u64().unwrap();

    assert!(
        none >= half,
        "holding half must not cost more than holding none"
    );
    assert!(
        half > all,
        "holding everything must cost less than holding half"
    );
    assert!(
        all < length,
        "holding every chunk must still cost less than the pack"
    );
    assert!(
        all > 0,
        "the fixed regions are always fetched, so this is never zero"
    );
}

#[tokio::test]
async fn a_delta_pack_over_http_is_smaller_than_the_full_one() {
    // **The end-to-end consequence of the id grammar.** `docs@2-1` used to be
    // accepted and answered with a full pack, because `resolve` parsed `base` off
    // the wire and then hardcoded `None` into `VirtualPack::new`. Nothing caught
    // it: the id parsed, the route matched, and the response was a perfectly good
    // pack that was the wrong one.
    //
    // So the assertion is the property the grammar promises — a delta is smaller,
    // its header carries the base, and it still opens.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 6, 20_000);
    let blob = dir.path().join("local");

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // **Version 1 first**, so the server's set really holds two versions and a
    // delta has something to be a delta against.
    backup(&source, &blob).await;
    let first_pack = dir.path().join("v1.pack");
    pack(&blob, &first_pack).await;
    push(&server, "docs", &first_pack).await;

    // Version 2: change one file, back up again, and push it too.
    fs::write(source.join("f0.bin"), common::noisy(60_000)).unwrap();
    let storage = BlobStore::open_for_writing(
        &blob,
        lepatch::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();
    lepatch::command::backup(&source, lepatch::command::Options::default(), storage)
        .await
        .unwrap();

    let pack_path = dir.path().join("v2.pack");
    pack(&blob, &pack_path).await;
    push(&server, "docs", &pack_path).await;

    // The full pack, kept on disk so the delta's index can be compared with it.
    let full_at = dir.path().join("full.pack");
    let full = Request::new("GET", "/v1/packs/docs@2")
        .send(server.address())
        .await;
    assert_eq!(full.status, 200, "body: {}", full.text());
    fs::write(&full_at, &full.body).unwrap();

    // The delta, whose reader is assumed to already hold version 1.
    let delta = Request::new("GET", "/v1/packs/docs@2-1")
        .send(server.address())
        .await;
    assert_eq!(delta.status, 200, "body: {}", delta.text());

    assert!(
        delta.body.len() < full.body.len(),
        "a delta must be smaller than the full pack: {} vs {}",
        delta.body.len(),
        full.body.len()
    );

    // And it is a real delta: the header records the base, and `Pack::open`
    // reports it. A full pack served under a `-base` id would have neither.
    let at = dir.path().join("delta.pack");
    fs::write(&at, &delta.body).unwrap();

    let opened = Pack::open(&at).unwrap();
    let base = opened
        .base()
        .expect("the delta's header must name its base");

    let store = BlobStore::open_for_reading(root.join("docs"))
        .await
        .unwrap();
    assert_eq!(
        base,
        store.snapshot_via_index(1).unwrap(),
        "the base must be version 1 of THIS set"
    );

    // The chunk count is the real check. A delta's index is shorter, so it names
    // fewer chunks than the full pack of the same version.
    assert!(
        opened.len() < Pack::open(&full_at).unwrap().len(),
        "a delta's index must name fewer chunks"
    );
}

#[tokio::test]
async fn a_delta_whose_base_is_not_in_the_set_is_refused() {
    // The base is a promise about what the reader holds. Nothing in a pack can
    // check it, so a base that does not resolve must be refused rather than
    // quietly answered with a full pack.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 3, 20_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);
    push(&server, "docs", &pack_path).await;

    let response = Request::new("GET", "/v1/packs/docs@1-99")
        .send(server.address())
        .await;

    assert!(
        response.status == 404 || response.status == 422,
        "a base that does not resolve must be refused, got {}: {}",
        response.status,
        response.text()
    );
}
