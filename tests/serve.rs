// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! `blu serve` over HTTP: the whole push and pull flows, end to end.
//!
//! The two directions a backup has over a network, driven through a real socket
//! to a real process:
//!
//! * **push** — a client backs a tree up locally, packs it, uploads the pack, and
//!   the server merges it into a set it keeps. The set then holds the version.
//! * **pull** — the server packs a version out of a set, and a client fetches only
//!   the chunks it lacks, assembles a file from the byte ranges it is given, and
//!   `Pack::open`s it. The file is byte-identical to one `blu pack` would
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
    future::Future,
    io::{Seek, SeekFrom, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use blulib::{
    command::report as report_history,
    metadata::Timestamp,
    pack::Pack,
    storage::{BlobStore, Compression, Plan, StorageGet},
};
use common::{http::Request, server::Server};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const PACK_MEDIA_TYPE: &str = "application/x-blu-pack";

/// A pack big enough that merging it outlasts the reporting interval.
///
/// ## Why this fixture is not `tree`
///
/// `a_job_can_be_watched_over_sse` sets the rule a test about progress has to
/// keep: a fixture small enough that the merge finishes before a socket connect
/// completes tests the wiring and not the timing. **Its own 32 MB tree is not big
/// enough for that**, and it is worth saying why rather than just growing the
/// number, because what a merge spends its time on is what *survives
/// deduplication*, not what is on disk. `tree` hands out `common::noisy`, a pure
/// function of position, so file `i` is a *prefix* of file `i + 1` and the store
/// keeps only the longest: its 32 MB tree deduplicates to 8 MB of chunks, and a
/// merge of that measures 128 ms against a 300 ms reporting interval — so exactly
/// one line, whatever the machine.
///
/// So the bytes here are four **windows of one long stream**, cut at disjoint
/// offsets: no window is a prefix of another, none of them repeats, and
/// deduplication has nothing to collapse. 48 MB of distinct content, which
/// measures several reporting intervals to merge. `common::untagged_stream`
/// looks like the right tool and is not — it splices one 64-byte block over the
/// second half, so past those 64 bytes every file is the same bytes again, and a
/// 128 MB tree made of them merges 17 MB.
///
/// The first tick is why none of this has to be lucky: `Progress::due` returns
/// true until a line has been emitted, so a merge of *any* size reports once, the
/// moment the chunk loop opens. The size is what makes the rest of it — a second
/// tick, and a run still going when a stream's live half is read — real rather
/// than asserted about.
async fn slow_pack(dir: &Path) -> PathBuf {
    const FILE_BYTES: usize = 12 << 20;

    let source = common::source(dir);
    for (i, window) in common::noisy(FILE_BYTES * 4).chunks(FILE_BYTES).enumerate() {
        fs::write(source.join(format!("f{i}.bin")), window).unwrap();
    }

    let blob = dir.join("local");
    backup(&source, &blob).await;

    let pack_path = dir.join("local.pack");
    pack(&blob, &pack_path).await;

    pack_path
}

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

/// A source tree a compressor can shrink, for a test about *which* level wrote it.
///
/// **`tree` is the wrong fixture here, and deliberately so.** `common::noisy` is
/// BLAKE3 output, which has no structure for a compressor to find, so a frame at
/// any level comes back larger than the input and the store's own fallback stores
/// the bytes raw. Every level would then produce the same set, and a test built
/// on it would pass against a server that ignored the level entirely — which is
/// the bug.
///
/// So the content is text. Words from a small vocabulary, in a deterministic order
/// that differs per file, so nothing deduplicates away and every level has
/// something real to disagree about.
fn compressible_tree(dir: &Path, files: usize, size: usize) -> PathBuf {
    const WORDS: [&str; 12] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima",
    ];

    let source = common::source(dir);

    for file in 0..files {
        // A splitmix64 finaliser, so the words are the same on every run without
        // being the same file. Starting from the file's number is what keeps
        // file `i` from being a prefix of file `i + 1`.
        let mut state = u64::try_from(file).unwrap_or(0).wrapping_add(1);
        let mut text = String::with_capacity(size * 2);

        while text.len() < size {
            state = state.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            state ^= state >> 30;
            state = state.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            state ^= state >> 27;

            let word = usize::try_from(state >> 33).unwrap_or(0) % WORDS.len();
            text.push_str(WORDS[word]);
            text.push(' ');
        }

        fs::write(source.join(format!("f{file}.txt")), text).unwrap();
    }

    source
}

/// Back a tree up into a set at `blob`, and return the set.
async fn backup(source: &Path, blob: &Path) -> blulib::storage::BlobId {
    let storage = BlobStore::open_for_writing(
        blob,
        blulib::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();

    let latest: Option<Timestamp> = match storage.latest() {
        None => None,
        Some((version, _)) => storage.version_time(version).await.ok(),
    };

    blulib::command::backup(source, blulib::command::Options::default(), storage, latest)
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
    let snapshot = blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();

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

    // The `202` is a **handle**, not the job: `Accepted.required` is `job` and
    // `Accepted` has no `kind`, no `state`, and no `progress`. The document used
    // to type this body as `Job`, whose `required` list is eight fields, so a
    // generated client read a handle and got a type error. The keys are asserted
    // exactly rather than by containment, because a field added to the handler
    // without a schema is the same bug wearing a different hat.
    let accepted_body: serde_json::Value = serde_json::from_str(&accepted.text()).unwrap();
    let keys: Vec<&String> = accepted_body.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        vec!["job"],
        "a 202 carries a job id and nothing else; the `Accepted` schema says so"
    );

    let job = accepted.field("job").unwrap().as_str().unwrap().to_string();
    await_job(server, &job).await
}

/// How long [`await_job`] waits for a job to reach a terminal state.
///
/// **The longest wait in the suite, on the biggest fixture.** The merge this
/// polls is a ~34 MB pack, and a slow machine must not turn a slow run into a
/// failed one — so this is a budget rather than a poll count.
const JOB_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Poll a job until it finishes, and return its `result`.
///
/// **A deadline, not an iteration count.** `400` rounds of `5 ms` is 2 s of
/// sleeping *plus* 400 HTTP round-trips, so the real budget grew with
/// per-request latency on a loaded machine — and this is the tightest clock in
/// the suite, because its fixture is the big one. So the budget is the number in
/// the source, and a failing run says how long it waited rather than how many
/// times it asked.
async fn await_job(server: &Server, job: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + JOB_BUDGET;

    loop {
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
            _ => {}
        }

        assert!(
            std::time::Instant::now() < deadline,
            "job {job} did not finish within {}s",
            JOB_BUDGET.as_secs()
        );

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Upload a pack and merge it into `set`, returning the merge's result.
async fn push(server: &Server, set: &str, pack_path: &Path) -> serde_json::Value {
    push_with(server, set, pack_path, None).await
}

/// Upload a pack and merge it into `set` under `compression`.
///
/// `None` leaves the field out, which is the server's own default rather than a
/// spelling of it, so a test that is not about the flag does not pin one.
async fn push_with(
    server: &Server,
    set: &str,
    pack_path: &Path,
    compression: Option<&str>,
) -> serde_json::Value {
    let job = start_merge(server, set, pack_path, compression).await;
    await_job(server, &job).await
}

/// Upload a pack and start a merge into `set`, and return the merge's job id.
///
/// [`push_with`] waits for the merge. This does not, because a test about two
/// streams at once has to open both of them while both merges are still running.
async fn start_merge(
    server: &Server,
    set: &str,
    pack_path: &Path,
    compression: Option<&str>,
) -> String {
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

    let mut body = serde_json::json!({ "upload": upload });
    if let Some(compression) = compression {
        body["compression"] = serde_json::json!(compression);
    }

    let accepted = Request::json("POST", &format!("/v1/sets/{set}/merges"), &body)
        .send(server.address())
        .await;
    assert_eq!(accepted.status, 202, "body: {}", accepted.text());

    accepted.field("job").unwrap().as_str().unwrap().to_string()
}

/// A pack of a fresh tree, in its own subdirectory of `dir`.
///
/// **Two calls give two versions.** A second push of the *same* pack merges as
/// `already` and publishes nothing, and a test about what a listing omits needs
/// a merge that really adds a version.
async fn pack_of(dir: &Path, tag: &str, files: usize) -> PathBuf {
    let source = tree(&dir.join(tag), files, 20_000);
    let blob = dir.join(format!("{tag}-local"));
    backup(&source, &blob).await;

    let at = dir.join(format!("{tag}.pack"));
    pack(&blob, &at).await;
    at
}

/// The version numbers a listing returns, oldest first.
///
/// Parsed out of the wire shape rather than the body as a string, so a listing
/// that drops a version fails as a missing number and not as a formatting
/// difference nobody was looking for.
async fn listed_versions(server: &Server, spelling: &str) -> Vec<u64> {
    let listed = Request::new("GET", &format!("/v1/sets/{spelling}/versions"))
        .send(server.address())
        .await;

    assert_eq!(listed.status, 200, "body: {}", listed.text());

    let parsed: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
    parsed["versions"]
        .as_array()
        .unwrap_or_else(|| panic!("no versions in {}", listed.text()))
        .iter()
        .map(|entry| entry["version"].as_u64().unwrap())
        .collect()
}

/// How long the replay read waits for bytes before calling the stream quiet.
///
/// The replay is written when the response starts, so the stream goes quiet
/// immediately and this only has to outlast a loopback round trip. It is short
/// because it is *added* to the time a job has left to run: the longer the read
/// waits, the longer the merge must be for the live half to prove anything.
const REPLAY_QUIET: Duration = Duration::from_millis(25);

/// How long one read of the live half waits for bytes, and how long the live
/// half is read for in total.
const LIVE_STEP: Duration = Duration::from_millis(250);
const LIVE_FOR: Duration = Duration::from_secs(10);

/// One SSE stream, read in the two halves a live event is told apart by.
struct EventStream {
    /// The status line's number.
    status: u16,
    /// Everything the server had sent by the time the stream went quiet.
    replay: String,
    /// Everything that arrived after `finished` resolved.
    live: String,
}

/// Read `GET /v1/jobs/{job}/events`, split around `finished`.
///
/// **Why not [`Request::stream_until`].** It reads a stream in one pass, stopping
/// at a terminator it finds in the body. On a job that has already finished the
/// REPLAY history alone carries every frame a test could assert on, so the live
/// half — the part chained after the replay — is never distinguished from it, and
/// a server that replayed and then sent nothing would pass.
///
/// So the read is split at a moment the caller knows the job had not finished:
/// `replay` is what had arrived by then and `live` is what arrived after
/// `finished` resolved. **A frame in `live` cannot have been in the replay**, and
/// the caller checks that `replay` does not already hold the frame it looks for
/// in `live`.
///
/// # Panics
///
/// Panics if the connection fails or the response carries no status line. A
/// stream that cannot be opened has nothing to assert about.
async fn read_events<F>(address: SocketAddr, job: &str, finished: F) -> EventStream
where
    F: Future<Output = ()>,
{
    let mut stream = TcpStream::connect(address)
        .await
        .expect("the server should accept a connection");

    // No `connection: close`: this body is closed by the server when the job is
    // over, and closing the request would take the stream with it.
    let head = format!("GET /v1/jobs/{job}/events HTTP/1.1\r\nhost: {address}\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut raw = Vec::new();
    let mut chunk = vec![0u8; 1024];

    while !raw.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&chunk[..read]);
    }

    let Some(split) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
        return EventStream {
            status: 0,
            replay: String::new(),
            live: String::new(),
        };
    };

    let status = String::from_utf8_lossy(&raw[..split])
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);

    // The first half: everything the server had already sent. A job that is
    // still running has nothing live to say, so this read ends when the stream
    // goes quiet.
    let mut opened = raw[split + 4..].to_vec();
    while let Ok(Ok(read)) = tokio::time::timeout(REPLAY_QUIET, stream.read(&mut chunk)).await {
        if read == 0 {
            break;
        }
        opened.extend_from_slice(&chunk[..read]);
    }

    let replay = String::from_utf8_lossy(&opened).into_owned();

    // The second half: from here on, whatever arrives is live by construction.
    finished.await;

    let mut live = Vec::new();
    let until = Instant::now() + LIVE_FOR;
    while !String::from_utf8_lossy(&live).contains("event: result") && Instant::now() < until {
        let Ok(Ok(read)) = tokio::time::timeout(LIVE_STEP, stream.read(&mut chunk)).await else {
            break;
        };
        if read == 0 {
            break;
        }
        live.extend_from_slice(&chunk[..read]);
    }

    EventStream {
        status,
        replay,
        live: String::from_utf8_lossy(&live).into_owned(),
    }
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

    // **All three format numbers**, against the constants themselves.
    //
    // Only `pack_format` was checked before. The other two are three constants in
    // three modules with nothing tying any of them to the API — so a format bump
    // could ship a client that believed it could read a set it could not, and the
    // check that would have caught it was on the one field nobody bumped.
    assert_eq!(
        document["snapshot_format"],
        serde_json::json!(blulib::metadata::FORMAT_VERSION),
        "a client asks this to decide whether it can read a snapshot"
    );
    assert_eq!(
        document["record_format"],
        serde_json::json!(blulib::storage::FORMAT_VERSION),
        "a client asks this to decide whether it can read a store"
    );
    assert_eq!(
        document["pack_format"],
        serde_json::json!(blulib::pack::FORMAT_VERSION),
        "a client asks this to decide whether it can read a pack"
    );

    // And they must be in the document the server serves, so a client reading
    // the spec sees the same numbers the endpoint reports.
    let spec = Request::new("GET", "/openapi.json")
        .send(server.address())
        .await;
    assert_eq!(spec.status, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&spec.body).unwrap();
    for field in ["snapshot_format", "record_format", "pack_format"] {
        let properties = &parsed["components"]["schemas"]["ServerInfo"]["properties"];
        assert!(
            properties.get(field).is_some(),
            "the document describes no {field}, so a client reading the spec is not told it"
        );
    }
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
async fn a_merge_into_a_base_is_visible_through_its_segment_name() {
    // **One store, one key, and the harm is a missing version.**
    //
    // `docs.0000.bin` is a spelling of `docs` — `IndexSet` cuts the tail, and
    // the store's files are named from the base either way. The registry keyed
    // its read cache by the name as it arrived, so it held two entries for one
    // store: a listing through the segment spelling cached a handle, the merge
    // through the base evicted only the base's entry, and the next listing
    // through the segment spelling answered from a `contents` map built before
    // the commit. Its version *numbers* came fresh off the index files and the
    // new one would not resolve against that stale map — so it was **skipped**,
    // and the server reported a set that had just lost a version.
    //
    // The order matters and is the point: the segment spelling is read once
    // before the merge, so the cache entry the merge fails to drop really is
    // there.
    let dir = tempfile::tempdir().unwrap();
    let first = pack_of(dir.path(), "first", 4).await;
    let second = pack_of(dir.path(), "second", 5).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    assert_eq!(
        push(&server, "docs", &first).await["version"],
        serde_json::json!(1)
    );
    assert_eq!(listed_versions(&server, "docs.0000.bin").await, vec![1]);

    // The merge that publishes version 2, under the *base* while the cached
    // handle is under the segment's spelling of it.
    assert_eq!(
        push(&server, "docs", &second).await["version"],
        serde_json::json!(2)
    );

    assert_eq!(
        listed_versions(&server, "docs.0000.bin").await,
        vec![1, 2],
        "the version this merge published must be listed under both spellings"
    );
    assert_eq!(listed_versions(&server, "docs").await, vec![1, 2]);

    // And it resolves, which is the check the omission above hid behind: the
    // figures come through the store, so a version that cannot be resolved is
    // skipped rather than named.
    let info = Request::new("GET", "/v1/sets/docs.0000.bin/versions/2")
        .send(server.address())
        .await;
    assert_eq!(info.status, 200, "body: {}", info.text());
}

#[tokio::test]
async fn a_merge_into_a_segment_name_is_visible_through_its_base() {
    // The other direction, which fails for the same reason and would not be
    // caught by the test above: the eviction looked up the name the merge used
    // and the cache was keyed by the other spelling, so the entry the merge
    // needed to drop is not the entry it dropped.
    let dir = tempfile::tempdir().unwrap();
    let first = pack_of(dir.path(), "first", 4).await;
    let second = pack_of(dir.path(), "second", 5).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    assert_eq!(
        push(&server, "notes.0000.bin", &first).await["version"],
        serde_json::json!(1)
    );
    assert_eq!(listed_versions(&server, "notes").await, vec![1]);

    assert_eq!(
        push(&server, "notes.0000.bin", &second).await["version"],
        serde_json::json!(2)
    );

    assert_eq!(
        listed_versions(&server, "notes").await,
        vec![1, 2],
        "the version this merge published must be listed under both spellings"
    );
    assert_eq!(listed_versions(&server, "notes.0000.bin").await, vec![1, 2]);
}

#[tokio::test]
async fn the_set_listing_names_a_set_once_and_by_its_base() {
    // The registry agreeing with itself. `Sets::list` already deduplicated the
    // two spellings into one entry, and that entry is the *base* — so a name a
    // request may use is not always the name the listing prints, and a client
    // that took the listing's names as the only names that work would be right
    // about `docs` and wrong about `docs.0000.bin` in the only way that shows
    // up as a stale answer later.
    let dir = tempfile::tempdir().unwrap();
    let pack_path = pack_of(dir.path(), "first", 4).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    assert_eq!(
        push(&server, "docs.0000.bin", &pack_path).await["version"],
        serde_json::json!(1)
    );

    let listed = Request::new("GET", "/v1/sets").send(server.address()).await;
    assert_eq!(listed.status, 200, "body: {}", listed.text());

    let sets = serde_json::from_slice::<serde_json::Value>(&listed.body).unwrap();
    let names: Vec<&str> = sets["sets"]
        .as_array()
        .expect("a sets array")
        .iter()
        .map(|set| set["name"].as_str().unwrap())
        .collect();

    assert_eq!(
        names,
        vec!["docs"],
        "one set, named by its base: {listed:?}"
    );
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
    // with no `Range` header, and it must be the same file `blu pack` wrote —
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
    let snapshot = blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
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
    let snapshot = blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
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
    //
    // **Against a set that exists.** This used to ask `nosuch@1`, so the set was
    // resolved first and answered `404` before a hash was ever looked at: the
    // test passed on the refusal of something else, and deleting have-hash
    // validation outright left it green.
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

    let planned = Request::json(
        "POST",
        "/v1/packs/docs@1/plan",
        &serde_json::json!({ "have": ["not-a-hash"] }),
    )
    .send(server.address())
    .await;

    assert_eq!(planned.status, 400, "body: {}", planned.text());
    assert!(
        planned.text().contains("64 hexadecimal"),
        "the refusal must say what a chunk hash looks like: {}",
        planned.text()
    );

    // And a well-formed hash the set does not hold is **not** refused. It is a
    // chunk the client claims to hold and does not, so the plan has to cover the
    // whole file — which is what refusing everything would break.
    let claimed = Request::json(
        "POST",
        "/v1/packs/docs@1/plan",
        &serde_json::json!({ "have": ["00".repeat(32)] }),
    )
    .send(server.address())
    .await;

    assert_eq!(claimed.status, 200, "body: {}", claimed.text());
    let plan: serde_json::Value = serde_json::from_slice(&claimed.body).unwrap();
    assert_eq!(
        plan["bytes"], plan["length"],
        "a chunk the client holds that is not in the set cannot shorten the plan"
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
    let snapshot = blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
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
async fn a_pack_larger_than_the_payload_limit_uploads_and_merges() {
    // **A pack that is big on the wire.** `Uploads::put` used to take `web::Bytes`,
    // so actix collected the whole body before the handler ran and refused
    // anything past its default `PayloadConfig` limit of 256 KiB with a bare
    // `413`. Every other test in this file uploads about 20 KB, so the whole suite
    // agreed with the bug.
    //
    // `tree` hands out `noisy` bytes, which are a function of position: file `i` is
    // a prefix of file `i + 1`, so the store deduplicates all but the last of them
    // and a pack is a good deal smaller than the tree that made it. Sixty-four
    // files of 250 KB is 18 MB on disk and about 2 MB on the wire, and the
    // assertion below says so rather than leaving it to be worked out from the
    // numbers.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 64, 250_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let on_the_wire = fs::metadata(&pack_path).unwrap().len();
    assert!(
        on_the_wire > 4 * 256 * 1024,
        "this test is about a body over the payload limit, so it must not quietly \
         pack into something small: the pack is {on_the_wire} bytes"
    );

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // `push` asserts `204` on the `PUT`, which is the assertion that matters: a
    // `413` here is the bug, and it is not something a client can work around.
    let merged = push(&server, "big", &pack_path).await;

    assert_eq!(merged["already"], serde_json::json!(false));
    assert_eq!(merged["version"], serde_json::json!(1));
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
async fn the_level_a_client_names_is_the_level_the_records_are_written_at() {
    // **This is the test that says the flag is a flag.**
    //
    // `POST /v1/sets/{set}/merges` took a `compression` of `"1"` or `"9"`,
    // range-checked it against the range its own 400 named, and then opened the
    // set under `Compression::default()`. The number reached nothing: `--compression
    // 1` and `--compression 9` were the same run, and `blu merge` on this
    // server said so without anybody noticing.
    //
    // Refusing the number was the other option, and it is not the right one
    // here. `blu merge --compression N` opens the receiving set at `N`
    // locally, the pack endpoint's refusal exists because a re-encode cannot be
    // *served from* a set's bytes rather than because a level is meaningless,
    // and this endpoint already has the set in hand. So the level is honoured:
    // it is threaded into the policy the set is opened with.
    //
    // **Asserted on the bytes on disk, never on the absence of an error.** A
    // server that ignored the level answers every one of these three requests
    // with a `202`, a `succeeded` job and the same reported figures. Only the
    // sets differ.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();

    let source = compressible_tree(dir.path(), 32, 64_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let server = Server::start(&root);

    // The same pack into three sets, one per level. `0` is the level that means
    // "store these bytes as they are", so it is the level whose effect cannot be
    // confused with anything else.
    let mut merged = Vec::new();

    for (level, set) in [("0", "raw"), ("1", "fast"), ("9", "small")] {
        let result = push_with(&server, set, &pack_path, Some(level)).await;

        assert_eq!(result["already"], serde_json::json!(false));
        merged.push((level, result, common::set_size(&root.join(set))));
    }

    let size_of = |level: &str| {
        merged
            .iter()
            .find_map(|(asked, _, bytes)| (*asked == level).then_some(*bytes))
    };
    let reported_of = |level: &str| {
        merged.iter().find_map(|(asked, result, _)| {
            (*asked == level).then(|| (result["chunks"].clone(), result["bytes"].clone()))
        })
    };

    let (raw, fast, small) = (
        size_of("0").expect("level 0 was merged"),
        size_of("1").expect("level 1 was merged"),
        size_of("9").expect("level 9 was merged"),
    );

    assert!(
        raw > fast,
        "`compression: 0` stores payloads as they are and must be the largest set. \
         It came out at {raw} bytes against {fast} for `compression: 1`, so the level \
         was not opened with."
    );
    assert!(
        small < fast,
        "`compression: 9` is zstd at its smallest, and must beat level 1 on the same \
         pack. It came out at {small} bytes against {fast}, so the level was not opened \
         with."
    );

    // The figures a merge reports are content bytes, and are the same under every
    // level. A run that got smaller on disk by storing *less* would have moved
    // these, so this is what says the two sets hold the same version.
    assert_eq!(
        reported_of("0"),
        reported_of("1"),
        "the level changes the encoding of a merge, not what the version holds"
    );
    assert_eq!(reported_of("1"), reported_of("9"));
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
    // **The stream, end to end, and a live event proved rather than assumed.**
    //
    // `Request::send` reads to end-of-stream, which for an SSE body means "until
    // the job is over" — so it would hand back the frames in a lump and prove
    // nothing about the stream. This reads it in two halves with [`read_events`],
    // around a moment the test knows the job was still running.
    //
    // The fixture is large for that reason. A merge of a few hundred kilobytes is
    // over before a socket connect completes, and a stream opened after the job
    // has finished replays everything: the replay alone then satisfies any
    // assertion about the body, and the live half of `handlers.rs` — the part
    // chained after the replay — is never exercised.
    let dir = tempfile::tempdir().unwrap();
    let source = tree(dir.path(), 4, 8_000_000);
    let blob = dir.path().join("local");
    backup(&source, &blob).await;
    let pack_path = dir.path().join("local.pack");
    pack(&blob, &pack_path).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    // Accept the merge, then read the stream while it is still running.
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

    let stream = read_events(server.address(), &job, async {
        await_job(&server, &job).await;
    })
    .await;

    assert_eq!(stream.status, 200);

    // The replay: what the server had already sent, which for a running job is
    // the one state transition that has happened.
    assert!(
        stream.replay.contains("event: state"),
        "the stream must replay the state transitions: {}",
        stream.replay
    );
    assert!(
        !stream.replay.contains("event: result"),
        "the job had already finished when the stream opened, so nothing here proves a live \
         event is delivered — make the fixture larger: {}",
        stream.replay
    );

    // The live half. `result` is broadcast when the job finishes, which the split
    // above establishes had not happened when this stream was opened — so these
    // bytes crossed the wire after the response started, rather than out of the
    // replay. **Deleting the live half of `job_events` empties this.**
    assert!(
        stream.live.contains("event: result"),
        "a live event must arrive on the stream: {}",
        stream.live
    );
    assert!(
        stream.live.contains("event: state") && stream.live.contains("succeeded"),
        "the terminal state must be delivered live: {}",
        stream.live
    );

    // And a client that connects after the job is over is told the whole of it in
    // one pass, which is what the replay is for.
    let late = Request::new("GET", &format!("/v1/jobs/{job}/events"))
        .stream_until(server.address(), 8192, "succeeded")
        .await;

    assert_eq!(late.status, 200);
    let text = String::from_utf8_lossy(&late.body).into_owned();

    // The three event names are part of the contract, not a detail: a client
    // switches on them rather than parsing the payload to find out what it is.
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
async fn two_concurrent_merges_streams_carry_only_their_own_result() {
    /// One window of the second merge's fixture.
    const WINDOW: usize = 4 << 20;

    // **One stream per job, end to end.** Both jobs broadcast on one channel, so
    // a subscriber to the first merge was sent the second merge's events, and a
    // `JobEvent` carries no job id, so the consumer had nothing to filter on.
    // A merge result names its set, so the leak is visible in the body.
    //
    // The split waits for both merges. A live event that crosses from the second
    // job is therefore certain to arrive, rather than being a race against the
    // second merge finishing first.
    let dir = tempfile::tempdir().unwrap();
    let alpha_pack = pack_of(dir.path(), "alpha", 2).await;

    // **Big enough that the second merge is still running when the first stream
    // opens**, because that overlap is the whole defect. Four windows of one
    // long stream, cut at disjoint offsets, so none is a prefix of another and
    // deduplication has nothing to collapse. Smaller than [`slow_pack`], because
    // this test needs the merge to outlast a socket connect, not a reporting
    // interval.
    let beta_source = common::source(&dir.path().join("beta"));
    for (i, window) in common::noisy(4 * WINDOW).chunks(WINDOW).enumerate() {
        fs::write(beta_source.join(format!("f{i}.bin")), window).unwrap();
    }
    let beta_blob = dir.path().join("beta-local");
    backup(&beta_source, &beta_blob).await;
    let beta_pack = dir.path().join("beta.pack");
    pack(&beta_blob, &beta_pack).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start(&root);

    let alpha = start_merge(&server, "alpha", &alpha_pack, None).await;
    let beta = start_merge(&server, "beta", &beta_pack, None).await;

    let alpha_stream = read_events(server.address(), &alpha, async {
        let _ = await_job(&server, &alpha).await;
        let _ = await_job(&server, &beta).await;
    })
    .await;

    let beta_stream = read_events(server.address(), &beta, async {
        let _ = await_job(&server, &beta).await;
    })
    .await;

    assert_eq!(alpha_stream.status, 200);
    assert_eq!(beta_stream.status, 200);

    for (stream, own, foreign) in [
        (&alpha_stream, "alpha", "beta"),
        (&beta_stream, "beta", "alpha"),
    ] {
        let text = format!("{}{}", stream.replay, stream.live);

        assert!(
            text.contains(&format!("\"{own}\"")),
            "a stream must carry its own merge result: {text}"
        );
        assert!(
            !text.contains(&format!("\"{foreign}\"")),
            "a stream must not carry another job's events: {text}"
        );
    }
}

#[tokio::test]
async fn a_merge_reports_progress_to_a_client() {
    // **The producer side of the whole mechanism.** `progress_events_carry_the
    // counters_that_moved` proves the plumbing — that counters handed to a
    // `JobHandle` reach both a poller and a watcher. Nothing proved that any
    // *command* ever produces them, which is why a merge over HTTP could report
    // nothing at all and every other test would still pass. This drives a real
    // merge through the real server, which only means anything because
    // `src/command/merge.rs` builds a `Progress` of its own and lets the
    // `tracing` layer forward it. **The counters here are not put there by the
    // server.**
    let dir = tempfile::tempdir().unwrap();
    let pack_path = slow_pack(dir.path()).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    // **Not** `Server::start`, and the difference is now deliberate rather than
    // a workaround: `start_with_progress` pins the terminal all the way down
    // (`--quiet` *and* `RUST_LOG=warn`), so a client seeing progress here is
    // seeing it in spite of the verbosity rather than because of it.
    let server = Server::start_with_progress(&root);

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

    // Time the merge itself: from the `202` to the job's terminal state.
    let started = Instant::now();
    let result = read_events(server.address(), &job, async {
        await_job(&server, &job).await;
    })
    .await;
    let elapsed = started.elapsed();

    assert_eq!(result.status, 200);
    let carried = format!("{}{}", result.replay, result.live);

    assert!(
        carried.contains("event: progress"),
        "a merge must put a progress frame on the stream, and nothing fabricated one \
         server-side: {carried}"
    );

    // The two halves, so a failure says which one was empty. A slow merge that
    // outlasted the connect puts its first frame in the replay; the rest are live.
    eprintln!(
        "merge of a ~48 MB pack took {elapsed:?}, against a {REPORTING_INTERVAL:?} reporting \
         interval: progress frames replayed = {}, live = {}",
        result.replay.matches("event: progress").count(),
        result.live.matches("event: progress").count()
    );

    // Whether the *live* half carried one depends on the merge outlasting the
    // read above it, and that is the fixture's job rather than an assertion that
    // can be made honestly on every machine. So it is checked when the run was
    // long enough for it to mean anything, and the measurement is printed either
    // way.
    if elapsed > REPORTING_INTERVAL {
        assert!(
            result.live.contains("event: progress"),
            "the merge of a ~48 MB pack took {elapsed:?}, so it must still have been running \
             when the stream's live half was read: {carried}"
        );
    }

    // And the poller's view, which is the one a client that never opens a stream
    // reads. The job is over now; its figures are still its own.
    let document = Request::new("GET", &format!("/v1/jobs/{job}"))
        .send(server.address())
        .await;
    assert_eq!(document.status, 200, "body: {}", document.text());

    let progress = document
        .field("progress")
        .expect("a merge reports progress, so the job document carries it");
    let counters = progress
        .as_object()
        .expect("progress is an object of counters");
    assert!(
        !counters.is_empty(),
        "a merge that stored chunks must not report an empty progress object: {}",
        document.text()
    );

    // Every key is a counter this crate knows the name of. Anything else would be
    // a counter no `Counter` can produce, which is exactly the silent gap the
    // layer's own test guards against — checked here from the client side as
    // well, because this is the document a client is written against.
    let names = blulib::progress::counter_names();
    for key in counters.keys() {
        assert!(
            names.contains(&key.as_str()),
            "the job document reports {key:?}, which is not a counter name: {names:?}"
        );
    }

    // **And the figures are the merge's own.** `chunks` is set from the resolved
    // plan before the first chunk is written, so it is exactly the number of
    // chunks the version reads — and the job's `result` carries the same figure,
    // counted independently at the end of the run. Two counts that agree because
    // they are the same count of the same thing; two that disagree are a wiring
    // bug.
    let merged = await_job(&server, &job).await;
    assert_eq!(
        counters.get("chunks").and_then(serde_json::Value::as_u64),
        merged["chunks"].as_u64(),
        "the reported chunk total must be the one the merge committed: {merged}"
    );

    // `distinct` counts up as chunks are stored, and the last tick is rate limited
    // like any other, so it is bounded by the figure rather than equal to it.
    let distinct = counters
        .get("distinct")
        .and_then(serde_json::Value::as_u64)
        .expect("a merge stores chunks, so it reports distinct ones");
    let fresh = merged["fresh"]
        .as_u64()
        .expect("a commit reports what it stored");

    assert!(
        distinct <= fresh,
        "the number of chunks stored must not pass the figure the run ended on: {distinct} \
         against {fresh}"
    );

    // **Conditional on the only thing it depends on.** A merge that outlasted one
    // reporting interval has ticked at least twice, and the second tick carries a
    // stored-chunk count. A merge that finished inside the interval legitimately
    // reports one line and leaves `distinct` at zero, so asserting `> 0` there
    // would be asserting that the fixture is slow enough — which is what the
    // measurement above reports and what this check defers to.
    if elapsed > REPORTING_INTERVAL {
        assert!(
            distinct > 0,
            "a merge that took {elapsed:?} outlasted the {REPORTING_INTERVAL:?} reporting \
             interval and so must have moved a counter while it ran: {}",
            document.text()
        );
    }
}

#[tokio::test]
async fn a_quiet_server_still_reports_progress_to_a_client() {
    // **The defect, end to end through a real process.** `blu --quiet serve`
    // used to reach every client with an empty `progress` object: the terminal's
    // `EnvFilter` was attached to the subscriber rather than to the writer layer,
    // so it refused the `blulib::progress` *callsite* outright and the progress
    // layer above it was never offered the event. `RUST_LOG=info` restored it,
    // which is what made this look like a filter bug rather than one of
    // architecture.
    //
    // **`Server::start_with_progress` sets `RUST_LOG=warn`**, so both verbosity
    // knobs are turned down as far as they go and the operator decision is
    // tested rather than assumed: what a client sees over HTTP does not move when
    // the terminal does.
    //
    // **This is a real merge through a real socket**, not a fixture of counters.
    // A test that handed the job registry its own numbers would pass with the
    // whole filter arrangement wrong, since the broken part is precisely the
    // path a *command's* events take to reach the registry.
    let dir = tempfile::tempdir().unwrap();
    let pack_path = slow_pack(dir.path()).await;

    let root = dir.path().join("served");
    fs::create_dir_all(&root).unwrap();
    let server = Server::start_with_progress(&root);

    let merged = push(&server, "docs", &pack_path).await;
    assert!(
        merged
            .get("chunks")
            .and_then(serde_json::Value::as_u64)
            .is_some(),
        "the merge itself must have done something, or this test is asserting \\
         about an empty run: {merged}"
    );

    // The job id, from the listing: `push` waits for the merge and hands back the
    // result, and the counters live on the *job* rather than in the result.
    let listed = Request::new("GET", "/v1/jobs").send(server.address()).await;
    assert_eq!(listed.status, 200, "body: {}", listed.text());
    let job = listed
        .field("jobs")
        .and_then(|jobs| jobs.as_array().and_then(|jobs| jobs.first().cloned()))
        .and_then(|job| {
            job.get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| panic!("the merge must have left a job: {}", listed.text()));

    let document = Request::new("GET", &format!("/v1/jobs/{job}"))
        .send(server.address())
        .await;
    assert_eq!(document.status, 200, "body: {}", document.text());

    // **Non-empty, and made of real counter names.** The empty object is exactly
    // what the defect produced, so `!counters.is_empty()` is the assertion that
    // fails when the filter is put back on the subscriber. The name check is what
    // makes it a test of progress rather than of any old object: a server that
    // invented a key to look busy would pass an emptiness check.
    let progress = document
        .field("progress")
        .expect("a merge reports progress, so the job document carries it");
    let counters = progress
        .as_object()
        .expect("progress is an object of counters");
    assert!(
        !counters.is_empty(),
        "a merge that stored chunks must report progress even with the terminal \\
         silent, or every client is told the job reported nothing: {}",
        document.text()
    );

    let names = blulib::progress::counter_names();
    for key in counters.keys() {
        assert!(
            names.contains(&key.as_str()),
            "the job document reports {key:?}, which is not a counter name: {names:?}"
        );
    }

    // **And the figures are the merge's own**, checked against the count the run
    // committed to independently. Two counts of the same thing that disagree are a
    // wiring bug; two that agree are the wiring working.
    assert_eq!(
        counters.get("chunks").and_then(serde_json::Value::as_u64),
        merged["chunks"].as_u64(),
        "the reported chunk total must be the one the merge committed: {merged}"
    );
}

/// How often a run is expected to say something.
///
/// `progress::INTERVAL`, restated rather than imported: it is private, and a test
/// that cannot see it can only guess what "long enough to tick" means. The two
/// have to agree, so this is the number the crate documents its own cadence with.
const REPORTING_INTERVAL: Duration = Duration::from_millis(300);

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
        blulib::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();

    let merged = blulib::command::merge(
        &storage,
        &Pack::open(&fetched).unwrap(),
        blulib::command::MergeCompression::Keep,
    )
    .await
    .unwrap();

    let blulib::command::Merged::Committed { version, .. } = merged else {
        panic!("the mirror set had nothing, so this must be a commit");
    };
    assert_eq!(version, 1);

    // The mirror restores to the same tree.
    let restored = dir.path().join("restored");
    let store = BlobStore::open_for_reading(&mirror).await.unwrap();
    let snapshot = store.snapshot_via_index(1).unwrap();
    blulib::command::restore(&restored, snapshot, store)
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
    fs::create_dir_all(root.join(".blu-serve")).unwrap();

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
    let snapshot = blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
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
    let segment = blulib::layout::IndexSet::new(root.join("docs")).segment_path(0);
    let from = blulib::layout::IndexSet::new(&blob).segment_path(0);
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
    //
    // Pushed one after the other, in time order. The two packs ran at different
    // times, so pushing both at once would race the tip. The concurrent case is
    // the next test. This one holds the lock and the order fixed, so both
    // merges commit and the set holds both versions.
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

    for pack in [&one_pack, &two_pack] {
        let merged = push(&server, "shared", pack).await;
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

/// Two merges at once race the tip, and the set stays consistent either way.
///
/// The two packs ran at different times. Whichever merge commits second commits
/// against a tip newer than its own stamp when the arrival order inverts the
/// time order. That merge is refused by the stamp check. The refusal publishes
/// nothing. The set holds the versions that committed, and every one of them
/// opens and resolves.
#[tokio::test]
async fn concurrent_merges_race_the_tip_and_leave_a_consistent_set() {
    let dir = tempfile::tempdir().unwrap();

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
    let first = start_merge(&server, "shared", &one_pack, None).await;
    let second = start_merge(&server, "shared", &two_pack, None).await;
    let (first, second) = tokio::join!(settle_job(&server, &first), settle_job(&server, &second));

    // Each job either committed or lost the race. Any other error is a failure.
    let mut committed = 0;
    for (state, text) in [&first, &second] {
        match state.as_str() {
            "succeeded" => committed += 1,
            "failed" => assert!(
                text.contains("refusing to commit"),
                "a failed merge should have lost the race, got: {text}"
            ),
            other => panic!("a merge ended in an unexpected state: {other}: {text}"),
        }
    }
    assert!(
        committed >= 1,
        "at least the newer pack always commits: {first:?} {second:?}"
    );

    // The set holds what committed, and opens cleanly. Interleaved records
    // would leave a set that fails to open.
    let store = BlobStore::open_for_reading(root.join("shared"))
        .await
        .unwrap();
    assert_eq!(store.versions().unwrap().len(), committed);

    let listed = Request::new("GET", "/v1/sets/shared/versions")
        .send(server.address())
        .await;
    assert_eq!(listed.status, 200, "body: {}", listed.text());
    let body: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
    assert_eq!(
        body["versions"].as_array().map(Vec::len),
        Some(committed),
        "the listing should name what committed and nothing else"
    );
}

/// Poll a job until it finishes, and report how it ended.
///
/// Returns the state with the job's text. A terminal job that failed carries
/// its error in the text. A succeeded one carries its result there.
async fn settle_job(server: &Server, job: &str) -> (String, String) {
    let deadline = std::time::Instant::now() + JOB_BUDGET;

    loop {
        let response = Request::new("GET", &format!("/v1/jobs/{job}"))
            .send(server.address())
            .await;

        assert_eq!(response.status, 200, "body: {}", response.text());

        let state = response
            .field("state")
            .and_then(|value| value.as_str().map(str::to_string))
            .expect("a job document carries a state");

        match state.as_str() {
            "succeeded" | "failed" | "cancelled" => return (state, response.text()),
            _ => {}
        }

        assert!(
            std::time::Instant::now() < deadline,
            "job {job} did not finish within {}s",
            JOB_BUDGET.as_secs()
        );

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
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

    let scratch = root.join(".blu-serve");
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
    let snapshot = blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap();
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
        blulib::storage::SegmentLimit::default(),
        Compression::default(),
    )
    .await
    .unwrap();
    let latest: Option<Timestamp> = match storage.latest() {
        None => None,
        Some((version, _)) => storage.version_time(version).await.ok(),
    };
    blulib::command::backup(
        &source,
        blulib::command::Options::default(),
        storage,
        latest,
    )
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

#[tokio::test]
async fn progress_events_carry_the_counters_that_moved() {
    // The same mechanism, driven through the registry rather than through a job,
    // so it does not depend on a merge happening to be slow enough to tick.
    //
    // This is the assertion that would have failed before: a `JobHandle` given
    // counters must have them in the job document a poller reads AND on the event
    // stream a watcher reads, because those two are the same registry.
    let jobs = blulib::serve::Jobs::new();
    let id = jobs.spawn("pack", |handle| async move {
        handle.progress(&[("files", 3), ("written", 4096)]);
        Ok(blulib::serve::Json::object(vec![]))
    });

    for _ in 0..200 {
        if jobs
            .get(id)
            .is_some_and(|job| job.state == blulib::serve::State::Succeeded)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let job = jobs.get(id).unwrap();
    assert_eq!(
        job.progress,
        vec![("files".to_string(), 3), ("written".to_string(), 4096)],
        "a poller must see the counters"
    );

    // And the stream carried them, so a watcher and a poller agree.
    let replay = jobs
        .subscribe(id)
        .map(|(_, history)| history)
        .unwrap_or_default();
    assert!(
        replay
            .iter()
            .any(|event| matches!(event, blulib::serve::JobEvent::Progress(_))),
        "a watcher must see the counters too"
    );
}

/// The history endpoint answers what a local report does.
///
/// One set moves a file through created, modified and deleted. The wire
/// events carry the same versions, paths and statuses the library reports
/// over the same set, so a remote report prints what a local one prints.
#[tokio::test]
async fn history_endpoint_answers_same_events_as_local() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(&root).unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    fs::write(source.join("a.txt"), b"one\n").unwrap();
    backup(&source, &root.join("docs")).await;
    fs::write(source.join("a.txt"), b"two\n").unwrap();
    backup(&source, &root.join("docs")).await;
    fs::remove_file(source.join("a.txt")).unwrap();
    backup(&source, &root.join("docs")).await;

    let server = Server::start(&root);
    let response = Request::new("GET", "/v1/sets/docs/history?files=a.txt")
        .send(server.address())
        .await;
    assert_eq!(response.status, 200, "body: {}", response.text());
    assert!(
        response.is_type("application/json"),
        "a history answer is JSON: {}",
        response.text()
    );

    // The library's own report over the same set, for the same path.
    let store = BlobStore::open_for_reading(root.join("docs"))
        .await
        .unwrap();
    let versions = store.versions().unwrap();
    let mut named = Vec::new();
    for version in versions {
        named.push((version, store.snapshot(version).unwrap()));
    }
    let events = report_history(&store, &named, &[PathBuf::from("a.txt")])
        .await
        .unwrap();

    let parsed: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let wire = parsed["events"]
        .as_array()
        .unwrap_or_else(|| panic!("no events in {}", response.text()));
    assert_eq!(wire.len(), events.len(), "body: {}", response.text());
    for (got, want) in wire.iter().zip(&events) {
        assert_eq!(got["version"].as_u64().unwrap(), u64::from(want.version));
        assert_eq!(got["path"].as_str().unwrap(), want.path.to_str().unwrap());
        assert_eq!(got["status"].as_str().unwrap(), want.status.word());
    }
}

/// The history endpoint refuses a set it does not hold.
///
/// A missing set is a 404 carrying the server's own sentence, like every
/// other endpoint that names a set.
#[tokio::test]
async fn history_endpoint_refuses_unknown_set() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(&root).unwrap();

    let server = Server::start(&root);
    let response = Request::new("GET", "/v1/sets/nope/history?files=a.txt")
        .send(server.address())
        .await;

    assert_eq!(response.status, 404, "body: {}", response.text());
}

/// The history endpoint refuses a request with no files value.
///
/// Without a path the request is a listing, and the listing has its own
/// endpoint. The refusal says so.
#[tokio::test]
async fn history_endpoint_refuses_a_request_with_no_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(&root).unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    fs::write(source.join("a.txt"), b"one\n").unwrap();
    backup(&source, &root.join("docs")).await;

    let server = Server::start(&root);
    let response = Request::new("GET", "/v1/sets/docs/history")
        .send(server.address())
        .await;

    assert_eq!(response.status, 400, "body: {}", response.text());
    assert!(
        response.text().contains("at least one"),
        "the refusal names the mistake: {}",
        response.text()
    );
}

/// The history endpoint reads glob patterns and directory prefixes.
///
/// The same dialect the command line reads, answered from the same code. A
/// star stays within one level, and a directory matches everything under it.
#[tokio::test]
async fn history_endpoint_answers_globs_and_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir_all(&root).unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(source.join("sub")).unwrap();

    fs::write(source.join("top.txt"), b"top\n").unwrap();
    fs::write(source.join("sub").join("d.txt"), b"deep\n").unwrap();
    backup(&source, &root.join("docs")).await;

    let server = Server::start(&root);

    let starred = Request::new("GET", "/v1/sets/docs/history?files=*.txt")
        .send(server.address())
        .await;
    assert_eq!(starred.status, 200, "body: {}", starred.text());
    let parsed: serde_json::Value = serde_json::from_slice(&starred.body).unwrap();
    let paths: Vec<&str> = parsed["events"]
        .as_array()
        .unwrap_or_else(|| panic!("no events in {}", starred.text()))
        .iter()
        .map(|entry| entry["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["top.txt"]);

    let prefixed = Request::new("GET", "/v1/sets/docs/history?files=sub")
        .send(server.address())
        .await;
    assert_eq!(prefixed.status, 200, "body: {}", prefixed.text());
    let parsed: serde_json::Value = serde_json::from_slice(&prefixed.body).unwrap();
    let paths: Vec<&str> = parsed["events"]
        .as_array()
        .unwrap_or_else(|| panic!("no events in {}", prefixed.text()))
        .iter()
        .map(|entry| entry["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["sub", "sub/d.txt"]);
}
