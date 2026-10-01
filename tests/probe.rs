//! A probe: what a directory already holds of a version.
//!
//! The probe is the only thing standing between a client and a delta download, so
//! the tests are about the answers a delta would be built on being *right*:
//!
//! * a directory that is the version holds every chunk, and one that is not holds
//!   what it should and nothing more;
//! * a chunk placed at several places is held only if it is right at **all** of
//!   them — the failure this is the whole module about, because the alternative
//!   produces a delta that skips a chunk it needed and a tree that is silently
//!   wrong;
//! * "short" and "different" are told apart, because they send someone looking at
//!   two different things;
//! * entries the version does not record are reported and left alone;
//! * and the answer is worth having: one edited file in a large tree is one range
//!   request, not one per chunk.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use lepatch::{
    command::{Unheld, probe},
    metadata::Snapshot,
    pack::Pack,
    storage::{BlobId, BlobStore, Plan, StorageGet},
};
use tokio::io::AsyncReadExt;

/// A tree, backed up once, with the version and plan that describe it.
///
/// The tempdir is *taken* rather than created here, because it is the thing that
/// owns the source tree: a `Built` that dropped the directory holding the tree it
/// describes would be probing a path that no longer exists, and every test using
/// it would fail on that rather than on what it is about.
struct Built {
    dir: tempfile::TempDir,
    source: std::path::PathBuf,
    blob: std::path::PathBuf,
    id: BlobId,
    snapshot: Snapshot,
    plan: Plan,
}

/// Back up `source`, and read back the version and plan describing it.
async fn built(dir: tempfile::TempDir, source: std::path::PathBuf) -> Built {
    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let mut reader = store.get(&committed.snapshot).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    Built {
        dir,
        source,
        blob,
        id: committed.snapshot,
        snapshot,
        plan,
    }
}

impl Built {
    /// Every chunk the version needs.
    fn all_chunks(&self) -> HashSet<BlobId> {
        self.plan.iter().map(|c| c.id).collect()
    }

    /// A pack of this version.
    async fn pack(&self) -> Pack {
        let store = BlobStore::open_for_reading(&self.blob).await.unwrap();
        Pack::write(
            &store,
            &self.plan,
            &self.snapshot,
            &self.dir.path().join("v1.pack"),
        )
        .await
        .unwrap()
    }

    /// A directory holding this version, restored from the store.
    ///
    /// The situation a client is actually in: the tree on disk *is* the version
    /// before someone edits it.
    async fn client(&self) -> std::path::PathBuf {
        let client = self.dir.path().join("client");
        common::run_restore(&self.blob, self.id, &client).await;
        client
    }

    /// Probe `root` against this version, reading chunks from the store.
    async fn probe(&self, root: &Path) -> lepatch::command::Probed {
        let store = BlobStore::open_for_reading(&self.blob).await.unwrap();
        probe(root, &self.snapshot, store).await.unwrap()
    }

    /// Probe `root` against this version, reading chunks from `pack`.
    ///
    /// The path a real client takes: everything it needs is in the one file.
    async fn probe_with_pack(&self, root: &Path, pack: &Pack) -> lepatch::command::Probed {
        probe(root, &self.snapshot, pack.clone()).await.unwrap()
    }
}

/// A source tree of `files` distinct files, and a backup of it.
async fn tree(files: usize, size: usize) -> Built {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    for i in 0..files {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap_or(u8::MAX), size + i * 1_000),
        )
        .unwrap();
    }

    built(dir, source).await
}

/// A directory that is the version, because it is where the version came from.
#[tokio::test]
async fn the_tree_a_version_was_taken_from_holds_all_of_it() {
    let built = tree(4, 120_000).await;

    let probed = built.probe(&built.source).await;

    assert!(
        probed.is_current(),
        "the tree a version was taken from should hold it, missing: {:?}",
        probed.mismatched
    );
    assert_eq!(probed.held, built.all_chunks());
    assert!(probed.extra.is_empty(), "nothing should be unrecorded");
}

/// Nothing is held by a directory with nothing in it.
#[tokio::test]
async fn an_empty_directory_holds_nothing() {
    let built = tree(3, 80_000).await;
    let empty = built.dir.path().join("empty");
    fs::create_dir_all(&empty).unwrap();

    let probed = built.probe(&empty).await;

    assert!(probed.held.is_empty(), "an empty tree holds no chunks");
    assert!(
        probed.mismatched.iter().all(|m| m.reason == Unheld::Absent),
        "every slice of a missing file is absent, not merely different"
    );
}

/// One edited file costs one chunk, not the tree.
///
/// This is the property the probe exists for: the answer has to be *fine-grained*,
/// because a probe that reported a tree as matching or not would be useless for
/// the one thing it is for.
#[tokio::test]
async fn one_edited_file_leaves_the_rest_held() {
    let built = tree(6, 150_000).await;
    let all = built.all_chunks();

    // A one-byte edit in the middle of the second file. Boundaries are
    // content-defined, so this condemns the chunks spanning that point and leaves
    // the rest of the file — and every other file — intact.
    let path = built.source.join("f2.bin");
    let mut bytes = fs::read(&path).unwrap();
    let at = bytes.len() / 2;
    bytes[at] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    let probed = built.probe(&built.source).await;

    assert!(!probed.is_current());
    assert!(
        probed.held.len() > all.len() / 2,
        "a one-byte edit in one of six files should not condemn most of the tree, \
         held {} of {}",
        probed.held.len(),
        all.len()
    );
    assert!(
        probed.held.len() < all.len(),
        "every chunk should not be held, since a file was edited"
    );
    assert!(
        probed
            .mismatched
            .iter()
            .all(|m| m.reason == Unheld::Content),
        "an edit is different content, not a missing file"
    );
}

/// A chunk placed in two places is held only if it is right in both.
///
/// This is the failure the whole design turns on. `held` is what
/// [`Pack::needed`] consults to decide a chunk needs no range request, so a
/// chunk recorded as held on the strength of one correct slice out of two is
/// skipped by the fetch — and the other slice is left wrong. The client would
/// believe it had applied the version.
///
/// So the rule is *every* slice or nothing, and a probe that answered "is this
/// chunk right somewhere" would look like an optimisation while being a silent
/// corruption.
#[tokio::test]
async fn a_chunk_in_two_places_is_held_only_if_it_is_right_in_both() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Two files of identical content, so the version places every one of their
    // chunks at two slices.
    let content = common::untagged_stream(9, 200_000);
    fs::write(source.join("a.bin"), &content).unwrap();
    fs::write(source.join("b.bin"), &content).unwrap();

    let built = built(dir, source).await;

    // The fixture has to be what the test needs, or the test proves nothing.
    let mut placements: HashMap<_, usize> = HashMap::new();
    for slice in &built.snapshot.file_chunks {
        *placements.entry(slice.chunk_index).or_default() += 1;
    }
    assert!(
        placements.values().any(|n| *n > 1),
        "two identical files should share chunks across two slices"
    );

    let shared = placements
        .iter()
        .find(|(_, count)| **count > 1)
        .map(|(index, _)| built.snapshot.chunks[*index as usize])
        .expect("a shared chunk");

    // The tree as taken holds them.
    assert!(built.probe(&built.source).await.held.contains(&shared));

    // Now corrupt one byte of *one* of the two files — at the first offset that
    // shared chunk is placed at, so that slice is the one that breaks.
    let victim = built.source.join("a.bin");
    let mut bytes = fs::read(&victim).unwrap();
    let offset = built
        .snapshot
        .file_chunks
        .iter()
        .find(|s| built.snapshot.chunks[s.chunk_index as usize] == shared)
        .expect("a slice placing the shared chunk")
        .file_offset;
    bytes[usize::try_from(offset).unwrap()] ^= 0xff;
    fs::write(&victim, &bytes).unwrap();

    let probed = built.probe(&built.source).await;

    assert!(
        !probed.held.contains(&shared),
        "a chunk right in one place and wrong in another must not be held, or the \
         fetch would skip it and leave the file wrong"
    );
    assert!(
        probed
            .mismatched
            .iter()
            .any(|m| m.expected == shared && m.path == Path::new("a.bin")),
        "the broken slice should be reported against the file it is in"
    );
}

/// A slice that runs off the end of one file into the next still verifies.
///
/// A `FileChunk` records a `chunk_offset`, so a slice is usually only *part* of a
/// chunk — the last chunk of a file runs on into the next one. The bytes in the
/// file are then not the whole of the chunk, so hashing them produces something
/// that is never equal to the chunk's hash. A probe that compared a file slice's
/// hash to the chunk's hash would condemn every boundary-crossing slice in every
/// tree, forever, and report a target that is byte-for-byte correct as holding
/// nothing.
///
/// So the two sides are compared as *bytes*, and this is the fixture that says so:
/// two files, with a chunk spanning the join, reported as held.
#[tokio::test]
async fn a_chunk_spanning_two_files_verifies_in_both() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Two small files, so the join between them lands inside a chunk rather than
    // on a boundary of its own.
    fs::write(source.join("a.bin"), common::untagged_stream(1, 40_000)).unwrap();
    fs::write(source.join("b.bin"), common::untagged_stream(2, 40_000)).unwrap();

    let built = built(dir, source).await;

    let spanning = built
        .snapshot
        .file_chunks
        .iter()
        .any(|s| s.chunk_offset > 0 || s.file_offset > 0);
    assert!(spanning, "two files should share a chunk across the join");

    let probed = built.probe(&built.source).await;

    assert!(
        probed.is_current(),
        "a chunk crossing a file boundary is still a chunk, and the tree that \
         holds it holds the version: {:?}",
        probed.mismatched
    );
    assert_eq!(probed.held, built.all_chunks());
}

/// Short and different are told apart, because they mean different things.
///
/// "This file is truncated" sends someone looking at the disk. "These bytes are
/// not what the version says" sends them looking at the version. Reporting both as
/// one finding makes them do both.
#[tokio::test]
async fn a_truncated_file_is_short_rather_than_different() {
    let built = tree(2, 150_000).await;

    let path = built.source.join("f0.bin");
    let bytes = fs::read(&path).unwrap();
    let cut = u64::try_from(bytes.len() / 2).unwrap();
    fs::write(&path, &bytes[..usize::try_from(cut).unwrap()]).unwrap();

    let probed = built.probe(&built.source).await;

    let short = probed
        .mismatched
        .iter()
        .filter(|m| m.reason == Unheld::Short)
        .count();
    assert!(
        short > 0,
        "a file cut in half should report slices past its end as short, got {:?}",
        probed
            .mismatched
            .iter()
            .map(|m| m.reason)
            .collect::<Vec<_>>()
    );
    assert!(
        probed
            .mismatched
            .iter()
            .filter(|m| m.path == Path::new("f0.bin"))
            .all(|m| m.reason == Unheld::Short),
        "every slice past a truncated file's end is short, not merely different"
    );

    // Every chunk the cut-off slices were holding is condemned — including the one
    // running off the end of the file into the next, which is why the assertion is
    // about the cut rather than about the tree: the chunks wholly inside `f1.bin`
    // are untouched and still held.
    let condemned: HashSet<BlobId> = built
        .snapshot
        .file_chunks
        .iter()
        .filter(|s| {
            s.file_offset >= cut
                && built.snapshot.files[s.file_index as usize].path == Path::new("f0.bin")
        })
        .map(|s| built.snapshot.chunks[s.chunk_index as usize])
        .collect();

    assert!(
        !condemned.is_empty(),
        "the fixture should have slices past the cut"
    );
    for chunk in &condemned {
        assert!(
            !probed.held.contains(chunk),
            "chunk {chunk} starts past the cut and cannot be held"
        );
    }
}

/// Entries the version does not record are reported, and left exactly where they
/// are.
///
/// A file here is as likely to be the user's own work as a leftover from an older
/// version. A probe that removed them would be destroying data it cannot tell
/// apart from data it should keep — so it names them and the caller decides.
#[tokio::test]
async fn an_unrecorded_entry_is_reported_and_not_removed() {
    let built = tree(3, 100_000).await;
    let stray = built.source.join("notes.txt");
    fs::write(&stray, b"mine, not yours").unwrap();

    let before = common::describe(&built.source);
    let probed = built.probe(&built.source).await;

    assert_eq!(
        probed.extra,
        vec![Path::new("notes.txt")],
        "the stray should be reported"
    );
    assert_eq!(
        common::describe(&built.source),
        before,
        "a probe reads; it must not change what it read"
    );
}

/// The payoff: what the probe saves is what the pack does not have to send.
///
/// One edited file in a six-file tree, asked for as ranges, is one request and a
/// fraction of the payload. This is the whole argument for probing rather than
/// re-sending, and it is the test that ties the two halves together.
#[tokio::test]
async fn what_a_probe_finds_held_is_what_the_pack_does_not_send() {
    let built = tree(6, 200_000).await;
    let pack = built.pack().await;

    // Start from a copy holding the version, then edit one file — the situation a
    // client is actually in.
    let client = built.client().await;

    let all = pack.needed_bytes(&HashSet::new());
    assert_eq!(
        all,
        pack.payload_len(),
        "a client holding nothing needs the whole payload"
    );

    let path = client.join("f3.bin");
    let mut bytes = fs::read(&path).unwrap();
    let at = bytes.len() / 3;
    bytes[at..at + 64].copy_from_slice(&common::untagged_stream(200, 64));
    fs::write(&path, &bytes).unwrap();

    // Probed against the pack, not the set: a client updating from one does not
    // have the other, so the answer it gets must be the same either way.
    let from_pack = built.probe_with_pack(&client, &pack).await;
    let from_store = built.probe(&client).await;
    assert_eq!(
        from_pack.held, from_store.held,
        "a pack and a store hold the same chunks, so they must agree on which"
    );

    let probed = from_pack;
    assert!(!probed.is_current());

    let needed = pack.needed(&probed.held);
    assert!(
        needed.len() < 12,
        "a one-file edit should need a handful of ranges, not one per chunk, got {}",
        needed.len()
    );
    assert!(
        pack.needed_bytes(&probed.held) < pack.payload_len() / 2,
        "a one-file edit should need well under half the payload, needs {} of {}",
        pack.needed_bytes(&probed.held),
        pack.payload_len()
    );
}

/// The slices reported are the ones the ranges cover, so a fetch can be planned
/// from the report alone.
#[tokio::test]
async fn the_ranges_a_probe_implies_cover_exactly_what_it_says_is_missing() {
    let built = tree(4, 180_000).await;
    let pack = built.pack().await;

    let client = built.client().await;

    // Truncate one file: several slices go short, and none of them is a whole
    // chunk's worth of correct data anywhere else.
    let path = client.join("f1.bin");
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() / 3]).unwrap();

    let probed = built.probe(&client).await;
    let ranges = pack.needed(&probed.held);
    let fetched = pack.chunks_covering(&ranges);

    for chunk in &pack {
        assert_eq!(
            fetched.contains(&chunk.id),
            !probed.held.contains(&chunk.id),
            "chunk {} is {} but the probe {}",
            chunk.id,
            if fetched.contains(&chunk.id) {
                "fetched"
            } else {
                "skipped"
            },
            if probed.held.contains(&chunk.id) {
                "holds"
            } else {
                "lacks"
            },
        );
    }
}
