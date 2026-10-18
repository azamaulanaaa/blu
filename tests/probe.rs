// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A probe: what a directory already holds of a version.
//!
//! The probe is the only thing standing between a client and a delta download,
//! so the tests are about the answers a delta would be built on being *right*:
//!
//! * a directory that is the version holds every chunk.
//! * a directory that is not the version holds only what it should.
//! * a chunk placed at several places is held only if it is right at **all** of
//!   them. The whole module is about that failure. The alternative produces a
//!   delta that skips a needed chunk. The alternative also produces a silently
//!   wrong tree.
//! * "short" and "different" are told apart. They send someone looking at two
//!   different things.
//! * entries the version does not record are reported and left alone.
//! * the answer saves work. One edited file in a large tree is one range request.
//!   It is not one request per chunk.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    path::Path,
};

use blulib::{
    command::{Mislength, Mismatch, Unheld, probe},
    metadata::Snapshot,
    pack::Pack,
    storage::{BlobId, BlobStore, Plan, StorageGet},
};
use tokio::io::AsyncReadExt;

/// A tree, backed up once, with the version and plan that describe it.
///
/// The tempdir is *taken* here rather than created here. The tempdir owns the
/// source tree. A `Built` that dropped the directory holding the tree it
/// describes would probe a path that no longer exists. Every test that uses it
/// would fail on the path. It would not fail on what the test is about.
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

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    let mut reader = store.get(&committed.snapshot()).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = Snapshot::try_from(buffer.as_slice()).unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    Built {
        dir,
        source,
        blob,
        id: committed.snapshot(),
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
        let store = BlobStore::builder(&self.blob).open_reader().await.unwrap();
        Pack::write(
            &store,
            &self.plan,
            &self.snapshot,
            1,
            &self.dir.path().join("v1.pack"),
        )
        .await
        .unwrap()
    }

    /// A directory holding this version, restored from the store.
    ///
    /// This is the situation a client is in. The tree on disk *is* the version
    /// before someone edits it.
    async fn client(&self) -> std::path::PathBuf {
        let client = self.dir.path().join("client");
        common::run_restore(&self.blob, self.id, &client).await;
        client
    }

    /// Probe `root` against this version, reading chunks from the store.
    async fn probe(&self, root: &Path) -> blulib::command::Probed {
        let store = BlobStore::builder(&self.blob).open_reader().await.unwrap();
        probe(root, &self.snapshot, store).await.unwrap()
    }

    /// Probe `root` with `established`, and count the chunks that were read.
    ///
    /// The count is the point. An accounted-for file is not merely left out of the
    /// report. The chunk that backs it must not be opened from the source at all.
    /// Otherwise the fast path saves nothing but arithmetic.
    async fn probe_skipping_counting(
        &self,
        root: &Path,
        established: &blulib::command::Established<'_>,
    ) -> (blulib::command::Probed, usize) {
        let store = BlobStore::builder(&self.blob).open_reader().await.unwrap();
        let counting = Counting {
            inner: store,
            reads: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let reads = std::sync::Arc::clone(&counting.reads);
        let probed = blulib::command::probe_skipping(root, &self.snapshot, counting, established)
            .await
            .unwrap();
        (probed, reads.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Every file path the version records.
    fn paths(&self) -> BTreeSet<&Path> {
        self.snapshot
            .files
            .iter()
            .map(|f| f.path.as_path())
            .collect()
    }

    /// Every file accounted for, so nothing is read at all.
    fn all_skipped(&self) -> blulib::command::Established<'_> {
        blulib::command::Established {
            files: self.paths(),
            ..Default::default()
        }
    }

    /// Every file accounted for but `keep`, which is then read.
    fn all_skipped_but(&self, keep: &Path) -> blulib::command::Established<'_> {
        blulib::command::Established {
            files: self
                .snapshot
                .files
                .iter()
                .map(|f| f.path.as_path())
                .filter(|p| *p != keep)
                .collect(),
            ..Default::default()
        }
    }

    /// Probe `root` against this version, reading chunks from `pack`.
    ///
    /// The path a real client takes: everything it needs is in the one file.
    async fn probe_with_pack(&self, root: &Path, pack: &Pack) -> blulib::command::Probed {
        probe(root, &self.snapshot, pack.clone()).await.unwrap()
    }
}

/// A chunk every one of whose files is accounted for is never read from the source.
///
/// The test asserts the saving. It does not describe the saving. Accounting for a
/// file must mean that the chunk which backs it is not fetched. Otherwise the fast
/// path costs the same read and saves only the comparison. A test that counted
/// findings instead of reads would pass either way.
#[tokio::test]
async fn a_chunk_whose_every_file_is_trusted_is_never_read() {
    let built = tree(4, 150_000).await;
    let client = built.client().await;

    let (probed, reads) = built
        .probe_skipping_counting(&client, &built.all_skipped())
        .await;

    assert_eq!(
        reads, 0,
        "accounting for every file should mean the source is never opened"
    );
    assert!(
        probed.mismatched.is_empty() && probed.mislengths.is_empty(),
        "and nothing should be reported missing"
    );
    assert_eq!(
        probed.held.len(),
        built.all_chunks().len(),
        "every chunk is held, because a skipped file is as good as one that was read"
    );
    assert!(probed.is_current(), "so the directory still is the version");
}

/// Skipping most files reads only the chunks the remaining ones need.
///
/// The partial case is the one that has to be right. A chunk that spans a skipped
/// and an unskipped file must still be read. One of its placements is unchecked.
/// Skipping it would report a chunk as held when half of it is not.
#[tokio::test]
async fn a_chunk_spanning_a_skipped_and_an_unskipped_file_is_still_read() {
    let built = tree(6, 200_000).await;
    let client = built.client().await;

    // The baseline: account for nothing and every chunk is read.
    let (_, every_chunk_read) = built
        .probe_skipping_counting(&client, &blulib::command::Established::default())
        .await;

    // Account for all of them, and nothing is read at all.
    let (nothing_read, no_chunk_read) = built
        .probe_skipping_counting(&client, &built.all_skipped())
        .await;
    assert_eq!(
        no_chunk_read, 0,
        "the baseline should read nothing when every file is accounted for"
    );

    // Now account for all but one, and count what reading that one costs.
    let first = built.snapshot.files[0].path.as_path();
    let (probed, one_file_read) = built
        .probe_skipping_counting(&client, &built.all_skipped_but(first))
        .await;

    assert!(
        one_file_read > 0,
        "an unaccounted file must have its chunks read"
    );
    assert!(
        one_file_read < every_chunk_read,
        "but fewer than reading the whole tree, which was {every_chunk_read}"
    );
    assert!(
        probed.is_current(),
        "and the answer must be unchanged: the tree still holds the version"
    );
    assert_eq!(
        probed.held, nothing_read.held,
        "the same chunks are held either way, which is what skipping a file asserts"
    );
}

/// An unskipped file is read and reported on, exactly as if nothing were skipped.
#[tokio::test]
async fn an_unskipped_file_is_checked_as_thoroughly_as_any_other() {
    let built = tree(4, 150_000).await;
    let client = built.client().await;

    // Break one file where a stat cannot see it.
    let victim = Path::new("f2.bin");
    let on_disk = client.join(victim);
    let before = fs::metadata(&on_disk).unwrap();
    let mut bytes = fs::read(&on_disk).unwrap();
    bytes[1_000] ^= 0xff;
    fs::write(&on_disk, &bytes).unwrap();
    filetime::set_file_mtime(
        &on_disk,
        filetime::FileTime::from_last_modification_time(&before),
    )
    .unwrap();

    // Account for everything *except* the victim. A caller with a fooled stat
    // passes this. Being fooled here costs nothing. The file is still read
    // properly.
    let (probed, _) = built
        .probe_skipping_counting(&client, &built.all_skipped_but(victim))
        .await;

    assert!(
        probed.mismatched.iter().any(|m| m.path.ends_with("f2.bin")),
        "the unaccounted file's difference must still be reported, got {:?}",
        probed.mismatched
    );
    assert!(
        !probed.is_current(),
        "so accounting for its neighbours does not make the directory current"
    );
}

/// A chunk the source cannot supply is held when the caller has accounted for it.
///
/// This is the delta pack's case. It is the only reason a delta can be applied at
/// all. A probe verifies a slice by reading the chunk. A chunk that the source does
/// not hold cannot be verified. A delta omits exactly those chunks. The probe never
/// asks for them. The probe reports them as held on the strength of the caller's
/// claim.
#[tokio::test]
async fn a_chunk_the_source_lacks_is_held_when_accounted_for() {
    let built = tree(4, 150_000).await;
    let client = built.client().await;
    let pack = built.pack().await;

    // Every chunk the version needs.
    let all = built.paths();

    // Two chunks the source cannot supply, as a delta pack's chunks cannot be.
    // The code takes them by reference out of the snapshot. The set borrows and
    // does not copy. `Established::chunks` holds a borrowing set.
    let missing: HashSet<&BlobId> = built.snapshot.chunks[..2].iter().collect();

    let established = blulib::command::Established {
        files: all,
        chunks: missing.iter().copied().collect(),
    };

    let (probed, reads) = built.probe_skipping_counting(&client, &established).await;

    // Files are accounted for, so nothing is read from the source at all.
    assert_eq!(reads, 0, "an accounted-for file is never read");
    assert!(
        probed.is_current(),
        "and the tree is the version, so nothing is reported"
    );
    assert!(
        !pack.is_empty(),
        "sanity: the pack has chunks, so this is not vacuous"
    );
    for id in &missing {
        assert!(
            probed.held.contains(*id),
            "a chunk accounted for but not read must be held, not merely absent"
        );
    }
}

/// A [`StorageGet`] that counts how many blobs it was asked to open.
///
/// This is the only way to tell that the trusted path saved a *read*. A comparison
/// alone does not show a saving. A probe that counted the mismatches correctly but
/// still fetched every chunk would pass every other test here. That probe would
/// save nothing.
struct Counting<S> {
    inner: S,
    reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<S: StorageGet> StorageGet for Counting<S> {
    fn get(
        &self,
        id: &BlobId,
    ) -> impl std::future::Future<Output = std::io::Result<blulib::storage::StreamReadSeeker>> + Send
    {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(id)
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
/// This is the property the probe exists for. The answer has to be
/// *fine-grained*. A probe that reports a whole tree as matching or not is useless
/// for the one thing it is for.
#[tokio::test]
async fn one_edited_file_leaves_the_rest_held() {
    let built = tree(6, 150_000).await;
    let all = built.all_chunks();

    // A one-byte edit in the middle of the second file. The boundaries come from
    // the content. The edit condemns the chunks that span that point. The rest of
    // the file stays intact. Every other file stays intact.
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
/// This is the failure the whole design turns on. [`Pack::needed`] consults
/// `held` to decide that a chunk needs no range request. A chunk that the report
/// records as held on the strength of one correct slice out of two is skipped by
/// the fetch. The other slice is left wrong. The client would believe it had
/// applied the version.
///
/// So the rule is *every* slice or nothing. A probe that answered "is this chunk
/// right somewhere" would only look like an optimisation. It would be a silent
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

    // Now corrupt one byte of *one* of the two files. Use the first offset where
    // the shared chunk is placed. That slice is the one that breaks.
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

/// A chunk placed many times is checked at every placement, from one buffer.
///
/// The check loop reads one chunk once, and then reads every slice that places
/// it. The loop keeps one slice buffer for those reads. A version of this
/// fixture places each of its chunks at many slices. Both the report of the
/// correct tree and the report of a tree with one broken placement must then be
/// exactly what the version says.
#[tokio::test]
async fn a_chunk_placed_many_times_is_reported_at_the_placement_that_broke() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // One block repeated. The chunker resynchronises on the repetition, so the
    // version places each of its chunks at every repetition.
    let block = common::untagged_stream(4, 128 * 1024);
    let mut bytes = Vec::with_capacity(block.len() * 32);
    for _ in 0..32 {
        bytes.extend_from_slice(&block);
    }
    fs::write(source.join("repeated.bin"), &bytes).unwrap();

    let built = built(dir, source).await;

    // The fixture has to be what the test needs, or the test proves nothing.
    let mut per_chunk: HashMap<u32, usize> = HashMap::new();
    for slice in &built.snapshot.file_chunks {
        *per_chunk.entry(slice.chunk_index).or_default() += 1;
    }
    let (chunk_index, placements) = per_chunk
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(chunk_index, count)| (*chunk_index, *count))
        .expect("the fixture should place some chunk");
    assert!(
        placements >= 8,
        "the fixture should place a chunk many times, chunk {chunk_index} is placed \
         {placements} times"
    );

    // The tree as taken holds the version, so the report is every chunk held.
    let probed = built.probe(&built.source).await;
    assert!(
        probed.is_current(),
        "a repeated file is still the tree the version was taken from: {:?}",
        probed.mismatched
    );
    assert_eq!(probed.held, built.all_chunks());

    // Break one byte of the last placement of that chunk. Every other placement
    // of it stays right, and no other chunk covers the byte.
    let last = built
        .snapshot
        .file_chunks
        .iter()
        .rfind(|slice| slice.chunk_index == chunk_index)
        .expect("a placement of the placed chunk");
    let file = &built.snapshot.files[usize::try_from(last.file_index).unwrap()];
    let at = usize::try_from(last.file_offset + u64::from(last.chunk_offset)).unwrap();
    let path = built.source.join(&file.path);
    let mut bytes = fs::read(&path).unwrap();
    bytes[at] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    let probed = built.probe(&built.source).await;

    let shared = built.snapshot.chunks[usize::try_from(chunk_index).unwrap()];
    assert_eq!(
        probed.mismatched,
        vec![Mismatch {
            path: file.path.clone(),
            offset: last.file_offset,
            length: u64::from(last.length),
            expected: shared,
            reason: Unheld::Content,
        }],
        "the broken placement should be the whole slice report"
    );
    assert!(
        !probed.held.contains(&shared),
        "a chunk wrong at one of its placements must not be held"
    );
    assert_eq!(
        probed.held.len(),
        built.all_chunks().len() - 1,
        "every other chunk is placed elsewhere and stays held"
    );
}

/// A slice that runs off the end of one file into the next still verifies.
///
/// A `FileChunk` records a `chunk_offset`. A slice is therefore usually only
/// *part* of a chunk. The last chunk of a file runs on into the next file. The
/// bytes in the file are then not the whole of the chunk, so hashing them
/// produces something that is never equal to the chunk's hash. A probe that
/// compared a file slice's hash to the chunk's hash would condemn every
/// boundary-crossing slice in every tree, forever, and would report a target that
/// is byte-for-byte correct as holding nothing.
///
/// The two sides are compared as *bytes*. This fixture says so. It uses two files.
/// One chunk spans the join. The probe reports that chunk as held.
#[tokio::test]
async fn a_chunk_spanning_two_files_verifies_in_both() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Two small files. The join between them lands inside a chunk rather than on
    // a boundary of its own.
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

    // Every chunk that the cut-off slices were holding is condemned. That includes
    // the chunk that runs off the end of the file into the next. The assertion is
    // therefore about the cut rather than about the tree. The chunks wholly inside
    // `f1.bin` are untouched and still held.
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

/// Entries that the version does not record are reported and left where they are.
///
/// A file here is as likely to be the user's own work as a leftover from an older
/// version. A probe that removed them would destroy data. The probe cannot tell
/// that data from data it should keep. The probe names them. The caller decides.
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
/// re-sending. This test ties the two halves together.
#[tokio::test]
async fn what_a_probe_finds_held_is_what_the_pack_does_not_send() {
    let built = tree(6, 200_000).await;
    let pack = built.pack().await;

    // Start from a copy that holds the version. Then edit one file. A client is
    // actually in this situation.
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

    // Probe against the pack and not the set. A client that updates from a pack
    // does not have the set. The answer must be the same either way.
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

/// The reported slices are the slices that the ranges cover. A fetch can be
/// planned from the report alone.
#[tokio::test]
async fn the_ranges_a_probe_implies_cover_exactly_what_it_says_is_missing() {
    let built = tree(4, 180_000).await;
    let pack = built.pack().await;

    let client = built.client().await;

    // Truncate one file. Several slices go short. None of them holds a whole
    // chunk of correct data anywhere else.
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

/// A file that has grown is not the version, even though every slice of it is.
///
/// The finding no slice can make. Appending to a file leaves every byte that the
/// version records where it was. Every slice then verifies. Every chunk is held.
/// The directory is still not the version. A probe that could only answer about
/// slices would report "holds this version" here. A client would fetch nothing and
/// leave the tail in place.
#[tokio::test]
async fn a_file_that_has_grown_is_reported_though_its_slices_are_right() {
    let built = tree(3, 150_000).await;
    let client = built.client().await;

    // Append, so no recorded byte moves and no chunk changes.
    let path = client.join("f2.bin");
    let before = fs::metadata(&path).unwrap().len();
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(b"appended by somebody");
    fs::write(&path, &bytes).unwrap();

    let probed = built.probe(&client).await;

    assert!(
        probed.mismatched.is_empty(),
        "appending moves no recorded byte, so no slice should differ: {:?}",
        probed.mismatched
    );
    assert!(
        !probed.is_current(),
        "but the directory is still not the version"
    );

    let reported: Vec<_> = probed
        .mislengths
        .iter()
        .filter(|m| m.path.ends_with("f2.bin"))
        .collect();
    assert_eq!(
        reported.len(),
        1,
        "the grown file should be reported exactly once, got {:?}",
        probed.mislengths
    );
    assert_eq!(reported[0].reason, Unheld::Long);
    assert_eq!(reported[0].expected, before);
    assert_eq!(
        reported[0].actual,
        before + "appended by somebody".len() as u64
    );
}

/// A file that has shrunk is reported by its slices, and not a second time.
///
/// The other half of the same pair. A second report for one file would make the
/// report longer. It would not make the report more useful. The slice already
/// says which bytes are missing. It does not only say that the file is the wrong
/// size.
#[tokio::test]
async fn a_file_that_has_shrunk_is_not_reported_twice() {
    let built = tree(3, 150_000).await;
    let client = built.client().await;

    let path = client.join("f2.bin");
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();

    let probed = built.probe(&client).await;

    assert!(
        probed.mismatched.iter().any(|m| m.reason == Unheld::Short),
        "the slices should say so"
    );
    assert!(
        probed.mislengths.is_empty(),
        "and the file should not be reported again on top of them: {:?}",
        probed.mislengths
    );
}

/// A file recorded as empty and missing is reported. Nothing else reports it.
///
/// An empty file has no slices at all, so every length finding about it has to come
/// from the length pass or not come from anywhere.
#[tokio::test]
async fn an_empty_file_that_is_missing_is_reported_once() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("empty.bin"), b"").unwrap();
    fs::write(source.join("data.bin"), common::untagged_stream(1, 40_000)).unwrap();

    let built = built(dir, source).await;
    let client = built.client().await;
    std::fs::remove_file(client.join("empty.bin")).unwrap();

    let probed = built.probe(&client).await;

    assert!(
        probed.mismatched.is_empty(),
        "an empty file has no slices, so no slice can report it: {:?}",
        probed.mismatched
    );
    assert_eq!(
        probed.mislengths,
        vec![Mislength {
            path: Path::new("empty.bin").to_path_buf(),
            expected: 0,
            actual: 0,
            reason: Unheld::Absent,
        }],
        "so the missing empty file is a finding only the length pass can make"
    );
}
