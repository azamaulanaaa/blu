//! An apply: bringing a directory up to the version a pack carries.
//!
//! A restore writes every byte a version needs.
//! An apply writes only the bytes the destination lacks.
//! The tests are about that difference being real rather than cosmetic:
//!
//! * the tree ends up equal to the version. A round trip makes the same assertion,
//!   and reaches it a different way.
//! * a file with nothing to write is *not opened*, so it keeps its inode and its
//!   timestamps.
//! * a **missing file whose chunks are shared with an intact one** is still
//!   created in full.
//!   The two answers are separated to prevent one bug.
//!   A chunk the destination holds elsewhere must not make the destination's own
//!   copy of it look present.
//! * a pack that does not hash to its own contents is refused before a byte is
//!   written. The pack does not overwrite a correct directory.
//! * an interrupted apply is finished by running it again, because a probe is
//!   idempotent.
//! * entries the version does not record are left alone unless pruning was asked
//!   for. Pruning takes them deepest-first.
//!
//! And the one that is the point of all of it.
//! Version 6 to 7 over a real change is a handful of ranges, not a whole tree.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{collections::HashSet, fs, path::Path};

use blulib::{
    command::{ApplyBuilder, ApplyOptions, Manifest},
    metadata::Snapshot,
    pack::Pack,
    storage::{BlobId, BlobStore, Plan, StorageGet},
};
use tokio::io::AsyncReadExt;

/// A tree and the versions taken of it.
struct Versions {
    dir: tempfile::TempDir,
    source: std::path::PathBuf,
    blob: std::path::PathBuf,
}

impl Versions {
    /// The source tree, with `files` distinct files.
    async fn new(files: usize, size: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let source = common::source(dir.path());

        for i in 0..files {
            fs::write(
                source.join(format!("f{i}.bin")),
                common::untagged_stream(u8::try_from(i).unwrap_or(u8::MAX), size + i * 1_000),
            )
            .unwrap();
        }

        let blob = dir.path().join("store");
        common::run_backup(&source, &blob).await;

        Self { dir, source, blob }
    }

    /// A pack of the newest version, written into the temp dir.
    async fn pack(&self) -> Pack {
        let store = BlobStore::builder(&self.blob).open_reader().await.unwrap();
        let (version, id) = store.latest().unwrap();

        let snapshot = read_snapshot(&store, id).await;
        let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

        Pack::write(
            &store,
            &plan,
            &snapshot,
            version,
            &self.dir.path().join("latest.pack"),
        )
        .await
        .unwrap()
    }

    /// A fresh directory holding the newest version, as a client would have it.
    async fn client(&self) -> std::path::PathBuf {
        let client = self.dir.path().join("client");
        let store = BlobStore::builder(&self.blob).open_reader().await.unwrap();
        common::run_restore(&self.blob, store.latest().unwrap().1, &client).await;
        client
    }

    /// Take another version of the current source tree.
    async fn commit(&self) {
        common::run_backup(&self.source, &self.blob).await;
    }
}

async fn read_snapshot(store: &BlobStore<blulib::storage::Read>, id: BlobId) -> Snapshot {
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    Snapshot::try_from(buffer.as_slice()).unwrap()
}

/// The identity of an inode, so "was this file touched" is answerable.
#[cfg(unix)]
fn inode(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt as _;
    let meta = fs::metadata(path)?;
    Ok((meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn inode(_path: &Path) -> std::io::Result<(u64, u64)> {
    Ok((0, 0))
}

/// An empty directory becomes the version.
#[tokio::test]
async fn an_empty_directory_becomes_the_version() {
    let versions = Versions::new(4, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");
    fs::create_dir_all(&target).unwrap();

    let applied = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    common::assert_same_tree(&versions.source, &target);
    assert_eq!(applied.held, 0, "an empty directory holds nothing");
    assert_eq!(applied.files, 4, "every file should have been written");
    assert_eq!(
        applied.fetched,
        pack.payload_len(),
        "an empty directory needs the whole payload"
    );
    assert_eq!(applied.ranges, 1, "and the whole payload is one range");
    assert!(applied.bytes > 0);
}

/// A directory already holding the version is left exactly as it is.
///
/// Not "ends up the same", but *untouched*.
/// No file is opened, so no inode changes and no timestamp moves.
/// That is more than a smaller download.
/// It is the difference between updating a tree someone is working in and
/// rewriting it.
#[tokio::test]
async fn a_directory_already_holding_the_version_is_not_touched() {
    let versions = Versions::new(4, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    let before: Vec<_> = ["f0.bin", "f1.bin", "f2.bin", "f3.bin"]
        .iter()
        .map(|name| (name, inode(&target.join(name)).unwrap()))
        .collect();

    let applied = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    assert_eq!(applied.files, 0, "no file should have been written");
    assert_eq!(applied.bytes, 0);
    assert_eq!(applied.written, 0);

    for (name, identity) in before {
        assert_eq!(
            inode(&target.join(name)).unwrap(),
            identity,
            "{name} was opened despite having nothing to write"
        );
    }
}

/// One edited file is repaired, and the others keep their inodes.
///
/// The property that makes an apply worth having over a restore.
/// The cost is proportional to what changed.
/// The blast radius is too.
#[tokio::test]
async fn one_edited_file_is_repaired_and_the_rest_are_left_alone() {
    let versions = Versions::new(5, 150_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    let untouched = ["f0.bin", "f1.bin", "f3.bin", "f4.bin"]
        .iter()
        .map(|name| (name, inode(&target.join(name)).unwrap()))
        .collect::<Vec<_>>();

    let path = target.join("f2.bin");
    let mut bytes = fs::read(&path).unwrap();
    let at = bytes.len() / 2;
    bytes[at..at + 96].copy_from_slice(&common::untagged_stream(77, 96));
    fs::write(&path, &bytes).unwrap();

    let applied = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    common::assert_same_tree(&versions.source, &target);
    assert_eq!(applied.files, 1, "only the edited file should be written");
    assert!(
        applied.fetched > 0,
        "the chunks around the edit were not held, so they had to be sent"
    );
    assert!(
        applied.fetched < pack.payload_len() / 8,
        "one edited file out of five should need well under an eighth of the \
         payload, needs {} of {}",
        applied.fetched,
        pack.payload_len()
    );
    assert!(
        applied.held > 0,
        "and most of the tree was already held, {}",
        applied.held
    );

    for (name, identity) in untouched {
        assert_eq!(
            inode(&target.join(name)).unwrap(),
            identity,
            "{name} was rewritten although it was already correct"
        );
    }
}

/// A file that is missing is created in full, even where its chunks are held
/// elsewhere.
///
/// This is the trap the two probe answers are separated to avoid.
/// `held` is per chunk and exists to decide what a *fetch* needs.
/// A chunk the destination holds correctly in `a.bin` is in `held`, because the
/// destination does hold it.
/// But `b.bin` is a *different file* and has none of those bytes.
/// Judge `b.bin` by `held` and every one of its slices is skipped.
/// The file is then created empty.
#[tokio::test]
async fn a_missing_file_is_created_even_where_its_chunks_are_held_elsewhere() {
    let versions = Versions::new(3, 150_000).await;

    // Two files of identical content, so every chunk of one is held correctly in
    // the other.
    fs::write(
        versions.source.join("dup.bin"),
        common::untagged_stream(5, 180_000),
    )
    .unwrap();
    fs::write(
        versions.source.join("twin.bin"),
        common::untagged_stream(5, 180_000),
    )
    .unwrap();
    versions.commit().await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    // The shared chunks are genuinely held.
    let store = BlobStore::builder(&versions.blob)
        .open_reader()
        .await
        .unwrap();
    let snapshot = read_snapshot(&store, store.latest().unwrap().1).await;
    let probed = blulib::command::probe(&target, &snapshot, store)
        .await
        .unwrap();
    assert!(
        probed.held.len() > 5,
        "the two identical files should share a lot, held {}",
        probed.held.len()
    );

    // Now delete one of them. Every chunk it needs is still held — in the other.
    fs::remove_file(target.join("twin.bin")).unwrap();

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    common::assert_same_tree(&versions.source, &target);
    assert_eq!(
        fs::metadata(target.join("twin.bin")).unwrap().len(),
        versions.source.join("twin.bin").metadata().unwrap().len(),
        "twin.bin was created but not filled"
    );
}

/// A file longer than the version is truncated to it.
///
/// A file whose slices all verify but whose length does not match is one whose
/// tail is not the version's.
/// Leaving it would mean the directory still is not the version while every probe
/// reports it as held.
#[tokio::test]
async fn a_file_longer_than_the_version_is_truncated() {
    let versions = Versions::new(2, 100_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    let path = target.join("f0.bin");
    let original = fs::metadata(&path).unwrap().len();
    fs::write(
        &path,
        vec![0xABu8; usize::try_from(original).unwrap() + 4_096],
    )
    .unwrap();

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    common::assert_same_tree(&versions.source, &target);
    assert_eq!(fs::metadata(&path).unwrap().len(), original);
}

/// A pack whose bytes do not hash to its own contents is refused, before any write.
///
/// The probe compares the destination against the chunk's bytes *as the pack gives
/// them*.
/// So a damaged pack makes every slice look absent.
/// Without this check, applying one would write its damage straight over a correct
/// directory, and report success.
#[tokio::test]
async fn a_corrupt_pack_is_refused_and_nothing_is_written() {
    let versions = Versions::new(3, 150_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    // The target already holds the version, so there is nothing to write.
    // The run should be a no-op, and the corruption has to be found anyway.
    // Delete a file so there *is* work, then damage a chunk that filling it needs.
    let path = target.join("f1.bin");
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();

    let at = pack.path().to_path_buf();
    let mut damaged = fs::read(&at).unwrap();
    let start = usize::try_from(pack.payload_start()).unwrap() + 64;
    damaged[start] ^= 0xff;
    fs::write(&at, &damaged).unwrap();

    let before = common::describe(&target);
    let broken = Pack::open(&at).unwrap();

    let error = ApplyBuilder::new(&target, &broken).run().await.unwrap_err();

    assert!(
        error.to_string().contains("do not hash to it"),
        "a chunk that does not hash to itself should be refused, got: {error}"
    );
    assert_eq!(
        common::describe(&target),
        before,
        "a refused apply must leave the destination exactly as it was"
    );
}

/// An interrupted apply is finished by running it again.
///
/// Not atomic, and not pretending to be.
/// The recovery is that a probe answers the same way twice.
/// So the second run finds the first run's work and only does what is left.
#[tokio::test]
async fn running_an_apply_twice_converges() {
    let versions = Versions::new(4, 150_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    let path = target.join("f0.bin");
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() / 3]).unwrap();

    // A first run that is interrupted partway: emulate it by applying to a
    // destination holding *part* of the change, then the whole of it.
    ApplyBuilder::new(&target, &pack).run().await.unwrap();
    common::assert_same_tree(&versions.source, &target);

    let again = ApplyBuilder::new(&target, &pack).run().await.unwrap();
    assert_eq!(again.files, 0, "a second run should find nothing to do");
    assert_eq!(again.written, 0);
    assert_eq!(again.bytes, 0);
    assert!(
        again.held > 0,
        "and it should now be holding every chunk, holds {}",
        again.held
    );
    common::assert_same_tree(&versions.source, &target);
}

/// Entries the version does not record survive, and pruning is what removes them.
#[tokio::test]
async fn unrecorded_entries_survive_unless_pruning_was_asked_for() {
    let versions = Versions::new(3, 100_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;

    let stray = target.join("scratch");
    fs::create_dir_all(&stray).unwrap();
    fs::write(stray.join("notes.txt"), b"mine").unwrap();

    let applied = ApplyBuilder::new(&target, &pack).run().await.unwrap();
    assert_eq!(applied.extra, 2, "the file and the directory holding it");
    assert_eq!(applied.removed, 0);
    assert!(
        target.join("scratch/notes.txt").exists(),
        "an unrecorded entry must not be deleted on a guess"
    );

    let pruned = ApplyBuilder::new(&target, &pack)
        .options(ApplyOptions {
            prune: true,
            ..Default::default()
        })
        .run()
        .await
        .unwrap();

    assert_eq!(pruned.removed, 2);
    assert!(
        !target.join("scratch").exists(),
        "pruning should have removed the directory too"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// Links and directories are laid down the same way a restore lays them.
///
/// Reusing the restore's own stages rather than writing an apply-specific copy is
/// the point.
/// An apply that created links its own way would be a second implementation to
/// keep in step with the first.
#[tokio::test]
async fn directories_and_links_come_out_the_same_as_a_restores() {
    let versions = Versions::new(2, 80_000).await;

    let nested = versions.source.join("deep/nested/tree");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("leaf.bin"), common::noisy(30_000)).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("../../f0.bin", versions.source.join("deep/link")).unwrap();
    versions.commit().await;

    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    common::assert_same_tree(&versions.source, &target);
}

/// The point of all of it: version 6 to 7 costs a delta, not a tree.
#[tokio::test]
async fn updating_a_version_costs_a_delta_and_not_a_tree() {
    let versions = Versions::new(8, 250_000).await;

    // Version 6, as the client has it.
    let client = versions.client().await;
    let pack6 = versions.pack().await;

    // Version 7: one file genuinely rewritten, one deleted, one added.
    fs::write(
        versions.source.join("f5.bin"),
        common::untagged_stream(200, 300_000),
    )
    .unwrap();
    fs::remove_file(versions.source.join("f7.bin")).unwrap();
    fs::write(
        versions.source.join("new.bin"),
        common::untagged_stream(201, 20_000),
    )
    .unwrap();
    versions.commit().await;

    let pack7 = versions.pack().await;

    // What a client holding version 6 would have to fetch.
    let held6: HashSet<BlobId> = pack6.iter().map(|c| c.id).collect();
    let transfer = pack7.needed_bytes(&held6);
    assert!(
        transfer < pack7.payload_len() / 2,
        "one rewritten file should need well under half the new payload, needs \
         {transfer} of {}",
        pack7.payload_len()
    );

    // Apply it.
    let applied = ApplyBuilder::new(&client, &pack7)
        .options(ApplyOptions {
            prune: true,
            ..Default::default()
        })
        .run()
        .await
        .unwrap();

    common::assert_same_tree(&versions.source, &client);
    assert!(
        applied.bytes < pack7.payload_len(),
        "an apply writes less than the pack holds, since some was already there"
    );
    assert!(
        !client.join("f7.bin").exists(),
        "pruning should have removed the file version 7 dropped"
    );
    assert_eq!(applied.removed, 1);
}

/// A file the version dropped stays when pruning was not asked for.
/// That is the right default, stated as a test.
#[tokio::test]
async fn a_file_the_version_dropped_stays_without_pruning() {
    let versions = Versions::new(3, 100_000).await;
    let client = versions.client().await;

    fs::remove_file(versions.source.join("f2.bin")).unwrap();
    versions.commit().await;
    let pack = versions.pack().await;

    ApplyBuilder::new(&client, &pack).run().await.unwrap();

    assert!(
        client.join("f2.bin").exists(),
        "a file the version no longer records is the user's call, not this tool's"
    );
}

/// A second apply trusts the first one's record and reads nothing.
///
/// The saving, measured.
/// `trusted` is how many files were taken on the record's word.
/// On a second run of an untouched tree it is every file, and `written` is zero.
/// So the pack was not read at all.
/// Every chunk of it was covered by a file nobody opened.
#[tokio::test]
async fn a_second_apply_trusts_the_first_and_reads_nothing() {
    let versions = Versions::new(4, 150_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    let first = ApplyBuilder::new(&target, &pack).run().await.unwrap();
    assert!(
        first.written > 0,
        "an empty directory has to be given everything"
    );
    assert!(
        Manifest::path_for(&target).unwrap().exists(),
        "an apply should leave a record of what it did"
    );

    let second = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    assert_eq!(
        second.written, 0,
        "nothing should have been written the second time"
    );
    assert_eq!(
        second.files, 0,
        "and no file opened, which is the whole saving"
    );
    assert_eq!(
        second.trusted, 4,
        "every file should have been taken on the record's word"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// A file edited after an apply is read, even though its neighbours are not.
///
/// The per-file fallback, and the reason it is per file.
/// If the fast path were all-or-nothing, one edited file would send the whole tree
/// back to being read.
/// If it were trusted anyway, the edit would survive an apply reporting success.
#[tokio::test]
async fn an_edited_file_is_read_while_its_neighbours_are_trusted() {
    let versions = Versions::new(5, 150_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    // One file rewritten, in place, same length.
    let edited = target.join("f2.bin");
    let mut bytes = fs::read(&edited).unwrap();
    let at = bytes.len() / 2;
    bytes[at..at + 32].copy_from_slice(&common::untagged_stream(200, 32));
    fs::write(&edited, &bytes).unwrap();

    let second = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    assert!(
        second.written > 0,
        "the edited file must be read and rewritten"
    );
    assert_eq!(second.files, 1, "and only it, the others being untouched");
    assert_eq!(
        second.trusted, 4,
        "the other four should still have been trusted"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// `--recheck` trusts nothing, and agrees with the fast path about the tree.
///
/// The paranoid mode has to reach the same answer as the ordinary one.
/// Without that, it is not a mode. It is a different tool.
#[tokio::test]
async fn a_recheck_trusts_nothing_and_agrees() {
    let versions = Versions::new(4, 150_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    // One byte changed, and the modification time put back to what the record
    // holds.
    // Length matches, and time matches, and the content is wrong.
    // That is the case the fast path provably cannot see.
    // That is the reason `--recheck` exists rather than being redundant.
    // Only something that puts timestamps back produces it.
    let victim = target.join("f1.bin");
    let before = fs::metadata(&victim).unwrap();
    let mut bytes = fs::read(&victim).unwrap();
    let at = bytes.len() / 3;
    bytes[at] ^= 0xff;
    fs::write(&victim, &bytes).unwrap();
    filetime::set_file_mtime(
        &victim,
        filetime::FileTime::from_last_modification_time(&before),
    )
    .unwrap();

    let fast = ApplyBuilder::new(&target, &pack).run().await.unwrap();
    assert_eq!(
        fast.written, 0,
        "nothing the record holds distinguishes this from the right bytes"
    );

    let checked = ApplyBuilder::new(&target, &pack)
        .options(ApplyOptions {
            recheck: true,
            ..Default::default()
        })
        .run()
        .await
        .unwrap();

    assert_eq!(
        checked.trusted, 0,
        "--recheck should trust nothing by construction"
    );
    assert!(
        checked.written > 0,
        "and so it should find the change the fast path could not"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// A file that has grown is still corrected with a record present.
///
/// A stat gives the length. The record does not.
/// So the fast path must not lose the check that catches this.
#[tokio::test]
async fn a_file_that_has_grown_is_truncated_with_a_record_present() {
    let versions = Versions::new(3, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    let grown = target.join("f0.bin");
    let original = fs::metadata(&grown).unwrap().len();
    let mut bytes = fs::read(&grown).unwrap();
    bytes.extend_from_slice(b"somebody appended this");
    fs::write(&grown, &bytes).unwrap();

    let second = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    assert_eq!(
        fs::metadata(&grown).unwrap().len(),
        original,
        "the tail that is not the version's should be gone"
    );
    assert_eq!(
        second.files, 1,
        "and it should have been the only file opened"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// A record naming another version is ignored rather than believed.
///
/// This is the case where the record does not describe a stale tree but a different
/// chunk layout entirely.
/// Reading is slower and correct.
/// Believing the record would be faster and could not be correct.
#[tokio::test]
async fn a_record_naming_another_version_is_not_believed() {
    let versions = Versions::new(3, 120_000).await;
    let pack7 = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack7).run().await.unwrap();

    // Change the tree and take another version.
    fs::write(
        versions.source.join("f0.bin"),
        common::untagged_stream(9, 130_000),
    )
    .unwrap();
    versions.commit().await;
    let pack8 = versions.pack().await;

    let second = ApplyBuilder::new(&target, &pack8).run().await.unwrap();

    assert_eq!(
        second.trusted, 0,
        "a record for another version can say nothing about this one"
    );
    assert!(
        second.written > 0,
        "so the changed file should have been read and rewritten"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// A destination with no record is read in full, and still works.
///
/// The first apply, and the case where something deleted the record.
/// It must be an ordinary run rather than a failure.
#[tokio::test]
async fn a_destination_with_no_record_is_read_in_full() {
    let versions = Versions::new(3, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();
    Manifest::remove(&target).unwrap();

    let second = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    assert_eq!(second.trusted, 0, "nothing to trust");
    assert_eq!(second.written, 0, "but nothing wrong either");
    assert!(
        Manifest::path_for(&target).unwrap().exists(),
        "and the record should be written again"
    );
    common::assert_same_tree(&versions.source, &target);
}

/// A corrupt record is a cache miss, not a failure.
#[tokio::test]
async fn a_corrupt_record_is_a_cache_miss() {
    let versions = Versions::new(3, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    // Bytes that name chunks belonging to some other version, in a format of their
    // own. Nothing here should be believed and nothing should fail.
    fs::write(
        Manifest::path_for(&target).unwrap(),
        b"not a manifest at all, but present",
    )
    .unwrap();

    let second = ApplyBuilder::new(&target, &pack).run().await.unwrap();

    assert_eq!(
        second.trusted, 0,
        "an undecodable record vouches for nothing"
    );
    assert_eq!(second.written, 0, "and the tree was still right");
    common::assert_same_tree(&versions.source, &target);
}

/// The record lives beside the destination, so it is never backed up with it.
///
/// A record inside the tree would be part of the next version's snapshot.
/// So every version would differ from the one before by a file.
/// Neither the user nor the version has anything to say about that file.
#[tokio::test]
async fn the_record_is_beside_the_destination_and_not_inside_it() {
    let versions = Versions::new(3, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");

    ApplyBuilder::new(&target, &pack).run().await.unwrap();

    let record = Manifest::path_for(&target).unwrap();
    assert!(record.exists(), "it should have been written");
    assert!(
        record.parent() == Some(versions.dir.path()),
        "and be a sibling of the destination, got {}",
        record.display()
    );
    assert!(
        !fs::read_dir(&target)
            .unwrap()
            .any(|entry| entry.unwrap().path() == record),
        "and never appear inside the tree"
    );
}

/// An apply with `--files` writes only the selection.
///
/// One file comes up to the version and the rest stay missing. A later full
/// apply of the same pack trusts the manifest for the file the selection
/// wrote and finishes the tree.
#[tokio::test]
async fn apply_with_files_writes_only_the_selection() {
    let versions = Versions::new(3, 1_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");
    fs::create_dir_all(&target).unwrap();

    let applied = ApplyBuilder::new(&target, &pack)
        .options(ApplyOptions {
            files: vec![std::path::PathBuf::from("f0.bin")],
            ..Default::default()
        })
        .run()
        .await
        .unwrap();

    assert_eq!(applied.files, 1);
    assert!(target.join("f0.bin").is_file());
    assert!(
        !target.join("f1.bin").exists(),
        "a file outside the selection must not be written"
    );

    let finished = ApplyBuilder::new(&target, &pack).run().await.unwrap();
    assert_eq!(
        finished.files, 2,
        "the full apply writes the two missing files"
    );
    assert_eq!(
        finished.trusted, 1,
        "the file the selection wrote is taken on the manifest's word"
    );
    for i in 0..3 {
        assert_eq!(
            fs::read(target.join(format!("f{i}.bin"))).unwrap(),
            fs::read(versions.source.join(format!("f{i}.bin"))).unwrap(),
            "f{i}.bin differs from the version"
        );
    }
}

/// An apply with `--files --prune` removes only the matching extras.
///
/// An unrecorded file under the selection goes, and one outside it stays.
#[tokio::test]
async fn apply_with_files_and_prune_scopes_removal() {
    let versions = Versions::new(2, 1_000).await;
    let pack = versions.pack().await;
    let target = versions.dir.path().join("target");
    fs::create_dir_all(&target).unwrap();

    ApplyBuilder::new(&target, &pack).run().await.unwrap();
    fs::write(target.join("fx.bin"), b"stale\n").unwrap();
    fs::write(target.join("notes.txt"), b"mine\n").unwrap();

    let pruned = ApplyBuilder::new(&target, &pack)
        .options(ApplyOptions {
            files: vec![std::path::PathBuf::from("f*.bin")],
            prune: true,
            ..Default::default()
        })
        .run()
        .await
        .unwrap();

    assert_eq!(pruned.removed, 1);
    assert!(
        !target.join("fx.bin").exists(),
        "an unrecorded file under the selection is pruned"
    );
    assert!(
        target.join("notes.txt").is_file(),
        "an unrecorded file outside the selection is left alone"
    );
}

/// The apply builder previews with `dry_run` and prunes extras on request.
///
/// A dry run changes nothing and still names the extra file. A prune run
/// removes it. Both runs report the same file count for the version.
#[tokio::test]
async fn apply_builder_previews_then_prunes_extras() {
    let versions = Versions::new(2, 120_000).await;
    let pack = versions.pack().await;
    let target = versions.client().await;
    fs::write(target.join("extra.bin"), b"mine\n").unwrap();

    let preview = ApplyBuilder::new(&target, &pack)
        .dry_run()
        .run()
        .await
        .unwrap();
    assert_eq!(preview.extra, 1, "the dry run names the extra file");
    assert_eq!(preview.removed, 0, "a dry run removes nothing");
    assert!(
        target.join("extra.bin").is_file(),
        "a dry run leaves the tree alone"
    );

    let pruned = ApplyBuilder::new(&target, &pack)
        .prune()
        .run()
        .await
        .unwrap();
    assert_eq!(pruned.extra, 1);
    assert_eq!(pruned.removed, 1, "the prune run removes the extra file");
    assert!(!target.join("extra.bin").exists(), "the extra file is gone");
    assert_eq!(pruned.files, preview.files);
}
