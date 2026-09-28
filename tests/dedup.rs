//! Deduplication: repeated content must be stored once, both within a single
//! run and across runs against a store that already holds it.
//!
//! There is no base snapshot any more. Deduplication is the store's own
//! business — it holds every version it has ever been given, and a `put` of
//! content it already has writes nothing — so these tests are about the
//! property a caller gets for free rather than about a knob it sets.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;

use lepatch::{
    command::Options,
    storage::{BlobId, BlobStore, StorageGet},
};

/// Read a snapshot back out of a store.
async fn snapshot(blob: &std::path::Path, id: &BlobId) -> lepatch::metadata::Snapshot {
    use tokio::io::AsyncReadExt;

    let storage = BlobStore::<false>::open(blob).await.unwrap();
    let mut reader = storage.get(id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    lepatch::metadata::Snapshot::from_bytes(&buffer).unwrap()
}

#[tokio::test]
async fn repeated_content_within_one_run_is_stored_once() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // The same 64 KiB block in three files. Identical content at an identical
    // offset, so the chunker is guaranteed to cut it the same way each time.
    let block = vec![0x5au8; 64 * 1024];
    for name in ["a.bin", "b.bin", "c.bin"] {
        fs::write(source.join(name), &block).unwrap();
    }

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;
    let snapshot = snapshot(&blob, &committed.snapshot).await;

    assert_eq!(
        snapshot.chunks.len(),
        1,
        "three identical files should collapse to a single stored chunk, got {}",
        snapshot.chunks.len()
    );

    // 192 KiB of input must not have grown the store by 192 KiB. The store file
    // carries record headers and the snapshot on top of the chunk data, so this
    // is a loose bound rather than an exact one.
    let stored = common::set_size(&blob);
    assert!(
        stored < 128 * 1024,
        "store grew to {stored} bytes, deduplication did not happen"
    );
}

/// Re-backing up an unchanged tree stores nothing.
///
/// This is the property the redesign is for. The old design deduplicated a run
/// only against a snapshot the caller named, which meant a run had to know which
/// version came before it and that version had to still be intact. The store
/// holds every version it has ever been given, so a run hands over every chunk
/// it read and the store recognises the content however far back it was last
/// seen — including content from a version that is not the latest.
#[tokio::test]
async fn a_second_run_of_an_unchanged_tree_stores_nothing_new() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("stable.bin"), vec![0x11u8; 100_000]).unwrap();

    let blob = dir.path().join("store");

    let first = common::run_backup(&source, &blob).await;
    let after_first = common::set_size(&blob);

    let second = common::run_backup(&source, &blob).await;
    let after_second = common::set_size(&blob);

    assert_ne!(
        first.version, second.version,
        "each run should publish its own version"
    );
    assert_eq!(
        first.snapshot, second.snapshot,
        "an unchanged tree describes the same snapshot, so it should be the same bytes"
    );

    // The second run writes a commit record and nothing else: the snapshot blob
    // is already there, and no chunk data is new.
    let growth = after_second - after_first;
    assert!(
        growth < 1024,
        "re-backing up an unchanged tree grew the store by {growth} bytes, \
         which suggests chunk data was stored again"
    );

    let first_snapshot = snapshot(&blob, &first.snapshot).await;
    let second_snapshot = snapshot(&blob, &second.snapshot).await;
    assert_eq!(first_snapshot.chunks, second_snapshot.chunks);
    assert_eq!(first_snapshot.file_chunks, second_snapshot.file_chunks);
}

/// Content last seen in an *earlier* version is still recognised.
///
/// The old design could only reuse what the immediately previous snapshot held,
/// so a file whose content had been absent from the last version was stored
/// again, however recently the store had last seen it. Dedup against a named base
/// cannot express this; a store that remembers every version it has been given
/// can, and this is the test for it.
///
/// The tree keeps the same shape throughout, so the chunker cuts the same
/// boundaries each time and version 3's chunks really are version 1's. That is
/// deliberate: the property under test is the *store* recognising content, and
/// varying the file set would move the boundaries and test something else.
#[tokio::test]
async fn content_last_seen_an_earlier_version_is_still_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let blob = dir.path().join("store");
    let stable = common::noisy(100_000);
    let changing = common::noisy(40_000);

    // What the tree costs to store the first time, in a store of its own.
    let cold = {
        fs::write(source.join("stable.bin"), &stable).unwrap();
        fs::write(source.join("changing.bin"), &changing).unwrap();
        let path = dir.path().join("cold");
        common::run_backup(&source, &path).await;
        common::set_size(&path)
    };

    // Version 1, and the baseline it is compared against.
    common::run_backup(&source, &blob).await;
    let after_first = common::set_size(&blob);
    assert_eq!(
        after_first, cold,
        "the first backup should match the cold store"
    );

    // Version 2 changes the second file, so its content is not in this version.
    let other = common::noisy(40_000);
    fs::write(source.join("changing.bin"), &other).unwrap();
    common::run_backup(&source, &blob).await;
    let after_second = common::set_size(&blob);

    // Version 3 puts the first file's content back. It is in the store — it has
    // been there since version 1 — and it is not in the version that came before.
    fs::write(source.join("changing.bin"), &changing).unwrap();
    let third = common::run_backup(&source, &blob).await;
    let after_third = common::set_size(&blob);

    // Version 3 costs what publishing a version costs — a snapshot blob and a
    // commit record — and none of the chunk data, because every chunk it names
    // was already in the store.
    let reused = after_third - after_second;
    assert!(
        reused * 4 < cold,
        "a tree whose content the store still held cost {reused} bytes, against \
         {cold} bytes to store it the first time, so it was written again"
    );

    // And the version is genuinely restorable, which is the part that matters:
    // skipping the write is only correct if the bytes were already there.
    let destination = dir.path().join("restored");
    common::run_restore(&blob, third.snapshot, &destination).await;
    assert_eq!(fs::read(destination.join("stable.bin")).unwrap(), stable);
    assert_eq!(
        fs::read(destination.join("changing.bin")).unwrap(),
        changing
    );
}

#[tokio::test]
async fn deduplication_does_not_change_the_restored_tree() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    for name in ["a.bin", "b.bin", "c.bin"] {
        fs::write(source.join(name), vec![0x5au8; 64 * 1024]).unwrap();
    }
    fs::write(source.join("unique.txt"), b"not shared").unwrap();

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;
    let destination = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &destination).await;

    assert_eq!(common::describe(&source), common::describe(&destination));
}

#[tokio::test]
async fn chunks_are_deduplicated_however_many_are_in_flight() {
    // Every one of these files is one 64 KiB run of identical bytes, which the
    // chunker cuts at its maximum size — so each file is exactly one chunk and
    // all of them hash the same. That is the case worth racing on: the same
    // content reaching several stores at once, where a deduplication check that
    // only works in sequence would store a copy per worker.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let shared = vec![0x5au8; 64 * 1024];
    for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        fs::write(source.join(format!("{name}.bin")), &shared).unwrap();
    }

    // Plus content that must *not* be deduplicated, so a run cannot pass simply
    // by collapsing everything into one entry.
    fs::write(source.join("one.bin"), common::noisy(70 * 1024)).unwrap();
    fs::write(source.join("two.bin"), common::noisy(70 * 1024)).unwrap();

    let serial = count_at(&dir.path().join("serial.bin"), &source, 1).await;

    // The eight identical files must have become one chunk, not eight.
    assert!(
        serial < 8,
        "eight identical files produced {serial} stored chunks, so they were not \
         deduplicated at all"
    );

    for concurrency in [2, 4, 8, 16, 32] {
        let blob = dir.path().join(format!("parallel{concurrency}.bin"));
        assert_eq!(
            count_at(&blob, &source, concurrency).await,
            serial,
            "with {concurrency} stores in flight a different number of chunks was stored"
        );
    }
}

/// Back up `source` with `concurrency` stores in flight, and report how many
/// distinct chunks ended up stored.
async fn count_at(blob: &std::path::Path, source: &std::path::Path, concurrency: usize) -> usize {
    let committed = common::run_backup_with(
        source,
        blob,
        Options {
            concurrency,
            ..Options::default()
        },
    )
    .await;

    snapshot(blob, &committed.snapshot).await.chunks.len()
}

#[tokio::test]
async fn concurrency_does_not_change_the_snapshot() {
    // Uploads finish in whatever order the store lets them, but the snapshot
    // must depend only on the order chunks were read in. If this drifts, two
    // backups of the same tree stop being comparable and the concurrency knob
    // stops being safe to touch.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // Several distinct files plus shared content between them, so a run has
    // chunks to deduplicate as well as chunks to keep in order. The shared part
    // is what makes two chunks with the same hash race for the same cell.
    let shared = common::noisy(48 * 1024);
    for i in 0..6 {
        let mut bytes = common::noisy(40 * 1024 + i * 997);
        bytes.extend_from_slice(&shared);
        fs::write(source.join(format!("file{i}.bin")), bytes).unwrap();
    }

    let serial = {
        let blob = dir.path().join("serial.bin");
        let committed = common::run_backup_with(
            &source,
            &blob,
            Options {
                concurrency: 1,
                ..Options::default()
            },
        )
        .await;
        snapshot(&blob, &committed.snapshot).await
    };

    for concurrency in [2, 8, 32] {
        // Repeated, because the failure this guards against is a race: it takes
        // a later-read duplicate reaching a hash's cell before the earlier-read
        // one does, which is a matter of scheduling rather than of input. A
        // single run can pass by luck, and did.
        for _ in 0..4 {
            let blob = dir.path().join(format!("parallel{concurrency}.bin"));
            let committed = common::run_backup_with(
                &source,
                &blob,
                Options {
                    concurrency,
                    ..Options::default()
                },
            )
            .await;
            let parallel = snapshot(&blob, &committed.snapshot).await;

            // Compared on what the design actually promises.
            //
            // `file_chunks` carries the chunk *indices*, so it transitively pins
            // the hash-to-index mapping, which is the part that has to be a
            // function of read order. `chunks` is a bare hash now, so comparing
            // it directly is comparing content identity: there is no offset in
            // it any more, and nothing about which store a run happened to write
            // to can leak into a snapshot.
            assert_eq!(
                serial.file_chunks, parallel.file_chunks,
                "concurrency {concurrency} laid the chunks out differently"
            );
            assert_eq!(
                serial.files, parallel.files,
                "concurrency {concurrency} recorded the files differently"
            );
            assert_eq!(
                serial.chunks, parallel.chunks,
                "concurrency {concurrency} indexed the stored content differently"
            );
        }
    }
}

#[tokio::test]
async fn no_two_stored_chunks_share_content() {
    // The invariant the commit order exists to keep: one entry per distinct
    // content, however many chunks referred to it and in whatever order they
    // finished storing.
    //
    // Asserted directly rather than inferred, because the bug it guards against
    // surfaced as a failed backup rather than as a wrong snapshot, so nothing
    // about the output would have given it away.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // Constant content, so the chunker cuts at its maximum size and every file
    // is a single chunk. That maximises how many chunks collide on one hash.
    let shared = vec![0x5au8; 64 * 1024];
    for name in ["a", "b", "c", "d"] {
        fs::write(source.join(format!("{name}.bin")), &shared).unwrap();
    }

    for concurrency in [1usize, 8, 32] {
        let blob = dir.path().join(format!("store{concurrency}.bin"));
        let committed = common::run_backup_with(
            &source,
            &blob,
            Options {
                concurrency,
                ..Options::default()
            },
        )
        .await;
        let snapshot = snapshot(&blob, &committed.snapshot).await;

        let mut seen = std::collections::BTreeSet::new();
        for chunk in &snapshot.chunks {
            assert!(
                seen.insert(*chunk),
                "with concurrency {concurrency}, content was stored more than once: \
                 {} chunks for 4 identical files",
                snapshot.chunks.len()
            );
        }

        // And every reference must resolve to an entry that exists.
        for slice in &snapshot.file_chunks {
            assert!(
                snapshot.chunks.get(slice.chunk_index as usize).is_some(),
                "a slice points at chunk {} but only {} are stored",
                slice.chunk_index,
                snapshot.chunks.len()
            );
        }
    }
}
