// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Deduplication: repeated content must be stored once, both within a single
//! run and across runs against a store that already holds it.
//!
//! There is no base snapshot any more.
//! Deduplication is the store's own business.
//! The store holds every version it has ever been given.
//! A `put` of content it already has writes nothing.
//! So these tests are about the property a caller gets for free rather than about
//! a knob it sets.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::fs;

use blulib::{
    command::{Options, backup},
    metadata::Timestamp,
    storage::{BlobId, BlobStore, StorageGet},
};

/// Read a snapshot back out of a store.
async fn snapshot(blob: &std::path::Path, id: &BlobId) -> blulib::metadata::Snapshot {
    use tokio::io::AsyncReadExt;

    let storage = BlobStore::builder(blob).open_reader().await.unwrap();
    let mut reader = storage.get(id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    blulib::metadata::Snapshot::try_from(buffer.as_slice()).unwrap()
}

#[tokio::test]
async fn repeated_content_within_one_run_is_stored_once() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // The same 64 KiB block in three files.
    // Identical content at an identical offset, so the chunker is guaranteed to
    // cut it the same way each time.
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

    // 192 KiB of input must not have grown the store by 192 KiB.
    // The store file carries record headers and the snapshot on top of the chunk
    // data.
    // So this is a loose bound rather than an exact one.
    let stored = common::set_size(&blob);
    assert!(
        stored < 128 * 1024,
        "store grew to {stored} bytes, deduplication did not happen"
    );
}

/// Re-backing up an unchanged tree stores no chunk data.
///
/// This is the property the redesign is for.
/// The old design deduplicated a run only against a snapshot the caller named.
/// A run had to know which version came before it.
/// That version had to still be intact.
/// The store holds every version it has ever been given.
/// A run hands over every chunk it read, and the store recognises the content
/// however far back it was last seen.
/// That includes content from a version that is not the latest.
#[tokio::test]
async fn a_second_run_of_an_unchanged_tree_stores_nothing_new() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
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

    let first_snapshot = snapshot(&blob, &first.snapshot).await;
    let second_snapshot = snapshot(&blob, &second.snapshot).await;

    // Each run stamps its own time, so the two snapshot blobs differ.
    // The tree did not move, so everything but the stamp matches.
    assert_ne!(
        first.snapshot, second.snapshot,
        "two runs stamp different times, so the bytes differ"
    );
    assert!(
        second_snapshot.taken_at >= first_snapshot.taken_at,
        "the second run must not predate the first"
    );
    assert_eq!(first_snapshot.files, second_snapshot.files);
    assert_eq!(first_snapshot.dirs, second_snapshot.dirs);
    assert_eq!(first_snapshot.chunks, second_snapshot.chunks);
    assert_eq!(first_snapshot.file_chunks, second_snapshot.file_chunks);
    assert_eq!(first_snapshot.links, second_snapshot.links);

    // The second run writes a snapshot blob and a commit record, and nothing
    // else. No chunk data is new.
    let growth = after_second - after_first;
    assert!(
        growth < 2048,
        "re-backing up an unchanged tree grew the store by {growth} bytes, \
         which suggests chunk data was stored again"
    );
}

/// Content last seen in an *earlier* version is still recognised.
///
/// The old design could only reuse what the immediately previous snapshot held.
/// So a file whose content was absent from the last version was stored again.
/// That happened however recently the store had last seen the content.
/// Dedup against a named base cannot express this.
/// A store that remembers every version it has been given can, and this is the
/// test for it.
///
/// The tree keeps the same shape throughout, so the chunker cuts the same
/// boundaries each time.
/// Version 3's chunks really are version 1's.
/// That is deliberate: the property under test is the *store* recognising
/// content.
/// Varying the file set would move the boundaries and test something else.
#[tokio::test]
async fn content_last_seen_an_earlier_version_is_still_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

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
    // Compared within a tolerance rather than exactly: the two snapshots
    // stamp different `taken_at` nanos, and the compressed snapshot record
    // can differ by a byte or two even for an identical tree.
    common::run_backup(&source, &blob).await;
    let after_first = common::set_size(&blob);
    assert!(
        after_first.abs_diff(cold) < 2048,
        "the first backup should match the cold store, got {after_first} against {cold}"
    );

    // Version 2 changes the second file, so its content is not in this version.
    let other = common::noisy(40_000);
    fs::write(source.join("changing.bin"), &other).unwrap();
    common::run_backup(&source, &blob).await;
    let after_second = common::set_size(&blob);

    // Version 3 puts the first file's content back.
    // It is in the store, because it has been there since version 1.
    // It is not in the version that came before.
    fs::write(source.join("changing.bin"), &changing).unwrap();
    let third = common::run_backup(&source, &blob).await;
    let after_third = common::set_size(&blob);

    // Version 3 costs what publishing a version costs.
    // That is a snapshot blob and a commit record.
    // Version 3 costs none of the chunk data, because every chunk it names was
    // already in the store.
    let reused = after_third - after_second;
    assert!(
        reused * 4 < cold,
        "a tree whose content the store still held cost {reused} bytes, against \
         {cold} bytes to store it the first time, so it was written again"
    );

    // And the version is genuinely restorable, which is the part that matters.
    // Skipping the write is only correct if the bytes were already there.
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
    let source = common::source(dir.path());

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
    // Every one of these files is one 64 KiB run of identical bytes.
    // The chunker cuts the run at its maximum size.
    // So each file is exactly one chunk, and all of them hash the same.
    // That is the case worth racing on: the same content reaching several stores
    // at once.
    // A deduplication check that only works in sequence would store a copy per
    // worker.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    let shared = vec![0x5au8; 64 * 1024];
    for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        fs::write(source.join(format!("{name}.bin")), &shared).unwrap();
    }

    // Plus content that must *not* be deduplicated, so a run cannot pass simply
    // by collapsing everything into one entry.
    fs::write(source.join("one.bin"), common::noisy(70 * 1024)).unwrap();
    fs::write(source.join("two.bin"), common::noisy(70 * 1024)).unwrap();

    let serial_blob = dir.path().join("serial.bin");
    let committed = common::run_backup_with(
        &source,
        &serial_blob,
        Options {
            concurrency: 1,
            ..Options::default()
        },
    )
    .await;
    let snap = snapshot(&serial_blob, &committed.snapshot).await;
    let serial = snap.chunks.len();

    // The eight identical files share one stored chunk.
    // The count below covers the noisy files too, and cut positions move it.
    let constant = blake3::hash(&shared);
    assert_eq!(
        snap.chunks.iter().filter(|hash| *hash == &constant).count(),
        1,
        "eight identical files did not collapse to one stored chunk"
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
    // must depend only on the order chunks were read in.
    // If this drifts, two backups of the same tree stop being comparable.
    // The concurrency knob then stops being safe to touch.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Several distinct files plus shared content between them, so a run has
    // chunks to deduplicate as well as chunks to keep in order.
    // The shared part is what makes two chunks with the same hash race for the
    // same cell.
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
        // Repeated, because the failure this guards against is a race.
        // It takes a later-read duplicate reaching a hash's cell before the
        // earlier-read one does.
        // That is a matter of scheduling rather than of input.
        // A single run can pass by luck, and did.
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
            // the hash-to-index mapping.
            // That mapping is the part that has to be a function of read order.
            // `chunks` is a bare hash now, so comparing it directly is comparing
            // content identity.
            // There is no offset in it any more.
            // Nothing about which store a run wrote to can leak into a snapshot.
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
    // The invariant the commit order exists to keep.
    // It is one entry per distinct content.
    // Many chunks may refer to that entry, and they may finish storing in any
    // order.
    //
    // Asserted directly rather than inferred.
    // The bug it guards against surfaced as a failed backup rather than as a
    // wrong snapshot.
    // Nothing about the output would have given it away.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Constant content, so the chunker cuts at its maximum size and every file
    // is a single chunk.
    // That maximises how many chunks collide on one hash.
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

        let mut seen = std::collections::HashSet::new();
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

/// A version older than the latest one is refused before it commits.
///
/// The run stamps the new version with the run time. A latest stamp from
/// the future therefore always predates it. Nothing is committed.
#[tokio::test]
async fn a_version_older_than_the_latest_one_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), b"first").unwrap();

    let blob = dir.path().join("store");
    common::run_backup(&source, &blob).await;

    let storage =
        BlobStore::builder(&blob).open_writer().await
            .unwrap();
    let future = Timestamp {
        secs: 9_999_999_999,
        nanos: 0,
    };
    let error = backup(&source, Options::default(), storage, Some(future))
        .await
        .expect_err("an older version should be refused");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("refusing to commit"),
        "the refusal should name the act, got: {error}"
    );

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    assert_eq!(
        store.versions().unwrap().len(),
        1,
        "a refused run must not have published a version"
    );
}

/// A version stamped at the latest time commits.
///
/// Equal stamps pass, so two runs in quick succession both land.
/// The stamp of the second run is never older than the first.
#[tokio::test]
async fn a_version_stamped_at_the_latest_time_commits() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(source.join("a.bin"), b"first").unwrap();

    let blob = dir.path().join("store");
    let first = common::run_backup(&source, &blob).await;

    let store = BlobStore::builder(&blob).open_reader().await.unwrap();
    let latest = store.version_time(1).await.unwrap();

    fs::write(source.join("b.bin"), b"second").unwrap();
    let storage =
        BlobStore::builder(&blob).open_writer().await
            .unwrap();
    let second = backup(&source, Options::default(), storage, Some(latest))
        .await
        .expect("an equal stamp should commit");

    assert_eq!(second.version, first.version + 1);
}
