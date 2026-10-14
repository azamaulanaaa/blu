// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A restore must only ever write inside the directory it was given.
//!
//! A snapshot is a blob in a file that gets synced, shared and backed up like
//! anything else, so its contents are untrusted input. These tests are the
//! guarantee that a tampered or hostile store cannot choose what a restore
//! writes, or where.
//!
//! The escape was confirmed working before it was fixed.
//! A snapshot recording `../../escaped-dir/owned.txt` was restored into a
//! destination two levels below it.
//! The restore landed outside, and the restore reported success.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
};

use lepatch::{
    command::restore,
    metadata::{Attributes, Dir, File, FileChunk, Link, Snapshot, Timestamp},
    storage::{BlobId, BlobStore, Compression, SegmentLimit, StoragePut},
};

/// A store holding a forged snapshot, and the hash it is under.
struct Forged {
    dir: tempfile::TempDir,
    blob: PathBuf,
    snapshot: BlobId,
}

impl Forged {
    /// Store `snapshot` in a fresh store, with its chunks' contents available so
    /// a restore that gets as far as writing would succeed.
    async fn new(mutate: impl FnOnce(&mut Snapshot)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("store.bin");
        let storage =
            BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
                .await
                .unwrap();

        let mut snapshot = Snapshot::new(Timestamp { secs: 1, nanos: 0 });
        mutate(&mut snapshot);

        // Give every referenced chunk real content.
        // A restore that passes the path checks then goes on to succeed.
        // The test can tell the two apart.
        let payload = b"OWNED\n";
        let hash = blake3::hash(payload);
        storage
            .put(&hash, Box::new(Cursor::new(payload.to_vec())), 6)
            .await
            .unwrap();

        if snapshot.chunks.is_empty() {
            snapshot.chunks.push(hash);
        }

        for slice in &mut snapshot.file_chunks {
            if slice.chunk_index as usize >= snapshot.chunks.len() {
                slice.chunk_index = 0;
            }
        }

        let encoded = snapshot.to_bytes().expect("encodes");
        let len = u64::try_from(encoded.len()).unwrap();
        let id = blake3::hash(&encoded);
        storage
            .put(&id, Box::new(Cursor::new(encoded)), len)
            .await
            .unwrap();

        // No commit: these snapshots are handed to a restore directly, and a
        // version of the store is not what is under test here.
        Self {
            dir,
            blob,
            snapshot: id,
        }
    }

    /// Restore into `dest`, and report the error if it refused.
    async fn restore_into(&self, dest: &Path) -> Result<(), String> {
        let storage = BlobStore::open_for_reading(&self.blob).await.unwrap();
        match restore(dest, self.snapshot, storage).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// A one-file snapshot whose only path is `path`.
fn snapshot_with_file(path: &str) -> impl FnOnce(&mut Snapshot) {
    let path = path.to_owned();
    move |snapshot: &mut Snapshot| {
        snapshot.files.push(File {
            path: PathBuf::from(&path),
            len: 6,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
            },
        });
        snapshot.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 0,
            length: 6,
        });
    }
}

#[tokio::test]
async fn a_file_path_escaping_the_root_is_refused() {
    let forged = Forged::new(snapshot_with_file("../../escaped.txt")).await;
    let dest = forged.dir.path().join("restore-here");
    fs::create_dir_all(&dest).unwrap();

    let error = forged.restore_into(&dest).await.expect_err("should refuse");

    assert!(
        error.contains("steps outside the restore root"),
        "unhelpful error: {error}"
    );

    // Nothing anywhere outside the destination.
    assert!(!forged.dir.path().join("escaped.txt").exists());
    assert!(dest.read_dir().unwrap().next().is_none(), "wrote something");
}

#[tokio::test]
async fn a_dot_dot_buried_mid_path_is_refused() {
    // The obvious case is a leading `..`.
    // A `..` in the middle is just as effective and much easier to miss.
    let forged = Forged::new(snapshot_with_file("a/b/../../../escaped.txt")).await;
    let dest = forged.dir.path().join("restore-here");
    fs::create_dir_all(&dest).unwrap();

    let error = forged.restore_into(&dest).await.expect_err("should refuse");
    assert!(
        error.contains("steps outside the restore root"),
        "unhelpful error: {error}"
    );
    assert!(dest.read_dir().unwrap().next().is_none(), "wrote something");
}

#[tokio::test]
async fn an_absolute_file_path_is_refused() {
    // A path no test run can collide with, and removed first so the test does not
    // depend on what a previous one left lying around.
    let outside = std::env::temp_dir().join("lepatch-absolute-path-should-not-exist");
    let _ = fs::remove_file(&outside);

    let forged = Forged::new(snapshot_with_file(&outside.display().to_string())).await;
    let dest = forged.dir.path().join("restore-here");
    fs::create_dir_all(&dest).unwrap();

    let error = forged.restore_into(&dest).await.expect_err("should refuse");
    assert!(error.contains("absolute path"), "unhelpful error: {error}");
    assert!(
        !outside.exists(),
        "the restore wrote outside its destination, to {}",
        outside.display()
    );
}

#[tokio::test]
async fn an_escaping_directory_path_is_refused() {
    let forged = Forged::new(|snapshot: &mut Snapshot| {
        snapshot.dirs.push(Dir {
            path: PathBuf::from("../../escaped-dir"),
            attrs: Attributes {
                owner: None,
                mode: Some(0o755),
                mtime: None,
            },
        });
    })
    .await;
    let dest = forged.dir.path().join("restore-here");
    fs::create_dir_all(&dest).unwrap();

    let error = forged.restore_into(&dest).await.expect_err("should refuse");
    assert!(
        error.contains("a directory"),
        "the error should say which kind of entry: {error}"
    );
    assert!(!forged.dir.path().join("escaped-dir").exists());
}

#[tokio::test]
async fn an_escaping_hard_link_target_is_refused() {
    // A hard link's second path names another entry in the tree.
    // The restore resolves it against the root, and it has to stay inside the
    // root.
    // This rule is the only thing that distinguishes the two link kinds.
    // This rule is why `Link` is a sum type rather than a struct with a `kind`.
    // The field is called `shares`, not `target`, because it is a claim about
    // this tree rather than a pointer.
    let forged = Forged::new(|snapshot: &mut Snapshot| {
        snapshot.files.push(File {
            path: PathBuf::from("innocent.txt"),
            len: 6,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
            },
        });
        snapshot.links.push(Link::Hard {
            path: PathBuf::from("sneaky.txt"),
            shares: PathBuf::from("../../etc/passwd"),
        });
    })
    .await;
    let dest = forged.dir.path().join("restore-here");
    fs::create_dir_all(&dest).unwrap();

    let error = forged.restore_into(&dest).await.expect_err("should refuse");
    assert!(
        error.contains("a hard link target"),
        "the error should name the entry: {error}"
    );
}

#[tokio::test]
async fn an_ordinary_symbolic_link_out_of_the_tree_is_restored() {
    // The counterpart to the case above, and the reason the rule differs by link
    // kind.
    // A restore writes a symbolic link's target into the link verbatim.
    // Whoever follows the link resolves that target.
    // So a tree containing a link to `/etc/passwd` is an ordinary tree.
    // Refusing it would break restoring real backups to accommodate a threat
    // that does not apply.
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("real.txt"), b"content").unwrap();
    std::os::unix::fs::symlink("/etc/passwd", src.join("to-etc")).unwrap();
    std::os::unix::fs::symlink("../nowhere", src.join("dangling")).unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&src, &blob).await;
    let dest = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &dest).await;

    assert_eq!(
        fs::read_link(dest.join("to-etc")).unwrap(),
        Path::new("/etc/passwd")
    );
    assert_eq!(
        fs::read_link(dest.join("dangling")).unwrap(),
        Path::new("../nowhere")
    );
    assert_eq!(fs::read(dest.join("real.txt")).unwrap(), b"content");
}

#[tokio::test]
async fn a_file_is_not_written_through_a_symlink_left_by_an_earlier_restore() {
    // Restores merge into their destination, so the second run finds what the
    // first left.
    // If that is a symbolic link, opening the file for writing follows it.
    // The content lands wherever the link points.
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("thing"), b"second version").unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&src, &blob).await;

    // A first restore that leaves a link where the file will go.
    let dest = dir.path().join("restored");
    fs::create_dir_all(&dest).unwrap();
    let victim = dir.path().join("victim.txt");
    fs::write(&victim, b"do not touch").unwrap();
    std::os::unix::fs::symlink(&victim, dest.join("thing")).unwrap();

    common::run_restore(&blob, committed.snapshot, &dest).await;

    assert_eq!(
        fs::read(&victim).unwrap(),
        b"do not touch",
        "the restore wrote through a symlink and modified a file outside the destination"
    );
    assert_eq!(fs::read(dest.join("thing")).unwrap(), b"second version");
}

#[tokio::test]
async fn an_ordinary_tree_still_restores() {
    // The fix must not cost anything.
    // Relative paths, nested directories and `.` components are all normal.
    // They all still work.
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(src.join("a/b/c")).unwrap();
    fs::write(src.join("a/b/c/deep.txt"), b"deep").unwrap();
    fs::write(src.join("top.txt"), b"top").unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&src, &blob).await;
    let dest = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &dest).await;

    assert_eq!(common::describe(&src), common::describe(&dest));
}

/// A snapshot whose recorded length is longer than its chunks is refused.
///
/// `File::len` is a number in a blob.
/// Nothing about a snapshot is trusted until it has been checked.
/// A length that the chunk slices do not account for is the easiest thing in
/// the format to get wrong.
/// It is one field, and nothing downstream of it can notice.
///
/// Restore is the dangerous direction.
/// A restore writes the recorded slices and then `set_len`s the file to the
/// recorded length, which is what closes a gap between slices.
/// A length longer than the slices therefore does not fail.
/// The restore *zero-fills*, and the result is a file the right size, with a
/// plausible amount of content, silently missing a tail.
/// A restore that claims success has lost data.
///
/// `backup` has always checked this.
/// `verify_file_lengths` is why a tree that changes mid-run is caught.
/// The check lived only on the writing side, where a hostile snapshot is not
/// the threat.
/// The same arithmetic, in the same terms, belongs wherever a snapshot is
/// consumed.
#[tokio::test]
async fn a_file_longer_than_its_chunks_is_refused() {
    let forged = Forged::new(|snapshot| {
        snapshot.files.push(File {
            path: PathBuf::from("a.bin"),
            // Six bytes are covered by the one slice below.
            // This claims twenty.
            len: 20,
            attrs: Attributes {
                owner: None,
                mode: Some(0o644),
                mtime: None,
            },
        });
        snapshot.file_chunks.push(FileChunk {
            chunk_index: 0,
            file_index: 0,
            chunk_offset: 0,
            file_offset: 0,
            length: 6,
        });
    })
    .await;

    let dest = forged.dir.path().join("restore-here");
    fs::create_dir_all(&dest).unwrap();

    let error = forged
        .restore_into(&dest)
        .await
        .expect_err("a length the chunks do not account for should be refused");

    assert!(
        error.contains("20")
            && (error.contains("6 bytes") || error.contains("does not match its own chunks")),
        "the error should say what was recorded and what the chunks cover: {error}"
    );

    // And nothing was written, rather than a zero-filled file being left behind.
    assert!(
        dest.read_dir().unwrap().next().is_none(),
        "a refused restore should not leave a file that looks complete"
    );
}
