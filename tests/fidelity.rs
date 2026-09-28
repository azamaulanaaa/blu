//! Fidelity of the restored tree: everything a backup has to reproduce beyond
//! raw file contents.

mod common;

use std::fs;

use common::assert_round_trip;

#[tokio::test]
async fn round_trip_recreates_empty_files() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("empty"), b"").unwrap();
    fs::write(source.join("nested/also-empty"), b"").unwrap();
    fs::write(source.join("not-empty"), b"data").unwrap();

    assert_round_trip(&source).await;
}

#[tokio::test]
async fn round_trip_recreates_empty_directories() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(source.join("a/b/c")).unwrap();
    fs::write(source.join("a/has-content"), b"x").unwrap();

    assert_round_trip(&source).await;
}

#[cfg(unix)]
#[tokio::test]
async fn round_trip_recreates_symbolic_links() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(source.join("subdir")).unwrap();
    fs::write(source.join("target.txt"), b"payload").unwrap();

    std::os::unix::fs::symlink("target.txt", source.join("relative-link")).unwrap();
    std::os::unix::fs::symlink("subdir", source.join("dir-link")).unwrap();
    std::os::unix::fs::symlink("/does/not/exist", source.join("dangling")).unwrap();

    assert_round_trip(&source).await;
}

#[cfg(unix)]
#[tokio::test]
async fn round_trip_recreates_hard_links() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("original"), b"shared contents").unwrap();

    // Three names for one inode: the first carries the content, the rest must
    // come back as links rather than as independent copies.
    std::fs::hard_link(source.join("original"), source.join("second")).unwrap();
    std::fs::hard_link(source.join("original"), source.join("third")).unwrap();

    assert_round_trip(&source).await;
}

#[cfg(unix)]
#[tokio::test]
async fn round_trip_preserves_permission_bits() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    fs::write(source.join("private"), b"secret").unwrap();
    fs::set_permissions(source.join("private"), fs::Permissions::from_mode(0o600)).unwrap();

    fs::write(source.join("executable"), b"#!/bin/sh\n").unwrap();
    fs::set_permissions(source.join("executable"), fs::Permissions::from_mode(0o755)).unwrap();

    fs::set_permissions(&source, fs::Permissions::from_mode(0o750)).unwrap();

    assert_round_trip(&source).await;
}

#[tokio::test]
async fn restore_does_not_leave_trailing_bytes_from_a_previous_larger_file() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("shrunk"), b"short").unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    // Restore over a file that is much longer than the backed-up one.
    let destination = dir.path().join("restored");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("shrunk"), vec![0xffu8; 10_000]).unwrap();

    common::run_restore(&blob, committed.snapshot, &destination).await;

    assert_eq!(fs::read(destination.join("shrunk")).unwrap(), b"short");
}

#[tokio::test]
async fn restore_over_an_existing_tree_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(source.join("sub")).unwrap();
    fs::write(source.join("sub/file"), b"fresh").unwrap();

    let blob = dir.path().join("store.bin");
    let committed = common::run_backup(&source, &blob).await;

    let destination = dir.path().join("restored");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("stale"), b"left over").unwrap();

    common::run_restore(&blob, committed.snapshot, &destination).await;

    assert_eq!(fs::read(destination.join("sub/file")).unwrap(), b"fresh");
}
