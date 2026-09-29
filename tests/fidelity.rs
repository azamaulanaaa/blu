//! Fidelity of the restored tree: everything a backup has to reproduce beyond
//! raw file contents.

mod common;

use std::fs;

use common::assert_round_trip;

#[tokio::test]
async fn round_trip_recreates_empty_files() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
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
    let source = common::source(dir.path());
    fs::write(source.join("original"), b"shared contents").unwrap();

    // Three names for one inode: the first carries the content, the rest must
    // come back as links rather than as independent copies.
    std::fs::hard_link(source.join("original"), source.join("second")).unwrap();
    std::fs::hard_link(source.join("original"), source.join("third")).unwrap();

    assert_round_trip(&source).await;
}

/// A backup root that is not a directory is refused, not backed up as an empty path.
///
/// The walk skips the root entry so that the destination directory itself is
/// never recorded as an entry inside itself — but the skip was conditioned on
/// the root being a *directory*, which it need not be. A regular file root
/// reaches `strip_prefix(root)` and comes back empty, and the entry is recorded
/// with an empty path. So does a symbolic link to a directory, because the walk
/// stats with `symlink_metadata` and never follows it.
///
/// Both cases produce a version that commits successfully and can then never be
/// restored: `command::restore` refuses an empty path before writing anything,
/// and there is no subcommand that removes a version. Refusing up front is the
/// only point at which this is cheap to report.
#[tokio::test]
async fn a_backup_root_that_is_a_file_is_refused() {
    use lepatch::{
        command::{Options, backup},
        storage::{BlobStore, Compression, SegmentLimit},
    };

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("notes.md");
    fs::write(&file, b"not a tree").unwrap();

    let blob = dir.path().join("store.bin");
    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();

    let error = backup(&file, Options::default(), store)
        .await
        .expect_err("a file is not a tree, and backing one up records an empty path");
    let message = error.to_string();
    assert!(
        message.contains("directory"),
        "the refusal should say what a backup root has to be: {message}"
    );
}

/// A symbolic link to a directory is a directory, and is backed up as one.
///
/// The companion to the test above, and the more likely of the two in practice:
/// `~/current-photos` pointing at a versioned directory is an ordinary thing to
/// type. Following it is what a user means, and it is what a shell would do.
#[cfg(unix)]
#[tokio::test]
async fn a_backup_root_that_is_a_symlink_to_a_directory_is_followed() {
    use lepatch::{
        command::{Options, backup},
        storage::{BlobStore, Compression, SegmentLimit},
    };

    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("realdir");
    fs::create_dir_all(&real).unwrap();
    fs::write(real.join("f.txt"), b"payload").unwrap();

    let link = dir.path().join("linkdir");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    let blob = dir.path().join("store.bin");
    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();
    let committed = backup(&link, Options::default(), store)
        .await
        .expect("a symlink to a directory names a directory");

    // And the version it produced is one a restore can actually use.
    let destination = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &destination).await;
    assert_eq!(fs::read(destination.join("f.txt")).unwrap(), b"payload");
}

#[cfg(unix)]
#[tokio::test]
async fn round_trip_preserves_permission_bits() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

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
    let source = common::source(dir.path());
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

/// A directory's recorded time survives the links created inside it.
///
/// The restore sets attributes *after* recreating links, and that order is load
/// bearing in a way almost nothing else about a restore is. Creating an entry
/// inside a directory updates that directory's modification time, so a restore that
/// set the time first and made the links afterwards would leave every directory
/// holding a link stamped with the *restore's* time instead of the recorded one.
///
/// Nothing reports that. The file contents are right, the link is right, `verify`
/// passes — `verify` checks chunk hashes, and a directory's timestamp is not in any
/// of them. The only evidence is `stat`, which is exactly the kind of fidelity
/// claim that gets checked once and then assumed forever.
///
/// **Verified by reversing the order**, not by reading the code: with attributes
/// applied before links, the restored directory's time comes back as the restore
/// time (2026-…) rather than the recorded one (2021-…).
///
/// The recorded time is made old by *waiting* rather than by setting it. The crate
/// forbids `unsafe` everywhere including tests, so `utimensat` is out;
/// `File::set_times` is safe but `OpenOptions` will not open a directory on Linux;
/// and a `touch` subprocess would make the test depend on a locale and on `PATH`.
///
/// Unix only, because a platform without permission bits has no directory time to
/// compare — and the test's own premise is the platform that has one.
#[cfg(unix)]
#[tokio::test]
async fn a_directory_holding_a_link_keeps_its_recorded_time() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    let holding = source.join("holding");
    fs::create_dir_all(&holding).unwrap();
    fs::write(holding.join("file.bin"), common::noisy(20_000)).unwrap();
    std::os::unix::fs::symlink("file.bin", holding.join("link")).unwrap();

    // Creating the link above is the last thing to touch the directory, so its time
    // is the moment the tree was built — which is what gets recorded.
    let recorded = whole_seconds_since_epoch(fs::metadata(&holding).unwrap().modified().unwrap());
    let taken_at = std::time::SystemTime::now();

    // Wait until the wall clock is in a later second than the directory's, so the
    // two answers cannot be equal by accident. A filesystem with one-second
    // resolution would otherwise make a reversed restore look correct whenever the
    // whole round trip finished inside the same second — which a small fixture
    // does, reliably.
    let elapsed = taken_at
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs();
    if elapsed <= recorded {
        std::thread::sleep(
            std::time::Duration::from_secs(recorded + 1 - elapsed)
                + std::time::Duration::from_millis(20),
        );
    }

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;

    let destination = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &destination).await;

    let restored = destination.join("holding");
    assert_eq!(
        fs::read_link(restored.join("link")).expect("the link was recreated"),
        std::path::PathBuf::from("file.bin"),
        "the link should point where it did"
    );

    let got = whole_seconds_since_epoch(
        fs::metadata(&restored)
            .expect("the directory was recreated")
            .modified()
            .expect("a platform with a modification time"),
    );

    assert_eq!(
        got, recorded,
        "the directory's time should be the one that was recorded. A time at or after \
         {recorded} + 1s means attributes were applied before the links were created, \
         and making a link inside a directory updates the directory's time"
    );
}

/// Whole seconds since the epoch, which is the resolution a directory's
/// modification time is compared at even where it is recorded more finely.
///
/// The `allow` is the one `tests/common/` uses, and for the same reason: this is
/// not inside a `#[test]`, so it is not covered by the test exemption in
/// `.clippy.toml`. A timestamp before 1970 is the only way this fails, and the
/// filesystem did not report one.
#[cfg(unix)]
#[allow(clippy::expect_used)]
fn whole_seconds_since_epoch(when: std::time::SystemTime) -> u64 {
    when.duration_since(std::time::UNIX_EPOCH)
        .expect("a timestamp after the epoch")
        .as_secs()
}
