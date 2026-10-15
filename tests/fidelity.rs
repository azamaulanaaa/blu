// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
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

    // Three names for one inode.
    // The first carries the content.
    // The rest must come back as links rather than as independent copies.
    std::fs::hard_link(source.join("original"), source.join("second")).unwrap();
    std::fs::hard_link(source.join("original"), source.join("third")).unwrap();

    assert_round_trip(&source).await;
}

/// A backup root that is not a directory is refused, not backed up as an empty path.
///
/// The walk skips the root entry so that the destination directory itself is
/// never recorded as an entry inside itself.
/// The skip was conditioned on the root being a *directory*, which it need not
/// be.
/// A regular file root reaches `strip_prefix(root)` and comes back empty.
/// The entry is then recorded with an empty path.
/// A symbolic link to a directory does the same, because the walk stats with
/// `symlink_metadata` and never follows it.
///
/// Both cases produce a version that commits successfully and can then never be
/// restored.
/// `command::restore` refuses an empty path before writing anything.
/// There is no subcommand that removes a version.
/// Refusing up front is the only point at which this is cheap to report.
#[tokio::test]
async fn a_backup_root_that_is_a_file_is_refused() {
    use blulib::{command::BackupBuilder, storage::BlobStore};

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("notes.md");
    fs::write(&file, b"not a tree").unwrap();

    let blob = dir.path().join("store.bin");
    let store = BlobStore::builder(&blob).open_writer().await.unwrap();

    let error = BackupBuilder::new(&file, store)
        .latest(None)
        .run()
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
/// The companion to the test above, and the more likely of the two in practice.
/// `~/current-photos` pointing at a versioned directory is an ordinary thing to
/// type.
/// Following it is what a user means, and it is what a shell would do.
#[cfg(unix)]
#[tokio::test]
async fn a_backup_root_that_is_a_symlink_to_a_directory_is_followed() {
    use blulib::{command::BackupBuilder, storage::BlobStore};

    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("realdir");
    fs::create_dir_all(&real).unwrap();
    fs::write(real.join("f.txt"), b"payload").unwrap();

    let link = dir.path().join("linkdir");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    let blob = dir.path().join("store.bin");
    let store = BlobStore::builder(&blob).open_writer().await.unwrap();
    let committed = BackupBuilder::new(&link, store)
        .latest(None)
        .run()
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

/// A restore without `--clean` leaves an entry the version does not name alone,
/// and writes the entries it does.
///
/// The name says the property rather than "succeeds": the interesting half is
/// the stale file, because a restore that pruned it would be correct against a
/// version whose tree it happens to match, and wrong against every other one.
#[tokio::test]
async fn a_restore_without_clean_leaves_a_stale_entry_in_place() {
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

    // The version's own entry is written.
    assert_eq!(fs::read(destination.join("sub/file")).unwrap(), b"fresh");
    // And the entry the version does not name is still there, byte for byte.
    assert_eq!(
        fs::read(destination.join("stale")).unwrap(),
        b"left over",
        "a restore without --clean must not remove an entry the version does not name"
    );
}

/// A directory's recorded time survives the links created inside it.
///
/// The restore sets attributes *after* recreating links.
/// That order matters in a way almost nothing else about a restore matters.
/// Creating an entry inside a directory updates that directory's modification
/// time.
/// So a restore that set the time first and made the links afterwards would leave
/// every directory holding a link stamped with the *restore's* time instead of
/// the recorded one.
///
/// Nothing reports that.
/// The file contents are right, the link is right, `verify` passes.
/// `verify` checks chunk hashes, and a directory's timestamp is not in any of
/// them.
/// The only evidence is `stat`.
/// That is exactly a fidelity claim of the kind that gets checked once and then
/// assumed forever.
///
/// **Verified by reversing the order**, not by reading the code.
/// With attributes applied before links, the restored directory's time comes back
/// as the restore time (2026-…), and not as the recorded one (2021-…).
///
/// The test makes the recorded time old by *waiting* rather than by setting it.
/// The crate forbids `unsafe` everywhere including tests, so `utimensat` is out.
/// `File::set_times` is safe but `OpenOptions` will not open a directory on
/// Linux.
/// A `touch` subprocess would make the test depend on a locale and on `PATH`.
///
/// Unix only, because a platform without permission bits has no directory time to
/// compare.
/// The test's own premise is a platform that has one.
#[cfg(unix)]
#[tokio::test]
async fn a_directory_holding_a_link_keeps_its_recorded_time() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    let holding = source.join("holding");
    fs::create_dir_all(&holding).unwrap();
    fs::write(holding.join("file.bin"), common::noisy(20_000)).unwrap();
    std::os::unix::fs::symlink("file.bin", holding.join("link")).unwrap();

    // Creating the link above is the last thing to touch the directory.
    // So its time is the moment the tree was built, and that is what gets
    // recorded.
    let recorded = whole_seconds_since_epoch(fs::metadata(&holding).unwrap().modified().unwrap());
    let taken_at = std::time::SystemTime::now();

    // Wait until the wall clock is in a later second than the directory's.
    // That way the two answers cannot be equal by accident.
    // A filesystem with one-second resolution would otherwise make a reversed
    // restore look correct whenever the whole round trip finished inside the same
    // second.
    // A small fixture does finish inside the same second, reliably.
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

/// Whole seconds since the epoch.
/// That is the resolution at which a directory's modification time is compared,
/// even where the filesystem records it more finely.
///
/// The `allow` is the one `tests/common/` uses, and for the same reason.
/// This is not inside a `#[test]`, so the test exemption in `.clippy.toml` does
/// not cover it.
/// A timestamp before 1970 is the only way this fails.
/// The filesystem did not report one.
#[cfg(unix)]
#[allow(clippy::expect_used)]
fn whole_seconds_since_epoch(when: std::time::SystemTime) -> u64 {
    when.duration_since(std::time::UNIX_EPOCH)
        .expect("a timestamp after the epoch")
        .as_secs()
}

/// The set-user-ID bit survives a restore that changes a file's owner.
///
/// The ordering constraint, pinned as an outcome rather than as a code reading.
/// Linux clears setuid and setgid when a file's owner changes.
/// So an `apply` that set the mode and *then* the owner would strip the bit from
/// everything it restored.
/// It would also look like a success.
/// The bytes match, and the lengths match.
/// The only thing wrong is a bit nobody was looking for.
///
/// The whole value of recording ownership is that a system restore brings back a
/// working `su` and `passwd`.
/// Getting this order wrong quietly removes exactly that.
///
/// Root only, because it is the only caller that can change an owner.
/// It is also the only one where the two orders differ.
/// For anyone else both orders produce the same file.
/// That is why the bug can sit unnoticed until someone restores `/usr` as root and
/// finds `su` no longer works.
#[cfg(unix)]
#[tokio::test]
async fn the_setuid_bit_survives_a_restore_that_changes_the_owner() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let file = source.join("su");
    fs::write(&file, b"#!/bin/sh\n").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o4755)).unwrap();

    assert_eq!(
        fs::metadata(&file).unwrap().mode() & 0o4000,
        0o4000,
        "sanity: the source file is setuid before anything happens"
    );

    if fs::metadata(&file).unwrap().uid() != 0 {
        // Not root: the owner cannot be changed, so both orders produce the same
        // file and there is nothing here to distinguish.
        // The test skips rather than passes, so a green run is not mistaken for
        // the ordering having been checked.
        eprintln!("skipped: changing an owner needs root");
        return;
    }

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;
    common::run_restore(&blob, committed.snapshot, &dir.path().join("restored")).await;

    let restored = dir.path().join("restored/su");
    assert_eq!(
        fs::metadata(&restored).unwrap().mode() & 0o4000,
        0o4000,
        "the restored file is still setuid, which it would not be if the mode were \
         applied before the owner"
    );
}

/// The owner of a file survives a restore, on a platform that records one.
///
/// Unix only, and root-gated for the same reason as the setuid case.
/// An unprivileged restore cannot change an owner.
/// So there is nothing for the assertion to be about.
/// The test skips rather than passing quietly.
/// "The tree matched" is true either way, and would read as though ownership had
/// been verified.
#[cfg(unix)]
#[tokio::test]
async fn an_owner_survives_a_restore() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let owned = source.join("owned");
    fs::write(&owned, b"data").unwrap();

    if fs::metadata(&owned).unwrap().uid() != 0 {
        eprintln!("skipped: changing an owner needs root");
        return;
    }

    // An owner nothing else in the tree has.
    // So "the same owner" is not satisfied by the file having been created by the
    // test process.
    let wanted = 20_001;
    std::os::unix::fs::chown(&owned, Some(wanted), None).unwrap();

    let blob = dir.path().join("store");
    let committed = common::run_backup(&source, &blob).await;
    let restored = dir.path().join("restored");
    common::run_restore(&blob, committed.snapshot, &restored).await;

    assert_eq!(
        fs::metadata(restored.join("owned")).unwrap().uid(),
        wanted,
        "the restored file should be owned by whoever the backup recorded"
    );
}

/// The owner and group a file has, as the filesystem reports them.
///
/// Unix only.
/// A file's owner is the identity that created it, so there is nothing to compare
/// against but what it already was.
/// The point of these tests is that `owner_of` and `set_owner` agree about the
/// same file.
/// The point is not that either agrees about some other one.
///
/// The `allow` is `tests/common/`'s, for the same reason.
/// This is not inside a `#[test]`, so the exemption in `.clippy.toml` does not
/// reach it.
#[cfg(unix)]
#[allow(clippy::unwrap_used)]
fn owner_and_group(path: &std::path::Path) -> (u32, u32) {
    use std::os::unix::fs::MetadataExt;

    let meta = fs::metadata(path).unwrap();
    (meta.uid(), meta.gid())
}

/// Reading a file's owner gives back what `stat` says.
#[test]
fn an_owner_is_read_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f");
    fs::write(&file, b"x").unwrap();

    let read = blulib::fsattr::owner_of(&fs::metadata(&file).unwrap()).unwrap();
    assert_eq!(
        read,
        owner_and_group(&file),
        "what owner_of reports should be what the filesystem holds"
    );
}

/// An owner can be put back, which is the whole point of recording one.
#[test]
#[cfg(unix)]
fn an_owner_is_applied_to_a_file() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f");
    fs::write(&file, b"x").unwrap();

    if fs::metadata(&file).unwrap().uid() == 0 {
        // Root can change the answer, which is the only way to check the call
        // actually does something rather than merely not failing.
        let (uid, gid) = (12_345, 23_456);
        blulib::fsattr::set_owner(&file, Some((uid, gid))).unwrap();

        let after = fs::metadata(&file).unwrap();
        assert_eq!(
            (after.uid(), after.gid()),
            (uid, gid),
            "a privileged caller should get the owner it asked for"
        );
        return;
    }

    // Unprivileged, setting the owner a file already has is the most a test can
    // assert.
    // It is a call the filesystem accepts without privilege.
    // That call is what makes an unprivileged restore of an unchanged tree work
    // at all.
    let before = owner_and_group(&file);
    blulib::fsattr::set_owner(&file, Some(before)).unwrap();

    assert_eq!(
        owner_and_group(&file),
        before,
        "setting the owner a file already has should leave it alone"
    );
}

/// No owner recorded means nothing applied, on every platform.
#[test]
fn an_absent_owner_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f");
    fs::write(&file, b"x").unwrap();

    #[cfg(unix)]
    let before = owner_and_group(&file);

    blulib::fsattr::set_owner(&file, None).unwrap();

    #[cfg(unix)]
    assert_eq!(
        owner_and_group(&file),
        before,
        "a snapshot from a platform with no users should change nothing"
    );
}

/// A symbolic link is skipped rather than followed.
///
/// Following one to set its destination's owner would be a claim about a file the
/// snapshot does not describe.
/// The link records a target, not a version of it.
/// The target is restored from its own recorded entry.
#[test]
#[cfg(unix)]
fn an_owners_target_is_not_followed_for_a_symbolic_link() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let link = dir.path().join("link");
    fs::write(&target, b"x").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let target_before = owner_and_group(&target);

    // An owner the link itself could not have, so applying it to the target would be
    // visible. `set_owner` skips the link either way.
    blulib::fsattr::set_owner(&link, Some((target_before.0 + 1, target_before.1))).unwrap();

    assert_eq!(
        owner_and_group(&target),
        target_before,
        "setting a link's owner must not reach its destination"
    );
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink(),
        "and the link is still a link"
    );
}
