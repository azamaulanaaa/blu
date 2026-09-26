//! The on-disk layout of a backup set: path naming, version numbering and
//! crash recovery.

use std::fs;

use lepatch::index::IndexSet;

#[test]
fn derived_paths_do_not_clobber_an_extension_in_the_base_name() {
    // `Path::with_extension` turns this into `photos.bin` and drops `.tar`.
    let set = IndexSet::new("photos.tar");

    assert_eq!(set.blob_path(), std::path::Path::new("photos.tar.bin"));
    assert_eq!(set.lock_path(), std::path::Path::new("photos.tar.lock"));
    assert_eq!(
        set.index_path(1),
        std::path::Path::new("photos.tar.001.idx")
    );
}

#[test]
fn derived_paths_honour_a_directory_component() {
    let set = IndexSet::new("backups/photos");

    assert_eq!(
        set.blob_path(),
        std::path::Path::new("backups/photos.bin")
    );
    assert_eq!(
        set.index_path(12),
        std::path::Path::new("backups/photos.012.idx")
    );
    assert_eq!(set.directory(), std::path::Path::new("backups"));
}

#[test]
fn a_bare_name_resolves_against_the_current_directory() {
    let set = IndexSet::new("photos");

    assert_eq!(set.directory(), std::path::Path::new("."));
}

#[test]
fn the_first_version_is_one() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    let versions = set.versions().unwrap();
    assert_eq!(versions, lepatch::index::Versions::default());

    let reservation = set.reserve().unwrap();
    assert_eq!(reservation.version(), 1);
    reservation.commit("key-one").unwrap();

    let versions = set.versions().unwrap();
    assert_eq!(versions.highest_existing, Some(1));
    assert_eq!(versions.latest, Some((1, "key-one".to_string())));
}

#[test]
fn versions_increment_and_never_reuse_a_number() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    for expected in 1..=3 {
        let reservation = set.reserve().unwrap();
        assert_eq!(reservation.version(), expected);
        reservation.commit(&format!("key-{expected}")).unwrap();
    }

    let versions = set.versions().unwrap();
    assert_eq!(versions.highest_existing, Some(3));
    assert_eq!(versions.latest, Some((3, "key-3".to_string())));
}

#[test]
fn a_failed_reservation_leaves_no_index_behind() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    // Dropping without committing is what a failed backup does.
    drop(set.reserve().unwrap());

    assert!(!set.index_path(1).exists());
    assert_eq!(set.versions().unwrap().highest_existing, None);

    // The number is free again, so the next run is still version 1.
    assert_eq!(set.reserve().unwrap().version(), 1);
}

#[test]
fn an_empty_index_left_by_a_crash_is_not_treated_as_the_latest_version() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    set.reserve().unwrap().commit("good-key").unwrap();

    // Simulate a process killed after claiming version 2: the file exists but
    // was never filled in.
    fs::write(set.index_path(2), b"").unwrap();

    let versions = set.versions().unwrap();

    // It counts as existing, so version 2 is not handed out again...
    assert_eq!(versions.highest_existing, Some(2));
    // ...but it is not a usable "latest", so a plain restore still finds the
    // last snapshot that actually completed.
    assert_eq!(versions.latest, Some((1, "good-key".to_string())));

    assert_eq!(set.reserve().unwrap().version(), 3);
}

#[test]
fn a_malformed_index_is_ignored_rather_than_becoming_the_latest_version() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    set.reserve().unwrap().commit("good-key").unwrap();
    fs::write(set.index_path(2), b"key-one\nkey-two\n").unwrap();

    let versions = set.versions().unwrap();
    assert_eq!(versions.latest, Some((1, "good-key".to_string())));
}

#[test]
fn reading_a_version_that_has_no_usable_index_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    let missing = set.read_key(7).unwrap_err();
    assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
    assert!(
        missing.to_string().contains("version 7"),
        "error should name the version, got: {missing}"
    );
}

#[test]
fn files_belonging_to_other_backup_sets_are_not_mistaken_for_versions() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    set.reserve().unwrap().commit("key-one").unwrap();

    // A different base, a different extension, a wrong digit count, and a
    // non-numeric middle segment.
    fs::write(dir.path().join("videos.001.idx"), b"other").unwrap();
    fs::write(dir.path().join("photos.001.txt"), b"other").unwrap();
    fs::write(dir.path().join("photos.1.idx"), b"other").unwrap();
    fs::write(dir.path().join("photos.abc.idx"), b"other").unwrap();

    let versions = set.versions().unwrap();
    assert_eq!(versions.highest_existing, Some(1));
    assert_eq!(versions.latest, Some((1, "key-one".to_string())));
}

#[test]
fn a_lock_excludes_a_second_holder_and_is_released_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("photos"));

    {
        let _lock = set.lock().unwrap();
        assert!(set.lock_path().exists());

        let second = set.lock().unwrap_err();
        assert!(
            second.to_string().contains("remove it"),
            "the error should say how to recover, got: {second}"
        );
    }

    assert!(!set.lock_path().exists());
    assert!(set.lock().is_ok());
}

#[test]
fn reserving_creates_the_containing_directory() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("nested/deeper/photos"));

    assert!(!set.directory().exists());

    let reservation = set.reserve().unwrap();

    assert!(set.directory().is_dir());
    assert!(set.index_path(reservation.version()).exists());
}
