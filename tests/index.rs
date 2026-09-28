//! The on-disk layout of a backup set: path naming and the write lock.
//!
//! Version numbering used to live here too, as a scan for `NNN.idx` files. It
//! does not any more: the store records its own versions in its commit records,
//! and a version number is a position in that log. What is left of this module
//! is the part that is still the store set's own business — where its files go,
//! and how two writers are kept apart.

use lepatch::index::IndexSet;

#[test]
fn derived_paths_do_not_clobber_an_extension_in_the_base_name() {
    // `Path::with_extension` turns this into `photos.bin` and drops `.tar`.
    let set = IndexSet::new("photos.tar");

    assert_eq!(set.blob_path(), std::path::Path::new("photos.tar.bin"));
    assert_eq!(set.lock_path(), std::path::Path::new("photos.tar.lock"));
}

#[test]
fn a_backup_set_is_one_file_and_a_lock() {
    // The whole point of the commit record: publishing a version is a single
    // append to a single file, so there is no second file whose contents have to
    // be kept in step with the first. If this ever grows a third path again, the
    // cross-file commit is back.
    let set = IndexSet::new("photos");

    assert_eq!(set.blob_path(), std::path::Path::new("photos.bin"));
    assert_eq!(set.lock_path(), std::path::Path::new("photos.lock"));
    assert_eq!(set.base(), std::path::Path::new("photos"));
}

#[test]
fn derived_paths_honour_a_directory_component() {
    let set = IndexSet::new("backups/photos");

    assert_eq!(set.blob_path(), std::path::Path::new("backups/photos.bin"));
    assert_eq!(set.lock_path(), std::path::Path::new("backups/photos.lock"));
    assert_eq!(set.directory(), std::path::Path::new("backups"));
}

#[test]
fn a_bare_name_resolves_against_the_current_directory() {
    let set = IndexSet::new("photos");

    assert_eq!(set.directory(), std::path::Path::new("."));
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
fn taking_the_lock_creates_the_containing_directory() {
    let dir = tempfile::tempdir().unwrap();
    let set = IndexSet::new(dir.path().join("nested/deeper/photos"));

    assert!(!set.directory().exists());

    let _lock = set.lock().unwrap();

    assert!(set.directory().is_dir());
    assert!(set.lock_path().exists());
}
