//! Path handling, in particular paths that are not valid UTF-8.

mod common;

use std::fs;

use common::assert_round_trip;

/// A filename that is not valid UTF-8 must survive the round trip rather than
/// aborting the whole backup, which is what `PathBuf`'s default `Serialize`
/// impl did when the snapshot encoding was delegated to a serialisation crate.
/// Both are gone: the encoding is hand-written and the path is written as its
/// raw bytes (`Writer::path` in `src/metadata/codec.rs`), so this test now
/// guards a property rather than a regression.
#[cfg(unix)]
#[tokio::test]
async fn round_trip_preserves_paths_that_are_not_valid_utf8() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::PathBuf};

    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let raw_name = OsStr::from_bytes(b"invalid-\xff\xfe-name.bin");
    fs::write(PathBuf::from(&source).join(raw_name), b"payload").unwrap();

    // A file in a directory whose own name is not valid UTF-8.
    let raw_dir = OsStr::from_bytes(b"dir-\xff");
    fs::create_dir(PathBuf::from(&source).join(raw_dir)).unwrap();
    fs::write(
        PathBuf::from(&source).join(raw_dir).join("inner"),
        b"nested",
    )
    .unwrap();

    assert_round_trip(&source).await;
}
