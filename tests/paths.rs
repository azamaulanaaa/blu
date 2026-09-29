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
    let source = common::source(dir.path());

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

/// Where a path is text, a name the encoding cannot hold is refused.
///
/// The unix arm above is the interesting one and gets most of the attention,
/// because on unix a path is a byte string and writing the bytes round-trips
/// exactly. This is the other platform, and it had the opposite behaviour.
///
/// `Writer::path` used to reach for `to_string_lossy` off unix. That does not
/// fail — it replaces whatever it cannot represent with `U+FFFD` and returns a
/// perfectly good `&str`. So two names differing only in an unrepresentable byte
/// would encode to *the same bytes*, and a restore would write both at one place,
/// having recorded a path that was never on disk. The decoder was strict while the
/// encoder was lossy, which is the worst pairing: the lossy half is the one nobody
/// sees.
///
/// Now it refuses, and says why. A tree the format cannot represent is refused
/// at the point of encoding, which is where a person is told.
///
/// This test cannot run on unix — there, the same tree round-trips, which is the
/// test above. It was written against the `not(unix)` arm by compiling that arm
/// on unix (`#[cfg(not(unix))]` → `#[cfg(all())]` in `src/metadata/codec.rs`) and
/// running it, so the code path is verified even though CI here never executes it.
#[cfg(not(unix))]
#[tokio::test]
async fn a_path_this_platform_cannot_represent_is_refused() {
    use std::{ffi::OsString, path::PathBuf};

    use lepatch::command::{Options, backup};
    use lepatch::storage::{BlobStore, Compression, SegmentLimit};

    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // A file name that is not valid UTF-8, built the way Windows allows: valid
    // UTF-16 with an unpaired surrogate, which is a legal name there and
    // impossible to express as text.
    let raw = {
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0x0066, 0xD800, 0x0069, 0x006C, 0x0065])
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![b'f', 0xED, b'i', b'l', b'e'])
        }
    };
    fs::write(PathBuf::from(&source).join(&raw), b"payload").unwrap();

    let blob = dir.path().join("store");
    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
        .await
        .unwrap();

    let error = backup(&source, Options::default(), store)
        .await
        .expect_err("a name this platform cannot write should be refused, not mangled");

    let message = error.to_string();
    assert!(
        message.contains("not valid text"),
        "the refusal should say what is wrong with the name: {message}"
    );
    assert!(
        !message.contains('\u{fffd}'),
        "and must not have mangled it on the way: {message}"
    );
}
