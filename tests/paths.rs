//! Path handling, in particular paths that are not valid UTF-8.

mod common;

use std::fs;

use common::assert_round_trip;
use lepatch::layout::IndexSet;

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

/// A segment's path names the set it belongs to, so either spelling opens the
/// same set.
///
/// `photos.0000.bin` is the name a directory listing shows and the one a person
/// copies out of it. If only `photos` worked, the obvious thing to type would be
/// the thing that does not, and the resulting error — a new empty set beside the
/// old one — is the kind that costs an afternoon.
///
/// What would falsify it: a name that is not shaped like a segment being
/// unwrapped anyway, which is the other half of this rule and is tested beside
/// it.
#[test]
fn a_segment_path_names_the_set_it_belongs_to() {
    use lepatch::layout::IndexSet;

    for (given, expected) in [
        ("photos.0000.bin", "photos"),
        ("photos.0007.bin", "photos"),
        ("/data/photos.0000.bin", "/data/photos"),
        ("/data/nightly/photos.0001.bin", "/data/nightly/photos"),
        // A base that itself contains dots, and a tail that is only
        // segment-shaped after one is removed.
        ("my.set.0000.bin", "my.set"),
        // The name it already had, and the shapes next to it that are not
        // segments.
        ("photos", "photos"),
        ("photos.tar", "photos.tar"),
        ("photos.v0001.idx", "photos.v0001.idx"),
        ("photos.lock", "photos.lock"),
        ("photos.0000.bin.tmp", "photos.0000.bin.tmp"),
        ("photos.00000.bin", "photos.00000.bin"),
        ("photos.000.bin", "photos.000.bin"),
        ("photos.000a.bin", "photos.000a.bin"),
        (".0000.bin", ".0000.bin"),
        // A base whose last character is two bytes, so the tail's arithmetic
        // counts four bytes off a three-character name and lands in the middle of
        // it. Left exactly as it was given: unwrapping it needs a byte boundary
        // the name does not have, and a panic here is what a set named by a
        // person rather than a script would get.
        ("café0000.bin", "café0000.bin"),
        ("café.0000.bin", "café"),
    ] {
        assert_eq!(
            IndexSet::new(given).base(),
            std::path::Path::new(expected),
            "{given} should name the set {expected}"
        );
    }
}

/// The two spellings must be one set on disk, not two.
///
/// Unwrapping the name is only half of it: what a backup writes has to land where
/// the other spelling looks, or `backup src photos` followed by
/// `restore out photos.0000.bin` would be two sets that each think they hold
/// nothing.
///
/// Which is the scenario this used to describe and not run: it wrote the same
/// spelling into two different directories and asserted the same two things twice,
/// so a set written as `photos` and read as `photos.0000.bin` — the case the
/// rule exists for — was never put through a backup at all. It is now the only
/// thing here: one set, written one way and read the other.
#[tokio::test]
async fn a_backup_named_by_a_segment_path_lands_in_that_set() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    fs::write(
        source.join("f.bin"),
        b"content that must survive the round trip\n",
    )
    .unwrap();

    let parent = dir.path().join("by-name");
    fs::create_dir_all(&parent).unwrap();

    // Written as the plain name, which is what a person says.
    common::run_backup(&source, &parent.join("photos")).await;

    // And read back as the name a directory listing shows, which is the other
    // thing a person says and the one a copy out of the directory brings with it.
    let by_name = parent.join("photos");
    let by_segment = parent.join("photos.0000.bin");
    let named = IndexSet::new(&by_name);
    let segment = IndexSet::new(&by_segment);

    assert_eq!(
        segment.base(),
        named.base(),
        "the two spellings should be one set"
    );
    assert_eq!(
        segment.segments().unwrap(),
        named.segments().unwrap(),
        "and hold the same segments"
    );
    assert_eq!(
        segment.indexed_versions().unwrap(),
        vec![1],
        "and the version the backup published, found through the other spelling"
    );

    // A restore naming the segment reads that version out of it, which is the
    // whole claim: the bytes land where the version 1 the backup committed is.
    let restored = dir.path().join("restored");
    common::run_restore_version(&by_segment, &restored, 1).await;
    common::assert_same_tree(&source, &restored);

    let nested = parent.join("photos.0000.bin.0000.bin");
    assert!(
        !nested.exists(),
        "the set should be named photos and hold a segment, not be a set named \
         after its own segment: {}",
        nested.display()
    );
}
