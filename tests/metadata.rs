//! Tests for the snapshot encoding: framing, versioning and error reporting.

use lepatch::metadata::{self, Error, Snapshot};

fn sample() -> Snapshot {
    let mut snapshot = Snapshot::new();
    snapshot.files.push(metadata::File {
        path: "a/b.bin".into(),
        len: 10,
        mode: 0o644,
        mtime: None,
    });
    snapshot.chunks.push(metadata::Chunk {
        hash: [7u8; 32],
        location: r#"{"offset":0,"length":10}"#.into(),
    });
    snapshot.file_chunks.push(metadata::FileChunk {
        chunk_index: 0,
        file_index: 0,
        chunk_offset: 0,
        file_offset: 0,
        length: 10,
    });
    snapshot
}

#[test]
fn snapshot_round_trips_through_the_framed_encoding() {
    let encoded = sample().to_bytes().unwrap();

    assert_eq!(&encoded[..metadata::MAGIC.len()], metadata::MAGIC);

    let decoded = Snapshot::from_bytes(&encoded).unwrap();
    assert_eq!(decoded.version, metadata::FORMAT_VERSION);
    assert_eq!(decoded.files, sample().files);
    assert_eq!(decoded.chunks, sample().chunks);
    assert_eq!(decoded.file_chunks, sample().file_chunks);
}

#[test]
fn a_blob_without_the_magic_prefix_is_rejected() {
    let mut encoded = sample().to_bytes().unwrap();
    encoded[0] = b'X';

    match Snapshot::from_bytes(&encoded) {
        Err(Error::BadMagic(_)) => {}
        other => panic!("expected BadMagic, got {other:?}"),
    }
}

#[test]
fn an_unknown_format_version_is_reported_as_such() {
    let mut encoded = sample().to_bytes().unwrap();
    let version_at = metadata::MAGIC.len();
    encoded[version_at..version_at + 4].copy_from_slice(&999u32.to_le_bytes());

    match Snapshot::from_bytes(&encoded) {
        Err(Error::UnsupportedVersion { found, supported }) => {
            assert_eq!(found, 999);
            assert_eq!(supported, metadata::FORMAT_VERSION);
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn a_truncated_blob_is_rejected_rather_than_panicking() {
    let encoded = sample().to_bytes().unwrap();

    for len in 0..metadata::MAGIC.len() + 4 {
        match Snapshot::from_bytes(&encoded[..len]) {
            Err(Error::Truncated) => {}
            other => panic!("expected Truncated at len {len}, got {other:?}"),
        }
    }
}

#[test]
fn a_corrupt_body_is_reported_as_a_decode_error() {
    let mut encoded = sample().to_bytes().unwrap();
    let last = encoded.len() - 1;
    encoded[last] ^= 0xff;

    assert!(matches!(
        Snapshot::from_bytes(&encoded),
        Err(Error::Decode(_))
    ));
}
