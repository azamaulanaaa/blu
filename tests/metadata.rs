//! Tests for the snapshot encoding: framing, versioning and error reporting.

use lepatch::{
    metadata::{self, Attributes, Error, Malformed, Snapshot, Timestamp},
    storage::BlobId,
};

fn sample() -> Snapshot {
    let mut snapshot = Snapshot::new(Timestamp { secs: 1, nanos: 0 });
    snapshot.files.push(metadata::File {
        path: "a/b.bin".into(),
        len: 10,
        attrs: Attributes {
            owner: None,
            mode: Some(0o644),
            mtime: None,
        },
    });
    // A chunk is the BLAKE3 of its contents, and that is also how the store
    // finds it.
    // So a snapshot records content identity and nothing about where the bytes
    // happen to sit in a file.
    snapshot.chunks.push(BlobId::from_bytes([7u8; 32]));
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
    let encoded = sample().to_bytes().expect("encodes");

    assert_eq!(&encoded[..metadata::MAGIC.len()], metadata::MAGIC);

    // One comparison, not five.
    // `Snapshot` could not be compared at all before.
    // So this test compared its fields one at a time.
    // That test would have silently stopped checking two of them the day a sixth
    // was added.
    let decoded = Snapshot::try_from(encoded.as_slice()).unwrap();
    assert_eq!(decoded, sample());
}

/// The version in the frame is this build's, whatever the snapshot contains.
///
/// There is no field to set any more: the version belongs to the encoder.
/// The frame still carries it, because that is the format.
/// `TryFrom` still checks it.
/// So a blob written by another build's layout is refused rather than misread.
#[test]
fn the_frame_carries_this_builds_version() {
    let encoded = sample().to_bytes().expect("encodes");
    let at = metadata::MAGIC.len();
    let version = u32::from_le_bytes(encoded[at..at + 4].try_into().unwrap());

    assert_eq!(
        version,
        metadata::FORMAT_VERSION,
        "the frame should name the version this build writes"
    );
    assert!(Snapshot::try_from(encoded.as_slice()).is_ok());
}

#[test]
fn a_blob_without_the_magic_prefix_is_rejected() {
    let mut encoded = sample().to_bytes().expect("encodes");
    encoded[0] = b'X';

    match Snapshot::try_from(encoded.as_slice()) {
        Err(Error::BadMagic(_)) => {}
        other => panic!("expected BadMagic, got {other:?}"),
    }
}

#[test]
fn an_unknown_format_version_is_reported_as_such() {
    let mut encoded = sample().to_bytes().expect("encodes");
    let version_at = metadata::MAGIC.len();
    encoded[version_at..version_at + 4].copy_from_slice(&999u32.to_le_bytes());

    match Snapshot::try_from(encoded.as_slice()) {
        Err(Error::UnsupportedVersion { found, supported }) => {
            assert_eq!(found, 999);
            assert_eq!(supported, metadata::FORMAT_VERSION);
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn a_truncated_blob_is_rejected_rather_than_panicking() {
    let encoded = sample().to_bytes().expect("encodes");

    for len in 0..metadata::MAGIC.len() + 4 {
        match Snapshot::try_from(&encoded[..len]) {
            Err(Error::Truncated) => {}
            other => panic!("expected Truncated at len {len}, got {other:?}"),
        }
    }
}

#[test]
fn a_corrupt_body_is_reported_as_a_decode_error() {
    let mut encoded = sample().to_bytes().expect("encodes");
    let last = encoded.len() - 1;
    encoded[last] ^= 0xff;

    // An equality rather than a `matches!`, now that the error is comparable.
    // It is worth knowing which error this produces.
    // Flipping the last byte of a snapshot makes the final value's length run
    // past the end.
    // So this is a truncation and not the trailing-bytes case a reader would
    // guess.
    // A `matches!(.., Err(Decode(_)))` could not tell those apart, and that is the
    // whole reason for the change.
    assert_eq!(
        Snapshot::try_from(encoded.as_slice()),
        Err(Error::Decode(Malformed(
            "the snapshot ends in the middle of a value"
        ))),
    );
}

#[test]
fn trailing_bytes_after_a_snapshot_are_refused_rather_than_ignored() {
    let mut encoded = sample().to_bytes().expect("encodes");
    encoded.push(0);

    // The other half of the case above, and the reason it needed naming.
    // These are two different malformations that a caller would otherwise have to
    // tell apart by reading the message.
    assert_eq!(
        Snapshot::try_from(encoded.as_slice()),
        Err(Error::Decode(Malformed("the snapshot has trailing bytes"))),
    );
}

#[test]
fn timestamps_round_trip_on_both_sides_of_the_epoch() {
    for original in [
        std::time::UNIX_EPOCH,
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000),
        // Before the epoch, where the sign has to survive the round trip.
        std::time::UNIX_EPOCH - std::time::Duration::from_secs(1),
        std::time::UNIX_EPOCH - std::time::Duration::from_secs(1_000_000),
    ] {
        let timestamp: metadata::Timestamp = original.into();
        let restored: std::time::SystemTime = timestamp.into();

        assert_eq!(
            restored, original,
            "{original:?} became {timestamp:?} and back to {restored:?}"
        );
    }
}

#[test]
fn an_extreme_timestamp_does_not_wrap_into_the_opposite_of_itself() {
    // A negative `secs` must stay negative on the way back.
    // Otherwise the pre-epoch branch subtracts a huge unsigned duration from the
    // epoch and lands billions of years in the future.
    let before_epoch = metadata::Timestamp {
        secs: -1_000_000_000,
        nanos: 500,
    };

    let restored: std::time::SystemTime = before_epoch.into();

    assert!(
        restored < std::time::UNIX_EPOCH,
        "a pre-epoch timestamp restored to {restored:?}"
    );
    assert_eq!(
        restored,
        std::time::UNIX_EPOCH - std::time::Duration::new(1_000_000_000, 500)
    );
}

/// A blob written by a pre-release build must be reported, not misread.
///
/// Version 1 was bincode, which is fixed-width little endian.
/// Version 2 introduced the hand-written encoding.
/// Version 3 changed what a chunk is named by.
/// They share a magic prefix and nothing else.
/// So a decoder that guessed rather than checked would turn an old backup set
/// into a plausible-looking and completely wrong one.
/// This is the test that says it does not.
#[test]
fn a_bincode_encoded_blob_is_reported_as_an_unsupported_version() {
    let mut encoded = sample().to_bytes().expect("encodes");
    let header = lepatch::metadata::MAGIC.len();

    // Claim to be version 1, with a body that is not a version 1 body.
    encoded[header..header + 4].copy_from_slice(&1u32.to_le_bytes());

    match lepatch::metadata::Snapshot::try_from(encoded.as_slice()) {
        Err(lepatch::metadata::Error::UnsupportedVersion { found, supported }) => {
            assert_eq!(found, 1);
            assert_eq!(supported, lepatch::metadata::FORMAT_VERSION);
        }
        other => panic!("expected an unsupported-version error, got {other:?}"),
    }
}

/// The encoded form should not have the same size as the old one by accident.
///
/// Not a strong property on its own.
/// It is a sanity check that the header really is being written and the body
/// really is variable width.
#[test]
fn the_encoding_is_compact_for_small_numbers() {
    let snapshot = sample();
    let encoded = snapshot.to_bytes().expect("encodes");

    // A path, a count and a handful of small indices should cost tens of bytes.
    // A fixed-width encoding of the same values would cost about 150 bytes.
    assert!(
        encoded.len() < 120,
        "expected a compact encoding, got {} bytes",
        encoded.len()
    );
}
