//! The decompressing reader that a compressed blob is handed out through.
//!
//! It exists because [`blulib::storage::StorageGet`] returns something seekable,
//! and a streaming zstd frame cannot seek backwards without an index.
//! Buffering the decompressed payload makes a seek a slice.
//! That is what callers expect, and a restore needs it when it assembles a file
//! from several chunks.
//!
//! So the tests are about the parts a plain reader would get for free.
//! The tests check that the bytes come back exactly, and that a seek lands where
//! it says.
//! They also check that a frame which does not decode to the length its record
//! claims is refused rather than handed on short.
//! A short read that looked like end-of-file would leave a restored file quietly
//! incomplete.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

use std::io::SeekFrom;

use blulib::slice::DecompressReader;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

/// Bytes that compress, so there is something to decompress.
fn payload(len: usize) -> Vec<u8> {
    b"a line of compressible text. ".repeat(len / 27 + 1)[..len].to_vec()
}

fn frame_of(bytes: &[u8]) -> Vec<u8> {
    zstd::bulk::compress(bytes, 1).unwrap()
}

/// The decompressed bytes come back, in order and complete.
#[tokio::test]
async fn a_decompress_reader_yields_the_decompressed_bytes() {
    let raw = payload(100_000);
    let frame = frame_of(&raw);

    let mut reader = DecompressReader::new(frame, 100_000).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, raw);
    assert_eq!(out.len(), 100_000);
}

/// A frame that decodes to a different length than its record claims is refused.
///
/// This is the check that makes a damaged store report itself.
/// Without it, a damaged store produces a file that is quietly short.
/// A reader that trusted the frame and ignored the header would hand back fewer
/// bytes than the snapshot asked for.
/// The restore would then succeed.
#[tokio::test]
async fn a_frame_that_decodes_to_the_wrong_length_is_refused() {
    let raw = payload(50_000);
    let frame = frame_of(&raw);

    // The record claims more than the frame holds.
    let error = DecompressReader::new(frame.clone(), 60_000)
        .await
        .expect_err("a short frame should be refused");
    assert!(
        error.to_string().contains("60000"),
        "the error should name the length the record claimed: {error}"
    );

    // And less.
    let error = DecompressReader::new(frame, 40_000)
        .await
        .expect_err("a long frame should be refused");
    assert!(error.to_string().contains("40000"), "{error}");
}

/// Bytes that are not a zstd frame are refused rather than passed through.
#[tokio::test]
async fn something_that_is_not_a_frame_is_refused() {
    let error = DecompressReader::new(b"this is not a frame".to_vec(), 18)
        .await
        .expect_err("a non-frame should be refused");
    assert!(
        !error.to_string().is_empty(),
        "the error should say something about what went wrong"
    );
}

/// A seek lands where it says, from the start, from the end and from here.
///
/// A restore assembles a file by seeking within each chunk.
/// So a reader whose seek reported a position it had not actually reached would
/// produce a file whose bytes came from the wrong offsets.
/// That is the kind of corruption that passes a length check.
/// Only comparing the whole tree catches it.
#[tokio::test]
async fn a_decompress_reader_seeks_where_it_says_from_every_origin() {
    let raw = payload(60_000);
    let mut reader = DecompressReader::new(frame_of(&raw), 60_000).await.unwrap();

    reader.seek(SeekFrom::Start(10_000)).await.unwrap();
    assert_eq!(reader.stream_position().await.unwrap(), 10_000);
    let mut window = [0u8; 16];
    reader.read_exact(&mut window).await.unwrap();
    assert_eq!(&window, &raw[10_000..10_016]);

    reader.seek(SeekFrom::Current(5)).await.unwrap();
    assert_eq!(reader.stream_position().await.unwrap(), 10_005 + 16);

    reader.seek(SeekFrom::End(-100)).await.unwrap();
    assert_eq!(reader.stream_position().await.unwrap(), 60_000 - 100);
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, &raw[59_900..]);

    // A rewind, which is what a second pass over the same blob does.
    reader.seek(SeekFrom::Start(0)).await.unwrap();
    let mut all = Vec::new();
    reader.read_to_end(&mut all).await.unwrap();
    assert_eq!(all, raw, "a rewind should replay the payload in full");
}

/// Seeking before the start is refused rather than wrapping or panicking.
#[tokio::test]
async fn seeking_before_the_start_is_refused() {
    let raw = payload(1_000);
    let mut reader = DecompressReader::new(frame_of(&raw), 1_000).await.unwrap();

    reader.seek(SeekFrom::Start(0)).await.unwrap();
    let error = reader
        .seek(SeekFrom::Current(-1))
        .await
        .expect_err("seeking before the start should be refused");
    assert!(
        error.to_string().contains("before the start"),
        "the error should explain why: {error}"
    );

    let error = reader
        .seek(SeekFrom::End(-2_000))
        .await
        .expect_err("seeking before the start should be refused");
    assert!(error.to_string().contains("before the start"), "{error}");
}

/// Seeking past the end is refused rather than underflowing or wrapping.
///
/// The sibling type in the same module bounds its seek by its limit, and this one
/// has to match.
/// Both are handed out by [`blulib::storage::StorageGet`] as the reader for a
/// blob.
/// `StorageGet::get` promises the stream is "bounded to the blob, so a blob cannot
/// be used to read past itself".
/// Only the sign of the target was checked.
/// So a snapshot naming a `chunk_offset` past the end of the chunk it addresses
/// made the subtraction below wrap.
///
/// A seek to a position beyond `usize::MAX` is the other half of the same gap.
/// It also fails differently.
/// `position` would not convert, and the fallback of 0 silently rewound to the
/// start of the payload and replayed it in full.
#[tokio::test]
async fn seeking_past_the_end_is_refused() {
    let raw = payload(1_000);
    let mut reader = DecompressReader::new(frame_of(&raw), 1_000).await.unwrap();

    // One past the end. The end itself is in bounds and has its own test.
    let error = reader
        .seek(SeekFrom::Start(1_001))
        .await
        .expect_err("a seek one past the end should be refused");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("past the end"),
        "the error should explain why: {error}"
    );

    let error = reader
        .seek(SeekFrom::Start(1_000_000))
        .await
        .expect_err("a seek well past the end should be refused");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

    // Relative seeks are bounded by the same rule, from either direction.
    let error = reader
        .seek(SeekFrom::End(1))
        .await
        .expect_err("a seek one past the end should be refused");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

    let error = reader
        .seek(SeekFrom::Start(u64::MAX))
        .await
        .expect_err("a seek past the end should be refused, not rewound to zero");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// A refused seek leaves the reader where it was, still serving the right bytes.
///
/// The position is only assigned once the target is known to be in bounds.
/// So a refusal cannot leave a reader mid-payload with a position that does not
/// describe it.
#[tokio::test]
async fn a_refused_seek_does_not_move_the_reader() {
    let raw = payload(1_000);
    let mut reader = DecompressReader::new(frame_of(&raw), 1_000).await.unwrap();

    reader.seek(SeekFrom::Start(400)).await.unwrap();
    let _refused = reader.seek(SeekFrom::Start(5_000)).await;

    assert_eq!(reader.stream_position().await.unwrap(), 400);
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).await.unwrap();
    assert_eq!(
        tail,
        &raw[400..],
        "a refused seek should not disturb the payload"
    );
}

/// Seeking to exactly the end is allowed, and reads as empty.
///
/// `len` is the first position with no byte behind it, so it is in bounds.
/// Only a position *past* it is not.
/// Confining the bound to `>` rather than `>=` is what makes `write_file` able to
/// seek to the end of a zero-length final slice.
#[tokio::test]
async fn seeking_to_the_end_is_allowed() {
    let raw = payload(1_000);
    let mut reader = DecompressReader::new(frame_of(&raw), 1_000).await.unwrap();

    reader.seek(SeekFrom::Start(1_000)).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    // `assert_eq!(out, [])` no longer type-checks: `serde_json` is a
    // dev-dependency, so it is linked into this binary, and its `Value` has a
    // `PartialEq<u8>` impl that makes the empty array's element type ambiguous.
    assert_eq!(out.len(), 0);
}

/// An empty payload is empty, not an error.
///
/// A zero-length chunk is a real thing a chunker can produce.
/// A reader that refused to open one would fail a restore over a file that
/// happened to chunk to nothing.
#[tokio::test]
async fn an_empty_payload_reads_as_empty() {
    let mut reader = DecompressReader::new(frame_of(&[]), 0).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out.len(), 0);
}

/// A record claiming an absurd length is refused, and does not allocate.
///
/// The length comes out of a record header, and a header is covered by a truncated
/// hash rather than a MAC.
/// So a file that has been tampered with could carry a valid check and a length of
/// `u64::MAX`.
/// The reader has to refuse that *before* sizing a buffer from it.
/// The alternative is a multi-gigabyte allocation.
/// The alternative is also a panic from inside the decompressor when the frame
/// turns out to be longer than the buffer it was handed.
#[tokio::test]
async fn a_record_claiming_an_absurd_length_is_refused() {
    let raw = payload(1_000);
    let error = DecompressReader::new(frame_of(&raw), u64::MAX)
        .await
        .expect_err("an absurd length should be refused");
    assert!(
        error.to_string().contains("18446744073709551615"),
        "the error should name the length the record claimed: {error}"
    );
}

/// Its debug form does not dump the payload.
///
/// A `{:?}` in a log line is a normal thing to have.
/// One that printed a whole decompressed chunk would put a hundred kilobytes of
/// text into somebody's terminal.
#[tokio::test]
async fn a_decompress_reader_debug_form_does_not_include_the_payload() {
    let raw = payload(2_000);
    let reader = DecompressReader::new(frame_of(&raw), 2_000).await.unwrap();

    let rendered = format!("{reader:?}");
    assert!(!rendered.contains("quick brown"), "{rendered}");
    assert!(rendered.contains("len"), "{rendered}");
}
