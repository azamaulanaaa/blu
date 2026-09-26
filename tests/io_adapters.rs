//! The IO adapters: bounded reads and writes over an inner stream.

use std::io::{Cursor, SeekFrom};

use lepatch::{reader::SliceAsyncReader, writer::SliceAsyncWriter};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

fn bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

#[tokio::test]
async fn a_slice_reader_stops_at_its_limit() {
    let data = bytes(1000);
    let mut reader = SliceAsyncReader::new(Cursor::new(data.clone()), 100);

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, data[..100]);
    assert_eq!(reader.remaining(), 0);
}

#[tokio::test]
async fn a_limit_longer_than_the_data_yields_all_of_it() {
    let data = bytes(100);
    let mut reader = SliceAsyncReader::new(Cursor::new(data.clone()), 10_000);

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, data);
}

#[tokio::test]
async fn a_zero_limit_yields_nothing() {
    let mut reader = SliceAsyncReader::new(Cursor::new(bytes(100)), 0);

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert!(out.is_empty());
}

#[tokio::test]
async fn a_reader_offered_no_space_does_not_poison_the_inner_reader() {
    let data = bytes(100);

    // Polling with a buffer that has no room must consume nothing and must
    // leave the inner reader at the start.
    let mut reader = SliceAsyncReader::new(Cursor::new(data.clone()), 50);
    let mut empty: [u8; 0] = [];
    let filled = reader.read(&mut empty).await.unwrap();

    assert_eq!(filled, 0);

    // The inner reader must not have been told it was at EOF.
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, data[..50]);
}

#[tokio::test]
async fn seeking_repositions_within_the_limit() {
    let data = bytes(1000);
    let mut reader = SliceAsyncReader::new(Cursor::new(data.clone()), 500);

    reader.seek(SeekFrom::Start(100)).await.unwrap();

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, data[100..500]);
}

#[tokio::test]
async fn seeking_relative_and_from_the_end_works() {
    let data = bytes(1000);
    let mut reader = SliceAsyncReader::new(Cursor::new(data.clone()), 500);

    reader.seek(SeekFrom::Start(100)).await.unwrap();
    reader.seek(SeekFrom::Current(50)).await.unwrap();

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, data[150..500]);
}

#[tokio::test]
async fn seeking_past_the_limit_is_refused() {
    let mut reader = SliceAsyncReader::new(Cursor::new(bytes(1000)), 100);

    let error = reader.seek(SeekFrom::Start(101)).await.unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("outside limit"));
}

#[tokio::test]
async fn a_slice_writer_stops_at_its_limit() {
    let mut sink = Vec::new();
    {
        let mut writer = SliceAsyncWriter::new(&mut sink, 10);
        // `write_all` would fail with `WriteZero` once the limit is reached,
        // which is the documented reason this type is for `copy` rather than
        // for `write_all`. Drive it the way `copy` does instead.
        let written = writer.write(&bytes(100)).await.unwrap();
        assert_eq!(written, 10);
        assert_eq!(writer.remaining(), 0);
    }

    assert_eq!(sink, bytes(10));
}

#[tokio::test]
async fn a_zero_limit_writer_writes_nothing() {
    let mut sink = Vec::new();
    {
        let mut writer = SliceAsyncWriter::new(&mut sink, 0);
        // `write_all` refuses a zero-length write, which is why this type is
        // documented as being for `copy`.
        let written = writer.write(&bytes(100)).await.unwrap();
        assert_eq!(written, 0);
    }

    assert!(sink.is_empty());
}

#[tokio::test]
async fn copy_between_the_two_adapters_moves_exactly_the_limit() {
    let data = bytes(5000);
    let mut source = SliceAsyncReader::new(Cursor::new(data.clone()), 4321);
    let mut sink = Vec::new();
    {
        let mut limited = SliceAsyncWriter::new(&mut sink, 4321);
        tokio::io::copy(&mut source, &mut limited).await.unwrap();
    }

    assert_eq!(sink, data[..4321]);
}
