// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The IO adapters: bounded reads and writes over an inner stream.

use std::io::{Cursor, SeekFrom};

use lepatch::slice::{SliceAsyncReader, SliceAsyncWriter};
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

    // `assert_eq!(out, [])` no longer type-checks: `serde_json` is a
    // dev-dependency, so it is linked into this binary, and its `Value` has a
    // `PartialEq<u8>` impl that makes the empty array's element type ambiguous.
    // The assertion is unchanged in what it says.
    assert_eq!(out.len(), 0);
}

#[tokio::test]
async fn a_reader_offered_no_space_does_not_poison_the_inner_reader() {
    let data = bytes(100);

    // Polling with a buffer that has no room must consume nothing.
    // It must also leave the inner reader at the start.
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

/// A relative seek is measured from where the reader is, and an end-relative
/// seek from the slice's limit.
///
/// The name says the property rather than "works": a reader that measured
/// `Current` from the start would return the right bytes for a first seek and
/// the wrong ones for a second, which is exactly what this asserts.
#[tokio::test]
async fn a_relative_seek_is_measured_from_the_current_position_and_an_end_seek_from_the_limit() {
    let data = bytes(1000);
    let mut reader = SliceAsyncReader::new(Cursor::new(data.clone()), 500);

    reader.seek(SeekFrom::Start(100)).await.unwrap();
    reader.seek(SeekFrom::Current(50)).await.unwrap();

    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, data[150..500]);

    // From the end now: -100 of a 500-byte limit is offset 400.
    reader.seek(SeekFrom::End(-100)).await.unwrap();

    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).await.unwrap();

    assert_eq!(tail, data[400..500]);
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
        // `write_all` would fail with `WriteZero` once the limit is reached.
        // That failure is the documented reason this type is for `copy` rather
        // than for `write_all`.
        // Drive it the way `copy` does instead.
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

    assert_eq!(sink.len(), 0);
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
