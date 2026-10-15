// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Isolate `BlobStore::put` from everything else.
//!
//! The end-to-end numbers cannot separate "the store is slow" from "the machine
//! is busy".
//! Both halves of the pipeline have been blamed incorrectly in this codebase's
//! history.
//! This example does the one thing that settles it.
//! It puts N blobs through the real store.
//! It compares that against the same bytes written by `std::fs` with a large
//! buffer, on the same machine, back to back.
//!
//! The gap between those two numbers is the part of the store that is ours.

#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::io::Cursor;
use std::time::Instant;

use blulib::{
    layout::IndexSet,
    storage::{BlobStore, StoragePut, Write},
};

const CHUNK: usize = 64 * 1024;
const CHUNKS: usize = 1500;
const CONCURRENCY: usize = 4;

fn payload(seed: u8) -> Vec<u8> {
    vec![seed; CHUNK]
}

async fn put_all(store: std::sync::Arc<BlobStore<Write>>) -> f64 {
    let start = Instant::now();

    let mut in_flight = Vec::new();
    for i in 0..CHUNKS {
        let seed = u8::try_from(i % 251).expect("in range") + 1;
        in_flight.push(tokio::spawn({
            let store = std::sync::Arc::clone(&store);
            async move {
                let data = payload(seed);
                // The store checks this itself, so the address is the payload's
                // own hash rather than anything the caller invented.
                let id = blake3::hash(&data);
                store
                    .put(
                        &id,
                        Box::new(Cursor::new(data)) as blulib::storage::StreamReadSeeker,
                        CHUNK as u64,
                    )
                    .await
            }
        }));

        if in_flight.len() >= CONCURRENCY {
            in_flight.remove(0).await.unwrap().unwrap();
        }
    }
    for task in in_flight {
        task.await.unwrap().unwrap();
    }

    start.elapsed().as_secs_f64()
}

/// The ceiling: the same bytes, the same place, one open and one big write.
fn write_all_blocking(path: &std::path::Path) -> f64 {
    let start = Instant::now();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("open");

    let mut buffer = vec![0u8; 1024 * 1024];
    for i in 0..CHUNKS {
        buffer[..CHUNK].fill(u8::try_from(i % 251).expect("in range") + 1);
        std::io::Write::write_all(&mut file, &buffer[..CHUNK]).expect("write");
    }
    file.sync_all().expect("sync");

    start.elapsed().as_secs_f64()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store.bin");
    let plain_path = dir.path().join("plain.bin");

    let total = f64::from(u32::try_from(CHUNK * CHUNKS).expect("fits in u32")) / (1024.0 * 1024.0);
    println!("{CHUNKS} blobs of {CHUNK} bytes = {total:.0} MiB, concurrency {CONCURRENCY}");
    println!(
        "  load: {}",
        std::fs::read_to_string("/proc/loadavg")
            .unwrap_or_default()
            .split(' ')
            .take(3)
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!();

    // Warm up, so the first run is not paying for page cache and allocator
    // growth that every later run gets for free.
    for _ in 0..2 {
        for segment in 0..64 {
            let _ = std::fs::remove_file(IndexSet::new(&store_path).segment_path(segment));
        }
        let store = std::sync::Arc::new(
            BlobStore::builder(&store_path)
                .open_writer()
                .await
                .expect("store"),
        );
        put_all(store.clone()).await;
    }
    let _ = write_all_blocking(&plain_path);

    let mut store_times = Vec::new();
    let mut plain_times = Vec::new();

    for _ in 0..5 {
        for segment in 0..64 {
            let _ = std::fs::remove_file(IndexSet::new(&store_path).segment_path(segment));
        }
        let store = std::sync::Arc::new(
            BlobStore::builder(&store_path)
                .open_writer()
                .await
                .expect("store"),
        );
        store_times.push(put_all(store.clone()).await);
        plain_times.push(write_all_blocking(&plain_path));
    }

    let report = |label: &str, times: &[f64]| {
        let mut sorted = times.to_vec();
        sorted.sort_by(f64::total_cmp);
        let min = sorted[0];
        let mib = total / min;
        println!(
            "  {label:<28} min {:.3}s   median {:.3}s   {mib:.0} MiB/s",
            min,
            sorted[sorted.len() / 2]
        );
        min
    };

    println!("same bytes, same filesystem, back to back:");
    let store_min = report("blu put (4 tasks)", &store_times);
    let plain_min = report("std::fs, 1 MiB writes", &plain_times);

    println!();
    println!(
        "  the store takes {:.1}x what one open and a big write takes",
        store_min / plain_min
    );
    if store_min > plain_min * 1.5 {
        let overhead = (store_min - plain_min) * 1000.0;
        let total = store_min * 1000.0;
        println!("  -> {overhead:.0} ms of the store's {total:.0} ms is not the filesystem");
    }
}
