//! The keystone: a [`VirtualPack`]'s bytes are the file [`Pack::write`] writes.
//!
//! `lepatch serve` answers a range request for a pack it has never written, by
//! computing the bytes out of the set's own segments. That is only safe if the
//! computed bytes and the written bytes are the same bytes. Not equivalent —
//! the same. A client may fetch some ranges from one and the rest from the other
//! and still hold one file.
//!
//! So this test compares the two directly, over real stores rather than over a
//! fixture chosen to be easy:
//!
//! * one version, the ordinary case;
//! * a delta, where the plan is [`Plan::since`] rather than the whole snapshot;
//! * a compressed set, so every record has a frame in it and the `raw_len` and
//!   `stored_len` in a header are genuinely different numbers;
//! * a set that rolled over a segment, so the payload spans two files and
//!   [`Placement::segment`] is not always zero.
//!
//! It then reads the virtual pack back the way the server serves it — through
//! [`VirtualPack::source`], one range at a time, against the real segment files —
//! and assembles the file the same way a client would. That file must open.
//!
//! [`Placement::segment`]: lepatch::storage::Placement::segment

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use lepatch::{
    command::Options,
    metadata::Snapshot,
    pack::{Pack, Range, Source, VirtualPack},
    storage::{BlobStore, Compression, Plan, SegmentLimit, StorageGet},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Read a version's snapshot out of a set.
async fn snapshot_of(store: &BlobStore<lepatch::storage::Read, Compression>) -> Snapshot {
    let id = store.latest().unwrap().1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    Snapshot::try_from(buffer.as_slice()).unwrap()
}

/// The plan for a set's latest version, resolved against its own placements.
async fn plan_of(store: &BlobStore<lepatch::storage::Read, Compression>) -> Plan {
    let snapshot = snapshot_of(store).await;
    Plan::from_snapshot(&snapshot, |id| Ok(store.locate(id).unwrap())).unwrap()
}

/// A tree of `files` noisy files, and the set it was backed up into.
async fn built(dir: &Path, files: usize, size: usize) -> (PathBuf, PathBuf) {
    let source = common::source(dir);
    for i in 0..files {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(size + i * 1_000),
        )
        .unwrap();
    }

    let blob = dir.join("store");
    common::run_backup(&source, &blob).await;

    (source, blob)
}

/// The segments of a set, so a resolver can name them.
fn segments(base: &Path) -> Vec<PathBuf> {
    lepatch::layout::IndexSet::new(base).segments().unwrap()
}

/// Serve `virtual_pack`'s whole length the way the server does, and write it to
/// `out` — assembling a file exactly as a client would from a fetch plan.
async fn serve_whole(virtual_pack: &VirtualPack, base: &Path, out: &Path) -> Vec<u8> {
    let segments = segments(base);
    let resolve = |segment: u32| {
        segments
            .get(usize::try_from(segment).unwrap_or(usize::MAX))
            .cloned()
            .unwrap_or_else(|| PathBuf::from("no-such-segment"))
    };

    let _ = resolve;

    let sources = virtual_pack
        .source(Range {
            start: 0,
            end: virtual_pack.file_len(),
        })
        .unwrap();

    let mut file = tokio::fs::File::create(out).await.unwrap();
    let mut written = Vec::new();

    for source in sources {
        match source {
            Source::Bytes(bytes) => {
                tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
                    .await
                    .unwrap();
                written.extend_from_slice(&bytes);
            }
            Source::Segment { path, offset, len } => {
                let mut handle = tokio::fs::File::open(&path).await.unwrap();
                handle.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
                let mut buffer = vec![0u8; usize::try_from(len).unwrap_or(0)];
                tokio::io::AsyncReadExt::read_exact(&mut handle, &mut buffer)
                    .await
                    .unwrap();
                tokio::io::AsyncWriteExt::write_all(&mut file, &buffer)
                    .await
                    .unwrap();
                written.extend_from_slice(&buffer);
            }
        }
    }

    tokio::io::AsyncWriteExt::flush(&mut file).await.unwrap();
    written
}

#[tokio::test]
async fn a_full_virtual_pack_is_the_written_pack_byte_for_byte() {
    // The ordinary case, and the one the whole feature rests on.
    let dir = tempfile::tempdir().unwrap();
    let (_, blob) = built(dir.path(), 4, 20_000).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let written = dir.path().join("written.pack");
    Pack::write(&store, &plan, &snapshot, 1, &written)
        .await
        .unwrap();

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let served = serve_whole(&virtual_pack, &blob, &dir.path().join("served.pack")).await;

    let on_disk = fs::read(&written).unwrap();
    assert!(
        served == on_disk,
        "served {} bytes, file {} bytes; first difference at {}",
        served.len(),
        on_disk.len(),
        served
            .iter()
            .zip(on_disk.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(usize::MAX),
    );
}

#[tokio::test]
async fn a_served_pack_opens() {
    // The consequence that matters. A client fetches ranges, writes them at their
    // offsets and calls `Pack::open` on the file. If the served bytes were wrong
    // in any way, this is where it would show.
    let dir = tempfile::tempdir().unwrap();
    let (_, blob) = built(dir.path(), 4, 20_000).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let served_path = dir.path().join("served.pack");
    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();
    serve_whole(&virtual_pack, &blob, &served_path).await;

    let opened = Pack::open(&served_path).unwrap();
    assert_eq!(opened.len(), plan.len());
    assert_eq!(opened.version(), 1);
    assert_eq!(opened.payload_len(), virtual_pack.payload_len());
}

#[tokio::test]
async fn a_delta_virtual_pack_is_the_written_delta() {
    // A delta's plan is `Plan::since`, which keeps only the chunks the base does
    // not hold. Its index is therefore SHORTER, and the payload starts where the
    // base's did not — so this exercises a different header, a different index
    // length and a different `payload_len` than a full pack does.
    //
    // The fixture guard at the end matters as much as the comparison: a delta that
    // silently degenerated into a full pack would still be byte-identical to what
    // `Pack::write_delta` wrote, so this asserts it is actually smaller.
    let dir = tempfile::tempdir().unwrap();
    let (source, blob) = built(dir.path(), 4, 20_000).await;

    // The base is version 1, captured BEFORE the rewrite, so `since` has a real
    // overlap to remove.
    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let base_id = store.latest().unwrap().1;
    let base_snapshot = snapshot_of(&store).await;

    // Change one file, and back up again.
    fs::write(source.join("f0.bin"), common::noisy(45_000)).unwrap();
    let storage =
        BlobStore::open_for_writing(&blob, SegmentLimit::default(), Compression::default())
            .await
            .unwrap();
    lepatch::command::backup(&source, Options::default(), storage)
        .await
        .unwrap();

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let full = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;
    let delta_plan = full.since(&snapshot, &base_snapshot);

    // The fixture guard. Without this the test would pass even if `since`
    // returned everything, because `Pack::write_delta` and `VirtualPack` would
    // then both be writing a full pack and would still agree.
    assert!(
        delta_plan.len() < full.len(),
        "the fixture must produce a real delta: {} of {} chunks survived",
        delta_plan.len(),
        full.len()
    );

    let written = dir.path().join("written.pack");
    let packed = Pack::write_delta(&store, &delta_plan, &snapshot, base_id, 2, &written)
        .await
        .unwrap();
    assert_eq!(
        packed.base(),
        Some(base_id),
        "the header must record the base"
    );

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&delta_plan, &snapshot, 2, Some(base_id), |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let served = serve_whole(&virtual_pack, &blob, &dir.path().join("served.pack")).await;
    assert_eq!(served, fs::read(&written).unwrap());

    // And the header carries the base flag, which is what a reader checks to
    // decide whether it can account for the pack at all.
    let opened = Pack::open(&dir.path().join("served.pack")).unwrap();
    assert_eq!(opened.base(), Some(base_id));
}

#[tokio::test]
async fn a_compressed_set_produces_the_same_bytes() {
    // A compressed set is where `stored_len` and `raw_len` are genuinely
    // different numbers. A record header carries both, and an index entry carries
    // the second. Getting them transposed would produce a file that opens and
    // then hashes wrong on every chunk.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..4 {
        fs::write(source.join(format!("f{i}.bin")), common::noisy(30_000)).unwrap();
    }

    let blob = dir.path().join("store");
    common::run_backup_compressed(&source, &blob, Compression::Zstd { level: 9 }).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let written = dir.path().join("written.pack");
    Pack::write(&store, &plan, &snapshot, 1, &written)
        .await
        .unwrap();

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let served = serve_whole(&virtual_pack, &blob, &dir.path().join("served.pack")).await;
    assert_eq!(served, fs::read(&written).unwrap());
}

#[tokio::test]
async fn a_set_that_rolled_over_a_segment_produces_the_same_bytes() {
    // The payload now spans several files, so `Placement::segment` is not always
    // zero and a resolver that ignored it would read the wrong one. The cap is
    // small enough that this tree cannot fit in one segment.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..6 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(40_000 + i * 1_000),
        )
        .unwrap();
    }

    let blob = dir.path().join("store");
    let storage =
        BlobStore::open_for_writing(&blob, SegmentLimit::new(50_000), Compression::default())
            .await
            .unwrap();
    lepatch::command::backup(&source, Options::default(), storage)
        .await
        .unwrap();

    assert!(
        segments(&blob).len() > 1,
        "this set must have rolled over for the test to mean anything"
    );

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let written = dir.path().join("written.pack");
    Pack::write(&store, &plan, &snapshot, 1, &written)
        .await
        .unwrap();

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let served = serve_whole(&virtual_pack, &blob, &dir.path().join("served.pack")).await;
    assert_eq!(served, fs::read(&written).unwrap());
}

#[tokio::test]
async fn needed_agrees_with_the_written_packs_needed() {
    // The negotiation answer, which is what a client fetches by. Two
    // implementations of the same walk would drift, and the drift would be a
    // client fetching bytes the pack does not have.
    let dir = tempfile::tempdir().unwrap();
    let (_, blob) = built(dir.path(), 6, 20_000).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let written = dir.path().join("written.pack");
    let pack = Pack::write(&store, &plan, &snapshot, 1, &written)
        .await
        .unwrap();

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    // Every subset worth testing: nothing held, everything held, and the two
    // halves.
    let every: HashSet<_> = plan.iter().map(|c| c.id).collect();
    let half: HashSet<_> = plan.iter().step_by(2).map(|c| c.id).collect();
    let none = HashSet::new();

    for have in [&none, &half, &every] {
        assert_eq!(
            virtual_pack.needed(have),
            pack.needed(have),
            "needed disagreed for a client holding {} of {} chunks",
            have.len(),
            every.len()
        );
    }
}

#[tokio::test]
async fn a_fetch_plan_always_covers_what_opens_the_file() {
    // `needed` alone is a payload and no header, snapshot, index or trailer.
    // `Pack::open` reads the trailer first, so a client that fetched only
    // `needed` would be refused on a file it had paid to download. `fetch_plan`
    // is the union, and this is the property that makes it the right answer.
    let dir = tempfile::tempdir().unwrap();
    let (_, blob) = built(dir.path(), 4, 20_000).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let every: HashSet<_> = plan.iter().map(|c| c.id).collect();
    let plan_ranges = virtual_pack.fetch_plan(&every);

    for region in [
        virtual_pack.header_range(),
        virtual_pack.snapshot_range(),
        virtual_pack.index_range(),
        virtual_pack.trailer_range(),
    ] {
        assert!(
            plan_ranges
                .iter()
                .any(|r| r.start <= region.start && r.end >= region.end),
            "the fetch plan omits {:?}",
            (region.start, region.end)
        );
    }

    // And the ranges are sorted and disjoint, because a client issues one
    // request per range and an overlap is a byte fetched twice.
    for pair in plan_ranges.windows(2) {
        assert!(
            pair[0].end <= pair[1].start,
            "ranges {:?} and {:?} overlap",
            (pair[0].start, pair[0].end),
            (pair[1].start, pair[1].end)
        );
    }
}

#[tokio::test]
async fn a_client_can_fetch_only_the_chunks_it_is_missing() {
    // The property the whole pull protocol exists for. A client holding most of
    // the chunks fetches less than the whole pack, assembles a file from the
    // ranges, and `Pack::open` accepts it.
    let dir = tempfile::tempdir().unwrap();
    let (_, blob) = built(dir.path(), 8, 20_000).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let all: Vec<_> = plan.iter().map(|c| c.id).collect();
    let have: HashSet<_> = all.iter().step_by(2).copied().collect();

    let ranges = virtual_pack.fetch_plan(&have);
    assert!(
        virtual_pack
            .fetch_plan(&have)
            .iter()
            .fold(0u64, |total, range| total.saturating_add(range.len()))
            < virtual_pack.file_len(),
        "a client holding half the chunks must fetch less than the whole pack"
    );

    // Assemble, writing each range at its offset into a sparse file.
    let assembled = dir.path().join("partial.pack");
    let file = tokio::fs::File::create(&assembled).await.unwrap();
    file.set_len(virtual_pack.file_len()).await.unwrap();
    let mut writer = file;

    for range in ranges {
        let _ = &segments;
        let sources = virtual_pack.source(range).unwrap();

        let mut at = range.start;
        for source in sources {
            let bytes = match source {
                Source::Bytes(bytes) => bytes,
                Source::Segment { path, len, .. } => {
                    let mut handle = tokio::fs::File::open(&path).await.unwrap();
                    let mut buffer = vec![0u8; usize::try_from(len).unwrap_or(0)];
                    tokio::io::AsyncReadExt::read_exact(&mut handle, &mut buffer)
                        .await
                        .unwrap();
                    buffer
                }
            };

            tokio::io::AsyncSeekExt::seek(&mut writer, std::io::SeekFrom::Start(at))
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut writer, &bytes)
                .await
                .unwrap();
            at += u64::try_from(bytes.len()).unwrap_or(0);
        }

        assert_eq!(at, range.end, "a range must yield exactly its own length");
    }

    tokio::io::AsyncWriteExt::flush(&mut writer).await.unwrap();
    drop(writer);

    // The file opens, and its index names every chunk including the ones this
    // client claimed to already hold — which it wrote as holes, so it must open
    // from the fixed regions alone.
    let opened = Pack::open(&assembled).unwrap();
    assert_eq!(opened.len(), all.len());
}

#[tokio::test]
async fn any_range_out_of_a_virtual_pack_reassembles_to_the_written_bytes() {
    // **The property, over ranges nobody thought to hand-pick.**
    //
    // `source()` is asked for arbitrary byte ranges: one starting inside a record
    // header, one ending inside one, one crossing the header/payload join, one
    // beginning in the payload and ending in the trailer. Those are the cases a
    // hand-written walk gets wrong, and a test that only fetches whole regions
    // never reaches them.
    //
    // So for each range: the parts must total exactly the range's length, and
    // laying them end to end must reproduce the written pack's bytes for that
    // span. The second half is the one that matters — a walk can return the right
    // *bytes* at the wrong *offsets*, and only reassembly catches it.
    let dir = tempfile::tempdir().unwrap();
    let (_, blob) = built(dir.path(), 5, 20_000).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&store).await;
    let snapshot = snapshot_of(&store).await;

    let written_at = dir.path().join("written.pack");
    Pack::write(&store, &plan, &snapshot, 1, &written_at)
        .await
        .unwrap();
    let written = fs::read(&written_at).unwrap();

    let segments = segments(&blob);
    let virtual_pack = VirtualPack::new(&plan, &snapshot, 1, None, |segment| {
        segments[segment as usize].clone()
    })
    .unwrap();

    let length = virtual_pack.file_len();
    assert_eq!(
        written.len() as u64,
        length,
        "the two must describe one file"
    );

    // Every boundary worth cutting at: each region edge, plus a sweep that lands
    // inside record headers and payloads, plus a few single bytes.
    let mut cuts: Vec<u64> = vec![
        0,
        length,
        virtual_pack.payload_start(),
        virtual_pack.payload_range().end,
        virtual_pack.snapshot_range().end,
        virtual_pack.index_range().end,
    ];
    // Record boundaries, read off the WRITTEN pack rather than recomputed. So the
    // cuts are the writer's own geometry, and a virtual pack whose offsets
    // disagreed with it would be cut in the wrong place and caught.
    let opened = Pack::open(&written_at).unwrap();
    let payload_start = opened.payload_start();
    for chunk in opened.chunks() {
        cuts.push(payload_start.saturating_add(chunk.offset));
        cuts.push(payload_start.saturating_add(chunk.offset).saturating_add(1));
        cuts.push(payload_start.saturating_add(chunk.offset).saturating_add(2));
    }
    let mut sweep = 0u64;
    while sweep < length {
        cuts.push(sweep);
        sweep = sweep.saturating_add(997);
    }

    let mut checked = 0usize;
    for start in &cuts {
        for end in &cuts {
            if start >= end || *end > length {
                continue;
            }

            let range = Range {
                start: *start,
                end: *end,
            };
            let sources = virtual_pack
                .source(range)
                .unwrap_or_else(|e| panic!("{range:?} should be inside the pack: {e}"));

            let total: u64 = sources
                .iter()
                .map(|source| match source {
                    Source::Bytes(bytes) => bytes.len() as u64,
                    Source::Segment { len, .. } => *len,
                })
                .sum();
            assert_eq!(
                total,
                range.len(),
                "{range:?}: source() must yield exactly the range's length"
            );

            let mut assembled = Vec::with_capacity(usize::try_from(range.len()).unwrap_or(0));
            for source in sources {
                match source {
                    Source::Bytes(bytes) => assembled.extend_from_slice(&bytes),
                    Source::Segment { path, offset, len } => {
                        let mut handle = tokio::fs::File::open(&path).await.unwrap();
                        handle.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
                        let mut buffer = vec![0u8; usize::try_from(len).unwrap_or(0)];
                        tokio::io::AsyncReadExt::read_exact(&mut handle, &mut buffer)
                            .await
                            .unwrap();
                        assembled.extend_from_slice(&buffer);
                    }
                }
            }

            let from = usize::try_from(range.start).unwrap_or(0);
            let to = usize::try_from(range.end).unwrap_or(0);
            assert_eq!(
                assembled,
                &written[from..to],
                "{range:?}: the bytes served do not match the written pack's"
            );

            checked += 1;
        }
    }

    // A meaningful number of cases, so the sweep cannot silently degenerate into
    // a handful — the failure mode of a property test that passes because it
    // tested nothing.
    assert!(
        checked > 500,
        "only {checked} ranges were checked, which is too few to mean anything"
    );
}
