mod common;

use std::fs;

use common::assert_round_trip;

/// Build a tree that exercises the shapes a backup tool has to survive:
/// nested directories, and files both smaller and much larger than the
/// chunker configuration.
///
/// Empty files, empty directories and links are covered separately in
/// `tests/fidelity.rs`.
fn build_tree(root: &std::path::Path) {
    fs::create_dir_all(root.join("nested/deep")).unwrap();
    fs::write(root.join("large.bin"), vec![0xabu8; 300_000]).unwrap();
    fs::write(root.join("nested/small.txt"), b"hello world\n").unwrap();
    fs::write(root.join("nested/deep/tiny"), b"x").unwrap();
}

#[tokio::test]
async fn round_trip_reproduces_nested_tree() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    build_tree(&source);

    assert_round_trip(&source).await;
}

#[tokio::test]
async fn round_trip_preserves_exact_bytes_across_chunk_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // Sizes straddling the 8 KiB minimum and 16 KiB average so that chunks
    // both fall inside a single file and span a file boundary.
    for size in [1usize, 8_191, 8_192, 16_384, 65_536, 200_000] {
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        fs::write(source.join(format!("f{size}.bin")), &bytes).unwrap();
    }

    assert_round_trip(&source).await;
}

#[tokio::test]
async fn round_trip_handles_files_larger_than_the_chunk_maximum() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    // 1 MiB of non-repeating data forces many chunks and exercises the
    // multi-chunk write path within a single file.
    let bytes: Vec<u8> = (0..1_048_576u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
    fs::write(source.join("big.bin"), &bytes).unwrap();

    assert_round_trip(&source).await;
}
