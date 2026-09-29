//! Splitting a set of files into content-defined chunks.
//!
//! Files are read as if they had been concatenated into one stream, so a chunk
//! may span a file boundary. A [`FileRegistry`] records where each file sits in
//! that stream, which lets a chunk's global range be mapped back onto the one or
//! more file slices it covers.

use std::{
    collections::VecDeque,
    fmt::Debug,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use fastcdc::{ronomon, v2020::StreamCDC};
use tracing::instrument;

/// The part of one file that a chunk covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSource {
    /// Path of the file, as it was passed to [`FileRegistry::new`].
    ///
    /// Shared, not owned. There is one of these per *file slice a chunk
    /// covers*, and a chunk spanning a file boundary has more than one, so a
    /// backup built a `PathBuf` per chunk - a fresh allocation of the whole
    /// path, bytes and all, to name a file the registry had already recorded
    /// once. An [`Arc`] makes that a refcount bump. The allocation is not the
    /// point; the copy of the path is, and it happened once per chunk rather
    /// than once per file.
    pub path: Arc<Path>,
    /// Byte offset of this slice within the file.
    pub offset: u64,
    /// Length of this slice in bytes.
    pub length: u32,
}

/// One chunk of the concatenated stream, and where it came from.
///
/// `Debug` is hand-written and omits `data`: a debug log of a backup that
/// printed every chunk would print the user's files. `PartialEq` is derived
/// rather than hand-written because comparing the contents is exactly what a
/// test of the chunker wants to do, and nothing in a log has to notice.
#[derive(Clone, PartialEq, Eq)]
pub struct Chunk {
    /// The file slices this chunk covers, in stream order. More than one entry
    /// means the chunk spans a file boundary.
    pub sources: Vec<ChunkSource>,
    /// The chunk's contents, owned rather than behind a reader: the chunker has
    /// already materialised them to find the boundary.
    pub data: Vec<u8>,
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Contents omitted: a `Debug` that dumped a backup would be a way to
        // print the user's data, and the length says everything useful anyway.
        f.debug_struct("Chunk")
            .field("sources", &self.sources)
            .field("len", &self.data.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct EntryFileRegistry {
    path: Arc<Path>,
    length: u64,
    global_offset: u64,
}

/// Where each file sits within the concatenated stream.
#[derive(Debug)]
pub struct FileRegistry {
    entries: Vec<EntryFileRegistry>,
}

impl FileRegistry {
    /// Record the length of each file, in the order given.
    ///
    /// # Errors
    ///
    /// Returns an error if any file cannot be stat'ed.
    #[instrument(level = "trace", skip(paths), err)]
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> io::Result<Self> {
        let mut global_offset = 0;

        let entries = paths
            .map(|path| {
                let path_buf = path.into();
                let length = std::fs::metadata(&path_buf)?.len();

                let entry = EntryFileRegistry {
                    // `into_boxed_path` then `Arc::from`, so the path bytes are moved
                    // into the `Arc` rather than copied into it.
                    path: Arc::from(path_buf.into_boxed_path()),
                    length,
                    global_offset,
                };

                global_offset += length;

                Ok(entry)
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self { entries })
    }

    /// Map a global range of the concatenated stream onto the file slices it
    /// covers.
    ///
    /// An empty result means the range lies in a file that was not registered,
    /// which for a well-formed chunker means a file changed size between being
    /// registered and being read.
    #[instrument(level = "trace", skip(self))]
    pub fn resolve_chunk(&self, global_start: u64, length: u32) -> Vec<ChunkSource> {
        let global_end = global_start + u64::from(length);
        let mut mappings = Vec::new();

        let start_index = self
            .entries
            .partition_point(|entry| (entry.global_offset + entry.length) <= global_start);

        for entry in self.entries.iter().skip(start_index) {
            if entry.global_offset >= global_end {
                break;
            }

            let file_end_global = entry.global_offset + entry.length;

            let overlap_start_global = std::cmp::max(global_start, entry.global_offset);
            let overlap_end_global = std::cmp::min(global_end, file_end_global);

            // Both endpoints lie in `[global_start, global_end]`, so the
            // difference cannot exceed `length`; clamping keeps that true rather
            // than wrapping to a plausible-looking wrong length.
            let overlap = overlap_end_global.saturating_sub(overlap_start_global);

            let overlap_len = u32::try_from(overlap).unwrap_or(length);

            if overlap_len > 0 {
                let file_local_offset = overlap_start_global - entry.global_offset;

                mappings.push(ChunkSource {
                    path: Arc::clone(&entry.path),
                    offset: file_local_offset,
                    length: overlap_len,
                });
            }
        }

        mappings
    }
}

/// The files in `paths` presented as one continuous byte stream, as if
/// concatenated in order.
#[derive(Debug)]
pub struct GlobalStream {
    paths: VecDeque<PathBuf>,
    current_file: Option<File>,
}

impl GlobalStream {
    /// Build a stream over `paths`, opening each one as it is reached.
    #[instrument(level = "trace", skip(paths))]
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> Self {
        let paths = paths.map(Into::into).collect();

        Self {
            paths,
            current_file: None,
        }
    }
}

impl Read for GlobalStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(ref mut file) = self.current_file {
                match file.read(buf) {
                    Ok(0) => {
                        self.current_file = None;
                        continue;
                    }
                    Ok(n) => return Ok(n),
                    Err(e) => return Err(e),
                }
            }

            match self.paths.pop_front() {
                Some(path) => {
                    let file = File::open(path)?;
                    self.current_file = Some(file);
                }
                None => return Ok(0),
            }
        }
    }
}

/// Content-defined chunking parameters.
///
/// Boundaries come from the content, so inserting or removing bytes shifts only
/// the chunks around the change rather than every chunk after it.
///
/// [`Default`] is what the command line uses. The three sizes are the whole
/// public surface of the chunker's policy, and the binary exposes no flags for
/// them, so the values in [`Default`] are the only ones any real backup has ever
/// been written with. Keeping them here rather than at the call site means a
/// second configuration cannot be a slightly different one by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkerConfig {
    /// No boundary is taken before this many bytes.
    pub min_size: u32,
    /// The size the boundary probability curve is centred on.
    pub avg_size: u32,
    /// A boundary is forced once this many bytes have accumulated.
    pub max_size: u32,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            min_size: 8 * 1024,
            avg_size: 16 * 1024,
            max_size: 64 * 1024,
        }
    }
}

impl ChunkerConfig {
    /// Check these parameters are ones fastcdc will actually chunk with.
    ///
    /// fastcdc asserts its own bounds with `debug_assert!`, so they are **gone
    /// from a release build**: a value it would refuse reaches the chunker and
    /// produces chunks of a nonsense shape rather than an error. Validating here
    /// is what makes a bad configuration a message instead of a corrupt backup.
    ///
    /// # Errors
    ///
    /// Returns an error naming the bound that was not met, whether it is one of
    /// fastcdc's own limits or the ordering the three sizes must satisfy.
    pub fn validate(&self) -> io::Result<()> {
        let (min, avg, max) = (
            usize::try_from(self.min_size).unwrap_or(usize::MAX),
            usize::try_from(self.avg_size).unwrap_or(usize::MAX),
            usize::try_from(self.max_size).unwrap_or(usize::MAX),
        );

        let bounds = [
            ("min_size", min, ronomon::MINIMUM_MIN, ronomon::MINIMUM_MAX),
            ("avg_size", avg, ronomon::AVERAGE_MIN, ronomon::AVERAGE_MAX),
            ("max_size", max, ronomon::MAXIMUM_MIN, ronomon::MAXIMUM_MAX),
        ];

        for (name, value, lowest, highest) in bounds {
            if value < lowest || value > highest {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{name} is {value} bytes, outside the {lowest}..={highest} that the \
                         chunker accepts"
                    ),
                ));
            }
        }

        // The ordering is not fastcdc's assertion but its algorithm's: a boundary
        // is only looked for above `min`, and `avg` sets the masks the gear
        // table is indexed with, so `avg <= min` chunks at `min` every time and
        // `max < avg` can never reach the forced boundary. Both are silent, so
        // both are refused.
        if self.avg_size <= self.min_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "avg_size ({}) must be greater than min_size ({}), or every chunk comes out \
                     the same length",
                    self.avg_size, self.min_size
                ),
            ));
        }

        if self.max_size <= self.avg_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "max_size ({}) must be greater than avg_size ({}), or the forced boundary is \
                     never reached",
                    self.max_size, self.avg_size
                ),
            ));
        }

        Ok(())
    }
}

/// Splits a set of files into chunks, yielding each chunk's contents along with
/// the file slices it covers.
pub struct Chunker {
    registry: Arc<FileRegistry>,
    cdc_iter: StreamCDC<GlobalStream>,
}

impl Debug for Chunker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `StreamCDC` is not `Debug`.
        f.debug_struct("Chunker")
            .field("files", &self.registry.entries.len())
            .finish_non_exhaustive()
    }
}

impl Chunker {
    /// Chunk `paths` according to `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if any file cannot be stat'ed while being registered, or
    /// if `config` is not one the chunker can chunk with. The second check is
    /// here rather than at the call site because it is the last point before the
    /// parameters stop being checked at all: `fastcdc` asserts its bounds with
    /// `debug_assert!`, which a release build does not contain.
    #[instrument(level = "trace", skip(paths, config), err)]
    pub fn new(paths: Vec<PathBuf>, config: ChunkerConfig) -> io::Result<Self> {
        config.validate()?;

        let registry = Arc::new(FileRegistry::new(paths.iter())?);

        // Bounds arrive as `u32` and are widened; a chunk longer than `u32::MAX` is
        // caught in `next`, where it can be reported against the chunk that
        // caused it.
        let widen = |value: u32| usize::try_from(value).unwrap_or(usize::MAX);

        let stream = GlobalStream::new(paths.iter());
        let cdc = StreamCDC::new(
            stream,
            widen(config.min_size),
            widen(config.avg_size),
            widen(config.max_size),
        );

        Ok(Self {
            registry,
            cdc_iter: cdc,
        })
    }
}

impl Iterator for Chunker {
    type Item = io::Result<Chunk>;

    #[instrument(level = "debug", skip_all)]
    fn next(&mut self) -> Option<Self::Item> {
        let cdc_chunk = self.cdc_iter.next()?;
        let cdc_chunk = match cdc_chunk {
            Ok(v) => v,
            Err(e) => return Some(Err(io::Error::other(e))),
        };

        // A configuration error, reported rather than narrowed: truncating would
        // map the chunk to the wrong files.
        let Ok(length) = u32::try_from(cdc_chunk.length) else {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunker produced a {} byte chunk, over the u32 limit",
                    cdc_chunk.length
                ),
            )));
        };

        let sources = self.registry.resolve_chunk(cdc_chunk.offset, length);

        Some(Ok(Chunk {
            sources,
            data: cdc_chunk.data,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkerConfig;

    /// A configuration the chunker accepts, for the cases that mutate one field.
    fn good() -> ChunkerConfig {
        ChunkerConfig {
            min_size: 8 * 1024,
            avg_size: 16 * 1024,
            max_size: 64 * 1024,
        }
    }

    /// A good configuration is accepted, so the cases below are testing the
    /// checks rather than a configuration that was never valid.
    #[test]
    fn an_ordinary_configuration_validates() {
        assert!(good().validate().is_ok());
    }

    /// Sizes below fastcdc's own floor are refused.
    #[test]
    fn a_min_size_below_the_floor_is_refused() {
        let config = ChunkerConfig {
            min_size: 1,
            ..good()
        };
        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("min_size is 1 bytes"),
            "the error should name the field and its value, got: {error}"
        );
    }

    /// So are sizes above its ceiling.
    #[test]
    fn an_avg_size_above_the_ceiling_is_refused() {
        let config = ChunkerConfig {
            avg_size: 512 * 1024 * 1024,
            max_size: 1024 * 1024 * 1024,
            ..good()
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    /// `avg <= min` is silent in fastcdc — every chunk comes out at `min` — so
    /// it is refused rather than left to produce a uniform-chunked backup.
    #[test]
    fn an_avg_at_or_below_min_is_refused() {
        let config = ChunkerConfig {
            min_size: 32 * 1024,
            avg_size: 32 * 1024,
            ..good()
        };
        let error = config.validate().unwrap_err();
        assert!(
            error.to_string().contains("must be greater than min_size"),
            "the error should say which way round they are, got: {error}"
        );
    }

    /// Likewise `max <= avg`: the forced boundary is never reached, so `max`
    /// does nothing and only the average is in play.
    #[test]
    fn a_max_at_or_below_avg_is_refused() {
        let config = ChunkerConfig {
            avg_size: 32 * 1024,
            max_size: 32 * 1024,
            ..good()
        };
        let error = config.validate().unwrap_err();
        assert!(
            error.to_string().contains("must be greater than avg_size"),
            "the error should say which way round they are, got: {error}"
        );
    }
}

/// Measure `resolve_chunk`, whose per-chunk work is one path clone.
///
/// The path in a [`ChunkSource`] is an [`Arc`] rather than a `PathBuf`, so building
/// one is a refcount bump instead of an allocation plus a copy of the path. That
/// is the only thing `resolve_chunk` does that touches memory beyond its output
/// vector, which makes it measurable in a way an end-to-end backup is not: a
/// backup spends its time on per-record filesystem calls, and a saved allocation
/// per chunk does not show up there at all.
///
/// So this drives `resolve_chunk` directly, over a registry of enough files that a
/// chunk lands inside one of them.
///
/// **It asserts that every call resolved something, and that assertion is the
/// point.** The first version of this benchmark built a registry of *empty* files
/// and reported 10ns per chunk for two milliseconds — the whole measurement was
/// two hundred real resolutions and a hundred and ninety-nine thousand iterations
/// that returned an empty vector because the global offset was past the end of the
/// set. It looked like a working benchmark. Counting the sources is what makes the
/// difference visible, so the count is checked on every run.
///
/// Ignored by default. Run it with:
///
/// ```text
/// cargo test --release -- --ignored --nocapture resolve_chunk
/// ```
#[test]
#[ignore = "builds a registry and takes a second; run it by hand"]
fn resolve_chunk_cost() {
    /// Files in the registry. Enough that a chunk lands in one of them, so the
    /// clone is the per-chunk work rather than a special case.
    const FILES: usize = 200;
    /// Bytes per file, and per chunk, so a chunk lands inside one file rather than
    /// spanning several. A registry of empty files resolves to nothing at all.
    const FILE_BYTES: usize = 64 * 1024;
    /// Resolutions per round, wrapping over the registry so every one of them is
    /// a chunk that exists.
    const CHUNKS: u32 = 200_000;

    let dir = tempfile::tempdir().expect("a temp dir for the registry");
    let paths: Vec<PathBuf> = (0..FILES)
        .map(|i| {
            let path = dir.path().join(format!("f{i}.bin"));
            std::fs::write(&path, vec![0u8; FILE_BYTES]).expect("a file");
            path
        })
        .collect();

    let registry = FileRegistry::new(paths.into_iter()).expect("a registry");

    let mut best = f64::MAX;
    let mut sources = 0usize;
    for _ in 0..5 {
        let started = std::time::Instant::now();
        sources = 0;
        for n in 0..CHUNKS {
            let file = u64::from(n % u32::try_from(FILES).expect("fits in u32"));
            let at = file * u64::try_from(FILE_BYTES).expect("fits in u64");
            sources += registry
                .resolve_chunk(at, u32::try_from(FILE_BYTES).expect("fits in u32"))
                .len();
        }
        best = best.min(started.elapsed().as_secs_f64());
    }

    assert_eq!(
        sources,
        usize::try_from(CHUNKS).expect("fits in usize"),
        "only {sources} of {CHUNKS} calls resolved a file, so the rest measured an \
         empty return — this benchmark is not measuring what it claims to"
    );

    println!(
        "\n{FILES} files, {CHUNKS} chunks, {sources} sources: {:.1}ms ({:.0} ns/chunk)",
        best * 1000.0,
        best * 1e9 / f64::from(CHUNKS)
    );
}
