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
    path::PathBuf,
    sync::Arc,
};

use fastcdc::v2020::StreamCDC;
use tracing::instrument;

/// The part of one file that a chunk covers.
#[derive(Debug, Clone)]
pub struct ChunkSource {
    /// Path of the file, as it was passed to [`FileRegistry::new`].
    pub path: PathBuf,
    /// Byte offset of this slice within the file.
    pub offset: u64,
    /// Length of this slice in bytes.
    pub length: u32,
}

/// One chunk of the concatenated stream, and where it came from.
pub struct Chunk {
    /// The file slices this chunk covers, in stream order. More than one entry
    /// means the chunk spans a file boundary.
    pub sources: Vec<ChunkSource>,
    /// The chunk's contents.
    ///
    /// Held directly rather than behind a reader: the chunker has already
    /// materialised these bytes to find the boundary, so wrapping them in a
    /// `Cursor` and reading them straight back out again was a copy of every
    /// byte of the tree for no benefit.
    pub data: Vec<u8>,
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The contents are left out on purpose: a `Debug` that dumped every
        // chunk of a backup would be a way to print the user's data, and its
        // length says everything useful anyway.
        f.debug_struct("Chunk")
            .field("sources", &self.sources)
            .field("len", &self.data.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct EntryFileRegistry {
    path: PathBuf,
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
                    path: path_buf,
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

            // Both endpoints lie within `[global_start, global_end]`, so the
            // difference cannot exceed `global_end - global_start`, which is
            // `length` and already a `u32`. Clamping to `length` keeps that true
            // even if a future change breaks the invariant, instead of wrapping
            // to a plausible-looking wrong length.
            let overlap = overlap_end_global.saturating_sub(overlap_start_global);

            let overlap_len = u32::try_from(overlap).unwrap_or(length);

            if overlap_len > 0 {
                let file_local_offset = overlap_start_global - entry.global_offset;

                mappings.push(ChunkSource {
                    path: entry.path.clone(),
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
/// Boundaries are chosen from the content itself rather than at fixed offsets,
/// so inserting or removing bytes shifts only the chunks around the change
/// instead of every chunk after it. That is what makes deduplication work
/// across revisions of a file.
#[derive(Debug, Clone)]
pub struct ChunkerConfig {
    /// No boundary is taken before this many bytes.
    pub min_size: u32,
    /// The size the boundary probability curve is centred on.
    pub avg_size: u32,
    /// A boundary is forced once this many bytes have accumulated.
    pub max_size: u32,
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
    /// Returns an error if any file cannot be stat'ed while being registered.
    #[instrument(level = "trace", skip(paths), err)]
    pub fn new(paths: Vec<PathBuf>, config: ChunkerConfig) -> io::Result<Self> {
        let registry = Arc::new(FileRegistry::new(paths.iter())?);

        // The chunker takes its bounds as `usize`, where the snapshot format
        // stores chunk lengths as `u32`. Widening is exact in this direction on
        // every platform that can hold a `u32`, and the reverse — a chunk longer
        // than `u32::MAX` — is caught in `next`, where it can still be reported
        // against the chunk that caused it.
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

    /// One chunk per call, so this is the natural place to trace how the
    /// stream was split up.
    #[instrument(level = "debug", skip_all)]
    fn next(&mut self) -> Option<Self::Item> {
        let cdc_chunk = self.cdc_iter.next()?;
        let cdc_chunk = match cdc_chunk {
            Ok(v) => v,
            Err(e) => return Some(Err(io::Error::other(e))),
        };

        // The chunker is configured with a `u32` `max_size`, so a chunk cannot
        // exceed `u32::MAX`. Narrowing it anyway is a configuration error worth
        // reporting rather than truncating into a length that maps the chunk to
        // the wrong files.
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
