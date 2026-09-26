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
    io::{self, Cursor, Read},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use fastcdc::v2020::StreamCDC;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tracing::instrument;

/// A stream that is both [`AsyncRead`] and [`AsyncSeek`].
///
/// This exists because `trait_alias` is still unstable. Every type that is both
/// implements it automatically, so it is never implemented by hand; use
/// [`StreamReadSeeker`] rather than naming it.
pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

/// A boxed, seekable async stream, as handed out by a storage backend.
pub type StreamReadSeeker = Box<dyn AsyncReadSeek + Unpin + Send>;

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
    pub reader: StreamReadSeeker,
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `StreamReadSeeker` is a trait object, so only the shape is shown.
        f.debug_struct("Chunk")
            .field("sources", &self.sources)
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

        let stream = GlobalStream::new(paths.iter());
        let cdc = StreamCDC::new(stream, config.min_size, config.avg_size, config.max_size);

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

        let reader = Cursor::new(cdc_chunk.data);
        let reader = Box::new(reader);

        Some(Ok(Chunk { sources, reader }))
    }
}

/// An [`AsyncRead`] over at most the first `limit` bytes of `inner`.
///
/// Reads stop at the limit, which is reported as end of stream in the usual way
/// — a `Ready(Ok(()))` that fills nothing.
pub struct SliceAsyncReader<R> {
    inner: R,
    position: u64,
    limit: u64,
}

impl<R> Debug for SliceAsyncReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately not derived: `R` is an arbitrary stream type, and
        // requiring `R: Debug` here would be a needless bound.
        f.debug_struct("SliceAsyncReader")
            .field("position", &self.position)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// Size of the stack scratch buffer used to stage bytes out of the inner
/// reader.
///
/// The inner reader cannot be handed the caller's buffer directly, because only
/// this type knows how much of it is within the limit. Matches
/// `tokio::io::copy`'s own buffer size so a copy through this reader does not
/// split every read in two.
const SCRATCH: usize = 8 * 1024;

impl<R> SliceAsyncReader<R> {
    /// Read at most `limit` bytes from `reader`.
    #[instrument(level = "trace", skip(reader))]
    pub fn new(reader: R, limit: u64) -> Self {
        Self {
            inner: reader,
            position: 0,
            limit,
        }
    }

    /// How many of the permitted bytes are left.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.position)
    }
}

impl<R> AsyncRead for SliceAsyncReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // `wanted` is bounded by `self.remaining()`, a `u64`, so narrowing it to
        // `usize` is exact on every target that can hold a `u32` limit.
        let offered = u64::try_from(buf.remaining()).unwrap_or(u64::MAX);
        let wanted = usize::try_from(self.remaining().min(offered)).unwrap_or(0);

        // The caller has no room. Polling the inner reader with an empty buffer
        // would make it report end of stream, which it would then keep doing —
        // so the inner reader is left untouched and only the caller's
        // zero-length request is answered.
        if wanted == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut scratch = [0u8; SCRATCH];
        let limit = std::cmp::min(wanted, scratch.len());

        let mut temp = ReadBuf::new(&mut scratch[..limit]);

        match Pin::new(&mut self.inner).poll_read(cx, &mut temp) {
            Poll::Ready(Ok(())) => {
                let filled = temp.filled();
                let n = filled.len();

                if n > 0 {
                    buf.put_slice(filled);
                    self.position += n as u64;
                }

                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<R> AsyncSeek for SliceAsyncReader<R>
where
    R: AsyncSeek + Unpin,
{
    /// Seeks are per slice rather than per read, so tracing them is cheap.
    #[instrument(level = "trace", skip(self), err)]
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let new_position = match position {
            io::SeekFrom::Start(n) => n,
            io::SeekFrom::Current(n) => self
                .position
                .checked_add_signed(n)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before start"))?,
            io::SeekFrom::End(n) => self
                .limit
                .checked_add_signed(n)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before start"))?,
        };

        if new_position > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek outside limit",
            ));
        }

        // Move the inner reader by a relative delta rather than an absolute
        // position, because its own origin is wherever the caller left it.
        let delta = i128::from(new_position) - i128::from(self.position);
        self.position = new_position;

        let delta = i64::try_from(delta).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek delta is too large to express as a relative seek",
            )
        })?;

        Pin::new(&mut self.inner).start_seek(io::SeekFrom::Current(delta))
    }

    #[instrument(level = "trace", skip(self, cx), ret)]
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let result = Pin::new(&mut self.inner).poll_complete(cx);

        match result {
            Poll::Ready(Ok(_n)) => Poll::Ready(Ok(self.position)),
            other => other,
        }
    }
}
