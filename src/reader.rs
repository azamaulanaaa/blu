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

pub trait AsyncReadSeek: AsyncRead + AsyncSeek {}

impl<T: AsyncRead + AsyncSeek + ?Sized> AsyncReadSeek for T {}

pub type StreamReadSeeker = Box<dyn AsyncReadSeek + Unpin + Send>;

#[derive(Debug, Clone)]
pub struct ChunkSource {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u32,
}

pub struct Chunk {
    pub sources: Vec<ChunkSource>,
    pub reader: StreamReadSeeker,
}

struct EntryFileRegistry {
    path: PathBuf,
    length: u64,
    global_offset: u64,
}

pub struct FileRegistry {
    entries: Vec<EntryFileRegistry>,
}

impl FileRegistry {
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

    #[instrument(level = "trace", skip(self))]
    pub fn resolve_chunk(&self, global_start: u64, length: u32) -> Vec<ChunkSource> {
        let global_end = global_start + length as u64;
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

            let overlap_len = overlap_end_global.saturating_sub(overlap_start_global) as u32;

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

pub struct GlobalStream {
    paths: VecDeque<PathBuf>,
    current_file: Option<File>,
}

impl GlobalStream {
    #[instrument(level = "trace", skip(paths))]
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> Self {
        let paths = paths.map(|v| v.into()).collect();

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

#[derive(Debug, Clone)]
pub struct ChunkerConfig {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

pub struct Chunker {
    registry: Arc<FileRegistry>,
    cdc_iter: StreamCDC<GlobalStream>,
}

impl Chunker {
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

        let sources = self
            .registry
            .resolve_chunk(cdc_chunk.offset as u64, cdc_chunk.length as u32);

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

        let wanted = std::cmp::min(self.remaining(), buf.remaining() as u64) as usize;

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

        let delta = new_position as i128 - self.position as i128;
        self.position = new_position;

        Pin::new(&mut self.inner).start_seek(io::SeekFrom::Current(delta as i64))
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
