//! Bounded async reads and writes.
//!
//! A blob store hands out a slice of a larger file, and a restore copies a slice
//! of a blob into a slice of a destination file. Both need a stream that stops
//! after exactly `limit` bytes rather than running to the end of what it wraps.
//!
//! [`SliceAsyncReader`] bounds the read side and [`SliceAsyncWriter`] the write
//! side. They are used together by [`tokio::io::copy`], which stops when either
//! side reports end of stream, so wrapping both ends of a copy pins it to one
//! blob's worth of one file's worth of bytes.
//!
//! Neither type is specific to chunking or to any storage backend; they are
//! general adapters.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::storage::{Decompressor, Zstd};

/// Size of the stack scratch buffer used to stage bytes out of the inner
/// reader.
///
/// The inner reader cannot be handed the caller's buffer, because only this type
/// knows how much of it is within the limit. Matches `tokio::io::copy`'s own
/// buffer so a copy through this reader does not split every read in two.
const SCRATCH: usize = 8 * 1024;

/// An [`AsyncRead`] over at most the first `limit` bytes of `inner`.
///
/// Reads stop at the limit, which is reported as end of stream in the usual way
/// — a `Ready(Ok(()))` that fills nothing.
pub struct SliceAsyncReader<R> {
    inner: R,
    position: u64,
    limit: u64,
}

impl<R> std::fmt::Debug for SliceAsyncReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-written rather than derived: `R` is an arbitrary stream type, and
        // a bound on it would be needless.
        f.debug_struct("SliceAsyncReader")
            .field("position", &self.position)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl<R> SliceAsyncReader<R> {
    /// Read at most `limit` bytes from `reader`.
    #[tracing::instrument(level = "trace", skip(reader))]
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
    ) -> Poll<std::io::Result<()>> {
        if self.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // Bounded by `self.remaining()`, a `u64`, so the narrowing to `usize` is
        // exact on any target that can hold the limit.
        let offered = u64::try_from(buf.remaining()).unwrap_or(u64::MAX);
        let wanted = self.remaining().min(offered);

        // `usize::MAX`, not zero. The minimum is at most `offered`, which is a
        // real `usize` widened, so the conversion cannot actually fail — but the
        // fallback is what decides what happens if it ever did, and a zero here
        // means `Ok(())` on the next line: a reader reporting end of stream while
        // it still owes bytes. `usize::MAX` means "as much as you can take",
        // which is what the scratch clamp below does with it anyway.
        let wanted = usize::try_from(wanted).unwrap_or(usize::MAX);

        // The caller has no room. Polling the inner reader with an empty buffer
        // would make it report end of stream, and keep doing so, so the inner
        // reader is left untouched.
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

impl<R> tokio::io::AsyncSeek for SliceAsyncReader<R>
where
    R: tokio::io::AsyncSeek + Unpin,
{
    /// # Errors
    ///
    /// Returns an error for a seek before the start of the slice, one past its
    /// limit, or one whose delta is too large to express as a relative seek.
    #[tracing::instrument(level = "trace", skip(self), err)]
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        let new_position = match position {
            std::io::SeekFrom::Start(n) => n,
            std::io::SeekFrom::Current(n) => {
                self.position.checked_add_signed(n).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "seek before start")
                })?
            }
            std::io::SeekFrom::End(n) => self.limit.checked_add_signed(n).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "seek before start")
            })?,
        };

        if new_position > self.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek outside limit",
            ));
        }

        // Relative, because the inner reader's origin is wherever the caller left it.
        let delta = i128::from(new_position) - i128::from(self.position);
        self.position = new_position;

        let delta = i64::try_from(delta).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek delta is too large to express as a relative seek",
            )
        })?;

        Pin::new(&mut self.inner).start_seek(std::io::SeekFrom::Current(delta))
    }

    #[tracing::instrument(level = "trace", skip(self, cx), ret)]
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let result = Pin::new(&mut self.inner).poll_complete(cx);

        match result {
            Poll::Ready(Ok(_n)) => Poll::Ready(Ok(self.position)),
            other => other,
        }
    }
}

/// An [`AsyncWrite`] that forwards at most `limit` bytes to `inner` and then
/// reports end of stream.
///
/// Reporting end of stream by returning `Ok(0)` is what makes
/// [`tokio::io::copy`] stop at exactly the limit. It is *not* a valid response
/// for [`tokio::io::AsyncWriteExt::write_all`], which treats a zero-length
/// write as an error, so this type is for `copy` and similar.
///
/// The `AsyncWrite + Unpin` bound is on the impls rather than here, as it is for
/// [`SliceAsyncReader`] above. On the struct it would be a second thing to keep in
/// step with the impls, and it already was: three of them repeated it, and the
/// type could not be *named* without satisfying it — which is a bound on a type
/// that holds an `AsyncWrite` and does nothing else with it yet.
pub struct SliceAsyncWriter<W> {
    inner: W,
    remaining: u64,
}

impl<W> std::fmt::Debug for SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-written, so `W` need not be `Debug`.
        f.debug_struct("SliceAsyncWriter")
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

impl<W> SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    /// Wrap `writer`, allowing at most `limit` bytes through.
    pub fn new(writer: W, limit: u64) -> Self {
        Self {
            inner: writer,
            remaining: limit,
        }
    }

    /// How many of the permitted bytes have not been written yet.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl<W> AsyncWrite for SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(0));
        }

        // Clamped in `usize` space, where `buf.len()` already is: taking the
        // minimum in `u64` and narrowing after would be lossy on a 32-bit target.
        // A limit above `usize::MAX` can only mean "more than any buffer".
        let remaining = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        let limit = std::cmp::min(remaining, buf.len());

        let result = Pin::new(&mut self.inner).poll_write(cx, &buf[..limit]);

        if let Poll::Ready(Ok(n)) = &result {
            self.remaining -= *n as u64;
        }

        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// An [`AsyncRead`] over the decompressed bytes of a compressed payload,
/// decompressed in one go on a blocking thread and served from memory.
///
/// It is seekable because [`crate::storage::AsyncReadSeek`] is part of the reader
/// every storage backend hands out, and a streaming zstd frame cannot seek
/// backwards without an index.
pub struct DecompressReader {
    bytes: Vec<u8>,
    position: u64,
}

impl std::fmt::Debug for DecompressReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecompressReader")
            .field("len", &self.bytes.len())
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

impl DecompressReader {
    /// Decompress a zstd frame into a reader, on a blocking thread.
    ///
    /// The codec is not a parameter: this is the reader for a zstd frame and
    /// nothing else. The decode behind it is [`Zstd`], the one [`Decompressor`]
    /// this crate ships, so the validation of the frame against `raw_len` lives
    /// with the codec rather than here — this used to hold a second copy of it,
    /// one that could disagree with the copy the store used.
    ///
    /// # Errors
    ///
    /// Whatever [`Zstd::decompress`] returns: the frame must be a readable zstd
    /// frame, must declare a size, and that size must be `raw_len`.
    pub async fn new(compressed: Vec<u8>, raw_len: u64) -> std::io::Result<Self> {
        let bytes = Decompressor::decompress(&Zstd, compressed, raw_len).await?;

        Ok(Self { bytes, position: 0 })
    }

    /// Serve already-decoded payload bytes.
    ///
    /// The other half of [`DecompressReader::new`], for a caller that has decoded
    /// through [`Decompressor`] itself — which is what [`super::BlobStore`] does,
    /// because it is the store that reads the codec byte and so is the place that
    /// knows which decompressor applies. Two constructors rather than one taking
    /// a decompressor, because a reader does not choose how its bytes were made.
    #[must_use]
    pub fn from_decoded(bytes: Vec<u8>) -> Self {
        Self { bytes, position: 0 }
    }
}

impl AsyncRead for DecompressReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // `unwrap_or(self.bytes.len())` rather than `unwrap_or(0)`: a position
        // this process cannot index is past the end by definition, so it reads
        // as empty. Falling back to 0 would replay the payload from the start.
        let Some(at) = usize::try_from(self.position)
            .ok()
            .filter(|at| *at < self.bytes.len())
        else {
            return Poll::Ready(Ok(()));
        };

        let wanted = buf.remaining().min(self.bytes.len() - at);
        buf.put_slice(&self.bytes[at..at + wanted]);
        self.position += u64::try_from(wanted).unwrap_or(0);

        Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for DecompressReader {
    fn start_seek(mut self: Pin<&mut Self>, position: tokio::io::SeekFrom) -> std::io::Result<()> {
        let target: i128 = match position {
            tokio::io::SeekFrom::Start(at) => i128::from(at),
            tokio::io::SeekFrom::End(offset) => {
                let len = self.bytes.len() as i128;
                len.saturating_add(i128::from(offset))
            }
            tokio::io::SeekFrom::Current(offset) => {
                let at = i128::from(self.position);
                at.saturating_add(i128::from(offset))
            }
        };

        // The `i128` target makes a negative position impossible to convert, so
        // the conversion is the sign check. The upper bound is a separate
        // question, and this reader is handed out by `StorageGet::get`, which
        // promises a stream "bounded to the blob" — so a position past the
        // payload is refused here rather than left to underflow in `poll_read`.
        // `SliceAsyncReader` bounds its seek the same way, for the same reason.
        let target = u64::try_from(target).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot seek before the start of a payload",
            )
        })?;
        if target > self.bytes.len() as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "cannot seek to byte {target} of a {}-byte payload: it is past the end",
                    self.bytes.len()
                ),
            ));
        }

        // Assigned only once known to be in bounds, so a refused seek leaves the
        // reader exactly where it was.
        self.position = target;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
