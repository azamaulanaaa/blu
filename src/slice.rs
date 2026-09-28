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
//! general adapters, which is why they live here rather than in [`crate::reader`]
//! next to the chunker.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Size of the stack scratch buffer used to stage bytes out of the inner
/// reader.
///
/// The inner reader cannot be handed the caller's buffer directly, because only
/// this type knows how much of it is within the limit. Matches
/// `tokio::io::copy`'s own buffer size so a copy through this reader does not
/// split every read in two.
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
        // Deliberately not derived: `R` is an arbitrary stream type, and
        // requiring `R: Debug` here would be a needless bound.
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

impl<R> tokio::io::AsyncSeek for SliceAsyncReader<R>
where
    R: tokio::io::AsyncSeek + Unpin,
{
    /// Seeks are per slice rather than per read, so tracing them is cheap.
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

        // Move the inner reader by a relative delta rather than an absolute
        // position, because its own origin is wherever the caller left it.
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
pub struct SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    inner: W,
    remaining: u64,
}

impl<W> std::fmt::Debug for SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately not derived: `W` is an arbitrary writer type.
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

        // Clamped in `usize` space, where `buf.len()` already is. Taking the
        // minimum in `u64` and narrowing afterwards would be lossy on a 32-bit
        // target; converting the limit first is exact everywhere, because a
        // limit above `usize::MAX` can only mean "more than any buffer".
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

/// An [`AsyncRead`] that yields the decompressed bytes of a compressed payload.
///
/// The compressed form is decompressed in one go, on a blocking thread, and the
/// result is served from memory. That is a deliberate shape rather than a lazy
/// stream:
///
/// * The payloads this is used for are chunks, which the chunker already caps
///   (64 KiB by default), so buffering one is the same order of memory the backup
///   pipeline already holds. A restore that streamed compressed bytes through a
///   streaming decoder would hold less, but would pay a decode call per read for
///   the privilege.
/// * Seeking is the real constraint. [`crate::storage::AsyncReadSeek`] is part of
///   the reader type every storage backend hands out, and a streaming zstd frame
///   cannot seek backwards without an index. Buffering makes seek a slice, which
///   is what a caller expects and needs.
///
/// A future format that stores large multi-gigabyte payloads would want the
/// streaming decoder here instead, and the seam for it is this one type.
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
    /// Decompress `compressed` into a reader, on a blocking thread.
    ///
    /// `raw_len` is the expected size of the result and is used to pre-size the
    /// buffer; a frame that does not decode to exactly that many bytes is an
    /// error, because a record's raw length is in its header and disagreeing with
    /// it means the record is not what the store says it is.
    ///
    /// The frame's own idea of how many bytes it decodes to, read out of its
    /// header, checked against the length the record claims before anything is
    /// allocated.
    ///
    /// This is the step that keeps a damaged or hostile record from being a crash.
    /// `raw_len` comes out of a record header, and a header is covered by a
    /// truncated hash rather than a MAC — so a file that has been tampered with
    /// could carry a perfectly valid check and an absurd length. Handing that to
    /// the decompressor means either a multi-gigabyte allocation or, when the
    /// frame turns out to be *longer* than the buffer, a panic from deep inside
    /// zstd. Neither is a thing a read of a backup should be able to do.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a zstd frame, if the frame does not
    /// declare a size, if that size is not `raw_len`, or if `raw_len` is not
    /// something this process can hold in memory.
    pub async fn new(compressed: Vec<u8>, raw_len: u64) -> std::io::Result<Self> {
        let expected = usize::try_from(raw_len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("a payload of {raw_len} bytes does not fit in memory"),
            )
        })?;

        // `None` means the frame does not say how big it is, which happens when
        // whatever produced it was compressing a stream rather than a buffer and
        // could not know the length in advance. Nothing here produces such a
        // frame, and accepting one would mean decoding blind — which is exactly
        // the allocation this check exists to prevent.
        let declared = zstd::zstd_safe::get_frame_content_size(&compressed)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("a compressed payload is not a readable zstd frame: {e}"),
                )
            })?
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "a compressed payload does not say how many bytes it decodes to",
                )
            })?;

        if usize::try_from(declared).unwrap_or(usize::MAX) != expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "a compressed payload declares {declared} bytes but its record claims \
                     {expected}"
                ),
            ));
        }

        // Now that the frame and the header agree, the decode cannot come up
        // short or long, and the buffer is exactly the size it needs to be.
        let bytes =
            tokio::task::spawn_blocking(move || zstd::bulk::decompress(&compressed, expected))
                .await
                .map_err(|join| {
                    std::io::Error::other(format!("decompression panicked: {join}"))
                })??;

        if bytes.len() != expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "a compressed payload decoded to {} bytes but its record claims {expected}",
                    bytes.len()
                ),
            ));
        }

        Ok(Self { bytes, position: 0 })
    }
}

impl AsyncRead for DecompressReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let remaining = self.bytes.len() - usize::try_from(self.position).unwrap_or(0);
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let wanted = buf.remaining().min(remaining);
        let at = usize::try_from(self.position).unwrap_or(0);
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

        // The `i128` above is what makes a negative target impossible to reach
        // as a `u64`, so the conversion is also the bounds check.
        self.position = u64::try_from(target).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot seek before the start of a payload",
            )
        })?;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
