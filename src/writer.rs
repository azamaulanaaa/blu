//! Bounded async writes.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::AsyncWrite;

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

        let limit = std::cmp::min(self.remaining, buf.len() as u64) as usize;

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
