use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use std::{
    io,
    ops::Deref,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::StreamReader;

const READ_BUFFER_BYTES: usize = 8192;
const GZIP_INPUT_BUFFER_BYTES: usize = 1024;
const MAX_EXCERPT_BYTES: usize = 1025;
const MAX_POOLED_BUFFERS: usize = 64;
const MAX_POOLED_CAPACITY: usize = 64 * 1024;

static BUFFER_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

pub(crate) struct RetainedBody(BufferLease);

impl Deref for RetainedBody {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct BufferLease(Option<Vec<u8>>);

impl BufferLease {
    fn new(buffer: Vec<u8>) -> Self {
        Self(Some(buffer))
    }
}

impl Deref for BufferLease {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("buffer lease is active")
    }
}

impl std::ops::DerefMut for BufferLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("buffer lease is active")
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.0.take() {
            return_buffer(buffer);
        }
    }
}

pub(super) struct ResponseBody {
    pub retained: Option<RetainedBody>,
    pub error_excerpt: Bytes,
}

#[derive(Debug)]
pub(super) struct BodyReadError(io::Error);

impl std::fmt::Display for BodyReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for BodyReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

pub(super) async fn read_response(
    body: Incoming,
    gzip: bool,
    retain: bool,
    excerpt_cap: usize,
) -> Result<ResponseBody, BodyReadError> {
    let excerpt_cap = excerpt_cap.min(MAX_EXCERPT_BYTES);
    if !gzip {
        return read_plain(body, retain, excerpt_cap).await;
    }

    Box::pin(read_gzip(body, retain, excerpt_cap)).await
}

async fn read_gzip(
    body: Incoming,
    retain: bool,
    excerpt_cap: usize,
) -> Result<ResponseBody, BodyReadError> {
    let mut stream = body.into_data_stream().map_err(io::Error::other);
    let first = loop {
        match stream.try_next().await.map_err(BodyReadError)? {
            Some(chunk) if chunk.is_empty() => {}
            chunk => break chunk,
        }
    };
    let Some(first) = first else {
        return Ok(empty_response(retain));
    };
    let stream =
        futures_util::stream::once(std::future::ready(Ok::<_, io::Error>(first))).chain(stream);
    let input_buffer = take_buffer(GZIP_INPUT_BUFFER_BYTES);
    let reader = PooledBufReader::new(StreamReader::new(stream), input_buffer);
    let mut decoder = async_compression::tokio::bufread::GzipDecoder::new(reader);
    decoder.multiple_members(true);
    read_decoded(decoder, retain, excerpt_cap).await
}

struct PooledBufReader<R> {
    inner: R,
    buffer: Vec<u8>,
    position: usize,
    filled: usize,
}

impl<R> PooledBufReader<R> {
    fn new(inner: R, mut buffer: Vec<u8>) -> Self {
        buffer.resize(GZIP_INPUT_BUFFER_BYTES, 0);
        Self {
            inner,
            buffer,
            position: 0,
            filled: 0,
        }
    }
}

impl<R> Drop for PooledBufReader<R> {
    fn drop(&mut self) {
        return_buffer(std::mem::take(&mut self.buffer));
    }
}

impl<R: AsyncRead + Unpin> AsyncBufRead for PooledBufReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.position >= this.filled {
            let filled = {
                let mut read_buf = ReadBuf::new(&mut this.buffer);
                match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => read_buf.filled().len(),
                }
            };
            this.position = 0;
            this.filled = filled;
        }
        Poll::Ready(Ok(&this.buffer[this.position..this.filled]))
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.position = this.position.saturating_add(amount).min(this.filled);
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for PooledBufReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let available = match self.as_mut().poll_fill_buf(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(available)) => available,
        };
        let count = available.len().min(buffer.remaining());
        buffer.put_slice(&available[..count]);
        self.consume(count);
        Poll::Ready(Ok(()))
    }
}

fn empty_response(retain: bool) -> ResponseBody {
    ResponseBody {
        retained: retain.then(|| RetainedBody(BufferLease::new(Vec::new()))),
        error_excerpt: Bytes::new(),
    }
}

async fn read_plain(
    mut body: Incoming,
    retain: bool,
    excerpt_cap: usize,
) -> Result<ResponseBody, BodyReadError> {
    let mut retained = retain.then(|| BufferLease::new(take_buffer(READ_BUFFER_BYTES)));
    let mut excerpt = if excerpt_cap > 0 {
        Vec::with_capacity(excerpt_cap)
    } else {
        Vec::new()
    };

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| BodyReadError(io::Error::other(error)))?;
        if let Ok(data) = frame.into_data() {
            if let Some(retained) = &mut retained {
                retained.extend_from_slice(&data);
            }
            let count = excerpt_cap.saturating_sub(excerpt.len()).min(data.len());
            excerpt.extend_from_slice(&data[..count]);
        }
    }

    Ok(ResponseBody {
        retained: retained.map(RetainedBody),
        error_excerpt: Bytes::from(excerpt),
    })
}

async fn read_decoded<R: AsyncRead + Unpin>(
    mut reader: R,
    retain: bool,
    excerpt_cap: usize,
) -> Result<ResponseBody, BodyReadError> {
    let mut retained = retain.then(|| BufferLease::new(take_buffer(READ_BUFFER_BYTES)));
    let mut excerpt = if excerpt_cap > 0 {
        Vec::with_capacity(excerpt_cap)
    } else {
        Vec::new()
    };
    let mut scratch = BufferLease::new(take_buffer(READ_BUFFER_BYTES));
    scratch.resize(READ_BUFFER_BYTES, 0);

    loop {
        let read = reader.read(&mut scratch).await.map_err(BodyReadError)?;
        if read == 0 {
            break;
        }

        if let Some(retained) = &mut retained {
            retained.extend_from_slice(&scratch[..read]);
        }
        let count = excerpt_cap.saturating_sub(excerpt.len()).min(read);
        excerpt.extend_from_slice(&scratch[..count]);
    }

    Ok(ResponseBody {
        retained: retained.map(RetainedBody),
        error_excerpt: Bytes::from(excerpt),
    })
}

fn take_buffer(min_capacity: usize) -> Vec<u8> {
    let mut pool = BUFFER_POOL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    take_buffer_from(&mut pool, min_capacity)
}

fn take_buffer_from(pool: &mut Vec<Vec<u8>>, min_capacity: usize) -> Vec<u8> {
    pool.iter()
        .position(|buffer| buffer.capacity() >= min_capacity)
        .map_or_else(
            || Vec::with_capacity(min_capacity),
            |index| pool.swap_remove(index),
        )
}

fn return_buffer(buffer: Vec<u8>) {
    let mut pool = BUFFER_POOL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    return_buffer_to(&mut pool, buffer);
}

fn return_buffer_to(pool: &mut Vec<Vec<u8>>, mut buffer: Vec<u8>) {
    if buffer.capacity() < GZIP_INPUT_BUFFER_BYTES
        || buffer.capacity() > MAX_POOLED_CAPACITY
        || pool.len() >= MAX_POOLED_BUFFERS
    {
        return;
    }
    buffer.clear();
    pool.push(buffer);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use std::io::Write;
    use tokio_util::io::StreamReader;

    fn memory_reader(chunks: Vec<Bytes>) -> impl AsyncRead + Unpin {
        let stream = stream::iter(chunks.into_iter().map(Ok::<_, io::Error>));
        StreamReader::new(stream)
    }

    #[tokio::test]
    async fn retains_body_and_limits_excerpt() {
        let body = b"feed body";
        let result = read_decoded(
            memory_reader(vec![
                Bytes::from_static(b"feed "),
                Bytes::from_static(b"body"),
            ]),
            true,
            4,
        )
        .await
        .unwrap();

        assert_eq!(&*result.retained.unwrap(), body);
        assert_eq!(result.error_excerpt.as_ref(), b"feed");
    }

    #[tokio::test]
    async fn drains_without_retaining_or_excerpt() {
        let result = read_decoded(
            memory_reader(vec![Bytes::from_static(b"discarded")]),
            false,
            0,
        )
        .await
        .unwrap();

        assert!(result.retained.is_none());
        assert!(result.error_excerpt.is_empty());
    }

    #[tokio::test]
    async fn streams_multiple_gzip_members_and_reports_invalid_input() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"decoded").unwrap();
        let mut compressed = encoder.finish().unwrap();
        let mut second = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        second.write_all(b" member").unwrap();
        compressed.extend(second.finish().unwrap());

        let mut decoder =
            async_compression::tokio::bufread::GzipDecoder::new(PooledBufReader::new(
                memory_reader(vec![Bytes::from(compressed)]),
                take_buffer(GZIP_INPUT_BUFFER_BYTES),
            ));
        decoder.multiple_members(true);
        let result = read_decoded(decoder, true, 0).await.unwrap();
        assert_eq!(&*result.retained.unwrap(), b"decoded member");

        let mut decoder =
            async_compression::tokio::bufread::GzipDecoder::new(PooledBufReader::new(
                memory_reader(vec![Bytes::from_static(b"not gzip")]),
                take_buffer(GZIP_INPUT_BUFFER_BYTES),
            ));
        decoder.multiple_members(true);
        assert!(read_decoded(decoder, false, 0).await.is_err());
    }

    #[test]
    fn pool_reuses_suitable_buffers_and_drops_oversized_or_excess_buffers() {
        let mut pool = Vec::new();
        let mut buffer = Vec::with_capacity(READ_BUFFER_BYTES);
        buffer.extend_from_slice(b"old");
        let pointer = buffer.as_ptr();
        return_buffer_to(&mut pool, buffer);
        let reused = take_buffer_from(&mut pool, READ_BUFFER_BYTES);
        assert_eq!(reused.as_ptr(), pointer);

        let oversized = Vec::with_capacity(MAX_POOLED_CAPACITY + 1);
        return_buffer_to(&mut pool, oversized);
        assert_eq!(pool.as_slice(), &[] as &[Vec<u8>]);

        for _ in 0..=MAX_POOLED_BUFFERS {
            return_buffer_to(&mut pool, Vec::with_capacity(READ_BUFFER_BYTES));
        }
        assert_eq!(pool.len(), MAX_POOLED_BUFFERS);
    }
}
