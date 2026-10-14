# Local Hyper patch

This directory contains the published Hyper 1.12.0 crate, licensed under MIT.
The upstream license and notices are retained. The changes documented below
are local benchmark patches layered on that crate; the client dispatch queue
boxing is an additional local patch beyond the imported Axum compatibility
changes.

- Upstream source: https://github.com/hyperium/hyper/tree/v1.12.0
- Upstream commit: `7f67699ce32d717081deb7018b4da7d679277ac0`
- Published crate SHA-256: `2c3e324da4c95177d6291d4c8730197c0d1822f8a9766814a4a44fa5ab797c9c`

HTTP/1's initial adaptive read size is 1,024 bytes instead of 8,192. Adaptive
reads retain their growth behavior and decrease to the new initial size. The
configured minimum remains 8,192 bytes and the default maximum remains 417,792
bytes. The reusable write-header vector starts without a per-connection
reserve. The existing HTTP/1 encoders reserve an estimate of 30 bytes plus 30
bytes per header on first use; the vector can grow further for actual headers
or flattened pipeline output and retains capacity for reuse. This removes only
the initial reserve: existing `max_buf_size` enforcement is unchanged, and no
new cap is imposed on retained capacity. The read buffer sizing is unchanged by
this write-side allocation change.

With `writev(false)`, HTTP/1 queues the body's existing buffers and writes each
contiguous chunk with ordinary `poll_write` calls. It avoids copying response
bodies into a connection's retained header buffer. Partial writes and pending
writes advance only the bytes actually written. The vectored strategy remains
available with `writev(true)`; `pipeline_flush(true)` keeps the flattening path.
The scalar queue is implemented in `src/proto/h1/io.rs`, forwarded through
`src/proto/h1/conn.rs`, and selected by both HTTP/1 connection builders.
Scalar body queues leave the lazy write-header vector untouched, avoiding its
previous 1,024-byte per-connection initial reserve.
`BufList` now requests one element of deque capacity on its first queued body;
the application returns `Full<Cursor<Vec<u8>>>` bodies as a single data buffer.
Further streamed buffers still use normal deque growth and preserve ordering.
This is a structural allocation reduction, not yet a measured performance
result.

`src/proto/h1/dispatch.rs` allocates the server's pinned handler future lazily
for an active request and drops it at completion. Idle connections retain an
empty option rather than an allocated slot large enough for the handler future.
This trades one reusable per-connection allocation for an allocation per active
request.

The client dispatch queues store `Box<Envelope<T, U>>` instead of keeping the
request and callback inline in Tokio's unbounded-channel blocks. This makes each
queue slot pointer-sized and reduces the fixed channel-block storage retained
by idle connections. Sending a request now allocates its envelope separately;
queue ordering, readiness, cancellation, and retry ownership continue to follow
the existing dispatch behavior.

The adaptive-read tests use the new initial size. Added tests check initial read
and lazy write-header capacities, unchanged maximums, and rejection of a
configured maximum below 8,192 bytes. Additional tests cover scalar-write
ordering, partial and pending writes, zero-write errors, pipeline-flush
selection, and handler-future allocation and cleanup on success, error, and
cancellation. Client-dispatch tests also check that rejected sends return the
original request, receiver closure returns retryable requests, non-retryable
sends report cancellation and return rejected requests to the sender.
