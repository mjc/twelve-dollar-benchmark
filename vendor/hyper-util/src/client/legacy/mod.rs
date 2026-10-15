#[cfg(any(feature = "http1", feature = "http2"))]
mod client;
#[cfg(any(feature = "http1", feature = "http2"))]
pub use client::{Builder, Client, Error, ResponseFuture};

pub mod connect;
#[doc(hidden)]
pub use hyper::ext::RequestTiming;

tokio::task_local! {
    /// Benchmark timing scope, forwarded to the HTTP/1 dispatcher.
    pub static REQUEST_TIMING: hyper::ext::RequestTiming;
}
#[doc(hidden)]
// Publicly available, but just for legacy purposes. A better pool will be
// designed.
pub mod pool;
