//! Taken pretty much verbatim from the
//! [rdkafka](https://github.com/fede1024/rust-rdkafka/blob/8afecbc5ab2c775b8928f6129bbd38777eed11d7/src/util.rs)
//! utils crate. Removes the support for `spawn` and adds ` [async-std] implementation as well.

use std::future::Future;
#[cfg(feature = "async-std")]
use std::pin::Pin;
#[cfg(feature = "naive-runtime")]
use std::thread;
use std::time::Duration;

#[cfg(feature = "naive-runtime")]
use futures_channel::oneshot;
#[cfg(feature = "naive-runtime")]
use futures_util::future::{FutureExt, Map};

/// An abstraction over asynchronous runtimes.
///
/// There are several asynchronous runtimes available for Rust. By default
/// snowflake-api uses Tokio, via the [`TokioRuntime`], but it has pluggable
/// support for any runtime that can satisfy this trait.
///
/// For an example of using the [smol] runtime, see the [runtime_smol] example.
///
/// [async-std]: https://docs.rs/async-std
/// [tokio]: https://docs.rs/tokio
/// [futures_executor]: https://docs.rs/futures_executor
/// [runtime_smol]: https://github.com/mycelial/snowflake-rs/blob/main/snowflake-api/examples/runtime_smol.rs
pub trait AsyncRuntime: Send + Sync + 'static {
    /// The type of the future returned by
    /// [`delay_for`](AsyncRuntime::delay_for).
    type Delay: Future<Output = ()> + Send;

    /// Constructs a future that will resolve after `duration` has elapsed.
    fn delay_for(duration: Duration) -> Self::Delay;
}

/// The default [`AsyncRuntime`] used when one is not explicitly specified.
///
/// This is defined to be the [`TokioRuntime`] when the `tokio` feature is
/// enabled, or the [`NaiveRuntime`] if the `naive-runtime` feature is enabled,
/// or [`AsyncStdRuntime`] if the `async-std` feature is enabled.
///
/// If neither of the features are enabled, this is
/// defined to be `()`, which is not a valid `AsyncRuntime` and will cause
/// compilation errors if used as one. You will need to explicitly specify a
/// custom async runtime wherever one is required.
#[cfg(not(any(feature = "tokio", feature = "naive-runtime", feature = "async-std")))]
pub type DefaultRuntime = ();

/// The default [`AsyncRuntime`] used when one is not explicitly specified.
///
/// This is defined to be the [`TokioRuntime`] when the `tokio` feature is
/// enabled, or the [`NaiveRuntime`] if the `naive-runtime` feature is enabled,
/// or [`AsyncStdRuntime`] if the `async-std` feature is enabled.
///
/// If neither of the features are enabled, this is
/// defined to be `()`, which is not a valid `AsyncRuntime` and will cause
/// compilation errors if used as one. You will need to explicitly specify a
/// custom async runtime wherever one is required.
#[cfg(all(
    not(feature = "tokio"),
    not(feature = "async-std"),
    feature = "naive-runtime"
))]
pub type DefaultRuntime = NaiveRuntime;

/// The default [`AsyncRuntime`] used when one is not explicitly specified.
///
/// This is defined to be the [`TokioRuntime`] when the `tokio` feature is
/// enabled, or the [`NaiveRuntime`] if the `naive-runtime` feature is enabled,
/// or [`AsyncStdRuntime`] if the `async-std` feature is enabled.
///
/// If neither of the features are enabled, this is
/// defined to be `()`, which is not a valid `AsyncRuntime` and will cause
/// compilation errors if used as one. You will need to explicitly specify a
/// custom async runtime wherever one is required.
#[cfg(all(
    feature = "tokio",
    not(feature = "async-std"),
    not(feature = "naive-runtime")
))]
pub type DefaultRuntime = TokioRuntime;

/// The default [`AsyncRuntime`] used when one is not explicitly specified.
///
/// This is defined to be the [`TokioRuntime`] when the `tokio` feature is
/// enabled, or the [`NaiveRuntime`] if the `naive-runtime` feature is enabled,
/// or [`AsyncStdRuntime`] if the `async-std` feature is enabled.
///
/// If neither of the features are enabled, this is
/// defined to be `()`, which is not a valid `AsyncRuntime` and will cause
/// compilation errors if used as one. You will need to explicitly specify a
/// custom async runtime wherever one is required.
#[cfg(all(
    not(feature = "tokio"),
    feature = "async-std",
    not(feature = "naive-runtime")
))]
pub type DefaultRuntime = AsyncStdRuntime;

/// An [`AsyncRuntime`] implementation backed by the executor in the
/// [`futures_executor`](futures_executor) crate.
///
/// This runtime should not be used when performance is a concern, as it makes
/// heavy use of threads to compensate for the lack of a timer in the futures
/// executor.
#[cfg(feature = "naive-runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "naive-runtime")))]
pub struct NaiveRuntime;

#[cfg(feature = "naive-runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "naive-runtime")))]
impl AsyncRuntime for NaiveRuntime {
    type Delay = Map<oneshot::Receiver<()>, fn(Result<(), oneshot::Canceled>)>;

    fn delay_for(duration: Duration) -> Self::Delay {
        let (tx, rx) = oneshot::channel();
        thread::spawn(move || {
            thread::sleep(duration);
            tx.send(())
        });
        rx.map(|_| ())
    }
}

/// An [`AsyncRuntime`] implementation backed by [Tokio](tokio).
///
/// This runtime is used by default throughout the crate, unless the `tokio`
/// feature is disabled.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub struct TokioRuntime;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl AsyncRuntime for TokioRuntime {
    type Delay = tokio::time::Sleep;

    fn delay_for(duration: Duration) -> Self::Delay {
        tokio::time::sleep(duration)
    }
}

/// An [`AsyncRuntime`] implementation backed by [async-std].
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub struct AsyncStdRuntime;

#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
impl AsyncRuntime for AsyncStdRuntime {
    type Delay = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn delay_for(duration: Duration) -> Self::Delay {
        Box::pin(async_std::task::sleep(duration))
    }
}
