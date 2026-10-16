//! A [`tower`](https://docs.rs/tower) layer that limits HTTP requests by a
//! key taken from each request; with the `tower` feature.
//!
//! ```no_run
//! # async fn run() -> Result<(), rateguard::Error> {
//! use rateguard::{Guard, tower::RateLimitLayer};
//!
//! let guard = Guard::builder()
//!     .bind("0.0.0.0:7946")
//!     .advertise("10.0.0.3:7946")
//!     .seeds(["10.0.0.1:7946"])
//!     .limit(1_000)
//!     .spawn()?;
//!
//! // One limit per tenant; a request with no tenant is not limited.
//! let layer = RateLimitLayer::new(guard, |req: &http::Request<String>| {
//!     let tenant = req.headers().get("x-tenant")?.to_str().ok()?;
//!     Some(format!("api:{tenant}"))
//! });
//! # let _ = layer;
//! # Ok(())
//! # }
//! ```
//!
//! With axum, `Router::new().route(..).layer(layer)`.
//!
//! To check in a class of limits, return the class with the key, as a pair:
//! `Some((plan, format!("api:{tenant}")))`; see [`Key`].

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderValue, Request, Response, StatusCode, header};
use pin_project_lite::pin_project;
use tower_layer::Layer;
use tower_service::Service;

use crate::{Decision, Guard};

/// What the key function of a [`RateLimitLayer`] returns for a request: a
/// key in the default class, any string; or a pair `(class, key)`, checked
/// with [`Guard::check_in`].
pub trait Key {
    /// Checks the request through `guard`.
    fn check(&self, guard: &Guard) -> Decision;
}
macro_rules! key_in_the_default_class {
    ($($t:ty),*) => {$(
        impl Key for $t {
            fn check(&self, guard: &Guard) -> Decision {
                guard.check(self)
            }
        }
    )*};
}
key_in_the_default_class!(
    String,
    &str,
    Box<str>,
    std::sync::Arc<str>,
    std::borrow::Cow<'_, str>
);
impl<C: AsRef<str>, K: AsRef<str>> Key for (C, K) {
    fn check(&self, guard: &Guard) -> Decision {
        guard.check_in(self.0.as_ref(), self.1.as_ref())
    }
}

/// Limits each request by the key `key` takes from it: a denied request
/// gets `429 Too Many Requests` with `Retry-After` in whole seconds,
/// rounded up, and an empty body; a request for which `key` returns `None`
/// is not limited.
///
/// The key is any string, as for [`Guard::check`]: a tenant, a client IP,
/// a tenant and an endpoint.
#[derive(Debug, Clone)]
pub struct RateLimitLayer<F> {
    guard: Guard,
    key: F,
}
impl<F> RateLimitLayer<F> {
    /// A layer limiting through `guard` by the key `key` takes from each
    /// request.
    pub fn new(guard: Guard, key: F) -> Self {
        Self { guard, key }
    }
}
impl<S, F: Clone> Layer<S> for RateLimitLayer<F> {
    type Service = RateLimit<S, F>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimit {
            inner,
            guard: self.guard.clone(),
            key: self.key.clone(),
        }
    }
}

/// The service [`RateLimitLayer`] wraps around another.
#[derive(Debug, Clone)]
pub struct RateLimit<S, F> {
    inner: S,
    guard: Guard,
    key: F,
}
impl<S, F, K, ReqBody, ResBody> Service<Request<ReqBody>> for RateLimit<S, F>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    F: Fn(&Request<ReqBody>) -> Option<K>,
    K: Key,
    ResBody: Default,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future, ResBody>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let denied = (self.key)(&req).and_then(|key| match key.check(&self.guard) {
            Decision::Allow => None,
            Decision::Deny { retry_after } => Some(retry_after),
        });
        let kind = match denied {
            None => Kind::Served {
                future: self.inner.call(req),
            },
            Some(retry_after) => Kind::Denied {
                response: Some(too_many_requests(retry_after)),
            },
        };
        ResponseFuture { kind }
    }
}

// Whole seconds, rounded up, at least one: rounding down invites an early
// retry, and `Retry-After: 0` an immediate one.
fn too_many_requests<B: Default>(retry_after: Duration) -> Response<B> {
    let seconds = retry_after.as_millis().div_ceil(1000).max(1);
    let mut response = Response::new(B::default());
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(seconds as u64));
    response
}

pin_project! {
    /// The future of [`RateLimit`]: the wrapped service's, or a ready 429.
    pub struct ResponseFuture<F, B> {
        #[pin]
        kind: Kind<F, B>,
    }
}

pin_project! {
    #[project = KindProj]
    enum Kind<F, B> {
        Served { #[pin] future: F },
        Denied { response: Option<Response<B>> },
    }
}

impl<F, B, E> Future for ResponseFuture<F, B>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Served { future } => future.poll(cx),
            KindProj::Denied { response } => {
                Poll::Ready(Ok(response.take().expect("polled after it completed")))
            }
        }
    }
}
