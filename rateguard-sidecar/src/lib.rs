//! rateguard as a sidecar: a [`Guard`] behind a small HTTP API on loopback,
//! for services in any language.
//!
//! The binary, `rateguard-sidecar`, is configured with flags or
//! `RATEGUARD_*` variables; see its `--help`. This library is the API
//! itself, as an axum [`Router`], for embedding and for tests.
//!
//! | Method | Path | Body | Answer |
//! |---|---|---|---|
//! | POST | `/v1/check` | `{"key": "tenant:42", "class": "pro"}`, `class` optional | `200 {"allowed": true}` or `200 {"allowed": false, "retry_after_ms": 120}` |
//! | GET | `/v1/stats` | | [`Guard::stats`] as JSON |
//! | GET | `/healthz` | | `200` while the node runs, `503` once it has left or its task died |
//! | GET | `/metrics` | | Prometheus metrics, when an exporter is given |
//! | PUT | `/v1/classes` | `{"pro": {"per_second": 1000, "burst": 50}}` | `204`; admin only |
//! | PUT | `/v1/limit` | `{"per_second": 100, "burst": 5}` | `204`; admin only |
//! | PUT | `/v1/dry_run` | `{"on": true}` | `204`; admin only |
//!
//! A decision is always a `200`: a `429` is the service's answer to its own
//! client, not the sidecar's to the service. A request it cannot read is a
//! `400`. Without `admin` the admin routes are not there at all.
//!
//! [`public_app`] is the part that may face the pod's network, for
//! kubelet's probes and Prometheus: `/healthz` and `/metrics`, nothing that
//! checks or changes anything.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod args;

use std::borrow::Cow;
use std::collections::HashMap;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use metrics_exporter_prometheus::PrometheusHandle;
use rateguard::{Decision, Guard, Limit};
use serde::Deserialize;
use serde_json::{Value, json};

/// What the API serves.
#[derive(Clone)]
pub struct App {
    /// The node the checks go through.
    pub guard: Guard,
    /// Whether the routes that change limits are there.
    pub admin: bool,
    /// Renders `/metrics`; none, no such route.
    pub metrics: Option<PrometheusHandle>,
}

/// The API as a router.
pub fn app(app: App) -> Router {
    let mut router = Router::new()
        .route("/v1/check", post(check))
        .route("/v1/stats", get(stats))
        .route("/healthz", get(health));
    if let Some(handle) = &app.metrics {
        router = router.route("/metrics", metrics(handle.clone()));
    }
    if app.admin {
        router = router
            .route("/v1/classes", put(set_classes))
            .route("/v1/limit", put(set_limit))
            .route("/v1/dry_run", put(set_dry_run));
    }
    router.with_state(app)
}

/// Health and metrics only, for an address outside loopback.
pub fn public_app(app: App) -> Router {
    let mut router = Router::new().route("/healthz", get(health));
    if let Some(handle) = &app.metrics {
        router = router.route("/metrics", metrics(handle.clone()));
    }
    router.with_state(app)
}

/// Serves [`public_app`] on `listener` until the process stops.
///
/// Unlike `axum::serve`, a connection is closed once it has waited
/// `header_timeout` for a request's head: whether idle or sending it a byte
/// at a time. The port is open to the pod's network, and connections nobody
/// closes would take the file descriptors and memory of the process that
/// also answers the checks.
pub async fn serve_public(
    listener: tokio::net::TcpListener,
    router: Router,
    header_timeout: std::time::Duration,
) {
    use hyper_util::rt::{TokioIo, TokioTimer};
    use hyper_util::service::TowerToHyperService;

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            // Out of file descriptors, most likely: give the others a
            // moment to close rather than spin.
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }
        };
        let service = TowerToHyperService::new(router.clone());
        tokio::spawn(async move {
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout)
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

#[derive(Deserialize)]
struct Check<'a> {
    // As bytes: serde_json refuses a lone surrogate in a str but hands it
    // over as WTF-8 here. Python and JavaScript send one as readily as any
    // other key, and their clients take a 400 for an outage and fail open,
    // so whoever chooses the key would go unlimited.
    #[serde(borrow, with = "serde_bytes")]
    key: Cow<'a, [u8]>,
    #[serde(borrow, default)]
    class: Option<Cow<'a, str>>,
}

async fn check(State(app): State<App>, body: Bytes) -> Response {
    let check: Check<'_> = match serde_json::from_slice(&body) {
        Ok(check) => check,
        Err(error) => return bad_request(error),
    };
    // Lossy, as the Go client is: two keys that differ only in their
    // surrogates share a limit, which is stricter, never looser.
    let key = String::from_utf8_lossy(&check.key);
    let decision = match &check.class {
        Some(class) => app.guard.check_in(class, &key),
        None => app.guard.check(&key),
    };
    let json = [(header::CONTENT_TYPE, "application/json")];
    match decision {
        Decision::Allow => (json, r#"{"allowed":true}"#).into_response(),
        Decision::Deny { retry_after } => {
            // Whole milliseconds, rounded up: rounding down invites an early
            // retry.
            let ms = retry_after.as_nanos().div_ceil(1_000_000);
            (
                json,
                format!(r#"{{"allowed":false,"retry_after_ms":{ms}}}"#),
            )
                .into_response()
        }
    }
}

async fn stats(State(app): State<App>) -> Response {
    let s = app.guard.stats();
    let most_denied: Vec<Value> = s
        .most_denied
        .iter()
        // As strings: a JSON number loses a u64's last digits in most
        // languages.
        .map(|d| json!({"key_hash": d.key_hash.to_string(), "denied_per_sec": d.denied_per_sec}))
        .collect();
    axum::Json(json!({
        "admitted": s.admitted,
        "denied": s.denied,
        "overflowed": s.overflowed,
        "unknown_class": s.unknown_class,
        "cluster_size": s.cluster_size,
        "counted_size": s.counted_size,
        "suspects": s.suspects,
        "tracked_keys": s.tracked_keys,
        "hot_keys": s.hot_keys,
        "joining": s.joining,
        "learning": s.learning,
        "without_quorum": s.without_quorum,
        "since_last_round_ms": s.since_last_round.map(|d| d.as_millis() as u64),
        "running": s.running,
        "dry_run": s.dry_run,
        "most_denied": most_denied,
    }))
    .into_response()
}

async fn health(State(app): State<App>) -> StatusCode {
    if app.guard.is_running() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn metrics(handle: PrometheusHandle) -> axum::routing::MethodRouter<App> {
    get(move || {
        let text = handle.render();
        async move { ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text) }
    })
}

#[derive(Deserialize)]
struct LimitBody {
    per_second: u32,
    #[serde(default)]
    burst: Option<u32>,
}
impl LimitBody {
    fn limit(&self) -> Limit {
        let limit = Limit::per_second(self.per_second);
        match self.burst {
            Some(burst) => limit.burst(burst),
            None => limit,
        }
    }
}

async fn set_classes(State(app): State<App>, body: Bytes) -> Response {
    let classes: HashMap<String, LimitBody> = match serde_json::from_slice(&body) {
        Ok(classes) => classes,
        Err(error) => return bad_request(error),
    };
    let classes = classes
        .into_iter()
        .map(|(name, limit)| (name, limit.limit()));
    match app.guard.set_classes(classes) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => bad_request(error),
    }
}

async fn set_limit(State(app): State<App>, body: Bytes) -> Response {
    let limit: LimitBody = match serde_json::from_slice(&body) {
        Ok(limit) => limit,
        Err(error) => return bad_request(error),
    };
    match app.guard.set_limit(limit.limit()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => bad_request(error),
    }
}

#[derive(Deserialize)]
struct DryRun {
    on: bool,
}

async fn set_dry_run(State(app): State<App>, body: Bytes) -> Response {
    match serde_json::from_slice::<DryRun>(&body) {
        Ok(dry_run) => {
            app.guard.set_dry_run(dry_run.on);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => bad_request(error),
    }
}

fn bad_request(error: impl std::fmt::Display) -> Response {
    let body = axum::Json(json!({"error": error.to_string()}));
    (StatusCode::BAD_REQUEST, body).into_response()
}
