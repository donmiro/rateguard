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
//! | GET | `/v1/stats` | | [`Guard::stats`] as JSON, `most_denied` named by `key` and `class` while the key is in [`DeniedKeys`] |
//! | GET | `/healthz` | | `200` while the node runs, `503` once it has left or its task died |
//! | GET | `/metrics` | | Prometheus metrics, when an exporter is given |
//! | PUT | `/v1/classes` | `{"pro": {"per_second": 1000, "burst": 50}}` | `204`; admin only |
//! | PUT | `/v1/limit` | `{"per_second": 100, "burst": 5}` | `204`; admin only |
//! | PUT | `/v1/dry_run` | `{"on": true}` | `204`; admin only |
//! | PUT | `/v1/key_limits` | `[{"class": "pro", "key": "tenant:42", "limit": {"per_second": 50}}, {"key": "tenant:7", "as_class": "pro"}]` | `204`; replaces all; admin only |
//! | PUT | `/v1/shadows` | `{"free": {"per_second": 10}, "": null}` | `204`; replaces all, `""` is the default class, `null` or a missing class has no shadow; admin only |
//! | GET | `/v1/keys?key=tenant:42&class=pro` | | `200` with the key's rates, share, `limit` and `shadow`, or `404` while it is not tracked; loopback only |
//!
//! A limit, in `/v1/classes` and `/v1/limit`, has exactly one period key,
//! `per_second`, `per_minute` or `per_hour`, such as
//! `{"per_minute": 600, "burst": 20}`; `burst` is optional. In
//! `/v1/key_limits` an entry has exactly one of `limit` and `as_class`, and
//! `class` may be left out for the default class. An `as_class` this node
//! does not have is taken: the key gets the default limit, and its checks
//! are counted in `unknown_class`, as with [`rateguard::KeyLimit::Class`].
//!
//! A decision is always a `200`: a `429` is the service's answer to its own
//! client, not the sidecar's to the service. A request it cannot read is a
//! `400`. Without `admin` the admin routes are not there at all.
//!
//! [`public_app`] is the part that may face the pod's network, for
//! kubelet's probes and Prometheus: `/healthz` and `/metrics`, nothing that
//! checks or changes anything, and nothing that names a key: `/v1/keys` and
//! the key names in `/v1/stats` may be personal data.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod args;
pub mod denied;
pub mod secrets;

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use metrics_exporter_prometheus::PrometheusHandle;
use rateguard::{Decision, GossipAuth, Guard, KeyLimit, Limit};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::denied::DeniedKeys;
use crate::secrets::read_secrets;

/// What the API serves.
#[derive(Clone)]
pub struct App {
    /// The node the checks go through.
    pub guard: Guard,
    /// Whether the routes that change limits are there.
    pub admin: bool,
    /// Renders `/metrics`; none, no such route.
    pub metrics: Option<PrometheusHandle>,
    /// The keys denied lately, for naming `/v1/stats` `most_denied`. Key
    /// strings may be personal data: they are served there, on loopback,
    /// and nowhere else, never in metrics.
    pub denied: Arc<Mutex<DeniedKeys>>,
}

impl App {
    /// An API over `guard`, remembering the last 1024 keys denied, and
    /// apart from them the last 1024 only a shadow denied.
    pub fn new(guard: Guard, admin: bool, metrics: Option<PrometheusHandle>) -> Self {
        App {
            guard,
            admin,
            metrics,
            denied: Arc::default(),
        }
    }
}

/// The API as a router.
pub fn app(app: App) -> Router {
    let mut router = Router::new()
        .route("/v1/check", post(check))
        .route("/v1/stats", get(stats))
        .route("/v1/keys", get(key_stats))
        .route("/healthz", get(health));
    if let Some(handle) = &app.metrics {
        router = router.route("/metrics", metrics(handle.clone()));
    }
    if app.admin {
        router = router
            .route("/v1/classes", put(set_classes))
            .route("/v1/limit", put(set_limit))
            .route("/v1/dry_run", put(set_dry_run))
            .route("/v1/key_limits", put(set_key_limits))
            .route("/v1/shadows", put(set_shadows));
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

/// Rereads the gossip secrets in `path` every `every`, and hands them to
/// `guard` in `mode`, until the node stops. A file that cannot be read or
/// is refused leaves the secrets in force alone: it is said on stderr, by
/// line number and never by content, and counted in
/// `rateguard_gossip_secret_reload_errors_total`.
///
/// Kubernetes updates a mounted Secret in place a minute or two after it
/// changes, so a rotation needs no restart. The mode stays as it was.
///
/// It holds a clone of `guard`, so the node keeps running until
/// `guard.leave()`: dropping your own `Guard`s does not stop it.
pub async fn reload_secrets(guard: Guard, path: PathBuf, mode: GossipAuth, every: Duration) {
    // Zero from the start, rather than absent until the first failure.
    reload_errors().increment(0);
    loop {
        tokio::time::sleep(every).await;
        if !guard.is_running() {
            return;
        }
        let reloaded = read_secrets(&path)
            .map_err(|error| error.to_string())
            .and_then(|secrets| {
                guard
                    .set_gossip_auth(mode, secrets)
                    .map_err(|error| error.to_string())
            });
        if let Err(error) = reloaded {
            eprintln!("rateguard-sidecar: keeping the previous gossip secrets: {error}");
            reload_errors().increment(1);
        }
    }
}

// No `guard` label, unlike the library's metrics: a sidecar runs one node,
// and its advertised address is not known here.
fn reload_errors() -> metrics::Counter {
    metrics::counter!("rateguard_gossip_secret_reload_errors_total")
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
    let class = check.class.as_deref().unwrap_or("");
    let checked = match &check.class {
        Some(class) => app.guard.check_in_traced(class, &key),
        None => app.guard.check_traced(&key),
    };
    let json = [(header::CONTENT_TYPE, "application/json")];
    // A shadow's denial names the key as a real one does, for
    // `most_shadow_denied`, in a table of its own so that it pushes out no
    // real one's name; the answer is the real decision's alone. Only
    // here: an allowed check takes no lock and hashes once. A dry run's
    // would-be denial is answered as allowed, so it is not recorded either.
    let shadow_denied = matches!(checked.shadow, Some(Decision::Deny { .. }));
    let denied = matches!(checked.decision, Decision::Deny { .. });
    if shadow_denied || denied {
        let hash = app.guard.key_hash_in(class, &key);
        let mut named = app.denied.lock().unwrap_or_else(PoisonError::into_inner);
        if denied {
            named.record(hash, class, &key);
        } else {
            named.record_shadow(hash, class, &key);
        }
    }
    match checked.decision {
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
    let named = app.denied.lock().unwrap_or_else(PoisonError::into_inner);
    let name_all = |list: &[rateguard::DeniedKey]| -> Vec<Value> {
        list.iter()
            .map(|d| {
                // As strings: a JSON number loses a u64's last digits in
                // most languages.
                let mut entry = json!({
                    "key_hash": d.key_hash.to_string(),
                    "denied_per_sec": d.denied_per_sec,
                    "requests_per_sec": d.requests_per_sec,
                    "shadow_denied_per_sec": d.shadow_denied_per_sec,
                });
                if let Some(named) = named.get(d.key_hash) {
                    entry["key"] = named.key.as_str().into();
                    entry["class"] = named.class.as_str().into();
                    if named.truncated {
                        entry["key_truncated"] = true.into();
                    }
                }
                entry
            })
            .collect()
    };
    let most_denied = name_all(&s.most_denied);
    let most_shadow_denied = name_all(&s.most_shadow_denied);
    drop(named);
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
        "shadow_denied": s.shadow_denied,
        "most_denied": most_denied,
        "most_shadow_denied": most_shadow_denied,
        "gossip_auth": args::mode_name(s.gossip_auth),
        "rejected_datagrams": {
            "unsigned": s.rejected_datagrams.unsigned,
            "bad_tag": s.rejected_datagrams.bad_tag,
            "malformed": s.rejected_datagrams.malformed,
        },
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

/// A limit from its JSON form: exactly one of `per_second`, `per_minute`
/// and `per_hour`, and optionally `burst`. The one parser of limits, for
/// every route that takes one; a body with only `per_second` and `burst`
/// is the 0.6 form and reads as before.
pub(crate) fn limit_from_json(value: &Value) -> Result<Limit, String> {
    let object = value
        .as_object()
        .ok_or("a limit is an object such as {\"per_second\": 100}")?;
    let count = |key: &str, value: &Value| -> Result<u32, String> {
        value
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| format!("{key} is not a whole number that fits in 32 bits"))
    };
    let mut limit = None;
    for (key, make) in [
        ("per_second", Limit::per_second as fn(u32) -> Limit),
        ("per_minute", Limit::per_minute),
        ("per_hour", Limit::per_hour),
    ] {
        let Some(value) = object.get(key) else {
            continue;
        };
        if limit.is_some() {
            return Err("a limit has one of per_second, per_minute and per_hour".into());
        }
        let n = count(key, value)?;
        if n == 0 {
            return Err(format!("{key} is 0, and a limit of none admits nothing"));
        }
        limit = Some(make(n));
    }
    let limit = limit.ok_or("a limit needs one of per_second, per_minute and per_hour")?;
    match object.get("burst") {
        None | Some(Value::Null) => Ok(limit),
        Some(burst) => Ok(limit.burst(count("burst", burst)?)),
    }
}

/// A limit as [`limit_from_json`] reads it: the inverse.
pub(crate) fn limit_to_json(limit: Limit) -> Value {
    let key = match limit.period().as_secs() {
        1 => "per_second",
        60 => "per_minute",
        _ => "per_hour",
    };
    json!({key: limit.count(), "burst": limit.burst_size()})
}

async fn set_classes(State(app): State<App>, body: Bytes) -> Response {
    let classes: HashMap<String, Value> = match serde_json::from_slice(&body) {
        Ok(classes) => classes,
        Err(error) => return bad_request(error),
    };
    let mut parsed = Vec::with_capacity(classes.len());
    for (name, value) in classes {
        match limit_from_json(&value) {
            Ok(limit) => parsed.push((name, limit)),
            Err(error) => return bad_request(format!("class {name}: {error}")),
        }
    }
    match app.guard.set_classes(parsed) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => bad_request(error),
    }
}

async fn set_limit(State(app): State<App>, body: Bytes) -> Response {
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => return bad_request(error),
    };
    match limit_from_json(&value).map(|limit| app.guard.set_limit(limit)) {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(error)) => bad_request(error),
        Err(error) => bad_request(error),
    }
}

async fn set_key_limits(State(app): State<App>, body: Bytes) -> Response {
    let entries: Vec<Value> = match serde_json::from_slice(&body) {
        Ok(entries) => entries,
        Err(error) => return bad_request(error),
    };
    let mut parsed = Vec::with_capacity(entries.len());
    for (at, entry) in entries.iter().enumerate() {
        match key_limit_from_json(entry) {
            Ok(parsed_entry) => parsed.push(parsed_entry),
            Err(error) => return bad_request(format!("entry {at}: {error}")),
        }
    }
    match app.guard.set_key_limits(parsed) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => bad_request(error),
    }
}

// One entry of `PUT /v1/key_limits`: its class, key and limit.
fn key_limit_from_json(entry: &Value) -> Result<(String, String, KeyLimit), String> {
    let object = entry.as_object().ok_or("an entry is an object")?;
    let text = |name: &str| -> Result<Option<String>, String> {
        match object.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(_) => Err(format!("{name} is not a string")),
        }
    };
    let key = text("key")?.ok_or("an entry needs a key")?;
    let class = text("class")?.unwrap_or_default();
    let limit = match (object.get("limit"), text("as_class")?) {
        (Some(limit), None) => KeyLimit::from(limit_from_json(limit)?),
        (None, Some(name)) => KeyLimit::class(name),
        _ => return Err("an entry has exactly one of limit and as_class".into()),
    };
    Ok((class, key, limit))
}

async fn set_shadows(State(app): State<App>, body: Bytes) -> Response {
    let shadows: HashMap<String, Value> = match serde_json::from_slice(&body) {
        Ok(shadows) => shadows,
        Err(error) => return bad_request(error),
    };
    let mut parsed = Vec::with_capacity(shadows.len());
    for (class, value) in shadows {
        // Replace-all: no shadow for a class is a class left out, or null.
        if value.is_null() {
            continue;
        }
        match limit_from_json(&value) {
            Ok(limit) => parsed.push((class, limit)),
            Err(error) => return bad_request(format!("class {class:?}: {error}")),
        }
    }
    match app.guard.set_shadows(parsed) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => bad_request(error),
    }
}

#[derive(Deserialize)]
struct KeyQuery {
    key: String,
    #[serde(default)]
    class: String,
}

async fn key_stats(
    State(app): State<App>,
    query: Result<Query<KeyQuery>, impl std::fmt::Display>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(error) => return bad_request(error),
    };
    match app.guard.key_stats(&query.class, &query.key).await {
        Some(s) => axum::Json(json!({
            "requests_per_sec": s.requests_per_sec,
            "denied_per_sec": s.denied_per_sec,
            "shadow_denied_per_sec": s.shadow_denied_per_sec,
            "share": s.share,
            "limit": limit_to_json(s.limit),
            "shadow": s.shadow.map(limit_to_json),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": "this node does not track the key"})),
        )
            .into_response(),
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
