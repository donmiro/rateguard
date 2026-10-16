// The sidecar's HTTP API, driven through the router itself: no listening
// socket for the API, a real UDP one for the node.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use http_body_util::BodyExt;
use rateguard::{Guard, Limit};
use rateguard_sidecar::{App, app, public_app};
use serde_json::{Value, json};
use tower::ServiceExt;

// The default class at 40 a second with a burst of 3, "pro" with a burst
// of 5: low rates, so that nothing is earned back while a test runs.
fn guard() -> Guard {
    Guard::builder()
        .bind("127.0.0.1:0")
        .limit(40)
        .burst(3)
        .class("pro", Limit::per_second(10).burst(5))
        .spawn()
        .unwrap()
}

fn router(guard: &Guard, admin: bool) -> Router {
    app(App {
        guard: guard.clone(),
        admin,
        metrics: None,
    })
}

async fn send(router: &Router, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let request = axum::http::Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(match body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

async fn check(router: &Router, body: Value) -> Value {
    let (status, value) = send(router, "POST", "/v1/check", Some(body)).await;
    assert_eq!(status, 200, "{value}");
    value
}

#[tokio::test]
async fn a_check_is_allowed_then_denied_with_a_retry_after() {
    let guard = guard();
    let router = router(&guard, false);
    for _ in 0..3 {
        assert_eq!(
            check(&router, json!({"key": "k"})).await,
            json!({"allowed": true})
        );
    }
    let denied = check(&router, json!({"key": "k"})).await;
    assert_eq!(denied["allowed"], false);
    let retry = denied["retry_after_ms"].as_u64().unwrap();
    // A cold key alone gets α × 40 = 20 a second: one every 50 ms.
    assert!((1..=50).contains(&retry), "{denied}");
}

#[tokio::test]
async fn a_check_may_name_its_class() {
    let guard = guard();
    let router = router(&guard, false);
    let mut allowed = 0;
    for _ in 0..10 {
        if check(&router, json!({"key": "k", "class": "pro"})).await["allowed"] == true {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 5, "pro's burst");
}

#[tokio::test]
async fn a_malformed_check_is_a_bad_request() {
    let guard = guard();
    let router = router(&guard, false);
    assert_eq!(
        send(&router, "POST", "/v1/check", Some(json!({}))).await.0,
        400
    );
    assert_eq!(
        send(&router, "POST", "/v1/check", Some(json!({"key": 7})))
            .await
            .0,
        400
    );
}

#[tokio::test]
async fn stats_tell_what_was_decided() {
    let guard = guard();
    let router = router(&guard, false);
    for _ in 0..5 {
        check(&router, json!({"key": "k"})).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, stats) = send(&router, "GET", "/v1/stats", None).await;
        assert_eq!(status, 200);
        if stats["admitted"] == 3 && stats["denied"] == 2 {
            assert_eq!(stats["cluster_size"], 1);
            assert_eq!(stats["running"], true);
            let denied = &stats["most_denied"];
            assert!(denied.is_array(), "{stats}");
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{stats}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn health_turns_unavailable_once_the_node_has_left() {
    let guard = guard();
    let router = router(&guard, false);
    assert_eq!(send(&router, "GET", "/healthz", None).await.0, 200);
    guard.leave().await;
    assert_eq!(send(&router, "GET", "/healthz", None).await.0, 503);
}

#[tokio::test]
async fn admin_routes_are_not_there_unless_allowed() {
    let guard = guard();
    let router = router(&guard, false);
    let classes = json!({"team": {"per_second": 30}});
    assert_eq!(
        send(&router, "PUT", "/v1/classes", Some(classes)).await.0,
        404
    );
    assert_eq!(
        send(&router, "PUT", "/v1/limit", Some(json!({"per_second": 5})))
            .await
            .0,
        404
    );
    assert_eq!(
        send(&router, "PUT", "/v1/dry_run", Some(json!({"on": true})))
            .await
            .0,
        404
    );
}

#[tokio::test]
async fn classes_are_replaced_whole_through_the_admin_route() {
    let guard = guard();
    let router = router(&guard, true);
    let classes = json!({"team": {"per_second": 30, "burst": 2}, "free": {"per_second": 10}});
    assert_eq!(
        send(&router, "PUT", "/v1/classes", Some(classes)).await.0,
        204
    );
    let mut names: Vec<String> = guard.classes().into_iter().map(|(name, _)| name).collect();
    names.sort();
    assert_eq!(names, ["free", "team"], "pro is gone");
    let mut allowed = 0;
    for _ in 0..5 {
        if check(&router, json!({"key": "k", "class": "team"})).await["allowed"] == true {
            allowed += 1;
        }
    }
    assert_eq!(allowed, 2);
}

#[tokio::test]
async fn a_refused_change_says_why() {
    let guard = guard();
    let router = router(&guard, true);
    let (status, body) = send(
        &router,
        "PUT",
        "/v1/classes",
        Some(json!({"team": {"per_second": 0}})),
    )
    .await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("0"), "{body}");
    assert_eq!(guard.classes().len(), 1, "nothing changed");
}

#[tokio::test]
async fn the_default_limit_and_the_dry_run_change_through_the_admin_routes() {
    let guard = guard();
    let router = router(&guard, true);
    assert_eq!(
        send(
            &router,
            "PUT",
            "/v1/limit",
            Some(json!({"per_second": 40, "burst": 1}))
        )
        .await
        .0,
        204
    );
    assert_eq!(check(&router, json!({"key": "a"})).await["allowed"], true);
    assert_eq!(check(&router, json!({"key": "a"})).await["allowed"], false);

    assert_eq!(
        send(&router, "PUT", "/v1/dry_run", Some(json!({"on": true})))
            .await
            .0,
        204
    );
    assert_eq!(check(&router, json!({"key": "a"})).await["allowed"], true);
    assert!(guard.stats().dry_run);
}

#[tokio::test]
async fn metrics_are_not_there_without_an_exporter() {
    let guard = guard();
    let router = router(&guard, false);
    assert_eq!(send(&router, "GET", "/metrics", None).await.0, 404);
}

// What may face the pod's network, for kubelet's probes and Prometheus:
// health and metrics, nothing that checks or changes anything.
#[tokio::test]
async fn the_public_router_serves_health_only_without_an_exporter() {
    let guard = guard();
    let public = public_app(App {
        guard: guard.clone(),
        admin: true,
        metrics: None,
    });
    assert_eq!(send(&public, "GET", "/healthz", None).await.0, 200);
    assert_eq!(
        send(&public, "POST", "/v1/check", Some(json!({"key": "k"})))
            .await
            .0,
        404
    );
    assert_eq!(send(&public, "GET", "/v1/stats", None).await.0, 404);
    let classes = json!({"team": {"per_second": 30}});
    assert_eq!(
        send(&public, "PUT", "/v1/classes", Some(classes)).await.0,
        404,
        "admin or not"
    );
}

#[tokio::test]
async fn metrics_are_served_with_an_exporter_on_both_routers() {
    let guard = guard();
    // An exporter of its own, not installed: rendering is what is tested.
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();
    let served = App {
        guard: guard.clone(),
        admin: false,
        metrics: Some(handle),
    };
    for router in [app(served.clone()), public_app(served)] {
        assert_eq!(send(&router, "GET", "/metrics", None).await.0, 200);
    }
}
