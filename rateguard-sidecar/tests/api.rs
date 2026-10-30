// The sidecar's HTTP API, driven through the router itself: no listening
// socket for the API, a real UDP one for the node.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use http_body_util::BodyExt;
use rateguard::{GossipAuth, Guard, Limit, Secret};
use rateguard_sidecar::{App, MAX_BODY, app, public_app};
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
    app(App::new(guard.clone(), admin, None))
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

// What Python's json.dumps and JavaScript's JSON.stringify send for a
// string with a lone surrogate. A 400 there would have the clients fail
// open: anyone choosing the key would go unlimited.
#[tokio::test]
async fn a_key_with_a_lone_surrogate_is_limited_like_any_other() {
    let guard = guard();
    let router = router(&guard, false);
    let mut decisions = Vec::new();
    for _ in 0..4 {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/check")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"key": "evil\ud800"}"#))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        decisions.push(value["allowed"].as_bool().unwrap());
    }
    assert_eq!(decisions, [true, true, true, false]);
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

// Polls `/v1/stats` until `most_denied` has an entry for `hash`.
async fn denied_entry(router: &Router, hash: u64) -> Value {
    let hash = hash.to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (_, stats) = send(router, "GET", "/v1/stats", None).await;
        let found = stats["most_denied"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["key_hash"] == hash.as_str())
            .cloned();
        if let Some(entry) = found {
            return entry;
        }
        assert!(tokio::time::Instant::now() < deadline, "{stats}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// Clients in other languages cannot compute a key's hash, and with a key
// salt nobody outside the node can: the sidecar names the key.
#[tokio::test]
async fn most_denied_names_the_key_and_its_class() {
    let guard = guard();
    let router = router(&guard, false);
    for _ in 0..8 {
        check(&router, json!({"key": "tenant:42", "class": "pro"})).await;
    }
    let entry = denied_entry(&router, guard.key_hash_in("pro", "tenant:42")).await;
    assert_eq!(entry["key"], "tenant:42", "{entry}");
    assert_eq!(entry["class"], "pro", "{entry}");
    assert!(entry.get("key_truncated").is_none(), "{entry}");
}

#[tokio::test]
async fn the_default_class_is_named_empty_and_a_long_key_is_cut() {
    let guard = guard();
    let router = router(&guard, false);
    let key = "k".repeat(300);
    for _ in 0..5 {
        check(&router, json!({"key": key})).await;
    }
    let entry = denied_entry(&router, guard.key_hash_in("", &key)).await;
    assert_eq!(entry["class"], "", "{entry}");
    assert_eq!(entry["key"], "k".repeat(256), "{entry}");
    assert_eq!(entry["key_truncated"], true, "{entry}");
}

#[tokio::test]
async fn an_allowed_key_is_never_recorded() {
    let guard = guard();
    let served = App::new(guard.clone(), false, None);
    let router = app(served.clone());
    assert_eq!(
        check(&router, json!({"key": "quiet"})).await["allowed"],
        true
    );
    for _ in 0..5 {
        check(&router, json!({"key": "noisy"})).await;
    }
    denied_entry(&router, guard.key_hash_in("", "noisy")).await;
    let denied = served.denied.lock().unwrap();
    assert_eq!(denied.len(), 1, "noisy only");
    assert!(denied.get(guard.key_hash_in("", "quiet")).is_none());
}

// A key denied before a restart, or pushed out of the table, keeps its
// hash and nothing else.
#[tokio::test]
async fn an_unknown_hash_has_no_key_or_class() {
    let guard = guard();
    let router = router(&guard, false);
    for _ in 0..5 {
        let _ = guard.check("unseen");
    }
    let entry = denied_entry(&router, guard.key_hash_in("", "unseen")).await;
    assert!(entry.get("key").is_none(), "{entry}");
    assert!(entry.get("class").is_none(), "{entry}");
    assert!(entry.get("key_truncated").is_none(), "{entry}");
}

#[tokio::test]
async fn stats_tell_the_gossip_auth_and_the_rejected_datagrams() {
    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let guard = Guard::builder()
        .bind(format!("127.0.0.1:{port}"))
        .limit(40)
        .spawn()
        .unwrap();
    let router = router(&guard, false);
    let (_, stats) = send(&router, "GET", "/v1/stats", None).await;
    assert_eq!(stats["gossip_auth"], "off", "{stats}");
    assert_eq!(stats["identity_conflicts"], 0, "{stats}");
    assert_eq!(stats["members_refused"], 0, "{stats}");
    assert_eq!(stats["members_full"], false, "{stats}");
    assert_eq!(
        stats["rejected_datagrams"],
        json!({"unsigned": 0, "bad_tag": 0, "malformed": 0, "mismatched_records": 0})
    );
    // A datagram that is no message, counted in every mode.
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.send_to(&[0xff; 40], ("127.0.0.1", port)).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (_, stats) = send(&router, "GET", "/v1/stats", None).await;
        if stats["rejected_datagrams"]["malformed"] == 1 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{stats}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let signing = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(40)
        .gossip_auth(
            GossipAuth::Transitional,
            vec![Secret::new([0xA1; 32].to_vec()).unwrap()],
        )
        .spawn()
        .unwrap();
    let signing = app(App::new(signing, false, None));
    let (_, stats) = send(&signing, "GET", "/v1/stats", None).await;
    assert_eq!(stats["gossip_auth"], "transitional", "{stats}");
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
    let unchanged = [("pro".to_owned(), Limit::per_second(10).burst(5))];
    assert_eq!(guard.classes(), unchanged, "nothing changed");
    // All or nothing: one bad class among good ones changes none of them.
    let (status, body) = send(
        &router,
        "PUT",
        "/v1/classes",
        Some(json!({"team": {"per_second": 30}, "free": {"per_hour": 0}})),
    )
    .await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("free"), "{body}");
    assert_eq!(guard.classes(), unchanged, "nothing changed");
}

#[tokio::test]
async fn a_refused_default_limit_says_why_and_keeps_the_old_one() {
    let guard = guard();
    let router = router(&guard, true);
    for bad in [
        json!({"per_second": 0}),
        json!({"per_second": 1, "per_minute": 60}),
        json!({"burst": 5}),
        json!(5),
    ] {
        let (status, body) = send(&router, "PUT", "/v1/limit", Some(bad.clone())).await;
        assert_eq!(status, 400, "{bad}");
        assert!(body["error"].is_string(), "{body}");
    }
    // Still 40 a second with a burst of 3.
    for _ in 0..3 {
        assert_eq!(check(&router, json!({"key": "a"})).await["allowed"], true);
    }
    assert_eq!(check(&router, json!({"key": "a"})).await["allowed"], false);
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
    let public = public_app(App::new(guard.clone(), true, None));
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
    let served = App::new(guard.clone(), false, Some(handle));
    for router in [app(served.clone()), public_app(served)] {
        assert_eq!(send(&router, "GET", "/metrics", None).await.0, 200);
    }
}

#[tokio::test]
async fn limits_take_a_period() {
    let guard = guard();
    let router = router(&guard, true);
    let put = |body: Value| send(&router, "PUT", "/v1/classes", Some(body));
    assert_eq!(put(json!({"m": {"per_minute": 600}})).await.0, 204);
    assert_eq!(guard.classes(), [("m".to_owned(), Limit::per_minute(600))]);
    assert_eq!(send(&router, "GET", "/v1/stats", None).await.0, 200);
    assert_eq!(put(json!({"h": {"per_hour": 5, "burst": 2}})).await.0, 204);
    for bad in [
        json!({"m": {"per_second": 1, "per_minute": 60}}),
        json!({"m": {}}),
        json!({"m": {"per_hour": 0}}),
        json!({"m": {"burst": 5}}),
        json!({"m": {"per_minute": -1}}),
        json!({"m": 5}),
    ] {
        let (status, body) = put(bad.clone()).await;
        assert_eq!(status, 400, "{bad}");
        assert!(body["error"].is_string(), "{body}");
    }
    assert_eq!(
        put(json!({"pro": {"per_second": 1000, "burst": 50}}))
            .await
            .0,
        204
    );
    let (status, _) = send(&router, "PUT", "/v1/limit", Some(json!({"per_hour": 100}))).await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn key_limits_route_replaces_all() {
    let guard = guard();
    let router = router(&guard, true);
    let body = json!([
        {"class": "pro", "key": "tenant:42", "limit": {"per_minute": 600, "burst": 20}},
        {"class": "free", "key": "tenant:7", "as_class": "pro"},
        {"key": "tenant:1", "limit": {"per_second": 9}},
    ]);
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(body)).await.0,
        204
    );
    assert_eq!(guard.key_limits().len(), 3);
    // Replaces all, and all or nothing: a bad entry leaves the table as it was.
    let replace = json!([{"class": "pro", "key": "only", "limit": {"per_second": 3}}]);
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(replace))
            .await
            .0,
        204
    );
    let keys: Vec<_> = guard.key_limits().into_iter().map(|(_, k, _)| k).collect();
    assert_eq!(keys, ["only"]);
    let bad = json!([{"key": "x", "limit": {"per_second": 0}}]);
    let (status, body) = send(&router, "PUT", "/v1/key_limits", Some(bad)).await;
    assert_eq!(status, 400);
    assert!(body["error"].is_string(), "{body}");
    assert_eq!(guard.key_limits().len(), 1);
    // An empty list clears.
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(json!([])))
            .await
            .0,
        204
    );
    assert!(guard.key_limits().is_empty());
}

#[tokio::test]
async fn key_limits_route_refuses_both_limit_and_as_class() {
    let guard = guard();
    let router = router(&guard, true);
    for bad in [
        json!([{"key": "k", "limit": {"per_second": 1}, "as_class": "pro"}]),
        json!([{"key": "k"}]),
        json!({"key": "k"}),
    ] {
        let (status, body) = send(&router, "PUT", "/v1/key_limits", Some(bad.clone())).await;
        assert_eq!(status, 400, "{bad}");
        assert!(body["error"].is_string(), "{body}");
    }
    assert!(guard.key_limits().is_empty());
}

#[tokio::test]
async fn a_null_limit_in_a_key_limit_is_no_limit() {
    let guard = guard();
    let router = router(&guard, true);
    // As with as_class, class and burst: null is the field left out.
    let body = json!([{"key": "k", "limit": null, "as_class": "pro"}]);
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(body)).await.0,
        204
    );
    assert_eq!(guard.key_limits().len(), 1);
    let bad = json!([{"key": "k", "limit": null, "as_class": null}]);
    let (status, body) = send(&router, "PUT", "/v1/key_limits", Some(bad)).await;
    assert_eq!(status, 400);
    assert!(body["error"].is_string(), "{body}");
}

#[tokio::test]
async fn shadows_route_sets_and_clears() {
    let guard = guard();
    let router = router(&guard, true);
    let body = json!({"pro": {"per_second": 10}, "": {"per_minute": 60}});
    assert_eq!(send(&router, "PUT", "/v1/shadows", Some(body)).await.0, 204);
    let mut shadows = guard.shadows();
    shadows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        shadows,
        [
            (String::new(), Limit::per_minute(60)),
            ("pro".to_owned(), Limit::per_second(10)),
        ]
    );
    // A null, or a class left out, is no shadow for it.
    let body = json!({"pro": {"per_second": 5}, "": null});
    assert_eq!(send(&router, "PUT", "/v1/shadows", Some(body)).await.0, 204);
    assert_eq!(guard.shadows(), [("pro".to_owned(), Limit::per_second(5))]);
    assert_eq!(
        send(&router, "PUT", "/v1/shadows", Some(json!({}))).await.0,
        204
    );
    assert!(guard.shadows().is_empty());
    // A class that does not exist, and a limit that is not one.
    let (status, body) = send(
        &router,
        "PUT",
        "/v1/shadows",
        Some(json!({"nope": {"per_second": 5}})),
    )
    .await;
    assert_eq!(status, 400);
    assert!(body["error"].is_string(), "{body}");
    let bad = json!({"pro": {"per_second": 5, "per_hour": 1}});
    assert_eq!(send(&router, "PUT", "/v1/shadows", Some(bad)).await.0, 400);
}

// Checks `key` of "pro", then polls `/v1/keys` until the node tracks it.
async fn tracked(router: &Router, key: &str) -> Value {
    check(router, json!({"key": key, "class": "pro"})).await;
    let uri = format!("/v1/keys?key={key}&class=pro");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = send(router, "GET", &uri, None).await;
        if status == 200 {
            return body;
        }
        assert_eq!(status, 404, "{body}");
        assert!(tokio::time::Instant::now() < deadline, "never tracked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn keys_route_names_a_tracked_key() {
    let guard = guard();
    let router = router(&guard, true);
    let put = json!([
        {"class": "pro", "key": "own", "limit": {"per_minute": 600, "burst": 20}},
        {"class": "pro", "key": "follows", "as_class": "pro"},
    ]);
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(put)).await.0,
        204
    );
    let shadows = json!({"pro": {"per_second": 7}});
    assert_eq!(
        send(&router, "PUT", "/v1/shadows", Some(shadows)).await.0,
        204
    );
    let body = tracked(&router, "own").await;
    assert_eq!(
        body["limit"],
        json!({"per_minute": 600, "burst": 20}),
        "{body}"
    );
    // A limit of its own keeps a key out of the class's shadow.
    assert_eq!(body["shadow"], Value::Null, "{body}");
    for field in [
        "requests_per_sec",
        "denied_per_sec",
        "shadow_denied_per_sec",
        "share",
    ] {
        assert!(body[field].is_f64(), "{field}: {body}");
    }
    let body = tracked(&router, "follows").await;
    assert_eq!(
        body["limit"],
        json!({"per_second": 10, "burst": 5}),
        "{body}"
    );
    assert_eq!(body["shadow"]["per_second"], 7, "{body}");
}

#[tokio::test]
async fn keys_route_takes_a_percent_encoded_key() {
    let guard = guard();
    let router = router(&guard, true);
    let key = "tenant 42/a&b=c+d";
    let put = json!([{"class": "pro", "key": key, "limit": {"per_minute": 600}}]);
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(put)).await.0,
        204
    );
    check(&router, json!({"key": key, "class": "pro"})).await;
    let uri = "/v1/keys?key=tenant%2042%2Fa%26b%3Dc%2Bd&class=pro";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let body = loop {
        let (status, body) = send(&router, "GET", uri, None).await;
        if status == 200 {
            break body;
        }
        assert_eq!(status, 404, "{body}");
        assert!(tokio::time::Instant::now() < deadline, "never tracked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // The key's own limit: the decoded key is the key that was set.
    assert_eq!(body["limit"]["per_minute"], 600, "{body}");
}

#[tokio::test]
async fn keys_route_404s_an_untracked_key() {
    let guard = guard();
    let router = router(&guard, false);
    let (status, body) = send(&router, "GET", "/v1/keys?key=nobody", None).await;
    assert_eq!(status, 404);
    assert!(body["error"].is_string(), "{body}");
    // No key at all is a bad request.
    assert_eq!(send(&router, "GET", "/v1/keys", None).await.0, 400);
}

#[tokio::test]
async fn keys_route_is_not_on_the_metrics_listener() {
    let guard = guard();
    let public = public_app(App::new(guard.clone(), true, None));
    assert_eq!(send(&public, "GET", "/v1/keys?key=k", None).await.0, 404);
    assert_eq!(
        send(&public, "PUT", "/v1/key_limits", Some(json!([])))
            .await
            .0,
        404
    );
    assert_eq!(
        send(&public, "PUT", "/v1/shadows", Some(json!({}))).await.0,
        404
    );
}

#[tokio::test]
async fn admin_routes_are_absent_without_admin() {
    let guard = guard();
    let router = router(&guard, false);
    assert_eq!(
        send(&router, "PUT", "/v1/key_limits", Some(json!([])))
            .await
            .0,
        404
    );
    assert_eq!(
        send(&router, "PUT", "/v1/shadows", Some(json!({}))).await.0,
        404
    );
}

#[tokio::test]
async fn stats_names_the_shadow_denied_keys() {
    let guard = guard();
    let router = router(&guard, true);
    // The default class's shadow, tighter than its limit's burst of 3.
    let body = json!({"": {"per_second": 1}});
    assert_eq!(send(&router, "PUT", "/v1/shadows", Some(body)).await.0, 204);
    for _ in 0..10 {
        check(&router, json!({"key": "tenant:9"})).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (_, stats) = send(&router, "GET", "/v1/stats", None).await;
        if let Some(entry) = stats["most_shadow_denied"].as_array().unwrap().first() {
            assert_eq!(entry["key"], "tenant:9", "{stats}");
            assert_eq!(entry["class"], "", "{stats}");
            assert!(
                entry["shadow_denied_per_sec"].as_f64().unwrap() > 0.0,
                "{stats}"
            );
            assert!(entry["requests_per_sec"].is_f64(), "{stats}");
            assert!(stats["shadow_denied"].as_u64().unwrap() > 0, "{stats}");
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{stats}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// The class is a field of the same body as the key, and Python and
// JavaScript send a lone surrogate in it as readily. A 400 would have their
// clients fail open.
#[tokio::test]
async fn a_class_with_a_lone_surrogate_is_checked() {
    let guard = guard();
    let router = router(&guard, false);
    let mut decisions = Vec::new();
    for _ in 0..4 {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/check")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"key":"k","class":"pro\ud800"}"#))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        decisions.push(value["allowed"].as_bool().unwrap());
    }
    // An unknown class is the default class, whose burst is 3.
    assert_eq!(decisions, [true, true, true, false]);
}

#[tokio::test]
async fn a_body_of_the_limit_is_checked_and_one_byte_more_is_413() {
    let guard = guard();
    let router = router(&guard, false);
    let body_of = |len: usize| {
        let frame = r#"{"key":""}"#.len();
        format!(r#"{{"key":"{}"}}"#, "a".repeat(len - frame))
    };
    let post = |body: String| {
        axum::http::Request::builder()
            .method("POST")
            .uri("/v1/check")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    };
    let at = body_of(MAX_BODY);
    assert_eq!(at.len(), MAX_BODY);
    let response = router.clone().oneshot(post(at)).await.unwrap();
    assert_eq!(response.status().as_u16(), 200);

    let response = router
        .clone()
        .oneshot(post(body_of(MAX_BODY + 1)))
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 413);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value, json!({"error": "the body is too large"}));
}

// The body limit is for checks: a bulk replace of key limits is far larger.
#[tokio::test]
async fn a_bulk_key_limits_body_over_the_check_limit_is_accepted() {
    let guard = guard();
    let router = router(&guard, true);
    let entries: Vec<Value> = (0..1500)
        .map(|i| json!({"key": format!("tenant:{i}"), "limit": {"per_second": 50}}))
        .collect();
    let body = Value::Array(entries);
    assert!(body.to_string().len() > MAX_BODY);
    let (status, value) = send(&router, "PUT", "/v1/key_limits", Some(body)).await;
    assert_eq!(status, 204, "{value}");
    assert_eq!(guard.key_limits().len(), 1500);
}
