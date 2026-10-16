// The `tower` feature: RateLimitLayer in front of a service.

use std::convert::Infallible;

use http::{Request, Response, StatusCode, header};
use rateguard::Guard;
use rateguard::tower::RateLimitLayer;
use tower::{Layer, Service, ServiceExt, service_fn};

fn guard(limit: u32, burst: u32) -> Guard {
    Guard::builder()
        .bind("127.0.0.1:0")
        .limit(limit)
        .burst(burst)
        .spawn()
        .unwrap()
}

fn tenant(req: &Request<String>) -> Option<String> {
    let tenant = req.headers().get("x-tenant")?.to_str().ok()?;
    Some(format!("api:{tenant}"))
}

fn request(tenant: Option<&str>) -> Request<String> {
    let mut req = Request::builder();
    if let Some(tenant) = tenant {
        req = req.header("x-tenant", tenant);
    }
    req.body(String::new()).unwrap()
}

type Svc = Box<
    dyn tower::Service<
            Request<String>,
            Response = Response<String>,
            Error = Infallible,
            Future = std::pin::Pin<
                Box<dyn Future<Output = Result<Response<String>, Infallible>> + Send>,
            >,
        > + Send,
>;

fn service(guard: Guard) -> Svc {
    let inner = service_fn(|_: Request<String>| async {
        Ok::<_, Infallible>(Response::new("served".to_owned()))
    });
    Box::new(
        RateLimitLayer::new(guard, tenant)
            .layer(inner)
            .map_future(|f| Box::pin(f) as std::pin::Pin<Box<_>>),
    )
}

async fn send(svc: &mut Svc, tenant: Option<&str>) -> Response<String> {
    svc.ready()
        .await
        .unwrap()
        .call(request(tenant))
        .await
        .unwrap()
}

#[tokio::test]
async fn requests_over_the_limit_get_429_with_retry_after() {
    let mut svc = service(guard(100, 3));
    for _ in 0..3 {
        let res = send(&mut svc, Some("42")).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.body(), "served");
    }
    let res = send(&mut svc, Some("42")).await;
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    // A wait of under a second, rounded up: rounding down invites an
    // early retry.
    assert_eq!(res.headers()[header::RETRY_AFTER], "1");
    assert_eq!(res.body(), "", "the body's default");
}

#[tokio::test]
async fn each_key_has_its_own_limit() {
    let mut svc = service(guard(100, 1));
    assert_eq!(send(&mut svc, Some("1")).await.status(), StatusCode::OK);
    assert_eq!(send(&mut svc, Some("2")).await.status(), StatusCode::OK);
    assert_eq!(
        send(&mut svc, Some("1")).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn a_request_with_no_key_is_not_limited() {
    let mut svc = service(guard(100, 1));
    for _ in 0..10 {
        assert_eq!(send(&mut svc, None).await.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn a_dry_run_lets_everything_through() {
    let guard = guard(100, 1);
    guard.set_dry_run(true);
    let mut svc = service(guard);
    for _ in 0..5 {
        assert_eq!(send(&mut svc, Some("42")).await.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn the_layer_works_in_axum() {
    let app = axum::Router::new()
        .route("/", axum::routing::get(|| async { "served" }))
        .layer(RateLimitLayer::new(guard(100, 1), |req: &Request<_>| {
            Some(req.uri().path().to_owned())
        }));
    let get = || {
        Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let first = app.clone().oneshot(get()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = app.oneshot(get()).await.unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_key_function_may_name_the_class_too() {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(100)
        .burst(5)
        .class("pro", rateguard::Limit::per_second(100).burst(2))
        .spawn()
        .unwrap();
    let inner = service_fn(|_: Request<String>| async {
        Ok::<_, Infallible>(Response::new("served".to_owned()))
    });
    let mut svc = RateLimitLayer::new(guard, |req: &Request<String>| {
        let plan = req.headers().get("x-plan")?.to_str().ok()?;
        Some((plan.to_owned(), "tenant:42"))
    })
    .layer(inner);
    let pro = || {
        Request::builder()
            .header("x-plan", "pro")
            .body(String::new())
            .unwrap()
    };
    let mut statuses = Vec::new();
    for _ in 0..3 {
        let res = svc.ready().await.unwrap().call(pro()).await.unwrap();
        statuses.push(res.status());
    }
    assert_eq!(
        statuses,
        [
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::TOO_MANY_REQUESTS
        ],
        "pro's burst of 2"
    );
}
