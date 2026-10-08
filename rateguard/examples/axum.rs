//! The README's axum middleware, runnable:
//!
//!     cargo run -p rateguard --example axum
//!     curl -i -H 'x-tenant: 42' http://127.0.0.1:3000/
//!
//! One instance alone, at 5 requests a second per tenant: the sixth curl
//! within a second gets a 429 with Retry-After.

use axum::{Router, routing::get};

// README: begin
use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rateguard::{Decision, Guard};

async fn rate_limit(State(guard): State<Guard>, req: Request, next: Next) -> Response {
    let key = format!("api:{}", tenant_of(&req));

    match guard.check(&key) {
        Decision::Allow => next.run(req).await,
        Decision::Deny { retry_after } => (
            StatusCode::TOO_MANY_REQUESTS,
            // Whole seconds, rounded up: rounding down invites an early retry.
            [(
                header::RETRY_AFTER,
                retry_after.as_millis().div_ceil(1000).max(1).to_string(),
            )],
        )
            .into_response(),
    }
}
// README: end

fn tenant_of(req: &Request) -> &str {
    req.headers()
        .get("x-tenant")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("anonymous")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let guard = Guard::builder()
        .bind("127.0.0.1:7946")
        .limit(5)
        .burst(5)
        .spawn()?;
    let app = Router::new()
        .route("/", get(|| async { "served\n" }))
        .layer(axum::middleware::from_fn_with_state(guard, rate_limit));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
