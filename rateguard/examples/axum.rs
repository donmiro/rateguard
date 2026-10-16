//! The README's axum setup, runnable:
//!
//!     cargo run -p rateguard --features tower --example axum
//!     curl -i -H 'x-tenant: 42' http://127.0.0.1:3000/
//!
//! One instance alone, at 5 requests a second per tenant: the sixth curl
//! within a second gets a 429 with Retry-After.

use axum::{Router, routing::get};
use rateguard::Guard;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let guard = Guard::builder()
        .bind("127.0.0.1:7946")
        .limit(5)
        .burst(5)
        .spawn()?;

    // README: begin
    use axum::extract::Request;
    use rateguard::tower::RateLimitLayer;

    // One limit per tenant. A request with no tenant is not limited:
    // return a key for it too, such as "api:anonymous", to limit it.
    let limit = RateLimitLayer::new(guard, |req: &Request| {
        let tenant = req.headers().get("x-tenant")?.to_str().ok()?;
        Some(format!("api:{tenant}"))
    });

    let app = Router::new()
        .route("/", get(|| async { "served\n" }))
        .layer(limit);
    // README: end

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
