// The port that may face the pod's network, served on a real socket: a
// client that never finishes its request must not hold a connection open.

use std::time::Duration;

use rateguard::Guard;
use rateguard_sidecar::{App, public_app, serve_public};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn served(header_timeout: Duration) -> std::net::SocketAddr {
    let guard = Guard::builder()
        .bind("127.0.0.1:0")
        .limit(10)
        .spawn()
        .unwrap();
    let router = public_app(App::new(guard, false, None));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(serve_public(listener, router, header_timeout));
    address
}

// Read until the server closes, or give up after `within`.
async fn closed_within(stream: &mut TcpStream, within: Duration) -> bool {
    let mut buf = [0u8; 1024];
    tokio::time::timeout(within, async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn a_request_left_half_sent_is_cut_off() {
    let address = served(Duration::from_millis(200)).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    assert!(closed_within(&mut stream, Duration::from_secs(3)).await);
}

#[tokio::test]
async fn an_idle_connection_is_cut_off() {
    let address = served(Duration::from_millis(200)).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    assert!(closed_within(&mut stream, Duration::from_secs(3)).await);
}

#[tokio::test]
async fn a_whole_request_is_answered() {
    let address = served(Duration::from_millis(200)).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
}
