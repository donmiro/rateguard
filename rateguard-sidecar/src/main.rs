//! `rateguard-sidecar`: see `--help`, and the crate's README.

use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use rateguard_sidecar::args::Args;
use rateguard_sidecar::{App, app, public_app, serve_public};

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("rateguard-sidecar: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Installed before the node starts, so that its first tick is counted.
    let metrics = PrometheusBuilder::new().install_recorder()?;
    let guard = args.builder().spawn()?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!(
        "rateguard-sidecar: listening on http://{}, gossip on {}",
        listener.local_addr()?,
        args.bind
    );
    let served = App {
        guard: guard.clone(),
        admin: args.admin,
        metrics: Some(metrics),
    };
    if let Some(address) = args.metrics_listen {
        let public = tokio::net::TcpListener::bind(address).await?;
        eprintln!(
            "rateguard-sidecar: health and metrics on http://{}",
            public.local_addr()?
        );
        // Stops with the process: nothing there to drain.
        tokio::spawn(serve_public(
            public,
            public_app(served.clone()),
            Duration::from_secs(10),
        ));
    }
    let drain = args.drain.0;
    axum::serve(listener, app(served))
        .with_graceful_shutdown(async move {
            stopping().await;
            // The peers take this instance's share at once, rather than
            // suspect it first and then hold its share for a split that is
            // not one.
            guard.leave().await;
            // The service next to it got the signal too and drains its
            // requests: they are still checked, at the floor, rather than
            // let through by a client that finds nobody to ask. A second
            // signal cuts it short.
            eprintln!("rateguard-sidecar: left the cluster, answering at the floor for {drain:?}");
            tokio::select! {
                _ = tokio::time::sleep(drain) => {}
                _ = stopping() => {}
            }
            eprintln!("rateguard-sidecar: stopping");
        })
        .await?;
    Ok(())
}

// SIGTERM, as Kubernetes sends before it stops a pod, or Ctrl-C.
async fn stopping() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("a signal handler for SIGTERM");
        tokio::select! {
            _ = interrupt => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = interrupt.await;
}
