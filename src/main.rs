//! exfer-indexer entry point.
//!
//! Parses the CLI, initializes tracing, and hands off to
//! [`exfer_indexer::server::run`] for the long-running follower +
//! HTTP server.

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("exfer_indexer=info".parse().unwrap()),
        )
        .with_target(false)
        .init();

    let cfg = exfer_indexer::config::Config::parse();
    exfer_indexer::server::run(cfg).await
}
