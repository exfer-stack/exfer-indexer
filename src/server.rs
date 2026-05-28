//! HTTP server bootstrap.
//!
//! The HTTP layer itself (axum routes, auth middleware, batch
//! support, TLS) lands in the same commit as the RPC handlers
//! (commit #14). For the scaffold commit this just builds the
//! `Db` / `NodeClient` / `Follower`, spawns the follower task,
//! validates the bind address, and emits the canonical startup log.

use std::sync::Arc;

use crate::api::ApiState;
use crate::config::Config;
use crate::db::Db;
use crate::follower::Follower;
use crate::upstream::NodeClient;

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    tracing::info!(
        bind     = %cfg.bind,
        node_rpc = %cfg.node_rpc,
        datadir  = %cfg.datadir.display(),
        "exfer-indexer starting (scaffold; follower + handlers wire up in subsequent commits)"
    );

    let db = Arc::new(Db::open(&cfg.datadir).map_err(anyhow_from)?);
    let node = NodeClient::new(&cfg.node_rpc, cfg.upstream_timeout()).map_err(anyhow_from)?;
    let (follower, tip_rx) = Follower::new(db.clone(), node.clone(), cfg.clone());

    let _state = ApiState {
        db: db.clone(),
        node,
        tip_rx,
    };

    if cfg.no_follower {
        tracing::warn!("follower disabled by --no-follower; index will not advance");
    } else {
        let _ = follower.spawn();
    }

    // Real HTTP server lands in the RPC-handlers commit. Until then
    // the binary stays running so an operator can verify the boot
    // path (`exfer-indexer node` exits cleanly with ^C).
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    Ok(())
}

fn anyhow_from(e: crate::error::Error) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

#[allow(dead_code)]
fn _state_size_hint(_s: &ApiState) {}
