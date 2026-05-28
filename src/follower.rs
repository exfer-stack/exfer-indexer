//! Block follower — populates the indexer's redb store.
//!
//! Detailed implementation arrives in commit #13. The scaffold keeps
//! the public surface (`Follower::new`, `Follower::run`,
//! `Follower::tick`) so the server boot path can wire it up.

use std::sync::Arc;

use tokio::sync::watch;

use crate::config::Config;
use crate::db::Db;
use crate::error::Result;
use crate::upstream::NodeClient;

pub struct Follower {
    #[allow(dead_code)]
    db: Arc<Db>,
    #[allow(dead_code)]
    node: NodeClient,
    #[allow(dead_code)]
    tip_tx: watch::Sender<u64>,
    #[allow(dead_code)]
    cfg: Config,
}

impl Follower {
    pub fn new(db: Arc<Db>, node: NodeClient, cfg: Config) -> (Arc<Self>, watch::Receiver<u64>) {
        let (tip_tx, tip_rx) = watch::channel(0u64);
        (
            Arc::new(Self {
                db,
                node,
                tip_tx,
                cfg,
            }),
            tip_rx,
        )
    }

    /// Spawn the follower as a long-running tokio task. Stub — real
    /// loop lands in the follower commit.
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // TODO(#13): poll → walk → process → save_meta loop.
            tracing::warn!(
                "follower: scaffold stub — no blocks will be indexed until the follower commit lands"
            );
        })
    }

    /// One iteration of the run loop. Stub.
    pub async fn tick(self: &Arc<Self>) -> Result<()> {
        Ok(())
    }
}
