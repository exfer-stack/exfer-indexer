//! Command-line + env configuration for `exfer-indexer`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// Indexer configuration. Every field is overridable via flag and
/// (where it makes sense) via env var with the matching `EXFER_INDEXER_*`
/// name, mirroring exfer-walletd's pattern.
#[derive(Parser, Debug, Clone)]
#[command(name = "exfer-indexer", about, version)]
pub struct Config {
    /// Bind address for the JSON-RPC HTTP server. Use `0.0.0.0:9335`
    /// for a public-facing deployment behind a TLS terminator (or use
    /// `--tls` for in-process TLS).
    #[arg(long, env = "EXFER_INDEXER_BIND", default_value = "127.0.0.1:9335")]
    pub bind: SocketAddr,

    /// URL(s) of the upstream Exfer node JSON-RPC. Comma-separated for
    /// round-robin + failover.
    #[arg(
        long,
        env = "EXFER_INDEXER_NODE_RPC",
        default_value = "http://127.0.0.1:9334"
    )]
    pub node_rpc: String,

    /// Local data directory. The redb file lives at `<datadir>/index.redb`.
    #[arg(long, env = "EXFER_INDEXER_DATADIR", default_value = "data")]
    pub datadir: PathBuf,

    /// Optional bearer token. When set, every request must carry
    /// `Authorization: Bearer <token>`. Unset = open API. Note the indexer
    /// serves only public chain data, so a token is a coarse abuse gate, not
    /// confidentiality — a public read replica can safely run anonymous with
    /// `--allow-public-bind`.
    #[arg(long, env = "EXFER_INDEXER_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Follower poll interval (seconds). The follower checks for new
    /// blocks at the upstream node at this cadence; raise this for
    /// remote / metered upstreams.
    #[arg(long, env = "EXFER_INDEXER_POLL_SECS", default_value_t = 2)]
    pub poll_secs: u64,

    /// Upstream RPC request timeout (seconds).
    #[arg(
        long,
        env = "EXFER_INDEXER_UPSTREAM_TIMEOUT_SECS",
        default_value_t = 30
    )]
    pub upstream_timeout_secs: u64,

    /// Disable the follower task on startup. The HTTP server still
    /// runs and serves whatever's already in the index, but no new
    /// rows are added. Useful for read-only replicas pointed at a
    /// shared volume.
    #[arg(long, env = "EXFER_INDEXER_NO_FOLLOWER")]
    pub no_follower: bool,

    /// Acknowledge a plaintext public bind. Without `--tls`, the indexer
    /// refuses to listen on a non-loopback address unless this is set; with
    /// it, a public endpoint is allowed even with no `--auth-token` (anonymous
    /// open API over public chain data). Mirrors walletd's safety rule.
    #[arg(long, env = "EXFER_INDEXER_ALLOW_PUBLIC_BIND")]
    pub allow_public_bind: bool,

    /// Enable in-process TLS termination on the bind address.
    #[arg(long, env = "EXFER_INDEXER_TLS")]
    pub tls: bool,
}

impl Config {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_secs)
    }

    pub fn upstream_timeout(&self) -> Duration {
        Duration::from_secs(self.upstream_timeout_secs)
    }
}
