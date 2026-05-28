//! HTTP server: axum + axum-server, JSON-RPC 2.0 over POST /.
//!
//! The HTTP layer is small on purpose — auth is a single bearer
//! token, dispatch routes through [`crate::api::dispatch`]. TLS,
//! batch requests, and the auth-token-vs-bind safety rule mirror
//! exfer-walletd's behaviour so operators can swap services without
//! re-learning the deployment story.

use std::net::IpAddr;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;

use crate::api::{dispatch, ApiState, RpcRequest};
use crate::config::Config;
use crate::db::Db;
use crate::error::Error;
use crate::follower::Follower;
use crate::upstream::NodeClient;

/// Wraps `ApiState` with the static auth-token bytes (if any).
#[derive(Clone)]
struct AppState {
    api: ApiState,
    token: Option<Arc<Vec<u8>>>,
}

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    // Bind safety: refuse 0.0.0.0 / public IPs without TLS or
    // --allow-public-bind. Loopback (127.x, ::1) is always fine.
    check_bind_safe(&cfg)?;

    tracing::info!(
        bind     = %cfg.bind,
        node_rpc = %cfg.node_rpc,
        datadir  = %cfg.datadir.display(),
        "exfer-indexer starting"
    );

    let db = Arc::new(Db::open(&cfg.datadir).map_err(anyhow_from)?);
    let node = NodeClient::new(&cfg.node_rpc, cfg.upstream_timeout()).map_err(anyhow_from)?;
    let (follower, tip_rx) = Follower::new(db.clone(), node.clone(), cfg.clone());

    if cfg.no_follower {
        tracing::warn!("follower disabled by --no-follower");
    } else {
        let _ = follower.spawn();
    }

    let api = ApiState {
        db: db.clone(),
        node,
        tip_rx,
    };
    let token = cfg.auth_token.as_ref().map(|s| Arc::new(s.as_bytes().to_vec()));
    let app_state = AppState { api, token };

    let app = Router::new()
        .route("/", post(handle_rpc))
        .route("/healthz", get(handle_healthz))
        .with_state(app_state);

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
        shutdown_signal.cancel();
    });

    tracing::info!("listening on http://{}", cfg.bind);
    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.cancelled().await;
    })
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// /healthz — unauthenticated liveness probe
// ---------------------------------------------------------------------------

async fn handle_healthz(State(state): State<AppState>) -> Response {
    // Read meta + tip to confirm the indexer can serve queries.
    let db = state.api.db.clone();
    let meta = tokio::task::spawn_blocking(move || db.load_meta()).await;
    match meta {
        Ok(Ok(m)) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "last_indexed_height": m.last_indexed_height,
                "full_scan_complete": m.full_scan_complete,
            })),
        )
            .into_response(),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false })),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST / — JSON-RPC entry
// ---------------------------------------------------------------------------

async fn handle_rpc(
    State(state): State<AppState>,
    ConnectInfo(_peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // ---- auth ----
    if let Some(expected) = state.token.as_ref() {
        let supplied = extract_bearer(&headers);
        if !ct_eq(supplied.as_bytes(), expected.as_slice()) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(rpc_error(None, &Error::Unauthorized)),
            )
                .into_response();
        }
    }

    // ---- body decode ----
    let v: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(rpc_error(None, &Error::ParseError(e.to_string()))),
            )
                .into_response();
        }
    };

    // ---- batch vs single ----
    if let Value::Array(items) = v {
        if items.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(rpc_error(
                    None,
                    &Error::InvalidRequest("empty batch".into()),
                )),
            )
                .into_response();
        }
        let mut results: Vec<Value> = Vec::with_capacity(items.len());
        for item in items {
            let r = handle_single(&state, item).await;
            results.push(r);
        }
        return (StatusCode::OK, Json(Value::Array(results))).into_response();
    }
    let result = handle_single(&state, v).await;
    (StatusCode::OK, Json(result)).into_response()
}

async fn handle_single(state: &AppState, body: Value) -> Value {
    let req: RpcRequest = match serde_json::from_value(body) {
        Ok(r) => r,
        Err(e) => return rpc_error(None, &Error::InvalidRequest(e.to_string())),
    };
    if req.jsonrpc != "2.0" {
        return rpc_error(req.id, &Error::InvalidRequest("jsonrpc must be \"2.0\"".into()));
    }
    let id = req.id.clone();
    match dispatch(&state.api, req).await {
        Ok(v) => json!({ "jsonrpc": "2.0", "result": v, "id": id }),
        Err(e) => rpc_error(id, &e),
    }
}

fn rpc_error(id: Option<Value>, err: &Error) -> Value {
    let mut body = json!({
        "jsonrpc": "2.0",
        "error": {
            "code": err.rpc_code(),
            "message": err.to_string(),
        },
        "id": id,
    });
    if let Some(data) = err.rpc_data() {
        body["error"]["data"] = data;
    }
    body
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

fn extract_bearer(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("")
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

// ---------------------------------------------------------------------------
// Bind safety
// ---------------------------------------------------------------------------

fn check_bind_safe(cfg: &Config) -> anyhow::Result<()> {
    let ip = cfg.bind.ip();
    if ip.is_loopback() {
        return Ok(());
    }
    if is_private(&ip) {
        if cfg.auth_token.is_none() {
            tracing::warn!(
                "binding to {} without --auth-token; any client on the LAN can query",
                cfg.bind
            );
        }
        return Ok(());
    }
    // Public bind.
    if cfg.auth_token.is_none() && !cfg.tls {
        anyhow::bail!(
            "refusing to bind {} publicly without --tls or --auth-token; \
             set EXFER_INDEXER_AUTH_TOKEN or use a TLS terminator + \
             --allow-public-bind to acknowledge",
            cfg.bind
        );
    }
    if !cfg.allow_public_bind && !cfg.tls {
        anyhow::bail!(
            "refusing to bind {} on a public interface without --tls; \
             pass --allow-public-bind if you have a TLS terminator in front",
            cfg.bind
        );
    }
    Ok(())
}

fn is_private(ip: &IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_link_local(),
        IpAddr::V6(_v6) => false, // conservative: treat all v6 non-loopback as public
    }
}

fn anyhow_from(e: Error) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

// ---------------------------------------------------------------------------
// Test-only state builder
// ---------------------------------------------------------------------------

/// Construct an `AppState` for integration tests that drive the
/// HTTP layer through a `TestServer` or similar. Not part of the
/// public API.
#[doc(hidden)]
pub fn build_test_app(api: ApiState, token: Option<&str>) -> Router {
    let app_state = AppState {
        api,
        token: token.map(|s| Arc::new(s.as_bytes().to_vec())),
    };
    Router::new()
        .route("/", post(handle_rpc))
        .route("/healthz", get(handle_healthz))
        .with_state(app_state)
}
