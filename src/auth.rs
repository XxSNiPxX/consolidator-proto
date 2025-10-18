// src/auth.rs
use crate::id::next_id;
use crate::router::Router;
use anyhow::Result;
use futures::SinkExt;
use futures::stream::SplitSink;
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{info, warn};

pub type SharedToken = std::sync::Arc<TokioMutex<Option<String>>>;

/// Authenticate over `write` using client credentials and store the access token in `token`.
/// Uses the provided `router` to await the RPC reply with a timeout.
pub async fn authenticate_ws(
    write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    client_id: &str,
    client_secret: &str,
    token: SharedToken,
    router: Router,
) -> Result<()> {
    let id = next_id();
    let login = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    });

    write.send(Message::Text(login.to_string())).await?;
    info!(
        target = "auth",
        "authenticate_ws: sent login request id={}", id
    );

    // wait via router with 5s timeout
    let reply = router
        .request_with_timeout(id, Duration::from_secs(5))
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    if let Some(err) = reply.get("error") {
        warn!(
            target = "auth",
            "authenticate_ws: auth error reply: {:?}", err
        );
        return Err(anyhow::anyhow!("auth error: {:?}", err));
    }

    if let Some(result) = reply.get("result") {
        if let Some(access) = result.get("access_token").and_then(|t| t.as_str()) {
            *token.lock().await = Some(access.to_string());
            info!(
                target = "auth",
                "authenticate_ws: got access token id={}", id
            );
            return Ok(());
        }
    }

    Err(anyhow::anyhow!("authenticate_ws: auth reply missing token"))
}
