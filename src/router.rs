// src/router.rs
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc::Receiver, oneshot};
use tokio::time::{Duration, timeout};
use tracing::{debug, trace, warn};

#[derive(Clone, Default)]
pub struct Router {
    inner: Arc<Mutex<RouterState>>,
}

#[derive(Default)]
struct RouterState {
    waiters: HashMap<u64, oneshot::Sender<Value>>,
    pending: HashMap<u64, Value>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RouterState::default())),
        }
    }

    /// Backwards-compatible request with a default 5s timeout.
    pub async fn request(&self, id: u64) -> Result<Value, anyhow::Error> {
        self.request_with_timeout(id, Duration::from_secs(5)).await
    }

    /// Register a new request id and return a oneshot receiver.
    /// If the reply already arrived earlier, the receiver will be immediately satisfied.
    pub async fn register(&self, id: u64) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        let mut state = self.inner.lock().await;
        if let Some(msg) = state.pending.remove(&id) {
            let _ = tx.send(msg);
            trace!(
                target = "router",
                "register: immediate-delivery for id={}", id
            );
        } else {
            state.waiters.insert(id, tx);
            trace!(
                target = "router",
                "register: registered waiter for id={}", id
            );
        }
        rx
    }

    /// Deliver an incoming RPC reply. If no waiter exists, buffer the reply.
    /// Returns true if delivered to a waiter, false if buffered.
    pub async fn deliver(&self, id: u64, msg: Value) -> bool {
        let mut state = self.inner.lock().await;
        if let Some(tx) = state.waiters.remove(&id) {
            match tx.send(msg) {
                Ok(()) => {
                    trace!(target = "router", "deliver: delivered to waiter id={}", id);
                    true
                }
                Err(_) => {
                    warn!(target = "router", "deliver: waiter dropped for id={}", id);
                    true
                }
            }
        } else {
            state.pending.insert(id, msg);
            trace!(target = "router", "deliver: buffered reply for id={}", id);
            false
        }
    }

    /// Register + wait for reply with explicit timeout.
    pub async fn request_with_timeout(
        &self,
        id: u64,
        dur: Duration,
    ) -> Result<Value, anyhow::Error> {
        let rx = self.register(id).await;
        match timeout(dur, rx).await {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_)) => Err(anyhow::anyhow!("router: receiver dropped for id={}", id)),
            Err(_) => Err(anyhow::anyhow!("router: timeout waiting for id={}", id)),
        }
    }
}

/// Run the router loop: consume `rpc_rx` (parsed JSON messages) and deliver them.
pub async fn run_router(mut rpc_rx: Receiver<Value>, router: Router) {
    debug!(target = "router", "run_router: started");
    while let Some(msg) = rpc_rx.recv().await {
        trace!(target = "router", "run_router: got rpc message: {:?}", msg);
        if let Some(id) = msg.get("id").and_then(|v| v.as_u64()) {
            let delivered = router.deliver(id, msg).await;
            if !delivered {
                debug!(
                    target = "router",
                    "run_router: buffered reply for id={}", id
                );
            }
        } else {
            // Notification without id
            trace!(
                target = "router",
                "run_router: incoming notification (no id): {:?}", msg
            );
        }
    }
    debug!(target = "router", "run_router: rpc_rx closed; exiting");
}
