use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMsg};
use tracing::{debug, error, info, warn};

use crate::auth::authenticate_ws;
use crate::id::next_id;
use crate::router::Router;
use crate::types::{AssetKind, OrderBookSnapshot, TradePrint, TradeSide};

pub type SharedToken = std::sync::Arc<tokio::sync::Mutex<Option<String>>>;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Clone)]
pub struct DeribitCfg {
    pub ws_url: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub books: Vec<String>,
    pub trades: Vec<String>,
    pub private: Option<bool>,
}

/// Spawn Deribit tasks (books + trades)
pub async fn spawn_deribit_from_config(
    cfg: &crate::config::DeribitSources,
    tx_snap: tokio::sync::mpsc::UnboundedSender<OrderBookSnapshot>,
    tx_trade: tokio::sync::mpsc::UnboundedSender<TradePrint>,
) -> Result<()> {
    let router = Router::new();
    let token = std::sync::Arc::new(tokio::sync::Mutex::new(None));

    for inst in cfg.books.clone() {
        let tx = tx_snap.clone();
        let cfg_cl = cfg.clone();
        let token_cl = token.clone();
        let router_cl = router.clone();
        tokio::spawn(async move {
            if let Err(e) = l2book_loop(
                &cfg_cl.ws_url,
                &inst,
                tx,
                token_cl,
                router_cl,
                cfg_cl.client_id.clone(),
                cfg_cl.client_secret.clone(),
                cfg_cl.private.unwrap_or(false),
            )
            .await
            {
                error!(instrument=%inst, "deribit book loop error: {:?}", e);
            }
        });
    }

    for inst in cfg.trades.clone() {
        let tx = tx_trade.clone();
        let cfg_cl = cfg.clone();
        let token_cl = token.clone();
        let router_cl = router.clone();
        tokio::spawn(async move {
            if let Err(e) = trades_loop(
                &cfg_cl.ws_url,
                &inst,
                tx,
                token_cl,
                router_cl,
                cfg_cl.client_id.clone(),
                cfg_cl.client_secret.clone(),
                cfg_cl.private.unwrap_or(false),
            )
            .await
            {
                error!(instrument=%inst, "deribit trades loop error: {:?}", e);
            }
        });
    }

    Ok(())
}

async fn l2book_loop(
    url: &str,
    instrument: &str,
    tx_snap: tokio::sync::mpsc::UnboundedSender<OrderBookSnapshot>,
    token: SharedToken,
    router: Router,
    client_id: Option<String>,
    client_secret: Option<String>,
    needs_auth: bool,
) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let backoff = ((2u64).pow(std::cmp::min(attempt, 10)) * 100).min(30_000);
        info!(instrument=%instrument, attempt, backoff_ms=backoff, "deribit L2Book connecting");
        match connect_async(url).await {
            Ok((ws_stream, _resp)) => {
                info!(instrument=%instrument, "deribit L2Book connected");
                let (mut write, mut read) = ws_stream.split();

                if needs_auth {
                    if let (Some(id), Some(secret)) = (client_id.clone(), client_secret.clone()) {
                        if let Err(e) =
                            authenticate_ws(&mut write, &id, &secret, token.clone(), router.clone())
                                .await
                        {
                            warn!(instrument=%instrument, "auth failed: {:?}", e);
                            let _ = write.send(WsMsg::Close(None)).await;
                            sleep(Duration::from_millis(backoff)).await;
                            continue;
                        } else {
                            info!(instrument=%instrument, "auth ok");
                        }
                    } else {
                        warn!(instrument=%instrument, "auth required but missing credentials");
                    }
                }

                // subscribe
                let sub = json!({
                    "jsonrpc":"2.0",
                    "id": next_id(),
                    "method":"public/subscribe",
                    "params": {
                        "channels":[ format!("book.{}.none.1.100ms", instrument) ]
                    }
                });
                let _ = write.send(WsMsg::Text(sub.to_string())).await;
                info!(instrument=%instrument, "deribit L2Book subscribed");

                let mut msg_count: u64 = 0;
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(WsMsg::Text(t)) => {
                            println!("{:?}", t);
                            msg_count += 1;
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                                if let Some(params) = v.get("params") {
                                    if let Some(data) = params.get("data") {
                                        let bids = data
                                            .get("bids")
                                            .and_then(|b| b.as_array())
                                            .cloned()
                                            .unwrap_or_default();
                                        let asks = data
                                            .get("asks")
                                            .and_then(|a| a.as_array())
                                            .cloned()
                                            .unwrap_or_default();
                                        if !bids.is_empty() && !asks.is_empty() {
                                            let best_bid = bids[0]
                                                .get(0)
                                                .and_then(|x| x.as_f64())
                                                .unwrap_or(0.0);
                                            let best_ask = asks[0]
                                                .get(0)
                                                .and_then(|x| x.as_f64())
                                                .unwrap_or(0.0);
                                            if best_bid.is_finite()
                                                && best_ask.is_finite()
                                                && best_ask >= best_bid
                                            {
                                                let mid = 0.5 * (best_bid + best_ask);
                                                let ts = data
                                                    .get("timestamp")
                                                    .and_then(|x| x.as_i64())
                                                    .unwrap_or(now_ms());
                                                let snap = OrderBookSnapshot {
                                                    exchange: "deribit".to_string(),
                                                    asset: instrument.to_string(),
                                                    kind: AssetKind::Futures,
                                                    bid: best_bid,
                                                    ask: best_ask,
                                                    mid,
                                                    ts_ms: ts,
                                                };
                                                let _ = tx_snap.send(snap);
                                            }
                                        }
                                    }
                                }
                            }
                            if msg_count % 500 == 1 {
                                debug!(instrument=%instrument, msg_count, "deribit book sample");
                            }
                        }
                        Ok(WsMsg::Close(cf)) => {
                            warn!(instrument=%instrument, ?cf, "deribit book close received");
                            break;
                        }
                        Err(e) => {
                            warn!(instrument=%instrument, err=?e, "deribit book error");
                            break;
                        }
                        _ => {}
                    }
                }

                warn!(instrument=%instrument, "deribit book socket ended; reconnecting in {}ms", backoff);
                let _ = write.send(WsMsg::Close(None)).await;
                sleep(Duration::from_millis(backoff)).await;
            }
            Err(e) => {
                warn!(instrument=%instrument, err=?e, "connect failed; backoff {}ms", backoff);
                sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
}

async fn trades_loop(
    url: &str,
    instrument: &str,
    tx_trade: tokio::sync::mpsc::UnboundedSender<TradePrint>,
    token: SharedToken,
    router: Router,
    client_id: Option<String>,
    client_secret: Option<String>,
    needs_auth: bool,
) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let backoff = ((2u64).pow(std::cmp::min(attempt, 10)) * 100).min(30_000);
        info!(instrument=%instrument, attempt, backoff_ms=backoff, "deribit trades connecting");
        match connect_async(url).await {
            Ok((ws_stream, _resp)) => {
                info!(instrument=%instrument, "deribit trades connected");
                let (mut write, mut read) = ws_stream.split();

                if needs_auth {
                    if let (Some(id), Some(secret)) = (client_id.clone(), client_secret.clone()) {
                        if let Err(e) =
                            authenticate_ws(&mut write, &id, &secret, token.clone(), router.clone())
                                .await
                        {
                            warn!(instrument=%instrument, "auth failed: {:?}", e);
                            let _ = write.send(WsMsg::Close(None)).await;
                            sleep(Duration::from_millis(backoff)).await;
                            continue;
                        } else {
                            info!(instrument=%instrument, "auth ok");
                        }
                    } else {
                        warn!(instrument=%instrument, "auth required but missing credentials");
                    }
                }

                let sub = json!({
                    "jsonrpc":"2.0",
                    "id": next_id(),
                    "method":"public/subscribe",
                    "params": { "channels":[ format!("trades.{}.100ms", instrument) ] }
                });
                let _ = write.send(WsMsg::Text(sub.to_string())).await;
                info!(instrument=%instrument, "deribit trades subscribed");

                let mut msg_count: u64 = 0;
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(WsMsg::Text(t)) => {
                            msg_count += 1;
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&t) {
                                if let Some(params) = v.get("params") {
                                    if let Some(arr) = params.get("data").and_then(|d| d.as_array())
                                    {
                                        for tr in arr {
                                            let px = tr
                                                .get("price")
                                                .and_then(|p| p.as_f64())
                                                .unwrap_or(f64::NAN);
                                            let sz = tr
                                                .get("amount")
                                                .and_then(|a| a.as_f64())
                                                .unwrap_or(0.0);
                                            if !px.is_finite() || sz <= 0.0 {
                                                continue;
                                            }
                                            let side = match tr
                                                .get("direction")
                                                .and_then(|s| s.as_str())
                                                .unwrap_or("")
                                            {
                                                s if s.eq_ignore_ascii_case("buy") => {
                                                    TradeSide::Buy
                                                }
                                                s if s.eq_ignore_ascii_case("sell") => {
                                                    TradeSide::Sell
                                                }
                                                _ => TradeSide::Unknown,
                                            };
                                            let ts = tr
                                                .get("timestamp")
                                                .and_then(|t| t.as_i64())
                                                .unwrap_or(now_ms());
                                            let p = TradePrint {
                                                exchange: "deribit".to_string(),
                                                asset: instrument.to_string(),
                                                kind: AssetKind::Futures,
                                                px,
                                                sz,
                                                side,
                                                ts_ms: ts,
                                            };
                                            let _ = tx_trade.send(p);
                                        }
                                    }
                                }
                            }
                            if msg_count % 500 == 1 {
                                debug!(instrument=%instrument, msg_count, "deribit trades sample");
                            }
                        }
                        Ok(WsMsg::Close(cf)) => {
                            warn!(instrument=%instrument, ?cf, "deribit trades close received");
                            break;
                        }
                        Err(e) => {
                            warn!(instrument=%instrument, err=?e, "deribit trades error");
                            break;
                        }
                        _ => {}
                    }
                }

                warn!(instrument=%instrument, "deribit trades socket ended; reconnecting in {}ms", backoff);
                let _ = write.send(WsMsg::Close(None)).await;
                sleep(Duration::from_millis(backoff)).await;
            }
            Err(e) => {
                warn!(instrument=%instrument, err=?e, "connect failed; backoff {}ms", backoff);
                sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
}
