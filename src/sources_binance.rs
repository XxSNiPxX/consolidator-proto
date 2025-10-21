// src/sources_binance.rs
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{AppConfig, BinanceSources};
use crate::types::{AssetKind, OrderBookSnapshot, TradePrint, TradeSide};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Spawn Binance connections according to `bin_cfg`.
/// This returns quickly after spawning tasks; tasks run forever (reconnect loops internally).
pub async fn spawn_binance_from_config(
    _cfg: &AppConfig,
    bin_cfg: &BinanceSources,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
    tx_tr: UnboundedSender<TradePrint>,
) -> Result<()> {
    let ws_url = bin_cfg.ws_url.clone();

    // spawn books connection if any
    if !bin_cfg.books.is_empty() {
        let books = bin_cfg.books.clone();
        let tx_snap_clone = tx_snap.clone();
        let ws_url_clone = ws_url.clone();
        tokio::spawn(async move {
            if let Err(e) = run_binance_book_stream(ws_url_clone, books, tx_snap_clone).await {
                tracing::error!("binance book stream fatal error: {:?}", e);
            }
        });
    }

    // spawn trades connection if any
    if !bin_cfg.trades.is_empty() {
        let trades = bin_cfg.trades.clone();
        let tx_tr_clone = tx_tr.clone();
        let ws_url_clone = ws_url.clone();
        tokio::spawn(async move {
            if let Err(e) = run_binance_trade_stream(ws_url_clone, trades, tx_tr_clone).await {
                tracing::error!("binance trade stream fatal error: {:?}", e);
            }
        });
    }

    Ok(())
}

/// Connect to Binance combined `bookTicker` streams and emit OrderBookSnapshot for each symbol.
/// This function will never return unless the spawn task is cancelled; it reconnects in a loop.
// helper: normalize a configured ws_url to a base (no /ws or /stream path)
fn normalize_binance_base(ws_url: &str) -> String {
    // trim trailing slashes
    let mut s = ws_url.trim_end_matches('/').to_string();
    // remove trailing "/ws" or "/stream" if present so we can append "/stream?streams=" safely
    if s.ends_with("/ws") {
        s.truncate(s.len() - "/ws".len());
    } else if s.ends_with("/stream") {
        s.truncate(s.len() - "/stream".len());
    }
    s
}

async fn run_binance_book_stream(
    ws_url: String,
    symbols: Vec<String>,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
) -> Result<()> {
    if symbols.is_empty() {
        tracing::warn!("run_binance_book_stream: no symbols configured, returning");
        return Ok(());
    }

    let stream_names: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_lowercase()))
        .collect();
    let streams_joined = stream_names.join("/");

    // normalize base
    let base = normalize_binance_base(&ws_url);

    loop {
        // Use combined-stream endpoint explicitly
        let url = format!("{}/stream?streams={}", base, streams_joined);
        tracing::info!(target: "binance", "connecting book stream: {}", url);

        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws_stream, _resp)) => {
                tracing::info!(target: "binance", "book stream connected");
                let (mut write, mut read) = ws_stream.split();

                // ... (rest unchanged) ...
                let mut ping_interval = tokio::time::interval(Duration::from_secs(15));

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                                tracing::warn!(target:"binance","book ping failed: {:?}", e);
                                break;
                            }
                        }

                        msg_opt = read.next() => {
                            let Some(msg) = msg_opt else {
                                tracing::warn!(target:"binance","book stream ended, reconnecting");
                                break;
                            };
                            match msg {
                                Ok(Message::Text(txt)) => {
                                    // same parsing as before...
                                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                        if let Some(data) = v.get("data") {
                                            if let Some(sym) = data.get("s").and_then(|x| x.as_str()) {
                                                let bid = data.get("b").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                let ask = data.get("a").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                let ts = data.get("E").and_then(|x| x.as_i64()).unwrap_or_else(|| now_ms());
                                                if bid.is_finite() && ask.is_finite() && ask >= bid {
                                                    let mid = 0.5*(bid+ask);
                                                    let snap = OrderBookSnapshot {
                                                        exchange: "binance".to_string(),
                                                        asset: sym.to_string(),
                                                        kind: AssetKind::Spot,
                                                        bid,
                                                        ask,
                                                        mid,
                                                        ts_ms: ts,
                                                    };
                                                    let _ = tx_snap.send(snap);
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    tracing::warn!(target:"binance","book close received, reconnecting");
                                    break;
                                }
                                Err(e) => {
                                    tracing::warn!(target:"binance","book recv error: {:?}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => {
                // log the error (this will include Http(Response ...) you saw)
                tracing::warn!(target: "binance", "failed to connect book stream: {:?}; retrying in 1s", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }
    }
}

async fn run_binance_trade_stream(
    ws_url: String,
    symbols: Vec<String>,
    tx_tr: UnboundedSender<TradePrint>,
) -> Result<()> {
    if symbols.is_empty() {
        tracing::warn!("run_binance_trade_stream: no symbols configured, returning");
        return Ok(());
    }

    let stream_names: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@trade", s.to_lowercase()))
        .collect();
    let streams_joined = stream_names.join("/");

    // normalize base
    let base = normalize_binance_base(&ws_url);

    loop {
        let url = format!("{}/stream?streams={}", base, streams_joined);
        tracing::info!(target: "binance", "connecting trades stream: {}", url);

        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws_stream, _resp)) => {
                tracing::info!(target: "binance", "trades stream connected");
                let (mut write, mut read) = ws_stream.split();

                let mut ping_interval = tokio::time::interval(Duration::from_secs(15));

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                                tracing::warn!(target:"binance","trade ping failed: {:?}", e);
                                break;
                            }
                        }

                        msg_opt = read.next() => {
                            let Some(msg) = msg_opt else {
                                tracing::warn!(target:"binance","trade stream ended, reconnecting");
                                break;
                            };

                            match msg {
                                Ok(Message::Text(txt)) => {
                                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                        if let Some(data) = v.get("data") {
                                            if let Some(sym) = data.get("s").and_then(|x| x.as_str()) {
                                                let px = data.get("p").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                let sz = data.get("q").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                let ts = data.get("E").and_then(|x| x.as_i64()).unwrap_or_else(|| now_ms());
                                                let side = data.get("m").and_then(|x| x.as_bool()).map(|is_maker| if is_maker { TradeSide::Sell } else { TradeSide::Buy }).unwrap_or(TradeSide::Unknown);
                                                if px.is_finite() && sz > 0.0 {
                                                    let tp = TradePrint {
                                                        exchange: "binance".to_string(),
                                                        asset: sym.to_string(),
                                                        kind: AssetKind::Spot,
                                                        px,
                                                        sz,
                                                        side,
                                                        ts_ms: ts,
                                                    };
                                                    let _ = tx_tr.send(tp);
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    tracing::warn!(target:"binance","trade close received, reconnecting");
                                    break;
                                }
                                Err(e) => {
                                    tracing::warn!(target:"binance","trade recv error: {:?}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => {
                tracing::warn!(target: "binance", "failed to connect trades stream: {:?}; retrying in 1s", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }
    }
}
