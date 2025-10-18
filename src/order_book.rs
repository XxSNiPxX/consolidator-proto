// src/order_book.rs
use anyhow::Result;
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message, Subscription};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;

use crate::types::{AssetKind, OrderBookSnapshot, TradePrint, TradeSide};

#[inline]
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Subscribe to HL L2Book for `coin` and send OrderBookSnapshot that includes `exchange`.
pub async fn run_feed_to_snapshots(
    exchange: String,
    coin: String,
    kind: AssetKind,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
    base: Option<BaseUrl>,
) -> Result<()> {
    let mut reconnect_count: u64 = 0;
    loop {
        reconnect_count += 1;
        tracing::info!(exchange=%exchange, coin=%coin, reconnect=reconnect_count, "connecting to HL L2Book");
        let mut info = InfoClient::new(None, base).await?;
        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        info.subscribe(Subscription::L2Book { coin: coin.clone() }, msg_tx)
            .await?;
        tracing::info!(exchange=%exchange, coin=%coin, "subscribed to HL L2Book");

        let mut seen: u64 = 0;
        while let Some(msg) = msg_rx.recv().await {
            if let Message::L2Book(book) = msg {
                if book.data.levels.len() < 2 {
                    continue;
                }
                let bids = &book.data.levels[0];
                let asks = &book.data.levels[1];
                if bids.is_empty() || asks.is_empty() {
                    continue;
                }

                let best_bid = bids[0].px.parse::<f64>().unwrap_or(0.0);
                let best_ask = asks[0].px.parse::<f64>().unwrap_or(0.0);
                if !(best_bid.is_finite() && best_ask.is_finite()) || best_ask < best_bid {
                    continue;
                }
                let mid = 0.5 * (best_bid + best_ask);

                seen += 1;
                if seen % 500 == 1 {
                    tracing::debug!(exchange=%exchange, coin=%coin, best_bid, best_ask, mid, ts=book.data.time, count=seen, "L2Book sample");
                }

                // clone kind so we don't move it
                let snap = OrderBookSnapshot {
                    exchange: exchange.clone(),
                    asset: coin.clone(),
                    kind: kind.clone(),
                    bid: best_bid,
                    ask: best_ask,
                    mid,
                    ts_ms: book.data.time as i64,
                };
                if tx_snap.send(snap).is_err() {
                    tracing::warn!(exchange=%exchange, coin=%coin, "tx_snap closed, breaking feed loop");
                    break;
                }
            } else {
                tracing::trace!(exchange=%exchange, coin=%coin, "non-L2Book message ignored");
            }
        }

        tracing::warn!(exchange=%exchange, coin=%coin, "L2Book stream ended; reconnecting in 1s");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Subscribe to HL Trades for `coin` and send TradePrint with `exchange`.
pub async fn run_trades_feed(
    exchange: String,
    coin: String,
    kind: AssetKind,
    tx_trade: UnboundedSender<TradePrint>,
    base: Option<BaseUrl>,
) -> Result<()> {
    let mut reconnect_count: u64 = 0;
    loop {
        reconnect_count += 1;
        tracing::info!(exchange=%exchange, coin=%coin, reconnect=reconnect_count, "connecting to HL Trades");
        let mut info = InfoClient::new(None, base).await?;
        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        info.subscribe(Subscription::Trades { coin: coin.clone() }, msg_tx)
            .await?;
        tracing::info!(exchange=%exchange, coin=%coin, "subscribed to HL Trades");

        let mut seen: u64 = 0;
        while let Some(msg) = msg_rx.recv().await {
            if let Message::Trades(trades) = msg {
                seen += trades.data.len() as u64;
                if seen % 500 == 1 {
                    if let Some(t0) = trades.data.get(0) {
                        tracing::debug!(exchange=%exchange, coin=%coin, px=%t0.px, sz=%t0.sz, side=%t0.side, ts=%t0.time, count=seen, "Trades sample");
                    }
                }

                for tr in trades.data {
                    let px = tr.px.parse::<f64>().unwrap_or(f64::NAN);
                    let sz = tr.sz.parse::<f64>().unwrap_or(0.0);
                    let side = match tr.side.as_str() {
                        s if s.eq_ignore_ascii_case("buy") => TradeSide::Buy,
                        s if s.eq_ignore_ascii_case("sell") => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    };
                    if !px.is_finite() || sz <= 0.0 {
                        continue;
                    }

                    // clone kind to avoid moving it
                    let print = TradePrint {
                        exchange: exchange.clone(),
                        asset: coin.clone(),
                        kind: kind.clone(),
                        px,
                        sz,
                        side,
                        ts_ms: tr.time as i64,
                    };
                    if tx_trade.send(print).is_err() {
                        tracing::warn!(exchange=%exchange, coin=%coin, "tx_trade closed, breaking trades loop");
                        break;
                    }
                }
            } else {
                tracing::trace!(exchange=%exchange, coin=%coin, "non-trades message ignored");
            }
        }

        tracing::warn!(exchange=%exchange, coin=%coin, "Trades stream ended; reconnecting in 1s");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
