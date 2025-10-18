use anyhow::Result;
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message, Subscription};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::types::{AssetKind, OrderBookSnapshot, TradePrint, TradeSide};

/// Spawn HL feed tasks for all exchanges entries named "hyperliquid" in config
pub async fn spawn_hl_from_config(
    cfg: &AppConfig,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
    tx_tr: UnboundedSender<TradePrint>,
) -> Result<()> {
    for ex in &cfg.exchanges {
        if ex.name.to_ascii_lowercase() != "hyperliquid" {
            continue;
        }

        // Decide BaseUrl
        let base = match ex.network.to_ascii_lowercase().as_str() {
            "mainnet" => Some(BaseUrl::Mainnet),
            "testnet" => Some(BaseUrl::Testnet),
            other => {
                warn!(exchange=%ex.name, network=%ex.network, other, "unknown HL network, default to mainnet");
                Some(BaseUrl::Mainnet)
            }
        };

        info!(
            exchange=%ex.name,
            network=%ex.network,
            books=?ex.subscriptions.books,
            trades=?ex.subscriptions.trades,
            "spawning Hyperliquid sources"
        );

        for coin in ex.subscriptions.books.clone() {
            let tx = tx_snap.clone();
            let base2 = base.clone();
            let exchange_name = ex.name.clone();
            debug!(exchange=%exchange_name, %coin, "spawn HL book task");
            tokio::spawn(async move {
                if let Err(e) = run_feed_to_snapshots(
                    exchange_name.clone(),
                    coin.clone(),
                    AssetKind::Spot,
                    tx,
                    base2,
                )
                .await
                {
                    error!(exchange=%exchange_name, %coin, error=?e, "HL L2Book task exited");
                }
            });
        }

        for coin in ex.subscriptions.trades.clone() {
            let tx = tx_tr.clone();
            let base2 = base.clone();
            let exchange_name = ex.name.clone();
            debug!(exchange=%exchange_name, %coin, "spawn HL trades task");
            tokio::spawn(async move {
                if let Err(e) = run_trades_feed(
                    exchange_name.clone(),
                    coin.clone(),
                    AssetKind::Futures,
                    tx,
                    base2,
                )
                .await
                {
                    error!(exchange=%exchange_name, %coin, error=?e, "HL Trades task exited");
                }
            });
        }
    }
    Ok(())
}

/// Runs the HL L2Book subscription loop (connects internally via InfoClient)
async fn run_feed_to_snapshots(
    exchange: String,
    coin: String,
    kind: AssetKind,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
    base: Option<BaseUrl>,
) -> Result<()> {
    let mut connect_attempt: u64 = 0;
    loop {
        connect_attempt += 1;
        info!(exchange=%exchange, coin=%coin, attempt=connect_attempt, "HL L2Book connect");
        let res = InfoClient::new(None, base).await;
        let mut info = match res {
            Ok(i) => i,
            Err(e) => {
                warn!(exchange=%exchange, coin=%coin, err=?e, "HL InfoClient::new failed; backoff 1s");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        if let Err(e) = info
            .subscribe(Subscription::L2Book { coin: coin.clone() }, msg_tx)
            .await
        {
            warn!(exchange=%exchange, coin=%coin, err=?e, "subscribe failed; will retry");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
        info!(exchange=%exchange, coin=%coin, "HL L2Book subscribed");

        let mut seen = 0u64;
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                Message::L2Book(book) => {
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
                        debug!(exchange=%exchange, coin=%coin, best_bid, best_ask, mid, ts=book.data.time, count=seen, "L2Book sample");
                    }

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
                        warn!(exchange=%exchange, coin=%coin, "tx_snap closed; stopping task");
                        break;
                    }
                }
                _ => {
                    debug!(exchange=%exchange, coin=%coin, "HL non-book message");
                }
            }
        }

        warn!(exchange=%exchange, coin=%coin, "HL stream ended; reconnecting in 1s");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// HL Trades loop (similar structure)
async fn run_trades_feed(
    exchange: String,
    coin: String,
    kind: AssetKind,
    tx_trade: UnboundedSender<TradePrint>,
    base: Option<BaseUrl>,
) -> Result<()> {
    let mut connect_attempt: u64 = 0;
    loop {
        connect_attempt += 1;
        info!(exchange=%exchange, coin=%coin, attempt=connect_attempt, "HL Trades connect");
        let res = InfoClient::new(None, base).await;
        let mut info = match res {
            Ok(i) => i,
            Err(e) => {
                warn!(exchange=%exchange, coin=%coin, err=?e, "HL InfoClient::new failed; backoff 1s");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        if let Err(e) = info
            .subscribe(Subscription::Trades { coin: coin.clone() }, msg_tx)
            .await
        {
            warn!(exchange=%exchange, coin=%coin, err=?e, "subscribe trades failed; will retry");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
        info!(exchange=%exchange, coin=%coin, "HL Trades subscribed");

        let mut seen = 0u64;
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                Message::Trades(trades) => {
                    seen += trades.data.len() as u64;
                    if seen % 500 == 1 {
                        if let Some(t0) = trades.data.get(0) {
                            debug!(exchange=%exchange, coin=%coin, px=%t0.px, sz=%t0.sz, side=%t0.side, ts=%t0.time, count=seen, "Trades sample");
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
                            warn!(exchange=%exchange, coin=%coin, "tx_trade closed; breaking");
                            break;
                        }
                    }
                }
                _ => {
                    debug!(exchange=%exchange, coin=%coin, "HL non-trades message");
                }
            }
        }

        warn!(exchange=%exchange, coin=%coin, "HL trades ended; reconnecting in 1s");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
