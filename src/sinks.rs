// src/sinks.rs
use crate::config::AppConfig;
use crate::mmap_writer::WriterMsg;
use crate::types::{OrderBookSnapshot, TradePrint};
use crate::writer_envelope::RType;
use anyhow::Result;
use bincode;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

use std::sync::atomic::{AtomicU32, Ordering};
use once_cell::sync::Lazy;

/// Global monotonic sequence used to debug ordering / detect wrap/overwrite.
/// Stored into Envelope.pad when building envelopes.
static GLOBAL_SEQ: Lazy<AtomicU32> = Lazy::new(|| AtomicU32::new(1));

fn next_seq() -> u32 {
    GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst)
}

/// Spawn a sink that listens for snapshots and forwards them to the writer.
pub fn spawn_snapshot_sink(
    cfg: &AppConfig,
    mut rx: UnboundedReceiver<OrderBookSnapshot>,
    txw: Sender<WriterMsg>,
) {
    let cfg = cfg.clone();
    tokio::spawn(async move {
        while let Some(snap) = rx.recv().await {
            match bincode::serialize(&snap) {
                Ok(payload) => {
                    // Build envelope using catalog if available (best-effort)
                    let mut env = crate::sinks::make_env_from_cfg(
                        &cfg,
                        "book",
                        &snap.exchange,
                        &snap.asset,
                        RType::BookSnapshot,
                        snap.ts_ms,
                        payload.len(),
                    );
                    // stamp monotonic seq for debugging in pad field
                    env.pad = next_seq();

                    // log for debugging
                    tracing::debug!(exchange=%snap.exchange, asset=%snap.asset, "snapshot -> writer len={} ts={} seq={}", payload.len(), snap.ts_ms, env.pad);
                    if let Err(e) = txw.send(WriterMsg { env, payload }).await {
                        tracing::warn!("snapshot_sink: tx_writer send failed: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(error=?e, "snapshot_sink: bincode serialize failed");
                    continue;
                }
            }
        }
        tracing::info!("snapshot_sink ended");
    });
}

/// Spawn a sink that listens for trades and forwards them to the writer.
pub fn spawn_trade_sink(
    cfg: &AppConfig,
    mut rx: UnboundedReceiver<TradePrint>,
    txw: Sender<WriterMsg>,
) {
    let cfg = cfg.clone();
    tokio::spawn(async move {
        while let Some(tr) = rx.recv().await {
            match bincode::serialize(&tr) {
                Ok(payload) => {
                    let mut env = crate::sinks::make_env_from_cfg(
                        &cfg,
                        "trades",
                        &tr.exchange,
                        &tr.asset,
                        RType::TradePrint,
                        tr.ts_ms,
                        payload.len(),
                    );
                    env.pad = next_seq();

                    tracing::debug!(exchange=%tr.exchange, asset=%tr.asset, px=%tr.px, "trade -> writer len={} ts={} seq={}", payload.len(), tr.ts_ms, env.pad);
                    if let Err(e) = txw.send(WriterMsg { env, payload }).await {
                        tracing::warn!("trade_sink: tx_writer send failed: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(error=?e, "trade_sink: bincode serialize failed");
                    continue;
                }
            }
        }
        tracing::info!("trade_sink ended");
    });
}

// helper to produce an Envelope struct using the catalog when possible
pub fn make_env_from_cfg(
    cfg: &AppConfig,
    channel: &str,
    exchange: &str,
    symbol: &str,
    rtype: crate::writer_envelope::RType,
    ts_ms: i64,
    payload_len: usize,
) -> crate::writer_envelope::Envelope {
    let src_id = cfg
        .catalog
        .sources
        .iter()
        .find(|s| s.name == exchange)
        .map(|s| s.id)
        .unwrap_or(0);
    let inst_id = cfg
        .catalog
        .instruments
        .iter()
        .find(|i| i.exchange == exchange && i.symbol == symbol && i.channel == channel)
        .map(|i| i.id)
        .unwrap_or(0);
    crate::writer_envelope::Envelope {
        rtype: rtype as u16,
        flags: 0,
        ts_ms: ts_ms as u64,
        src_id,
        inst_id,
        len: payload_len as u32,
        pad: 0, // overwritten by caller via next_seq()
    }
}
