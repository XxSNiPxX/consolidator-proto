// src/aggregator.rs
use crate::config::AppConfig;
use crate::mmap_writer::WriterMsg;
use crate::types::{
    ExchangeState, GlobalOrderBook, InstrumentState, OrderBookSnapshot, TradePrint,
};
use crate::writer_envelope::{Envelope, RType};
use anyhow::Result;
use bincode;
use serde_json;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Spawn aggregator: consumes rx_snap and rx_trade, writes GlobalOrderBook to tx_writer on every update.
pub fn spawn_aggregator(
    cfg: &AppConfig,
    mut rx_snap: UnboundedReceiver<OrderBookSnapshot>,
    mut rx_trade: UnboundedReceiver<TradePrint>,
    tx_writer: Sender<WriterMsg>,
) {
    // clone config for the spawned task (AppConfig is Clone).
    let cfg = cfg.clone();

    tokio::spawn(async move {
        // exchange -> symbol -> snapshot/trade
        let mut snaps: BTreeMap<String, BTreeMap<String, OrderBookSnapshot>> = BTreeMap::new();
        let mut trades: BTreeMap<String, BTreeMap<String, TradePrint>> = BTreeMap::new();
        let mut hb_count: u64 = 0;

        loop {
            tokio::select! {
                Some(snap) = rx_snap.recv() => {
                    snaps.entry(snap.exchange.clone()).or_default().insert(snap.asset.clone(), snap);
                    hb_count = hb_count.wrapping_add(1);
                    if let Err(e) = emit_global(&cfg, &snaps, &trades, hb_count, &tx_writer).await {
                        tracing::warn!("aggregator emit error: {:?}", e);
                    }
                }
                Some(tr) = rx_trade.recv() => {
                    trades.entry(tr.exchange.clone()).or_default().insert(tr.asset.clone(), tr);
                    hb_count = hb_count.wrapping_add(1);
                    if let Err(e) = emit_global(&cfg, &snaps, &trades, hb_count, &tx_writer).await {
                        tracing::warn!("aggregator emit error: {:?}", e);
                    }
                }
                else => {
                    tracing::info!("aggregator channels closed; exiting");
                    break;
                }
            }
        }
    });
}

async fn emit_global(
    _cfg: &AppConfig,
    snaps: &BTreeMap<String, BTreeMap<String, OrderBookSnapshot>>,
    trades: &BTreeMap<String, BTreeMap<String, TradePrint>>,
    heartbeat: u64,
    tx_writer: &Sender<WriterMsg>,
) -> Result<()> {
    // timestamp
    let ts = now_ms();

    // union of exchanges
    let mut ex_keys: Vec<String> = snaps.keys().cloned().collect();
    for k in trades.keys() {
        if !ex_keys.contains(k) {
            ex_keys.push(k.clone());
        }
    }
    ex_keys.sort();

    let mut exchanges: Vec<ExchangeState> = Vec::with_capacity(ex_keys.len());

    for ex in ex_keys {
        let snap_map = snaps.get(&ex);
        let trade_map = trades.get(&ex);

        // union of symbols
        let mut symbols: Vec<String> = Vec::new();
        if let Some(sm) = snap_map {
            for k in sm.keys() {
                if !symbols.contains(k) {
                    symbols.push(k.clone());
                }
            }
        }
        if let Some(tm) = trade_map {
            for k in tm.keys() {
                if !symbols.contains(k) {
                    symbols.push(k.clone());
                }
            }
        }
        symbols.sort();

        let mut insts: Vec<InstrumentState> = Vec::with_capacity(symbols.len());
        for sym in symbols {
            let last_snapshot = snap_map.and_then(|m| m.get(&sym).cloned());
            let snapshot_age_ms = last_snapshot.as_ref().map(|s| ts - s.ts_ms);
            let last_trade = trade_map.and_then(|m| m.get(&sym).cloned());
            let trade_age_ms = last_trade.as_ref().map(|t| ts - t.ts_ms);

            let inst = InstrumentState {
                exchange: ex.clone(),
                symbol: sym.clone(),
                inst_id: 0u32,
                last_snapshot,
                snapshot_age_ms,
                last_trade: last_trade.clone(),
                trade_age_ms,
                has_trade: last_trade.is_some(),
            };
            insts.push(inst);
        }

        exchanges.push(ExchangeState {
            exchange: ex.clone(),
            instruments: insts,
        });
    }

    let gob = GlobalOrderBook {
        ts_ms: ts,
        exchanges,
        heartbeat_count: heartbeat,
    };

    // 1) bincode payload for consumers expecting binary GlobalOrderBook
    let payload_bin = bincode::serialize(&gob)?;
    let env_bin = Envelope {
        rtype: RType::GlobalOrderBook as u16,
        flags: 0u16,
        ts_ms: ts as u64,
        src_id: 0,
        inst_id: 0,
        len: payload_bin.len() as u32,
        pad: 0,
    };

    tx_writer
        .send(WriterMsg {
            env: env_bin,
            payload: payload_bin,
        })
        .await
        .map_err(|e| anyhow::anyhow!(format!("tx_writer send failed (bincode): {:?}", e)))?;

    // 2) JSON debug copy so reader.js can print a readable state (rtype = GlobalOrderBookJson)
    let payload_json = serde_json::to_vec(&gob)?;
    let env_json = Envelope {
        rtype: RType::GlobalOrderBookJson as u16,
        flags: 0u16,
        ts_ms: ts as u64,
        src_id: 0,
        inst_id: 0,
        len: payload_json.len() as u32,
        pad: 0,
    };

    tx_writer
        .send(WriterMsg {
            env: env_json,
            payload: payload_json,
        })
        .await
        .map_err(|e| anyhow::anyhow!(format!("tx_writer send failed (json): {:?}", e)))?;

    Ok(())
}
