// src/aggregator.rs
use crate::config::AppConfig;
use crate::mmap_writer::WriterMsg;
use crate::types::{
    ExchangeState, GlobalOrderBook, InstrumentState, OrderBookSnapshot, TradePrint,
};
use crate::writer_envelope::{Envelope, RType};
use anyhow::Result;
use bincode;
use blake3;
use serde_json;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Float-equality epsilon used to suppress micro-changes/noise.
/// Tune as needed for your instrument scale; 1e-8 is conservative for price values.
const EPS: f64 = 1e-8;

/// compare floats with epsilon
#[inline]
fn feq(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPS
}

/// Return true if snapshots are meaningfully equal (ignore ts_ms).
fn snapshot_eq(a: &OrderBookSnapshot, b: &OrderBookSnapshot) -> bool {
    feq(a.bid, b.bid) && feq(a.ask, b.ask) && feq(a.mid, b.mid)
}

/// Return true if trades are meaningfully equal (ignore ts_ms).
fn trade_eq(a: &TradePrint, b: &TradePrint) -> bool {
    a.side == b.side && feq(a.px, b.px) && feq(a.sz, b.sz)
}

/// Spawn aggregator: consumes rx_snap and rx_trade, writes GlobalOrderBook to tx_writer on meaningful updates.
///
/// Simpler semantics:
///  - Keep `snaps` and `trades` maps storing the latest record per exchange+asset.
///  - If incoming record equals stored latest -> ignore.
///  - If incoming record differs -> update stored latest and attempt to emit GlobalOrderBook,
///    but actually only write to mmap if the serialized GlobalOrderBook hash differs from the last emitted hash.
pub fn spawn_aggregator(
    cfg: &AppConfig,
    mut rx_snap: UnboundedReceiver<OrderBookSnapshot>,
    mut rx_trade: UnboundedReceiver<TradePrint>,
    tx_writer: Sender<WriterMsg>,
) {
    let cfg = cfg.clone();

    tokio::spawn(async move {
        // latest seen state (used to build GlobalOrderBook)
        let mut snaps: BTreeMap<String, BTreeMap<String, OrderBookSnapshot>> = BTreeMap::new();
        let mut trades: BTreeMap<String, BTreeMap<String, TradePrint>> = BTreeMap::new();

        // last emitted GOB hash (blake3)
        let mut last_gob_hash: Option<[u8; 32]> = None;

        let mut hb_count: u64 = 0;

        loop {
            tokio::select! {
                Some(snap) = rx_snap.recv() => {
                    // clone small keys up-front
                    let exch_key = snap.exchange.clone();
                    let asset_key = snap.asset.clone();

                    // check against stored latest (if any)
                    let stored_same = snaps
                        .get(&exch_key)
                        .and_then(|m| m.get(&asset_key))
                        .map_or(false, |existing| snapshot_eq(existing, &snap));

                    if stored_same {
                        tracing::trace!(exchange=%exch_key, asset=%asset_key, "incoming snapshot identical to stored; ignored");
                        continue;
                    }

                    // update stored latest (move snap in)
                    snaps.entry(exch_key.clone()).or_default().insert(asset_key.clone(), snap);

                    // build a candidate GlobalOrderBook, serialize and compute hash
                    hb_count = hb_count.wrapping_add(1);
                    match build_gob_and_hash(&snaps, &trades, hb_count) {
                        Ok((gob_bin, hash)) => {
                            // compare with last emitted
                            if last_gob_hash.as_ref().map(|h| h.as_ref()) == Some(&hash) {
                                tracing::trace!(exchange=%exch_key, asset=%asset_key, "gob hash == last emitted; emit skipped");
                                continue;
                            }

                            // different: attempt to emit (both bincode + json)
                            match emit_global_with_bin(&cfg, &gob_bin, hb_count, &tx_writer).await {
                                Ok(()) => {
                                    last_gob_hash = Some(hash);
                                    tracing::trace!(exchange=%exch_key, asset=%asset_key, "snapshot stored and emitted (hash updated)");
                                }
                                Err(e) => {
                                    tracing::warn!(exchange=%exch_key, asset=%asset_key, "aggregator emit error (snapshot): {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(exchange=%exch_key, asset=%asset_key, "failed to build gob: {:?}", e);
                        }
                    }
                }

                Some(tr) = rx_trade.recv() => {
                    let exch_key = tr.exchange.clone();
                    let asset_key = tr.asset.clone();

                    let stored_same = trades
                        .get(&exch_key)
                        .and_then(|m| m.get(&asset_key))
                        .map_or(false, |existing| trade_eq(existing, &tr));

                    if stored_same {
                        tracing::trace!(exchange=%exch_key, asset=%asset_key, "incoming trade identical to stored; ignored");
                        continue;
                    }

                    // update stored latest (move tr in)
                    trades.entry(exch_key.clone()).or_default().insert(asset_key.clone(), tr);

                    // build candidate gob, hash and compare
                    hb_count = hb_count.wrapping_add(1);
                    match build_gob_and_hash(&snaps, &trades, hb_count) {
                        Ok((gob_bin, hash)) => {
                            if last_gob_hash.as_ref().map(|h| h.as_ref()) == Some(&hash) {
                                tracing::trace!(exchange=%exch_key, asset=%asset_key, "gob hash == last emitted; emit skipped");
                                continue;
                            }

                            match emit_global_with_bin(&cfg, &gob_bin, hb_count, &tx_writer).await {
                                Ok(()) => {
                                    last_gob_hash = Some(hash);
                                    tracing::trace!(exchange=%exch_key, asset=%asset_key, "trade stored and emitted (hash updated)");
                                }
                                Err(e) => {
                                    tracing::warn!(exchange=%exch_key, asset=%asset_key, "aggregator emit error (trade): {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(exchange=%exch_key, asset=%asset_key, "failed to build gob: {:?}", e);
                        }
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

/// Build GlobalOrderBook from current maps and return (bincode_payload, blake3_hash)
fn build_gob_and_hash(
    snaps: &BTreeMap<String, BTreeMap<String, OrderBookSnapshot>>,
    trades: &BTreeMap<String, BTreeMap<String, TradePrint>>,
    heartbeat: u64,
) -> Result<(Vec<u8>, [u8; 32])> {
    let ts = now_ms();

    // union of exchanges (keys from snaps and trades)
    let mut ex_keys_set: BTreeSet<String> = BTreeSet::new();
    for k in snaps.keys() {
        ex_keys_set.insert(k.clone());
    }
    for k in trades.keys() {
        ex_keys_set.insert(k.clone());
    }
    let ex_keys: Vec<String> = ex_keys_set.into_iter().collect();

    let mut exchanges: Vec<ExchangeState> = Vec::with_capacity(ex_keys.len());

    for ex in ex_keys {
        let snap_map = snaps.get(&ex);
        let trade_map = trades.get(&ex);

        // union of symbols (from latest snapshots and latest trades)
        let mut symbols_set: BTreeSet<String> = BTreeSet::new();
        if let Some(sm) = snap_map {
            for k in sm.keys() {
                symbols_set.insert(k.clone());
            }
        }
        if let Some(tm) = trade_map {
            for k in tm.keys() {
                symbols_set.insert(k.clone());
            }
        }
        let symbols: Vec<String> = symbols_set.into_iter().collect();

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

    // serialize to bincode
    let payload_bin = bincode::serialize(&gob)?;

    // blake3 hash
    let mut out = [0u8; 32];
    let hash = blake3::hash(&payload_bin);
    out.copy_from_slice(&hash.as_bytes()[0..32]);

    Ok((payload_bin, out))
}

/// Emit the already-serialized bincode payload (plus JSON copy)
async fn emit_global_with_bin(
    _cfg: &AppConfig,
    payload_bin: &Vec<u8>,
    heartbeat: u64,
    tx_writer: &Sender<WriterMsg>,
) -> Result<()> {
    let ts = now_ms();

    // build envelope for bincode payload
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
            payload: payload_bin.clone(),
        })
        .await
        .map_err(|e| anyhow::anyhow!(format!("tx_writer send failed (bincode): {:?}", e)))?;

    // json debug copy
    // deserialize to GlobalOrderBook to produce JSON (or you could serialize original gob)
    // To avoid double-deserializing in heavy loads we could instead keep gob constructed earlier
    // but here it's fine.
    let gob: GlobalOrderBook = bincode::deserialize(&payload_bin)?;
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
