// src/types.rs
use serde::{Deserialize, Serialize};

pub type Ms = i64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AssetKind {
    Spot,
    Futures,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: String, // NEW
    pub asset: String,
    pub kind: AssetKind,
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub ts_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradePrint {
    pub exchange: String, // NEW
    pub asset: String,
    pub kind: AssetKind,
    pub px: f64,
    pub sz: f64,
    pub side: TradeSide,
    pub ts_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CombinedTick {
    pub ts_ms: i64,
    pub spot: OrderBookSnapshot,
    pub fut: OrderBookSnapshot,
    pub spread: f64,
    pub stale: bool,
    pub stale_reason: Option<String>,
}

/// ========== Global aggregator output ==========

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstrumentState {
    pub exchange: String,
    pub symbol: String,
    pub inst_id: u32, // optional mapping from catalog
    pub last_snapshot: Option<OrderBookSnapshot>,
    pub snapshot_age_ms: Option<i64>,
    pub last_trade: Option<TradePrint>,
    pub trade_age_ms: Option<i64>,
    pub has_trade: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExchangeState {
    pub exchange: String,
    pub instruments: Vec<InstrumentState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalOrderBook {
    pub ts_ms: i64,
    pub exchanges: Vec<ExchangeState>,
    pub heartbeat_count: u64,
}
