// src/config.rs
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinanceSources {
    pub ws_url: String,
    #[serde(default)]
    pub books: Vec<String>,
    #[serde(default)]
    pub trades: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeribitSources {
    pub ws_url: String,
    #[serde(default)]
    pub books: Vec<String>,
    #[serde(default)]
    pub trades: Vec<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub private: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub mode: Mode,
    pub exchanges: Vec<Exchange>,
    pub combiners: Vec<CombinerCfg>,
    pub mmap: MmapCfg,
    pub logging: LoggingCfg,
    pub reconnect: ReconnectCfg,
    #[serde(default)]
    pub secrets: Secrets,
    #[serde(default)]
    pub catalog: Catalog,
    #[serde(default)]
    pub deribit: Option<DeribitSources>,
    #[serde(default)]
    pub binance: Option<BinanceSources>,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let s = fs::read_to_string(path)
            .with_context(|| format!("reading config: {}", path.display()))?;
        let cfg: AppConfig = serde_yaml::from_str(&s).context("parsing YAML config")?;
        Ok(cfg)
    }
}

/* ===== app mode ===== */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mode {
    pub shard_strategy: String, // "global"
    pub enable_combined_ticks: bool,
}

/* ===== exchanges ===== */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exchange {
    pub name: String,    // "hyperliquid"
    pub network: String, // "mainnet" | "testnet"
    pub subscriptions: Subscriptions,
    #[serde(default)]
    pub options: ExchangeOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Subscriptions {
    #[serde(default)]
    pub books: Vec<String>, // HL book symbols (e.g. "@107", "BTC")
    #[serde(default)]
    pub trades: Vec<String>, // HL trade symbols (e.g. "HYPE", "BTC")
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExchangeOptions {
    pub ping_interval_ms: Option<u64>,
    pub read_timeout_ms: Option<u64>,
    pub max_message_bytes: Option<usize>,
}

/* ===== combiners ===== */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinerCfg {
    pub spot_asset: String,
    pub fut_asset: String,
    pub stale_ms: i64,
}

/* ===== mmap writer ===== */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MmapCfg {
    pub path: String,
    pub bytes: u64,
    pub mode: String,   // "ring"
    pub format: String, // "bincode"
    pub header: HeaderCfg,
    pub flush: FlushCfg,
    pub channel: ChannelCfg,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderCfg {
    pub magic: String, // "MDLG"
    pub version: u16,
    pub embed_catalog: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushCfg {
    pub bytes: u64,
    pub interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelCfg {
    pub bound_records: usize,
    pub on_overflow: String, // "block" | "drop_oldest" | "drop_new"
}

/* ===== logging ===== */

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LoggingCfg {
    pub path: String,
    pub level: String,  // "debug" while testing
    pub format: String, // "text" | "json"
    #[serde(default)]
    pub also_stdout: Option<bool>,
}

/* ===== reconnect ===== */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectCfg {
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: f64,
    pub heartbeat_ms: u64,
    pub read_idle_timeout_ms: u64,
    pub reset_backoff_after_ms: u64,
}

/* ===== secrets ===== */

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Secrets {
    // Generic: keep here for other venues that require signing
    pub private_keys: Option<Vec<String>>,
    pub api_keys: Option<Vec<ApiKey>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub name: String,
    pub key: String,
    pub secret: String,
}

/* ===== stable catalog ids ===== */

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    #[serde(default)]
    pub sources: Vec<CatalogSource>, // (exchange, network) -> src_id
    #[serde(default)]
    pub instruments: Vec<CatalogInstrument>, // (exchange, symbol, channel) -> inst_id
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSource {
    pub id: u32,
    pub name: String,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogInstrument {
    pub id: u32,
    pub exchange: String,
    pub symbol: String,
    pub channel: String, // "book" | "trades" | "combined"
}
