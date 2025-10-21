// main.rs
mod aggregator;
mod auth;
mod config;
mod id;
mod logger;
mod mmap_writer; // single global ring writer
mod order_book; // HL feeds
mod router;
mod shutdown;
mod sinks; // channel sinks → writer
mod sources_binance; // <-- new Binance source module
mod sources_deribit;
mod sources_hl; // hyperliquid source spawner
mod types;
mod writer_envelope; // mmap envelope (src_id/inst_id + rtype)

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::logger::init_logger;
use crate::mmap_writer::{GlobalWriter, WriterMsg};
use crate::sinks::{spawn_snapshot_sink, spawn_trade_sink};
use crate::sources_binance::spawn_binance_from_config; // <-- import spawn fn
use crate::sources_deribit::spawn_deribit_from_config;
use crate::sources_hl::spawn_hl_from_config;
use crate::types::{CombinedTick, OrderBookSnapshot, TradePrint};

#[derive(Parser, Debug)]
#[command(name = "consolidator-proto")]
struct Cli {
    /// Path to YAML config
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // 1) Load config
    let cfg = AppConfig::load(&cli.config)?;

    // 2) Logger
    init_logger(&cfg.logging)?;

    // 3) Writer + unified channel
    let (tx_writer, rx_writer) = mpsc::channel::<WriterMsg>(cfg.mmap.channel.bound_records);
    let mut writer = GlobalWriter::new(&cfg.mmap, cfg.catalog.clone()).await?;
    tokio::spawn(async move {
        if let Err(e) = writer.run(rx_writer).await {
            tracing::error!("writer stopped: {e:?}");
        }
    });

    // ---------------------
    // Snapshot/Trade channel topology:
    //
    // sources -> tx_snap_in/tx_tr_in (single inbound senders)
    //                 v
    //           duplicator tasks
    //         /                     \
    //   sink receivers           aggregator receivers
    //
    // This avoids trying to clone UnboundedReceiver.
    // ---------------------

    // inbound channels that sources will send into
    let (tx_snap_in, mut rx_snap_in) = mpsc::unbounded_channel::<OrderBookSnapshot>();
    let (tx_tr_in, mut rx_tr_in) = mpsc::unbounded_channel::<TradePrint>();

    // sink-facing channels (consumed by sinks that write to mmap)
    let (tx_snap_sink, rx_snap_sink) = mpsc::unbounded_channel::<OrderBookSnapshot>();
    let (tx_tr_sink, rx_tr_sink) = mpsc::unbounded_channel::<TradePrint>();

    // aggregator-facing channels (consumed by aggregator)
    let (tx_snap_agg, rx_snap_agg) = mpsc::unbounded_channel::<OrderBookSnapshot>();
    let (tx_tr_agg, rx_tr_agg) = mpsc::unbounded_channel::<TradePrint>();

    // Spawn small duplicators: inbound -> (sink + agg)
    // snapshots duplicator
    {
        let tx_snap_sink_clone = tx_snap_sink.clone();
        let tx_snap_agg_clone = tx_snap_agg.clone();
        tokio::spawn(async move {
            while let Some(snap) = rx_snap_in.recv().await {
                // best-effort forward to sink and aggregator
                let _ = tx_snap_sink_clone.send(snap.clone());
                let _ = tx_snap_agg_clone.send(snap);
            }
            tracing::info!("snap inbound closed; snapshot duplicator exiting");
        });
    }

    // trades duplicator
    {
        let tx_tr_sink_clone = tx_tr_sink.clone();
        let tx_tr_agg_clone = tx_tr_agg.clone();
        tokio::spawn(async move {
            while let Some(tr) = rx_tr_in.recv().await {
                let _ = tx_tr_sink_clone.send(tr.clone());
                let _ = tx_tr_agg_clone.send(tr);
            }
            tracing::info!("trade inbound closed; trade duplicator exiting");
        });
    }

    // 4) Spawn HL feeds (multi-coin, real WS) - point them at the inbound senders
    spawn_hl_from_config(&cfg, tx_snap_in.clone(), tx_tr_in.clone()).await?;

    // 5) Spawn Deribit feeds if configured - also point at inbound senders
    if let Some(der_cfg) = &cfg.deribit {
        spawn_deribit_from_config(der_cfg, tx_snap_in.clone(), tx_tr_in.clone()).await?;
    }

    // 5b) Spawn Binance feeds if configured - also point at inbound senders
    if let Some(bin_cfg) = &cfg.binance {
        // spawn_binance_from_config is expected to spawn its own tasks and return quickly.
        spawn_binance_from_config(&cfg, bin_cfg, tx_snap_in.clone(), tx_tr_in.clone()).await?;
    }

    // 6) Attach sinks → writer (sink receivers)
    spawn_snapshot_sink(&cfg, rx_snap_sink, tx_writer.clone());
    spawn_trade_sink(&cfg, rx_tr_sink, tx_writer.clone());
    // combined ticks are optional; if you have a combined tick generator, hook it here.
    if cfg.mode.enable_combined_ticks {
        // if user later adds spawn_combined_sink in sinks.rs, they'd wire it in here.
        tracing::info!("combined tick emit enabled, but no combined sink was wired at startup");
    }

    // 7) Aggregator — produce a GlobalOrderBook record on updates (uses aggregator-facing receivers)
    aggregator::spawn_aggregator(&cfg, rx_snap_agg, rx_tr_agg, tx_writer.clone());

    // 8) Run forever (ctrl-c to exit)
    shutdown::graceful_shutdown().await;
    Ok(())
}
