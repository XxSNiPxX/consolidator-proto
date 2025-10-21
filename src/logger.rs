// src/logger.rs (patched)
use crate::config::LoggingCfg;
use anyhow::Result;
use once_cell::sync::OnceCell;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry, fmt}; // <-- bridge log -> tracing

// keep the file guard alive so the appender background thread doesn't exit
static GUARD: OnceCell<WorkerGuard> = OnceCell::new();

pub fn init_logger(cfg: &LoggingCfg) -> Result<()> {
    // ensure log dir exists
    let path = std::path::Path::new(&cfg.path);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }

    // file appender (non-blocking) + keep guard alive
    let file_appender = tracing_appender::rolling::never(
        path.parent().unwrap_or_else(|| std::path::Path::new(".")),
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("consolidator.log")),
    );
    let (nb_file, guard) = tracing_appender::non_blocking(file_appender);
    let _ = GUARD.set(guard);

    // Bridge `log` macros into `tracing` so log::debug! works
    let _ = LogTracer::init();

    // level / filter:
    // prefer RUST_LOG if set, otherwise fall back to cfg.level
    let level = match cfg.level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => cfg.level.clone(),
        _ => "info".to_string(),
    };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    // Build subscriber depending on format and whether stdout is enabled.
    if cfg.also_stdout.unwrap_or(false) {
        if cfg.format.eq_ignore_ascii_case("json") {
            let subscriber = Registry::default()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .with_writer(nb_file.clone())
                        .json()
                        .with_timer(SystemTime),
                )
                .with(
                    fmt::layer()
                        .with_writer(std::io::stdout)
                        .json()
                        .with_timer(SystemTime),
                );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|e| anyhow::anyhow!("failed to set global tracing subscriber: {}", e))?;
        } else {
            let subscriber = Registry::default()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .with_writer(nb_file.clone())
                        .compact()
                        .with_timer(SystemTime),
                )
                .with(
                    fmt::layer()
                        .with_writer(std::io::stdout)
                        .compact()
                        .with_timer(SystemTime),
                );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|e| anyhow::anyhow!("failed to set global tracing subscriber: {}", e))?;
        }
    } else {
        if cfg.format.eq_ignore_ascii_case("json") {
            let subscriber = Registry::default().with(env_filter).with(
                fmt::layer()
                    .with_writer(nb_file)
                    .json()
                    .with_timer(SystemTime),
            );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|e| anyhow::anyhow!("failed to set global tracing subscriber: {}", e))?;
        } else {
            let subscriber = Registry::default().with(env_filter).with(
                fmt::layer()
                    .with_writer(nb_file)
                    .compact()
                    .with_timer(SystemTime),
            );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|e| anyhow::anyhow!("failed to set global tracing subscriber: {}", e))?;
        }
    }

    // log one confirmation (this will respect the filter above)
    tracing::info!("logging initialized → file={}", cfg.path);
    Ok(())
}
