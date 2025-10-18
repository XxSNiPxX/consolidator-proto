pub async fn graceful_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received, exiting...");
}
