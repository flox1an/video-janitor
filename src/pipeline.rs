use crate::config::Config;
use crate::db::Database;
use crate::{
    stage1_collection, stage2_processing, stage3_validation, stage4_filter_generation,
    stage5_related_events,
};
use tokio::sync::mpsc;
use tracing::info;

pub async fn run(
    mut config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting VideoJanitor pipeline");

    // Register all configured relays upfront so they appear in the DB regardless of event count.
    // This must happen before the active-relay query below.
    for relay_url in &config.source_relays {
        if let Err(e) = db.upsert_relay(relay_url, "source").await {
            tracing::warn!("Failed to register source relay {}: {}", relay_url, e);
        }
    }
    for relay_url in &config.target_relays {
        if let Err(e) = db.upsert_relay(relay_url, "target").await {
            tracing::warn!("Failed to register target relay {}: {}", relay_url, e);
        }
    }

    let active_target_relays = db
        .get_write_enabled_relay_urls(&config.target_relays)
        .await?;
    let disabled_target_count = config
        .target_relays
        .len()
        .saturating_sub(active_target_relays.len());
    if disabled_target_count > 0 {
        let disabled: Vec<&str> = config
            .target_relays
            .iter()
            .filter(|r| !active_target_relays.contains(r))
            .map(String::as_str)
            .collect();
        info!(
            "Pipeline: skipping {} disabled target relay(s): {:?}",
            disabled_target_count, disabled
        );
    }
    config.target_relays = active_target_relays;

    // Replace the config's source relay list with only DB read-enabled relays.
    // Relays marked read_enabled = FALSE in the database are skipped even if they
    // are still listed in SOURCE_RELAYS / SOURCE_RELAYS_FILE.
    let active_source_relays = db.get_read_enabled_source_relay_urls().await?;
    let disabled_count = config
        .source_relays
        .len()
        .saturating_sub(active_source_relays.len());
    if disabled_count > 0 {
        let disabled: Vec<&str> = config
            .source_relays
            .iter()
            .filter(|r| !active_source_relays.contains(r))
            .map(String::as_str)
            .collect();
        info!(
            "Pipeline: skipping {} disabled relay(s): {:?}",
            disabled_count, disabled
        );
    }
    config.source_relays = active_source_relays;

    if config.source_relays.is_empty() {
        info!("Pipeline: no active source relays — skipping collection stages");
        return Ok(());
    }

    let (event_tx, event_rx) = mpsc::channel::<(nostr_sdk::Event, String)>(1000);

    let stage1 = tokio::spawn({
        let config = config.clone();
        let db = db.clone();
        async move { stage1_collection::run(config, db, event_tx).await }
    });

    let stage2 = tokio::spawn({
        let config = config.clone();
        let db = db.clone();
        async move { stage2_processing::run(config, db, event_rx).await }
    });

    let (r1, r2) = tokio::try_join!(stage1, stage2)?;
    r1?;
    r2?;

    info!("Stages 1 and 2 completed, sweeping expired events");
    match db.mark_expired_events().await {
        Ok(0) => {}
        Ok(n) => info!("Swept {} event(s) past their NIP-40 expiration", n),
        Err(e) => tracing::warn!("Failed to sweep expired events: {}", e),
    }

    info!("Starting Stage 3");
    stage3_validation::run(config.clone(), db.clone()).await?;

    info!("Stage 3 completed, starting Stage 4");
    stage4_filter_generation::run(config.clone(), db.clone()).await?;

    info!("Stage 4 completed, starting Stage 5 (related events)");
    stage5_related_events::run(config, db).await?;

    info!("Pipeline completed successfully");
    Ok(())
}
