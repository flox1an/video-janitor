use crate::config::Config;
use crate::db::Database;
use crate::{stage1_collection, stage2_processing, stage3_validation, stage4_filter_generation};
use tokio::sync::mpsc;
use tracing::info;

/// Run the complete 4-stage pipeline
///
/// Stage 1: Event Collection - Fetch events from relays
/// Stage 2: Event Processing - Store in DB, forward to target relay
/// Stage 3: URL Validation - Check URL availability
/// Stage 4: Filter Generation - Create BinaryFuse16 filter of failed events
pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting VideoJanitor pipeline");

    // Create channel for events (Stage 1 -> Stage 2)
    let (event_tx, event_rx) = mpsc::channel(1000);

    // Spawn Stage 1 and Stage 2 concurrently
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

    // Wait for Stage 1 and Stage 2 to complete
    let (r1, r2) = tokio::try_join!(stage1, stage2)?;
    r1?;
    r2?;

    // Run Stage 3 after Stage 2 completes (queries database for pending URLs)
    info!("Stage 1 and 2 completed, starting Stage 3");
    stage3_validation::run(config.clone(), db.clone()).await?;

    // Run Stage 4 after Stage 3 completes (generates filter from fully failed events)
    info!("Stage 3 completed, starting Stage 4");
    stage4_filter_generation::run(config, db).await?;

    info!("Pipeline completed successfully");
    Ok(())
}
