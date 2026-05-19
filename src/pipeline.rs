use crate::config::Config;
use crate::db::Database;
use crate::{stage1_collection, stage2_processing, stage3_validation, stage4_filter_generation, stage5_related_events};
use tokio::sync::mpsc;
use tracing::info;

pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting VideoJanitor pipeline");

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

    info!("Stages 1 and 2 completed, starting Stage 3");
    stage3_validation::run(config.clone(), db.clone()).await?;

    info!("Stage 3 completed, starting Stage 4");
    stage4_filter_generation::run(config.clone(), db.clone()).await?;

    info!("Stage 4 completed, starting Stage 5 (related events)");
    stage5_related_events::run(config, db).await?;

    info!("Pipeline completed successfully");
    Ok(())
}
