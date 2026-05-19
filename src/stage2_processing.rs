use crate::config::Config;
use crate::db::Database;
use crate::parser;
use nostr_sdk::{Client, Event};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Stage 2: Event Processing
///
/// Consumes events from Stage 1, extracts video URLs, stores in database,
/// and forwards events to target relays.
pub async fn run(
    config: Config,
    db: Database,
    mut event_rx: mpsc::Receiver<Event>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 2: Starting event processing");

    info!(
        "Stage 2: Connecting to {} target relay(s)",
        config.target_relays.len()
    );
    let target_client = Client::default();
    for relay in &config.target_relays {
        info!("Stage 2: Adding target relay: {}", relay);
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;
    info!("Stage 2: Target relays connected (or will retry on first send)");

    let mut processed_count = 0;
    let mut skipped_count = 0;
    let mut url_count = 0;

    while let Some(event) = event_rx.recv().await {
        debug!("Processing event: {}", event.id);

        // Skip if already in database
        if db.event_exists(&event.id.to_hex()).await? {
            debug!("Event {} already exists, skipping", event.id);
            skipped_count += 1;
            continue;
        }

        // Extract video URLs from imeta tags
        let urls = parser::extract_video_urls(&event);

        if urls.is_empty() {
            debug!("Event {} has no video URLs", event.id);
        }

        // Store event and URLs in database
        let event_id = event.id.to_hex();
        let pubkey = event.pubkey.to_hex();
        let kind = event.kind.as_u16() as i32;
        let created_at = event.created_at.as_u64() as i64;
        let content = if event.content.is_empty() {
            None
        } else {
            Some(event.content.as_str())
        };

        // Serialize event to JSON
        let raw_event = serde_json::to_value(&event)?;

        // Insert event
        db.insert_event(
            &event_id, &pubkey, kind, created_at, content, &raw_event,
            "unknown", // We don't track which specific relay in Stage 1 anymore
        )
        .await?;

        // Insert video URLs (will be validated by Stage 3)
        for url in urls {
            match db.insert_video_url(&url).await {
                Ok(id) => {
                    debug!("Inserted URL: {} (id={})", url.url, id);
                    url_count += 1;
                }
                Err(e) => {
                    warn!("Failed to insert URL {}: {}", url.url, e);
                }
            }
        }

        // Forward event to target relays (best-effort)
        debug!(
            "Forwarding event {} (kind {}) to target relays",
            event.id,
            event.kind.as_u16()
        );
        match target_client.send_event(event.clone()).await {
            Ok(output) => {
                debug!("Forwarded event {} -> {:?}", event.id, output);
            }
            Err(e) => {
                warn!(
                    "Failed to forward event {} to target relays: {}",
                    event.id, e
                );
            }
        }

        processed_count += 1;

        if processed_count % 25 == 0 {
            info!(
                "Stage 2: {} new events stored ({} skipped, {} URLs so far)",
                processed_count, skipped_count, url_count
            );
        }
    }

    target_client.disconnect().await?;

    info!(
        "Stage 2: Event processing completed - {} processed, {} skipped, {} URLs extracted",
        processed_count, skipped_count, url_count
    );

    Ok(())
}
