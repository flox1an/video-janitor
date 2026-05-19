use crate::config::Config;
use crate::db::Database;
use crate::parser;
use nostr_sdk::{Client, Event};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub async fn run(
    config: Config,
    db: Database,
    mut event_rx: mpsc::Receiver<(Event, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 2: Starting event processing");

    // Register target relays in the relays table and cache their IDs
    let mut target_relay_ids: HashMap<String, i32> = HashMap::new();
    for relay_url in &config.target_relays {
        match db.upsert_relay(relay_url, "target").await {
            Ok(id) => {
                let key = relay_url.trim_end_matches('/').to_string();
                target_relay_ids.insert(key, id);
            }
            Err(e) => warn!("Failed to upsert target relay {}: {}", relay_url, e),
        }
    }

    info!(
        "Stage 2: Connecting to {} target relay(s)",
        config.target_relays.len()
    );
    let target_client = Client::default();
    for relay in &config.target_relays {
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;

    // Cache source relay IDs to avoid a DB round-trip per event
    let mut source_relay_id_cache: HashMap<String, i32> = HashMap::new();

    let mut processed_count = 0;
    let mut skipped_count = 0;
    let mut url_count = 0;

    while let Some((event, source_relay_url)) = event_rx.recv().await {
        debug!("Processing event: {}", event.id);

        if db.event_exists(&event.id.to_hex()).await? {
            debug!("Event {} already exists, skipping", event.id);
            skipped_count += 1;
            continue;
        }

        let urls = parser::extract_video_urls(&event);

        let event_id = event.id.to_hex();
        let pubkey = event.pubkey.to_hex();
        let kind = event.kind.as_u16() as i32;
        let created_at = event.created_at.as_u64() as i64;
        let content = if event.content.is_empty() {
            None
        } else {
            Some(event.content.as_str())
        };
        let raw_event = serde_json::to_value(&event)?;

        db.insert_event(&event_id, &pubkey, kind, created_at, content, &raw_event)
            .await?;

        // Record where we saw this event
        let source_relay_id = if let Some(&id) = source_relay_id_cache.get(&source_relay_url) {
            id
        } else {
            match db.upsert_relay(&source_relay_url, "source").await {
                Ok(id) => {
                    source_relay_id_cache.insert(source_relay_url.clone(), id);
                    id
                }
                Err(e) => {
                    warn!("Failed to upsert source relay {}: {}", source_relay_url, e);
                    -1
                }
            }
        };
        if source_relay_id >= 0 {
            db.insert_event_sighting(&event_id, source_relay_id)
                .await
                .ok();
        }

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

        debug!(
            "Forwarding event {} (kind {}) to target relays",
            event.id,
            event.kind.as_u16()
        );
        match target_client.send_event(event.clone()).await {
            Ok(output) => {
                for relay_url in &output.success {
                    let url_str = relay_url.to_string();
                    let url_key = url_str.trim_end_matches('/');
                    if let Some(&relay_id) = target_relay_ids.get(url_key) {
                        db.insert_publication(&event_id, relay_id).await.ok();
                    }
                }
                debug!("Forwarded event {} to {} relay(s)", event.id, output.success.len());
            }
            Err(e) => {
                warn!("Failed to forward event {} to target relays: {}", event.id, e);
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
        "Stage 2: Event processing completed — {} processed, {} skipped, {} URLs extracted",
        processed_count, skipped_count, url_count
    );

    Ok(())
}
