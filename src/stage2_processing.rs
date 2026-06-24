use crate::config::Config;
use crate::db::Database;
use crate::parser;
use crate::relay_access;
use nostr_sdk::{Client, Event};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const ADDRESSABLE_KINDS: [u16; 2] = [34235, 34236];

pub async fn run(
    config: Config,
    db: Database,
    mut event_rx: mpsc::Receiver<(Event, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 2: Starting event processing");

    for relay_url in &config.target_relays {
        if let Err(e) = db.upsert_relay(relay_url, "target").await {
            warn!("Failed to upsert target relay {}: {}", relay_url, e);
        }
    }

    // Keep only active targets. Disabled relays are never used for broadcasting.
    let mut active_target_relays = db
        .get_write_enabled_relay_urls(&config.target_relays)
        .await?;

    let mut target_relay_ids: HashMap<String, i32> = HashMap::new();
    for relay_url in &active_target_relays {
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
        active_target_relays.len()
    );
    let target_client = Client::default();
    for relay in &active_target_relays {
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;

    // Cache source relay IDs to avoid a DB round-trip per event
    let mut source_relay_id_cache: HashMap<String, i32> = HashMap::new();

    let mut processed_count = 0;
    let mut skipped_count = 0;
    let mut url_count = 0;
    while let Some((event, source_relay_url)) = event_rx.recv().await {
        let event_id = event.id.to_hex();

        if !db.event_exists(&event_id).await? {
            let kind_u16 = event.kind.as_u16();
            let kind = kind_u16 as i32;
            let pubkey = event.pubkey.to_hex();
            let created_at = event.created_at.as_u64() as i64;
            let now = chrono::Utc::now();

            // --- NIP-40: expiration ---
            let expires_at = parser::extract_expiration(&event);
            let lifecycle_status = if expires_at.map_or(false, |exp| exp <= now) {
                "expired"
            } else {
                "active"
            };

            // --- Addressable-event replacement (kinds 34235/34236) ---
            // Extract d-tag as owned String now so we don't hold a borrow on `event`
            // across the subsequent async DB calls.
            let identifier: Option<String> = if ADDRESSABLE_KINDS.contains(&kind_u16) {
                parser::extract_d_tag(&event).map(str::to_owned)
            } else {
                None
            };

            if lifecycle_status == "active" {
                if let Some(ref ident) = identifier {
                    match db
                        .get_active_addressable_event(&pubkey, kind, ident)
                        .await?
                    {
                        Some((old_id, old_ts)) if created_at <= old_ts => {
                            // Existing event is the same age or newer — discard.
                            debug!(
                                "Event {} (kind {}) superseded by existing {}; discarding",
                                event_id, kind_u16, old_id
                            );
                            skipped_count += 1;
                            // Still record the sighting below.
                            // Jump to sighting logic by going to the else branch.
                            // We use a flag to skip insert/forward cleanly.
                            let source_relay_id = if let Some(&id) =
                                source_relay_id_cache.get(&source_relay_url)
                            {
                                id
                            } else {
                                match db.upsert_relay(&source_relay_url, "source").await {
                                    Ok(id) => {
                                        source_relay_id_cache.insert(source_relay_url.clone(), id);
                                        id
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to upsert source relay {}: {}",
                                            source_relay_url, e
                                        );
                                        -1
                                    }
                                }
                            };
                            if source_relay_id >= 0 {
                                db.insert_event_sighting(&event_id, source_relay_id)
                                    .await
                                    .ok();
                            }
                            continue;
                        }
                        Some((old_id, _)) => {
                            // This event is newer — retire the old one first so the
                            // address index stays consistent before we insert.
                            debug!("Event {} (kind {}) replaces {}", event_id, kind_u16, old_id);
                            db.mark_event_replaced(&old_id).await?;
                        }
                        None => {}
                    }
                }
            }

            let urls = parser::extract_video_urls(&event);
            let content = if event.content.is_empty() {
                None
            } else {
                Some(event.content.as_str())
            };
            let raw_event = serde_json::to_value(&event)?;

            db.insert_event(
                &event_id,
                &pubkey,
                kind,
                created_at,
                content,
                &raw_event,
                lifecycle_status,
                expires_at,
                identifier.as_deref(),
            )
            .await?;

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

            // Only forward active events — expired events are not re-publishable.
            if lifecycle_status == "active" {
                debug!(
                    "Forwarding event {} (kind {}) to target relays",
                    event.id,
                    event.kind.as_u16()
                );
                let success_count = relay_access::send_event_to_write_relays(
                    &db,
                    &target_client,
                    &mut active_target_relays,
                    &target_relay_ids,
                    &event,
                    &event_id,
                    "Stage 2 forward",
                )
                .await;
                debug!("Forwarded event {} to {} relay(s)", event.id, success_count);
            } else {
                debug!(
                    "Skipping forward for event {} — lifecycle_status={}",
                    event_id, lifecycle_status
                );
            }

            processed_count += 1;
            if processed_count == 1 {
                info!("Stage 2: First new event stored — pipeline is flowing");
            }
            if processed_count % 10 == 0 {
                info!(
                    "Stage 2: {} new events stored ({} skipped, {} URLs so far)",
                    processed_count, skipped_count, url_count
                );
            }
        } else {
            debug!(
                "Event {} already exists, skipping metadata/forwarding",
                event.id
            );
            skipped_count += 1;
        }

        // Record where we saw this event (must happen after the event is in the database due to foreign key constraint)
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
    }

    target_client.disconnect().await?;

    info!(
        "Stage 2: Event processing completed — {} processed, {} skipped, {} URLs extracted",
        processed_count, skipped_count, url_count
    );

    Ok(())
}
