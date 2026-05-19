use crate::config::Config;
use crate::db::Database;
use nostr_sdk::{Client, Event, EventId, Filter, Kind};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

fn relation_type_for_kind(kind: u16) -> &'static str {
    match kind {
        1 => "note",
        5 => "delete",
        7 => "reaction",
        1111 => "comment",
        9734 => "zap_request",
        9735 => "zap_receipt",
        _ => "note",
    }
}

pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 5: Starting related events collection");

    let related_kinds = vec![
        Kind::from(1),
        Kind::from(5),
        Kind::from(7),
        Kind::from(1111),
        Kind::from(9734),
        Kind::from(9735),
    ];

    let source_client = Client::default();
    for relay in &config.source_relays {
        source_client.add_relay(relay).await?;
    }
    source_client.connect().await;
    info!("Stage 5: Connected to {} source relay(s)", config.source_relays.len());

    let target_client = Client::default();
    for relay in &config.target_relays {
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;

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

    let all_relay_urls: Vec<String> = {
        let mut urls: Vec<String> = config.source_relays.clone();
        for url in &config.target_relays {
            if !urls.contains(url) {
                urls.push(url.clone());
            }
        }
        urls
    };
    let broadcast_client = Client::default();
    for relay in &all_relay_urls {
        broadcast_client.add_relay(relay).await?;
    }
    broadcast_client.connect().await;

    let batch_size = config.related_events_batch_size as i64;
    let mut offset: i64 = 0;
    let mut total_fetched = 0usize;
    let mut total_stored = 0usize;

    loop {
        let video_ids = db.get_video_event_ids_paginated(batch_size, offset).await?;
        if video_ids.is_empty() {
            break;
        }

        info!(
            "Stage 5: Processing batch of {} video events (offset {})",
            video_ids.len(),
            offset
        );

        let event_ids: Vec<EventId> = video_ids
            .iter()
            .filter_map(|id| EventId::from_hex(id).ok())
            .collect();

        let filter = Filter::new()
            .kinds(related_kinds.clone())
            .events(event_ids);

        let related_events = match tokio::time::timeout(
            Duration::from_secs(30),
            source_client.get_events_of(
                vec![filter],
                nostr_sdk::EventSource::relays(None),
            ),
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(e)) => {
                warn!("Stage 5: Relay query failed for batch at offset {}: {}", offset, e);
                offset += batch_size;
                continue;
            }
            Err(_) => {
                warn!("Stage 5: Relay query timed out for batch at offset {}", offset);
                offset += batch_size;
                continue;
            }
        };

        total_fetched += related_events.len();
        debug!(
            "Stage 5: Fetched {} related events for batch at offset {}",
            related_events.len(),
            offset
        );

        let mut delete_events: Vec<Event> = Vec::new();

        for event in related_events {
            let event_id_hex = event.id.to_hex();
            let kind = event.kind.as_u16();

            if db.event_exists(&event_id_hex).await? {
                debug!("Related event {} already in DB, skipping insert", event_id_hex);
            } else {
                let pubkey = event.pubkey.to_hex();
                let created_at = event.created_at.as_u64() as i64;
                let content = if event.content.is_empty() {
                    None
                } else {
                    Some(event.content.as_str())
                };
                let raw_event = serde_json::to_value(&event)?;

                db.insert_event(
                    &event_id_hex,
                    &pubkey,
                    kind as i32,
                    created_at,
                    content,
                    &raw_event,
                )
                .await?;

                total_stored += 1;
            }

            let relation = relation_type_for_kind(kind);
            for tag in event.tags.iter() {
                let tag_vec = tag.as_slice();
                if tag_vec.len() >= 2 && (tag_vec[0] == "e" || tag_vec[0] == "E") {
                    let referenced_id = tag_vec[1].as_str();
                    if video_ids.contains(&referenced_id.to_string()) {
                        db.insert_event_relation(&event_id_hex, referenced_id, relation)
                            .await
                            .ok();
                    }
                }
            }

            match target_client.send_event(event.clone()).await {
                Ok(output) => {
                    for relay_url in &output.success {
                        let url_str = relay_url.as_str();
                        let url_key = url_str.trim_end_matches('/');
                        if let Some(&relay_id) = target_relay_ids.get(url_key) {
                            db.insert_publication(&event_id_hex, relay_id).await.ok();
                        }
                    }
                }
                Err(e) => warn!("Stage 5: Failed to publish event {}: {}", event_id_hex, e),
            }

            if kind == 5 {
                delete_events.push(event);
            }
        }

        for event in delete_events {
            match broadcast_client.send_event(event.clone()).await {
                Ok(output) => {
                    info!(
                        "Stage 5: Broadcast delete event {} to {}/{} relays",
                        event.id,
                        output.success.len(),
                        all_relay_urls.len()
                    );
                }
                Err(e) => warn!("Stage 5: Delete broadcast failed for {}: {}", event.id, e),
            }
        }

        offset += batch_size;
    }

    source_client.disconnect().await?;
    target_client.disconnect().await?;
    broadcast_client.disconnect().await?;

    info!(
        "Stage 5: Completed — {} related events fetched, {} newly stored",
        total_fetched, total_stored
    );

    Ok(())
}
