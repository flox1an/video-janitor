use crate::config::{Config, SyncMode};
use crate::db::Database;
use crate::relay_access;
use nostr_sdk::{Alphabet, Client, Event, EventId, Filter, Kind, SingleLetterTag, Timestamp};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{info, warn};

fn relation_type_for_kind(kind: u16) -> &'static str {
    match kind {
        1 => "note",
        5 => "delete",
        7 => "reaction",
        1111 => "comment",
        1985 => "label",
        9734 => "zap_request",
        9735 => "zap_receipt",
        1063 => "file_metadata",
        _ => "note",
    }
}

pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "Stage 5: Starting related events collection (mode: {:?})",
        config.sync_mode
    );
    let run_start_time = Timestamp::now().as_u64() as i64;

    let related_kinds = vec![
        Kind::from(1),
        Kind::from(5),
        Kind::from(7),
        Kind::from(1111),
        Kind::from(1985),
        Kind::from(1063),
        Kind::from(9734),
        Kind::from(9735),
    ];

    // Only process relays for which we actually have video event sightings.
    let relays = db.get_relays_with_video_sightings().await?;
    if relays.is_empty() {
        info!("Stage 5: No sightings found, nothing to do");
        return Ok(());
    }
    info!("Stage 5: {} relay(s) with video sightings", relays.len());

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
    let mut active_target_relays = db
        .get_write_enabled_relay_urls(&config.target_relays)
        .await?;

    // Target client for publishing related events (stays open for the whole stage).
    let target_client = Client::default();
    for relay in &active_target_relays {
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;

    let batch_size = config.related_events_batch_size as i64;
    let mut total_fetched = 0usize;
    let mut total_stored = 0usize;
    let mut all_delete_events: Vec<Event> = Vec::new();

    for (relay_id, relay_url) in &relays {
        info!(
            "Stage 5: Connecting to relay {} (id={})",
            relay_url, relay_id
        );

        let source_client = Client::default();
        if let Err(reason) =
            relay_access::add_and_connect(&source_client, relay_url, Duration::from_secs(10)).await
        {
            relay_access::disable_relay_read_write(&db, relay_url, &reason).await;
            warn!("Stage 5: Skipping disabled relay {}: {}", relay_url, reason);
            continue;
        }

        let mut offset: i64 = 0;
        let mut relay_fetched = 0usize;
        let mut relay_stored = 0usize;

        loop {
            let video_refs = db
                .get_video_event_references_for_relay_paginated(*relay_id, batch_size, offset)
                .await?;
            if video_refs.is_empty() {
                break;
            }

            let video_ids: Vec<String> = video_refs.iter().map(|v| v.event_id.clone()).collect();
            let video_id_set: HashSet<String> = video_ids.iter().cloned().collect();
            let address_to_event_id: HashMap<String, String> = video_refs
                .iter()
                .filter_map(|v| v.address().map(|address| (address, v.event_id.clone())))
                .collect();
            let mut hash_to_event_ids: HashMap<String, Vec<String>> = HashMap::new();
            for video_ref in &video_refs {
                for hash in &video_ref.video_hashes {
                    hash_to_event_ids
                        .entry(hash.clone())
                        .or_default()
                        .push(video_ref.event_id.clone());
                }
            }
            let video_hashes: Vec<String> = hash_to_event_ids.keys().cloned().collect();

            info!(
                "Stage 5: [{}] Querying related events for {} video events, {} addressable references, {} video hashes (offset {})",
                relay_url,
                video_ids.len(),
                address_to_event_id.len(),
                video_hashes.len(),
                offset
            );

            let event_ids: Vec<EventId> = video_ids
                .iter()
                .filter_map(|id| EventId::from_hex(id).ok())
                .collect();

            // Fetch last sync timestamps for these video IDs
            let sync_states = db
                .get_relay_video_sync_states(*relay_id, &video_ids)
                .await?;

            // Group video IDs by their sync timestamp
            let mut groups: HashMap<i64, Vec<EventId>> = HashMap::new();
            for id in &event_ids {
                let id_str = id.to_hex();
                let ts = sync_states.get(&id_str).copied().unwrap_or(0);
                groups.entry(ts).or_default().push(*id);
            }

            let mut filters = Vec::new();
            for (ts, ids) in groups {
                let mut lower_e_filter = Filter::new()
                    .kinds(related_kinds.clone())
                    .events(ids.clone());
                let mut upper_e_filter = Filter::new()
                    .kinds(related_kinds.clone())
                    .custom_tag(SingleLetterTag::uppercase(Alphabet::E), ids);
                if ts > 0 && config.sync_mode == SyncMode::Incremental {
                    let since = Timestamp::from(ts as u64);
                    lower_e_filter = lower_e_filter.since(since);
                    upper_e_filter = upper_e_filter.since(since);
                }
                filters.push(lower_e_filter);
                filters.push(upper_e_filter);
            }

            if !video_hashes.is_empty() {
                let mut hash_groups: HashMap<i64, Vec<String>> = HashMap::new();
                for video_ref in &video_refs {
                    let ts = sync_states
                        .get(&video_ref.event_id)
                        .copied()
                        .unwrap_or_default();
                    let entry = hash_groups.entry(ts).or_default();
                    entry.extend(video_ref.video_hashes.iter().cloned());
                }

                for (ts, mut hashes) in hash_groups {
                    hashes.sort();
                    hashes.dedup();

                    let mut x_filter = Filter::new()
                        .kinds(vec![Kind::from(1063)])
                        .custom_tag(SingleLetterTag::lowercase(Alphabet::X), hashes);

                    if ts > 0 && config.sync_mode == SyncMode::Incremental {
                        x_filter = x_filter.since(Timestamp::from(ts as u64));
                    }

                    filters.push(x_filter);
                }
            }

            let addresses: Vec<String> = address_to_event_id.keys().cloned().collect();
            if !addresses.is_empty() {
                let min_sync_ts = sync_states.values().copied().min().unwrap_or(0);
                let mut lower_a_filter = Filter::new()
                    .kinds(related_kinds.clone())
                    .custom_tag(SingleLetterTag::lowercase(Alphabet::A), addresses.clone());
                let mut upper_a_filter = Filter::new()
                    .kinds(related_kinds.clone())
                    .custom_tag(SingleLetterTag::uppercase(Alphabet::A), addresses);

                if min_sync_ts > 0 && config.sync_mode == SyncMode::Incremental {
                    let since = Timestamp::from(min_sync_ts as u64);
                    lower_a_filter = lower_a_filter.since(since);
                    upper_a_filter = upper_a_filter.since(since);
                }

                filters.push(lower_a_filter);
                filters.push(upper_a_filter);
            }

            let related_events = match tokio::time::timeout(
                Duration::from_secs(30),
                source_client.get_events_of(filters, nostr_sdk::EventSource::relays(None)),
            )
            .await
            {
                Ok(Ok(events)) => events,
                Ok(Err(e)) => {
                    let reason = format!("Stage 5 query failed at offset {offset}: {e}");
                    if relay_access::disable_if_disconnected(
                        &source_client,
                        &db,
                        relay_url,
                        &reason,
                    )
                    .await
                    {
                        warn!("Stage 5: [{}] Disabled relay: {}", relay_url, reason);
                        break;
                    }
                    warn!("Stage 5: [{}] {}", relay_url, reason);
                    offset += batch_size;
                    continue;
                }
                Err(_) => {
                    let reason = format!("Stage 5 query timed out after 30s at offset {offset}");
                    if relay_access::disable_if_disconnected(
                        &source_client,
                        &db,
                        relay_url,
                        &reason,
                    )
                    .await
                    {
                        warn!("Stage 5: [{}] Disabled relay: {}", relay_url, reason);
                        break;
                    }
                    warn!("Stage 5: [{}] {}", relay_url, reason);
                    offset += batch_size;
                    continue;
                }
            };

            let batch_len = related_events.len();
            relay_fetched += batch_len;
            total_fetched += batch_len;

            let mut batch_stored = 0usize;
            let mut batch_relations = 0usize;

            for event in related_events {
                let event_id_hex = event.id.to_hex();
                let kind = event.kind.as_u16();

                if !db.event_exists(&event_id_hex).await? {
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
                        "active",
                        None,
                        None,
                    )
                    .await?;

                    total_stored += 1;
                    relay_stored += 1;
                    batch_stored += 1;
                }

                let relation = relation_type_for_kind(kind);
                for tag in event.tags.iter() {
                    let tag_vec = tag.as_slice();
                    if tag_vec.len() >= 2 && (tag_vec[0] == "e" || tag_vec[0] == "E") {
                        let referenced_id = tag_vec[1].as_str();
                        if video_id_set.contains(referenced_id) {
                            if db
                                .insert_event_relation(&event_id_hex, referenced_id, relation)
                                .await
                                .is_ok()
                            {
                                batch_relations += 1;
                            }
                        }
                    } else if tag_vec.len() >= 2 && (tag_vec[0] == "a" || tag_vec[0] == "A") {
                        let referenced_address = tag_vec[1].as_str();
                        if let Some(video_event_id) = address_to_event_id.get(referenced_address) {
                            if db
                                .insert_event_relation(&event_id_hex, video_event_id, relation)
                                .await
                                .is_ok()
                            {
                                batch_relations += 1;
                            }
                        }
                    } else if kind == 1063
                        && tag_vec.len() >= 2
                        && (tag_vec[0] == "x" || tag_vec[0] == "ox")
                    {
                        let referenced_hash = tag_vec[1].as_str();
                        if let Some(video_event_ids) = hash_to_event_ids.get(referenced_hash) {
                            for video_event_id in video_event_ids {
                                if db
                                    .insert_event_relation(&event_id_hex, video_event_id, relation)
                                    .await
                                    .is_ok()
                                {
                                    batch_relations += 1;
                                }
                            }
                        }
                    }
                }

                relay_access::send_event_to_write_relays(
                    &db,
                    &target_client,
                    &mut active_target_relays,
                    &target_relay_ids,
                    &event,
                    &event_id_hex,
                    "Stage 5 publish",
                )
                .await;

                if kind == 5 {
                    all_delete_events.push(event);
                }
            }

            info!(
                "Stage 5: [{}] offset={} — {} fetched, {} newly stored, {} relations linked",
                relay_url, offset, batch_len, batch_stored, batch_relations
            );

            // Update sync states for this batch of video IDs on this relay
            if let Err(e) = db
                .upsert_relay_video_sync_states(*relay_id, &video_ids, run_start_time)
                .await
            {
                warn!(
                    "Stage 5: [{}] Failed to update sync states for batch: {}",
                    relay_url, e
                );
            }

            offset += batch_size;
        }

        info!(
            "Stage 5: [{}] done — {} fetched, {} stored",
            relay_url, relay_fetched, relay_stored
        );

        let _ = source_client.disconnect().await;
    }

    if !all_delete_events.is_empty() {
        let mut unique_deletes = HashMap::new();
        for event in all_delete_events {
            unique_deletes.insert(event.id, event);
        }

        info!(
            "Stage 5: Broadcasting {} unique delete event(s) to target and seen relays...",
            unique_deletes.len()
        );

        let broadcast_candidates: Vec<String> = {
            let mut urls: Vec<String> = relays.iter().map(|(_, url)| url.clone()).collect();
            for url in &active_target_relays {
                if !urls.contains(url) {
                    urls.push(url.clone());
                }
            }
            urls
        };
        let mut all_relay_urls = db
            .get_write_enabled_relay_urls(&broadcast_candidates)
            .await?;

        let broadcast_client = Client::default();
        for relay in &all_relay_urls {
            if let Err(e) = broadcast_client.add_relay(relay).await {
                warn!(
                    "Stage 5: Failed to add relay {} for delete broadcast: {}",
                    relay, e
                );
            }
        }
        broadcast_client.connect().await;

        let empty_relay_ids = HashMap::new();
        for event in unique_deletes.into_values() {
            let event_id_hex = event.id.to_hex();
            let success_count = relay_access::send_event_to_write_relays(
                &db,
                &broadcast_client,
                &mut all_relay_urls,
                &empty_relay_ids,
                &event,
                &event_id_hex,
                "Stage 5 delete broadcast",
            )
            .await;
            info!(
                "Stage 5: Broadcast delete {} to {}/{} relays",
                event.id,
                success_count,
                all_relay_urls.len()
            );
        }

        if let Err(e) = broadcast_client.disconnect().await {
            warn!("Stage 5: Failed to disconnect broadcast client: {}", e);
        }
    }

    // Mark video events as deleted where a same-pubkey kind:5 event references them.
    // This runs after all relays have been processed and all delete events are stored.
    match db.mark_events_deleted().await {
        Ok(0) => {}
        Ok(n) => info!("Stage 5: Marked {} video event(s) as deleted", n),
        Err(e) => warn!("Stage 5: Failed to mark deleted events: {}", e),
    }

    target_client.disconnect().await?;

    info!(
        "Stage 5: Completed — {}/{} sighted relays processed, {} related events fetched, {} newly stored",
        relays.len(),
        relays.len(),
        total_fetched,
        total_stored
    );

    Ok(())
}
