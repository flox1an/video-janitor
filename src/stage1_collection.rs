use crate::config::{Config, SyncMode};
use crate::db::Database;
use crate::relay_access;
use nostr_sdk::{Client, Event, Filter, Kind, Timestamp};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};

/// Stage 1: Event Collection
///
/// Spawns parallel tasks to fetch video events from multiple relays.
/// Each relay is processed independently, implementing backfill or update mode
/// based on whether we've synced it before.
pub async fn run(
    config: Config,
    db: Database,
    event_tx: mpsc::Sender<(Event, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "Stage 1: Starting event collection from {} relays (concurrency: {})",
        config.source_relays.len(),
        config.relay_concurrency
    );

    let total_relays = config.source_relays.len();
    let semaphore = Arc::new(Semaphore::new(config.relay_concurrency));
    let completed = Arc::new(AtomicUsize::new(0));
    let total_events = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for relay_url in config.source_relays.clone() {
        let tx = event_tx.clone();
        let db = db.clone();
        let config = config.clone();
        let sem = semaphore.clone();
        let completed = completed.clone();
        let total_events = total_events.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await;
            info!(
                "[{}/{}] Connecting: {}",
                completed.load(Ordering::Relaxed) + 1,
                total_relays,
                relay_url
            );
            match sync_relay(&relay_url, &config, &db, tx).await {
                Ok(count) => {
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    let total = total_events.fetch_add(count, Ordering::Relaxed) + count;
                    info!(
                        "[{}/{}] Done: {} — {} events (running total: {})",
                        done, total_relays, relay_url, count, total
                    );
                }
                Err(e) => {
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    warn!("[{}/{}] Failed: {} — {}", done, total_relays, relay_url, e);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("Relay task panicked: {}", e);
        }
    }

    // Close channel to signal Stage 2 we're done
    drop(event_tx);

    info!("Stage 1: Event collection completed");
    Ok(())
}

/// Sync a single relay
async fn sync_relay(
    relay_url: &str,
    config: &Config,
    db: &Database,
    event_tx: mpsc::Sender<(Event, String)>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    info!("Syncing relay: {}", relay_url);

    // Check if we've synced this relay before
    let state = if config.sync_mode == SyncMode::Full {
        None
    } else {
        db.get_relay_state(relay_url).await?
    };

    let count = match state {
        None => {
            info!(
                "First sync/Full sync for relay {}, starting backfill",
                relay_url
            );
            backfill_relay(relay_url, config, db, event_tx).await?
        }
        Some(state) => {
            info!(
                "Incremental sync for relay {} from timestamp {}",
                relay_url, state.last_event_timestamp
            );
            fetch_new_events(relay_url, state.last_event_timestamp, config, db, event_tx).await?
        }
    };

    Ok(count)
}

/// Backfill mode: paginate backwards through all historical events
async fn backfill_relay(
    relay_url: &str,
    config: &Config,
    db: &Database,
    event_tx: mpsc::Sender<(Event, String)>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::default();
    if let Err(reason) =
        relay_access::add_and_connect(&client, relay_url, Duration::from_secs(10)).await
    {
        relay_access::disable_relay_read_write(db, relay_url, &reason).await;
        return Err(std::io::Error::other(reason).into());
    }

    let mut until = Timestamp::now();
    let mut total = 0;
    let mut latest_timestamp: Option<i64> = None;

    let video_kinds = vec![
        Kind::from(21),
        Kind::from(22),
        Kind::from(34235),
        Kind::from(34236),
    ];

    loop {
        info!(
            "Backfill batch for {}: until={}, total so far={}",
            relay_url,
            until.as_u64(),
            total
        );

        let filter = Filter::new()
            .kinds(video_kinds.clone())
            .until(until)
            .limit(config.backfill_batch_size);

        // Fetch events with timeout
        let events = match tokio::time::timeout(
            Duration::from_secs(30),
            client.get_events_of(vec![filter], nostr_sdk::EventSource::relays(None)),
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(e)) => {
                let reason = format!("backfill query failed: {e}");
                relay_access::disable_if_disconnected(&client, db, relay_url, &reason).await;
                return Err(std::io::Error::other(reason).into());
            }
            Err(_) => {
                let reason = "backfill query timed out after 30s".to_string();
                relay_access::disable_if_disconnected(&client, db, relay_url, &reason).await;
                return Err(std::io::Error::other(reason).into());
            }
        };

        if events.is_empty() || total >= config.backfill_max_events {
            info!(
                "Backfill complete for {}: {} total events",
                relay_url, total
            );
            break;
        }

        // Track latest timestamp
        for event in &events {
            let ts = event.created_at.as_u64() as i64;
            if latest_timestamp.is_none() || ts > latest_timestamp.unwrap() {
                latest_timestamp = Some(ts);
            }
        }

        // Send events to Stage 2
        for event in events {
            // Move cursor backward, subtract 1 to avoid fetching the same event twice
            // (because .until() is inclusive in Nostr protocol: created_at <= until)
            until = Timestamp::from(event.created_at.as_u64().saturating_sub(1));
            if let Err(e) = event_tx.send((event, relay_url.to_string())).await {
                warn!("Failed to send event to Stage 2: {}", e);
                break;
            }
            total += 1;
        }

        // Safety check
        if total >= config.backfill_max_events {
            warn!(
                "Hit backfill max limit for {}: {}",
                relay_url, config.backfill_max_events
            );
            break;
        }
    }

    // Save relay state — use latest found timestamp, or now() if relay had no events
    let ts = latest_timestamp.unwrap_or_else(|| Timestamp::now().as_u64() as i64);
    db.upsert_relay_state(relay_url, ts, total as i32).await?;

    client.disconnect().await?;
    Ok(total)
}

/// Update mode: fetch only new events since last sync
async fn fetch_new_events(
    relay_url: &str,
    since: i64,
    _config: &Config,
    db: &Database,
    event_tx: mpsc::Sender<(Event, String)>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::default();
    if let Err(reason) =
        relay_access::add_and_connect(&client, relay_url, Duration::from_secs(10)).await
    {
        relay_access::disable_relay_read_write(db, relay_url, &reason).await;
        return Err(std::io::Error::other(reason).into());
    }

    let video_kinds = vec![
        Kind::from(21),
        Kind::from(22),
        Kind::from(34235),
        Kind::from(34236),
    ];

    let filter = Filter::new()
        .kinds(video_kinds)
        .since(Timestamp::from(since as u64));

    info!("Fetching new events for {} since {}", relay_url, since);

    let events = match tokio::time::timeout(
        Duration::from_secs(30),
        client.get_events_of(vec![filter], nostr_sdk::EventSource::relays(None)),
    )
    .await
    {
        Ok(Ok(events)) => events,
        Ok(Err(e)) => {
            let reason = format!("incremental query failed: {e}");
            relay_access::disable_if_disconnected(&client, db, relay_url, &reason).await;
            return Err(std::io::Error::other(reason).into());
        }
        Err(_) => {
            let reason = "incremental query timed out after 30s".to_string();
            relay_access::disable_if_disconnected(&client, db, relay_url, &reason).await;
            return Err(std::io::Error::other(reason).into());
        }
    };

    let count = events.len();
    let mut latest_timestamp = since;

    // Send events to Stage 2
    for event in events {
        let ts = event.created_at.as_u64() as i64;
        if ts > latest_timestamp {
            latest_timestamp = ts;
        }

        if let Err(e) = event_tx.send((event, relay_url.to_string())).await {
            warn!("Failed to send event to Stage 2: {}", e);
            break;
        }
    }

    info!(
        "Incremental fetch complete for {}: {} new events",
        relay_url, count
    );

    // Update relay state
    if count > 0 {
        db.upsert_relay_state(relay_url, latest_timestamp, count as i32)
            .await?;
    }

    client.disconnect().await?;
    Ok(count)
}
