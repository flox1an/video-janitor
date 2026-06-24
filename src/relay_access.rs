use crate::db::Database;
use nostr_sdk::{Client, Event, RelayMessage, RelayPoolNotification};
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

pub async fn add_and_connect(
    client: &Client,
    relay_url: &str,
    timeout: Duration,
) -> Result<(), String> {
    client.automatic_authentication(false);
    let mut notifications = client.notifications();

    client
        .add_relay(relay_url)
        .await
        .map_err(|e| format!("failed to add relay: {e}"))?;

    client.connect_with_timeout(timeout).await;

    if let Some(reason) =
        auth_challenge_reason(&mut notifications, relay_url, Duration::from_millis(1500)).await
    {
        return Err(reason);
    }

    let relay = client
        .relay(relay_url)
        .await
        .map_err(|e| format!("failed to inspect relay after connect: {e}"))?;

    if relay.is_connected().await {
        Ok(())
    } else {
        let status = relay.status().await;
        Err(format!(
            "relay not accessible after {}s connection timeout (status: {status})",
            timeout.as_secs()
        ))
    }
}

async fn auth_challenge_reason(
    notifications: &mut tokio::sync::broadcast::Receiver<RelayPoolNotification>,
    relay_url: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
        match tokio::time::timeout(remaining, notifications.recv()).await {
            Ok(Ok(RelayPoolNotification::Message {
                relay_url: notification_relay_url,
                message: RelayMessage::Auth { .. },
            })) if same_relay(notification_relay_url.as_str(), relay_url) => {
                return Some("relay requires NIP-42 authentication; signer not configured".into());
            }
            Ok(Ok(_)) => {}
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
            Ok(Err(_)) | Err(_) => return None,
        }
    }
}

fn same_relay(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
}

pub async fn disable_relay_read_write(db: &Database, relay_url: &str, reason: &str) {
    match db.disable_relay_read_write(relay_url, reason).await {
        Ok(_) => warn!(
            "Disabled relay reads and writes for {}: {}",
            relay_url, reason
        ),
        Err(e) => warn!(
            "Failed to disable relay {} after accessibility failure ({}): {}",
            relay_url, reason, e
        ),
    }
}

pub async fn disable_relay_write(db: &Database, relay_url: &str, reason: &str) {
    match db.disable_relay_write(relay_url, reason).await {
        Ok(_) => warn!("Disabled relay writes for {}: {}", relay_url, reason),
        Err(e) => warn!(
            "Failed to disable writes for relay {} after publish failure ({}): {}",
            relay_url, reason, e
        ),
    }
}

pub async fn disable_if_disconnected(
    client: &Client,
    db: &Database,
    relay_url: &str,
    reason: &str,
) -> bool {
    match client.relay(relay_url).await {
        Ok(relay) => {
            if relay.is_connected().await {
                false
            } else {
                let status = relay.status().await;
                let reason = format!("{reason}; relay status: {status}");
                disable_relay_read_write(db, relay_url, &reason).await;
                true
            }
        }
        Err(e) => {
            let reason = format!("{reason}; failed to inspect relay status: {e}");
            disable_relay_read_write(db, relay_url, &reason).await;
            true
        }
    }
}

pub fn write_disable_reason(error: &str) -> Option<String> {
    let normalized = error.to_ascii_lowercase();

    let event_specific = [
        "invalid:",
        "bad event",
        "tag val too large",
        "deleted:",
        "duplicate",
        "already",
        "pow",
        "rate-limited",
    ];
    if event_specific
        .iter()
        .any(|needle| normalized.contains(needle))
    {
        return None;
    }

    let write_policy = [
        "auth",
        "restricted",
        "blocked",
        "payment",
        "pay",
        "write disabled",
        "forbidden",
        "not allowed",
        "permission",
        "signer not configured",
    ];
    if write_policy
        .iter()
        .any(|needle| normalized.contains(needle))
    {
        Some(format!(
            "write disabled after relay publish rejection: {error}"
        ))
    } else {
        None
    }
}

pub async fn send_event_to_write_relays(
    db: &Database,
    client: &Client,
    relay_urls: &mut Vec<String>,
    relay_ids: &HashMap<String, i32>,
    event: &Event,
    event_id_hex: &str,
    context: &str,
) -> usize {
    let mut success_count = 0usize;

    let mut write_disabled_relays = Vec::new();

    for relay_url in relay_urls.clone() {
        match client
            .send_event_to([relay_url.as_str()], event.clone())
            .await
        {
            Ok(output) => {
                if !output.success.is_empty() {
                    success_count += output.success.len();
                    let url_key = relay_url.trim_end_matches('/');
                    if let Some(&relay_id) = relay_ids.get(url_key) {
                        db.insert_publication(event_id_hex, relay_id).await.ok();
                    }
                }

                for (failed_relay, error) in output.failed {
                    let reason = error.unwrap_or_else(|| "publish failed".to_string());
                    if let Some(disable_reason) = write_disable_reason(&reason) {
                        disable_relay_write(db, failed_relay.as_str(), &disable_reason).await;
                        write_disabled_relays.push(failed_relay.as_str().to_string());
                    } else {
                        warn!(
                            "{}: relay {} rejected event {} without disabling writes: {}",
                            context, failed_relay, event_id_hex, reason
                        );
                    }
                }
            }
            Err(e) => {
                let reason = e.to_string();
                if let Some(disable_reason) = write_disable_reason(&reason) {
                    disable_relay_write(db, &relay_url, &disable_reason).await;
                    write_disabled_relays.push(relay_url.clone());
                } else {
                    warn!(
                        "{}: relay {} rejected event {} without disabling writes: {}",
                        context, relay_url, event_id_hex, reason
                    );
                }
            }
        }
    }

    if !write_disabled_relays.is_empty() {
        relay_urls.retain(|relay_url| {
            !write_disabled_relays
                .iter()
                .any(|disabled| same_relay(disabled, relay_url))
        });
    }

    success_count
}
