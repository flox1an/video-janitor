use crate::config::Config;
use crate::db::Database;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use tracing::{info, warn};
use xorf::BinaryFuse16;

#[derive(Serialize)]
struct BinaryFuseFilterJson {
    seed: u64,
    segment_length: u32,
    segment_length_mask: u32,
    segment_count_length: u32,
    fingerprints: Vec<u16>,
}

#[derive(Serialize)]
struct FilterOutput {
    filter_type: String,
    filter_base64: String,
    filter_json: BinaryFuseFilterJson,
    event_count: usize,
    generated_at: String,
}

/// Stage 4: Filter Generation
///
/// Queries the database for events where all video URLs have failed,
/// builds a BinaryFuse16 filter from the event IDs, and writes it to disk
/// as a base64-encoded JSON file.
pub async fn run(
    _config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 4: Starting filter generation for fully failed events");

    // Fetch all event IDs where all URLs have failed
    let failed_event_ids = db.get_fully_failed_event_ids().await?;

    if failed_event_ids.is_empty() {
        info!("Stage 4: No fully failed events found");
        return Ok(());
    }

    info!(
        "Stage 4: Found {} events with all URLs failed",
        failed_event_ids.len()
    );

    // Convert event IDs to u64 hashes for the filter
    let hashes: Vec<u64> = failed_event_ids
        .iter()
        .map(|id| {
            let mut hasher = DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish()
        })
        .collect();

    // Build the BinaryFuse16 filter
    info!(
        "Stage 4: Building BinaryFuse16 filter with {} entries",
        hashes.len()
    );
    let filter = match BinaryFuse16::try_from(&hashes) {
        Ok(f) => f,
        Err(e) => {
            warn!("Stage 4: Failed to build filter: {}", e);
            return Err(format!("Failed to build BinaryFuse16 filter: {}", e).into());
        }
    };

    // Serialize the filter to bytes using bincode (with serde support)
    let filter_bytes = bincode::serialize(&filter)?;

    // Base64 encode the filter
    let filter_base64 = BASE64.encode(&filter_bytes);

    // Create plain JSON representation of the filter
    // Extract descriptor fields by serializing/deserializing the filter
    let filter_serde_value = serde_json::to_value(&filter)?;
    let seed = filter_serde_value["seed"]
        .as_u64()
        .ok_or("Missing seed field")?;
    let segment_length = filter_serde_value["segment_length"]
        .as_u64()
        .ok_or("Missing segment_length")? as u32;
    let segment_length_mask = filter_serde_value["segment_length_mask"]
        .as_u64()
        .ok_or("Missing segment_length_mask")? as u32;
    let segment_count_length = filter_serde_value["segment_count_length"]
        .as_u64()
        .ok_or("Missing segment_count_length")? as u32;

    let filter_json = BinaryFuseFilterJson {
        seed,
        segment_length,
        segment_length_mask,
        segment_count_length,
        fingerprints: filter.fingerprints.to_vec(),
    };

    // Create output structure
    let output = FilterOutput {
        filter_type: "BinaryFuse16".to_string(),
        filter_base64,
        filter_json,
        event_count: failed_event_ids.len(),
        generated_at: chrono::Utc::now().to_rfc3339(),
    };

    // Write to JSON file
    let output_path = "failed_events_filter.json";
    let json_output = serde_json::to_string_pretty(&output)?;
    fs::write(output_path, json_output)?;

    info!(
        "Stage 4: Filter generation completed. Written to {}",
        output_path
    );
    info!("  - Event count: {}", output.event_count);
    info!(
        "  - Filter size: {} bytes (base64 encoded)",
        output.filter_base64.len()
    );

    Ok(())
}
