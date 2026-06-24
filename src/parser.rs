use crate::db::VideoUrl;
use nostr_sdk::Event;

/// Extract video URLs from imeta tags in a Nostr event
///
/// Nostr video events contain imeta tags with structure per NIP-92:
/// ["imeta", "url https://...", "m video/mp4", ...]
/// Each entry after "imeta" is a space-delimited key/value pair.
///
/// This function:
/// 1. Iterates through all tags
/// 2. Finds tags starting with "imeta"
/// 3. Extracts "url" and "m" (mime type) values from space-delimited pairs
/// 4. Filters for mime types starting with "video/"
/// 5. Returns all matching URLs
pub fn extract_video_urls(event: &Event) -> Vec<VideoUrl> {
    let mut urls = Vec::new();

    for tag in event.tags.iter() {
        let tag_vec = tag.as_slice();

        // Check if this is an imeta tag
        if tag_vec.is_empty() || tag_vec[0] != "imeta" {
            continue;
        }

        let mut url: Option<String> = None;
        let mut mime_type: Option<String> = None;

        // Parse space-delimited key-value pairs in the tag
        // Format per NIP-92: ["imeta", "url <value>", "m <value>", ...]
        for i in 1..tag_vec.len() {
            let entry = tag_vec[i].as_str();

            // Split on first space to get key and value
            if let Some(space_pos) = entry.find(' ') {
                let key = &entry[..space_pos];
                let value = &entry[space_pos + 1..];

                match key {
                    "url" => url = Some(value.to_string()),
                    "m" => mime_type = Some(value.to_string()),
                    _ => {} // Ignore other keys
                }
            }
        }

        // Only include if we have both URL and mime type, and mime type is video
        if let (Some(u), Some(m)) = (url, mime_type) {
            if m.starts_with("video/") {
                urls.push(VideoUrl {
                    id: None, // Will be set by database
                    event_id: event.id.to_hex(),
                    url: u,
                    url_type: "original".to_string(),
                    mime_type: Some(m),
                });
            }
        }
    }

    urls
}

/// Extract NIP-92/NIP-94 `x` and `ox` hashes from video `imeta` tags.
///
/// Hashes are only returned for `imeta` entries whose MIME type starts with `video/`.
pub fn extract_video_hashes(event: &Event) -> Vec<String> {
    let mut hashes = Vec::new();

    for tag in event.tags.iter() {
        let tag_vec = tag.as_slice();

        if tag_vec.is_empty() || tag_vec[0] != "imeta" {
            continue;
        }

        let mut mime_type: Option<&str> = None;
        let mut tag_hashes = Vec::new();

        for entry in tag_vec.iter().skip(1).map(|field| field.as_str()) {
            if let Some(space_pos) = entry.find(' ') {
                let key = &entry[..space_pos];
                let value = &entry[space_pos + 1..];

                match key {
                    "m" => mime_type = Some(value),
                    "x" | "ox" if !value.is_empty() => tag_hashes.push(value.to_string()),
                    _ => {}
                }
            }
        }

        if mime_type.is_some_and(|m| m.starts_with("video/")) {
            hashes.extend(tag_hashes);
        }
    }

    hashes.sort();
    hashes.dedup();
    hashes
}

/// Extract the NIP-40 expiration timestamp from an event's tags, if present.
///
/// Returns `None` when there is no `expiration` tag or the value is not a valid i64.
pub fn extract_expiration(event: &Event) -> Option<chrono::DateTime<chrono::Utc>> {
    for tag in event.tags.iter() {
        let v = tag.as_slice();
        if v.len() >= 2 && v[0] == "expiration" {
            if let Ok(ts) = v[1].parse::<i64>() {
                return chrono::DateTime::from_timestamp(ts, 0);
            }
        }
    }
    None
}

/// Extract the `d` tag identifier from an addressable event (kinds 34235/34236).
///
/// Returns a `&str` borrowed from the event's tag storage, or `None` if absent.
pub fn extract_d_tag(event: &Event) -> Option<&str> {
    for tag in event.tags.iter() {
        let v = tag.as_slice();
        if v.len() >= 2 && v[0] == "d" {
            return Some(v[1].as_str());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::{EventBuilder, Keys, Kind, Tag};

    #[test]
    fn test_extract_video_urls() {
        let keys = Keys::generate();

        // Create event with imeta tags in NIP-92 format (space-delimited key-value pairs)
        let event = EventBuilder::new(
            Kind::from(34235),
            "Test video event",
            vec![
                Tag::parse(&["imeta", "url https://example.com/video.mp4", "m video/mp4"]).unwrap(),
                Tag::parse(&[
                    "imeta",
                    "url https://example.com/video.webm",
                    "m video/webm",
                ])
                .unwrap(),
                Tag::parse(&["imeta", "url https://example.com/thumb.jpg", "m image/jpeg"])
                    .unwrap(),
            ],
        )
        .to_event(&keys)
        .unwrap();

        let urls = extract_video_urls(&event);

        assert_eq!(urls.len(), 2); // Only video URLs, not image
        assert_eq!(urls[0].url, "https://example.com/video.mp4");
        assert_eq!(urls[0].mime_type, Some("video/mp4".to_string()));
        assert_eq!(urls[1].url, "https://example.com/video.webm");
        assert_eq!(urls[1].mime_type, Some("video/webm".to_string()));
    }

    #[test]
    fn test_extract_no_video_urls() {
        let keys = Keys::generate();

        // Create event with only image imeta tag in NIP-92 format
        let event = EventBuilder::new(
            Kind::from(34235),
            "Test event",
            vec![
                Tag::parse(&["imeta", "url https://example.com/image.jpg", "m image/jpeg"])
                    .unwrap(),
            ],
        )
        .to_event(&keys)
        .unwrap();

        let urls = extract_video_urls(&event);

        assert_eq!(urls.len(), 0); // No video URLs
    }

    #[test]
    fn test_extract_video_hashes() {
        let keys = Keys::generate();

        let event = EventBuilder::new(
            Kind::from(34235),
            "Test video event",
            vec![
                Tag::parse(&[
                    "imeta",
                    "url https://example.com/video.mp4",
                    "m video/mp4",
                    "x 1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
                    "ox aaaabbbbccccddddeeeeffff1111222233334444555566667777888899990000",
                ])
                .unwrap(),
                Tag::parse(&[
                    "imeta",
                    "url https://example.com/thumb.jpg",
                    "m image/jpeg",
                    "x ffff222233334444555566667777888899990000aaaabbbbccccddddeeee1111",
                ])
                .unwrap(),
            ],
        )
        .to_event(&keys)
        .unwrap();

        let hashes = extract_video_hashes(&event);

        assert_eq!(
            hashes,
            vec![
                "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
                "aaaabbbbccccddddeeeeffff1111222233334444555566667777888899990000",
            ]
        );
    }
}
