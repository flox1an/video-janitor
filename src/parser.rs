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
}
