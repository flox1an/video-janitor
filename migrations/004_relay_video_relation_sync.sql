-- 004_relay_video_relation_sync.sql
-- Tracks the latest related-event sync timestamp for each source relay/video pair.

CREATE TABLE IF NOT EXISTS relay_video_relation_sync (
    relay_id              INTEGER NOT NULL REFERENCES relays(id) ON DELETE CASCADE,
    video_event_id        TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    last_sync_timestamp   BIGINT NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (relay_id, video_event_id)
);

CREATE INDEX IF NOT EXISTS idx_relay_video_relation_sync_video_id
    ON relay_video_relation_sync(video_event_id);
