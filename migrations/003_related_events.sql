-- 003_related_events.sql

CREATE TABLE IF NOT EXISTS event_relations (
    related_event_id  TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    video_event_id    TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relation_type     TEXT NOT NULL CHECK (relation_type IN (
                        'reaction', 'comment', 'note', 'delete',
                        'zap_request', 'zap_receipt'
                      )),
    PRIMARY KEY (related_event_id, video_event_id)
);

CREATE INDEX IF NOT EXISTS idx_event_relations_video_id ON event_relations(video_event_id);

CREATE TABLE IF NOT EXISTS event_sightings (
    event_id      TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relay_id      INTEGER NOT NULL REFERENCES relays(id),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, relay_id)
);

CREATE TABLE IF NOT EXISTS relay_publications (
    event_id     TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relay_id     INTEGER NOT NULL REFERENCES relays(id),
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, relay_id)
);

CREATE INDEX IF NOT EXISTS idx_relay_publications_relay_id ON relay_publications(relay_id);
