-- Create events table
CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    created_at BIGINT NOT NULL,
    content TEXT,
    raw_event JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    relay_source TEXT NOT NULL
);

CREATE INDEX idx_events_pubkey ON events(pubkey);
CREATE INDEX idx_events_created_at ON events(created_at DESC);
CREATE INDEX idx_events_kind ON events(kind);

-- Create video_urls table
CREATE TABLE video_urls (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    url_type TEXT NOT NULL,
    mime_type TEXT,

    -- Status tracking
    status TEXT NOT NULL DEFAULT 'pending',
    http_status_code SMALLINT,
    last_checked_at TIMESTAMPTZ,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,

    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(event_id, url)
);

CREATE INDEX idx_video_urls_event_id ON video_urls(event_id);
CREATE INDEX idx_video_urls_status ON video_urls(status);
CREATE INDEX idx_video_urls_last_checked ON video_urls(last_checked_at);

-- Create relay_state table
CREATE TABLE relay_state (
    relay_url TEXT PRIMARY KEY,
    last_event_timestamp BIGINT NOT NULL,
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_events_fetched INTEGER NOT NULL DEFAULT 0
);
