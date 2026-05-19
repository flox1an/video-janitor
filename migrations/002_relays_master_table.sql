-- 002_relays_master_table.sql
-- Replaces relay_state with a general relays master table.
-- Idempotent: wrapped in DO blocks that check for existence before acting.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.tables WHERE table_name = 'relays'
    ) THEN
        CREATE TABLE relays (
            id          SERIAL PRIMARY KEY,
            relay_url   TEXT UNIQUE NOT NULL,
            relay_type  TEXT NOT NULL CHECK (relay_type IN ('source', 'target', 'both')),
            last_event_timestamp  BIGINT,
            last_sync_at          TIMESTAMPTZ,
            total_events_fetched  INTEGER NOT NULL DEFAULT 0
        );

        IF EXISTS (
            SELECT FROM information_schema.tables WHERE table_name = 'relay_state'
        ) THEN
            INSERT INTO relays (relay_url, relay_type, last_event_timestamp, last_sync_at, total_events_fetched)
            SELECT relay_url, 'source', last_event_timestamp, last_sync_at, total_events_fetched
            FROM relay_state;

            DROP TABLE relay_state;
        END IF;
    END IF;
END
$$;

-- Remove relay_source column from events (contained only "unknown", no data loss)
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'events' AND column_name = 'relay_source'
    ) THEN
        ALTER TABLE events DROP COLUMN relay_source;
    END IF;
END
$$;
