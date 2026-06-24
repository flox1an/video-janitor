-- 005_lifecycle_status.sql
--
-- Adds per-event lifecycle tracking:
--   lifecycle_status: 'active' | 'deleted' | 'expired' | 'replaced'
--   expires_at:       parsed NIP-40 expiration tag (nullable)
--   identifier:       d-tag value for addressable events (kinds 34235/34236); nullable

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS lifecycle_status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS expires_at       TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS identifier       TEXT;

CREATE INDEX IF NOT EXISTS idx_events_lifecycle
    ON events(lifecycle_status);

-- Partial index: only rows that actually expire need this
CREATE INDEX IF NOT EXISTS idx_events_expires_at
    ON events(expires_at)
    WHERE expires_at IS NOT NULL;

-- Non-unique index for addressable-event replacement lookups (pubkey+kind+d-tag).
-- Uniqueness of the active event per address is enforced by application logic
-- (mark_event_replaced before insert) rather than a DB constraint, because the
-- INSERT uses ON CONFLICT (event_id) DO NOTHING and cannot satisfy a partial unique index.
CREATE INDEX IF NOT EXISTS idx_events_address
    ON events(pubkey, kind, identifier)
    WHERE identifier IS NOT NULL;
