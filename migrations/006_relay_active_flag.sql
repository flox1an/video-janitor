-- 006_relay_active_flag.sql
--
-- Adds relay health flags so broken, retired, or invalid relays can be disabled
-- for reads and writes independently without deleting sync history.
--
-- The original is_active/disable_reason/disabled_at columns are kept for
-- compatibility. Code should use read_enabled/write_enabled going forward.

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS disable_reason TEXT;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS read_enabled BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS write_enabled BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS read_disabled_reason TEXT;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS write_disabled_reason TEXT;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS read_disabled_at TIMESTAMPTZ;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS write_disabled_at TIMESTAMPTZ;

ALTER TABLE relays
  ADD COLUMN IF NOT EXISTS relay_health_split_migrated BOOLEAN;

-- One-time reset from the legacy single is_active flag. We want to retry all
-- relays under the new split read/write health model, then let fresh failures
-- repopulate read_disabled_* or write_disabled_*.
UPDATE relays
SET is_active = TRUE,
    disable_reason = NULL,
    disabled_at = NULL,
    read_enabled = TRUE,
    write_enabled = TRUE,
    read_disabled_reason = NULL,
    write_disabled_reason = NULL,
    read_disabled_at = NULL,
    write_disabled_at = NULL,
    relay_health_split_migrated = TRUE
WHERE relay_health_split_migrated IS NULL;

ALTER TABLE relays
  ALTER COLUMN relay_health_split_migrated SET DEFAULT TRUE;

UPDATE relays
SET relay_health_split_migrated = TRUE
WHERE relay_health_split_migrated IS NULL;

ALTER TABLE relays
  ALTER COLUMN relay_health_split_migrated SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_relays_active
    ON relays(is_active)
    WHERE is_active = FALSE;

CREATE INDEX IF NOT EXISTS idx_relays_read_enabled
    ON relays(read_enabled)
    WHERE read_enabled = FALSE;

CREATE INDEX IF NOT EXISTS idx_relays_write_enabled
    ON relays(write_enabled)
    WHERE write_enabled = FALSE;
