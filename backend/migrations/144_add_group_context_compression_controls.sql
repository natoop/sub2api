-- Migration 144: Add per-group context compression strategy and parameter overrides.
-- Empty strategy and zero numeric values inherit gateway.context_compression defaults.

ALTER TABLE groups ADD COLUMN IF NOT EXISTS context_compression_strategy VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE groups ADD COLUMN IF NOT EXISTS context_compression_trigger_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS context_compression_keep_last_messages INTEGER NOT NULL DEFAULT 0;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS context_compression_keep_last_tokens INTEGER NOT NULL DEFAULT 0;
