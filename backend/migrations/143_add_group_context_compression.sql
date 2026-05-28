-- Migration 143: Add context_compression_enabled column to groups table.
-- Allows admins to selectively enable context compression per group.

ALTER TABLE groups ADD COLUMN IF NOT EXISTS context_compression_enabled BOOLEAN NOT NULL DEFAULT false;
