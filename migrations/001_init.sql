-- Distributed Job Queue System — Database Migration 001
-- Run: psql -U postgres -d job_queue -f migrations/001_init.sql

-- Create database (run separately if needed)
-- CREATE DATABASE job_queue;

-- ─── Jobs Table ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS jobs (
  id           UUID PRIMARY KEY,
  name         VARCHAR(255)   NOT NULL,
  data         JSONB          NOT NULL DEFAULT '{}',
  priority     VARCHAR(20)    NOT NULL DEFAULT 'default'
                              CHECK (priority IN ('high', 'default', 'low')),
  status       VARCHAR(20)    NOT NULL DEFAULT 'queued'
                              CHECK (status IN ('queued', 'active', 'retrying', 'completed', 'failed', 'dead')),
  attempts     INTEGER        NOT NULL DEFAULT 0,
  result       JSONB,
  error        TEXT,
  created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_priority   ON jobs (priority);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);

-- ─── Audit Logs Table ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS audit_logs (
  id         BIGSERIAL    PRIMARY KEY,
  job_id     UUID         NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event      VARCHAR(100) NOT NULL,
  payload    JSONB        NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_job_id    ON audit_logs (job_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at DESC);

-- ─── Verify ───────────────────────────────────────────────────────────────────

SELECT 'Migration 001 applied successfully ✅' AS message;
