-- V001: Core schema - all tables, columns, indexes
-- Initial migration capturing the full existing schema

CREATE TABLE IF NOT EXISTS transcripts (
    call_id             TEXT PRIMARY KEY,
    pipeline_stage       TEXT,
    stt_run_id           UUID,
    stt_config_hash      TEXT,
    source_text_sha256   TEXT,
    data                JSONB NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analyses (
    call_id         TEXT PRIMARY KEY,
    direction       TEXT,
    manager_id      TEXT,
    manager_name    TEXT,
    role            TEXT,
    spam_probability FLOAT,
    effective_call  BOOLEAN,
    intent          TEXT,
    outcome         TEXT,
    summary         TEXT,
    audio_seconds   FLOAT,
    call_datetime   TIMESTAMPTZ,
    src_number       TEXT,
    dst_number       TEXT,
    key_questions    JSONB,
    objections       JSONB,
    analysis_error   TEXT,
    data            JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS calls (
    call_id         TEXT PRIMARY KEY,
    source_file     TEXT,
    source_path     TEXT,
    call_datetime   TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'discovered',
    error_message   TEXT,
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    transcribed_at  TIMESTAMPTZ,
    translated_at   TIMESTAMPTZ,
    analyzed_at     TIMESTAMPTZ,
    synced_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS keywords (
    keyword_id    TEXT PRIMARY KEY,
    label         TEXT NOT NULL,
    category      TEXT NOT NULL DEFAULT 'general',
    match_fields  JSONB NOT NULL DEFAULT '["summary","key_questions","objections"]'::jsonb,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS keyword_aliases (
    keyword_id    TEXT NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    phrase        TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (keyword_id, phrase)
);

CREATE TABLE IF NOT EXISTS call_keywords (
    call_id          TEXT NOT NULL REFERENCES analyses(call_id) ON DELETE CASCADE,
    keyword_id       TEXT NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    match_count      INTEGER NOT NULL DEFAULT 0,
    matched_fields   JSONB NOT NULL DEFAULT '[]'::jsonb,
    matched_terms    JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (call_id, keyword_id)
);

CREATE TABLE IF NOT EXISTS keyword_materialization_state (
    state_key            TEXT PRIMARY KEY,
    last_materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_calls      INTEGER NOT NULL DEFAULT 0,
    matched_calls        INTEGER NOT NULL DEFAULT 0,
    stored_rows          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS keyword_ai_analyses (
    analysis_id                     TEXT PRIMARY KEY,
    keyword_source                  TEXT NOT NULL,
    reporting_source                TEXT,
    ai_model                        TEXT,
    ai_summary                      TEXT NOT NULL DEFAULT '',
    analyzed_keywords               INTEGER NOT NULL DEFAULT 0,
    total_candidates_before_limit   INTEGER NOT NULL DEFAULT 0,
    truncated                       BOOLEAN NOT NULL DEFAULT FALSE,
    request_data                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    analysis_input                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    ai_analysis                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS keyword_ai_analysis_items (
    analysis_id  TEXT NOT NULL REFERENCES keyword_ai_analyses(analysis_id) ON DELETE CASCADE,
    item_type    TEXT NOT NULL,
    item_key     TEXT NOT NULL,
    data         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (analysis_id, item_type, item_key)
);

-- AI Apply table
CREATE TABLE IF NOT EXISTS ai_apply (
    apply_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analysis_id       TEXT NOT NULL REFERENCES keyword_ai_analyses(analysis_id) ON DELETE CASCADE,
    applied_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by        TEXT DEFAULT current_user,
    dry_run           BOOLEAN NOT NULL DEFAULT false,
    actions_applied   JSONB NOT NULL DEFAULT '[]'::jsonb,
    actions_skipped   JSONB NOT NULL DEFAULT '[]'::jsonb,
    mutations         JSONB NOT NULL DEFAULT '[]'::jsonb,
    keyword_refreshed BOOLEAN NOT NULL DEFAULT false,
    follow_up_ran     BOOLEAN NOT NULL DEFAULT false,
    error             TEXT
);

-- AI Alias Suggestions
CREATE TABLE IF NOT EXISTS ai_alias_suggestions (
    suggestion_id     UUID PRIMARY KEY,
    keyword_id        TEXT NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    suggested_aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_evidence   JSONB,
    ai_model          TEXT,
    status            TEXT NOT NULL DEFAULT 'pending',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Deep Insights
CREATE TABLE IF NOT EXISTS ai_deep_insights_runs (
    run_id        UUID PRIMARY KEY,
    ai_model      TEXT,
    insight_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    request_data  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ai_deep_insights (
    insight_id            UUID PRIMARY KEY,
    run_id                UUID NOT NULL REFERENCES ai_deep_insights_runs(run_id) ON DELETE CASCADE,
    insight_type          TEXT NOT NULL,
    title                 TEXT NOT NULL DEFAULT '',
    description           TEXT NOT NULL DEFAULT '',
    severity              TEXT NOT NULL DEFAULT 'low',
    affected_calls_count  INTEGER NOT NULL DEFAULT 0,
    evidence_summary      TEXT,
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_analyses_manager_id ON analyses(manager_id);
CREATE INDEX IF NOT EXISTS idx_analyses_role ON analyses(role);
CREATE INDEX IF NOT EXISTS idx_analyses_intent ON analyses(intent);
CREATE INDEX IF NOT EXISTS idx_analyses_outcome ON analyses(outcome);
CREATE INDEX IF NOT EXISTS idx_analyses_direction ON analyses(direction);
CREATE INDEX IF NOT EXISTS idx_analyses_filter_path ON analyses(call_datetime, manager_id, role, direction);
CREATE INDEX IF NOT EXISTS idx_analyses_effective_filter ON analyses(call_datetime, manager_id) WHERE effective_call IS TRUE;
CREATE INDEX IF NOT EXISTS idx_analyses_spam_filter ON analyses(spam_probability, call_datetime) WHERE spam_probability IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status);
CREATE INDEX IF NOT EXISTS idx_calls_updated_at ON calls(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_analyses_call_datetime ON analyses(call_datetime);
CREATE INDEX IF NOT EXISTS idx_calls_call_datetime ON calls(call_datetime);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_id ON call_keywords(keyword_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_call_id ON call_keywords(call_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_call ON call_keywords(keyword_id, call_id);
CREATE INDEX IF NOT EXISTS idx_call_keywords_keyword_match_sort ON call_keywords(keyword_id, match_count DESC, call_id DESC);
CREATE INDEX IF NOT EXISTS idx_keyword_ai_analyses_created_at ON keyword_ai_analyses(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_keyword_ai_analysis_items_analysis_id ON keyword_ai_analysis_items(analysis_id);
CREATE INDEX IF NOT EXISTS idx_ai_apply_analysis_id ON ai_apply(analysis_id);
CREATE INDEX IF NOT EXISTS idx_ai_apply_applied_at ON ai_apply(applied_at);
CREATE INDEX IF NOT EXISTS idx_ai_alias_suggestions_keyword_status ON ai_alias_suggestions(keyword_id, status);
CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_runs_created_at ON ai_deep_insights_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_run_id ON ai_deep_insights(run_id);
CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_type_severity ON ai_deep_insights(insight_type, severity);
CREATE INDEX IF NOT EXISTS idx_ai_deep_insights_created_at ON ai_deep_insights(created_at DESC);

-- Backfill call_datetime from call_date (if it exists) and drop call_date
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='analyses' AND column_name='call_date') THEN
    UPDATE analyses SET call_datetime = to_timestamp(call_date, 'YYYYMMDD') AT TIME ZONE 'UTC'
    WHERE call_datetime IS NULL AND call_date ~ '^\d{8}$';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='calls' AND column_name='call_date') THEN
    UPDATE calls SET call_datetime = to_timestamp(call_date, 'YYYYMMDD') AT TIME ZONE 'UTC'
    WHERE call_datetime IS NULL AND call_date ~ '^\d{8}$';
  END IF;
END $$;

ALTER TABLE analyses DROP COLUMN IF EXISTS call_date;
ALTER TABLE calls DROP COLUMN IF EXISTS call_date;
