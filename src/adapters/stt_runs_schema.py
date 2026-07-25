STT_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS stt_runs (
    run_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    purpose TEXT NOT NULL,
    provider TEXT NOT NULL,
    model_id TEXT NOT NULL,
    model_revision TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    config_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    code_revision TEXT,
    dataset_manifest_hash TEXT,
    hardware_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    total_calls INTEGER NOT NULL DEFAULT 0,
    completed_calls INTEGER NOT NULL DEFAULT 0,
    failed_calls INTEGER NOT NULL DEFAULT 0,
    skipped_calls INTEGER NOT NULL DEFAULT 0,
    total_audio_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    model_load_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    inference_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    wall_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stt_results (
    run_id UUID NOT NULL REFERENCES stt_runs(run_id) ON DELETE CASCADE,
    call_id TEXT NOT NULL,
    audio_sha256 TEXT NOT NULL,
    audio_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    canonical_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    text_sha256 TEXT,
    elapsed_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    rtf DOUBLE PRECISION NOT NULL DEFAULT 0,
    peak_vram_mb DOUBLE PRECISION,
    batch_size INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_category TEXT,
    error_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, call_id)
);

ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS stt_run_id UUID;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS stt_config_hash TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS source_text_sha256 TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS input_text_sha256 TEXT;

CREATE INDEX IF NOT EXISTS idx_stt_results_call_id ON stt_results(call_id);
CREATE INDEX IF NOT EXISTS idx_stt_results_run_status ON stt_results(run_id, status);
CREATE INDEX IF NOT EXISTS idx_stt_results_audio_sha256 ON stt_results(audio_sha256);
CREATE INDEX IF NOT EXISTS idx_stt_runs_provider_model ON stt_runs(provider, model_id);
"""
