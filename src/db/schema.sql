BEGIN;

DROP TABLE IF EXISTS transcripts;
DROP TABLE IF EXISTS videos;

CREATE TABLE videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identity & Multitenancy
    state VARCHAR(2) NOT NULL,           -- 'MI'
    source TEXT NOT NULL,                -- 'house', 'senate'
    external_id TEXT NOT NULL,           -- 'HAGRI-111325.mp4'
    slug TEXT NOT NULL,                  -- 'mi-house-agri-111325-2025-12-23'
    
    -- Content Metadata
    title TEXT,
    hearing_date TIMESTAMP WITH TIME ZONE,
    
    -- Links (The "Breadcrumbs")
    video_page_url TEXT,                 -- Human-facing page
    original_video_url TEXT,             -- Direct source file URL (the .mp4 link)
    s3_key TEXT,                         -- S3 storage path
    
    -- Pipeline State
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    last_error TEXT,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT valid_video_status CHECK (status IN (
            'queued', 'pending', 'downloading', 'downloaded', 
            'transcribing', 'completed', 'failed', 'permanent_failure'
          )),
    CONSTRAINT unique_video_source UNIQUE (state, source, external_id),
    CONSTRAINT unique_video_slug UNIQUE (slug)
);

CREATE TABLE transcripts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    text TEXT,
    raw_json JSONB,                      -- The full response
    provider TEXT,
    language TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    
    CONSTRAINT transcripts_video_id_unique UNIQUE (video_id);
);

CREATE TYPE job_status AS ENUM ('running', 'completed', 'failed', 'completed_with_errors');

CREATE TABLE job_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  state VARCHAR(15) NOT NULL,            -- e.g., 'MI'
  source VARCHAR(15) NOT NULL;                    -- e.g., 'house'
  executor VARCHAR(100) NOT NULL,        -- e.g., 'cron-worker-1'
  start_time TIMESTAMP NOT NULL DEFAULT NOW(),
  end_time TIMESTAMP,
  status job_status DEFAULT 'running',
  
  -- Metrics for the "Glanceable" Report
  items_discovered INTEGER DEFAULT 0,
  items_processed INTEGER DEFAULT 0,
  items_failed INTEGER DEFAULT 0,
  
  error_summary TEXT                     -- High-level failure reason (if any)
);

CREATE TABLE job_logs (
  id SERIAL PRIMARY KEY,
  run_id UUID REFERENCES job_runs(id),
  level VARCHAR(10) NOT NULL,            -- 'INFO', 'WARN', 'ERROR'
  message TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_videos_state_source_status ON videos(state, source, status);
CREATE INDEX idx_transcripts_video_id ON transcripts(video_id);

COMMIT;