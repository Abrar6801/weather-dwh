-- Composite: city + time (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_fact_obs_city_date
    ON warehouse.fact_observations (city_sk, date_key DESC);

-- BRIN: efficient range scans on sequential timestamped data
CREATE INDEX IF NOT EXISTS idx_fact_obs_brin_time
    ON warehouse.fact_observations USING BRIN (observed_at)
    WITH (pages_per_range = 32);

-- Partial: only index high-quality records (used by dashboard queries)
CREATE INDEX IF NOT EXISTS idx_fact_obs_high_dq
    ON warehouse.fact_observations (city_sk, observed_at)
    WHERE dq_score >= 80;

-- Covering: eliminates table lookup for the most common SELECT list
CREATE INDEX IF NOT EXISTS idx_fact_obs_covering
    ON warehouse.fact_observations (date_key, city_sk)
    INCLUDE (temp_celsius, humidity_pct, wind_speed_ms);

CREATE INDEX IF NOT EXISTS idx_fact_daily_city_date
    ON warehouse.fact_daily_weather (city_sk, date_key DESC);

CREATE INDEX IF NOT EXISTS idx_dim_city_natural
    ON warehouse.dim_city (city_id) WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_audit_table_time
    ON audit.change_log (table_name, changed_at DESC);

-- Watermarks table (CDC layer tracking)
CREATE TABLE IF NOT EXISTS pipeline.watermarks (
    layer             VARCHAR(50)  PRIMARY KEY,
    last_processed_at TIMESTAMPTZ  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watermarks_layer
    ON pipeline.watermarks (layer);
