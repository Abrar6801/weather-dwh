-- FIX v3: This file was missing entirely in v2.

-- Latest observation per city
CREATE OR REPLACE VIEW warehouse.v_latest_per_city AS
SELECT DISTINCT ON (o.city_id)
    o.*,
    c.city_name,
    c.country,
    c.latitude,
    c.longitude
FROM warehouse.fact_observations o
JOIN warehouse.dim_city c ON o.city_sk = c.city_sk AND c.is_current
ORDER BY o.city_id, o.observed_at DESC;

-- Global current conditions snapshot
CREATE OR REPLACE VIEW warehouse.v_global_snapshot AS
SELECT
    c.country,
    COUNT(DISTINCT l.city_id)               AS city_count,
    ROUND(AVG(l.temp_celsius)::NUMERIC, 2)  AS avg_temp_celsius,
    ROUND(MIN(l.temp_celsius)::NUMERIC, 2)  AS min_temp_celsius,
    ROUND(MAX(l.temp_celsius)::NUMERIC, 2)  AS max_temp_celsius,
    ROUND(AVG(l.humidity_pct)::NUMERIC, 1)  AS avg_humidity_pct,
    ROUND(AVG(l.wind_speed_ms)::NUMERIC, 2) AS avg_wind_ms,
    MAX(l.observed_at)                      AS last_updated_utc
FROM warehouse.v_latest_per_city l
JOIN warehouse.dim_city c ON l.city_sk = c.city_sk AND c.is_current
GROUP BY c.country
ORDER BY avg_temp_celsius DESC;

-- 7-day temperature trend per city
CREATE OR REPLACE VIEW warehouse.v_7day_trends AS
SELECT
    d.avg_temp_celsius,
    d.min_temp_celsius,
    d.max_temp_celsius,
    TO_DATE(d.date_key::TEXT, 'YYYYMMDD') AS obs_date,
    c.city_name,
    c.country,
    LAG(d.avg_temp_celsius, 1) OVER (
        PARTITION BY d.city_id ORDER BY d.date_key
    ) AS prev_day_temp_celsius,
    ROUND((d.avg_temp_celsius - LAG(d.avg_temp_celsius, 1) OVER (
        PARTITION BY d.city_id ORDER BY d.date_key
    ))::NUMERIC, 2) AS temp_delta_celsius
FROM warehouse.fact_daily_weather d
JOIN warehouse.dim_city c ON d.city_sk = c.city_sk AND c.is_current
WHERE d.date_key >= TO_CHAR(CURRENT_DATE - 7, 'YYYYMMDD')::INT;

-- SCD2 history view — all versions of a city record
CREATE OR REPLACE VIEW warehouse.v_city_history AS
SELECT
    city_id,
    city_name,
    country,
    timezone_offset,
    effective_from,
    COALESCE(effective_to, 'infinity'::timestamptz) AS effective_to,
    is_current,
    row_hash
FROM warehouse.dim_city
ORDER BY city_id, effective_from;

-- Extreme events last 30 days
CREATE OR REPLACE VIEW warehouse.v_recent_extremes AS
SELECT
    o.city_id,
    c.city_name,
    c.country,
    o.observed_at,
    o.temp_celsius,
    o.wind_speed_ms,
    o.humidity_pct,
    o.visibility_m,
    CASE
        WHEN o.temp_celsius >= 40      THEN 'extreme_heat'
        WHEN o.temp_celsius <= -20     THEN 'extreme_cold'
        WHEN o.wind_speed_ms >= 24.5   THEN 'storm_wind'
        WHEN o.humidity_pct >= 95      THEN 'saturation_humidity'
        WHEN o.visibility_m <= 200     THEN 'low_visibility'
    END AS event_type
FROM warehouse.fact_observations o
JOIN warehouse.dim_city c ON o.city_sk = c.city_sk AND c.is_current
WHERE
    o.observed_at >= NOW() - INTERVAL '30 days'
    AND (
        o.temp_celsius >= 40 OR o.temp_celsius <= -20
        OR o.wind_speed_ms >= 24.5
        OR o.humidity_pct >= 95
        OR o.visibility_m <= 200
    )
ORDER BY o.observed_at DESC;
