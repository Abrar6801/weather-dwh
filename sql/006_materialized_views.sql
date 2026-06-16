CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_country_snapshot AS
SELECT
    c.country,
    COUNT(DISTINCT c.city_id)               AS city_count,
    ROUND(AVG(f.temp_celsius)::NUMERIC, 2)  AS avg_temp_celsius,
    ROUND(MIN(f.temp_celsius)::NUMERIC, 2)  AS min_temp_celsius,
    ROUND(MAX(f.temp_celsius)::NUMERIC, 2)  AS max_temp_celsius,
    ROUND(AVG(f.humidity_pct)::NUMERIC, 1)  AS avg_humidity_pct,
    ROUND(AVG(f.wind_speed_ms)::NUMERIC, 2) AS avg_wind_ms,
    MAX(f.observed_at)                      AS last_updated_utc
FROM warehouse.fact_observations f
JOIN warehouse.dim_city c ON f.city_sk = c.city_sk AND c.is_current
WHERE f.observed_at >= NOW() - INTERVAL '24 hours'
GROUP BY c.country
WITH DATA;

CREATE UNIQUE INDEX ON api.mv_country_snapshot (country);

CREATE MATERIALIZED VIEW IF NOT EXISTS api.mv_city_7day_trends AS
SELECT
    c.city_id,
    c.city_name,
    c.country,
    dt.full_date,
    f.avg_temp_celsius,
    f.min_temp_celsius,
    f.max_temp_celsius,
    f.avg_humidity_pct,
    f.avg_wind_speed_ms,
    f.dominant_condition,
    LAG(f.avg_temp_celsius, 1) OVER (PARTITION BY c.city_id ORDER BY dt.full_date)
        AS prev_day_temp_celsius,
    ROUND((f.avg_temp_celsius
        - LAG(f.avg_temp_celsius, 1) OVER (PARTITION BY c.city_id ORDER BY dt.full_date)
    )::NUMERIC, 2) AS temp_delta_celsius
FROM warehouse.fact_daily_weather f
JOIN warehouse.dim_city c  ON f.city_sk = c.city_sk AND c.is_current
JOIN warehouse.dim_date dt ON f.date_key = dt.date_key
WHERE dt.full_date >= CURRENT_DATE - 7
WITH DATA;

CREATE UNIQUE INDEX ON api.mv_city_7day_trends (city_id, full_date);
