ALTER TABLE warehouse.fact_observations  ENABLE ROW LEVEL SECURITY;
ALTER TABLE warehouse.fact_daily_weather ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='weather_readonly') THEN
    CREATE ROLE weather_readonly; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='weather_analyst') THEN
    CREATE ROLE weather_analyst; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='weather_pipeline') THEN
    CREATE ROLE weather_pipeline; END IF;
END $$;

GRANT USAGE ON SCHEMA warehouse TO weather_readonly, weather_analyst, weather_pipeline;
GRANT USAGE ON SCHEMA api       TO weather_readonly, weather_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA api       TO weather_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO weather_analyst;
GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA warehouse TO weather_pipeline;
GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA staging   TO weather_pipeline;

CREATE POLICY obs_analyst ON warehouse.fact_observations
    FOR SELECT TO weather_analyst USING (TRUE);
CREATE POLICY daily_analyst ON warehouse.fact_daily_weather
    FOR SELECT TO weather_analyst USING (TRUE);
CREATE POLICY obs_pipeline ON warehouse.fact_observations
    FOR ALL TO weather_pipeline USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY daily_pipeline ON warehouse.fact_daily_weather
    FOR ALL TO weather_pipeline USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE warehouse.fact_observations  FORCE ROW LEVEL SECURITY;
ALTER TABLE warehouse.fact_daily_weather FORCE ROW LEVEL SECURITY;
