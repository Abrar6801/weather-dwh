CREATE TABLE IF NOT EXISTS audit.change_log (
    log_id       BIGSERIAL   PRIMARY KEY,
    table_schema VARCHAR(50) NOT NULL,
    table_name   VARCHAR(100)NOT NULL,
    operation    CHAR(6)     NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE')),
    changed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by   TEXT        NOT NULL DEFAULT current_user,
    old_values   JSONB,
    new_values   JSONB
);

CREATE OR REPLACE FUNCTION audit.log_changes() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit.change_log
        (table_schema, table_name, operation, old_values, new_values)
    VALUES (
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP,
        CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
        CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) END
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE TRIGGER audit_fact_obs
    AFTER INSERT OR UPDATE OR DELETE ON warehouse.fact_observations
    FOR EACH ROW EXECUTE FUNCTION audit.log_changes();

CREATE OR REPLACE TRIGGER audit_fact_daily
    AFTER INSERT OR UPDATE OR DELETE ON warehouse.fact_daily_weather
    FOR EACH ROW EXECUTE FUNCTION audit.log_changes();
