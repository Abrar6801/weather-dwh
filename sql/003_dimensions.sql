CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INTEGER     PRIMARY KEY,
    full_date       DATE        NOT NULL UNIQUE,
    day_of_week     SMALLINT    NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name        VARCHAR(9)  NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    day_of_year     SMALLINT    NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    month_number    SMALLINT    NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name      VARCHAR(9)  NOT NULL,
    month_short     CHAR(3)     NOT NULL,
    quarter         SMALLINT    NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    quarter_label   VARCHAR(10) NOT NULL,
    year            SMALLINT    NOT NULL,
    year_month      CHAR(7)     NOT NULL,
    is_weekend      BOOLEAN     NOT NULL,
    is_weekday      BOOLEAN     NOT NULL,
    is_leap_year    BOOLEAN     NOT NULL,
    days_in_month   SMALLINT    NOT NULL,
    fiscal_year     SMALLINT    NOT NULL,
    fiscal_quarter  SMALLINT    NOT NULL CHECK (fiscal_quarter BETWEEN 1 AND 4),
    season          VARCHAR(6)  NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_city (
    city_sk         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id         BIGINT      NOT NULL,
    city_name       VARCHAR(100)NOT NULL,
    country         CHAR(2)     NOT NULL,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    timezone_offset INTEGER,
    effective_from  TIMESTAMPTZ NOT NULL,
    effective_to    TIMESTAMPTZ,
    is_current      BOOLEAN     NOT NULL DEFAULT TRUE,
    row_hash        CHAR(32)    NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX uix_dim_city_current ON warehouse.dim_city (city_id) WHERE is_current = TRUE;

CREATE TABLE IF NOT EXISTS warehouse.dim_weather_condition (
    condition_id    INTEGER     PRIMARY KEY,
    main_category   VARCHAR(50) NOT NULL,
    description     VARCHAR(100)NOT NULL,
    icon_day        VARCHAR(10),
    icon_night      VARCHAR(10),
    severity        SMALLINT    NOT NULL CHECK (severity BETWEEN 1 AND 5)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_key        INTEGER     PRIMARY KEY,
    hour_24         SMALLINT    NOT NULL CHECK (hour_24 BETWEEN 0 AND 23),
    minute          SMALLINT    NOT NULL CHECK (minute BETWEEN 0 AND 59),
    hour_12         SMALLINT    NOT NULL,
    am_pm           CHAR(2)     NOT NULL,
    time_label_24   VARCHAR(5)  NOT NULL,
    time_label_12   VARCHAR(8)  NOT NULL,
    time_of_day     VARCHAR(10) NOT NULL,
    is_business_hour BOOLEAN    NOT NULL,
    is_peak_hour    BOOLEAN     NOT NULL
);
