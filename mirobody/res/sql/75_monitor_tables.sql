-- Monitor report tables for W3.1/W3.2 data health evaluation (TH-141)
-- platform_hourly_profile: hourly platform-level ingestion stats
-- indicator_daily_profile: daily indicator-level quality profiles

CREATE TABLE IF NOT EXISTS platform_hourly_profile (
    stat_hour          timestamp without time zone NOT NULL,
    platform           varchar(32) NOT NULL,
    source             varchar(128) NOT NULL,
    records_ingested   integer NOT NULL DEFAULT 0,
    unique_users       integer NOT NULL DEFAULT 0,
    unique_indicators  integer NOT NULL DEFAULT 0,
    filtered_count     integer NOT NULL DEFAULT 0,
    created_at         timestamp with time zone NOT NULL DEFAULT now(),
    updated_at         timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT uq_platform_hourly UNIQUE (stat_hour, platform, source)
);

CREATE INDEX IF NOT EXISTS idx_php_stat_hour ON platform_hourly_profile (stat_hour DESC);
CREATE INDEX IF NOT EXISTS idx_php_platform ON platform_hourly_profile (platform, stat_hour DESC);


CREATE TABLE IF NOT EXISTS indicator_daily_profile (
    stat_date          date NOT NULL,
    indicator          varchar(128) NOT NULL,
    source             varchar(128) NOT NULL,
    record_count       integer NOT NULL DEFAULT 0,
    non_numeric_count  integer NOT NULL DEFAULT 0,
    filtered_count     integer NOT NULL DEFAULT 0,
    min_val            double precision,
    max_val            double precision,
    mean_val           double precision,
    stddev_val         double precision,
    p1                 double precision,
    p5                 double precision,
    p25                double precision,
    median_val         double precision,
    p75                double precision,
    p95                double precision,
    p99                double precision,
    issues             jsonb NOT NULL DEFAULT '[]'::jsonb,
    health             varchar(16) NOT NULL DEFAULT 'ok',
    created_at         timestamp with time zone NOT NULL DEFAULT now(),
    updated_at         timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT uq_indicator_daily UNIQUE (stat_date, indicator, source)
);

CREATE INDEX IF NOT EXISTS idx_idp_stat_date ON indicator_daily_profile (stat_date DESC);
CREATE INDEX IF NOT EXISTS idx_idp_indicator ON indicator_daily_profile (indicator, stat_date DESC);
