-- ============================================================
-- Logcelot ClickHouse Schema - High Availability
-- ============================================================

CREATE DATABASE IF NOT EXISTS logcelot ON CLUSTER '{cluster}';

-- ============================================================
-- Main Logs Table (LOCAL) - ReplicatedMergeTree
-- ============================================================

CREATE TABLE IF NOT EXISTS logcelot.logs_local ON CLUSTER '{cluster}'
(
    id              String,

    -- Event time (original timestamp if present, otherwise ingested_at fallback)
    created_at      DateTime64(3, 'UTC'),

    -- Ingestion time (always set by application)
    ingested_at     DateTime64(3, 'UTC'),

    -- 1 if event time was missing/invalid and created_at was set to ingested_at
    event_time_missing UInt8,

    source          LowCardinality(String),
    source_host     String,
    source_service  LowCardinality(String),

    level           LowCardinality(String),

    message         String CODEC(ZSTD(1)),
    parsed_message  String CODEC(ZSTD(1)),

    format          LowCardinality(String),

    metadata        String CODEC(ZSTD(1)),

    user_id         Nullable(String),
    request_id      Nullable(String),
    trace_id        Nullable(String),
    span_id         Nullable(String),

    http_method     Nullable(String),
    http_path       Nullable(String),
    http_status     Nullable(UInt16),
    http_response_time_ms Nullable(UInt32),

    error_type      Nullable(String),
    error_stack     Nullable(String) CODEC(ZSTD(1)),

    is_parsed       UInt8,
    parse_errors    Nullable(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs_local', '{replica}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (source, level, created_at)
SETTINGS index_granularity = 8192;

-- ============================================================
-- Distributed Table (GLOBAL)
-- ============================================================

CREATE TABLE IF NOT EXISTS logcelot.logs ON CLUSTER '{cluster}'
AS logcelot.logs_local
ENGINE = Distributed('{cluster}', logcelot, logs_local, rand());

-- ============================================================
-- Materialized View #1: Error Types Summary
-- Purpose: Top error types for "Top-3 Error Types" dashboard
-- Course Requirement: Топ-3 типов ошибок
-- ============================================================

CREATE TABLE IF NOT EXISTS logcelot.error_types_summary_local ON CLUSTER '{cluster}'
(
    date            Date,
    error_type      String,
    level           String,
    error_count     UInt64,
    sample_messages AggregateFunction(topK(3), String)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/error_types_summary_local', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (date, error_type, level);

CREATE MATERIALIZED VIEW IF NOT EXISTS logcelot.error_types_summary_mv ON CLUSTER '{cluster}'
TO logcelot.error_types_summary_local
AS
SELECT
    toDate(created_at) as date,
    ifNull(error_type, 'Unknown') as error_type,
    level,
    count() as error_count,
    topKState(3)(message) as sample_messages
FROM logcelot.logs_local
WHERE level IN ('ERROR', 'FATAL')
GROUP BY date, error_type, level;

CREATE TABLE IF NOT EXISTS logcelot.error_types_summary ON CLUSTER '{cluster}'
AS logcelot.error_types_summary_local
ENGINE = Distributed('{cluster}', logcelot, error_types_summary_local);

-- ============================================================
-- Materialized View #2: Service Error Matrix
-- Purpose: Top errors per service for "Top-3 Errors by Source" dashboard
-- Course Requirement: Топ-3 ошибок по источникам
-- ============================================================

CREATE TABLE IF NOT EXISTS logcelot.service_error_matrix_local ON CLUSTER '{cluster}'
(
    date            Date,
    source_service  String,
    error_type      String,
    level           String,
    error_count     UInt64,
    last_error_time AggregateFunction(max, DateTime64(3))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/service_error_matrix_local', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (date, source_service, error_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS logcelot.service_error_matrix_mv ON CLUSTER '{cluster}'
TO logcelot.service_error_matrix_local
AS
SELECT
    toDate(created_at) as date,
    source_service,
    ifNull(error_type, 'Unknown') as error_type,
    level,
    count() as error_count,
    maxState(created_at) as last_error_time
FROM logcelot.logs_local
WHERE level IN ('ERROR', 'FATAL')
GROUP BY date, source_service, error_type, level;

CREATE TABLE IF NOT EXISTS logcelot.service_error_matrix ON CLUSTER '{cluster}'
AS logcelot.service_error_matrix_local
ENGINE = Distributed('{cluster}', logcelot, service_error_matrix_local);

-- ============================================================
-- Materialized View #3: Service Health Tracker
-- Purpose: Dead service detection for "Dead Services" dashboard
-- Course Requirement: Dead service detection (источники без логов >1 часа)
-- ============================================================

CREATE TABLE IF NOT EXISTS logcelot.service_health_tracker_local ON CLUSTER '{cluster}'
(
    hour            DateTime,
    source_service  String,
    log_count       UInt64,
    last_log_time   AggregateFunction(max, DateTime64(3)),
    min_log_time    AggregateFunction(min, DateTime64(3))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/service_health_tracker_local', '{replica}')
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, source_service);

CREATE MATERIALIZED VIEW IF NOT EXISTS logcelot.service_health_tracker_mv ON CLUSTER '{cluster}'
TO logcelot.service_health_tracker_local
AS
SELECT
    toStartOfHour(created_at) as hour,
    source_service,
    count() as log_count,
    maxState(created_at) as last_log_time,
    minState(created_at) as min_log_time
FROM logcelot.logs_local
GROUP BY hour, source_service;

CREATE TABLE IF NOT EXISTS logcelot.service_health_tracker ON CLUSTER '{cluster}'
AS logcelot.service_health_tracker_local
ENGINE = Distributed('{cluster}', logcelot, service_health_tracker_local);

-- ============================================================
-- Indexes for Fast Queries (LOCAL)
-- ============================================================

ALTER TABLE logcelot.logs_local ON CLUSTER '{cluster}'
ADD INDEX IF NOT EXISTS idx_message_bloom message TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1;

ALTER TABLE logcelot.logs_local ON CLUSTER '{cluster}'
ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter() GRANULARITY 1;

ALTER TABLE logcelot.logs_local ON CLUSTER '{cluster}'
ADD INDEX IF NOT EXISTS idx_user_id user_id TYPE bloom_filter() GRANULARITY 1;
