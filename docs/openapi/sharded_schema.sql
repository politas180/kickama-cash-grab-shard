-- =============================================================================
-- Sharded Schema Migration for Analytics Events
-- =============================================================================
-- Adds horizontal sharding support to the analytics events table.
-- Uses PostgreSQL's native partitioning (declarative partitioning)
-- with hash-based distribution across N shards.
-- =============================================================================

CREATE OR REPLACE FUNCTION get_shard_count()
RETURNS integer AS $$ BEGIN RETURN 8; END; $$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION get_shard_number(event_type_id bigint)
RETURNS integer AS $$ BEGIN RETURN abs(event_type_id % get_shard_count()); END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE TABLE IF NOT EXISTS analytics_events (
    id              BIGSERIAL,
    event_type_id   BIGINT NOT NULL,
    event_timestamp timestamptz NOT NULL DEFAULT NOW(),
    user_id         BIGINT,
    session_id      text,
    endpoint_id     BIGINT,
    metric_type     text NOT NULL,
    metric_value    double precision NOT NULL,
    metadata        jsonb NOT NULL DEFAULT '{}',
    tags            text[] NOT NULL DEFAULT '{}',
    source          text NOT NULL DEFAULT 'unknown',
    created_at      timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, event_type_id)
) PARTITION BY HASH (event_type_id);

CREATE TABLE IF NOT EXISTS analytics_events_shard0 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS analytics_events_shard1 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS analytics_events_shard2 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS analytics_events_shard3 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS analytics_events_shard4 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS analytics_events_shard5 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS analytics_events_shard6 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS analytics_events_shard7 PARTITION OF analytics_events FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- Per-shard indexes for dashboard query optimization
DO $$ DECLARE i integer; s text; BEGIN
    FOR i IN 0..7 LOOP
        s := format('analytics_events_shard%s', i);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_ts ON %s (event_timestamp DESC)', s, s);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_user ON %s (user_id) WHERE user_id IS NOT NULL', s, s);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_metric ON %s (metric_type)', s, s);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_tags ON %s USING GIN(tags)', s, s);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_time_metric ON %s (event_timestamp DESC, metric_type)', s, s);
    END LOOP;
END $$;

-- Event types reference table
CREATE TABLE IF NOT EXISTS analytics_event_types (
    id BIGSERIAL PRIMARY KEY, name text NOT NULL UNIQUE,
    description text, category text NOT NULL, is_active boolean DEFAULT true,
    created_at timestamptz DEFAULT NOW()
);

-- Dashboard aggregation views
CREATE OR REPLACE VIEW analytics_summary AS
SELECT metric_type, date_trunc('hour', event_timestamp) AS hour,
    COUNT(*) AS event_count, AVG(metric_value) AS avg_value,
    MIN(metric_value) AS min_value, MAX(metric_value) AS max_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95_value,
    COUNT(DISTINCT user_id) AS unique_users
FROM analytics_events GROUP BY metric_type, date_trunc('hour', event_timestamp);

CREATE OR REPLACE VIEW analytics_error_summary AS
SELECT metric_type, date_trunc('day', event_timestamp) AS day,
    COUNT(*) AS error_count, COUNT(DISTINCT user_id) AS affected_users
FROM analytics_events WHERE metric_type IN ('api_errors','grpc_errors','db_error','bg_job_error')
GROUP BY metric_type, date_trunc('day', event_timestamp);

CREATE OR REPLACE VIEW shard_distribution AS
SELECT get_shard_number(event_type_id) AS shard, COUNT(*) AS event_count
FROM analytics_events GROUP BY get_shard_number(event_type_id) ORDER BY shard;

-- Query optimization: partial indexes for common dashboard queries
CREATE INDEX IF NOT EXISTS idx_analytics_recent ON analytics_events (event_timestamp DESC, metric_type)
    WHERE event_timestamp > NOW() - INTERVAL '24 hours';
CREATE INDEX IF NOT EXISTS idx_analytics_critical ON analytics_events (event_timestamp DESC)
    WHERE metric_type IN ('api_errors','grpc_errors','db_error');
