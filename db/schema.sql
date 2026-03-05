-- 1. Create sensor_readings table
-- We use a "Narrow" table design (metric_type column) for flexibility 
-- so this tutorial can handle any sensor data without changing schema.
CREATE TABLE sensor_readings (
    ts TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    metric_type VARCHAR(32) NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

-- 2. Convert to hypertable
-- '1 day' interval is chosen for this tutorial to ensure chunks are created 
-- visibly even with small amounts of sample data.
SELECT create_hypertable('sensor_readings', 'ts', chunk_time_interval => INTERVAL '1 day');

-- 3. Create unique index for Deduplication (Upserts)
-- The 'ts' column MUST be included to enforce uniqueness across partitions.
CREATE UNIQUE INDEX idx_readings_dedup 
    ON sensor_readings (ts, device_id, metric_type);

-- PRODUCTION ENHANCEMENT: Compression
-- In production, enable this to save 90%+ storage.
-- Note: We segment by 'metric_type' so that different metrics (Temp vs Pressure)
-- are compressed separately for better efficiency.
--
-- ALTER TABLE sensor_readings SET (
--   timescaledb.compress,
--   timescaledb.compress_segmentby = 'device_id, metric_type',
--   timescaledb.compress_orderby = 'ts DESC'
-- );
--
-- SELECT add_compression_policy('sensor_readings', INTERVAL '7 days');