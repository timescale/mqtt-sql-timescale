-- Hypertable Information Queries
-- Use these to show TimescaleDB internals in the tutorial

-- 1. Show hypertable configuration
SELECT 
    hypertable_name,
    num_dimensions,
    num_chunks,
    compression_enabled,
    tablespaces
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'sensor_readings';

-- 2. Show chunks and their time ranges
SELECT 
    chunk_name,
    range_start,
    range_end,
    is_compressed,
    chunk_tablespace
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sensor_readings'
ORDER BY range_start DESC;

-- 3. Show chunk statistics (size, row count)
SELECT 
    chunk_schema || '.' || chunk_name AS chunk,
    pg_size_pretty(total_bytes) AS total_size,
    pg_size_pretty(index_bytes) AS index_size,
    pg_size_pretty(toast_bytes) AS toast_size,
    pg_size_pretty(table_bytes) AS table_size
FROM timescaledb_information.chunks c
JOIN LATERAL (
    SELECT 
        pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name)) AS total_bytes,
        pg_indexes_size(format('%I.%I', chunk_schema, chunk_name)) AS index_bytes,
        pg_total_relation_size(reltoastrelid) AS toast_bytes,
        pg_relation_size(format('%I.%I', chunk_schema, chunk_name)) AS table_bytes
    FROM pg_class
    WHERE relname = chunk_name
) sizes ON true
WHERE hypertable_name = 'sensor_readings'
ORDER BY total_bytes DESC;

-- 4. Show indexes on the hypertable
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'sensor_readings'
ORDER BY indexname;

-- 5. Show compression settings (even if not enabled)
SELECT 
    attname AS column_name,
    segmentby_column_index,
    orderby_column_index,
    orderby_asc,
    orderby_nullsfirst
FROM timescaledb_information.compression_settings
WHERE hypertable_name = 'sensor_readings';

-- 6. Show data distribution across devices
SELECT 
    device_id,
    COUNT(*) AS reading_count,
    MIN(ts) AS first_reading,
    MAX(ts) AS last_reading
FROM sensor_readings
GROUP BY device_id
ORDER BY device_id
LIMIT 10;

-- 7. Show data distribution across metrics
SELECT 
    metric_type,
    COUNT(*) AS reading_count,
    ROUND(AVG(value)::numeric, 2) AS avg_value,
    ROUND(MIN(value)::numeric, 2) AS min_value,
    ROUND(MAX(value)::numeric, 2) AS max_value
FROM sensor_readings
GROUP BY metric_type
ORDER BY metric_type;

-- 8. Show time-series continuity (gaps in behavior)
SELECT 
    device_id,
    metric_type,
    ts,
    LAG(ts) OVER (PARTITION BY device_id, metric_type ORDER BY ts) AS prev_ts,
    ts - LAG(ts) OVER (PARTITION BY device_id, metric_type ORDER BY ts) AS gap
FROM sensor_readings
WHERE device_id = 'sensor-001'
ORDER BY ts DESC
LIMIT 20;

-- 9. Show total row count and table size
SELECT 
    COUNT(*) AS total_rows,
    pg_size_pretty(pg_total_relation_size('sensor_readings')) AS total_size
FROM sensor_readings;

-- 10. Show chunk interval information
SELECT 
    hypertable_name,
    column_name,
    time_interval
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'sensor_readings';
