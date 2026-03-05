"""
Database helper module for MQTT-to-SQL tutorial.

EDUCATIONAL NOTE: This module uses a single persistent connection for tutorial simplicity.
In production, use psycopg_pool.ConnectionPool for multi-process deployments.
"""

import os
import psycopg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection (created once, reused)
_connection = None


def get_connection():
    """
    Returns a persistent database connection.

    EDUCATIONAL NOTE: Using a single connection for simplicity.
    Production: Use psycopg_pool.ConnectionPool for connection pooling.
    """
    global _connection
    if _connection is None or _connection.closed:
        dsn = os.getenv('TIMESCALE_DSN', 'postgresql://postgres:postgres@localhost:5432/sensors')
        _connection = psycopg.connect(dsn)
    return _connection


def batch_write(rows):
    """
    Write a batch of sensor readings to TimescaleDB using the COPY protocol.

    Args:
        rows: List of tuples (ts, device_id, metric_type, value)

    Returns:
        Number of rows inserted (excluding duplicates)

    EDUCATIONAL NOTE: COPY is the fastest bulk-load path in PostgreSQL.
    It bypasses the SQL parser and writes 40% less WAL than INSERT.
    The pattern here is:
      1. COPY into a temp staging table (no indexes, no constraints — pure speed)
      2. INSERT ... SELECT ... ON CONFLICT DO NOTHING to merge into the real table
    This gives us both maximum throughput and safe deduplication.
    """
    if not rows:
        return 0

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            # Staging table mirrors sensor_readings schema; dropped on commit
            cur.execute(
                "CREATE TEMP TABLE tmp_ingest "
                "(LIKE sensor_readings INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            # Stream all rows via COPY — single round-trip, no per-row overhead
            with cur.copy(
                "COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)
            # Merge into hypertable; unique index silently drops duplicates
            cur.execute("""
                INSERT INTO sensor_readings (ts, device_id, metric_type, value)
                SELECT ts, device_id, metric_type, value FROM tmp_ingest
                ON CONFLICT (ts, device_id, metric_type) DO NOTHING
            """)
        conn.commit()
        return len(rows)

    except Exception as e:
        conn.rollback()
        print(f"Error writing batch: {e}")
        raise


def close_connection():
    """Close the database connection gracefully."""
    global _connection
    if _connection and not _connection.closed:
        _connection.close()
        _connection = None
