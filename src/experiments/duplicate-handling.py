#!/usr/bin/env python3
"""
Duplicate Handling Experiment

Demonstrates how unique constraints + ON CONFLICT DO NOTHING prevent duplicate
data when messages are redelivered (e.g., MQTT QoS 1 retries, network issues).

The experiment:
1. Sends N unique messages
2. Resends M of them (simulating duplicates)
3. Compares two tables:
   - WITHOUT unique constraint (stores duplicates)
   - WITH unique constraint + ON CONFLICT (deduplicates)
"""

import json
import os
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import psycopg
from dotenv import load_dotenv

load_dotenv()

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TIMESCALE_DSN = os.getenv("TIMESCALE_DSN")

# Experiment configuration (can be overridden in .env)
UNIQUE_MESSAGES = int(os.getenv("EXPERIMENT_UNIQUE_MESSAGES", "1000"))
DUPLICATE_MESSAGES = int(os.getenv("EXPERIMENT_DUPLICATE_MESSAGES", "500"))


def setup_tables():
    """Create test tables - one with deduplication, one without."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS test_without_dedup CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_with_dedup CASCADE;")
        
        # Table WITHOUT deduplication (no unique constraint)
        cur.execute("""
            CREATE TABLE test_without_dedup (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            );
        """)
        cur.execute("SELECT create_hypertable('test_without_dedup', 'ts');")
        
        # Table WITH deduplication (unique constraint)
        cur.execute("""
            CREATE TABLE test_with_dedup (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            );
        """)
        cur.execute("SELECT create_hypertable('test_with_dedup', 'ts');")
        
        # Add unique index for deduplication
        cur.execute("""
            CREATE UNIQUE INDEX idx_dedup 
            ON test_with_dedup (ts, device_id, metric_type);
        """)
    
    conn.commit()
    conn.close()
    print("✓ Test tables created\n")


def cleanup_tables():
    """Drop test tables."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_without_dedup CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_with_dedup CASCADE;")
    conn.commit()
    conn.close()


def publish_messages(messages):
    """Publish a list of messages to MQTT using drift-corrected rate control."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="duplicate-handling-publisher"
    )

    connected = False

    def on_connect(c, userdata, flags, reason_code, properties=None):
        nonlocal connected
        if reason_code == 0:
            connected = True

    client.on_connect = on_connect
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    # Wait for connection
    while not connected:
        time.sleep(0.1)

    # Publish at 1000 msg/s with drift correction
    publish_rate = 1000
    last_msg_info = None
    start_time = time.time()

    for count, msg in enumerate(messages, start=1):
        last_msg_info = client.publish("sensors/dedup/data", json.dumps(msg), qos=1)

        # Drift-corrected sleep: only pause when ahead of the target rate.
        elapsed = time.time() - start_time
        expected = count / publish_rate
        drift = expected - elapsed
        if drift > 0:
            time.sleep(drift)

    # Flush paho's outbound QoS 1 queue before stopping the network thread.
    if last_msg_info is not None:
        last_msg_info.wait_for_publish(timeout=10)
    client.loop_stop()
    client.disconnect()


def ingest_without_dedup():
    """Ingest messages WITHOUT deduplication using batching."""
    buffer = []
    buffer_lock = threading.Lock()
    messages_received = 0
    db_conn = psycopg.connect(TIMESCALE_DSN)

    def _do_flush(batch):
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE tmp_ingest "
                    "(LIKE test_without_dedup INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                with cur.copy(
                    "COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
                ) as copy:
                    for msg in batch:
                        copy.write_row((msg['ts'], msg['device_id'], msg['metric'], msg['value']))
                # No ON CONFLICT — duplicates are stored intentionally to prove the point
                cur.execute("""
                    INSERT INTO test_without_dedup (ts, device_id, metric_type, value)
                    SELECT ts, device_id, metric_type, value FROM tmp_ingest
                """)
            db_conn.commit()
        except Exception as e:
            print(f"  [WITHOUT] Flush error: {e}")
            db_conn.rollback()

    def flush_buffer():
        with buffer_lock:
            if not buffer:
                return
            batch = buffer[:]
            buffer.clear()
        _do_flush(batch)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/dedup/data")

    def on_message(client, userdata, message, properties=None):
        nonlocal messages_received
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append(payload)
                messages_received += 1
                need_flush = len(buffer) >= 100
                batch = buffer[:] if need_flush else None
                if need_flush:
                    buffer.clear()
            if batch:
                _do_flush(batch)
        except Exception as e:
            print(f"  [WITHOUT] Error: {e}")
    
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="ingest-without-dedup"
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    
    return client, flush_buffer


def ingest_with_dedup():
    """Ingest messages WITH deduplication using batching."""
    buffer = []
    buffer_lock = threading.Lock()
    messages_received = 0
    db_conn = psycopg.connect(TIMESCALE_DSN)

    def _do_flush(batch):
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE tmp_ingest "
                    "(LIKE test_with_dedup INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                with cur.copy(
                    "COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
                ) as copy:
                    for msg in batch:
                        copy.write_row((msg['ts'], msg['device_id'], msg['metric'], msg['value']))
                # ON CONFLICT targets the unique index — duplicates are silently dropped
                cur.execute("""
                    INSERT INTO test_with_dedup (ts, device_id, metric_type, value)
                    SELECT ts, device_id, metric_type, value FROM tmp_ingest
                    ON CONFLICT (ts, device_id, metric_type) DO NOTHING
                """)
            db_conn.commit()
        except Exception as e:
            print(f"  [WITH] Flush error: {e}")
            db_conn.rollback()

    def flush_buffer():
        with buffer_lock:
            if not buffer:
                return
            batch = buffer[:]
            buffer.clear()
        _do_flush(batch)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/dedup/data")

    def on_message(client, userdata, message, properties=None):
        nonlocal messages_received
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append(payload)
                messages_received += 1
                need_flush = len(buffer) >= 100
                batch = buffer[:] if need_flush else None
                if need_flush:
                    buffer.clear()
            if batch:
                _do_flush(batch)
        except Exception as e:
            print(f"  [WITH] Error: {e}")
    
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="ingest-with-dedup"
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    
    return client, flush_buffer


def main():
    print(f"\n{'='*70}")
    print("DUPLICATE HANDLING EXPERIMENT")
    print(f"{'='*70}\n")
    
    print("This experiment demonstrates idempotent inserts using unique constraints.\n")
    print("Setup:")
    print(f"  • Unique messages: {UNIQUE_MESSAGES:,}")
    print(f"  • Duplicate messages: {DUPLICATE_MESSAGES:,} (resent)")
    print(f"  • Total messages sent: {UNIQUE_MESSAGES + DUPLICATE_MESSAGES:,}")
    print(f"  • Two tables:")
    print(f"    - WITHOUT unique constraint (stores duplicates)")
    print(f"    - WITH unique constraint + ON CONFLICT (deduplicates)\n")
    
    # Setup
    setup_tables()
    
    # Generate unique messages
    print(f"Generating {UNIQUE_MESSAGES:,} unique messages...")
    base_timestamp = datetime.now(timezone.utc)
    unique_messages = []
    
    for i in range(UNIQUE_MESSAGES):
        # Add microseconds to make each timestamp unique
        msg_timestamp = base_timestamp.replace(microsecond=i % 1000000)
        msg = {
            'device_id': f"sensor-{i % 10:03d}",
            'metric': 'temperature',
            'value': 20.0 + (i % 100) / 10.0,
            'ts': msg_timestamp.isoformat()
        }
        unique_messages.append(msg)
    
    # Select messages to duplicate (first N messages)
    duplicate_messages = unique_messages[:DUPLICATE_MESSAGES]
    
    print(f"Selecting {DUPLICATE_MESSAGES:,} messages to resend (duplicates)...\n")
    
    # Test 1: WITHOUT deduplication
    print("="*70)
    print("TEST 1: WITHOUT DEDUPLICATION")
    print("="*70)
    print("\nStarting ingestion process (no unique constraint)...")
    client_without, flush_without = ingest_without_dedup()
    time.sleep(1)
    
    print(f"Publishing {UNIQUE_MESSAGES:,} unique messages...")
    publish_messages(unique_messages)
    time.sleep(3)
    
    print(f"Republishing {DUPLICATE_MESSAGES:,} messages (duplicates)...")
    publish_messages(duplicate_messages)
    time.sleep(3)
    
    # Final flush
    flush_without()
    client_without.loop_stop()
    client_without.disconnect()
    print("✓ Test 1 complete\n")
    
    # Test 2: WITH deduplication
    print("="*70)
    print("TEST 2: WITH DEDUPLICATION")
    print("="*70)
    print("\nStarting ingestion process (unique constraint + ON CONFLICT)...")
    client_with, flush_with = ingest_with_dedup()
    time.sleep(1)
    
    print(f"Publishing {UNIQUE_MESSAGES:,} unique messages...")
    publish_messages(unique_messages)
    time.sleep(3)
    
    print(f"Republishing {DUPLICATE_MESSAGES:,} messages (duplicates)...")
    publish_messages(duplicate_messages)
    time.sleep(3)
    
    # Final flush
    flush_with()
    client_with.loop_stop()
    client_with.disconnect()
    print("✓ Test 2 complete\n")
    
    # Compare results
    print(f"\n{'='*70}")
    print("COMPARISON RESULTS")
    print(f"{'='*70}\n")
    
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_without_dedup")
        count_without = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM test_with_dedup")
        count_with = cur.fetchone()[0]
    conn.close()
    
    total_sent = UNIQUE_MESSAGES + DUPLICATE_MESSAGES
    duplicates_stored = count_without - UNIQUE_MESSAGES
    duplicates_prevented = count_without - count_with
    
    print(f"Messages sent: {total_sent:,}")
    print(f"  • Unique: {UNIQUE_MESSAGES:,}")
    print(f"  • Duplicates: {DUPLICATE_MESSAGES:,}\n")
    
    print(f"{'Approach':<35} {'Rows Stored':<15} {'Duplicates':<15}")
    print(f"{'-'*70}")
    print(f"{'WITHOUT deduplication':<35} {count_without:<15,} {duplicates_stored:<15,}")
    print(f"{'WITH deduplication':<35} {count_with:<15,} {0:<15,}")
    
    # Show proof of duplicates
    if duplicates_stored > 0:
        conn = psycopg.connect(TIMESCALE_DSN)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, device_id, COUNT(*) as count 
                FROM test_without_dedup 
                GROUP BY ts, device_id 
                HAVING COUNT(*) > 1 
                LIMIT 3
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\nSample duplicates in 'test_without_dedup':")
                for row in rows:
                    print(f"  • {row[0]} | {row[1]} | {row[2]} copies")
        conn.close()
    
    print(f"\n{'='*70}")
    print("KEY INSIGHT")
    print(f"{'='*70}\n")
    
    if duplicates_prevented > 0:
        print(f"✅ Unique constraint + ON CONFLICT prevented {duplicates_prevented:,} duplicate inserts!")
        print(f"\nThis pattern ensures idempotent ingestion:")
        print(f"  • MQTT QoS 1 can redeliver messages")
        print(f"  • Network retries won't create duplicates")
        print(f"  • Multiple publishers can send same data safely")
    else:
        print("⚠️  No duplicates detected (timing issue)")
    
    print(f"\n✓ Experiment complete!\n")
    
    # Cleanup
    cleanup_tables()


if __name__ == "__main__":
    main()
