#!/usr/bin/env python3
"""
Error Recovery Experiment (Poison Pill)

Demonstrates how to handle data quality issues (e.g., type mismatches, invalid JSON)
without losing an entire batch of valid data.

The experiment:
1. Sends N valid messages
2. Sends 1 "poison pill" message (invalid data type)
3. Sends N more valid messages
4. Compares two ingestion approaches:
   - NAIVE: Fails entire batch on error (high data loss)
   - ROBUST: Catches error, logs it, and continues (minimal data loss)
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
VALID_MESSAGES_BATCH = int(os.getenv("EXPERIMENT_VALID_MESSAGES_BATCH", "100"))
POISON_PILL_MESSAGE = os.getenv("EXPERIMENT_POISON_PILL_MESSAGE", '{"ts": "2025-01-01T00:00:00Z", "device_id": "bad-sensor", "metric": "temperature", "value": "check-engine-light"}')


def setup_tables():
    """Create test tables."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS test_naive_error CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_robust_error CASCADE;")
        
        # Create tables
        for table in ["test_naive_error", "test_robust_error"]:
            cur.execute(f"""
                CREATE TABLE {table} (
                    ts TIMESTAMPTZ NOT NULL,
                    device_id TEXT NOT NULL,
                    metric_type TEXT NOT NULL,
                    value DOUBLE PRECISION NOT NULL
                );
            """)
            cur.execute(f"SELECT create_hypertable('{table}', 'ts');")
    
    conn.commit()
    conn.close()
    print("✓ Test tables created\n")


def cleanup_tables():
    """Drop test tables."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_naive_error CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_robust_error CASCADE;")
    conn.commit()
    conn.close()


def publish_workload():
    """Publish workload: Valid -> Poison Pill -> Valid."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="error-recovery-publisher"
    )

    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    time.sleep(0.5)  # Wait for connection

    publish_rate = 1000
    last_msg_info = None

    # 1. First batch of valid messages
    print(f"  Sending {VALID_MESSAGES_BATCH} valid messages...")
    base_time = datetime.now(timezone.utc)
    start_time = time.time()
    for count, i in enumerate(range(VALID_MESSAGES_BATCH), start=1):
        msg = {
            'ts': base_time.isoformat(),
            'device_id': f"sensor-{i}",
            'metric': 'temperature',
            'value': 25.5
        }
        last_msg_info = client.publish("sensors/error_test", json.dumps(msg), qos=1)
        elapsed = time.time() - start_time
        drift = count / publish_rate - elapsed
        if drift > 0:
            time.sleep(drift)

    # 2. Poison Pill (String value in DOUBLE PRECISION column)
    print("  Sending 1 POISON PILL message (invalid type)...")
    last_msg_info = client.publish("sensors/error_test", POISON_PILL_MESSAGE, qos=1)

    # 3. Second batch of valid messages
    print(f"  Sending {VALID_MESSAGES_BATCH} more valid messages...")
    start_time = time.time()
    for count, i in enumerate(range(VALID_MESSAGES_BATCH), start=1):
        msg = {
            'ts': base_time.isoformat(),
            'device_id': f"sensor-{i+VALID_MESSAGES_BATCH}",
            'metric': 'temperature',
            'value': 25.5
        }
        last_msg_info = client.publish("sensors/error_test", json.dumps(msg), qos=1)
        elapsed = time.time() - start_time
        drift = count / publish_rate - elapsed
        if drift > 0:
            time.sleep(drift)

    # Flush paho's outbound QoS 1 queue before stopping the network thread.
    if last_msg_info is not None:
        last_msg_info.wait_for_publish(timeout=10)
    client.loop_stop()
    client.disconnect()


def ingest_naive():
    """Naive ingestion: Fails entire batch on one error."""
    buffer = []
    buffer_lock = threading.Lock()

    def flush_buffer():
        with buffer_lock:
            if not buffer:
                return
            batch = buffer[:]
            buffer.clear()

        conn = psycopg.connect(TIMESCALE_DSN)
        try:
            # All inserts in a single transaction — one failure rolls back everything
            with conn.cursor() as cur:
                for msg in batch:
                    cur.execute(
                        "INSERT INTO test_naive_error (ts, device_id, metric_type, value) VALUES (%s, %s, %s, %s)",
                        (msg.get('ts'), msg.get('device_id'), msg.get('metric'), msg.get('value'))
                    )
            conn.commit()
        except Exception as e:
            print(f"  [NAIVE] 💥 Batch failed! Error: {e}")
            print(f"  [NAIVE] ❌ Dropping batch of {len(batch)} messages")
            conn.rollback()
        finally:
            conn.close()

    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append(payload)
                should_flush = len(buffer) >= (VALID_MESSAGES_BATCH * 2 + 1)
            if should_flush:
                flush_buffer()
        except Exception:
            pass  # Ignore JSON parse errors for this demo, focus on DB errors

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="ingest-naive"
    )
    client.on_connect = lambda c, u, f, r, p: c.subscribe("sensors/error_test") if r == 0 else None
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    return client, flush_buffer


def ingest_robust():
    """Robust ingestion: Retries row-by-row on batch failure."""
    buffer = []
    buffer_lock = threading.Lock()

    def flush_buffer():
        with buffer_lock:
            if not buffer:
                return
            batch = buffer[:]
            buffer.clear()

        conn = psycopg.connect(TIMESCALE_DSN)
        try:
            # 1. Try fast path (Batch — single transaction)
            with conn.cursor() as cur:
                with conn.transaction():
                    for msg in batch:
                        cur.execute(
                            "INSERT INTO test_robust_error (ts, device_id, metric_type, value) VALUES (%s, %s, %s, %s)",
                            (msg.get('ts'), msg.get('device_id'), msg.get('metric'), msg.get('value'))
                        )
            # conn.transaction() auto-commits on success
            print(f"  [ROBUST] ✅ Batch of {len(batch)} succeeded")

        except Exception as e:
            print(f"  [ROBUST] ⚠️ Batch failed. Falling back to row-by-row...")

            # 2. Slow path (Row-by-row with savepoints)
            success_count = 0
            fail_count = 0

            with conn.cursor() as cur:
                for msg in batch:
                    try:
                        with conn.transaction():  # Savepoint per row
                            cur.execute(
                                "INSERT INTO test_robust_error (ts, device_id, metric_type, value) VALUES (%s, %s, %s, %s)",
                                (msg.get('ts'), msg.get('device_id'), msg.get('metric'), msg.get('value'))
                            )
                        success_count += 1
                    except Exception as row_error:
                        # Log the bad row (in production, send to Dead Letter Queue)
                        print(f"  [ROBUST] 🚫 Sidelined bad message: {msg.get('value', 'unknown')}")
                        fail_count += 1

            conn.commit()
            print(f"  [ROBUST] Recovered {success_count} valid messages. Lost {fail_count} bad messages.")
        finally:
            conn.close()

    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append(payload)
                should_flush = len(buffer) >= (VALID_MESSAGES_BATCH * 2 + 1)
            if should_flush:
                flush_buffer()
        except Exception:
            pass

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="ingest-robust"
    )
    client.on_connect = lambda c, u, f, r, p: c.subscribe("sensors/error_test") if r == 0 else None
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    return client, flush_buffer


def main():
    print(f"\n{'='*70}")
    print("ERROR RECOVERY EXPERIMENT (POISON PILL)")
    print(f"{'='*70}\n")
    
    print("This experiment demonstrates handling of 'poison pill' messages (invalid data).")
    print("Setup:")
    print(f"  • Batch size: {VALID_MESSAGES_BATCH * 2 + 1} messages")
    print(f"  • Contains: {VALID_MESSAGES_BATCH * 2} valid + 1 poison pill")
    print(f"  • Poison pill: String value in numeric column ('check-engine-light')\n")
    
    setup_tables()
    
    # TEST 1: NAIVE
    print("="*70)
    print("TEST 1: NAIVE INGESTION (Fails Batch)")
    print("="*70)
    client_naive, flush_naive = ingest_naive()
    time.sleep(1)
    publish_workload()
    time.sleep(2)
    flush_naive()
    client_naive.loop_stop()
    client_naive.disconnect()
    print("✓ Test 1 complete\n")
    
    # TEST 2: ROBUST
    print("="*70)
    print("TEST 2: ROBUST INGESTION (Row-by-Row Fallback)")
    print("="*70)
    client_robust, flush_robust = ingest_robust()
    time.sleep(1)
    publish_workload()
    time.sleep(2)
    flush_robust()
    client_robust.loop_stop()
    client_robust.disconnect()
    print("✓ Test 2 complete\n")
    
    # RESULTS
    print(f"\n{'='*70}")
    print("COMPARISON RESULTS")
    print(f"{'='*70}\n")
    
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_naive_error")
        count_naive = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM test_robust_error")
        count_robust = cur.fetchone()[0]
    conn.close()
    
    total_valid = VALID_MESSAGES_BATCH * 2
    
    print(f"Valid messages sent: {total_valid}")
    print(f"Poison pills sent: 1\n")
    
    print(f"{'Approach':<35} {'Saved':<15} {'Lost':<15} {'Success Rate':<15}")
    print(f"{'-'*70}")
    print(f"{'NAIVE (Batch fail)':<35} {count_naive:<15} {total_valid - count_naive:<15} {(count_naive/total_valid*100):.1f}%")
    print(f"{'ROBUST (Row recovery)':<35} {count_robust:<15} {total_valid - count_robust:<15} {(count_robust/total_valid*100):.1f}%")

    print(f"\n{'='*70}")
    print("KEY INSIGHT")
    print(f"{'='*70}\n")
    
    if count_robust > count_naive:
        print(f"✅ Robust ingestion saved {count_robust} messages by isolating the error!")
        print(f"❌ Naive ingestion dropped the entire batch because of 1 bad record.")
    else:
        print("⚠️  Results inconclusive (timing issue?)")

    print(f"\n✓ Experiment complete!\n")
    cleanup_tables()

if __name__ == "__main__":
    main()
