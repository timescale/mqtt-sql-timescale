#!/usr/bin/env python3
"""
Graceful Shutdown Experiment

Demonstrates the importance of signal handlers by running two parallel
ingestion processes:
1. WITHOUT graceful shutdown (loses buffered data on Ctrl+C)
2. WITH graceful shutdown (flushes buffer before exit)

User presses Ctrl+C to interrupt both, then we compare results.
"""

import json
import multiprocessing
import os
import signal
import sys
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
# Intentionally high so buffer is guaranteed full when Ctrl+C is pressed
FLUSH_INTERVAL_SEC = float(os.getenv("EXPERIMENT_GRACEFUL_FLUSH_SEC", "30.0"))
PUBLISH_RATE = int(os.getenv("EXPERIMENT_PUBLISH_RATE", "5000"))
TEST_DURATION = int(os.getenv("EXPERIMENT_TEST_DURATION", "10"))


def setup_tables():
    """Create test tables for the experiment."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        # Drop and recreate tables
        cur.execute("DROP TABLE IF EXISTS test_without_graceful CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_with_graceful CASCADE;")
        
        # Create both tables with same schema
        for table in ["test_without_graceful", "test_with_graceful"]:
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
        cur.execute("DROP TABLE IF EXISTS test_without_graceful CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_with_graceful CASCADE;")
    conn.commit()
    conn.close()


def publish_messages(duration_sec, stop_event):
    """Publish messages for specified duration using drift-corrected rate control."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="graceful-shutdown-publisher"
    )

    def on_connect(c, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            print("  Publisher connected to MQTT broker")

    client.on_connect = on_connect
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    time.sleep(0.5)

    message_count = 0
    start_time = time.time()
    last_msg_info = None

    while not stop_event.is_set() and (time.time() - start_time) < duration_sec:
        reading = {
            'device_id': f"sensor-{message_count % 10:03d}",
            'metric': 'temperature',
            'value': 20.0 + (message_count % 100) / 10.0,
            'ts': datetime.now(timezone.utc).isoformat()
        }

        last_msg_info = client.publish("sensors/test/data", json.dumps(reading), qos=1)
        message_count += 1

        # Drift-corrected sleep: only pause when ahead of the target rate.
        elapsed = time.time() - start_time
        expected = message_count / PUBLISH_RATE
        drift = expected - elapsed
        if drift > 0:
            time.sleep(drift)

    # Flush paho's outbound QoS 1 queue before stopping the network thread.
    if last_msg_info is not None:
        last_msg_info.wait_for_publish(timeout=10)
    client.loop_stop()
    client.disconnect()
    return message_count


def ingest_without_graceful(table_name):
    """Ingest WITHOUT graceful shutdown - loses buffer on exit."""
    buffer = []
    buffer_lock = threading.Lock()
    test_complete = threading.Event()

    db_conn = psycopg.connect(TIMESCALE_DSN)

    def batch_write(batch):
        nonlocal db_conn
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    f"CREATE TEMP TABLE tmp_ingest (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                with cur.copy(
                    f"COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
                ) as copy:
                    for msg in batch:
                        copy.write_row((msg['ts'], msg['device_id'], msg['metric'], msg['value']))
                cur.execute(f"""
                    INSERT INTO {table_name} (ts, device_id, metric_type, value)
                    SELECT ts, device_id, metric_type, value FROM tmp_ingest
                """)
            db_conn.commit()
        except Exception as e:
            print(f"  [WITHOUT] Batch write error: {e}")
            db_conn.rollback()
            raise
        return len(batch)
    
    def flush_buffer():
        if test_complete.is_set():
            return

        with buffer_lock:
            if not buffer:
                if not test_complete.is_set():
                    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
                return
            batch = buffer[:]
            buffer.clear()

        try:
            batch_write(batch)
        except Exception as e:
            print(f"  [WITHOUT] Error: {e}")

        if not test_complete.is_set():
            threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/test/data")

    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append(payload)
        except Exception as e:
            print(f"  [WITHOUT] Error: {e}")

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="ingest-without-graceful"
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    # Start flush timer
    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()

    # Wait indefinitely (will be killed by SIGINT)
    while True:
        time.sleep(1)


def ingest_with_graceful(table_name):
    """Ingest WITH graceful shutdown - flushes buffer on exit."""
    buffer = []
    buffer_lock = threading.Lock()
    test_complete = threading.Event()
    shutdown_event = threading.Event()

    db_conn = psycopg.connect(TIMESCALE_DSN)

    def batch_write(batch):
        nonlocal db_conn
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    f"CREATE TEMP TABLE tmp_ingest (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                with cur.copy(
                    f"COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
                ) as copy:
                    for msg in batch:
                        copy.write_row((msg['ts'], msg['device_id'], msg['metric'], msg['value']))
                cur.execute(f"""
                    INSERT INTO {table_name} (ts, device_id, metric_type, value)
                    SELECT ts, device_id, metric_type, value FROM tmp_ingest
                """)
            db_conn.commit()
        except Exception as e:
            print(f"  [WITH] Batch write error: {e}")
            db_conn.rollback()
            raise
        return len(batch)
    
    def signal_handler(signum, frame):
        """Handle shutdown gracefully."""
        print("\n  [WITH] Caught signal! Flushing buffer...")
        shutdown_event.set()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    def flush_buffer():
        if test_complete.is_set() or shutdown_event.is_set():
            return

        with buffer_lock:
            if not buffer:
                if not (test_complete.is_set() or shutdown_event.is_set()):
                    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
                return
            batch = buffer[:]
            buffer.clear()

        try:
            batch_write(batch)
        except Exception as e:
            print(f"  [WITH] Error: {e}")

        if not (test_complete.is_set() or shutdown_event.is_set()):
            threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
    
    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/test/data")
    
    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append(payload)
        except Exception as e:
            print(f"  [WITH] Error: {e}")
    
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="ingest-with-graceful"
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    
    # Start flush timer
    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
    
    # Wait for shutdown signal
    while not shutdown_event.is_set():
        time.sleep(0.1)
    
    # Final flush
    with buffer_lock:
        if buffer:
            try:
                rows = batch_write(buffer)
                print(f"  [WITH] Flushed {rows} buffered messages")
            except Exception as e:
                print(f"  [WITH] Final flush error: {e}")
    
    client.loop_stop()
    client.disconnect()


def main():
    print(f"\n{'='*70}")
    print("GRACEFUL SHUTDOWN EXPERIMENT")
    print(f"{'='*70}\n")
    
    print("This experiment demonstrates why signal handlers are critical.\n")
    print("Setup:")
    print(f"  • Publisher rate: {PUBLISH_RATE} msg/s")
    print(f"  • Flush interval: {FLUSH_INTERVAL_SEC}s")
    print(f"  • Test runs for {TEST_DURATION}s, then you interrupt")
    print(f"  • Two parallel ingest processes:")
    print(f"    - WITHOUT graceful shutdown (loses buffer)")
    print(f"    - WITH graceful shutdown (flushes buffer)\n")
    
    # Setup
    setup_tables()
    
    # Start both ingest processes
    print("Starting ingestion processes...")
    process_without = multiprocessing.Process(
        target=ingest_without_graceful,
        args=("test_without_graceful",)
    )
    process_with = multiprocessing.Process(
        target=ingest_with_graceful,
        args=("test_with_graceful",)
    )
    
    process_without.start()
    process_with.start()
    time.sleep(1)
    
    # Start publisher
    print("Starting publisher...\n")
    stop_event = threading.Event()
    published_count = multiprocessing.Value('i', 0)  # Shared counter
    
    def publisher_wrapper():
        count = publish_messages(TEST_DURATION, stop_event)
        published_count.value = count
    
    publisher_thread = threading.Thread(target=publisher_wrapper)
    publisher_thread.start()
    
    # Wait for test duration
    time.sleep(TEST_DURATION)
    
    # Prompt user
    print(f"\n{'='*70}")
    print("👉 PRESS Ctrl+C NOW TO INTERRUPT BOTH PROCESSES")
    print(f"{'='*70}\n")
    
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n\nInterrupting processes...")
        
        # Kill WITHOUT process immediately (no cleanup)
        print("  Terminating WITHOUT graceful shutdown (hard kill)...")
        process_without.kill()  # SIGKILL - no cleanup possible
        
        # Send SIGTERM to WITH process (will be caught by signal handler)
        print("  Terminating WITH graceful shutdown (signal handler)...")
        process_with.terminate()  # SIGTERM - caught by handler
        
        # Wait for them to finish
        process_without.join(timeout=5)
        process_with.join(timeout=5)
        
        # Stop publisher
        stop_event.set()
        publisher_thread.join()
    
    # Give database time to complete the final flush and settle
    print("  Waiting for final flush to complete...")
    for i in range(5, 0, -1):
        print(f"    {i}s remaining...", end='\r')
        time.sleep(1)
    print("    Complete!       ")
    
    # Compare results
    print(f"\n{'='*70}")
    print("COMPARISON RESULTS")
    print(f"{'='*70}\n")
    
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_without_graceful")
        count_without = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM test_with_graceful")
        count_with = cur.fetchone()[0]
    conn.close()
    
    actual_published = published_count.value
    loss_without = actual_published - count_without
    loss_with = actual_published - count_with
    
    print(f"Test Duration: {TEST_DURATION} seconds")
    print(f"Publisher sent: {actual_published:,} messages (actual)\n")
    
    print(f"{'Approach':<35} {'Saved':<15} {'Lost':<15} {'Success Rate':<15}")
    print(f"{'-'*70}")
    print(f"{'WITHOUT graceful shutdown':<35} {count_without:<15,} {loss_without:<15,} {(count_without/actual_published*100 if actual_published > 0 else 0):<14.1f}%")
    print(f"{'WITH graceful shutdown':<35} {count_with:<15,} {loss_with:<15,} {(count_with/actual_published*100 if actual_published > 0 else 0):<14.1f}%")
    
    print(f"\n{'='*70}")
    print("KEY INSIGHT")
    print(f"{'='*70}\n")
    
    if loss_without > loss_with:
        print(f"✅ Signal handler prevented loss of ~{loss_without - loss_with:,} buffered messages!")
    else:
        print("⚠️  Both processes lost similar amounts (timing dependent)")
    
    print(f"\n✓ Experiment complete!\n")
    
    # Cleanup
    cleanup_tables()


if __name__ == "__main__":
    main()
