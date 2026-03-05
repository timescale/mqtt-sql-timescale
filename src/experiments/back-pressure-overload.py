#!/usr/bin/env python3
"""
Back-pressure / Load Test Experiment

Demonstrates system behavior under increasing load to identify when
back-pressure handling becomes necessary.

Runs 3 phases (configurable via .env):
1. Baseline:    Low load    (Default: 1,000 devices @ 1Hz =  1k msg/s)
2. High Load:   Medium load (Default: 5,000 devices @ 1Hz =  5k msg/s)
3. Stress Test: Overload    (Default: 30,000 devices @ 1Hz = 30k msg/s)

Phase 2 matches the main demo's load (per-message-insert.py) to show
the single consumer handling it comfortably. Phase 3 deliberately
overwhelms it, motivating the horizontal scaling experiment.

Output shows per-batch statistics to visualize latency degradation.
"""

import json
import multiprocessing
import os
import time
import queue
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import psycopg
from dotenv import load_dotenv

load_dotenv()

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
TIMESCALE_DSN = os.getenv("TIMESCALE_DSN")

# Experiment settings
FLUSH_INTERVAL_SEC = float(os.getenv("EXPERIMENT_FLUSH_INTERVAL_SEC", "5.0"))
BATCH_COUNT = int(os.getenv("EXPERIMENT_BATCH_COUNT", "5"))
TIMEOUT_SEC = int(os.getenv("EXPERIMENT_TIMEOUT_SEC", "60"))

PUBLISHER_BATCH_SIZE = int(os.getenv("EXPERIMENT_PUBLISHER_BATCH_SIZE", "100"))
PHASE_1_DEVICES = int(os.getenv("EXPERIMENT_PHASE_1_DEVICES", "1000"))
PHASE_2_DEVICES = int(os.getenv("EXPERIMENT_PHASE_2_DEVICES", "5000"))
PHASE_3_DEVICES = int(os.getenv("EXPERIMENT_PHASE_3_DEVICES", "30000"))
RATE_HZ = int(os.getenv("EXPERIMENT_RATE_HZ", "1"))
PUBLISH_QOS = int(os.getenv("EXPERIMENT_OVERLOAD_QOS", "1"))


def setup_tables():
    """Create test table."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        # Drop existing table
        cur.execute("DROP TABLE IF EXISTS test_backpressure CASCADE;")
        
        # Create table
        cur.execute("""
            CREATE TABLE test_backpressure (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            );
        """)
        cur.execute("SELECT create_hypertable('test_backpressure', 'ts');")

        # Unique index enables deduplication via ON CONFLICT DO NOTHING
        cur.execute("""
            CREATE UNIQUE INDEX idx_backpressure_dedup
            ON test_backpressure (ts, device_id, metric_type);
        """)
    
    conn.commit()
    conn.close()


def cleanup_tables():
    """Drop test table."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_backpressure CASCADE;")
    conn.commit()
    conn.close()


def ingest_process(queue_out, stop_event):
    """
    Ingestion process that consumes from MQTT and writes to DB using batching.
    Reports batch stats to the main process via queue.

    Uses COPY + staging table + INSERT ON CONFLICT for fast dedup.
    """
    buffer = []

    # Persistent DB connection for this consumer's lifetime
    db_conn = psycopg.connect(TIMESCALE_DSN)

    # Metrics
    last_flush_time = time.time()

    def flush_buffer(batch_num):
        nonlocal last_flush_time, db_conn
        if not buffer:
            return

        start_write = time.time()

        try:
            # COPY + staging table: fast bulk load with deduplication
            with db_conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE tmp_ingest (LIKE test_backpressure INCLUDING DEFAULTS) ON COMMIT DROP")
                with cur.copy(
                    "COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
                ) as copy:
                    for msg in buffer:
                        copy.write_row((msg['ts'], msg['device_id'], msg['metric'], msg['value']))
                cur.execute("""
                    INSERT INTO test_backpressure (ts, device_id, metric_type, value)
                    SELECT ts, device_id, metric_type, value FROM tmp_ingest
                    ON CONFLICT (ts, device_id, metric_type) DO NOTHING
                """)
            db_conn.commit()
        except Exception as e:
            print(f"Error writing batch: {e}")
            try:
                db_conn.close()
            except Exception:
                pass
            db_conn = psycopg.connect(TIMESCALE_DSN)

        end_write = time.time()

        # Calculate stats
        write_latency_ms = (end_write - start_write) * 1000
        duration_s = end_write - last_flush_time
        throughput = len(buffer) / duration_s if duration_s > 0 else 0

        # Report stats
        stats = {
            'batch_num': batch_num,
            'rows': len(buffer),
            'throughput': throughput,
            'latency': write_latency_ms
        }
        queue_out.put(stats)

        # Reset
        buffer.clear()
        last_flush_time = end_write

    # MQTT Callbacks
    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/backpressure/#")

    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            buffer.append(payload)
        except Exception:
            pass

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    # Main loop
    batch_counter = 0
    next_flush = time.time() + FLUSH_INTERVAL_SEC

    while not stop_event.is_set():
        now = time.time()
        if len(buffer) > 0 and now >= next_flush:
            batch_counter += 1
            flush_buffer(batch_counter)
            next_flush = time.time() + FLUSH_INTERVAL_SEC
        time.sleep(0.1)

    # Final flush: drain any messages buffered in the last partial window
    if buffer:
        batch_counter += 1
        flush_buffer(batch_counter)

    client.loop_stop()
    client.disconnect()
    db_conn.close()


def publish_process(devices, rate_hz, stop_event, pub_queue):
    """
    Drift-corrected publisher.

    Publishes PUBLISHER_BATCH_SIZE messages per loop iteration, then sleeps
    only if ahead of the wall-clock target. This avoids the sub-millisecond
    sleep granularity problem of sleeping once per message.
    """
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    rate_msg_s = devices * rate_hz
    message_count = 0
    start_time = time.time()

    last_msg_info = None
    while not stop_event.is_set():
        for i in range(PUBLISHER_BATCH_SIZE):
            reading = {
                'device_id': f"sensor-{message_count % devices}", # Rotate through configured device count
                'metric': 'temperature',
                'value': 25.5,
                'ts': datetime.now(timezone.utc).isoformat()
            }
            last_msg_info = client.publish("sensors/backpressure/data", json.dumps(reading), qos=PUBLISH_QOS)
            message_count += 1

        # Drift correction
        elapsed = time.time() - start_time
        expected = message_count / rate_msg_s
        delay = expected - elapsed

        if delay > 0:
            time.sleep(delay)

    # Flush paho's outbound QoS 1 queue before stopping the network thread.
    # Under extreme overload the broker queue may be full (ERR_QUEUE_SIZE), so guard.
    if last_msg_info is not None:
        try:
            last_msg_info.wait_for_publish(timeout=10)
        except (ValueError, RuntimeError):
            pass  # Queue full under overload — expected in stress phase
    print("  [Publisher] Stopping...")
    client.loop_stop()
    client.disconnect()
    
    # Report final count
    pub_queue.put(message_count)


def run_phase(phase_num, devices, rate_hz):
    target_rate = devices * rate_hz
    print(f"\n{'='*70}")
    print(f"BATCH INGESTION TEST {phase_num}: {devices} DEVICES @ {rate_hz}HZ (~{target_rate} msg/s)")
    print(f"{'='*70}\n")
    print(f"  Subscribed to sensors/backpressure/#")
    
    # Coordination
    stop_event = multiprocessing.Event()
    stats_queue = multiprocessing.Queue()
    pub_queue = multiprocessing.Queue()
    
    # Start Consumer
    consumer = multiprocessing.Process(target=ingest_process, args=(stats_queue, stop_event))
    consumer.start()

    # Wait for consumer to connect and subscribe before publisher fires
    time.sleep(1)

    # Start Publisher
    publisher = multiprocessing.Process(target=publish_process, args=(devices, rate_hz, stop_event, pub_queue))
    publisher.start()
    
    # Monitor and Print Stats
    batches_seen = 0
    total_processed = 0
    
    while batches_seen < BATCH_COUNT:
        try:
            stats = stats_queue.get(timeout=TIMEOUT_SEC) # Configurable timeout
            print(f"  Batch {stats['batch_num']:>3}: {stats['rows']:>5} rows | Throughput: {stats['throughput']:>6.1f} msg/s | Latency: {stats['latency']:>6.2f}ms")
            
            total_processed += stats['rows']
            batches_seen += 1
        except queue.Empty:
            print(f"  [Warning] Batch timeout - system may be overloaded (latency > {TIMEOUT_SEC}s)")
            batches_seen += 1 # Advance anyway to avoid infinite hang
            
    # Cleanup Phase
    stop_event.set()
    
    # Get actual total sent from publisher queue (wait for it before joining)
    try:
        print("  Waiting for publisher stats...")
        total_sent = pub_queue.get(timeout=10.0)
    except queue.Empty:
        print("  [Warning] Publisher stats timeout")
        total_sent = 0
        
    # Wait for processes to exit gracefully, else force kill
    if publisher.is_alive():
        print("  Waiting for publisher to exit...")
        publisher.join(timeout=5.0)
    if publisher.is_alive():
        print("  [Warning] Publisher stuck, forcing kill.")
        publisher.terminate()
        
    if consumer.is_alive():
        print("  Waiting for consumer to exit...")
        consumer.join(timeout=10.0)
    if consumer.is_alive():
        print("  [Warning] Consumer stuck, forcing kill.")
        consumer.terminate()

    # Drain any final-flush stats reported after the monitoring loop exited
    while True:
        try:
            stats = stats_queue.get_nowait()
            total_processed += stats['rows']
        except queue.Empty:
            break

    # Calculate final queue depth (lag)
    # If processed < sent, the difference is what's left in the queue/buffer
    queue_depth = max(0, total_sent - total_processed)
    
    print(f"{'-'*70}")
    print("  Summary:")
    print(f"  - Messages sent:      {total_sent:,}")
    print(f"  - Messages processed: {total_processed:,}")
    print(f"  - Efficiency:         {(total_processed/total_sent*100) if total_sent > 0 else 0:.1f}%")
    print(f"  - Queue Depth:        {queue_depth:,}")
    print(f"{'='*70}\n")


def main():
    print(f"\n{'='*70}")
    print("BACK-PRESSURE / LOAD TEST EXPERIMENT")
    print(f"{'='*70}\n")
    print("Simulating increasing load to demonstrate system limits.\n")
    
    try:
        setup_tables()
        
        # Test 1: Low Load
        run_phase(1, devices=PHASE_1_DEVICES, rate_hz=RATE_HZ)
        time.sleep(2) # Cooldown
        
        # Test 2: High Load
        run_phase(2, devices=PHASE_2_DEVICES, rate_hz=RATE_HZ)
        time.sleep(2) # Cooldown
        
        # Test 3: Stress Test
        run_phase(3, devices=PHASE_3_DEVICES, rate_hz=RATE_HZ)
        
    except KeyboardInterrupt:
        print("\nExperiment interrupted.")
    finally:
        cleanup_tables()
        print("\nExperiment complete.")

if __name__ == "__main__":
    main()
