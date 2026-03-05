#!/usr/bin/env python3
"""
Back-pressure Solution: Horizontal Scaling (Topic-Based Partitioning)

Demonstrates how to solve the back-pressure/overload issue by scaling out
the consumption layer using topic-based partitioning.

Strategy:
1. Publisher partitions messages across N topics by device_id hash.
2. Each consumer subscribes to its own partition topic.
3. Each consumer gets the full throughput of its MQTT client (~16k msg/s).
4. DB writes use COPY protocol with persistent connections.

Why not Shared Subscriptions?
  Shared subscriptions ($share/group/) split a single topic's throughput
  across consumers. But each paho-mqtt client is limited to ~16k msg/s
  with QoS 1. Splitting 16k/5 = 3.2k per consumer — worse than 1 consumer.
  Topic partitioning gives each consumer its own 16k msg/s channel.
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

# Experiment settings (shared with back-pressure-overload.py via .env)
FLUSH_INTERVAL_SEC = float(os.getenv("EXPERIMENT_FLUSH_INTERVAL_SEC", "5.0"))
BATCH_COUNT = int(os.getenv("EXPERIMENT_BATCH_COUNT", "5"))
TIMEOUT_SEC = int(os.getenv("EXPERIMENT_TIMEOUT_SEC", "60"))

PUBLISHER_BATCH_SIZE = int(os.getenv("EXPERIMENT_PUBLISHER_BATCH_SIZE", "100"))
# Reuse Phase 3 device count so this experiment runs the exact load that
# broke the single consumer in back-pressure-overload.py.
TARGET_DEVICES = int(os.getenv("EXPERIMENT_PHASE_3_DEVICES", "30000"))
RATE_HZ = int(os.getenv("EXPERIMENT_RATE_HZ", "1"))
PUBLISH_QOS = int(os.getenv("EXPERIMENT_SCALING_QOS", "1"))
CONSUMER_COUNT = int(os.getenv("EXPERIMENT_CONSUMER_COUNT", "3"))


def setup_tables():
    """Create test table."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        # Drop existing table
        cur.execute("DROP TABLE IF EXISTS test_backpressure_scaling CASCADE;")

        # Same schema as test_backpressure in back-pressure-overload.py
        cur.execute("""
            CREATE TABLE test_backpressure_scaling (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            );
        """)
        cur.execute("SELECT create_hypertable('test_backpressure_scaling', 'ts');")

        # Unique index enables deduplication via ON CONFLICT DO NOTHING
        cur.execute("""
            CREATE UNIQUE INDEX idx_backpressure_scaling_dedup
            ON test_backpressure_scaling (ts, device_id, metric_type);
        """)

    conn.commit()
    conn.close()


def cleanup_tables():
    """Drop test table."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_backpressure_scaling CASCADE;")
    conn.commit()
    conn.close()


def ingest_process(consumer_id, queue_out, stop_event):
    """
    Ingestion process that consumes from its own partitioned MQTT topic.

    Each consumer subscribes to: sensors/backpressure/p{consumer_id}/#
    This gives each consumer the full throughput of its MQTT client.

    Optimizations:
    - Persistent DB connection (avoids reconnect overhead per flush)
    - COPY into temp table + INSERT ON CONFLICT for fast dedup
    - Staging pattern avoids COPY's lack of ON CONFLICT support
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
            # 1. Stream into temp table (fastest possible write)
            # 2. Merge into hypertable with ON CONFLICT (honors unique index)
            with db_conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE tmp_ingest (LIKE test_backpressure_scaling INCLUDING DEFAULTS) ON COMMIT DROP")
                with cur.copy(
                    "COPY tmp_ingest (ts, device_id, metric_type, value) FROM STDIN"
                ) as copy:
                    for msg in buffer:
                        copy.write_row((msg['ts'], msg['device_id'], msg['metric'], msg['value']))
                cur.execute("""
                    INSERT INTO test_backpressure_scaling (ts, device_id, metric_type, value)
                    SELECT ts, device_id, metric_type, value FROM tmp_ingest
                    ON CONFLICT (ts, device_id, metric_type) DO NOTHING
                """)
            db_conn.commit()
        except Exception as e:
            print(f"[Consumer-{consumer_id}] Error writing batch: {e}")
            # Reconnect on failure
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
            'consumer_id': consumer_id,
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
            # PARTITIONED TOPIC: each consumer owns its partition
            topic = f"sensors/backpressure/p{consumer_id}/#"
            client.subscribe(topic)

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

    print(f"[Consumer-{consumer_id}] Starting loop...")

    while not stop_event.is_set():
        now = time.time()
        if len(buffer) > 0 and now >= next_flush:
            batch_counter += 1
            flush_buffer(batch_counter)
            next_flush = time.time() + FLUSH_INTERVAL_SEC
        time.sleep(0.01)  # 10ms poll — tighter than overload experiment (100ms) to keep latency low under high throughput

    # Final flush: drain any messages buffered in the last partial window
    if buffer:
        batch_counter += 1
        flush_buffer(batch_counter)

    client.loop_stop()
    client.disconnect()
    db_conn.close()


def publish_process(partition_id, devices_for_partition, rate_hz, stop_event, pub_queue):
    """
    Publisher for a single partition.

    Each publisher owns a partition and publishes to sensors/backpressure/p{partition_id}/data.
    This avoids the single-publisher QoS 1 bottleneck (~16k msg/s per MQTT connection).
    """
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    rate_msg_s = devices_for_partition * rate_hz
    message_count = 0
    start_time = time.time()
    batch_size = min(PUBLISHER_BATCH_SIZE, max(10, rate_msg_s // 100))

    last_msg_info = None
    while not stop_event.is_set():
        for i in range(batch_size):
            device_num = message_count % devices_for_partition
            reading = {
                'device_id': f"sensor-p{partition_id}-{device_num}",
                'metric': 'temperature',
                'value': 25.5,
                'ts': datetime.now(timezone.utc).isoformat()
            }
            last_msg_info = client.publish(f"sensors/backpressure/p{partition_id}/data", json.dumps(reading), qos=PUBLISH_QOS)
            message_count += 1

        # Drift correction
        elapsed = time.time() - start_time
        expected = message_count / rate_msg_s
        delay = expected - elapsed

        if delay > 0:
            time.sleep(delay)

    # Flush paho's outbound QoS 1 queue before stopping the network thread.
    if last_msg_info is not None:
        last_msg_info.wait_for_publish(timeout=10)
    print(f"  [Publisher-{partition_id}] Stopping...")
    client.loop_stop()
    client.disconnect()
    pub_queue.put(message_count)


def run_scaling_test():
    devices = TARGET_DEVICES
    rate_hz = RATE_HZ
    target_rate = devices * rate_hz
    devices_per_partition = devices // CONSUMER_COUNT

    print(f"\n{'='*70}")
    print(f"SCALING TEST: {devices} DEVICES @ {rate_hz}HZ (~{target_rate} msg/s)")
    print(f"Spawning {CONSUMER_COUNT} Consumers")
    print(f"{'='*70}\n")

    # Coordination
    stop_event = multiprocessing.Event()
    stats_queue = multiprocessing.Queue()
    pub_queue = multiprocessing.Queue()

    # Start Consumers (one per partition)
    consumers = []
    for i in range(CONSUMER_COUNT):
        p = multiprocessing.Process(target=ingest_process, args=(i+1, stats_queue, stop_event))
        p.start()
        consumers.append(p)

    # Wait for all consumers to connect and subscribe before publishers fire
    time.sleep(1)

    # Start Publishers (one per partition to avoid single-publisher QoS 1 bottleneck)
    publishers = []
    for i in range(CONSUMER_COUNT):
        p = multiprocessing.Process(
            target=publish_process,
            args=(i+1, devices_per_partition, rate_hz, stop_event, pub_queue)
        )
        p.start()
        publishers.append(p)

    # Monitor and Print Stats
    batches_seen = 0
    total_processed = 0

    # We run for a fixed duration of batches (scaled by consumer count)
    target_batches = BATCH_COUNT * CONSUMER_COUNT
    while batches_seen < target_batches:
        try:
            stats = stats_queue.get(timeout=TIMEOUT_SEC)
            print(f"  [Consumer-{stats['consumer_id']}] Batch {stats['batch_num']:>3}: {stats['rows']:>5} rows | Latency: {stats['latency']:>6.2f}ms")

            total_processed += stats['rows']
            batches_seen += 1
        except queue.Empty:
            print(f"  [Warning] Batch timeout")
            break

    # Cleanup Phase
    stop_event.set()

    # Collect publisher stats
    total_sent = 0
    print("  Waiting for publisher stats...")
    for _ in range(CONSUMER_COUNT):
        try:
            total_sent += pub_queue.get(timeout=10.0)
        except queue.Empty:
            break

    for p in publishers:
        if p.is_alive():
            p.join(timeout=5.0)
        if p.is_alive(): p.terminate()

    for i, p in enumerate(consumers):
        if p.is_alive():
            p.join(timeout=10.0)
        if p.is_alive(): p.terminate()

    # Drain any final-flush stats reported after the monitoring loop exited
    while True:
        try:
            stats = stats_queue.get_nowait()
            total_processed += stats['rows']
        except queue.Empty:
            break

    queue_depth = max(0, total_sent - total_processed)

    print(f"{'-'*70}")
    print("  Summary:")
    print(f"  - Messages sent:      {total_sent:,}")
    print(f"  - Messages processed: {total_processed:,}")
    print(f"  - Efficiency:         {(total_processed/total_sent*100) if total_sent > 0 else 0:.1f}%")
    print(f"  - Queue Depth:        {queue_depth:,}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    print(f"\n{'='*70}")
    print("BACK-PRESSURE / HORIZONTAL SCALING EXPERIMENT")
    print(f"{'='*70}\n")

    try:
        setup_tables()
        run_scaling_test()
    except KeyboardInterrupt:
        print("\nExperiment interrupted.")
    finally:
        cleanup_tables()
        print("\nExperiment complete.")
