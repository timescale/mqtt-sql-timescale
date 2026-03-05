"""
Unified demo script comparing naive vs batched ingestion.

This script runs both approaches sequentially and displays a comparison,
making it easy for readers to see the difference in a single run.
"""

import os
import signal
import time
import threading
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import psycopg
from db import batch_write, close_connection

# Load environment variables
load_dotenv()

# Configuration
BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
NUM_DEVICES = int(os.getenv('NUM_DEVICES', 5000))
PUBLISH_RATE_HZ = int(os.getenv('PUBLISH_RATE_HZ', 1))
FLUSH_INTERVAL_SEC = float(os.getenv('FLUSH_INTERVAL_SEC', 5.0))
TIMESCALE_DSN = os.getenv('TIMESCALE_DSN', 'postgresql://postgres:postgres@localhost:5432/sensors')
TEST_DURATION_SEC = int(os.getenv('DEMO_TEST_DURATION_SEC', 15))

# Sensor rate (total messages/second across all simulated devices)
SENSOR_RATE = NUM_DEVICES * PUBLISH_RATE_HZ

# Metrics storage
naive_metrics = {
    'messages': 0, 'total_latency': 0,
    'start_time': None, 'end_time': None, 'sensor_count': 0,
}
batched_metrics = {
    'messages': 0, 'batches': 0, 'total_latency': 0,
    'start_time': None, 'end_time': None, 'sensor_count': 0,
}


def clear_database():
    """Clear all data from sensor_readings table."""
    conn = psycopg.connect(TIMESCALE_DSN)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE sensor_readings;")
    conn.commit()
    conn.close()
    print("✓ Database cleared\n")


def simulate_sensors(duration_sec, test_name='naive'):
    """
    Simulate sensor fleet publishing at SENSOR_RATE msg/s for duration_sec.

    Uses drift-corrected sleeping so the actual publish rate matches the
    target reliably — avoids the sub-millisecond sleep problem of a naive
    fixed-interval loop.
    """
    import random
    RANGES = {
        'temperature': {'min': 18.0, 'max': 35.0},
        'pressure':    {'min': 950.0, 'max': 1050.0},
        'humidity':    {'min': 30.0,  'max': 80.0},
    }
    METRICS = list(RANGES.keys())

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"demo-sensors-{test_name}",
    )
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    time.sleep(0.5)  # Wait for connection

    count = 0
    start_time = time.time()
    last_msg_info = None

    while (time.time() - start_time) < duration_sec:
        device_id = f"sensor-{(count % NUM_DEVICES):03d}"
        metric_type = METRICS[count % len(METRICS)]
        reading = {
            'device_id': device_id,
            'metric': metric_type,
            'value': round(random.uniform(RANGES[metric_type]['min'], RANGES[metric_type]['max']), 2),
            'ts': datetime.now(timezone.utc).isoformat(),
        }
        last_msg_info = client.publish(f"sensors/{device_id}/data", json.dumps(reading), qos=1)
        count += 1

        # Drift-corrected sleep: only pause when ahead of the target rate.
        elapsed = time.time() - start_time
        expected = count / SENSOR_RATE
        drift = expected - elapsed
        if drift > 0:
            time.sleep(drift)

    # Flush paho's outbound QoS 1 queue before stopping the network thread.
    # Without this, loop_stop() drops any messages still awaiting PUBACK.
    if last_msg_info is not None:
        last_msg_info.wait_for_publish(timeout=10)
    client.loop_stop()
    client.disconnect()

    if test_name == 'naive':
        naive_metrics['sensor_count'] = count
    else:
        batched_metrics['sensor_count'] = count

    return count


def run_naive_ingestion(duration_sec):
    """
    Naive ingestion: one INSERT per message, committed immediately.
    Reports throughput and latency at each FLUSH_INTERVAL_SEC checkpoint
    so progress lines align with the batched test.
    """
    print(f"\n{'='*70}")
    print("TEST 1: NAIVE INGESTION (Per-Message Inserts)")
    print(f"{'='*70}\n")

    naive_metrics['start_time'] = time.time()

    db_conn = psycopg.connect(TIMESCALE_DSN)

    INSERT_SQL = """
    INSERT INTO sensor_readings (ts, device_id, metric_type, value)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (ts, device_id, metric_type) DO NOTHING;
    """

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/#")
            print("  Subscribed to sensors/#")

    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            start = time.time()
            with db_conn.cursor() as cur:
                cur.execute(INSERT_SQL, (
                    payload['ts'], payload['device_id'],
                    payload['metric'], payload['value'],
                ))
            db_conn.commit()
            latency_ms = (time.time() - start) * 1000
            naive_metrics['total_latency'] += latency_ms
            naive_metrics['messages'] += 1
        except Exception as e:
            print(f"  Error: {e}")

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="demo-naive",
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    # Report at each FLUSH_INTERVAL_SEC checkpoint (mirrors batched cadence)
    checkpoints = int(duration_sec / FLUSH_INTERVAL_SEC)
    for i in range(1, checkpoints + 1):
        time.sleep(FLUSH_INTERVAL_SEC)
        elapsed = time.time() - naive_metrics['start_time']
        msgs = naive_metrics['messages']
        throughput = msgs / elapsed if elapsed > 0 else 0
        avg_latency = naive_metrics['total_latency'] / msgs if msgs > 0 else 0
        print(f"  T+{elapsed:4.0f}s | Throughput: {throughput:6.1f} msg/s | "
              f"Avg latency: {avg_latency:5.2f}ms")

    naive_metrics['end_time'] = time.time()
    client.loop_stop()
    client.disconnect()
    db_conn.close()


def run_batched_ingestion(duration_sec):
    """
    Batched ingestion: buffer messages in memory, flush to DB every FLUSH_INTERVAL_SEC.
    Reports throughput and latency after each batch write.
    """
    print(f"\n{'='*70}")
    print("TEST 2: BATCHED INGESTION (Time-Window Strategy)")
    print(f"{'='*70}\n")

    batched_metrics['start_time'] = time.time()

    buffer = []
    buffer_lock = threading.Lock()
    test_complete = threading.Event()
    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    def flush_buffer():
        if test_complete.is_set() or shutdown_event.is_set():
            return

        with buffer_lock:
            if not buffer:
                if not test_complete.is_set() and not shutdown_event.is_set():
                    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
                return
            batch = buffer[:]
            buffer.clear()

        try:
            start = time.time()
            rows_written = batch_write(batch)
            latency_ms = (time.time() - start) * 1000

            batched_metrics['batches'] += 1
            batched_metrics['messages'] += rows_written
            batched_metrics['total_latency'] += latency_ms

            elapsed = time.time() - batched_metrics['start_time']
            throughput = batched_metrics['messages'] / elapsed

            print(f"  Batch {batched_metrics['batches']:3d} | "
                  f"Throughput: {throughput:6.1f} msg/s | "
                  f"Latency: {latency_ms:6.2f}ms")
        except Exception as e:
            print(f"  Error: {e}")

        if not test_complete.is_set() and not shutdown_event.is_set():
            threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("sensors/#")
            print("  Subscribed to sensors/#")

    def on_message(client, userdata, message, properties=None):
        try:
            payload = json.loads(message.payload.decode())
            with buffer_lock:
                buffer.append((
                    payload['ts'], payload['device_id'],
                    payload['metric'], payload['value'],
                ))
        except Exception as e:
            print(f"  Error: {e}")

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="demo-batched",
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()

    # Run for duration_sec, or exit early if a shutdown signal is received.
    deadline = time.time() + duration_sec
    while time.time() < deadline and not shutdown_event.is_set():
        time.sleep(0.1)

    batched_metrics['end_time'] = time.time()
    test_complete.set()

    # Final flush: capture messages received during the last partial window
    time.sleep(0.5)
    with buffer_lock:
        if buffer:
            try:
                rows_written = batch_write(buffer)
                batched_metrics['batches'] += 1
                batched_metrics['messages'] += rows_written
                elapsed = time.time() - batched_metrics['start_time']
                latency_ms = 0  # final flush latency not tracked separately
                throughput = batched_metrics['messages'] / elapsed
                print(f"  Batch {batched_metrics['batches']:3d} | "
                      f"Throughput: {throughput:6.1f} msg/s | "
                      f"(final flush)")
            except Exception as e:
                print(f"  Warning: Final flush failed: {e}")

    client.loop_stop()
    client.disconnect()


def print_comparison():
    """Print the three-metric comparison table and key insight."""
    sensor_rate = SENSOR_RATE
    total_sent = sensor_rate * TEST_DURATION_SEC

    naive_stored = naive_metrics['messages']
    batched_stored = batched_metrics['messages']

    naive_backlog = total_sent - naive_stored
    batched_backlog = total_sent - batched_stored

    naive_duration = naive_metrics['end_time'] - naive_metrics['start_time']
    batched_duration = batched_metrics['end_time'] - batched_metrics['start_time']
    naive_throughput = naive_stored / naive_duration if naive_duration > 0 else 0
    batched_throughput = batched_stored / batched_duration if batched_duration > 0 else 0

    naive_avg_latency = (naive_metrics['total_latency'] / naive_stored
                         if naive_stored > 0 else 0)
    batched_avg_latency = (batched_metrics['total_latency'] / batched_metrics['batches']
                           if batched_metrics['batches'] > 0 else 0)

    print(f"\n{'='*70}")
    print("COMPARISON RESULTS")
    print(f"{'='*70}\n")
    print(f"  Sensor rate: {sensor_rate:,} msg/s  |  "
          f"Duration: {TEST_DURATION_SEC}s  |  "
          f"Total fired: {total_sent:,}\n")

    col = 22
    print(f"  {'Metric':<28} {'Naive':>{col}} {'Batched':>{col}}")
    print(f"  {'-'*72}")
    print(f"  {'Stored in DB @ T={:d}s'.format(TEST_DURATION_SEC):<28} "
          f"{naive_stored:>{col},} {batched_stored:>{col},}")
    print(f"  {'Queue backlog':<28} "
          f"{naive_backlog:>{col},} {batched_backlog:>{col},}")
    print(f"  {'Throughput (msg/s)':<28} "
          f"{naive_throughput:>{col}.1f} {batched_throughput:>{col}.1f}")
    print(f"  {'Avg write latency (ms)':<28} "
          f"{naive_avg_latency:>{col}.2f} {batched_avg_latency:>{col}.2f}")

    print(f"\n{'='*70}")
    print("KEY INSIGHT")
    print(f"{'='*70}\n")

    if naive_backlog > 0 and naive_throughput > 0:
        print(f"  The naive pipeline stored {naive_stored:,} of {total_sent:,} messages "
              f"({naive_stored/total_sent*100:.1f}%).")
        print(f"  Its backlog of {naive_backlog:,} messages grows by "
              f"~{(sensor_rate - naive_throughput):,.0f} msg/s — it will never recover.")
        print(f"\n  The batched pipeline stored {batched_stored:,} of {total_sent:,} messages "
              f"({batched_stored/total_sent*100:.1f}%).")


def main():
    """Run the demo."""
    print(f"\n{'='*70}")
    print("MQTT-TO-SQL BATCHING DEMO")
    print(f"{'='*70}\n")
    print(f"Configuration:")
    print(f"  Simulated sensors: {NUM_DEVICES}")
    print(f"  Publish rate:      {PUBLISH_RATE_HZ} Hz per sensor")
    print(f"  Total rate:        {SENSOR_RATE:,} messages/second")
    print(f"  Test duration:     {TEST_DURATION_SEC} seconds per test")
    print(f"  Flush interval:    {FLUSH_INTERVAL_SEC} seconds\n")

    print("Preparing database...")
    clear_database()

    # Test 1: Naive ingestion
    print("Starting Test 1: Naive Ingestion...")
    sensor_thread = threading.Thread(
        target=simulate_sensors, args=(TEST_DURATION_SEC, 'naive')
    )
    sensor_thread.start()
    run_naive_ingestion(TEST_DURATION_SEC)
    sensor_thread.join()

    # Clear database for second test
    print("\nPreparing for Test 2...")
    clear_database()

    # Test 2: Batched ingestion
    print("Starting Test 2: Batched Ingestion...")
    sensor_thread = threading.Thread(
        target=simulate_sensors, args=(TEST_DURATION_SEC, 'batched')
    )
    sensor_thread.start()
    run_batched_ingestion(TEST_DURATION_SEC)
    sensor_thread.join()

    print_comparison()
    close_connection()
    print(f"\n✓ Demo complete!\n")


if __name__ == '__main__':
    main()
