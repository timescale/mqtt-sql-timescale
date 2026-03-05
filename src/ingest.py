"""
Batched MQTT-to-SQL ingestion script with time-window strategy.

This script demonstrates THE SOLUTION: buffering MQTT messages and flushing
them in batches at regular intervals using psycopg3 pipeline mode.

EDUCATIONAL PURPOSE: Compare this to naive_ingest.py to see the performance difference.
"""

import os
import json
import time
import signal
import threading
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from db import batch_write, close_connection

# Load environment variables
load_dotenv()

# Configuration
BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'sensors/#')
FLUSH_INTERVAL_SEC = float(os.getenv('FLUSH_INTERVAL_SEC', 5.0))

# Shared buffer and lock
buffer = []
buffer_lock = threading.Lock()

# Metrics
message_count = 0
batch_count = 0
total_rows_written = 0

# Shutdown flag
shutdown_flag = threading.Event()


def flush_buffer():
    """
    Flush the buffer to the database.
    
    EDUCATIONAL NOTE: Lock-per-flush-cycle pattern.
    We copy the buffer under lock, then flush outside the lock.
    This minimizes lock contention - the MQTT callback can keep appending
    while we're doing I/O.
    """
    global batch_count, total_rows_written
    
    # Don't schedule next flush if shutting down
    if shutdown_flag.is_set():
        return
    
    # Copy buffer under lock
    with buffer_lock:
        if not buffer:
            # Schedule next flush and return
            if not shutdown_flag.is_set():
                threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
            return
        
        batch = buffer[:]
        buffer.clear()
    
    # Flush outside lock (I/O doesn't block message receipt)
    try:
        start_time = time.time()
        rows_written = batch_write(batch)
        latency_ms = (time.time() - start_time) * 1000
        
        batch_count += 1
        total_rows_written += rows_written
        
        print(f"Batch {batch_count:4d}: {rows_written:4d} rows | Latency: {latency_ms:6.2f}ms | Buffer size: {len(buffer):4d}")
    
    except Exception as e:
        print(f"Error flushing batch: {e}")
    
    # Schedule next flush
    if not shutdown_flag.is_set():
        threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()


def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker."""
    if reason_code == 0:
        print(f"✓ Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"✓ Subscribed to {MQTT_TOPIC}\n")
    else:
        print(f"✗ Connection failed: {reason_code}")


def on_message(client, userdata, message, properties=None):
    """
    Callback for each MQTT message.
    
    EDUCATIONAL NOTE: We just append to the buffer here.
    The flush happens on a timer, not per message.
    """
    global message_count
    
    try:
        # Parse message
        payload = json.loads(message.payload.decode())
        
        # Extract fields
        ts = payload['ts']
        device_id = payload['device_id']
        metric_type = payload['metric']
        value = payload['value']
        
        # Append to buffer (thread-safe)
        with buffer_lock:
            buffer.append((ts, device_id, metric_type, value))
        
        message_count += 1
    
    except Exception as e:
        print(f"Error processing message: {e}")


def signal_handler(signum, frame):
    """
    Handle shutdown signals (SIGINT/SIGTERM).
    
    EDUCATIONAL NOTE: Graceful shutdown is critical.
    We need to flush any remaining buffered data before exiting.
    """
    print("\n\nReceived shutdown signal. Flushing remaining buffer...")
    
    # Set shutdown flag to stop timer
    shutdown_flag.set()
    
    # Flush remaining buffer
    with buffer_lock:
        if buffer:
            batch = buffer[:]
            buffer.clear()
            
            try:
                rows_written = batch_write(batch)
                print(f"✓ Flushed {rows_written} remaining rows")
            except Exception as e:
                print(f"✗ Error flushing final batch: {e}")
    
    # Print summary
    print(f"\nSummary:")
    print(f"  Messages received: {message_count}")
    print(f"  Batches written: {batch_count}")
    print(f"  Total rows written: {total_rows_written}")
    
    # Close database connection
    close_connection()
    
    # Exit
    os._exit(0)


def main():
    """Main ingestion loop."""
    print("=" * 70)
    print("BATCHED INGESTION (Time-Window Strategy)")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Flush interval: {FLUSH_INTERVAL_SEC} seconds")
    print(f"  Strategy: Time-window (flush every N seconds)\n")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create MQTT client
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="batched-ingestion"
    )
    client.on_connect = on_connect
    client.on_message = on_message
    
    # Connect to broker
    print(f"Connecting to MQTT broker at {BROKER_HOST}:{BROKER_PORT}...")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    
    # Start flush timer
    print(f"Starting flush timer (every {FLUSH_INTERVAL_SEC}s)...")
    threading.Timer(FLUSH_INTERVAL_SEC, flush_buffer).start()
    
    print("\nPress Ctrl+C to stop\n")
    
    # Start MQTT loop
    client.loop_forever()


if __name__ == '__main__':
    main()
