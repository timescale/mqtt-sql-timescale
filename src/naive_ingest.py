"""
Naive MQTT-to-SQL ingestion script (per-message inserts).

This script demonstrates THE PROBLEM: inserting each MQTT message individually
creates massive overhead and fails under production load.

EDUCATIONAL PURPOSE: This script exists to show what NOT to do.
Run this alongside the publisher to observe degradation at scale.
"""

import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import psycopg

# Load environment variables
load_dotenv()

# Configuration
BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
MQTT_TOPIC = os.getenv('MQTT_TOPIC', 'sensors/#')
TIMESCALE_DSN = os.getenv('TIMESCALE_DSN', 'postgresql://postgres:postgres@localhost:5432/sensors')
NAIVE_LOG_INTERVAL = int(os.getenv('NAIVE_LOG_INTERVAL', 50))

# Metrics
message_count = 0
total_latency = 0.0

# Database connection
db_conn = None

INSERT_SQL = """
INSERT INTO sensor_readings (ts, device_id, metric_type, value)
VALUES (%s, %s, %s, %s)
ON CONFLICT (ts, device_id, metric_type) DO NOTHING;
"""


def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker."""
    if reason_code == 0:
        print(f"✓ Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"✓ Subscribed to {MQTT_TOPIC}")
    else:
        print(f"✗ Connection failed: {reason_code}")


def on_message(client, userdata, message, properties=None):
    """
    Callback for each MQTT message.
    
    EDUCATIONAL NOTE: This inserts EACH message individually.
    This is the anti-pattern we're demonstrating.
    """
    global message_count, total_latency, db_conn
    
    try:
        # Parse message
        payload = json.loads(message.payload.decode())
        
        # Extract fields
        ts = payload['ts']
        device_id = payload['device_id']
        metric_type = payload['metric']
        value = payload['value']
        
        # TIME THE INSERT - this is the key metric
        start_time = time.time()
        
        # Insert ONE row (this is the problem!)
        with db_conn.cursor() as cur:
            cur.execute(INSERT_SQL, (ts, device_id, metric_type, value))
        db_conn.commit()
        
        # Calculate latency
        latency_ms = (time.time() - start_time) * 1000
        total_latency += latency_ms
        
        message_count += 1
        
        # Log every N messages
        if message_count % NAIVE_LOG_INTERVAL == 0:
            avg_latency = total_latency / message_count
            print(f"Messages: {message_count:5d} | Avg insert latency: {avg_latency:6.2f}ms | Last: {latency_ms:6.2f}ms")
    
    except Exception as e:
        print(f"Error processing message: {e}")


def main():
    """Main ingestion loop."""
    global db_conn
    
    print("=" * 70)
    print("NAIVE INGESTION (Per-Message Inserts)")
    print("=" * 70)
    print("\nEDUCATIONAL NOTE: This script demonstrates the PROBLEM.")
    print("Watch the insert latency grow as message rate increases.\n")
    
    # Connect to database
    print(f"Connecting to TimescaleDB...")
    db_conn = psycopg.connect(TIMESCALE_DSN)
    print(f"✓ Connected to database\n")
    
    # Create MQTT client
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="naive-ingestion"
    )
    client.on_connect = on_connect
    client.on_message = on_message
    
    # Connect and start loop
    print(f"Connecting to MQTT broker at {BROKER_HOST}:{BROKER_PORT}...")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    
    print("\nPress Ctrl+C to stop\n")
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n\nStopping ingestion...")
        print(f"Total messages processed: {message_count}")
        if message_count > 0:
            print(f"Average insert latency: {total_latency / message_count:.2f}ms")
        
        client.disconnect()
        db_conn.close()


if __name__ == '__main__':
    main()
