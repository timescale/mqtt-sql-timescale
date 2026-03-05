"""
Synthetic MQTT publisher for sensor data.

Generates realistic sensor readings (temperature, pressure, humidity) and publishes
to MQTT broker at configurable rate.

EDUCATIONAL NOTE: Using simple random values for clarity.
Production: Consider random walk or actual sensor simulation for smoother data.
"""

import os
import json
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# Load environment variables
load_dotenv()

# Configuration
BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
NUM_DEVICES = int(os.getenv('NUM_DEVICES', 50))
PUBLISH_RATE_HZ = int(os.getenv('PUBLISH_RATE_HZ', 10))

# Sensor value ranges (realistic industrial ranges)
RANGES = {
    'temperature': {'min': 18.0, 'max': 35.0, 'unit': '°C'},
    'pressure': {'min': 950.0, 'max': 1050.0, 'unit': 'hPa'},
    'humidity': {'min': 30.0, 'max': 80.0, 'unit': '%'}
}

METRICS = list(RANGES.keys())


def generate_reading(device_id, metric_type):
    """
    Generate a synthetic sensor reading.
    
    EDUCATIONAL NOTE: Using simple random values within realistic bounds.
    Production: Consider random walk for smoother value transitions.
    """
    value = random.uniform(
        RANGES[metric_type]['min'],
        RANGES[metric_type]['max']
    )
    
    return {
        'device_id': device_id,
        'metric': metric_type,
        'value': round(value, 2),
        'ts': datetime.now(timezone.utc).isoformat()
    }


def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker."""
    if reason_code == 0:
        print(f"✓ Connected to MQTT broker at {BROKER_HOST}:{BROKER_PORT}")
    else:
        print(f"✗ Connection failed with code: {reason_code}")


def main():
    """Main publisher loop."""
    # Create MQTT client with v2 callback API
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="sensor-publisher"
    )
    client.on_connect = on_connect
    
    # Connect to broker
    print(f"Connecting to MQTT broker at {BROKER_HOST}:{BROKER_PORT}...")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    
    # Wait for connection
    time.sleep(1)
    
    # Calculate sleep time between publishes
    total_messages_per_second = NUM_DEVICES * PUBLISH_RATE_HZ
    sleep_time = 1.0 / total_messages_per_second
    
    print(f"\nPublishing sensor data:")
    print(f"  Devices: {NUM_DEVICES}")
    print(f"  Rate: {PUBLISH_RATE_HZ} Hz per device")
    print(f"  Total: {total_messages_per_second} messages/second")
    print(f"\nPress Ctrl+C to stop\n")
    
    message_count = 0
    
    try:
        while True:
            # Round-robin through devices and metrics
            device_id = f"sensor-{(message_count % NUM_DEVICES):03d}"
            metric_type = METRICS[message_count % len(METRICS)]
            
            # Generate and publish reading
            reading = generate_reading(device_id, metric_type)
            topic = f"sensors/{device_id}/data"
            payload = json.dumps(reading)
            
            client.publish(topic, payload, qos=1)
            
            message_count += 1
            
            # Log progress every 100 messages
            if message_count % 100 == 0:
                print(f"Published {message_count} messages...")
            
            time.sleep(sleep_time)
    
    except KeyboardInterrupt:
        print(f"\n\nStopping publisher. Published {message_count} total messages.")
        client.loop_stop()
        client.disconnect()


if __name__ == '__main__':
    main()
