import paho.mqtt.client as mqtt
import ssl
import time
import threading
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os

# Get configuration from environment variables with fallbacks to local config
try:
    from config import *  # Local config for development
except ImportError:
    # Environment variables for production
    MQTT_BROKER = os.environ.get("MQTT_BROKER")
    MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
    MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
    MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
    INFLUX_URL = os.environ.get("INFLUX_URL")
    INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
    INFLUX_ORG = os.environ.get("INFLUX_ORG")
    INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")
    
    
# ===== MQTT Configuration =====
mqtt_broker = MQTT_BROKER
mqtt_port = MQTT_PORT  # 8883 for TLS, 1883 for non-TLS
mqtt_username = MQTT_USERNAME
mqtt_password = MQTT_PASSWORD

# Tank monitoring topics
topics = [
    "tank/topic1",  # Tank 1 level in cm
    "tank/topic2",  # Tank 1 volume in liters
    "tank/topic3",  # Tank 2 level in cm
    "tank/topic4",  # Tank 2 volume in liters
    "tank/topic5",  # Current timestamp
]

# ===== InfluxDB Configuration =====
influx_url = INFLUX_URL
influx_token = INFLUX_TOKEN
influx_org = INFLUX_ORG
influx_bucket = INFLUX_BUCKET

# Store the latest values
tank_data = {
    "tank/topic1": None,  # Tank 1 level in cm
    "tank/topic2": None,  # Tank 1 volume in liters
    "tank/topic3": None,  # Tank 2 level in cm
    "tank/topic4": None,  # Tank 2 volume in liters
    "tank/topic5": None   # Current timestamp
}

# Flag to track if all data has been received
values_received = {topic: False for topic in topics}
last_write_time = 0
WRITE_INTERVAL = 5  # seconds, minimum time between writes

# Heartbeat interval (in seconds)
HEARTBEAT_INTERVAL = 60
last_message_time = time.time()
connection_healthy = True

# Heartbeat topic
HEARTBEAT_TOPIC = "tank/heartbeat"

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def write_to_influxdb():
    """Write all tank data to InfluxDB as a single row"""
    global last_write_time
    
    current_time = time.time()
    # Check if we have new data and if minimum interval has passed
    if all(values_received.values()) and (current_time - last_write_time) >= WRITE_INTERVAL:
        try:
            # Create a single point with all tank data
            point = Point("tank_monitoring")
            
            # Add all fields to the same point
            if tank_data["tank/topic1"] is not None:
                point.field("tank1_level_cm", float(tank_data["tank/topic1"]))
            
            if tank_data["tank/topic2"] is not None:
                point.field("tank1_volume_liters", float(tank_data["tank/topic2"]))
            
            if tank_data["tank/topic3"] is not None:
                point.field("tank2_level_cm", float(tank_data["tank/topic3"]))
            
            if tank_data["tank/topic4"] is not None:
                point.field("tank2_volume_liters", float(tank_data["tank/topic4"]))
            
            if tank_data["tank/topic5"] is not None:
                # Store the timestamp as a string field
                point.field("system_timestamp", tank_data["tank/topic5"])
            
            # Write the single point to InfluxDB
            write_api.write(bucket=influx_bucket, record=point)
            
            print(f"Data written to InfluxDB at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("Single row with all tank data:")
            print(f"  Tank 1: {tank_data['tank/topic1']} cm, {tank_data['tank/topic2']} liters")
            print(f"  Tank 2: {tank_data['tank/topic3']} cm, {tank_data['tank/topic4']} liters")
            print(f"  Timestamp: {tank_data['tank/topic5']}")
            print("-" * 50)
            
            # Reset received flags after successful write
            for topic in values_received:
                values_received[topic] = False
            
            last_write_time = current_time
            
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")

def heartbeat_check():
    """Function to periodically check connection health and send heartbeats"""
    global last_message_time, connection_healthy
    
    while True:
        current_time = time.time()
        time_since_last_message = current_time - last_message_time
        
        # Check if we've received any message recently
        if time_since_last_message > HEARTBEAT_INTERVAL * 2:
            if connection_healthy:
                print(f"WARNING: No messages received for {time_since_last_message:.1f} seconds")
                connection_healthy = False
        else:
            if not connection_healthy:
                print("Connection appears to be restored, messages are flowing again")
                connection_healthy = True
        
        # Send a heartbeat to the broker
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            client.publish(HEARTBEAT_TOPIC, f"HEARTBEAT: {timestamp}")
            print(f"Heartbeat sent at {timestamp}")
            
            # Log connection status to InfluxDB
            heartbeat_point = Point("mqtt_heartbeat") \
                .field("connection_healthy", connection_healthy) \
                .field("seconds_since_last_message", time_since_last_message)
            write_api.write(bucket=influx_bucket, record=heartbeat_point)
            
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
            # Try to reconnect
            try:
                client.reconnect()
                print("Reconnected to MQTT broker")
            except Exception as reconnect_error:
                print(f"Failed to reconnect: {reconnect_error}")
        
        # Sleep until next heartbeat
        time.sleep(HEARTBEAT_INTERVAL)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # Subscribe to all topics
        for topic in topics:
            client.subscribe(topic)
            print(f"Subscribed to {topic}")
    else:
        print(f"Failed to connect to MQTT broker. Return code: {rc}")

def on_disconnect(client, userdata, rc):
    global connection_healthy
    connection_healthy = False
    print(f"Disconnected from MQTT broker with code {rc} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if rc != 0:
        print("Unexpected disconnection, attempting to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            print(f"Failed to reconnect: {e}")

def on_message(client, userdata, msg):
    global last_message_time
    
    # Update last message time whenever any message is received
    last_message_time = time.time()
    
    topic = msg.topic
    payload = msg.payload.decode('utf-8')
    
    # Update our stored data
    tank_data[topic] = payload
    values_received[topic] = True
    
    # Print received message
    topic_descriptions = {
        "tank/topic1": "Tank 1 Level",
        "tank/topic2": "Tank 1 Volume",
        "tank/topic3": "Tank 2 Level",
        "tank/topic4": "Tank 2 Volume",
        "tank/topic5": "Timestamp"
    }
    
    description = topic_descriptions.get(topic, topic)
    print(f"Received: {description} = {payload}")
    
    # Write to InfluxDB if we have all the data
    write_to_influxdb()

# Create an MQTT client instance
client = mqtt.Client(client_id="tank-influxdb-client")

# Set the callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Set up authentication if required
client.username_pw_set(mqtt_username, mqtt_password)

# Enable TLS/SSL if using secure connection
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

# Main execution
try:
    # Connect to MQTT broker
    print(f"Connecting to MQTT broker at {mqtt_broker}:{mqtt_port}...")
    client.connect(mqtt_broker, mqtt_port, 60)
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=heartbeat_check, daemon=True)
    heartbeat_thread.start()
    print(f"Heartbeat monitor started with {HEARTBEAT_INTERVAL} second interval")
    
    # Start the loop
    print("Starting MQTT client loop. Press Ctrl+C to exit.")
    client.loop_forever()
    
except KeyboardInterrupt:
    print("\nProgram terminated by user")
    client.disconnect()
    influx_client.close()
    print("Disconnected from MQTT and InfluxDB")
except Exception as e:
    print(f"Error: {e}")
    client.disconnect()
    influx_client.close()
