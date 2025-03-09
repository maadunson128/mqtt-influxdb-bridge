import paho.mqtt.client as mqtt
import ssl
import time
import threading
import os
import socket
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from config import *

# ===== Environment Variables =====
# Use environment variables with fallbacks for local development
mqtt_broker = os.environ.get("MQTT_BROKER", MQTT_BROKER)
mqtt_port = int(os.environ.get("MQTT_PORT", MQTT_PORT))
mqtt_username = os.environ.get("MQTT_USERNAME", MQTT_USERNAME)
mqtt_password = os.environ.get("MQTT_PASSWORD", MQTT_PASSWORD)
influx_url = os.environ.get("INFLUX_URL", INFLUX_URL)
influx_token = os.environ.get("INFLUX_TOKEN", INFLUX_TOKEN)
influx_org = os.environ.get("INFLUX_ORG", INFLUX_ORG)
influx_bucket = os.environ.get("INFLUX_BUCKET", INFLUX_BUCKET)

# ===== HTTP Server for Render =====
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        status = "Connected" if mqtt_client_connected else "Disconnected"
        self.wfile.write(f"MQTT Bridge is running (Status: {status})".encode())
        
    def log_message(self, format, *args):
        # Suppress logs from HTTP requests to avoid cluttering the console
        return

def start_health_server():
    port = int(os.environ.get('PORT', 8080))  # Render assigns a PORT env variable
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"Starting health check server on port {port}")
    server.serve_forever()

# ===== MQTT Configuration =====
# Tank monitoring topics
topics = [
    "tank/topic1",  # Tank 1 level in cm
    "tank/topic2",  # Tank 1 volume in liters
    "tank/topic3",  # Tank 2 level in cm
    "tank/topic4",  # Tank 2 volume in liters
    "tank/topic5",  # Current timestamp
]

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

# Connection tracking
mqtt_client_connected = False
last_successful_connection = time.time()
last_message_time = time.time()
HEARTBEAT_INTERVAL = 60
RECONNECT_BASE_DELAY = 5  # Start with 5 seconds delay
RECONNECT_MAX_DELAY = 300  # Maximum 5 minutes between reconnects
current_reconnect_delay = RECONNECT_BASE_DELAY
connection_healthy = False

# Heartbeat topic
HEARTBEAT_TOPIC = "tank/heartbeat"

# ===== InfluxDB Configuration =====
# Initialize InfluxDB client
influx_client = None
write_api = None

def initialize_influxdb():
    global influx_client, write_api
    try:
        print(f"Connecting to InfluxDB at {influx_url}...")
        influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        print("InfluxDB connection established")
        return True
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        return False

def write_to_influxdb():
    """Write all tank data to InfluxDB as a single row"""
    global last_write_time
    
    if write_api is None:
        if not initialize_influxdb():
            return
    
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
            # Try to reinitialize the connection
            initialize_influxdb()

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
        
        # Send a heartbeat if connected, otherwise try to reconnect
        if mqtt_client_connected:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                client.publish(HEARTBEAT_TOPIC, f"HEARTBEAT: {timestamp}")
                print(f"Heartbeat sent at {timestamp}")
                
                # Log connection status to InfluxDB
                try:
                    if write_api is not None:
                        heartbeat_point = Point("mqtt_heartbeat") \
                            .field("connection_healthy", connection_healthy) \
                            .field("seconds_since_last_message", time_since_last_message)
                        write_api.write(bucket=influx_bucket, record=heartbeat_point)
                except Exception as e:
                    print(f"Error writing heartbeat to InfluxDB: {e}")
                
            except Exception as e:
                print(f"Error sending heartbeat: {e}")
                # Try to reconnect
                reconnect()
        else:
            print("Not connected to MQTT broker during heartbeat check, attempting to reconnect...")
            reconnect()
        
        # Sleep until next heartbeat
        time.sleep(HEARTBEAT_INTERVAL)

def watchdog_timer():
    """Function to detect and recover from extended periods without connection"""
    global last_successful_connection
    
    while True:
        time.sleep(120)  # Check every 2 minutes
        now = time.time()
        if now - last_successful_connection > 300:  # 5 minutes without connection
            print("WATCHDOG ALERT: No successful connection for over 5 minutes, forcing reconnect")
            # Force a full reconnection
            try:
                client.disconnect()
            except:
                pass  # Ignore errors during forced disconnect
            
            time.sleep(2)  # Brief pause
            
            try:
                print("Watchdog initiating fresh connection...")
                client.connect(mqtt_broker, mqtt_port, keepalive=120)
            except Exception as e:
                print(f"Watchdog reconnect failed: {e}")

def dns_check():
    """Periodically verify DNS resolution of the MQTT broker"""
    while True:
        try:
            print(f"DNS check: Resolving {mqtt_broker}...")
            ip = socket.gethostbyname(mqtt_broker)
            print(f"DNS check successful, resolved to {ip}")
        except Exception as e:
            print(f"DNS check failed: {e}")
        
        time.sleep(3600)  # Check once per hour

def reconnect():
    """Attempt to reconnect with exponential backoff"""
    global current_reconnect_delay, mqtt_client_connected
    
    if mqtt_client_connected:
        print("Reconnect called while already connected. Skipping.")
        return
    
    print(f"Attempting to reconnect in {current_reconnect_delay} seconds...")
    time.sleep(current_reconnect_delay)
    
    try:
        client.reconnect()
        print("Reconnection successful")
        # Reset the reconnect delay on successful connection
        current_reconnect_delay = RECONNECT_BASE_DELAY
    except Exception as e:
        print(f"Reconnection failed: {e}")
        # Increase the delay for next attempt (exponential backoff)
        current_reconnect_delay = min(current_reconnect_delay * 2, RECONNECT_MAX_DELAY)
        # Schedule another reconnect attempt
        threading.Timer(current_reconnect_delay, reconnect).start()

def on_connect(client, userdata, flags, rc):
    global mqtt_client_connected, last_successful_connection, current_reconnect_delay
    
    if rc == 0:
        mqtt_client_connected = True
        last_successful_connection = time.time()
        current_reconnect_delay = RECONNECT_BASE_DELAY  # Reset backoff on successful connection
        
        print(f"Connected to MQTT broker successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        # Subscribe to all topics
        for topic in topics:
            client.subscribe(topic)
            print(f"Subscribed to {topic}")
    else:
        mqtt_client_connected = False
        print(f"Failed to connect to MQTT broker. Return code: {rc}")
        # Schedule a reconnection attempt
        threading.Timer(current_reconnect_delay, reconnect).start()

def on_disconnect(client, userdata, rc):
    global mqtt_client_connected, connection_healthy
    
    mqtt_client_connected = False
    connection_healthy = False
    print(f"Disconnected from MQTT broker with code {rc} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if rc != 0:
        print("Unexpected disconnection, scheduling reconnect...")
        # Schedule a reconnection attempt
        threading.Timer(current_reconnect_delay, reconnect).start()

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
client = mqtt.Client(client_id=f"tank-influxdb-client-{int(time.time())}")  # Add timestamp to make unique

# Set the callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Set up authentication if required
client.username_pw_set(mqtt_username, mqtt_password)

# Enable TLS/SSL if using secure connection
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

# Set a longer keepalive interval
client.keepalive = 120  # 2 minutes

# Main execution
try:
    # Initialize InfluxDB connection
    initialize_influxdb()
    
    # First, start the health check server for Render
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    print("Health check server started")
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=heartbeat_check, daemon=True)
    heartbeat_thread.start()
    print(f"Heartbeat monitor started with {HEARTBEAT_INTERVAL} second interval")
    
    # Start watchdog thread
    watchdog_thread = threading.Thread(target=watchdog_timer, daemon=True)
    watchdog_thread.start()
    print("Watchdog timer started")
    
    # Start DNS check thread
    dns_thread = threading.Thread(target=dns_check, daemon=True)
    dns_thread.start()
    print("DNS check thread started")
    
    # Resolve the hostname before connecting
    try:
        print(f"Resolving hostname for {mqtt_broker}...")
        ip_address = socket.gethostbyname(mqtt_broker)
        print(f"Resolved to IP: {ip_address}")
    except Exception as e:
        print(f"Warning: Could not resolve MQTT broker hostname: {e}")
    
    # Connect to MQTT broker
    print(f"Connecting to MQTT broker at {mqtt_broker}:{mqtt_port}...")
    client.connect(mqtt_broker, mqtt_port, keepalive=120)
    
    # Start the network loop in a non-blocking way
    client.loop_start()
    
    # Main program loop
    print("MQTT bridge is running. Press Ctrl+C to exit.")
    while True:
        time.sleep(10)
        # Check connection status periodically in the main thread
        if not mqtt_client_connected:
            print("Main loop detected client is not connected")
            if time.time() - last_successful_connection > 300:  # 5 minutes
                print("No connection for extended period, forcing new connection attempt...")
                try:
                    client.disconnect()
                except:
                    pass
                time.sleep(2)
                try:
                    client.connect(mqtt_broker, mqtt_port, keepalive=120)
                except Exception as e:
                    print(f"Reconnection from main loop failed: {e}")
    
except KeyboardInterrupt:
    print("\nProgram terminated by user")
    client.loop_stop()
    client.disconnect()
    if influx_client:
        influx_client.close()
    print("Disconnected from MQTT and InfluxDB")
except Exception as e:
    print(f"Error: {e}")
    client.loop_stop()
    client.disconnect()
    if influx_client:
        influx_client.close()