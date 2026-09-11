from paho.mqtt import client as mqtt_client
import threading
import logging
import time
import json
from kafka import KafkaProducer
import matplotlib.pyplot as plt
import psutil
from simulate_node import SimulatedNode  

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('data_receiver.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger()
logger.addHandler(file_handler)


class DataReceiver:
    """
    A class to receive data from MQTT broker and publish it to Kafka.

    Attributes:
        broker_address (str): The address of the MQTT broker.
        topic (str): The topic to subscribe to for receiving data from MQTT and sending data to Kafka.
        batch_size (int): The number of messages to collect before publishing to Kafka.
        client (mqtt.Client): The MQTT client instance.
        producer (KafkaProducer): The Kafka producer instance.
        message_buffer (list): A buffer to store received messages before publishing.
        inactive_node_threshold (int): The threshold (in seconds) to consider a node inactive.
        active_nodes (dict): Dictionary to track active nodes and their last active timestamps.
    """

    def __init__(self, broker_address, topic, batch_size=1, inactive_node_threshold=0):
        """
        Initialize the DataReceiver.

        Args:
            broker_address (str): The address of the MQTT broker.
            topic (str): The topic to subscribe to for receiving data.
            batch_size (int, optional): The number of messages to collect before publishing to Kafka. Default is 1.
        """
        self.broker_address = broker_address
        self.topic = topic
        self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.batch_size = batch_size
        self.message_buffer = []
        self.inactive_node_threshold = inactive_node_threshold
        self.active_nodes = {}
        self.virtual_nodes = {}
        self.time_taken = 0.2  # Initial value
        self.response_times = [0.2]  # Initial value
        self.current_delays = [15] 
        self.plot_lock = threading.Lock()
        self.cpu_utilization = [0]

        # Initialize Kafka producer
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def connect(self):
        """Connect to the MQTT broker."""
        self.client.connect(self.broker_address)
    
    def start_virtual_node(self, node_id):
        """
        Start a virtual node for the specified node ID.

        Args:
            node_id (str): The ID of the node.
        """
        simulated_node = SimulatedNode(node_id, "VirtualNode", 0, 0, self.broker_address, 1883, self.topic)
        simulated_node.start_simulation()
        self.virtual_nodes[node_id] = simulated_node
    
    def kill_virtual_node(self, node_id):
        """
        Kill the virtual node with the specified node ID.

        Args:
            node_id (str): The ID of the virtual node to kill.
        """
        self.virtual_nodes[node_id].stop_simulation()
        del self.virtual_nodes[node_id]
    
    def check_inactive_nodes(self):
        """
        Check for inactive nodes and start virtual nodes for them.
        """
        current_time = time.time()
        for node_id, last_active_time in self.active_nodes.items():
            if current_time - last_active_time > self.inactive_node_threshold:
                self.start_virtual_node(node_id)
                logging.info(f"Node {node_id} is inactive. Starting virtual node.")

    def start_listening(self):
        """Start listening for incoming messages."""
        self.check_inactive_nodes()
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        """Callback for MQTT client when connected to broker."""
        logging.info("Connected with result code " + str(rc))
        client.subscribe(self.topic)
    
    def update_current_delays(self, current_delay):
        with self.plot_lock:
            self.current_delays.append(current_delay)
            self.response_times.append(self.time_taken)

    def get_cpu_utilization(self):
        cpu_percent = psutil.cpu_percent(interval=1, percpu=False)  # Get overall CPU utilization
        self.cpu_utilization.append(cpu_percent)

    def on_message(self, client, userdata, msg):
        """
        Callback for MQTT client when message is received.

        Args:
            client (mqtt.Client): The MQTT client instance.
            userdata: The user data.
            msg (mqtt.MQTTMessage): The received MQTT message.
        """
        start_time = time.time()
        message_content = msg.payload.decode("utf-8")
        message_lines = message_content.strip().split("\n")
        processed_lines = self.preprocess_data(message_lines)
        for node in processed_lines:
            if node['id'] in self.virtual_nodes:
                if node['name'] != 'VirtualNode':
                    self.kill_virtual_node(node['id'])
                    
        self.message_buffer.extend(processed_lines)
        if len(self.message_buffer) >= self.batch_size:
            self.send_data()
        self.check_inactive_nodes()
        self.update_active_nodes(list(set([node['id'] for node in self.message_buffer])))
        end_time = time.time()  # Record the end time
        self.time_taken = end_time - start_time
        
        logging.info("Time taken from receiving MQTT message to sending to Kafka: %.2f seconds", self.time_taken)
    
    def update_active_nodes(self, ids):
        """
        Update the active nodes dictionary with the last active time for the node.

        Args:
            topic (str): The topic of the message received.
        """
        for node_id in ids:
            self.active_nodes[node_id] = time.time()

    def send_data(self):
        """Send collected data to Kafka."""
        logging.info("Sending data")
        preprocessed_data = self.message_buffer
        for data in preprocessed_data:
            content = json.dumps(data)
            self.producer.send(self.topic, value=content.encode('utf-8'))
            logging.info("Data sent to Kafka: %s", data)
        self.producer.flush()
        self.message_buffer.clear()

    def preprocess_data(self, message_lines):
        """
        Preprocess received message data.

        Args:
            message_lines (list): The list of message lines.

        Returns:
            list: The preprocessed data.
        """
        processed_data = []
        for line in message_lines:
            if line.startswith("{"):
                processed_data.append(json.loads(line))
        return processed_data
        
    
    def plot_data(self):
        plt.ion()  # Turn on interactive mode
        fig, axes = plt.subplots(3, 1, figsize=(10, 15))

        # Adding legends outside the loop
        axes[1].legend(['Response Time (ms)'])
        axes[2].legend(['Current Delay (s)'])

        while True:
            with self.plot_lock:
                # Clearing existing plots
                for ax in axes:
                    ax.clear()

                axes[0].plot(self.cpu_utilization, 'm-')
                axes[0].set_xlabel('Time (s)')
                axes[0].set_ylabel('CPU Utilization (%)')
                axes[0].set_title('CPU Utilization vs Time')
                axes[0].grid(True)


                axes[1].plot([t * 1000 for t in self.response_times], 'r-')
                axes[1].set_xlabel('Time (s)')
                axes[1].set_ylabel('Response Time (ms)')
                axes[1].set_title('Response Time vs Time')
                axes[1].grid(True)

                axes[2].plot(range(len(self.current_delays)), self.current_delays, 'g-')
                axes[2].set_xlabel('Time (s)')
                axes[2].set_ylabel('Frequency of Request (Hz)')
                axes[2].set_title('Frequency of Requests to broker vs Time')
                axes[2].grid(True)

                plt.tight_layout(pad=3.0)
                fig.canvas.draw()
            plt.pause(5)  # Update plot every 5 seconds
import time
import logging

def publish_get_info(receiver, target_response_time, response_time_tolerance):
    """
    Publish a request to get data at regular intervals.

    Args:
        receiver (DataReceiver): The DataReceiver instance.
        target_response_time (float): The target response time in seconds.
        response_time_tolerance (float): The tolerance level for response time.
    """
    min_delay = 5  # Minimum delay
    max_delay = 60  # Maximum delay
    current_delay = 15  # Initial delay

    while True:
        receiver.client.publish(receiver.topic, "get_data")
        logging.info("Request for getting data sent")

        if receiver.time_taken > target_response_time * (1 + response_time_tolerance):
            current_delay = min(current_delay + 1, max_delay)
        elif receiver.time_taken < target_response_time * (1 - response_time_tolerance):
            current_delay = max(current_delay - 1, min_delay)

        logging.info(f"Response time: {receiver.time_taken:.3f}s, Current delay: {current_delay}s")
        receiver.update_current_delays(current_delay)
        time.sleep(current_delay)

import sys
if __name__ == '__main__':
    broker_address = "broker.hivemq.com"
    topic = sys.argv[1] if len(sys.argv) > 1 else 'temperature'
    receiver = DataReceiver(broker_address, topic, batch_size=2)
    receiver.connect()
    target_response_time = 0.2  # 200ms
    response_time_tolerance = 0.02  # 10% of target response time
    publish_thread = threading.Thread(target=publish_get_info, args=(receiver, target_response_time, response_time_tolerance))
    publish_thread.start()
    listen_thread = threading.Thread(target=receiver.start_listening)
    listen_thread.start()

    # Plot data
    receiver.plot_data()
