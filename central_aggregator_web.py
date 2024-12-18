import csv
import time
import threading
from flask import Flask, render_template, request, jsonify
from cyclonedds.domain import DomainParticipant
from cyclonedds.pub import Publisher, DataWriter
from cyclonedds.sub import Subscriber, DataReader
from cyclonedds.core import Qos, Policy
from cyclonedds.topic import Topic
from cyclonedds.idl import IdlStruct
from cyclonedds.idl.types import float32
from dataclasses import dataclass
from collections import deque
import matplotlib.pyplot as plt
import traceback

# Flask app
app = Flask(__name__)

# Global variables
node_metrics = []
latencies = deque(maxlen=60)
throughput_data = deque(maxlen=60)
metrics_lock = threading.Lock()
stop_event = threading.Event()
pause_event = threading.Event()  # Added pause event
best_node = None
optimal_value = float('inf')
best_mac_address = None
messages_received = 0
start_time = time.time()

# Dictionary to store the last processed timestamp for each node
last_processed_time = {}

# CSV File path for saving optimal node data
csv_file_path = '/Users/azizahalq/Desktop/project2/optimal_node_data.csv'

# Define DDS data structures
@dataclass
class NodeMetrics(IdlStruct):
    cpu_load: float32
    memory_usage: float32
    battery_level: float32
    load_avg: float32
    node_id: str
    timestamp: float

@dataclass
class TaskAssignment(IdlStruct):
    task: str
    node_id: str

# DDS setup
participant = DomainParticipant()
metrics_topic = Topic(participant, 'node_metrics', NodeMetrics)
task_topic = Topic(participant, 'task_assignment', TaskAssignment)

subscriber = Subscriber(participant)
reader = DataReader(subscriber, metrics_topic, qos=Qos(Policy.Reliability.BestEffort, Policy.Durability.Volatile))
publisher = Publisher(participant)
writer = DataWriter(publisher, task_topic, qos=Qos(Policy.Reliability.BestEffort, Policy.Durability.Volatile))

# Selection criteria and weights
criteria_map = {"1": "CPU", "2": "Memory", "3": "Battery", "4": "Load", "5": "ALL"}
selection_criteria = "CPU"
weights = {"CPU": 0.25, "Memory": 0.25, "Battery": 0.25, "Load": 0.25}

# DDS Listener
# DDS Listener
def dds_listener():
    print("Starting DDS listener with interval check...")
    global start_time, messages_received
    interval_seconds = 60  # Define the interval (e.g., 1 minute)

    while not stop_event.is_set():
        if pause_event.is_set():  # Pause the listener if pause_event is set
            time.sleep(0.1)  # Brief sleep to avoid busy-waiting
            continue

        try:
            samples = reader.take()
            current_time = time.time()
            for msg in samples:
                with metrics_lock:
                    last_time = last_processed_time.get(msg.node_id, 0)
                    if current_time - last_time >= interval_seconds:
                        latencies.append(abs(current_time - msg.timestamp) * 1000)  # Latency in ms
                        update_node_metrics(msg, node_metrics)
                        last_processed_time[msg.node_id] = current_time
                        update_best_node(msg, node_metrics)
                        
                        messages_received += 1
                        elapsed_time = abs(current_time - start_time)
                        if elapsed_time >= 1.0:
                            throughput = messages_received / elapsed_time
                            throughput_data.append(throughput)
                            messages_received = 0
                            start_time = current_time
                            print(f"Throughput: {throughput:.2f} messages/sec")
                    else:
                        print(f"Message from node {msg.node_id} ignored due to interval check.")
        except Exception as e:
            print(f"Error in DDS listener: {e}")
            traceback.print_exc()
            
# Update or append node data
def update_node_metrics(msg, node_metrics):
    for i, node in enumerate(node_metrics):
        if node.node_id == msg.node_id:
            node_metrics[i] = msg  # Replace the existing entry
            return
    node_metrics.append(msg)  # Add new node if it doesn't exist

# Update best node selection based on score
def update_best_node(data, node_metrics):
    global best_node, optimal_value, best_mac_address
    # Calculate the score for each node
    node_scores = {}
    for node in node_metrics:
        score = calculate_score(node)
        node_scores[node.node_id] = score
        print(f"Node {node.node_id} has score {score}")
    print(f"Node Scores: {node_scores}")

    # Find the node with the lowest score
    best_node_id = min(node_scores, key=node_scores.get)
    best_node_score = node_scores[best_node_id]
    best_node = best_node_id
    optimal_value = best_node_score
    best_mac_address = best_node_id

    # Get the latest data for the best node
    best_node_data = max(
        (node for node in node_metrics if node.node_id == best_node_id),
        key=lambda n: n.timestamp,
        default=None
    )

    # Save data for the best node
    if best_node_data:
        save_optimal_node_data(best_node_data)
    assign_task(best_node)
    print(f"New best node: {best_node} with score {optimal_value}")

# Pause and Resume Functions
def pause_listener():
    print("Pausing DDS listener...")
    pause_event.set()

def resume_listener():
    print("Resuming DDS listener...")
    pause_event.clear()

# Assign task to the best node
def assign_task(node_id):
    task_name = "Perform task"
    task = TaskAssignment(task=task_name, node_id=node_id)
    writer.write(task)
    print(f"Assigned task '{task_name}' to node {node_id}")


   # threading.Timer(40, resume_listener).start()  # Resume listener after 5 seconds

# Save optimal node data
def save_optimal_node_data(data):
    with open(csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([data.node_id, optimal_value, data.cpu_load, data.memory_usage, data.battery_level, data.load_avg])

# Calculate score
def calculate_score(data):
    if selection_criteria == "ALL":
        scores = [
            calculate_score_for_metric("CPU", data),
            calculate_score_for_metric("Memory", data),
            calculate_score_for_metric("Battery", data),
            calculate_score_for_metric("Load", data)
        ]
        return sum(scores) / len(scores)
    return calculate_score_for_metric(selection_criteria, data)

def calculate_score_for_metric(metric, data):
    metric_value = {
        "CPU": data.cpu_load,
        "Memory": data.memory_usage,
        "Battery": data.battery_level,
        "Load": data.load_avg
    }[metric]

    if metric in ["CPU", "Load", "Memory"]:
        normalized_value = 1 / (1 + metric_value)  # Lower is better, so inverse
    elif metric == "Battery":
        normalized_value = metric_value / 100  # Assumes the max value is 100
    else:
        normalized_value = metric_value

    score = normalized_value * weights.get(metric, 0.7)
    return score

# Plot metrics locally
def plot_metrics():
    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
    while not stop_event.is_set():
        with metrics_lock:
            # Plot Latency
            ax1.clear()
            if len(latencies) > 0:
                ax1.plot(range(len(latencies)), list(latencies), label="Latency (ms)", color='blue')
                ax1.set_title("Latency Over Time")
                ax1.set_ylabel("Latency (ms)")
                ax1.legend()
                ax1.grid(True)

            # Plot Throughput
            ax2.clear()
            if len(throughput_data) > 0:
                ax2.plot(range(len(throughput_data)), list(throughput_data), label="Throughput (msgs/sec)", color='green')
                ax2.set_title("Throughput Over Time")
                ax2.set_ylabel("Throughput (msgs/sec)")
                ax2.legend()
                ax2.grid(True)

        plt.pause(1)
    plt.ioff()


# Flask routes and plotting functions remain unchanged

if __name__ == "__main__":
    threading.Thread(target=dds_listener, daemon=True).start()
    threading.Thread(target=lambda: app.run(debug=True, use_reloader=False), daemon=True).start()
    plot_metrics()
