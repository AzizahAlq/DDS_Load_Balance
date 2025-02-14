import zenoh
import time
import threading
import csv
from flask import Flask, jsonify
import matplotlib.pyplot as plt
from collections import deque
import traceback
import json

# Flask app
app = Flask(__name__)

# Global variables
node_metrics = []
latencies = deque(maxlen=60)
throughput_data = deque(maxlen=60)
metrics_lock = threading.Lock()
pause_event = threading.Event()
plot_running = True
best_node = None
optimal_value = float('inf')
best_mac_address = None
messages_received = 0
start_time = time.time()

# Dictionary to store the last processed timestamp for each node
last_processed_time = {}

# CSV File path for saving optimal node data
csv_file_path = '/Users/azizahalq/Desktop/project2/optimal_node_data.csv'

# Selection criteria and weights
criteria_map = {"1": "CPU", "2": "Memory", "3": "Battery", "4": "Load", "5": "ALL"}
selection_criteria = "CPU"
weights = {"CPU": 0.25, "Memory": 0.25, "Battery": 0.25, "Load": 0.25}

#  Initialize Zenoh with Config
config = zenoh.Config()
zenoh_session = zenoh.open(config)

# Zenoh Topics
metrics_topic = "zenoh/node_metrics"
task_topic = "zenoh/task_assignment"

# Zenoh Publisher
task_publisher = zenoh_session.declare_publisher(task_topic)

#  Zenoh Subscriber Callback with `ZBytes` Handling
def metrics_callback(sample):
    global start_time, messages_received
    try:
        with metrics_lock:
            # Convert ZBytes to bytes before decoding
            data = json.loads(sample.payload.to_bytes().decode())

            current_time = time.time()
            last_time = last_processed_time.get(data["node_id"], 0)

            # Process data if enough time has passed
            if current_time - last_time >= 60:
                latencies.append(abs(current_time - data["timestamp"]) * 1000)
                update_node_metrics(data, node_metrics)
                last_processed_time[data["node_id"]] = current_time
                update_best_node(data, node_metrics)

                messages_received += 1
                elapsed_time = abs(current_time - start_time)
                if elapsed_time >= 1.0:
                    throughput = messages_received / elapsed_time
                    throughput_data.append(throughput)
                    messages_received = 0
                    start_time = current_time
                    print(f"Throughput: {throughput:.2f} messages/sec")
            else:
                print(f"Message from node {data['node_id']} ignored due to interval check.")

    except Exception as e:
        print(f"Error processing Zenoh message: {e}")
        traceback.print_exc()

# Subscribe to Node Metrics over Zenoh
zenoh_session.declare_subscriber(metrics_topic, metrics_callback)

# Update or append node data
def update_node_metrics(msg, node_metrics):
    for i, node in enumerate(node_metrics):
        if node["node_id"] == msg["node_id"]:
            node_metrics[i] = msg  # Replace existing entry
            return
    node_metrics.append(msg)  # Add new node if it doesn't exist

# Update best node selection based on score
def update_best_node(data, node_metrics):
    global best_node, optimal_value, best_mac_address
    node_scores = {node["node_id"]: calculate_score(node) for node in node_metrics}
    
    best_node_id = min(node_scores, key=node_scores.get)
    best_node_score = node_scores[best_node_id]
    best_node = best_node_id
    optimal_value = best_node_score
    best_mac_address = best_node_id

    # Get latest data for best node
    best_node_data = max(
        (node for node in node_metrics if node["node_id"] == best_node_id),
        key=lambda n: n["timestamp"],
        default=None
    )

    # Save optimal node data
    if best_node_data:
        save_optimal_node_data(best_node_data)

    assign_task(best_node)
    print(f"New best node: {best_node} with score {optimal_value}")

# Assign task to the best node
def assign_task(node_id):
    task_data = json.dumps({"task": "Perform task", "node_id": node_id})
    task_publisher.put(task_data)
    print(f"Assigned task to node {node_id}")

# Save optimal node data
def save_optimal_node_data(data):
    with open(csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([data["node_id"], optimal_value, data["cpu_load"], data["memory_usage"], data["battery_level"], data["load_avg"]])

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
        "CPU": data["cpu_load"],
        "Memory": data["memory_usage"],
        "Battery": data["battery_level"],
        "Load": data["load_avg"]
    }[metric]

    if metric in ["CPU", "Load", "Memory"]:
        normalized_value = 1 / (1 + metric_value)  # Lower is better
    elif metric == "Battery":
        normalized_value = metric_value / 100  # Assumes max 100
    else:
        normalized_value = metric_value

    return normalized_value * weights.get(metric, 0.7)

# Plot metrics
def plot_metrics():
    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    while plot_running:
        with metrics_lock:
            ax1.clear()
            if latencies:
                ax1.plot(range(len(latencies)), list(latencies), label="Latency (ms)", color='blue')
                ax1.set_title("Latency Over Time")
                ax1.set_ylabel("Latency (ms)")
                ax1.legend(loc="upper right")
                ax1.grid(True)

            ax2.clear()
            if throughput_data:
                ax2.plot(range(len(throughput_data)), list(throughput_data), label="Throughput (msgs/sec)", color='green')
                ax2.set_title("Throughput Over Time")
                ax2.set_ylabel("Throughput (msgs/sec)")
                ax2.legend(loc="upper right")
                ax2.grid(True)

        plt.pause(1)

    plt.ioff()
    plt.show()

# Flask Routes
@app.route('/get_best_node', methods=['GET'])
def get_best_node():
    return jsonify({"best_node": best_node, "optimal_value": optimal_value})

@app.route('/pause', methods=['POST'])
def pause_listener():
    pause_event.set()
    return jsonify({"message": "Listener paused"})

@app.route('/resume', methods=['POST'])
def resume_listener():
    pause_event.clear()
    return jsonify({"message": "Listener resumed"})

if __name__ == "__main__":
    try:
        threading.Thread(target=lambda: app.run(debug=True, use_reloader=False), daemon=True).start()
        plot_metrics()
    except KeyboardInterrupt:
        plot_running = False
