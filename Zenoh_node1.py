import time
import psutil
import threading
import json
import os
import zenoh

class NodeSimulator:
    def __init__(self, node_id):
        self.node_id = node_id
        print(f"Initializing Zenoh Participant for node {self.node_id}")

        # Zenoh session with configuration
        config = zenoh.Config()
        self.zenoh_session = zenoh.open(config)

        # Zenoh Topics
        self.metrics_topic = "zenoh/node_metrics"
        self.task_topic = "zenoh/task_assignments"

        # Zenoh Publisher for sending metrics
        self.metrics_publisher = self.zenoh_session.declare_publisher(self.metrics_topic)

        # Start background threads for publishing and task listening
        threading.Thread(target=self.send_metrics, daemon=True).start()
        threading.Thread(target=self.listen_for_task_assignments, daemon=True).start()

    def send_metrics(self):
        """Publishes system metrics every 60 seconds."""
        while True:
            start_time = time.time()
            metrics = self.get_system_metrics()
            
            # Convert metrics to JSON and publish
            self.metrics_publisher.put(json.dumps(metrics))
            print(f"Node {self.node_id} sent metrics: {metrics}")

            # Maintain a strict 60-second interval
            elapsed = time.time() - start_time
            if elapsed < 60:
                time.sleep(60 - elapsed)

    def listen_for_task_assignments(self):
        """Listens for task assignments from the aggregator."""
        print(f"Node {self.node_id} listening for task assignments...")

        def callback(sample):
            task_data = json.loads(sample.payload.decode())
            if task_data["node_id"] == self.node_id:
                print(f"Node {self.node_id} received task: {task_data['task']}")
                self.execute_task(task_data["task"])

        # Subscribe to task assignments
        self.zenoh_session.declare_subscriber(self.task_topic, callback)

    def execute_task(self, task_type):
        """Executes the assigned task based on system metrics."""
        if task_type == "load_task":
            self.simulate_load_task()
        else:
            print(f"Node {self.node_id} received unknown task type: {task_type}")

    def simulate_load_task(self):
        """Simulates a CPU-intensive task."""
        print(f"Node {self.node_id} starting load task...")
        for _ in range(1000):
            _ = sum(i * i for i in range(1000))  # Heavy computation
        print(f"Node {self.node_id} completed load task.")

    def get_system_metrics(self):
        """Retrieve system metrics using psutil."""
        cpu_load = psutil.cpu_percent(interval=1)  # CPU usage percentage
        memory = psutil.virtual_memory()  # Memory statistics
        memory_usage = memory.percent  # Memory usage percentage
        battery = psutil.sensors_battery()  # Battery statistics
        battery_level = battery.percent if battery else 100.0  # Assume 100% if no battery
        load_avg = os.getloadavg()[0]  # 1-minute load average

        return {
            "cpu_load": cpu_load,
            "memory_usage": memory_usage,
            "battery_level": battery_level,
            "load_avg": load_avg,
            "node_id": self.node_id,
            "timestamp": time.time()
        }

if __name__ == "__main__":
    print("Starting Node2 Simulator with System Metrics...")

    node_id = "node_2"
    node = NodeSimulator(node_id)

    # Keep the main thread alive for Zenoh to function properly
    while True:
        time.sleep(1)
