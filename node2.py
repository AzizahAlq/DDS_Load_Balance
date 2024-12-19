import time
import psutil
import threading
from cyclonedds.domain import DomainParticipant
from cyclonedds.pub import DataWriter, Publisher
from cyclonedds.sub import DataReader, Subscriber
from cyclonedds.core import Qos, Policy
from cyclonedds.topic import Topic
from cyclonedds.idl import IdlStruct
from cyclonedds.idl.types import float32
from dataclasses import dataclass
import matplotlib.pyplot as plt
from collections import deque
import os

# Define the nodeMetrics struct
@dataclass
class nodeMetrics(IdlStruct):
    cpu_load: float32
    memory_usage: float32
    battery_level: float32
    load_avg: float32
    node_id: str
    timestamp: float

# Define the TaskAssignment struct
@dataclass
class TaskAssignment(IdlStruct):
    task: str
    node_id: str

class NodeSimulator:
    def __init__(self, node_id) -> None:
        self.node_id = node_id
        print(f"Initializing DDS Participant for node {self.node_id}")

        self.participant = DomainParticipant(domain_id=0)


        # Metrics publisher
        self.metrics_topic = Topic(self.participant, "node_metrics", nodeMetrics)
        self.metrics_writer = DataWriter(Publisher(self.participant), self.metrics_topic,  qos=Qos(Policy.Reliability.Reliable(1),Policy.Durability.Volatile))

        # Task publisher
        self.task_topic = Topic(self.participant, "task_assignments", TaskAssignment)
        self.task_reader = DataReader(Subscriber(self.participant), self.task_topic,  qos=Qos(Policy.Reliability.Reliable(1),Policy.Durability.Volatile))

        # Data for plotting
        self.cpu_load_data = deque(maxlen=30)
        self.memory_usage_data = deque(maxlen=30)
        self.battery_level_data = deque(maxlen=30)
        self.load_avg_data = deque(maxlen=30)

        # Start threads for metrics publishing and task handling
        threading.Thread(target=self.send_metrics, daemon=True).start()
        threading.Thread(target=self.listen_for_task_assignments, daemon=True).start()

    def send_metrics(self):
       """Publishes periodic metrics from this node."""
       while True:
          start_time = time.time()
          metrics = self.get_system_metrics()
          self.metrics_writer.write(metrics)
          print(f"Node {self.node_id} sent metrics: {metrics}")
        
        # Enforce 60-second interval
          elapsed = time.time() - start_time
          if elapsed < 60:
           time.sleep(60 - elapsed)

    def listen_for_task_assignments(self):
        """Listens for task assignments from the aggregator."""
        print(f"Node {self.node_id} listening for task assignments...")
        while True:
            for sample in self.task_reader.take_iter():
                if sample and sample.node_id == self.node_id:
                    print(f"Node {self.node_id} received task: {sample.task}")
                    self.execute_task(sample.task)
            time.sleep(60)

    def execute_task(self, task_type):
        """Simulates task execution based on system metrics."""
        if task_type == "load_task":
            self.simulate_load_task()
        else:
            print(f"Node {self.node_id} received unknown task type: {task_type}")

    def simulate_load_task(self):
        """Simulates a heavy load task by performing a mathematical operation in a loop."""
        print(f"Node {self.node_id} starting load task...")
        for _ in range(1000):
            # Perform a dummy task that consumes CPU resources
            result = sum([i * i for i in range(1000)])
        print(f"Node {self.node_id} completed load task.")

    def get_system_metrics(self):
        """Retrieve system metrics using psutil."""
        cpu_load = psutil.cpu_percent(interval=1)  # Get CPU usage percentage
        memory = psutil.virtual_memory()  # Memory statistics
        memory_usage = memory.percent  # Memory usage percentage
        battery = psutil.sensors_battery()  # Battery statistics
        battery_level = battery.percent if battery else 100.0  # Assume 100% if no battery
        load_avg = os.getloadavg()[0]  # Get the 1-minute load average
        return nodeMetrics(
            cpu_load=cpu_load,
            memory_usage=memory_usage,
            battery_level=battery_level,
            load_avg=load_avg,
            node_id=self.node_id,
            timestamp=time.time()
        )

    def update_plot(self):
        """Update the plot with the latest metrics."""
        plt.clf()  # Clear the current figure
        
        # Plot the metrics
        plt.subplot(2, 2, 1)
        plt.plot(self.cpu_load_data)
        plt.title('CPU Load NODE2(%)')
        
        plt.subplot(2, 2, 2)
        plt.plot(self.memory_usage_data)
        plt.title('Memory Usage NODE2 (%)')
        
        plt.subplot(2, 2, 3)
        plt.plot(self.battery_level_data)
        plt.title('Battery Level NODE2 (%)')
        
        plt.subplot(2, 2, 4)
        plt.plot(self.load_avg_data)
        plt.title('Load Average NODE2 ')
        
        plt.tight_layout()
        plt.draw()
        plt.pause(1)  # Pause to allow for real-time updating

    def simulate_metrics(self):
        """Simulates the periodic publishing of metrics and updates the plot."""
        while True:
            metrics = self.get_system_metrics()
            print(f"Publishing metrics from {self.node_id}: {metrics}")
            self.metrics_writer.write(metrics)
            
            # Update data for plotting
            self.cpu_load_data.append(metrics.cpu_load)
            self.memory_usage_data.append(metrics.memory_usage)
            self.battery_level_data.append(metrics.battery_level)
            self.load_avg_data.append(metrics.load_avg)
            
            # Update the plot in the main thread
            self.update_plot()
            time.sleep(60)  # Publish and update plot every 60 seconds

if __name__ == "__main__":
    print("Starting Node2 Simulator with System Metrics...")

    # Set up the plot
    plt.ion()  # Interactive mode on
    plt.figure(figsize=(10, 8))

    node_id = "node_2"
    node = NodeSimulator(node_id)
    
    # Start the metrics simulation and task handling in the main thread
    node.simulate_metrics()
