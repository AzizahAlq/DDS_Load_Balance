# node_client.py
import os
import time
import argparse
import threading
import traceback

import psutil

from cyclonedds.domain import DomainParticipant
from cyclonedds.pub import Publisher, DataWriter
from cyclonedds.sub import Subscriber, DataReader
from cyclonedds.core import Qos, Policy
from cyclonedds.topic import Topic

from idl_types import NodeMetrics, TaskAssignment, TaskAck
from node_tasks import run_task


def qos_reliable():
    # Reliable() needs max_blocking_time (seconds) for CycloneDDS Python binding
    return Qos(
        Policy.Reliability.Reliable(1),
        Policy.Durability.Volatile,
        Policy.History.KeepLast(10),
    )


class DDSNode:
    def __init__(
        self,
        node_id: str,
        domain_id: int,
        publish_interval: float,
        print_every_n: int,
        # overload thresholds (local guard)
        cpu_max: float = 80.0,
        mem_avail_min: float = 0.25,
        load_per_core_max: float = 1.0,
    ):
        self.node_id = node_id
        self.domain_id = int(domain_id)
        self.publish_interval = float(publish_interval)
        self.print_every_n = int(print_every_n)

        # thresholds
        self.cpu_max = float(cpu_max)
        self.mem_avail_min = float(mem_avail_min)
        self.cpu_cores = max(1, int(psutil.cpu_count(logical=True) or 1))
        self.load_max = float(load_per_core_max) * float(self.cpu_cores)

        self.stop_flag = threading.Event()

        # Task gating
        self.task_lock = threading.Lock()
        self.task_busy = False        # True while executing a task
        self.overloaded = False       # True when local thresholds say "reject"
        # (public Busy view)
        # Busy := task_busy OR overloaded

        # DDS
        self.participant = DomainParticipant(domain_id=self.domain_id)
        self.sub = Subscriber(self.participant)
        self.pub = Publisher(self.participant)

        self.topic_metrics = Topic(self.participant, "node_metrics", NodeMetrics)
        self.topic_assign  = Topic(self.participant, "task_assignment", TaskAssignment)
        self.topic_ack     = Topic(self.participant, "task_ack", TaskAck)

        qos = qos_reliable()

        self.metrics_writer = DataWriter(self.pub, self.topic_metrics, qos=qos)
        self.assign_reader  = DataReader(self.sub, self.topic_assign, qos=qos)
        self.ack_writer     = DataWriter(self.pub, self.topic_ack, qos=qos)

        self._print_counter = 0

        # Warm up psutil cpu_percent so first read isn't 0.0
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass

    def compute_overloaded(self, m: NodeMetrics) -> bool:
        # Busy should become True if ANY stress indicator exceeds thresholds
        if float(m.cpu_load) >= self.cpu_max:
            return True
        if float(m.mem_available_ratio) < self.mem_avail_min:
            return True
        if float(m.load_avg_1m) >= self.load_max:
            return True
        return False

    def get_metrics(self) -> NodeMetrics:
        cpu = float(psutil.cpu_percent(interval=None))

        vm = psutil.virtual_memory()
        mem_available_ratio = float(vm.available / vm.total) if vm.total > 0 else 0.0

        batt = psutil.sensors_battery()
        battery_level = float(batt.percent) if batt and batt.percent is not None else 100.0

        try:
            load1, _, _ = os.getloadavg()
            load_avg_1m = float(load1)
        except Exception:
            load_avg_1m = 0.0

        t_pub = float(time.time())

        return NodeMetrics(
            cpu_load=cpu,
            mem_available_ratio=mem_available_ratio,
            battery_level=battery_level,
            load_avg_1m=load_avg_1m,
            node_id=self.node_id,
            timestamp=t_pub,
        )

    def publish_loop(self):
        while not self.stop_flag.is_set():
            m = self.get_metrics()

            # Update local overload state each publish
            self.overloaded = self.compute_overloaded(m)

            # Publish metrics
            self.metrics_writer.write(m)

            # Print
            self._print_counter += 1
            if self._print_counter % max(1, self.print_every_n) == 0:
                mem_used_percent = 100.0 * (1.0 - float(m.mem_available_ratio))
                busy_view = (self.task_busy or self.overloaded)

                print(
                    f"[NODE {self.node_id}] CPU={float(m.cpu_load):.1f}%  "
                    f"MEM_used={mem_used_percent:.1f}%  "
                    f"Batt={float(m.battery_level):.1f}%  "
                    f"Load1m={float(m.load_avg_1m):.2f}  "
                    f"Busy={busy_view} "
                    f"(task_busy={self.task_busy} overloaded={self.overloaded} load_max={self.load_max:.2f})"
                )

            time.sleep(max(0.01, self.publish_interval))

    def send_ack(self, task_id: str, task_type: str, status: str, details: str,
                 t_rx: float, t_start: float, t_end: float):
        ack = TaskAck(
            task_id=str(task_id),
            task_type=str(task_type),
            node_id=self.node_id,
            t_rx=float(t_rx),
            t_start=float(t_start),
            t_end=float(t_end),
            status=str(status),
            details=str(details)[:900],
        )
        self.ack_writer.write(ack)

    def handle_assignment(self, a: TaskAssignment):
        if a.node_id != self.node_id:
            return

        # Reject if overloaded (even if not running a task)
        if self.overloaded:
            now = time.time()
            self.send_ack(a.task_id, a.task_type, "FAIL", "node_overloaded_rejected", now, now, now)
            return

        with self.task_lock:
            if self.task_busy:
                now = time.time()
                self.send_ack(a.task_id, a.task_type, "FAIL", "node_busy_rejected", now, now, now)
                return
            self.task_busy = True

        t_rx = time.time()

        try:
            result = run_task(a.task_type, a.params_json)
            t_start = float(result.get("t_start", t_rx))
            t_end = float(result.get("t_end", time.time()))
            status = str(result.get("status", "FAIL"))
            details = str(result.get("details", ""))
        except Exception as e:
            t_start = time.time()
            t_end = time.time()
            status = "FAIL"
            details = f"exception: {e}"
            traceback.print_exc()

        self.send_ack(a.task_id, a.task_type, status, details, t_rx, t_start, t_end)

        with self.task_lock:
            self.task_busy = False

    def assignment_loop(self):
        while not self.stop_flag.is_set():
            samples = self.assign_reader.take()
            if not samples:
                time.sleep(0.02)
                continue

            for s in samples:
                a = getattr(s, "sample", s)
                if a is None:
                    continue
                self.handle_assignment(a)

    def run(self):
        threading.Thread(target=self.publish_loop, daemon=True).start()
        threading.Thread(target=self.assignment_loop, daemon=True).start()

        print(f"[NODE] {self.node_id} running... Ctrl+C to stop.")
        print(f"[NODE] thresholds: cpu_max={self.cpu_max} mem_avail_min={self.mem_avail_min} load_max={self.load_max:.2f} (cores={self.cpu_cores})")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_flag.set()
            time.sleep(0.2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node_id", required=True)
    ap.add_argument("--domain_id", type=int, default=0)
    ap.add_argument("--publish_interval", type=float, default=1.0)
    ap.add_argument("--print_every_n", type=int, default=1)

    # thresholds (local overload guard)
    ap.add_argument("--cpu_max", type=float, default=80.0)
    ap.add_argument("--mem_avail_min", type=float, default=0.25)
    ap.add_argument("--load_per_core_max", type=float, default=1.0)

    args = ap.parse_args()

    DDSNode(
        node_id=args.node_id,
        domain_id=args.domain_id,
        publish_interval=args.publish_interval,
        print_every_n=args.print_every_n,
        cpu_max=args.cpu_max,
        mem_avail_min=args.mem_avail_min,
        load_per_core_max=args.load_per_core_max,
    ).run()


if __name__ == "__main__":
    main()
