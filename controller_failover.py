# controller_failover.py
# - Two controllers with heartbeat election (lowest controller_id wins)
# - CSV logging to --log_dir (default: ~/Desktop/Test/logs on the CONTROLLER machine)
# - Sends tasks ONLY when:
#     (1) this controller is leader
#     (2) node is not busy
#     (3) per-node cooldown passed
#     (4) thresholds OK
# - Random tasks from 3 types: open_url, run_script, ai_inference
# - Computes E2E timings from TaskAck:
#     ctrl_to_node_ms, queue_ms, exec_ms, total_e2e_ms
#
# IMPORTANT:
#   node_os refers to the OS of the NODES (Kali/Linux/Mac/Windows),
#   because run_script paths must exist on the NODE side.

import os
import csv
import json
import time
import uuid
import random
import argparse
import threading
import sys
from typing import Dict, Any, Tuple, List
from collections import deque

import psutil

from cyclonedds.domain import DomainParticipant
from cyclonedds.pub import Publisher, DataWriter
from cyclonedds.sub import Subscriber, DataReader
from cyclonedds.core import Qos, Policy
from cyclonedds.topic import Topic

from idl_types import NodeMetrics, TaskAssignment, TaskAck, ControllerHeartbeat


# -------------------------
# CSV helpers
# -------------------------
def qos_reliable() -> Qos:
    return Qos(
        Policy.Reliability.Reliable(1),
        Policy.Durability.Volatile,
        Policy.History.KeepLast(10),
    )


def ensure_csv(path: str, header: List[str]) -> None:
    # Creates dir + header only if file missing/empty (will NOT delete old data)
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    if (not os.path.exists(path)) or os.path.getsize(path) == 0:
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(header)


def append_row(path: str, row: List[Any]) -> None:
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow(row)


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def default_controller_log_dir() -> str:
    # Controller-local logs (Mac/Linux/Windows)
    return os.path.join(os.path.expanduser("~"), "Desktop", "Test", "logs")


class FailoverController:
    def __init__(
        self,
        controller_id: str,
        log_dir: str,
        node_os: str = "linux",          # <--- OS of NODES
        domain_id: int = 0,
        hb_period_s: float = 1.0,
        hb_timeout_s: float = 3.0,
        # thresholds (gate before dispatch)
        cpu_max: float = 70.0,
        mem_avail_min: float = 0.25,
        load_per_core_max: float = 1.0,
        # per-node cooldown (seconds) after a task finishes
        per_node_cooldown_s: float = 10.0,
        # optional system cooldown to reduce spam
        system_cooldown_s: float = 0.0,
    ):
        self.controller_id = str(controller_id)
        self.domain_id = int(domain_id)
        self.node_os = str(node_os).strip().lower()

        # Controller logs are LOCAL to controller machine
        self.log_dir = os.path.abspath(log_dir)
        os.makedirs(self.log_dir, exist_ok=True)

        self.hb_period_s = float(hb_period_s)
        self.hb_timeout_s = float(hb_timeout_s)

        self.cpu_max = float(cpu_max)
        self.mem_avail_min = float(mem_avail_min)

        self.cpu_cores = max(1, int(psutil.cpu_count(logical=True) or 1))
        # compare load_avg_1m against load_max
        self.load_max = float(load_per_core_max) * float(self.cpu_cores)

        self.per_node_cooldown_s = float(per_node_cooldown_s)
        self.system_cooldown_s = float(system_cooldown_s)
        self.last_system_dispatch = 0.0

        # leader election
        self.last_hb_seen: Dict[str, float] = {}
        self.leader_id: str = self.controller_id
        self.stop_flag = threading.Event()

        # node state
        self.node_metrics: Dict[str, NodeMetrics] = {}
        self.node_busy: Dict[str, bool] = {}
        self.node_last_finish: Dict[str, float] = {}  # last t_end from ack

        self.lock = threading.Lock()

        # timing + windows
        self.last_rx_time: Dict[str, float] = {}
        self.msgs_received = 0
        self.sec_start = time.time()
        self.lat_window_ms = deque(maxlen=5000)

        # assignment index (task_id -> t_tx)
        self.assign_index: Dict[str, Dict[str, Any]] = {}

        # weights for score
        self.w_cpu = 0.35
        self.w_mem = 0.35
        self.w_bat = 0.15
        self.w_load = 0.15

        # CSV paths
        def p(name: str) -> str:
            return os.path.join(self.log_dir, f"{name}_{self.controller_id}.csv")

        self.timing_csv = p("controller_timing_no_filter")
        self.optimal_csv = p("optimal_node_data")
        self.thr_csv = p("throughput_log")
        self.lat_csv = p("latency_log")
        self.assign_csv = p("assignment_log")
        self.ack_csv = p("task_ack_log")
        self.e2e_csv = p("task_e2e_log")

        ensure_csv(
            self.timing_csv,
            ["node_id", "t_rx", "t_pub", "latency_ms", "rx_interval_s", "controller_id", "leader_id_at_rx"],
        )
        ensure_csv(
            self.optimal_csv,
            ["t_rx", "t_pub", "best_node_id", "best_score", "cpu", "mem_avail", "battery", "load1m", "leader_id_at_rx", "controller_id"],
        )
        ensure_csv(self.thr_csv, ["t_sec", "msgs_per_sec", "controller_id", "leader_id_at_sec"])
        ensure_csv(self.lat_csv, ["t_sec", "avg_latency_ms", "min_latency_ms", "max_latency_ms", "count", "controller_id", "leader_id_at_sec"])
        ensure_csv(
            self.assign_csv,
            ["t_tx", "task_id", "task_type", "node_id", "reason", "params_json", "scores_json", "leader_id", "controller_id"],
        )
        ensure_csv(
            self.ack_csv,
            ["t_ack_rx_ctrl", "task_id", "task_type", "node_id", "status", "t_rx_node", "t_start", "t_end", "details", "leader_id", "controller_id"],
        )
        ensure_csv(
            self.e2e_csv,
            [
                "task_id", "task_type", "node_id",
                "t_tx_ctrl", "t_rx_node", "t_start", "t_end", "t_ack_rx_ctrl",
                "ctrl_to_node_ms", "queue_ms", "exec_ms", "total_e2e_ms", "status",
                "leader_id", "controller_id",
            ],
        )

        print(f"[CTRL {self.controller_id}] CSV folder: {self.log_dir}")

        # DDS setup
        self.participant = DomainParticipant(domain_id=self.domain_id)
        qos = qos_reliable()

        self.topic_metrics = Topic(self.participant, "node_metrics", NodeMetrics)
        self.topic_assign = Topic(self.participant, "task_assignment", TaskAssignment)
        self.topic_ack = Topic(self.participant, "task_ack", TaskAck)
        self.topic_hb = Topic(self.participant, "controller_heartbeat", ControllerHeartbeat)

        self.sub = Subscriber(self.participant)
        self.pub = Publisher(self.participant)

        self.metrics_reader = DataReader(self.sub, self.topic_metrics, qos=qos)
        self.ack_reader = DataReader(self.sub, self.topic_ack, qos=qos)
        self.hb_reader = DataReader(self.sub, self.topic_hb, qos=qos)

        self.assign_writer = DataWriter(self.pub, self.topic_assign, qos=qos)
        self.hb_writer = DataWriter(self.pub, self.topic_hb, qos=qos)

    # ---------------- leader election ----------------
    def _alive_controllers(self) -> Dict[str, float]:
        now = time.time()
        return {cid: ts for cid, ts in self.last_hb_seen.items() if now - ts <= self.hb_timeout_s}

    def elect_leader(self) -> None:
        alive = self._alive_controllers()
        alive[self.controller_id] = time.time()
        self.leader_id = sorted(alive.keys())[0]  # deterministic

    def is_leader(self) -> bool:
        return self.leader_id == self.controller_id

    # ---------------- normalization/score ----------------
    def score(self, m: NodeMetrics) -> float:
        cpu_good = clamp01(1.0 - float(m.cpu_load) / 100.0)
        mem_good = clamp01(float(m.mem_available_ratio))  # higher better
        bat_good = clamp01(float(m.battery_level) / 100.0)
        load_good = clamp01(1.0 / (1.0 + max(0.0, float(m.load_avg_1m))))
        return self.w_cpu * cpu_good + self.w_mem * mem_good + self.w_bat * bat_good + self.w_load * load_good

    def eligible_by_threshold(self, m: NodeMetrics) -> Tuple[bool, str]:
        if float(m.cpu_load) >= self.cpu_max:
            return False, "cpu_over"
        if float(m.mem_available_ratio) < self.mem_avail_min:
            return False, "mem_low"
        if float(m.load_avg_1m) >= self.load_max:
            return False, "load_over"
        return True, "ok"

    def cooldown_ok(self, node_id: str) -> bool:
        last_finish = float(self.node_last_finish.get(node_id, 0.0))
        if last_finish <= 0:
            return True
        return (time.time() - last_finish) >= self.per_node_cooldown_s

    def pick_best_free_node(self) -> Tuple[str, float, Dict[str, float], str]:
        scores: Dict[str, float] = {}
        for nid, m in self.node_metrics.items():
            if self.node_busy.get(nid, False):
                continue
            if not self.cooldown_ok(nid):
                continue
            ok, _reason = self.eligible_by_threshold(m)
            if not ok:
                continue
            scores[nid] = self.score(m)

        if not scores:
            return "", 0.0, {}, "no_free_eligible_node"

        best_id = max(scores, key=scores.get)
        return best_id, scores[best_id], scores, "best_free_node"

    # ---------------- logging ----------------
    def log_timing(self, m: NodeMetrics, t_rx: float) -> float:
        t_pub = float(m.timestamp)
        lat_ms = abs(t_rx - t_pub) * 1000.0

        rx_dt = ""
        if m.node_id in self.last_rx_time:
            rx_dt = f"{(t_rx - self.last_rx_time[m.node_id]):.6f}"
        self.last_rx_time[m.node_id] = t_rx

        append_row(
            self.timing_csv,
            [m.node_id, f"{t_rx:.6f}", f"{t_pub:.6f}", f"{lat_ms:.3f}", rx_dt, self.controller_id, self.leader_id],
        )
        return lat_ms

    def log_thr_lat_per_sec(self, now: float) -> None:
        elapsed = now - self.sec_start
        if elapsed < 1.0:
            return

        thr = self.msgs_received / elapsed
        t_sec = int(now)
        append_row(self.thr_csv, [t_sec, f"{thr:.3f}", self.controller_id, self.leader_id])

        if self.lat_window_ms:
            avg_lat = sum(self.lat_window_ms) / len(self.lat_window_ms)
            mn = min(self.lat_window_ms)
            mx = max(self.lat_window_ms)
            append_row(self.lat_csv, [t_sec, f"{avg_lat:.3f}", f"{mn:.3f}", f"{mx:.3f}", len(self.lat_window_ms), self.controller_id, self.leader_id])
            self.lat_window_ms.clear()

        self.msgs_received = 0
        self.sec_start = now

    def log_optimal(self, t_rx: float, best_id: str, best_score: float) -> None:
        m = self.node_metrics[best_id]
        append_row(
            self.optimal_csv,
            [
                f"{t_rx:.6f}",
                f"{float(m.timestamp):.6f}",
                best_id,
                f"{best_score:.6f}",
                f"{float(m.cpu_load):.2f}",
                f"{float(m.mem_available_ratio):.4f}",
                f"{float(m.battery_level):.1f}",
                f"{float(m.load_avg_1m):.3f}",
                self.leader_id,
                self.controller_id,
            ],
        )

    # ---------------- tasks (multi-platform) ----------------
    @staticmethod
    def _node_run_paths(node_os: str) -> Tuple[str, str, str]:
        """
        Returns (python_bin, script_path, cwd) FOR THE NODE MACHINE.
        node_os: "linux" | "mac" | "windows"
        """
        os_key = (node_os or "").strip().lower()

        if os_key in ("linux", "kali", "ubuntu", "debian"):
            return (
                "python3",
                "/home/kali/Desktop/Test/Standalone_CIC-BCCC-NRC_2024.py",
                "/home/kali/Desktop/Test",
            )

        if os_key in ("mac", "macos", "darwin"):
          home = os.path.expanduser("~")  # automatically /Users/username 
          script_path = os.path.join(home, "Desktop", "Test", "Standalone_CIC-BCCC-NRC_2024.py")
          cwd = os.path.join(home, "Desktop", "Test")
          return ("python3", script_path, cwd)

        if os_key in ("win", "windows"):
             return (
                 "python",  # or "python3" if installed
                 r"C:\Users\nasse\OneDrive\Desktop\Test\node_client.py",
                r"C:\Users\nasse\OneDrive\Desktop\Test",
            )
        # default fallback
        return (
            "python3",
            "/home/kali/Desktop/Test/Standalone_CIC-BCCC-NRC_2024.py",
            "/home/kali/Desktop/Test",
        )

    def make_random_task(self) -> Tuple[str, str]:
        choice = random.choice(["open_url", "run_script", "ai_inference"])

        if choice == "open_url":
            params = {"url": "https://www.youtube.com/watch?v=qYNweeDHiyU&t=4s"}

        elif choice == "ai_inference":
            params = {"sleep_s": 1.5}

        else:
            # IMPORTANT: paths must exist on the NODE machine.
            pybin, script_path, cwd = self._node_run_paths(self.node_os)
            params = {"python_bin": pybin, "script_path": script_path, "cwd": cwd, "args": []}

        return choice, json.dumps(params)

    def send_task_if_leader(self, node_id: str, reason: str, scores: Dict[str, float]) -> None:
        if not self.is_leader():
            return

        if self.system_cooldown_s > 0:
            now = time.time()
            if (now - self.last_system_dispatch) < self.system_cooldown_s:
                return
            self.last_system_dispatch = now

        # mark busy BEFORE send to prevent duplicates
        self.node_busy[node_id] = True

        task_id = str(uuid.uuid4())
        task_type, params_json = self.make_random_task()
        t_tx = time.time()

        msg = TaskAssignment(
            task_id=task_id,
            task_type=task_type,
            params_json=params_json,
            node_id=node_id,
            t_tx=float(t_tx),
        )
        self.assign_writer.write(msg)

        append_row(
            self.assign_csv,
            [f"{t_tx:.6f}", task_id, task_type, node_id, reason, params_json, json.dumps(scores), self.leader_id, self.controller_id],
        )

        self.assign_index[task_id] = {"t_tx_ctrl": t_tx, "task_type": task_type, "node_id": node_id}

        print(f"[{self.controller_id}] LEADER={self.leader_id} SEND {task_type} -> {node_id} task_id={task_id}")

    # ---------------- ack handling ----------------
    def handle_ack(self, ack: TaskAck) -> None:
        t_ack_rx_ctrl = time.time()

        append_row(
            self.ack_csv,
            [
                f"{t_ack_rx_ctrl:.6f}",
                ack.task_id,
                ack.task_type,
                ack.node_id,
                ack.status,
                f"{float(ack.t_rx):.6f}",
                f"{float(ack.t_start):.6f}",
                f"{float(ack.t_end):.6f}",
                str(ack.details)[:900],
                self.leader_id,
                self.controller_id,
            ],
        )

        # clear busy + update finish time
        self.node_busy[ack.node_id] = False
        self.node_last_finish[ack.node_id] = float(ack.t_end)

        rec = self.assign_index.get(ack.task_id)
        if not rec:
            return

        t_tx_ctrl = float(rec["t_tx_ctrl"])
        t_rx_node = float(ack.t_rx)
        t_start = float(ack.t_start)
        t_end = float(ack.t_end)

        ctrl_to_node_ms = (t_rx_node - t_tx_ctrl) * 1000.0
        queue_ms = (t_start - t_rx_node) * 1000.0
        exec_ms = (t_end - t_start) * 1000.0
        total_e2e_ms = (t_ack_rx_ctrl - t_tx_ctrl) * 1000.0

        append_row(
            self.e2e_csv,
            [
                ack.task_id,
                ack.task_type,
                ack.node_id,
                f"{t_tx_ctrl:.6f}",
                f"{t_rx_node:.6f}",
                f"{t_start:.6f}",
                f"{t_end:.6f}",
                f"{t_ack_rx_ctrl:.6f}",
                f"{ctrl_to_node_ms:.3f}",
                f"{queue_ms:.3f}",
                f"{exec_ms:.3f}",
                f"{total_e2e_ms:.3f}",
                ack.status,
                self.leader_id,
                self.controller_id,
            ],
        )

    # ---------------- DDS loops ----------------
    def heartbeat_loop(self) -> None:
        while not self.stop_flag.is_set():
            self.elect_leader()
            hb = ControllerHeartbeat(controller_id=self.controller_id, leader_id=self.leader_id, timestamp=float(time.time()))
            self.hb_writer.write(hb)
            time.sleep(self.hb_period_s)

    def hb_listen_loop(self) -> None:
        while not self.stop_flag.is_set():
            samples = self.hb_reader.take()
            if not samples:
                time.sleep(0.05)
                continue
            for s in samples:
                hb = getattr(s, "sample", s)
                if hb is None:
                    continue
                self.last_hb_seen[hb.controller_id] = time.time()

    def metrics_loop(self) -> None:
        while not self.stop_flag.is_set():
            samples = self.metrics_reader.take()
            if not samples:
                time.sleep(0.01)
                continue

            for s in samples:
                m = getattr(s, "sample", s)
                if m is None:
                    continue

                now = time.time()
                lat_ms = self.log_timing(m, now)

                self.msgs_received += 1
                self.lat_window_ms.append(lat_ms)
                self.log_thr_lat_per_sec(now)

                with self.lock:
                    self.node_metrics[m.node_id] = m
                    self.node_busy.setdefault(m.node_id, False)

                    best_id, best_score, scores, reason = self.pick_best_free_node()
                    if not best_id:
                        continue

                    self.log_optimal(now, best_id, best_score)
                    self.send_task_if_leader(best_id, reason, scores)

    def ack_loop(self) -> None:
        while not self.stop_flag.is_set():
            samples = self.ack_reader.take()
            if not samples:
                time.sleep(0.02)
                continue
            for s in samples:
                ack = getattr(s, "sample", s)
                if ack is None:
                    continue
                self.handle_ack(ack)

    def run(self) -> None:
        threads = [
            threading.Thread(target=self.heartbeat_loop, daemon=True),
            threading.Thread(target=self.hb_listen_loop, daemon=True),
            threading.Thread(target=self.metrics_loop, daemon=True),
            threading.Thread(target=self.ack_loop, daemon=True),
        ]
        for t in threads:
            t.start()

        print(f"[CTRL] controller_id={self.controller_id} running")
        print(f"[CTRL] leader election: lowest controller_id wins")
        print(f"[CTRL] log_dir (controller local): {self.log_dir}")
        print(f"[CTRL] node_os (paths sent to nodes): {self.node_os}")
        print(f"[CTRL] thresholds: cpu_max={self.cpu_max} mem_avail_min={self.mem_avail_min} load_max={self.load_max:.2f} (cores={self.cpu_cores})")
        print(f"[CTRL] cooldowns: per_node_cooldown_s={self.per_node_cooldown_s} system_cooldown_s={self.system_cooldown_s}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_flag.set()
            time.sleep(0.2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--controller_id", required=True)  # controllerA / controllerB
    ap.add_argument("--log_dir", default=default_controller_log_dir())  # controller-local
    ap.add_argument("--node_os", default="linux", help="linux | mac | windows (NODE OS)")

    ap.add_argument("--domain_id", type=int, default=0)
    ap.add_argument("--hb_period", type=float, default=1.0)
    ap.add_argument("--hb_timeout", type=float, default=3.0)

    ap.add_argument("--cpu_max", type=float, default=80.0)
    ap.add_argument("--mem_avail_min", type=float, default=0.25)
    ap.add_argument("--load_per_core_max", type=float, default=1.0)

    ap.add_argument("--per_node_cooldown", type=float, default=10.0)
    ap.add_argument("--system_cooldown", type=float, default=0.0)

    args = ap.parse_args()

    c = FailoverController(
        controller_id=args.controller_id,
        log_dir=args.log_dir,
        node_os=args.node_os,
        domain_id=args.domain_id,
        hb_period_s=args.hb_period,
        hb_timeout_s=args.hb_timeout,
        cpu_max=args.cpu_max,
        mem_avail_min=args.mem_avail_min,
        load_per_core_max=args.load_per_core_max,
        per_node_cooldown_s=args.per_node_cooldown,
        system_cooldown_s=args.system_cooldown,
    )
    c.run()


if __name__ == "__main__":
    main()
