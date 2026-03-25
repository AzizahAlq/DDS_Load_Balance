# node_tasks.py
import os
import json
import time
import platform
import subprocess
import webbrowser


def open_url_task(params: dict) -> dict:
    url = params.get("url", "https://www.youtube.com/watch?v=qYNweeDHiyU&t=4s")
    t_start = time.time()
    try:
        system = platform.system().lower()
        if "darwin" in system:
            subprocess.run(["open", url], check=False)
        elif "linux" in system:
            subprocess.run(["xdg-open", url], check=False)
        else:
            webbrowser.open(url)

        status = "SUCCESS"
        details = f"opened_url: {url}"
    except Exception as e:
        status = "FAIL"
        details = f"open_url error: {e}"
    t_end = time.time()
    return {"t_start": t_start, "t_end": t_end, "status": status, "details": details}


def ai_inference_task(params: dict) -> dict:
    # Just a CPU-like workload simulation
    sleep_s = float(params.get("sleep_s", 0.7))
    t_start = time.time()
    try:
        time.sleep(sleep_s)
        status = "SUCCESS"
        details = f"ai_inference simulated sleep_s={sleep_s}"
    except Exception as e:
        status = "FAIL"
        details = f"ai_inference error: {e}"
    t_end = time.time()
    return {"t_start": t_start, "t_end": t_end, "status": status, "details": details}


def run_script_task(params: dict) -> dict:
    python_bin = params.get("python_bin", "python3.10")
    script_path = params.get("script_path")
    cwd = params.get("cwd", None)
    args = params.get("args", [])

    t_start = time.time()
    try:
        if not script_path:
            raise ValueError("missing script_path")

        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"script not found: {script_path}")

        cmd = [python_bin, script_path] + list(args)
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)

        status = "SUCCESS" if proc.returncode == 0 else "FAIL"

        # keep details short
        out_tail = (proc.stdout or "")[-400:]
        err_tail = (proc.stderr or "")[-400:]
        details = f"rc={proc.returncode} out_tail={out_tail} err_tail={err_tail}".replace("\n", " | ")

    except Exception as e:
        status = "FAIL"
        details = f"run_script error: {e}"

    t_end = time.time()
    return {"t_start": t_start, "t_end": t_end, "status": status, "details": details}


def run_task(task_type: str, params_json: str) -> dict:
    params = {}
    try:
        if params_json:
            params = json.loads(params_json)
    except Exception:
        params = {}

    if task_type == "open_url":
        return open_url_task(params)
    if task_type == "ai_inference":
        return ai_inference_task(params)
    if task_type == "run_script":
        return run_script_task(params)

    now = time.time()
    return {"t_start": now, "t_end": now, "status": "FAIL", "details": f"unknown_task: {task_type}"}
