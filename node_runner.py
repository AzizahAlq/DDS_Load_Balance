from node_tasks import open_url_task

def run_task(task_type: str):
    if task_type == "open_url":
        return open_url_task()
    else:
        return {
            "t_start": 0,
            "t_end": 0,
            "status": "FAIL",
            "details": "unknown_task"
        }

# Example local test (without DDS)
if __name__ == "__main__":
    result = run_task("open_url")
    print(result)
