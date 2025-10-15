"""
worker.py
-----------

This module defines a simple worker that connects to Redis, polls for
tasks from a list called "queue", and delegates each task to the
appropriate handler. The worker spawns multiple threads to process
tasks concurrently. Each worker process (container) can run many
threads, and multiple worker processes can be scaled via Docker
replicas.

To customise behaviour, add new functions corresponding to the
different types of tasks or tasks names in your YAML configuration and
update the dispatch logic accordingly.
"""

import argparse
import json
import logging
import os
import threading
import time
from typing import Any, Dict

import httpx  # unused for now but available for future enhancements
import yaml   # unused for now but available for future enhancements
from redis import Redis

logging.basicConfig(level=logging.INFO, format="%(threadName)s %(message)s")


def get_redis() -> Redis:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL environment variable is not set")
    return Redis.from_url(redis_url)


def handle_task(task: Dict[str, Any]) -> None:
    """Dispatch a single task based on its type and payload."""
    task_type = task.get("type")
    payload = task.get("payload")

    if task_type == "GOAL":
        goal_desc = payload.get("description") if isinstance(payload, dict) else str(payload)
        logging.info(f"[GOAL] {goal_desc}")
        simulate_work()
    elif task_type == "TASK":
        name = payload.get("name") if isinstance(payload, dict) else str(payload)
        logging.info(f"[TASK] {name}")
        simulate_work()
    else:
        logging.warning(f"Unknown task type: {task_type}")
        simulate_work()


def simulate_work(seconds: int = 1) -> None:
    """Simulate doing work by sleeping for a given number of seconds."""
    time.sleep(seconds)


def worker_loop() -> None:
    """Continuously pop tasks from the queue and handle them."""
    redis_conn = get_redis()
    while True:
        try:
            item = redis_conn.blpop("queue", timeout=5)
            if item is None:
                continue  # No task; loop again
            _, raw_task = item
            try:
                task = json.loads(raw_task)
            except json.JSONDecodeError:
                logging.error(f"Failed to decode task: {raw_task!r}")
                continue
            handle_task(task)
        except Exception as exc:
            logging.error(f"Worker loop error: {exc}")
            time.sleep(1)


def main(concurrency: int) -> None:
    """Start multiple threads to process tasks concurrently."""
    threads = []
    for _ in range(concurrency):
        thread = threading.Thread(target=worker_loop, daemon=True)
        thread.start()
        threads.append(thread)
    logging.info(f"Started {concurrency} worker threads")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run task workers.")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent threads per process")
    args = parser.parse_args()
    main(args.concurrency)
