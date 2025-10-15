"""
controller.py
---------------

This module defines a simple FastAPI application that acts as the
central controller for the TapPay mega‑agent system. The controller
provides endpoints for health checking and for synchronising the task
queue with the goals and tasks defined in a YAML configuration file.

Endpoints:

* GET /health — returns basic health information and the number of
  queued tasks.
* POST /task/push — accepts a JSON payload representing a single task
  and appends it to the queue.
* POST /plan/sync — downloads the YAML configuration file specified by
  the `GITHUB_REPO_CONFIG` environment variable, converts its goals and
  tasks into queue entries, and populates the Redis list.

The controller uses the Redis service to store tasks in a list called
"queue". Workers consume tasks from this list and perform
corresponding actions.
"""

import json
import os
from typing import Dict

import httpx
import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from redis import Redis


class Task(BaseModel):
    """Pydantic model representing the expected payload for tasks."""
    type: str
    payload: Dict


def get_redis() -> Redis:
    """Initialise and return a Redis connection using the REDIS_URL env var."""
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL environment variable is not set")
    return Redis.from_url(redis_url)


app = FastAPI(title="TapPay Mega‑Agent Controller")


@app.get("/health")
def health() -> Dict[str, int]:
    """Return a simple health check and the current queue length."""
    r = get_redis()
    length = r.llen("queue")
    return {"ok": True, "tasks": length}


@app.post("/task/push")
def push_task(task: Task) -> Dict[str, bool]:
    """Push a single task onto the queue."""
    r = get_redis()
    r.rpush("queue", task.json())
    return {"queued": True}


@app.post("/plan/sync")
def plan_sync() -> Dict[str, int]:
    """Synchronise the task queue with the YAML configuration file.

    This endpoint downloads the YAML file referenced by the
    GITHUB_REPO_CONFIG environment variable, parses its goals and tasks,
    and enqueues one item per goal and per task. Each enqueued item is
    a JSON string with a `type` field ("GOAL" or "TASK") and a
    `payload` field containing the original YAML entry.
    """
    config_url = os.getenv("GITHUB_REPO_CONFIG")
    if not config_url:
        raise HTTPException(status_code=500, detail="GITHUB_REPO_CONFIG not set")

    try:
        # Use a short timeout to avoid hanging indefinitely
        with httpx.Client(timeout=20) as client:
            response = client.get(config_url)
            response.raise_for_status()
            yaml_data = yaml.safe_load(response.text)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch config: {exc}")

    agent = yaml_data.get("agent")
    if not agent:
        raise HTTPException(status_code=400, detail="'agent' section missing in YAML")

    goals = agent.get("goals", [])
    tasks = agent.get("tasks", [])

    r = get_redis()
    queued = 0
    for goal in goals:
        item = json.dumps({"type": "GOAL", "payload": goal})
        r.rpush("queue", item)
        queued += 1
    for task_entry in tasks:
        item = json.dumps({"type": "TASK", "payload": task_entry})
        r.rpush("queue", item)
        queued += 1

    return {"queued": queued}
