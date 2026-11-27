"""The documented samples load, and each script talks to its own broker."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from lupaxa.relay.client import RelayClient
from lupaxa.relay.config import load_config
from tests.conftest import RunningBroker

_ROOT = Path(__file__).resolve().parents[1] / "samples"


def test_sample_configs_load() -> None:
    hello = _load("hello")
    queue = _load("queue")
    subscribe = _load("subscribe")
    assert hello.socket_path() is not None
    assert [item.name for item in queue.queues] == ["image.resize", "image.resize.failed"]
    assert subscribe.bindings[0].queue == "deployment.jobs"
    assert subscribe.bindings[0].topic == "build.completed"


def test_hello_sample_publishes() -> None:
    payload = _run("hello", "publish.py")
    assert payload["sequence"] == 1
    assert payload["stored"] == 1


def test_queue_sample_acknowledges_one_job() -> None:
    root = _ROOT / "queue"
    _clean(root)
    previous = Path.cwd()
    os.chdir(root)
    try:
        running = RunningBroker(_load("queue"))
        try:
            with RelayClient(socket=running.socket) as client:
                job_id = client.enqueue("image.resize", {"image": "a.jpg"})
            completed = subprocess.run([sys.executable, str(root / "worker.py")], check=True, capture_output=True, text=True)
            heard = json.loads(completed.stdout)
            with RelayClient(socket=running.socket) as client:
                rows = client.inspect("image.resize")
        finally:
            running.stop()
    finally:
        os.chdir(previous)
        _clean(root)
    assert heard["message_id"] == job_id
    assert rows[0]["state"] == "completed"


def test_subscribe_sample_reads_one_event_and_copies_a_job() -> None:
    root = _ROOT / "subscribe"
    _clean(root)
    previous = Path.cwd()
    os.chdir(root)
    try:
        running = RunningBroker(_load("subscribe"))
        try:
            with RelayClient(socket=running.socket) as client:
                message_id = client.publish("build.completed", {"version": "1.2.0"}).message_id
            completed = subprocess.run([sys.executable, str(root / "listen.py")], check=True, capture_output=True, text=True)
            heard = json.loads(completed.stdout)
            with RelayClient(socket=running.socket) as client:
                jobs = client.inspect("deployment.jobs")
        finally:
            running.stop()
    finally:
        os.chdir(previous)
        _clean(root)
    assert heard["message_id"] == message_id
    assert jobs[0]["data"] == {"version": "1.2.0"}
    assert jobs[0]["id"] != message_id


def _load(name: str):
    root = _ROOT / name
    return load_config(root / "relay.yml", user_path=None)


def _run(name: str, script: str) -> dict[str, object]:
    root = _ROOT / name
    _clean(root)
    previous = Path.cwd()
    os.chdir(root)
    try:
        running = RunningBroker(_load(name))
        try:
            completed = subprocess.run([sys.executable, str(root / script)], check=True, capture_output=True, text=True)
        finally:
            running.stop()
    finally:
        os.chdir(previous)
        _clean(root)
    parsed = json.loads(completed.stdout)
    assert isinstance(parsed, dict)
    return parsed


def _clean(root: Path) -> None:
    for path in root.iterdir():
        generated = path.name in {"r.sock", "relay.sqlite3"} or path.name.startswith("relay.sqlite3")
        if generated or path.suffix in {".lock", ".pid", ".log"}:
            path.unlink(missing_ok=True)
