"""Kill the broker during critical operations and reopen a consistent database."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

from lupaxa.relay.client import RelayClient
from lupaxa.relay.config import load_config
from lupaxa.relay.exceptions import ConnectionFailed
from tests.conftest import RunningBroker, write_config
from tests.test_queues import _worker

_STATES = {"pending", "retry", "leased", "completed", "dead"}


def test_a_committed_publish_survives_a_kill(workspace: Path) -> None:
    path = _config(workspace)
    process = _start(path, workspace)
    try:
        with RelayClient(socket=workspace / "r.sock") as client:
            message_id = client.publish(
                "build.completed",
                {"n": 1},
                idempotency_key="same",
            ).message_id
    finally:
        _kill(process)
    _consistent(workspace / "relay.sqlite3")
    running = RunningBroker(load_config(path, user_path=None))
    try:
        with RelayClient(socket=running.socket) as client:
            again = client.publish("build.completed", {"n": 1}, idempotency_key="same")
    finally:
        running.stop()
    assert again.message_id == message_id


def test_a_kill_during_publication_leaves_a_readable_database(workspace: Path) -> None:
    path = _config(workspace)
    process = _start(path, workspace)
    stop = threading.Event()

    def publish() -> None:
        try:
            with RelayClient(socket=workspace / "r.sock") as client:
                number = 0
                while not stop.is_set():
                    client.publish("build.completed", {"n": number})
                    number += 1
        except ConnectionFailed:
            return

    thread = threading.Thread(target=publish)
    thread.start()
    time.sleep(0.2)
    stop.set()
    _kill(process)
    thread.join(2)
    _consistent(workspace / "relay.sqlite3")


def test_a_kill_keeps_a_leased_job(workspace: Path) -> None:
    path = _config(workspace)
    process = _start(path, workspace)
    try:
        with RelayClient(socket=workspace / "r.sock") as client:
            job_id = client.enqueue("image.resize", {"n": 1})
        worker = _worker(workspace / "r.sock", "image.resize")
        assert worker.take().message_id == job_id
        _kill(process)
        worker.close()
    except Exception:
        _kill(process)
        raise
    _consistent(workspace / "relay.sqlite3")
    with sqlite3.connect(workspace / "relay.sqlite3") as conn:
        row = conn.execute("SELECT state FROM queue_jobs WHERE id = ?", (job_id,)).fetchone()
    assert row is not None
    assert row[0] == "leased"


def test_a_kill_keeps_an_acknowledged_job(workspace: Path) -> None:
    path = _config(workspace)
    process = _start(path, workspace)
    try:
        with RelayClient(socket=workspace / "r.sock") as client:
            job_id = client.enqueue("image.resize", {"n": 1})
        with _worker(workspace / "r.sock", "image.resize") as worker:
            worker.take().ack()
    finally:
        _kill(process)
    _consistent(workspace / "relay.sqlite3")
    with sqlite3.connect(workspace / "relay.sqlite3") as conn:
        row = conn.execute("SELECT state FROM queue_jobs WHERE id = ?", (job_id,)).fetchone()
    assert row is not None
    assert row[0] == "completed"


def test_a_kill_keeps_a_dead_letter(workspace: Path) -> None:
    path = _config(workspace)
    process = _start(path, workspace)
    try:
        with RelayClient(socket=workspace / "r.sock") as client:
            job_id = client.enqueue("image.resize", {"n": 1})
        with _worker(workspace / "r.sock", "image.resize") as worker:
            worker.take().nack()
    finally:
        _kill(process)
    _consistent(workspace / "relay.sqlite3")
    with sqlite3.connect(workspace / "relay.sqlite3") as conn:
        row = conn.execute(
            "SELECT queue, state FROM queue_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
    assert row == ("image.resize.failed", "pending")


def test_a_kill_during_retention_leaves_a_readable_database(workspace: Path) -> None:
    path = _config(workspace, max_messages=1)
    process = _start(path, workspace)
    stop = threading.Event()

    def publish() -> None:
        try:
            with RelayClient(socket=workspace / "r.sock") as client:
                number = 0
                while not stop.is_set():
                    client.publish("build.completed", {"n": number})
                    number += 1
        except ConnectionFailed:
            return

    thread = threading.Thread(target=publish)
    thread.start()
    time.sleep(0.2)
    stop.set()
    _kill(process)
    thread.join(2)
    _consistent(workspace / "relay.sqlite3")


def _config(root: Path, *, max_messages: int = 100000) -> Path:
    text = (
        "version: 1\n"
        "server:\n"
        f"  socket: {root / 'r.sock'}\n"
        "storage:\n"
        f"  path: {root / 'relay.sqlite3'}\n"
        "  retention:\n"
        "    events:\n"
        f"      max_messages: {max_messages}\n"
        "queues:\n"
        "  - name: image.resize\n"
        "    max_attempts: 1\n"
        "    dead_letter_queue: image.resize.failed\n"
        "  - name: image.resize.failed\n"
    )
    return write_config(root, text)


def _start(path: Path, root: Path) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "lupaxa.relay",
            "--config",
            str(path),
            "--socket",
            str(root / "r.sock"),
            "serve",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("broker exited during start")
        try:
            with RelayClient(socket=root / "r.sock", timeout=0.3) as client:
                client.status()
            return process
        except ConnectionFailed:
            time.sleep(0.05)
    _kill(process)
    raise AssertionError("broker did not start")


def _kill(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    if os.name == "posix":
        os.kill(process.pid, signal.SIGKILL)
    else:
        process.kill()
    process.wait(timeout=5)


def _consistent(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        row = conn.execute("PRAGMA quick_check").fetchone()
        assert row is not None
        assert row[0] == "ok"
        for data, headers in conn.execute("SELECT data, headers FROM messages"):
            json.loads(data)
            json.loads(headers)
        found = conn.execute("SELECT payload, headers, state FROM queue_jobs")
        for payload, headers, state in found:
            json.loads(payload)
            json.loads(headers)
            assert state in _STATES
