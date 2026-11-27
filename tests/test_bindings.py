"""A binding stores a queue job in the same commit as the event."""

from __future__ import annotations

import threading
from pathlib import Path
from queue import Queue

from lupaxa.relay.client import Delivery, RelayClient
from lupaxa.relay.config import load_config
from tests.conftest import RunningBroker, write_config
from tests.test_queues import _worker


def test_a_published_event_is_copied_onto_the_bound_queue(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with _worker(running.socket, "deployment.jobs") as worker:
            worker.wait_ready()
            with RelayClient(socket=running.socket) as client:
                event = client.publish("build.completed", {"version": "1.2.0"})
                history = client.history("build.completed")
            job = worker.take()
        assert history[0]["id"] == event.message_id
        assert history[0]["data"] == {"version": "1.2.0"}
        assert job.message_id != event.message_id
        assert job.queue == "deployment.jobs"
        assert job.data == {"version": "1.2.0"}
        assert job.attempt == 1
    finally:
        running.stop()


def test_an_unbound_topic_does_not_create_a_job(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with _worker(running.socket, "deployment.jobs", timeout=0.4) as worker:
            worker.wait_ready()
            with RelayClient(socket=running.socket) as client:
                client.publish("build.failed", {"version": "1.2.0"})
            worker.empty(0.6)
    finally:
        running.stop()


def test_each_matching_binding_gets_its_own_job(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with _worker(running.socket, "deployment.jobs") as deploy, _worker(running.socket, "audit.jobs") as audit:
            deploy.wait_ready()
            audit.wait_ready()
            with RelayClient(socket=running.socket) as client:
                event = client.publish("build.completed", {"version": "1.2.0"})
            left = deploy.take()
            right = audit.take()
        assert left.message_id != right.message_id
        assert left.message_id != event.message_id
        assert {left.queue, right.queue} == {"deployment.jobs", "audit.jobs"}
        assert left.data == right.data == {"version": "1.2.0"}
    finally:
        running.stop()


def test_publish_idempotency_does_not_copy_the_job_twice(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with _worker(running.socket, "deployment.jobs") as worker:
            worker.wait_ready()
            with RelayClient(socket=running.socket) as client:
                first = client.publish("build.completed", {"version": "1.2.0"}, idempotency_key="build-1")
                second = client.publish("build.completed", {"version": "1.2.0"}, idempotency_key="build-1")
            job = worker.take()
            assert worker.poll(0.3) is None
        assert first.message_id == second.message_id
        assert job.data == {"version": "1.2.0"}
    finally:
        running.stop()


def test_a_bound_job_survives_a_restart(workspace: Path) -> None:
    config = _config(workspace)
    first = RunningBroker(config)
    try:
        with RelayClient(socket=first.socket) as client:
            event = client.publish("build.completed", {"version": "1.2.0"})
    finally:
        first.stop()
    second = RunningBroker(config)
    try:
        with _worker(second.socket, "deployment.jobs") as worker:
            job = worker.take()
        assert job.message_id != event.message_id
        assert job.data == {"version": "1.2.0"}
    finally:
        second.stop()


def test_a_subscriber_still_receives_the_bound_event(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    heard: Queue[Delivery] = Queue()
    ready = threading.Event()

    def _listen() -> None:
        with RelayClient(socket=running.socket) as client:
            for item in client.subscribe("build.*", on_ready=ready.set):
                if isinstance(item, Delivery):
                    heard.put(item)
                    return

    thread = threading.Thread(target=_listen, daemon=True)
    try:
        thread.start()
        assert ready.wait(2)
        with _worker(running.socket, "deployment.jobs") as worker:
            worker.wait_ready()
            with RelayClient(socket=running.socket) as client:
                event = client.publish("build.completed", {"version": "1.2.0"})
            job = worker.take()
        delivery = heard.get(timeout=2)
        assert delivery.message_id == event.message_id
        assert delivery.data == job.data
        assert job.message_id != event.message_id
    finally:
        running.stop()
        thread.join(2)


def _config(root: Path):
    text = (
        "version: 1\n"
        "server:\n"
        f"  socket: {root / 'r.sock'}\n"
        "storage:\n"
        f"  path: {root / 'relay.sqlite3'}\n"
        "queues:\n"
        "  - name: deployment.jobs\n"
        "  - name: audit.jobs\n"
        "bindings:\n"
        "  - id: deployment-events\n"
        "    topic: build.completed\n"
        "    queue: deployment.jobs\n"
        "  - id: audit-events\n"
        "    topic: build.completed\n"
        "    queue: audit.jobs\n"
    )
    return load_config(write_config(root, text), user_path=None)
