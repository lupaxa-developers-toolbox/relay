"""Administration commands talk to the broker instead of opening its database."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from lupaxa.relay.cli import main
from lupaxa.relay.client import RelayClient
from lupaxa.relay.config import load_config
from lupaxa.relay.exceptions import ProtocolError
from tests.conftest import RunningBroker, write_config
from tests.test_queues import _worker


def test_queue_stats_and_history_show_a_published_event(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with RelayClient(socket=running.socket) as client:
            event = client.publish("build.completed", {"version": "1"})
        code = main(["--socket", str(running.socket), "history", "--message", event.message_id])
        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["event"]["id"] == event.message_id
        code = main(["--socket", str(running.socket), "topic", "list"])
        assert code == 0
        topics = json.loads(capsys.readouterr().out)
        assert topics == [{"name": "build.completed", "messages": 1}]
    finally:
        running.stop()


def test_retry_puts_a_dead_lettered_job_back(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with _worker(running.socket, "once") as worker:
            worker.wait_ready()
            with RelayClient(socket=running.socket) as client:
                message_id = client.enqueue("once", {"n": 1})
            item = worker.take()
            item.nack(requeue=True)
        code = main(
            [
                "--socket",
                str(running.socket),
                "queue",
                "retry",
                "image.resize.failed",
                "--message",
                message_id,
            ]
        )
        assert code == 0
        job = json.loads(capsys.readouterr().out)
        assert job["id"] == message_id
        assert job["queue"] == "once"
        assert job["state"] == "pending"
        assert job["attempts"] == 0
        assert job["lifetime_attempts"] == 1
        assert job["failure_reason"]
        with _worker(running.socket, "once") as worker:
            again = worker.take()
        assert again.message_id == message_id
        assert again.attempt == 1
    finally:
        running.stop()


def test_purge_requires_confirmation_and_leaves_a_leased_job(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = RunningBroker(_config(workspace))
    try:
        refused = main(["--socket", str(running.socket), "queue", "purge", "image.resize"])
        assert refused == 2
        with RelayClient(socket=running.socket) as client:
            first = client.enqueue("image.resize", {"n": 1})
            second = client.enqueue("image.resize", {"n": 2})
        with _worker(running.socket, "image.resize") as worker:
            worker.wait_ready()
            held = worker.take()
            code = main(
                ["--socket", str(running.socket), "queue", "purge", "image.resize", "--yes"]
            )
            assert code == 0
            assert json.loads(capsys.readouterr().out)["deleted"] == 1
            held.ack()
        with RelayClient(socket=running.socket) as client:
            left = {job["id"] for job in client.inspect("image.resize")}
        assert held.message_id in left
        assert second not in left
        assert first == held.message_id
    finally:
        running.stop()


def test_created_queue_and_subscription_survive_a_restart(workspace: Path) -> None:
    config = _config(workspace)
    first = RunningBroker(config)
    try:
        with RelayClient(socket=first.socket) as client:
            client.create_queue("reports.generate")
            created = client.create_subscription("auditor", "build.*")
            client.enqueue("reports.generate", {"n": 1})
        assert created["name"] == "auditor"
    finally:
        first.stop()
    second = RunningBroker(config)
    try:
        with RelayClient(socket=second.socket) as client:
            status = client.status()
        queues = status["queues"]
        subscriptions = status["subscriptions"]
        assert isinstance(queues, list)
        names = {item["name"] for item in queues if isinstance(item, dict)}
        assert "reports.generate" in names
        assert isinstance(subscriptions, list)
        assert subscriptions[0]["name"] == "auditor"
    finally:
        second.stop()


def test_backup_copies_the_event(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    destination = workspace / "copy.sqlite3"
    try:
        with RelayClient(socket=running.socket) as client:
            event = client.publish("build.completed", {"version": "1"})
            client.backup(destination)
    finally:
        running.stop()
    with sqlite3.connect(destination) as conn:
        row = conn.execute("SELECT id FROM messages").fetchone()
    assert row is not None
    assert row[0] == event.message_id


def test_storage_check_and_subscription_delete(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with RelayClient(socket=running.socket) as client:
            client.create_subscription("auditor", "build.*")
        code = main(["--socket", str(running.socket), "storage", "check"])
        assert code == 0
        assert "healthy" in capsys.readouterr().out
        code = main(
            [
                "--socket",
                str(running.socket),
                "subscription",
                "delete",
                "auditor",
                "--yes",
            ]
        )
        assert code == 0
        with (
            pytest.raises(ProtocolError) as caught,
            RelayClient(socket=running.socket) as client,
        ):
            client.delete_subscription("auditor")
        assert caught.value.code == "NOT_FOUND"
    finally:
        running.stop()


def test_daemon_start_status_and_stop(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = write_config(workspace, _text(workspace))
    code = main(["--config", str(path), "daemon", "start"])
    assert code == 0
    started = json.loads(capsys.readouterr().out)
    try:
        code = main(["--config", str(path), "daemon", "status"])
        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["process"] is True
        assert payload["reachable"] is True
        assert payload["pid"] == started["pid"]
    finally:
        code = main(["--config", str(path), "daemon", "stop"])
        assert code == 0


def _config(root: Path):
    return load_config(write_config(root, _text(root)), user_path=None)


def _text(root: Path) -> str:
    return (
        "version: 1\n"
        "server:\n"
        f"  socket: {root / 'r.sock'}\n"
        "storage:\n"
        f"  path: {root / 'relay.sqlite3'}\n"
        "queues:\n"
        "  - name: image.resize\n"
        "    max_attempts: 5\n"
        "    visibility_timeout_seconds: 10\n"
        "    dead_letter_queue: image.resize.failed\n"
        "  - name: image.resize.failed\n"
        "    max_attempts: 2\n"
        "  - name: once\n"
        "    max_attempts: 1\n"
        "    visibility_timeout_seconds: 10\n"
        "    dead_letter_queue: image.resize.failed\n"
    )
