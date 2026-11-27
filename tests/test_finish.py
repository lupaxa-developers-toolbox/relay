"""History, counters, backpressure, diagnostics, and CLI conventions."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from pytest import CaptureFixture

from lupaxa.relay.cli import main
from lupaxa.relay.client import Delivery, RelayClient
from lupaxa.relay.config import load_config
from lupaxa.relay.exceptions import ProtocolError, UsageError
from lupaxa.relay.observe import configure_logging
from tests.conftest import RunningBroker, isolated_config, write_config
from tests.test_queues import _worker


def test_history_pattern_and_delivery_log(running: RunningBroker) -> None:
    with RelayClient(socket=running.socket) as client:
        completed = client.publish("build.completed", {"n": 1})
        client.publish("build.failed", {"n": 2})
        client.publish("image.resized", {"n": 3})
        listed = client.history("build.*")
    names = {item["destination"] for item in listed}
    assert names == {"build.completed", "build.failed"}
    listener = RelayClient(socket=running.socket)
    listener.__enter__()
    try:
        delivery = next(iter(listener.subscribe("build.*", durable="auditor", start="earliest")))
        assert isinstance(delivery, Delivery)
        delivery.ack()
    finally:
        listener.close()
    with RelayClient(socket=running.socket) as client:
        detail = client.message(completed.message_id)
    logged = detail["deliveries"]
    assert isinstance(logged, list)
    actions = [item["action"] for item in logged if isinstance(item, dict)]
    assert actions == ["deliver", "ack"]


def test_counters_record_a_job_attempt(workspace: Path) -> None:
    running = RunningBroker(_queue_config(workspace))
    try:
        with RelayClient(socket=running.socket) as client:
            client.publish("build.completed", {"n": 1})
            job_id = client.enqueue("image.resize", {"n": 1})
        with _worker(running.socket, "image.resize") as worker:
            held = worker.take()
            held.ack()
        with RelayClient(socket=running.socket) as client:
            metrics = client.status()["metrics"]
            detail = client.message(job_id)
    finally:
        running.stop()
    assert isinstance(metrics, dict)
    assert metrics["published_events"] == 1
    assert metrics["enqueued_jobs"] == 1
    assert metrics["delivered_messages"] >= 1
    assert metrics["acknowledged_deliveries"] >= 1
    logged = detail["deliveries"]
    assert isinstance(logged, list)
    actions = [item["action"] for item in logged if isinstance(item, dict)]
    assert "deliver" in actions
    assert "ack" in actions


def test_a_full_queue_rejects_another_job(workspace: Path) -> None:
    running = RunningBroker(_queue_config(workspace, max_depth=1))
    try:
        with RelayClient(socket=running.socket) as client:
            client.enqueue("image.resize", {"n": 1})
            with pytest.raises(ProtocolError) as caught:
                client.enqueue("image.resize", {"n": 2})
            left = client.inspect("image.resize")
    finally:
        running.stop()
    assert caught.value.code == "CAPACITY"
    assert len(left) == 1


def test_prefetch_stops_a_slow_consumer(workspace: Path) -> None:
    running = RunningBroker(_queue_config(workspace, max_prefetch=2))
    try:
        with RelayClient(socket=running.socket) as producer:
            for number in range(5):
                producer.enqueue("image.resize", {"n": number})
            with pytest.raises(ProtocolError) as caught:
                producer._roundtrip(
                    {
                        "op": "consume",
                        "request_id": "too-many",
                        "queue": "image.resize",
                        "prefetch": 9,
                    }
                )
        with RelayClient(socket=running.socket, timeout=1) as client:
            client._roundtrip(
                {
                    "op": "consume",
                    "request_id": "one",
                    "queue": "image.resize",
                    "prefetch": 1,
                }
            )
            first = client.poll(0.5)
            second = client.poll(0.3)
    finally:
        running.stop()
    assert caught.value.code == "INVALID_MESSAGE"
    assert isinstance(first, dict)
    assert first["op"] == "delivery"
    assert second is None


def test_prefetch_default_cannot_exceed_the_maximum(workspace: Path) -> None:
    text = "version: 1\nconsumers:\n  default_prefetch: 50\n  max_prefetch: 10\n"
    with pytest.raises(UsageError, match="default_prefetch"):
        load_config(write_config(workspace, text), user_path=None)


def test_doctor_reports_schema_socket_and_a_listening_broker(workspace: Path, capsys: CaptureFixture[str]) -> None:
    config = isolated_config(workspace)
    absent = main(["--config", str(workspace / "relay.yml"), "doctor"])
    assert absent == 0
    before = capsys.readouterr().out
    assert "Database has not been created." in before
    assert "Socket is not present." in before
    running = RunningBroker(config)
    try:
        code = main(
            [
                "--config",
                str(workspace / "relay.yml"),
                "--socket",
                str(running.socket),
                "doctor",
            ]
        )
        heard = capsys.readouterr().out
    finally:
        running.stop()
    assert code == 0
    assert "Schema 5 is compatible." in heard
    assert "Socket permissions are restrictive." in heard
    assert "Broker is listening." in heard


def test_quiet_hides_doctor_text(workspace: Path, capsys: CaptureFixture[str]) -> None:
    isolated_config(workspace)
    assert main(["--quiet", "--config", str(workspace / "relay.yml"), "doctor"]) == 0
    assert capsys.readouterr().out == ""


def test_quiet_and_verbose_conflict() -> None:
    assert main(["--quiet", "--verbose", "config", "path"]) == 2


def test_timeout_is_applied_when_nothing_is_listening(workspace: Path, capsys: CaptureFixture[str]) -> None:
    code = main(["--socket", str(workspace / "missing.sock"), "--timeout", "0.2", "status"])
    assert code == 3
    assert "Traceback" not in capsys.readouterr().err


def test_dry_run_purge_does_not_delete(workspace: Path, capsys: CaptureFixture[str]) -> None:
    running = RunningBroker(_queue_config(workspace))
    try:
        with RelayClient(socket=running.socket) as client:
            client.enqueue("image.resize", {"n": 1})
        code = main(["--socket", str(running.socket), "--dry-run", "queue", "purge", "image.resize"])
        payload = json.loads(capsys.readouterr().out)
        with RelayClient(socket=running.socket) as client:
            left = client.inspect("image.resize")
    finally:
        running.stop()
    assert code == 0
    assert payload == {"queue": "image.resize", "deleted": 1, "dry_run": True}
    assert len(left) == 1


def test_json_logs_omit_payloads(workspace: Path, capsys: CaptureFixture[str]) -> None:
    text = "version: 1\nlogging:\n  format: json\n  level: info\n"
    config = load_config(write_config(workspace, text), user_path=None)
    configure_logging(config)
    try:
        logging.getLogger("lupaxa.relay").info("accepted event")
        captured = capsys.readouterr().err.strip().splitlines()[-1]
    finally:
        logging.getLogger("lupaxa.relay").handlers.clear()
    payload = json.loads(captured)
    assert payload["message"] == "accepted event"
    assert payload["level"] == "INFO"
    assert "data" not in payload


def _queue_config(root: Path, *, max_depth: int | None = None, max_prefetch: int = 100):
    depth = f"    max_depth: {max_depth}\n" if max_depth is not None else ""
    text = (
        "version: 1\n"
        "consumers:\n"
        "  default_prefetch: 1\n"
        f"  max_prefetch: {max_prefetch}\n"
        "server:\n"
        f"  socket: {root / 'r.sock'}\n"
        "storage:\n"
        f"  path: {root / 'relay.sqlite3'}\n"
        "queues:\n"
        "  - name: image.resize\n"
        "    max_attempts: 5\n"
        f"{depth}"
        "    dead_letter_queue: image.resize.failed\n"
        "  - name: image.resize.failed\n"
    )
    return load_config(write_config(root, text), user_path=None)
