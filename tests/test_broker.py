"""A local broker persists events before it acknowledges them."""

from __future__ import annotations

import json
import socket
import stat
from pathlib import Path

import pytest

from lupaxa.relay.client import AsyncRelayClient, RelayClient
from lupaxa.relay.exceptions import BrokerBusy, ProtocolError, UsageError
from lupaxa.relay.ids import uuid7
from tests.conftest import RunningBroker, isolated_config, write_config


def test_uuid7_is_unique_and_versioned() -> None:
    values = [uuid7() for _ in range(50)]
    assert len(set(values)) == 50
    for value in values:
        assert value[14] == "7"


def test_publish_is_durable_and_sequenced(running: RunningBroker) -> None:
    with RelayClient(socket=running.socket) as client:
        first = client.publish("build.completed", {"project": "example", "version": "1.2.0"})
        second = client.publish("build.completed", {"project": "example", "version": "1.2.1"})
        history = client.history(topic="build.completed")
    assert first.sequence == 1
    assert second.sequence == 2
    assert first.message_id != second.message_id
    assert [item["sequence"] for item in history] == [1, 2]
    data = history[0]["data"]
    assert isinstance(data, dict)
    assert data["version"] == "1.2.0"
    assert history[0]["kind"] == "event"


def test_messages_survive_a_restart(workspace: Path) -> None:
    config = isolated_config(workspace)
    first = RunningBroker(config)
    try:
        with RelayClient(socket=first.socket) as client:
            published = client.publish("build.completed", {"version": "1"})
    finally:
        first.stop()
    second = RunningBroker(config)
    try:
        with RelayClient(socket=second.socket) as client:
            history = client.history()
            status = client.status()
    finally:
        second.stop()
    assert history[0]["id"] == published.message_id
    assert status["messages"] == 1


def test_oversized_payload_is_not_stored(workspace: Path) -> None:
    text = (
        "version: 1\n"
        "messaging:\n"
        "  max_payload_kb: 1\n"
        "server:\n"
        f"  socket: {workspace / 'r.sock'}\n"
        "storage:\n"
        f"  path: {workspace / 'relay.sqlite3'}\n"
    )
    path = workspace / "relay.yml"
    path.write_text(text)
    from lupaxa.relay.config import load_config

    running = RunningBroker(load_config(path, user_path=None))
    try:
        with RelayClient(socket=running.socket) as client:
            with pytest.raises(ProtocolError) as caught:
                client.publish("build.completed", {"blob": "x" * 2048})
            assert caught.value.code == "MESSAGE_TOO_LARGE"
            assert client.history() == []
    finally:
        running.stop()


def test_idempotency_key_replays_and_conflicts(running: RunningBroker) -> None:
    with RelayClient(socket=running.socket) as client:
        first = client.publish(
            "build.completed",
            {"version": "1.2.0"},
            idempotency_key="build-example-1.2.0",
        )
        replay = client.publish(
            "build.completed",
            {"version": "1.2.0"},
            idempotency_key="build-example-1.2.0",
        )
        with pytest.raises(ProtocolError) as caught:
            client.publish(
                "build.completed",
                {"version": "9.9.9"},
                idempotency_key="build-example-1.2.0",
            )
        assert caught.value.code == "IDEMPOTENCY_CONFLICT"
        assert client.status()["messages"] == 1
    assert replay.message_id == first.message_id
    assert replay.sequence == first.sequence


def test_malformed_frame_does_not_kill_the_connection(running: RunningBroker) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(str(running.socket))
        sock.sendall(b"{not-json\n")
        error = _read_frame(sock)
        assert error["op"] == "error"
        assert error["code"] == "PROTOCOL_ERROR"
        sock.sendall(
            (
                json.dumps(
                    {
                        "op": "hello",
                        "request_id": "req-1",
                        "protocol": 1,
                        "client": "test",
                        "client_version": "0",
                    }
                )
                + "\n"
            ).encode()
        )
        hello = _read_frame(sock)
    assert hello["op"] == "hello_ok"
    assert hello["request_id"] == "req-1"
    assert hello["protocol"] == 1


def test_incompatible_protocol_is_rejected(running: RunningBroker) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(str(running.socket))
        sock.sendall(
            (
                json.dumps(
                    {
                        "op": "hello",
                        "request_id": "req-old",
                        "protocol": 0,
                        "client": "test",
                        "client_version": "0",
                    }
                )
                + "\n"
            ).encode()
        )
        frame = _read_frame(sock)
    assert frame["code"] == "INCOMPATIBLE_PROTOCOL"


def test_publish_before_hello_is_rejected(running: RunningBroker) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(str(running.socket))
        sock.sendall(
            (
                json.dumps(
                    {
                        "op": "publish",
                        "request_id": "req-2",
                        "topic": "build.completed",
                        "data": {"version": "1"},
                    }
                )
                + "\n"
            ).encode()
        )
        frame = _read_frame(sock)
    assert frame["code"] == "HELLO_REQUIRED"


def test_a_second_broker_cannot_open_the_same_database(running: RunningBroker) -> None:
    with pytest.raises(BrokerBusy):
        RunningBroker(running.config)


def test_version_1_database_is_migrated(workspace: Path) -> None:
    import sqlite3

    from lupaxa.relay.config import load_config

    database = workspace / "relay.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                destination TEXT NOT NULL,
                sequence INTEGER NOT NULL UNIQUE,
                timestamp TEXT NOT NULL,
                headers TEXT NOT NULL,
                content_type TEXT NOT NULL,
                data TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO messages (
                id, kind, destination, sequence, timestamp, headers, content_type, data
            ) VALUES (
                'm1', 'event', 'build.completed', 1, '2026-01-01T00:00:00Z',
                '{}', 'application/json', '{"version": "1"}'
            )
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (1)")
    text = f"version: 1\nserver:\n  socket: {workspace / 'r.sock'}\nstorage:\n  path: {database}\n"
    running = RunningBroker(load_config(write_config(workspace, text), user_path=None))
    try:
        with RelayClient(socket=running.socket) as client:
            history = client.history()
    finally:
        running.stop()
    with sqlite3.connect(database) as conn:
        version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert version is not None
    assert version[0] == 5
    assert "subscriptions" in tables
    assert "queue_jobs" in tables
    assert "runtime_queues" in tables
    assert "counters" in tables
    assert "deliveries" in tables
    assert history[0]["id"] == "m1"


def test_newer_schema_is_refused(workspace: Path) -> None:
    import sqlite3

    database = workspace / "relay.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (99)")
    text = f"version: 1\nserver:\n  socket: {workspace / 'r.sock'}\nstorage:\n  path: {database}\n"
    path = workspace / "relay.yml"
    path.write_text(text)
    from lupaxa.relay.config import load_config

    with pytest.raises(UsageError, match="schema"):
        RunningBroker(load_config(path, user_path=None))


def test_unix_socket_is_private(running: RunningBroker) -> None:
    mode = running.socket.stat().st_mode & 0o777
    assert mode == stat.S_IRUSR | stat.S_IWUSR


@pytest.mark.asyncio
async def test_async_client_publishes(running: RunningBroker) -> None:
    async with AsyncRelayClient(socket=running.socket) as client:
        result = await client.publish("build.completed", {"version": "1"})
    assert result.sequence == 1
    assert result.message_id


def _read_frame(sock: socket.socket) -> dict[str, object]:
    data = b""
    while b"\n" not in data:
        chunk = sock.recv(4096)
        assert chunk
        data += chunk
    return json.loads(data.split(b"\n", 1)[0])
