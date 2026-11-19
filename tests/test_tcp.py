"""Loopback TCP works where a Unix socket is not the transport."""

from __future__ import annotations

from pathlib import Path

import pytest

from lupaxa.relay.client import RelayClient
from lupaxa.relay.config import load_config
from tests.conftest import RunningBroker, write_config


@pytest.mark.tcp
def test_tcp_publish_and_status(workspace: Path) -> None:
    text = (
        "version: 1\n"
        "server:\n"
        "  transport: tcp\n"
        "  tcp:\n"
        "    enabled: true\n"
        "    host: 127.0.0.1\n"
        "    port: 0\n"
        "storage:\n"
        f"  path: {workspace / 'relay.sqlite3'}\n"
    )
    running = RunningBroker(load_config(write_config(workspace, text), user_path=None))
    try:
        port = running.broker.tcp_port
        assert isinstance(port, int)
        with RelayClient(host="127.0.0.1", port=port) as client:
            result = client.publish("build.completed", {"n": 1})
            status = client.status()
    finally:
        running.stop()
    assert result.sequence == 1
    assert status["messages"] == 1
