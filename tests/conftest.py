"""Shared broker fixtures."""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from lupaxa.relay.broker import Broker
from lupaxa.relay.config import RelayConfig, load_config


def short_workspace() -> Path:
    """A directory whose path is short enough for a Unix socket."""
    base = "/tmp" if Path("/tmp").is_dir() else None
    return Path(tempfile.mkdtemp(prefix="relay-", dir=base))


def write_config(root: Path, body: str = "version: 1\n") -> Path:
    path = root / "relay.yml"
    path.write_text(body)
    return path


def isolated_config(root: Path) -> RelayConfig:
    """A broker configuration that stays inside root."""
    text = f"version: 1\nserver:\n  socket: {root / 'r.sock'}\nstorage:\n  path: {root / 'relay.sqlite3'}\n"
    return load_config(write_config(root, text), user_path=None)


class RunningBroker:
    """Run a broker on a background event loop so sync clients can connect."""

    def __init__(self, config: RelayConfig, *, now: Callable[[], float] | None = None) -> None:
        self.config = config
        self.loop = asyncio.new_event_loop()
        self.broker = Broker(config, now=now)
        self.ready = threading.Event()
        self.error: Exception | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError("broker did not start")
        if self.error is not None:
            raise self.error

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.broker.start())
        except Exception as exc:
            self.error = exc
            self.ready.set()
            return
        self.ready.set()
        self.loop.run_forever()

    def stop(self) -> None:
        if self.error is not None or not self.thread.is_alive():
            return
        future = asyncio.run_coroutine_threadsafe(self.broker.stop(), self.loop)
        future.result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)

    @property
    def socket(self) -> Path:
        path = self.broker.socket_path
        if path is None:
            raise RuntimeError("broker has no socket")
        return path


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Unix-socket tests do not run where the platform has no Unix sockets."""
    if os.name == "posix":
        return
    skip = pytest.mark.skip(reason="Unix sockets are not available on this platform.")
    for item in items:
        if item.get_closest_marker("tcp") is None:
            item.add_marker(skip)


@pytest.fixture
def workspace() -> Iterator[Path]:
    root = short_workspace()
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def running(workspace: Path) -> Iterator[RunningBroker]:
    broker = RunningBroker(isolated_config(workspace))
    try:
        yield broker
    finally:
        broker.stop()
