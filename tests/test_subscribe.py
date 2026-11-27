"""Pub/sub delivers each event to every matching subscriber."""

from __future__ import annotations

import asyncio
import queue
import threading
from pathlib import Path

import pytest

from lupaxa.relay.client import AsyncDelivery, AsyncRelayClient, Delivery, GapNotice, RelayClient
from lupaxa.relay.config import load_config
from lupaxa.relay.exceptions import ConnectionFailed, ProtocolError
from tests.conftest import RunningBroker, isolated_config, write_config


def test_one_event_reaches_each_matching_subscriber(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*") as build, _listen(running.socket, "build.completed") as exact, _listen(running.socket, "image.*", timeout=0.4) as other:
        build.wait_ready()
        exact.wait_ready()
        other.wait_ready()
        _publish(running.socket, "build.completed", {"version": "1"})
        assert build.take().sequence == 1
        assert exact.take().sequence == 1
        other.empty(1)


def test_ephemeral_subscribers_miss_events_published_while_disconnected(running: RunningBroker) -> None:
    _publish(running.socket, "build.completed", {"version": "1"})
    with _listen(running.socket, "build.*", timeout=0.4) as heard:
        heard.wait_ready()
        heard.empty(1)


def test_durable_latest_skips_existing_events(running: RunningBroker) -> None:
    _publish(running.socket, "build.completed", {"version": "old"})
    with _listen(running.socket, "build.*", durable="auditor", start="latest") as heard:
        heard.wait_ready()
        heard.empty(0.3)
        _publish(running.socket, "build.completed", {"version": "new"})
        item = heard.take()
        assert item.sequence == 2
        assert item.message_id


def test_durable_earliest_replays_retained_events(running: RunningBroker) -> None:
    _publish(running.socket, "build.completed", {"version": "1"})
    _publish(running.socket, "build.started", {"version": "1"})
    with _listen(running.socket, "build.*", durable="auditor", start="earliest") as heard:
        first = heard.take()
        assert first.sequence == 1
        first.ack()
        heard.resume()
        second = heard.take()
        assert second.sequence == 2


def test_durable_sequence_starts_at_the_requested_event(running: RunningBroker) -> None:
    _publish(running.socket, "build.completed", {"n": 1})
    _publish(running.socket, "build.completed", {"n": 2})
    _publish(running.socket, "build.completed", {"n": 3})
    with _listen(running.socket, "build.*", durable="auditor", start="sequence", sequence=2) as heard:
        first = heard.take()
        assert first.sequence == 2
        first.ack()
        heard.resume()
        assert heard.take().sequence == 3


def test_durable_subscriber_receives_events_published_while_disconnected(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*", durable="auditor", start="latest") as first:
        first.wait_ready()
    _publish(running.socket, "build.completed", {"version": "1"})
    with _listen(running.socket, "build.*", durable="auditor") as second:
        item = second.take()
        assert item.sequence == 1
        item.ack()


def test_unacknowledged_event_is_redelivered(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*", durable="auditor", start="latest") as first:
        first.wait_ready()
        _publish(running.socket, "build.completed", {"version": "1"})
        original = first.take()
        message_id = original.message_id
        sequence = original.sequence
    with _listen(running.socket, "build.*", durable="auditor", start="earliest") as second:
        again = second.take()
        assert again.message_id == message_id
        assert again.sequence == sequence


def test_acknowledged_event_is_not_redelivered(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*", durable="auditor", start="latest") as heard:
        heard.wait_ready()
        _publish(running.socket, "build.completed", {"version": "1"})
        item = heard.take()
        item.ack()
        item.ack()
    with _listen(running.socket, "build.*", durable="auditor", start="earliest", timeout=0.4) as again:
        again.wait_ready()
        again.empty(1)


def test_only_one_durable_consumer_can_attach(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*", durable="auditor") as owner, pytest.raises(ProtocolError) as caught, RelayClient(socket=running.socket, timeout=2) as client:
        owner.wait_ready()
        next(iter(client.subscribe("build.*", durable="auditor")))
    assert caught.value.code == "SUBSCRIPTION_CONFLICT"


def test_durable_pattern_cannot_change(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*", durable="auditor") as first:
        first.wait_ready()
    with pytest.raises(ProtocolError) as caught, RelayClient(socket=running.socket, timeout=2) as client:
        next(iter(client.subscribe("image.*", durable="auditor")))
    assert caught.value.code == "SUBSCRIPTION_CONFLICT"


def test_durable_delivery_stays_in_order_and_waits_for_ack(running: RunningBroker) -> None:
    with _listen(running.socket, "build.*", durable="auditor", start="latest") as heard:
        heard.wait_ready()
        _publish(running.socket, "build.completed", {"n": 1})
        _publish(running.socket, "build.completed", {"n": 2})
        first = heard.take()
        assert first.sequence == 1
        assert heard.poll(0.3) is None
        first.ack()
        heard.resume()
        assert heard.take().sequence == 2


def test_retention_reports_a_gap_without_acking_the_expired_event(workspace: Path) -> None:
    running = _limited_broker(workspace)
    try:
        with _listen(running.socket, "build.*", durable="auditor", start="earliest") as created:
            created.wait_ready()
        _publish(running.socket, "build.completed", {"n": 1})
        _publish(running.socket, "build.completed", {"n": 2})
        with _listen(running.socket, "build.*", durable="auditor", start="latest") as heard:
            heard.wait_ready()
            gap = heard.gap
            item = heard.take()
    finally:
        running.stop()
    assert gap is not None
    assert gap.from_sequence == 1
    assert gap.to_sequence == 1
    assert item.sequence == 2


def test_unrelated_retention_is_not_a_gap(workspace: Path) -> None:
    running = _limited_broker(workspace)
    try:
        with _listen(running.socket, "build.*", durable="auditor", start="earliest") as created:
            created.wait_ready()
        _publish(running.socket, "image.resized", {"n": 1})
        _publish(running.socket, "build.completed", {"n": 2})
        with _listen(running.socket, "build.*", durable="auditor") as heard:
            heard.wait_ready()
            assert heard.gap is None
            assert heard.take().sequence == 2
    finally:
        running.stop()


def test_unacked_event_survives_a_restart(workspace: Path) -> None:
    config = isolated_config(workspace)
    first = RunningBroker(config)
    heard = _listen(first.socket, "build.*", durable="auditor", start="latest")
    try:
        heard.wait_ready()
        _publish(first.socket, "build.completed", {"version": "1"})
        original = heard.take()
        delivery_id = original.delivery_id
        message_id = original.message_id
    finally:
        heard.close()
        first.stop()
    second = RunningBroker(config)
    again = _listen(second.socket, "build.*", durable="auditor", start="earliest")
    try:
        item = again.take()
        assert item.delivery_id == delivery_id
        assert item.message_id == message_id
    finally:
        again.close()
        second.stop()


@pytest.mark.asyncio
async def test_async_subscriber_receives_one_event(running: RunningBroker) -> None:
    ready = asyncio.Event()

    async def _collect() -> int:
        async with AsyncRelayClient(socket=running.socket, timeout=2) as client:
            async for item in client.subscribe("build.*", on_ready=ready.set):
                if isinstance(item, AsyncDelivery):
                    return item.sequence
        raise AssertionError("subscription ended")

    task = asyncio.create_task(_collect())
    await asyncio.wait_for(ready.wait(), 2)
    await asyncio.to_thread(_publish, running.socket, "build.completed", {"version": "1"})
    assert await asyncio.wait_for(task, 2) == 1


class _Live:
    """One subscriber thread. It pauses after each delivery so tests can ack."""

    def __init__(self, socket: Path, pattern: str, *, durable: str | None, start: str, sequence: int | None, timeout: float) -> None:
        self.ready = threading.Event()
        self.gaps: list[GapNotice] = []
        self.client = RelayClient(socket=socket, timeout=timeout)
        self.client.__enter__()
        self._queue: queue.Queue[Delivery] = queue.Queue()
        self._resume = threading.Event()
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._closed = False
        self.error: Exception | None = None
        self._pattern = pattern
        self._durable = durable
        self._start = start
        self._sequence = sequence
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def __enter__(self) -> _Live:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def wait_ready(self) -> None:
        if not self.ready.wait(2):
            raise AssertionError("subscription was not accepted")
        if self.error is not None:
            raise self.error

    def take(self, timeout: float = 2) -> Delivery:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty as exc:
            if self.error is not None:
                raise self.error from exc
            raise AssertionError("timed out waiting for a delivery") from exc

    def empty(self, timeout: float) -> None:
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty as exc:
            if self.error is not None and not isinstance(self.error, ConnectionFailed):
                raise self.error from exc
            return
        raise AssertionError(item.sequence)

    def poll(self, timeout: float) -> dict[str, object] | None:
        if not self._paused.wait(2):
            raise AssertionError("subscription is not paused")
        return self.client.poll(timeout)

    def resume(self) -> None:
        self._paused.clear()
        self._resume.set()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        self._resume.set()
        self.client.close()
        self._thread.join(2)

    @property
    def gap(self) -> GapNotice | None:
        return self.gaps[0] if self.gaps else None

    def _run(self) -> None:
        try:
            iterator = self.client.subscribe(self._pattern, durable=self._durable, start=self._start, sequence=self._sequence, on_ready=self.ready.set, gaps=self.gaps)
            for item in iterator:
                if isinstance(item, GapNotice):
                    continue
                self._queue.put(item)
                self._paused.set()
                self._resume.wait()
                self._resume.clear()
                if self._stop.is_set():
                    break
        except Exception as exc:
            self.error = exc
            self.ready.set()
        finally:
            self.client.close()


def _listen(socket: Path, pattern: str, *, durable: str | None = None, start: str = "latest", sequence: int | None = None, timeout: float = 2.0) -> _Live:
    return _Live(socket, pattern, durable=durable, start=start, sequence=sequence, timeout=timeout)


def _publish(socket: Path, topic: str, data: dict[str, object]) -> None:
    with RelayClient(socket=socket) as client:
        client.publish(topic, data)


def _limited_broker(workspace: Path) -> RunningBroker:
    text = f"version: 1\nserver:\n  socket: {workspace / 'r.sock'}\nstorage:\n  path: {workspace / 'relay.sqlite3'}\n  retention:\n    events:\n      max_messages: 1\n"
    return RunningBroker(load_config(write_config(workspace, text), user_path=None))
