"""A queue hands each job to one worker, then retries or dead-letters it."""

from __future__ import annotations

import asyncio
import json
import queue
import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from lupaxa.relay.cli import main
from lupaxa.relay.client import AsyncRelayClient, JobDelivery, RelayClient
from lupaxa.relay.config import load_config
from lupaxa.relay.exceptions import ConnectionFailed, ProtocolError
from tests.conftest import RunningBroker, write_config


class ManualClock:
    """A clock tests can move without sleeping."""

    def __init__(self) -> None:
        self.now = 1_700_000_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_unknown_queue_is_rejected(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    with RelayClient(socket=running.socket) as client, pytest.raises(ProtocolError) as caught:
        client.enqueue("missing.queue", {"image": "a.jpg"})
    assert caught.value.code == "QUEUE_NOT_FOUND"


def test_a_worker_receives_the_enqueued_payload(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    with _worker(running.socket, "image.resize") as worker:
        worker.wait_ready()
        message_id = _enqueue(running.socket, "image.resize", {"image": "a.jpg", "width": 800})
        item = worker.take()
    assert item.message_id == message_id
    assert item.data == {"image": "a.jpg", "width": 800}
    assert item.attempt == 1
    assert item.queue == "image.resize"


def test_two_workers_do_not_share_one_job(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    with (
        _worker(running.socket, "image.resize") as first,
        _worker(running.socket, "image.resize", timeout=0.4) as second,
    ):
        first.wait_ready()
        second.wait_ready()
        _enqueue(running.socket, "image.resize", {"n": 1})
        assert first.take().message_id
        second.empty(0.6)


def test_workers_receive_different_jobs(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    with (
        _worker(running.socket, "image.resize") as first,
        _worker(running.socket, "image.resize") as second,
    ):
        first.wait_ready()
        second.wait_ready()
        _enqueue(running.socket, "image.resize", {"n": 1})
        _enqueue(running.socket, "image.resize", {"n": 2})
        left = first.take()
        right = second.take()
    assert left.message_id != right.message_id


def test_the_second_job_waits_until_the_first_is_acked(
    jobs: tuple[RunningBroker, ManualClock],
) -> None:
    running, _clock = jobs
    with _worker(running.socket, "image.resize") as worker:
        worker.wait_ready()
        _enqueue(running.socket, "image.resize", {"n": 1})
        _enqueue(running.socket, "image.resize", {"n": 2})
        first = worker.take()
        assert worker.poll(0.3) is None
        first.ack()
        worker.resume()
        second = worker.take()
    assert first.data == {"n": 1}
    assert second.data == {"n": 2}


def test_disconnect_returns_the_job_and_blocks_the_old_lease(
    jobs: tuple[RunningBroker, ManualClock],
) -> None:
    running, _clock = jobs
    with _worker(running.socket, "fast") as first:
        first.wait_ready()
        message_id = _enqueue(running.socket, "fast", {"n": 1})
        original = first.take()
        delivery_id = original.delivery_id
        token = original.lease_token
    with _worker(running.socket, "fast") as second:
        again = second.take()
        assert again.message_id == message_id
        assert again.attempt == 2
        assert again.delivery_id != delivery_id
    with RelayClient(socket=running.socket) as client, pytest.raises(ProtocolError) as caught:
        client.settle_job("ack", delivery_id, token)
    assert caught.value.code == "LEASE_EXPIRED"


def test_nack_waits_for_the_retry_delay(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, clock = jobs
    with _worker(running.socket, "image.resize") as worker:
        worker.wait_ready()
        message_id = _enqueue(running.socket, "image.resize", {"n": 1})
        item = worker.take()
        item.nack(requeue=True)
    with _worker(running.socket, "image.resize", timeout=0.4) as early:
        early.wait_ready()
        early.empty(0.6)
    clock.advance(30)
    with _worker(running.socket, "image.resize") as later:
        again = later.take()
    assert again.message_id == message_id
    assert again.attempt == 2


def test_reject_moves_the_job_to_the_dead_letter_queue(
    jobs: tuple[RunningBroker, ManualClock],
) -> None:
    running, _clock = jobs
    with _worker(running.socket, "image.resize") as worker:
        worker.wait_ready()
        message_id = _enqueue(running.socket, "image.resize", {"image": "a.jpg"})
        item = worker.take()
        item.reject("Unsupported image format")
    with _worker(running.socket, "image.resize", timeout=0.4) as original:
        original.wait_ready()
        original.empty(0.6)
    with _worker(running.socket, "image.resize.failed") as failed:
        dead = failed.take()
    assert dead.message_id == message_id
    assert dead.data == {"image": "a.jpg"}
    assert dead.original_queue == "image.resize"
    assert dead.failure_reason == "Unsupported image format"


def test_exhausted_attempts_are_dead_lettered(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    message_id = _enqueue(running.socket, "once", {"n": 1})
    with _worker(running.socket, "once") as worker:
        worker.wait_ready()
        item = worker.take()
        assert item.message_id == message_id
        item.nack(requeue=True)
    with _worker(running.socket, "once", timeout=0.4) as again:
        again.wait_ready()
        again.empty(0.6)
    with _worker(running.socket, "image.resize.failed") as failed:
        assert failed.take().message_id == message_id


def test_expired_lease_cannot_ack_or_extend(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, clock = jobs
    with _worker(running.socket, "fast") as worker:
        worker.wait_ready()
        message_id = _enqueue(running.socket, "fast", {"n": 1})
        item = worker.take()
        clock.advance(11)
        with pytest.raises(ProtocolError) as expired:
            item.extend(10)
        assert expired.value.code == "LEASE_EXPIRED"
        with pytest.raises(ProtocolError) as acked:
            item.ack()
        assert acked.value.code == "LEASE_EXPIRED"
    with _worker(running.socket, "fast") as later:
        again = later.take()
        assert again.message_id == message_id
        assert again.attempt == 2
        again.ack()


def test_extend_keeps_the_lease_past_the_original_deadline(
    jobs: tuple[RunningBroker, ManualClock],
) -> None:
    running, clock = jobs
    with _worker(running.socket, "image.resize") as worker:
        worker.wait_ready()
        _enqueue(running.socket, "image.resize", {"n": 1})
        item = worker.take()
        clock.advance(8)
        item.extend(10)
        clock.advance(8)
        item.ack()
    with _worker(running.socket, "image.resize", timeout=0.4) as later:
        later.wait_ready()
        later.empty(0.6)


def test_pending_job_survives_a_restart(workspace: Path) -> None:
    clock = ManualClock()
    config = _config(workspace)
    first = RunningBroker(config, now=clock)
    try:
        message_id = _enqueue(first.socket, "image.resize", {"n": 1})
    finally:
        first.stop()
    second = RunningBroker(config, now=clock)
    try:
        with _worker(second.socket, "image.resize") as worker:
            assert worker.take().message_id == message_id
    finally:
        second.stop()


def test_unexpired_lease_survives_a_restart(workspace: Path) -> None:
    clock = ManualClock()
    config = _config(workspace)
    first = RunningBroker(config, now=clock)
    with _worker(first.socket, "fast") as worker:
        worker.wait_ready()
        message_id = _enqueue(first.socket, "fast", {"n": 1})
        worker.take()
        first.stop()
    second = RunningBroker(config, now=clock)
    try:
        with _worker(second.socket, "fast", timeout=0.4) as early:
            early.wait_ready()
            early.empty(0.6)
        clock.advance(11)
    finally:
        second.stop()
    third = RunningBroker(config, now=clock)
    try:
        with _worker(third.socket, "fast") as later:
            again = later.take()
            assert again.message_id == message_id
            assert again.attempt == 2
    finally:
        third.stop()


def test_enqueue_idempotency_replays_the_same_job(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    with RelayClient(socket=running.socket) as client:
        first = client.enqueue("image.resize", {"n": 1}, idempotency_key="job-1")
        second = client.enqueue("image.resize", {"n": 1}, idempotency_key="job-1")
        with pytest.raises(ProtocolError) as caught:
            client.enqueue("image.resize", {"n": 2}, idempotency_key="job-1")
    assert first == second
    assert caught.value.code == "IDEMPOTENCY_CONFLICT"
    with _worker(running.socket, "image.resize") as worker:
        worker.wait_ready()
        assert worker.take().message_id == first
        assert worker.poll(0.3) is None


def test_completed_retention_leaves_pending_jobs(
    workspace: Path,
) -> None:
    text = _body(workspace, completed_age=0)
    running = RunningBroker(load_config(write_config(workspace, text), user_path=None))
    try:
        done = _enqueue(running.socket, "image.resize", {"n": 1})
        pending = _enqueue(running.socket, "image.resize", {"n": 2})
        with _worker(running.socket, "image.resize") as worker:
            item = worker.take()
            assert item.message_id == done
            item.ack()
        database = workspace / "relay.sqlite3"
        with sqlite3.connect(database) as conn:
            rows = {row[0] for row in conn.execute("SELECT id FROM queue_jobs")}
    finally:
        running.stop()
    assert pending in rows
    assert done not in rows


def test_enqueue_command_prints_the_job_id(
    jobs: tuple[RunningBroker, ManualClock],
    capsys: pytest.CaptureFixture[str],
) -> None:
    running, _clock = jobs
    code = main(
        [
            "--socket",
            str(running.socket),
            "enqueue",
            "image.resize",
            "--json",
            '{"image":"a.jpg"}',
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["message_id"]


@pytest.mark.asyncio
async def test_async_worker_receives_one_job(jobs: tuple[RunningBroker, ManualClock]) -> None:
    running, _clock = jobs
    ready = asyncio.Event()

    async def _collect() -> str:
        async with AsyncRelayClient(socket=running.socket, timeout=2) as client:
            async for item in client.consume("image.resize", on_ready=ready.set):
                return item.message_id
        raise AssertionError("consumer ended")

    task = asyncio.create_task(_collect())
    await asyncio.wait_for(ready.wait(), 2)
    message_id = await asyncio.to_thread(_enqueue, running.socket, "image.resize", {"n": 1})
    assert await asyncio.wait_for(task, 2) == message_id


class _Worker:
    """One queue consumer. It pauses after each job so the test can settle it."""

    def __init__(self, socket: Path, name: str, *, timeout: float) -> None:
        self.ready = threading.Event()
        self.client = RelayClient(socket=socket, timeout=timeout)
        self.client.__enter__()
        self._queue: queue.Queue[JobDelivery] = queue.Queue()
        self._resume = threading.Event()
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._closed = False
        self.error: Exception | None = None
        self._name = name
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def __enter__(self) -> _Worker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def wait_ready(self) -> None:
        if not self.ready.wait(2):
            raise AssertionError("consumer was not accepted")
        if self.error is not None:
            raise self.error

    def take(self, timeout: float = 2) -> JobDelivery:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty as exc:
            if self.error is not None:
                raise self.error from exc
            raise AssertionError("timed out waiting for a job") from exc

    def empty(self, timeout: float) -> None:
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty as exc:
            if self.error is not None and not isinstance(self.error, ConnectionFailed):
                raise self.error from exc
            return
        raise AssertionError(item.message_id)

    def poll(self, timeout: float) -> dict[str, object] | None:
        if not self._paused.wait(2):
            raise AssertionError("consumer is not paused")
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

    def _run(self) -> None:
        try:
            iterator = self.client.consume(self._name, on_ready=self.ready.set)
            for item in iterator:
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


def _worker(socket: Path, name: str, *, timeout: float = 2.0) -> _Worker:
    return _Worker(socket, name, timeout=timeout)


def _enqueue(socket: Path, name: str, data: dict[str, object]) -> str:
    with RelayClient(socket=socket) as client:
        return client.enqueue(name, data)


def _body(root: Path, *, completed_age: int | None = None) -> str:
    retention = ""
    if completed_age is not None:
        retention = f"  retention:\n    completed_jobs:\n      max_age_days: {completed_age}\n"
    return (
        "version: 1\n"
        "server:\n"
        f"  socket: {root / 'r.sock'}\n"
        "storage:\n"
        f"  path: {root / 'relay.sqlite3'}\n"
        f"{retention}"
        "queues:\n"
        "  - name: image.resize\n"
        "    max_attempts: 5\n"
        "    visibility_timeout_seconds: 10\n"
        "    retry:\n"
        "      strategy: fixed\n"
        "      initial_delay_seconds: 30\n"
        "    dead_letter_queue: image.resize.failed\n"
        "  - name: image.resize.failed\n"
        "    max_attempts: 2\n"
        "  - name: fast\n"
        "    max_attempts: 5\n"
        "    visibility_timeout_seconds: 10\n"
        "    retry:\n"
        "      strategy: immediate\n"
        "  - name: once\n"
        "    max_attempts: 1\n"
        "    visibility_timeout_seconds: 10\n"
        "    dead_letter_queue: image.resize.failed\n"
    )


def _config(root: Path):
    return load_config(write_config(root, _body(root)), user_path=None)


@pytest.fixture
def jobs(workspace: Path) -> Iterator[tuple[RunningBroker, ManualClock]]:
    clock = ManualClock()
    running = RunningBroker(_config(workspace), now=clock)
    try:
        yield running, clock
    finally:
        running.stop()
