"""Synchronous and asynchronous clients for a local Relay broker."""

from __future__ import annotations

import asyncio
import json
import socket as socketlib
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

from lupaxa.relay.exceptions import ConnectionFailed, ProtocolError
from lupaxa.relay.version import __version__

PROTOCOL = 1
_UNSOLICITED = {"delivery", "gap"}


class PublishResult:
    """The identifier assigned when a publish commits."""

    def __init__(self, message_id: str, sequence: int) -> None:
        self.message_id = message_id
        self.sequence = sequence


class GapNotice:
    """Events a durable subscriber will not see because retention removed them."""

    def __init__(self, from_sequence: int, to_sequence: int) -> None:
        self.from_sequence = from_sequence
        self.to_sequence = to_sequence


class Delivery:
    """One event pushed to a subscriber. Durable deliveries wait for ack."""

    def __init__(self, client: RelayClient, frame: dict[str, object], *, durable: bool) -> None:
        self.delivery_id = _required_str(frame, "delivery_id")
        self.message_id = _required_str(frame, "message_id")
        self.sequence = _required_int(frame, "sequence")
        self.destination = _required_str(frame, "destination")
        self.data = frame.get("data")
        self.headers = frame.get("headers") if isinstance(frame.get("headers"), dict) else {}
        self.content_type = str(frame.get("content_type", "application/json"))
        self._client = client
        self._durable = durable

    def ack(self) -> None:
        """Confirm a durable delivery. Ephemeral deliveries do not need it."""
        if self._durable:
            self._client.acknowledge(self.delivery_id)


class JobDelivery:
    """One leased job. Ack, nack, reject, and extend all name this lease."""

    def __init__(self, client: RelayClient, frame: dict[str, object]) -> None:
        self.delivery_id = _required_str(frame, "delivery_id")
        self.message_id = _required_str(frame, "message_id")
        self.queue = _required_str(frame, "queue")
        self.lease_token = _required_str(frame, "lease_token")
        self.attempt = _required_int(frame, "attempt")
        self.data = frame.get("data")
        self.headers = frame.get("headers") if isinstance(frame.get("headers"), dict) else {}
        self.content_type = str(frame.get("content_type", "application/json"))
        original = frame.get("original_queue")
        self.original_queue = original if isinstance(original, str) else self.queue
        reason = frame.get("failure_reason")
        self.failure_reason = reason if isinstance(reason, str) else None
        self._client = client

    def ack(self) -> None:
        self._client.settle_job("ack", self.delivery_id, self.lease_token)

    def nack(self, *, requeue: bool = True) -> None:
        self._client.settle_job("nack", self.delivery_id, self.lease_token, requeue=requeue)

    def reject(self, reason: str) -> None:
        self._client.settle_job("reject", self.delivery_id, self.lease_token, reason=reason)

    def extend(self, seconds: int) -> None:
        self._client.settle_job("extend", self.delivery_id, self.lease_token, seconds=seconds)


class AsyncJob:
    """A job read by the async client. The milestone test only needs its id."""

    def __init__(self, frame: dict[str, object]) -> None:
        self.message_id = _required_str(frame, "message_id")


class AsyncDelivery:
    """One event pushed to an async subscriber."""

    def __init__(
        self,
        client: AsyncRelayClient,
        frame: dict[str, object],
        *,
        durable: bool,
    ) -> None:
        self.delivery_id = _required_str(frame, "delivery_id")
        self.message_id = _required_str(frame, "message_id")
        self.sequence = _required_int(frame, "sequence")
        self.destination = _required_str(frame, "destination")
        self.data = frame.get("data")
        self.headers = frame.get("headers") if isinstance(frame.get("headers"), dict) else {}
        self.content_type = str(frame.get("content_type", "application/json"))
        self._client = client
        self._durable = durable

    async def ack(self) -> None:
        """Confirm a durable delivery. Ephemeral deliveries do not need it."""
        if self._durable:
            await self._client.acknowledge(self.delivery_id)


class RelayClient:
    """A blocking client. One connection, no hidden retries."""

    def __init__(
        self,
        socket_path: Path | None = None,
        *,
        socket: Path | None = None,
        host: str | None = None,
        port: int | None = None,
        timeout: float = 5.0,
        token: str | None = None,
    ) -> None:
        chosen = socket if socket is not None else socket_path
        if chosen is None and (host is None or port is None):
            raise ConnectionFailed("Pass a Unix socket or a TCP host and port.")
        if chosen is not None and host is not None:
            raise ConnectionFailed("Pass either a Unix socket or a TCP address.")
        self.socket = chosen
        self.host = host
        self.port = port
        self.timeout = timeout
        self.token = token
        self._sock: socketlib.socket | None = None
        self._buffer = b""
        self._pending: list[dict[str, object]] = []
        self._count = 0

    def __enter__(self) -> RelayClient:
        self._sock = self._open()
        self._roundtrip(self._hello())
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def publish(
        self,
        topic: str,
        data: object,
        *,
        headers: dict[str, object] | None = None,
        content_type: str = "application/json",
        idempotency_key: str | None = None,
    ) -> PublishResult:
        body: dict[str, object] = {
            "op": "publish",
            "request_id": self._next_id(),
            "topic": topic,
            "data": data,
            "headers": headers or {},
            "content_type": content_type,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        frame = self._roundtrip(body)
        return PublishResult(str(frame["message_id"]), _required_int(frame, "sequence"))

    def history(self, topic: str | None = None, limit: int = 50) -> list[dict[str, object]]:
        body: dict[str, object] = {"op": "history", "request_id": self._next_id(), "limit": limit}
        if topic is not None:
            body["topic"] = topic
        frame = self._roundtrip(body)
        messages = frame["messages"]
        if not isinstance(messages, list):
            raise ProtocolError("PROTOCOL_ERROR", "The history response has no messages.")
        return [item for item in messages if isinstance(item, dict)]

    def status(self) -> dict[str, object]:
        return self._roundtrip({"op": "status", "request_id": self._next_id()})

    def inspect(self, queue: str, limit: int = 50) -> list[dict[str, object]]:
        frame = self._roundtrip(
            {"op": "inspect", "request_id": self._next_id(), "queue": queue, "limit": limit}
        )
        jobs = frame["jobs"]
        if not isinstance(jobs, list):
            raise ProtocolError("PROTOCOL_ERROR", "The inspect response has no jobs.")
        return [item for item in jobs if isinstance(item, dict)]

    def retry_job(self, message_id: str) -> dict[str, object]:
        frame = self._roundtrip(
            {"op": "retry", "request_id": self._next_id(), "message_id": message_id}
        )
        job = frame["job"]
        if not isinstance(job, dict):
            raise ProtocolError("PROTOCOL_ERROR", "The retry response has no job.")
        return job

    def purge(self, queue: str, *, dry_run: bool = False) -> int:
        body: dict[str, object] = {
            "op": "purge",
            "request_id": self._next_id(),
            "queue": queue,
        }
        if dry_run:
            body["dry_run"] = True
        frame = self._roundtrip(body)
        return _required_int(frame, "deleted")

    def create_queue(
        self,
        queue: str,
        *,
        dead_letter_queue: str | None = None,
        max_attempts: int | None = 5,
    ) -> None:
        body: dict[str, object] = {
            "op": "queue_create",
            "request_id": self._next_id(),
            "queue": queue,
            "max_attempts": max_attempts,
        }
        if dead_letter_queue is not None:
            body["dead_letter_queue"] = dead_letter_queue
        self._roundtrip(body)

    def create_subscription(self, name: str, pattern: str) -> dict[str, object]:
        frame = self._roundtrip(
            {
                "op": "subscription_create",
                "request_id": self._next_id(),
                "name": name,
                "pattern": pattern,
            }
        )
        record = frame["subscription"]
        if not isinstance(record, dict):
            raise ProtocolError("PROTOCOL_ERROR", "The subscription response was empty.")
        return record

    def reset_subscription(self, name: str, start: str = "earliest") -> dict[str, object]:
        frame = self._roundtrip(
            {
                "op": "subscription_reset",
                "request_id": self._next_id(),
                "name": name,
                "start": start,
            }
        )
        record = frame["subscription"]
        if not isinstance(record, dict):
            raise ProtocolError("PROTOCOL_ERROR", "The subscription response was empty.")
        return record

    def delete_subscription(self, name: str) -> None:
        self._roundtrip({"op": "subscription_delete", "request_id": self._next_id(), "name": name})

    def message(self, message_id: str) -> dict[str, object]:
        return self._roundtrip(
            {"op": "message", "request_id": self._next_id(), "message_id": message_id}
        )

    def backup(self, path: Path) -> None:
        self._roundtrip(
            {"op": "backup", "request_id": self._next_id(), "path": str(path.resolve())}
        )

    def enqueue(
        self,
        queue: str,
        data: object,
        *,
        idempotency_key: str | None = None,
        content_type: str = "application/json",
        headers: dict[str, object] | None = None,
    ) -> str:
        body: dict[str, object] = {
            "op": "enqueue",
            "request_id": self._next_id(),
            "queue": queue,
            "data": data,
            "headers": headers or {},
            "content_type": content_type,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        frame = self._roundtrip(body)
        return str(frame["message_id"])

    def consume(
        self,
        queue: str,
        *,
        prefetch: int | None = None,
        on_ready: Callable[[], None] | None = None,
    ) -> Iterator[JobDelivery]:
        """Yield jobs from one queue. Each job stays leased until it is settled."""
        body: dict[str, object] = {
            "op": "consume",
            "request_id": self._next_id(),
            "queue": queue,
        }
        if prefetch is not None:
            body["prefetch"] = prefetch
        self._roundtrip(body)
        if on_ready is not None:
            on_ready()
        while True:
            nxt = self._read_frame()
            if nxt.get("op") != "delivery" or nxt.get("kind") != "job":
                code = str(nxt.get("code", "PROTOCOL_ERROR"))
                message = str(nxt.get("message", "Relay returned an unexpected frame."))
                raise ProtocolError(code, message)
            yield JobDelivery(self, nxt)

    def settle_job(
        self,
        op: str,
        delivery_id: str,
        lease_token: str,
        *,
        requeue: bool = True,
        reason: str | None = None,
        seconds: int | None = None,
    ) -> None:
        body: dict[str, object] = {
            "op": op,
            "request_id": self._next_id(),
            "delivery_id": delivery_id,
            "lease_token": lease_token,
        }
        if op == "nack":
            body["requeue"] = requeue
        if reason is not None:
            body["reason"] = reason
        if seconds is not None:
            body["seconds"] = seconds
        self._roundtrip(body)

    def subscribe(
        self,
        pattern: str,
        *,
        durable: str | None = None,
        start: str = "latest",
        sequence: int | None = None,
        on_ready: Callable[[], None] | None = None,
        gaps: list[GapNotice] | None = None,
    ) -> Iterator[GapNotice | Delivery]:
        """Yield events matching pattern. Durable subscribers must ack each one."""
        body: dict[str, object] = {
            "op": "subscribe",
            "request_id": self._next_id(),
            "pattern": pattern,
            "start": start,
        }
        if durable is not None:
            body["durable"] = durable
        if sequence is not None:
            body["sequence"] = sequence
        frame = self._roundtrip(body)
        notice = _gap_from_subscribe(frame)
        if notice is not None and gaps is not None:
            gaps.append(notice)
        if on_ready is not None:
            on_ready()
        if notice is not None:
            yield notice
        is_durable = durable is not None
        while True:
            nxt = self._read_frame()
            op = nxt.get("op")
            if op == "gap":
                found = GapNotice(
                    _required_int(nxt, "from_sequence"),
                    _required_int(nxt, "to_sequence"),
                )
                if gaps is not None:
                    gaps.append(found)
                yield found
                continue
            if op == "delivery":
                yield Delivery(self, nxt, durable=is_durable)
                continue
            raise ProtocolError("PROTOCOL_ERROR", "Unexpected frame during subscribe.")

    def acknowledge(self, delivery_id: str) -> None:
        """Record that a durable delivery was processed."""
        self._roundtrip(
            {"op": "ack", "request_id": self._next_id(), "delivery_id": delivery_id},
        )

    def poll(self, timeout: float) -> dict[str, object] | None:
        """Return the next unread frame, or None when none arrives in time."""
        sock = self._sock
        if sock is None:
            raise ConnectionFailed("The client is not connected.")
        if self._pending:
            return self._pending.pop(0)
        if b"\n" in self._buffer:
            return self._pop_buffered_frame()
        sock.settimeout(timeout)
        try:
            while b"\n" not in self._buffer:
                try:
                    chunk = sock.recv(4096)
                except OSError:
                    return None
                if not chunk:
                    return None
                self._buffer += chunk
        finally:
            if self._sock is not None:
                self._sock.settimeout(self.timeout)
        return self._pop_buffered_frame()

    def _open(self) -> socketlib.socket:
        if self.socket is not None:
            sock = socketlib.socket(socketlib.AF_UNIX, socketlib.SOCK_STREAM)
            address: str | tuple[str, int] = str(self.socket)
            label = str(self.socket)
        else:
            sock = socketlib.socket(socketlib.AF_INET, socketlib.SOCK_STREAM)
            assert self.host is not None and self.port is not None
            address = (self.host, self.port)
            label = f"{self.host}:{self.port}"
        sock.settimeout(self.timeout)
        try:
            sock.connect(address)
        except OSError as exc:
            sock.close()
            raise ConnectionFailed(f"Could not connect to Relay at {label}.") from exc
        return sock

    def _hello(self) -> dict[str, object]:
        body: dict[str, object] = {
            "op": "hello",
            "request_id": self._next_id(),
            "protocol": PROTOCOL,
            "client": "python-sdk",
            "client_version": __version__,
        }
        if self.token is not None:
            body["token"] = self.token
        return body

    def _roundtrip(self, body: dict[str, object]) -> dict[str, object]:
        sock = self._sock
        if sock is None:
            raise ConnectionFailed("The client is not connected.")
        sock.sendall(json.dumps(body, separators=(",", ":")).encode() + b"\n")
        while True:
            frame = self._take_frame()
            op = frame.get("op")
            if op == "error":
                raise ProtocolError(
                    str(frame.get("code", "PROTOCOL_ERROR")),
                    str(frame.get("message")),
                )
            if op in _UNSOLICITED:
                self._pending.append(frame)
                continue
            return frame

    def _read_frame(self) -> dict[str, object]:
        frame = self._pending.pop(0) if self._pending else self._take_frame()
        if frame.get("op") == "error":
            raise ProtocolError(str(frame.get("code", "PROTOCOL_ERROR")), str(frame.get("message")))
        return frame

    def _take_frame(self) -> dict[str, object]:
        sock = self._sock
        if sock is None:
            raise ConnectionFailed("The client is not connected.")
        while b"\n" not in self._buffer:
            try:
                chunk = sock.recv(4096)
            except OSError as exc:
                raise ConnectionFailed("The broker did not respond.") from exc
            if not chunk:
                raise ConnectionFailed("The broker closed the connection.")
            self._buffer += chunk
        return self._pop_buffered_frame()

    def _pop_buffered_frame(self) -> dict[str, object]:
        line, _, rest = self._buffer.partition(b"\n")
        self._buffer = rest
        frame = json.loads(line)
        if not isinstance(frame, dict):
            raise ProtocolError("PROTOCOL_ERROR", "The broker response was not an object.")
        return frame

    def _next_id(self) -> str:
        self._count += 1
        return f"req-{self._count}"


class AsyncRelayClient:
    """An asyncio client with the same publish, history, and status calls."""

    def __init__(
        self,
        socket: Path | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        timeout: float = 5.0,
        token: str | None = None,
    ) -> None:
        if socket is None and (host is None or port is None):
            raise ConnectionFailed("Pass a Unix socket or a TCP host and port.")
        self.socket = socket
        self.host = host
        self.port = port
        self.timeout = timeout
        self.token = token
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._pending: list[dict[str, object]] = []
        self._count = 0

    async def __aenter__(self) -> AsyncRelayClient:
        if self.socket is not None:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self.socket)),
                self.timeout,
            )
        else:
            assert self.host is not None and self.port is not None
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                self.timeout,
            )
        await self._roundtrip(self._hello())
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._writer is not None:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None

    async def publish(
        self,
        topic: str,
        data: object,
        *,
        headers: dict[str, object] | None = None,
        content_type: str = "application/json",
        idempotency_key: str | None = None,
    ) -> PublishResult:
        body: dict[str, object] = {
            "op": "publish",
            "request_id": self._next_id(),
            "topic": topic,
            "data": data,
            "headers": headers or {},
            "content_type": content_type,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        frame = await self._roundtrip(body)
        return PublishResult(str(frame["message_id"]), _required_int(frame, "sequence"))

    async def consume(
        self,
        queue: str,
        *,
        on_ready: Callable[[], None] | None = None,
    ) -> AsyncIterator[AsyncJob]:
        """Yield jobs from one queue."""
        await self._roundtrip({"op": "consume", "request_id": self._next_id(), "queue": queue})
        if on_ready is not None:
            on_ready()
        while True:
            nxt = await self._read_frame()
            if nxt.get("op") != "delivery" or nxt.get("kind") != "job":
                raise ProtocolError("PROTOCOL_ERROR", "Unexpected frame during consume.")
            yield AsyncJob(nxt)

    async def subscribe(
        self,
        pattern: str,
        *,
        durable: str | None = None,
        start: str = "latest",
        sequence: int | None = None,
        on_ready: Callable[[], None] | None = None,
    ) -> AsyncIterator[GapNotice | AsyncDelivery]:
        """Yield events matching pattern."""
        body: dict[str, object] = {
            "op": "subscribe",
            "request_id": self._next_id(),
            "pattern": pattern,
            "start": start,
        }
        if durable is not None:
            body["durable"] = durable
        if sequence is not None:
            body["sequence"] = sequence
        frame = await self._roundtrip(body)
        notice = _gap_from_subscribe(frame)
        if on_ready is not None:
            on_ready()
        if notice is not None:
            yield notice
        is_durable = durable is not None
        while True:
            nxt = await self._read_frame()
            op = nxt.get("op")
            if op == "gap":
                yield GapNotice(
                    _required_int(nxt, "from_sequence"),
                    _required_int(nxt, "to_sequence"),
                )
                continue
            if op == "delivery":
                yield AsyncDelivery(self, nxt, durable=is_durable)
                continue
            raise ProtocolError("PROTOCOL_ERROR", "Unexpected frame during subscribe.")

    async def acknowledge(self, delivery_id: str) -> None:
        """Record that a durable delivery was processed."""
        await self._roundtrip(
            {"op": "ack", "request_id": self._next_id(), "delivery_id": delivery_id},
        )

    def _hello(self) -> dict[str, object]:
        body: dict[str, object] = {
            "op": "hello",
            "request_id": self._next_id(),
            "protocol": PROTOCOL,
            "client": "python-sdk",
            "client_version": __version__,
        }
        if self.token is not None:
            body["token"] = self.token
        return body

    async def _roundtrip(self, body: dict[str, object]) -> dict[str, object]:
        await self._send(body)
        while True:
            frame = await self._take_frame()
            op = frame.get("op")
            if op == "error":
                raise ProtocolError(
                    str(frame.get("code", "PROTOCOL_ERROR")),
                    str(frame.get("message")),
                )
            if op in _UNSOLICITED:
                self._pending.append(frame)
                continue
            return frame

    async def _read_frame(self) -> dict[str, object]:
        if self._pending:
            frame = self._pending.pop(0)
        else:
            frame = await self._take_frame()
        if frame.get("op") == "error":
            raise ProtocolError(str(frame.get("code", "PROTOCOL_ERROR")), str(frame.get("message")))
        return frame

    async def _send(self, body: dict[str, object]) -> None:
        writer = self._writer
        if writer is None:
            raise ConnectionFailed("The client is not connected.")
        writer.write(json.dumps(body, separators=(",", ":")).encode() + b"\n")
        await writer.drain()

    async def _take_frame(self) -> dict[str, object]:
        reader = self._reader
        if reader is None:
            raise ConnectionFailed("The client is not connected.")
        try:
            line = await asyncio.wait_for(reader.readline(), self.timeout)
        except TimeoutError as exc:
            raise ConnectionFailed("The broker did not respond.") from exc
        if not line:
            raise ConnectionFailed("The broker closed the connection.")
        frame = json.loads(line)
        if not isinstance(frame, dict):
            raise ProtocolError("PROTOCOL_ERROR", "The broker response was not an object.")
        return frame

    def _next_id(self) -> str:
        self._count += 1
        return f"req-{self._count}"


def _gap_from_subscribe(frame: dict[str, object]) -> GapNotice | None:
    gap = frame.get("gap")
    if gap is None:
        return None
    if not isinstance(gap, dict):
        raise ProtocolError("PROTOCOL_ERROR", "The gap is not an object.")
    return GapNotice(_required_int(gap, "from_sequence"), _required_int(gap, "to_sequence"))


def _required_str(frame: dict[str, object], key: str) -> str:
    value = frame.get(key)
    if not isinstance(value, str) or not value:
        raise ProtocolError("PROTOCOL_ERROR", f"The broker response has no {key}.")
    return value


def _required_int(frame: dict[str, object], key: str) -> int:
    value = frame[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError("PROTOCOL_ERROR", f"The broker response has no {key}.")
    return value
