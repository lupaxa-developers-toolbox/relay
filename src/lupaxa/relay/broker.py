"""Async broker process: sockets, protocol, and durable publication."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hmac
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from lupaxa.relay.config import QueueSpec, RelayConfig
from lupaxa.relay.exceptions import BrokerBusy, ProtocolError, UsageError
from lupaxa.relay.ids import uuid7
from lupaxa.relay.matching import matches, validate_pattern
from lupaxa.relay.names import validate_destination
from lupaxa.relay.retry import AttemptPolicy
from lupaxa.relay.storage import JobLease, PendingDelivery, Storage, Subscription
from lupaxa.relay.version import __version__

PROTOCOL = 1
logger = logging.getLogger("lupaxa.relay")
_OVERFLOW = object()
_JSON = "application/json"
_TEXT = "text/plain"
_BINARY = "application/octet-stream"


class InstanceLock:
    """A process-wide lock beside the database. SQLite locking is not enough."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: object | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+")
        try:
            if os.name == "posix":
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                _lock_windows(handle)
        except OSError as exc:
            handle.close()
            raise BrokerBusy("Another broker is using this database.") from exc
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        if os.name == "posix":
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
        handle.close()  # type: ignore[attr-defined]
        self._handle = None


def _lock_windows(handle: object) -> None:
    import msvcrt

    handle.seek(0)  # type: ignore[attr-defined]
    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]


class _Ephemeral:
    """A live subscription that disappears with its connection."""

    def __init__(self, pattern: str, subscription_id: str) -> None:
        self.pattern = pattern
        self.subscription_id = subscription_id
        self.queued = 0


@dataclass
class _Outgoing:
    """One frame for a connection, optionally counting against an ephemeral cap."""

    frame: dict[str, object]
    ephemeral: _Ephemeral | None = None


class _Session:
    """Per-connection state. Only the pump writes to the socket."""

    def __init__(self) -> None:
        self.outbound: asyncio.Queue[_Outgoing | None] = asyncio.Queue()
        self.ephemeral: list[_Ephemeral] = []
        self.durables: list[str] = []
        self.sent: set[str] = set()
        self.send_lock = asyncio.Lock()
        self.queue: str | None = None
        self.prefetch = 1
        self.held = 0
        self.worker_id: str | None = None
        self.leases: set[tuple[str, str, str]] = set()
        self.fill_lock = asyncio.Lock()
        self.writer: asyncio.StreamWriter | None = None


class Broker:
    """One local broker. Start it, let clients connect, then stop it."""

    def __init__(self, config: RelayConfig, *, now: Callable[[], float] | None = None) -> None:
        self.config = config
        self.socket_path: Path | None = None
        self.tcp_port: int | None = None
        self._now = time.time if now is None else now
        self._lock = InstanceLock(Path(str(config.database_path()) + ".lock"))
        self._storage: Storage | None = None
        self._unix: asyncio.base_events.Server | None = None
        self._tcp: asyncio.base_events.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._sweep: asyncio.Task[None] | None = None
        self._wake: asyncio.Event | None = None
        self._connections = 0
        self._started = time.monotonic()
        self._stopping = False
        self._owned_lock = False
        self._sessions: set[_Session] = set()
        self._owners: dict[str, _Session] = {}
        self._workers: dict[str, list[_Session]] = {}

    async def start(self) -> None:
        """Open the database and listen. The database is locked until stop."""
        try:
            self._lock.acquire()
            self._owned_lock = True
        except BrokerBusy:
            raise
        storage = Storage(
            self.config.database_path(),
            durability=self.config.storage.durability,
            now=self._now,
        )
        self._storage = storage
        try:
            await storage.open()
            await self._load_runtime_queues()
            for spec in self.config.queues:
                await storage.reap_queue(spec.name, self._policy(spec))
            self._wake = asyncio.Event()
            self._sweep = asyncio.create_task(self._sweep_leases())
            await self._listen()
        except Exception:
            await self.stop()
            raise
        logger.info("relay listening")

    async def stop(self) -> None:
        """Stop accepting work and release the database."""
        self._stopping = True
        if self._wake is not None:
            self._wake.set()
        for session in list(self._sessions):
            if session.writer is not None:
                session.writer.close()
        pending = set(self._tasks)
        if self._sweep is not None:
            pending.add(self._sweep)
        if pending:
            await asyncio.wait(pending, timeout=5)
        self._sweep = None
        for server in (self._unix, self._tcp):
            if server is not None:
                server.close()
                await server.wait_closed()
        self._unix = None
        self._tcp = None
        if self._storage is not None:
            await self._storage.close()
            self._storage = None
        if self._owned_lock:
            self._lock.release()
            self._owned_lock = False
        if self.socket_path is not None and self.socket_path.exists():
            self.socket_path.unlink()
            self.socket_path = None

    async def _listen(self) -> None:
        socket_path = self.config.socket_path()
        if socket_path is not None:
            if socket_path.exists():
                socket_path.unlink()
            socket_path.parent.mkdir(parents=True, exist_ok=True)
            previous = os.umask(0o177)
            try:
                self._unix = await asyncio.start_unix_server(self._on_unix, path=str(socket_path))
            finally:
                os.umask(previous)
            os.chmod(socket_path, 0o600)
            self.socket_path = socket_path
        tcp = self.config.server.tcp
        if tcp.enabled:
            self._tcp = await asyncio.start_server(self._on_tcp, host=tcp.host, port=tcp.port)
            raw = self._tcp.sockets
            if raw:
                self.tcp_port = int(raw[0].getsockname()[1])

    def _on_unix(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._spawn(reader, writer, tcp=False)

    def _on_tcp(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._spawn(reader, writer, tcp=True)

    def _spawn(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        tcp: bool,
    ) -> None:
        task = asyncio.create_task(self._session(reader, writer, tcp=tcp))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _session(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        tcp: bool,
    ) -> None:
        session = _Session()
        session.writer = writer
        self._sessions.add(session)
        self._connections += 1
        pump = asyncio.create_task(self._pump(writer, session))
        inbound = bytearray()
        greeted = False
        try:
            if self._connections > self.config.server.max_connections:
                await self._send(
                    session,
                    _error(None, "CAPACITY", "The broker is not accepting more connections."),
                )
                return
            while not self._stopping:
                frame = await self._read_frame(reader, inbound)
                if frame is None or frame is _OVERFLOW:
                    if frame is _OVERFLOW:
                        await self._send(
                            session,
                            _error(None, "FRAME_TOO_LARGE", "The frame exceeds the size limit."),
                        )
                    return
                if not isinstance(frame, dict):
                    return
                try:
                    response, greeted = await self._dispatch(
                        frame,
                        greeted=greeted,
                        tcp=tcp,
                        session=session,
                    )
                except Exception:
                    logger.exception("request failed")
                    response = _error(None, "STORAGE", "The broker could not handle the request.")
                if response is not None:
                    await self._send(session, response)
                if response is not None and response.get("code") == "INCOMPATIBLE_PROTOCOL":
                    return
        finally:
            if not self._stopping:
                try:
                    await self._release_session(session)
                except Exception:
                    logger.exception("lease release failed")
            self._detach(session)
            self._connections -= 1
            await self._stop_pump(pump, session)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _pump(self, writer: asyncio.StreamWriter, session: _Session) -> None:
        try:
            while True:
                item = await session.outbound.get()
                if item is None:
                    return
                await _write(writer, item.frame)
                ephemeral = item.ephemeral
                if ephemeral is not None and ephemeral.queued > 0:
                    async with session.send_lock:
                        if ephemeral.queued > 0:
                            ephemeral.queued -= 1
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("subscriber pump stopped", exc_info=True)

    async def _stop_pump(self, pump: asyncio.Task[None], session: _Session) -> None:
        if pump.done():
            return
        session.outbound.put_nowait(None)
        try:
            await asyncio.wait_for(pump, 1)
        except TimeoutError:
            pump.cancel()
            try:
                await pump
            except asyncio.CancelledError:
                logger.debug("subscriber pump cancelled")

    def _detach(self, session: _Session) -> None:
        self._sessions.discard(session)
        for name in session.durables:
            if self._owners.get(name) is session:
                del self._owners[name]
        session.durables.clear()
        session.ephemeral.clear()

    async def _send(self, session: _Session, frame: dict[str, object]) -> None:
        async with session.send_lock:
            await session.outbound.put(_Outgoing(frame))

    async def _read_frame(
        self,
        reader: asyncio.StreamReader,
        inbound: bytearray,
    ) -> dict[str, object] | None | object:
        limit = self.config.server.max_frame_kb * 1024
        timeout = self.config.server.idle_timeout_seconds
        while b"\n" not in inbound:
            if len(inbound) > limit:
                inbound.clear()
                return _OVERFLOW
            try:
                chunk = await asyncio.wait_for(reader.read(4096), timeout)
            except TimeoutError:
                return None
            if not chunk:
                return None
            inbound.extend(chunk)
            if len(inbound) > limit and b"\n" not in inbound:
                inbound.clear()
                return _OVERFLOW
        line, _, rest = bytes(inbound).partition(b"\n")
        inbound.clear()
        inbound.extend(rest)
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            return _error(None, "PROTOCOL_ERROR", "The frame is not JSON.")
        if not isinstance(parsed, dict) or not isinstance(parsed.get("op"), str):
            return {
                "op": "error",
                "request_id": None,
                "code": "PROTOCOL_ERROR",
                "message": "The frame must be an object with an op.",
            }
        return parsed

    async def _dispatch(
        self,
        frame: dict[str, object],
        *,
        greeted: bool,
        tcp: bool,
        session: _Session,
    ) -> tuple[dict[str, object] | None, bool]:
        if frame.get("op") == "error":
            return frame, greeted
        op = str(frame.get("op"))
        request_id = frame.get("request_id")
        request = request_id if isinstance(request_id, str) else None
        if op == "hello":
            accepted = _hello_ok(frame, self.config, tcp=tcp)
            return self._hello(frame, request, tcp=tcp), accepted or greeted
        if not greeted:
            return (
                _error(request, "HELLO_REQUIRED", "Send hello before any other operation."),
                False,
            )
        try:
            if op == "ping":
                return {"op": "pong", "request_id": request}, True
            if op == "publish":
                await self._publish(frame, request, session)
                return None, True
            if op == "enqueue":
                await self._enqueue(frame, request, session)
                return None, True
            if op == "consume":
                await self._consume(frame, request, session)
                return None, True
            if op == "subscribe":
                await self._subscribe(frame, request, session)
                return None, True
            if op == "ack":
                if "lease_token" in frame:
                    await self._ack_job(frame, request, session)
                else:
                    await self._ack(frame, request, session)
                return None, True
            if op == "nack":
                await self._nack(frame, request, session)
                return None, True
            if op == "reject":
                await self._reject(frame, request, session)
                return None, True
            if op == "extend":
                await self._extend(frame, request, session)
                return None, True
            if op == "history":
                return await self._history(frame, request), True
            if op == "status":
                return await self._status(request), True
            if op == "inspect":
                return await self._inspect(frame, request), True
            if op == "retry":
                return await self._retry_admin(frame, request), True
            if op == "purge":
                return await self._purge(frame, request), True
            if op == "queue_create":
                return await self._queue_create(frame, request), True
            if op == "subscription_create":
                return await self._subscription_admin(frame, request, action="create"), True
            if op == "subscription_delete":
                return await self._subscription_admin(frame, request, action="delete"), True
            if op == "subscription_reset":
                return await self._subscription_admin(frame, request, action="reset"), True
            if op == "message":
                return await self._message(frame, request), True
            if op == "backup":
                return await self._backup(frame, request), True
            return _error(request, "UNSUPPORTED", f"Unsupported operation: {op}"), True
        except ProtocolError as exc:
            return _error(request, exc.code, str(exc)), True
        except UsageError as exc:
            return _error(request, "INVALID_MESSAGE", str(exc)), True

    def _hello(
        self,
        frame: dict[str, object],
        request: str | None,
        *,
        tcp: bool,
    ) -> dict[str, object]:
        if not _hello_ok(frame, self.config, tcp=tcp):
            if frame.get("protocol") != PROTOCOL:
                return _error(
                    request,
                    "INCOMPATIBLE_PROTOCOL",
                    f"This broker speaks protocol {PROTOCOL}.",
                )
            return _error(request, "UNAUTHORIZED", "The authentication token was rejected.")
        return {
            "op": "hello_ok",
            "request_id": request,
            "protocol": PROTOCOL,
            "server": "relay",
            "server_version": __version__,
        }

    async def _publish(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        storage = self._require_storage()
        topic = frame.get("topic")
        if not isinstance(topic, str):
            raise ProtocolError("INVALID_MESSAGE", "A publish request needs a topic.")
        topic = validate_destination(topic)
        content_type = frame.get("content_type", _JSON)
        if not isinstance(content_type, str):
            raise ProtocolError("INVALID_MESSAGE", "content_type must be a string.")
        headers = frame.get("headers", {})
        if headers is None:
            headers = {}
        if not isinstance(headers, dict):
            raise ProtocolError("INVALID_MESSAGE", "Headers must be an object.")
        data = frame.get("data")
        _check_sizes(data, headers, content_type, self.config)
        key = frame.get("idempotency_key")
        if key is not None and not isinstance(key, str):
            raise ProtocolError("INVALID_MESSAGE", "idempotency_key must be a string.")
        if key == "":
            raise ProtocolError("INVALID_MESSAGE", "idempotency_key must not be empty.")
        bound = [binding.queue for binding in self.config.bindings if binding.topic == topic]
        limits = {
            queue.name: queue.max_depth
            for queue in self.config.queues
            if queue.max_depth is not None and queue.name in bound
        }
        message_id, sequence, created, jobs = await storage.publish_event(
            topic=topic,
            data=data,
            headers=headers,
            content_type=content_type,
            idempotency_key=key,
            window_seconds=self.config.messaging.idempotency_window_seconds,
            bound_queues=bound,
            queue_limits=limits,
        )
        if created:
            logger.info("accepted event %s sequence %s", topic, sequence)
            for queue, job_id in jobs:
                logger.info("bound event %s as job %s on %s", topic, job_id, queue)
            try:
                await self._after_publish(
                    topic=topic,
                    message_id=message_id,
                    sequence=sequence,
                    data=data,
                    headers=headers,
                    content_type=content_type,
                )
            except Exception:
                logger.exception("fan-out failed")
            for queue, _job_id in jobs:
                try:
                    await self._fill_queue(queue)
                except Exception:
                    logger.exception("binding delivery failed")
        await self._send(
            session,
            {
                "op": "publish_ok",
                "request_id": request,
                "message_id": message_id,
                "sequence": sequence,
            },
        )

    async def _after_publish(
        self,
        *,
        topic: str,
        message_id: str,
        sequence: int,
        data: object,
        headers: dict[str, object],
        content_type: str,
    ) -> None:
        storage = self._require_storage()
        retention = self.config.storage.retention.events
        changed = await storage.apply_retention(
            max_messages=retention.max_messages,
            max_age_days=retention.max_age_days,
        )
        await self._notify_gaps(changed)
        await self._fanout(
            topic=topic,
            message_id=message_id,
            sequence=sequence,
            data=data,
            headers=headers,
            content_type=content_type,
        )

    async def _fanout(
        self,
        *,
        topic: str,
        message_id: str,
        sequence: int,
        data: object,
        headers: dict[str, object],
        content_type: str,
    ) -> None:
        limit = self.config.messaging.pubsub.max_unacknowledged
        noted: list[str] = []
        for session in list(self._sessions):
            async with session.send_lock:
                for sub in session.ephemeral:
                    if sub.queued >= limit or not matches(sub.pattern, topic):
                        continue
                    sub.queued += 1
                    noted.append(sub.subscription_id)
                    await session.outbound.put(
                        _Outgoing(
                            _event_frame(
                                delivery_id=uuid7(),
                                message_id=message_id,
                                subscription=sub.subscription_id,
                                sequence=sequence,
                                destination=topic,
                                data=data,
                                headers=headers,
                                content_type=content_type,
                                durable=False,
                            ),
                            ephemeral=sub,
                        )
                    )
        storage = self._require_storage()
        for subscription in noted:
            try:
                await storage.record_delivery(
                    message_id=message_id,
                    kind="event",
                    action="deliver",
                    subscription=subscription,
                    counter="delivered_messages",
                )
            except Exception:
                logger.exception("delivery record failed")
        for name, owner in list(self._owners.items()):
            record = await storage.get_subscription(name)
            if record is None or record.pending_delivery_id is not None:
                continue
            if record.cursor_sequence >= sequence or not matches(record.pattern, topic):
                continue
            delivery = await storage.claim_next(name, resend=False)
            await self._enqueue_durable(owner, name, delivery)

    async def _notify_gaps(self, names: list[str]) -> None:
        storage = self._require_storage()
        for name in names:
            owner = self._owners.get(name)
            if owner is None:
                continue
            record = await storage.get_subscription(name)
            if record is None or record.gap_from is None or record.gap_to is None:
                continue
            async with owner.send_lock:
                if self._owners.get(name) is not owner:
                    continue
                await owner.outbound.put(
                    _Outgoing(
                        {
                            "op": "gap",
                            "subscription": name,
                            "from_sequence": record.gap_from,
                            "to_sequence": record.gap_to,
                        }
                    )
                )
            delivery = await storage.claim_next(name, resend=False)
            await self._enqueue_durable(owner, name, delivery)

    async def _subscribe(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        pattern_value = frame.get("pattern")
        if not isinstance(pattern_value, str):
            raise ProtocolError("INVALID_MESSAGE", "A subscribe request needs a pattern.")
        pattern = validate_pattern(pattern_value)
        start_value = frame.get("start", self.config.messaging.pubsub.default_start_position)
        if not isinstance(start_value, str):
            raise ProtocolError("INVALID_MESSAGE", "start must be a string.")
        sequence = frame.get("sequence")
        if start_value == "sequence" and (
            isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1
        ):
            raise ProtocolError("INVALID_MESSAGE", "sequence must be a positive integer.")
        chosen = sequence if isinstance(sequence, int) and not isinstance(sequence, bool) else None
        durable = frame.get("durable")
        if durable is None:
            await self._subscribe_ephemeral(session, request, pattern)
            return
        if not isinstance(durable, str):
            raise ProtocolError("INVALID_MESSAGE", "durable must be a subscription name.")
        name = validate_destination(durable)
        storage = self._require_storage()
        record = await storage.open_subscription(
            name=name,
            pattern=pattern,
            start=start_value,
            sequence=chosen,
        )
        async with session.send_lock:
            current = self._owners.get(name)
            if current is not None:
                raise ProtocolError(
                    "SUBSCRIPTION_CONFLICT",
                    "This durable subscription already has a consumer.",
                )
            self._owners[name] = session
            session.durables.append(name)
            await session.outbound.put(_Outgoing(_subscribe_ok(request, record)))
        delivery = await storage.claim_next(name, resend=True)
        await self._enqueue_durable(session, name, delivery)

    async def _subscribe_ephemeral(
        self,
        session: _Session,
        request: str | None,
        pattern: str,
    ) -> None:
        subscription_id = uuid7()
        ephemeral = _Ephemeral(pattern, subscription_id)
        frame: dict[str, object] = {
            "op": "subscribe_ok",
            "request_id": request,
            "subscription_id": subscription_id,
            "durable": None,
            "pattern": pattern,
        }
        async with session.send_lock:
            session.ephemeral.append(ephemeral)
            await session.outbound.put(_Outgoing(frame))

    async def _enqueue_durable(
        self,
        session: _Session,
        name: str,
        delivery: PendingDelivery | None,
    ) -> None:
        if delivery is None:
            return
        frame = _event_frame(
            delivery_id=delivery.delivery_id,
            message_id=delivery.message_id,
            subscription=name,
            sequence=delivery.sequence,
            destination=delivery.destination,
            data=delivery.data,
            headers=delivery.headers,
            content_type=delivery.content_type,
            durable=True,
        )
        async with session.send_lock:
            if self._owners.get(name) is not session:
                return
            if delivery.delivery_id in session.sent:
                return
            session.sent.add(delivery.delivery_id)
            await session.outbound.put(_Outgoing(frame))

    async def _ack(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        delivery_id = frame.get("delivery_id")
        if not isinstance(delivery_id, str) or not delivery_id:
            raise ProtocolError("INVALID_MESSAGE", "An acknowledgement needs a delivery_id.")
        storage = self._require_storage()
        name, advanced = await storage.acknowledge(delivery_id)
        async with session.send_lock:
            await session.outbound.put(
                _Outgoing(
                    {"op": "ack_ok", "request_id": request, "delivery_id": delivery_id},
                )
            )
            session.sent.discard(delivery_id)
        if not advanced:
            return
        owner = self._owners.get(name)
        if owner is None:
            return
        delivery = await storage.claim_next(name, resend=False)
        await self._enqueue_durable(owner, name, delivery)

    async def _enqueue(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        storage = self._require_storage()
        queue = self._queue_name(frame.get("queue"))
        spec = self._queue_spec(queue)
        data, headers, content_type, key = _message_parts(frame, self.config)
        job_id, created = await storage.enqueue_job(
            queue=queue,
            data=data,
            headers=headers,
            content_type=content_type,
            idempotency_key=key,
            window_seconds=self.config.messaging.idempotency_window_seconds,
            max_depth=spec.max_depth,
        )
        if created:
            logger.info("accepted job %s on %s", job_id, spec.name)
            await self._fill_queue(queue)
        await self._send(
            session,
            {
                "op": "enqueue_ok",
                "request_id": request,
                "message_id": job_id,
                "created": created,
            },
        )

    async def _consume(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        if session.queue is not None:
            raise ProtocolError("CONSUMER_BUSY", "This connection is already consuming a queue.")
        queue = self._queue_name(frame.get("queue"))
        self._queue_spec(queue)
        consumers = self.config.consumers
        prefetch = frame.get("prefetch", consumers.default_prefetch)
        if (
            not isinstance(prefetch, int)
            or isinstance(prefetch, bool)
            or not 1 <= prefetch <= consumers.max_prefetch
        ):
            raise ProtocolError(
                "INVALID_MESSAGE",
                f"prefetch must be an integer from 1 to {consumers.max_prefetch}.",
            )
        session.queue = queue
        session.prefetch = prefetch
        session.worker_id = uuid7()
        self._workers.setdefault(queue, []).append(session)
        async with session.send_lock:
            await session.outbound.put(
                _Outgoing({"op": "consume_ok", "request_id": request, "queue": queue})
            )
        await self._fill(session)

    async def _ack_job(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        delivery_id, token = _lease_pair(frame)
        storage = self._require_storage()
        retention = self.config.storage.retention
        await storage.complete_job(
            delivery_id,
            token,
            completed_max_age_days=retention.completed_jobs.max_age_days,
            dead_max_age_days=retention.dead_letters.max_age_days,
        )
        self._forget_lease(session, delivery_id, token)
        await self._send(
            session,
            {"op": "ack_ok", "request_id": request, "delivery_id": delivery_id},
        )
        if session.queue is not None:
            await self._fill(session)

    async def _nack(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        delivery_id, token = _lease_pair(frame)
        requeue = frame.get("requeue", True)
        if not isinstance(requeue, bool):
            raise ProtocolError("INVALID_MESSAGE", "requeue must be a boolean.")
        reason = frame.get("reason", "The worker declined the job.")
        if not isinstance(reason, str) or not reason:
            raise ProtocolError("INVALID_MESSAGE", "reason must be a non-empty string.")
        await self._fail_job(session, delivery_id, token, reason=reason, requeue=requeue)
        await self._send(
            session,
            {"op": "nack_ok", "request_id": request, "delivery_id": delivery_id},
        )

    async def _reject(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        delivery_id, token = _lease_pair(frame)
        reason = frame.get("reason")
        if not isinstance(reason, str) or not reason:
            raise ProtocolError("INVALID_MESSAGE", "reason must be a non-empty string.")
        await self._fail_job(session, delivery_id, token, reason=reason, requeue=False)
        await self._send(
            session,
            {"op": "reject_ok", "request_id": request, "delivery_id": delivery_id},
        )

    async def _extend(
        self,
        frame: dict[str, object],
        request: str | None,
        session: _Session,
    ) -> None:
        delivery_id, token = _lease_pair(frame)
        seconds = frame.get("seconds")
        if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds < 1:
            raise ProtocolError("INVALID_MESSAGE", "seconds must be a positive integer.")
        expires = await self._require_storage().extend_job(delivery_id, token, seconds)
        await self._send(
            session,
            {
                "op": "extend_ok",
                "request_id": request,
                "delivery_id": delivery_id,
                "lease_expires_at": expires,
            },
        )

    async def _fail_job(
        self,
        session: _Session,
        delivery_id: str,
        token: str,
        *,
        reason: str,
        requeue: bool,
    ) -> None:
        queue = session.queue
        if queue is None:
            raise ProtocolError("DELIVERY_NOT_FOUND", "This connection is not consuming a queue.")
        policy = self._policy(self._queue_spec(queue))
        outcome = await self._require_storage().fail_delivery(
            delivery_id,
            token,
            reason=reason,
            requeue=requeue,
            policy=policy,
        )
        self._forget_lease(session, delivery_id, token)
        if outcome == "dead":
            logger.info("dead-lettered job delivery %s", delivery_id)
        await self._fill_queue(queue)
        if outcome == "dead" and policy.dead_letter_queue:
            await self._fill_queue(policy.dead_letter_queue)

    async def _fill_queue(self, queue: str) -> None:
        for session in list(self._workers.get(queue, [])):
            await self._fill(session)

    async def _fill(self, session: _Session) -> None:
        queue = session.queue
        worker_id = session.worker_id
        if queue is None or worker_id is None:
            return
        policy = self._policy(self._queue_spec(queue))
        async with session.fill_lock:
            while session.held < session.prefetch and not self._stopping:
                storage = self._storage
                if storage is None or session.queue != queue:
                    return
                lease = await storage.claim_job(queue, worker_id=worker_id, policy=policy)
                if lease is None:
                    return
                session.held += 1
                session.leases.add((lease.id, lease.delivery_id, lease.lease_token))
                async with session.send_lock:
                    await session.outbound.put(_Outgoing(_job_frame(lease)))

    async def _release_session(self, session: _Session) -> None:
        queue = session.queue
        leases = list(session.leases)
        session.leases.clear()
        session.held = 0
        self._drop_worker(session)
        storage = self._storage
        if queue is None or storage is None or not leases:
            return
        policy = self._policy(self._queue_spec(queue))
        for job_id, _delivery_id, token in leases:
            await storage.release_lease(
                job_id=job_id,
                lease_token=token,
                policy=policy,
                reason="The worker disconnected.",
            )
        await self._fill_queue(queue)

    async def _sweep_leases(self) -> None:
        while not self._stopping:
            wake = self._wake
            if wake is not None:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wake.wait(), 0.05)
                wake.clear()
            storage = self._storage
            if self._stopping or storage is None:
                return
            for spec in self.config.queues:
                if self._stopping:
                    return
                try:
                    await storage.reap_queue(spec.name, self._policy(spec))
                    await self._fill_queue(spec.name)
                except Exception:
                    logger.exception("lease sweep failed")

    def _drop_worker(self, session: _Session) -> None:
        queue = session.queue
        if queue is None:
            return
        current = self._workers.get(queue, [])
        self._workers[queue] = [item for item in current if item is not session]
        session.queue = None

    def _forget_lease(self, session: _Session, delivery_id: str, token: str) -> None:
        for item in list(session.leases):
            if item[1] == delivery_id and item[2] == token:
                session.leases.discard(item)
                if session.held > 0:
                    session.held -= 1
                return

    def _queue_name(self, value: object) -> str:
        if not isinstance(value, str):
            raise ProtocolError("INVALID_MESSAGE", "queue must be a string.")
        return validate_destination(value)

    def _queue_spec(self, name: str) -> QueueSpec:
        for spec in self.config.queues:
            if spec.name == name:
                return spec
        raise ProtocolError("QUEUE_NOT_FOUND", f"Queue is not configured: {name}")

    def _policy(self, spec: QueueSpec) -> AttemptPolicy:
        retry = spec.retry
        return AttemptPolicy(
            max_attempts=spec.max_attempts,
            strategy=retry.strategy,
            initial_delay_seconds=retry.initial_delay_seconds,
            multiplier=retry.multiplier,
            max_delay_seconds=retry.max_delay_seconds,
            increment_seconds=retry.increment_seconds,
            dead_letter_queue=spec.dead_letter_queue,
            visibility_timeout_seconds=spec.visibility_timeout_seconds,
        )

    async def _history(self, frame: dict[str, object], request: str | None) -> dict[str, object]:
        storage = self._require_storage()
        topic = frame.get("topic")
        if topic is not None and not isinstance(topic, str):
            raise ProtocolError("INVALID_MESSAGE", "topic must be a string.")
        if isinstance(topic, str):
            topic = validate_pattern(topic) if "*" in topic else validate_destination(topic)
        limit = frame.get("limit", 50)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise ProtocolError("INVALID_MESSAGE", "limit must be an integer from 1 to 1000.")
        messages = await storage.history(topic=topic, limit=limit)
        return {"op": "history_ok", "request_id": request, "messages": messages}

    async def _status(self, request: str | None) -> dict[str, object]:
        snapshot = await self._snapshot()
        snapshot["op"] = "status_ok"
        snapshot["request_id"] = request
        return snapshot

    async def _snapshot(self) -> dict[str, object]:
        storage = self._require_storage()
        report = await storage.storage_report()
        counts = await storage.queue_counts(self._now())
        topics = await storage.topics()
        subscriptions = [_subscription_brief(item) for item in await storage.list_subscriptions()]
        names = [queue.name for queue in self.config.queues]
        for name in counts:
            if name not in names:
                names.append(name)
        queues: list[dict[str, object]] = []
        empty = {"pending": 0, "retry": 0, "leased": 0, "completed": 0, "dead": 0, "delayed": 0}
        for name in names:
            bucket = counts.get(name, empty)
            queues.append(
                {
                    "name": name,
                    "pending": bucket["pending"],
                    "retry": bucket["retry"],
                    "leased": bucket["leased"],
                    "completed": bucket["completed"],
                    "dead": bucket["dead"],
                    "delayed": bucket["delayed"],
                    "workers": len(self._workers.get(name, [])),
                }
            )
        return {
            "messages": report["messages"],
            "jobs": report["jobs"],
            "connections": self._connections,
            "uptime_seconds": round(time.monotonic() - self._started, 3),
            "storage_bytes": report["bytes"],
            "healthy": report["healthy"],
            "topics": topics,
            "queues": queues,
            "subscriptions": subscriptions,
            "metrics": await storage.counters(),
        }

    async def _load_runtime_queues(self) -> None:
        storage = self._require_storage()
        known = {queue.name for queue in self.config.queues}
        for row in await storage.runtime_queues():
            name = str(row["name"])
            if name in known:
                continue
            attempts = row["max_attempts"]
            dead = row["dead_letter_queue"]
            self.config.queues.append(
                QueueSpec(
                    name=name,
                    max_attempts=attempts if attempts is None or isinstance(attempts, int) else 5,
                    visibility_timeout_seconds=int(str(row["visibility_timeout_seconds"])),
                    dead_letter_queue=dead if isinstance(dead, str) else None,
                )
            )
            known.add(name)

    async def _inspect(self, frame: dict[str, object], request: str | None) -> dict[str, object]:
        queue = self._queue_name(frame.get("queue"))
        self._queue_spec(queue)
        limit = frame.get("limit", 50)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ProtocolError("INVALID_MESSAGE", "limit must be an integer from 1 to 100.")
        jobs = await self._require_storage().inspect_queue(queue, limit)
        return {"op": "inspect_ok", "request_id": request, "queue": queue, "jobs": jobs}

    async def _retry_admin(
        self,
        frame: dict[str, object],
        request: str | None,
    ) -> dict[str, object]:
        message_id = frame.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise ProtocolError("INVALID_MESSAGE", "message_id must be a string.")
        job = await self._require_storage().retry_job(message_id)
        queue = str(job["queue"])
        logger.info("retried job %s on %s", message_id, queue)
        await self._fill_queue(queue)
        return {"op": "retry_ok", "request_id": request, "job": job}

    async def _purge(self, frame: dict[str, object], request: str | None) -> dict[str, object]:
        queue = self._queue_name(frame.get("queue"))
        self._queue_spec(queue)
        dry_run = frame.get("dry_run", False)
        if not isinstance(dry_run, bool):
            raise ProtocolError("INVALID_MESSAGE", "dry_run must be a boolean.")
        deleted = await self._require_storage().purge_queue(queue, dry_run=dry_run)
        if not dry_run:
            logger.info("purged %s jobs from %s", deleted, queue)
        return {
            "op": "purge_ok",
            "request_id": request,
            "queue": queue,
            "deleted": deleted,
            "dry_run": dry_run,
        }

    async def _queue_create(
        self,
        frame: dict[str, object],
        request: str | None,
    ) -> dict[str, object]:
        name = self._queue_name(frame.get("queue"))
        if any(queue.name == name for queue in self.config.queues):
            raise ProtocolError("QUEUE_EXISTS", f"Queue already exists: {name}")
        dead = frame.get("dead_letter_queue")
        if dead is not None:
            if not isinstance(dead, str):
                raise ProtocolError("INVALID_MESSAGE", "dead_letter_queue must be a string.")
            dead = validate_destination(dead)
            if dead == name:
                raise ProtocolError(
                    "INVALID_MESSAGE",
                    "A queue cannot be its own dead-letter queue.",
                )
            self._queue_spec(dead)
        attempts = frame.get("max_attempts", 5)
        if attempts is not None and (
            not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 1
        ):
            raise ProtocolError("INVALID_MESSAGE", "max_attempts must be at least 1, or null.")
        visibility = frame.get("visibility_timeout_seconds", 60)
        if not isinstance(visibility, int) or isinstance(visibility, bool) or visibility < 1:
            raise ProtocolError(
                "INVALID_MESSAGE",
                "visibility_timeout_seconds must be a positive integer.",
            )
        await self._require_storage().create_runtime_queue(
            name=name,
            max_attempts=attempts if isinstance(attempts, int) else None,
            visibility_timeout_seconds=visibility,
            dead_letter_queue=dead if isinstance(dead, str) else None,
        )
        self.config.queues.append(
            QueueSpec(
                name=name,
                max_attempts=attempts if isinstance(attempts, int) else None,
                visibility_timeout_seconds=visibility,
                dead_letter_queue=dead if isinstance(dead, str) else None,
            )
        )
        logger.info("created queue %s", name)
        return {"op": "queue_ok", "request_id": request, "queue": name}

    async def _subscription_admin(
        self,
        frame: dict[str, object],
        request: str | None,
        *,
        action: str,
    ) -> dict[str, object]:
        raw_name = frame.get("name")
        if not isinstance(raw_name, str):
            raise ProtocolError("INVALID_MESSAGE", "name must be a string.")
        name = validate_destination(raw_name)
        storage = self._require_storage()
        if action == "delete":
            await storage.delete_subscription(name)
            owner = self._owners.pop(name, None)
            if owner is not None:
                owner.durables = [item for item in owner.durables if item != name]
            logger.info("deleted subscription %s", name)
            return {"op": "subscription_ok", "request_id": request, "name": name}
        if action == "reset":
            start = frame.get("start", "earliest")
            if start not in {"latest", "earliest"}:
                raise ProtocolError("INVALID_MESSAGE", "start must be latest or earliest.")
            record = await storage.reset_subscription(name, str(start))
            logger.info("reset subscription %s", name)
            brief = _subscription_brief(record)
            return {"op": "subscription_ok", "request_id": request, "subscription": brief}
        pattern = frame.get("pattern")
        if not isinstance(pattern, str):
            raise ProtocolError("INVALID_MESSAGE", "pattern must be a string.")
        pattern = validate_pattern(pattern)
        try:
            await storage.subscription(name)
        except ProtocolError as exc:
            if exc.code != "NOT_FOUND":
                raise
        else:
            raise ProtocolError("SUBSCRIPTION_CONFLICT", "This subscription already exists.")
        record = await storage.open_subscription(
            name=name,
            pattern=pattern,
            start="latest",
            sequence=None,
        )
        logger.info("created subscription %s", name)
        brief = _subscription_brief(record)
        return {"op": "subscription_ok", "request_id": request, "subscription": brief}

    async def _message(self, frame: dict[str, object], request: str | None) -> dict[str, object]:
        message_id = frame.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise ProtocolError("INVALID_MESSAGE", "message_id must be a string.")
        found = await self._require_storage().find_record(message_id)
        return {"op": "message_ok", "request_id": request, **found}

    async def _backup(self, frame: dict[str, object], request: str | None) -> dict[str, object]:
        raw = frame.get("path")
        if not isinstance(raw, str) or not raw:
            raise ProtocolError("INVALID_MESSAGE", "path must be a string.")
        destination = Path(raw)
        if not destination.is_absolute():
            raise ProtocolError("INVALID_MESSAGE", "The backup path must be absolute.")
        await self._require_storage().backup_to(destination)
        logger.info("backed up the database to %s", destination)
        return {"op": "backup_ok", "request_id": request, "path": str(destination)}

    def _require_storage(self) -> Storage:
        if self._storage is None:
            raise ProtocolError("STORAGE", "The database is not open.")
        return self._storage


def _job_frame(lease: JobLease) -> dict[str, object]:
    frame: dict[str, object] = {
        "op": "delivery",
        "kind": "job",
        "delivery_id": lease.delivery_id,
        "message_id": lease.id,
        "queue": lease.queue,
        "lease_token": lease.lease_token,
        "attempt": lease.attempts,
        "lifetime_attempts": lease.lifetime_attempts,
        "data": lease.data,
        "headers": lease.headers if isinstance(lease.headers, dict) else {},
        "content_type": lease.content_type,
    }
    if lease.original_queue != lease.queue:
        frame["original_queue"] = lease.original_queue
    if lease.failure_reason is not None:
        frame["failure_reason"] = lease.failure_reason
    return frame


def _message_parts(
    frame: dict[str, object],
    config: RelayConfig,
) -> tuple[object, dict[str, object], str, str | None]:
    content_type = frame.get("content_type", _JSON)
    if not isinstance(content_type, str):
        raise ProtocolError("INVALID_MESSAGE", "content_type must be a string.")
    headers = frame.get("headers", {})
    if headers is None:
        headers = {}
    if not isinstance(headers, dict):
        raise ProtocolError("INVALID_MESSAGE", "Headers must be an object.")
    data = frame.get("data")
    typed = {str(key): value for key, value in headers.items()}
    _check_sizes(data, typed, content_type, config)
    raw_key = frame.get("idempotency_key")
    if raw_key is None:
        return data, typed, content_type, None
    if not isinstance(raw_key, str) or raw_key == "":
        raise ProtocolError("INVALID_MESSAGE", "idempotency_key must be a non-empty string.")
    return data, typed, content_type, raw_key


def _lease_pair(frame: dict[str, object]) -> tuple[str, str]:
    delivery_id = frame.get("delivery_id")
    token = frame.get("lease_token")
    if not isinstance(delivery_id, str) or not delivery_id:
        raise ProtocolError("INVALID_MESSAGE", "An acknowledgement needs a delivery_id.")
    if not isinstance(token, str) or not token:
        raise ProtocolError("INVALID_MESSAGE", "A job settlement needs a lease_token.")
    return delivery_id, token


def _subscription_brief(record: Subscription) -> dict[str, object]:
    return {
        "name": record.name,
        "pattern": record.pattern,
        "cursor_sequence": record.cursor_sequence,
        "pending_message_id": record.pending_message_id,
    }


def _subscribe_ok(request: str | None, record: Subscription) -> dict[str, object]:
    frame: dict[str, object] = {
        "op": "subscribe_ok",
        "request_id": request,
        "subscription_id": record.id,
        "durable": record.name,
        "pattern": record.pattern,
    }
    if record.gap_from is not None and record.gap_to is not None:
        frame["gap"] = {"from_sequence": record.gap_from, "to_sequence": record.gap_to}
    return frame


def _event_frame(
    *,
    delivery_id: str,
    message_id: str,
    subscription: str,
    sequence: int,
    destination: str,
    data: object,
    headers: object,
    content_type: str,
    durable: bool,
) -> dict[str, object]:
    return {
        "op": "delivery",
        "delivery_id": delivery_id,
        "message_id": message_id,
        "subscription": subscription,
        "sequence": sequence,
        "destination": destination,
        "data": data,
        "headers": headers if isinstance(headers, dict) else {},
        "content_type": content_type,
        "durable": durable,
    }


def _hello_ok(frame: dict[str, object], config: RelayConfig, *, tcp: bool) -> bool:
    if frame.get("protocol") != PROTOCOL:
        return False
    token = config.security.token
    if tcp and token:
        provided = frame.get("token")
        if not isinstance(provided, str) or not hmac.compare_digest(provided, token):
            return False
    return True


def _check_sizes(
    data: object,
    headers: dict[str, object],
    content_type: str,
    config: RelayConfig,
) -> None:
    if content_type == _TEXT and not isinstance(data, str):
        raise ProtocolError("INVALID_MESSAGE", "A text payload must be a string.")
    if content_type == _BINARY:
        if not isinstance(data, str):
            raise ProtocolError("INVALID_MESSAGE", "A binary payload must be a base64 string.")
        try:
            base64.b64decode(data, validate=True)
        except Exception as exc:
            raise ProtocolError(
                "INVALID_MESSAGE",
                "A binary payload must be valid base64.",
            ) from exc
    elif content_type != _JSON:
        raise ProtocolError("INVALID_MESSAGE", f"Unsupported content type: {content_type}")
    try:
        payload = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
        header_bytes = json.dumps(headers, separators=(",", ":"), sort_keys=True).encode()
    except TypeError as exc:
        raise ProtocolError("INVALID_MESSAGE", "The payload and headers must be JSON.") from exc
    if len(payload) > config.messaging.max_payload_kb * 1024:
        raise ProtocolError("MESSAGE_TOO_LARGE", "The payload exceeds the configured limit.")
    if len(header_bytes) > config.messaging.max_headers_kb * 1024:
        raise ProtocolError("MESSAGE_TOO_LARGE", "The headers exceed the configured limit.")


def _error(request: str | None, code: str, message: str) -> dict[str, object]:
    return {"op": "error", "request_id": request, "code": code, "message": message}


async def _write(writer: asyncio.StreamWriter, frame: dict[str, object]) -> None:
    writer.write(json.dumps(frame, separators=(",", ":")).encode() + b"\n")
    await writer.drain()
