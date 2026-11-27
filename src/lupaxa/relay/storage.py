"""SQLite storage for committed messages and durable subscriptions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite

from lupaxa.relay.exceptions import ProtocolError, UsageError
from lupaxa.relay.ids import uuid7
from lupaxa.relay.matching import matches
from lupaxa.relay.retry import AttemptPolicy, retry_delay

SCHEMA_VERSION = 5
_RETENTION_BATCH = 100

_CREATE = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    destination TEXT NOT NULL,
    sequence INTEGER NOT NULL UNIQUE,
    timestamp TEXT NOT NULL,
    headers TEXT NOT NULL,
    content_type TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_destination_sequence
    ON messages (destination, sequence);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    operation TEXT NOT NULL,
    key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    message_id TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (operation, key)
);
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    pattern TEXT NOT NULL,
    cursor_sequence INTEGER NOT NULL,
    pending_delivery_id TEXT,
    pending_message_id TEXT,
    pending_sequence INTEGER,
    last_delivery_id TEXT,
    gap_from INTEGER,
    gap_to INTEGER,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS queue_jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    original_queue TEXT NOT NULL,
    state TEXT NOT NULL,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL,
    content_type TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    lifetime_attempts INTEGER NOT NULL,
    available_at REAL NOT NULL,
    created_at REAL NOT NULL,
    lease_token TEXT,
    lease_expires_at REAL,
    delivery_id TEXT,
    worker_id TEXT,
    failure_reason TEXT,
    failed_at REAL,
    completed_at REAL
);
CREATE INDEX IF NOT EXISTS queue_jobs_ready
    ON queue_jobs (queue, state, available_at, created_at);
CREATE INDEX IF NOT EXISTS queue_jobs_lease
    ON queue_jobs (state, lease_expires_at);
CREATE TABLE IF NOT EXISTS runtime_queues (
    name TEXT PRIMARY KEY,
    max_attempts INTEGER,
    visibility_timeout_seconds INTEGER NOT NULL,
    dead_letter_queue TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY,
    message_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    action TEXT NOT NULL,
    subscription TEXT,
    queue TEXT,
    worker_id TEXT,
    attempt INTEGER,
    detail TEXT,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS deliveries_message ON deliveries (message_id, id);
"""

_CREATE_SUBSCRIPTIONS = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    pattern TEXT NOT NULL,
    cursor_sequence INTEGER NOT NULL,
    pending_delivery_id TEXT,
    pending_message_id TEXT,
    pending_sequence INTEGER,
    last_delivery_id TEXT,
    gap_from INTEGER,
    gap_to INTEGER,
    created_at TEXT NOT NULL
);
"""

_CREATE_JOBS = """
CREATE TABLE IF NOT EXISTS queue_jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    original_queue TEXT NOT NULL,
    state TEXT NOT NULL,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL,
    content_type TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    lifetime_attempts INTEGER NOT NULL,
    available_at REAL NOT NULL,
    created_at REAL NOT NULL,
    lease_token TEXT,
    lease_expires_at REAL,
    delivery_id TEXT,
    worker_id TEXT,
    failure_reason TEXT,
    failed_at REAL,
    completed_at REAL
);
CREATE INDEX IF NOT EXISTS queue_jobs_ready
    ON queue_jobs (queue, state, available_at, created_at);
CREATE INDEX IF NOT EXISTS queue_jobs_lease
    ON queue_jobs (state, lease_expires_at);
"""

_CREATE_RUNTIME = """
CREATE TABLE IF NOT EXISTS runtime_queues (
    name TEXT PRIMARY KEY,
    max_attempts INTEGER,
    visibility_timeout_seconds INTEGER NOT NULL,
    dead_letter_queue TEXT,
    created_at REAL NOT NULL
);
"""

_CREATE_OBSERVABILITY = """
CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY,
    message_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    action TEXT NOT NULL,
    subscription TEXT,
    queue TEXT,
    worker_id TEXT,
    attempt INTEGER,
    detail TEXT,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS deliveries_message ON deliveries (message_id, id);
"""

_COUNTER_NAMES = ("published_events", "enqueued_jobs", "delivered_messages", "acknowledged_deliveries", "negative_acknowledgements", "retries", "dead_lettered_jobs")


@dataclass(frozen=True)
class Subscription:
    """A durable subscription and its stored delivery position."""

    id: str
    name: str
    pattern: str
    cursor_sequence: int
    pending_delivery_id: str | None
    pending_message_id: str | None
    pending_sequence: int | None
    last_delivery_id: str | None
    gap_from: int | None
    gap_to: int | None


@dataclass(frozen=True)
class PendingDelivery:
    """One event claimed for a durable subscriber."""

    delivery_id: str
    message_id: str
    sequence: int
    destination: str
    data: object
    headers: object
    content_type: str


@dataclass(frozen=True)
class JobLease:
    """A job currently leased to one worker."""

    id: str
    queue: str
    original_queue: str
    delivery_id: str
    lease_token: str
    attempts: int
    lifetime_attempts: int
    data: object
    headers: object
    content_type: str
    failure_reason: str | None


class Storage:
    """One SQLite connection. Callers must stay on the broker's event loop."""

    def __init__(self, path: Path, *, durability: str, now: Callable[[], float] | None = None) -> None:
        self.path = path
        self.durability = durability
        self._now = now or time.time
        self._db: aiosqlite.Connection | None = None
        self._gate: asyncio.Lock | None = None

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        self._gate = asyncio.Lock()
        sync = "FULL" if self.durability == "full" else "NORMAL"
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute(f"PRAGMA synchronous={sync}")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._migrate()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def publish_event(
        self,
        *,
        topic: str,
        data: object,
        headers: dict[str, object],
        content_type: str,
        idempotency_key: str | None,
        window_seconds: int,
        bound_queues: list[str] | None = None,
        queue_limits: dict[str, int] | None = None,
    ) -> tuple[str, int, bool, list[tuple[str, str]]]:
        """Store an event and any bound jobs. The bool is false on idempotency replay."""
        gate, db = self._require()
        digest = _fingerprint(topic, content_type, headers, data)
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                if idempotency_key:
                    existing_id = await self._replay(db, "publish", idempotency_key, digest, window_seconds)
                    if existing_id is not None:
                        found = await db.execute("SELECT id, sequence FROM messages WHERE id = ?", (existing_id,))
                        message = await found.fetchone()
                        await found.close()
                        if message is None:
                            raise UsageError("The idempotency record has no message.")
                        await db.rollback()
                        return str(message["id"]), int(message["sequence"]), False, []
                message_id = uuid7()
                cursor = await db.execute("SELECT COALESCE(MAX(sequence), 0) FROM messages")
                row = await cursor.fetchone()
                await cursor.close()
                if row is None or row[0] is None:
                    raise UsageError("Could not allocate a message sequence.")
                sequence = int(row[0]) + 1
                timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                payload = json.dumps(data, separators=(",", ":"), sort_keys=True)
                header_json = json.dumps(headers, separators=(",", ":"), sort_keys=True)
                inserted = await db.execute(
                    """
                    INSERT INTO messages (
                        id, kind, destination, sequence, timestamp, headers, content_type, data
                    ) VALUES (?, 'event', ?, ?, ?, ?, ?, ?)
                    """,
                    (message_id, topic, sequence, timestamp, header_json, content_type, payload),
                )
                await inserted.close()
                now = self._now()
                if idempotency_key:
                    stored = await db.execute(
                        """
                        INSERT INTO idempotency_keys (
                            operation, key, fingerprint, message_id, created_at
                        ) VALUES ('publish', ?, ?, ?, ?)
                        ON CONFLICT(operation, key) DO UPDATE SET
                            fingerprint = excluded.fingerprint,
                            message_id = excluded.message_id,
                            created_at = excluded.created_at
                        """,
                        (idempotency_key, digest, message_id, now),
                    )
                    await stored.close()
                jobs = await self._insert_bound_jobs(db, queues=bound_queues or [], payload=payload, headers=header_json, content_type=content_type, now=now, limits=queue_limits or {})
                await self._bump(db, "published_events")
                if jobs:
                    await self._bump(db, "enqueued_jobs", len(jobs))
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return message_id, sequence, True, jobs

    async def _insert_bound_jobs(self, db: aiosqlite.Connection, *, queues: list[str], payload: str, headers: str, content_type: str, now: float, limits: dict[str, int]) -> list[tuple[str, str]]:
        """Insert one pending job per queue. The caller owns the transaction."""
        jobs: list[tuple[str, str]] = []
        for queue in queues:
            limit = limits.get(queue)
            if limit is not None and await self._depth(db, queue) >= limit:
                raise ProtocolError("CAPACITY", f"Queue {queue} is full.")
            job_id = uuid7()
            cursor = await db.execute(
                """
                INSERT INTO queue_jobs (
                    id, queue, original_queue, state, payload, headers, content_type,
                    attempts, lifetime_attempts, available_at, created_at
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, 0, ?, ?)
                """,
                (job_id, queue, queue, payload, headers, content_type, now, now),
            )
            await cursor.close()
            jobs.append((queue, job_id))
        return jobs

    async def history(self, *, topic: str | None, limit: int) -> list[dict[str, object]]:
        gate, db = self._require()
        async with gate:
            if topic is not None and "*" in topic:
                rows = await self._history_pattern(db, topic, limit)
            elif topic is None:
                cursor = await db.execute(
                    """
                    SELECT * FROM messages
                    ORDER BY sequence DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                rows = list(await cursor.fetchall())
                await cursor.close()
            else:
                cursor = await db.execute(
                    """
                    SELECT * FROM messages
                    WHERE destination = ?
                    ORDER BY sequence DESC
                    LIMIT ?
                    """,
                    (topic, limit),
                )
                rows = list(await cursor.fetchall())
                await cursor.close()
        return [_envelope(row) for row in reversed(rows)]

    async def message_count(self) -> int:
        gate, db = self._require()
        async with gate:
            cursor = await db.execute("SELECT COUNT(*) FROM messages")
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    async def topics(self) -> list[dict[str, object]]:
        """Return destinations that have stored events, with their counts."""
        gate, db = self._require()
        async with gate:
            cursor = await db.execute(
                """
                SELECT destination, COUNT(*) AS messages
                FROM messages
                GROUP BY destination
                ORDER BY destination ASC
                """
            )
            rows = list(await cursor.fetchall())
            await cursor.close()
        return [{"name": str(row["destination"]), "messages": int(row["messages"])} for row in rows]

    async def queue_counts(self, now: float) -> dict[str, dict[str, int]]:
        """Count jobs by queue and state. Delayed jobs are retries that are not yet due."""
        gate, db = self._require()
        async with gate:
            cursor = await db.execute(
                """
                SELECT queue, state, COUNT(*) AS total,
                       SUM(CASE WHEN available_at > ? THEN 1 ELSE 0 END) AS delayed
                FROM queue_jobs
                GROUP BY queue, state
                """,
                (now,),
            )
            rows = list(await cursor.fetchall())
            await cursor.close()
        counts: dict[str, dict[str, int]] = {}
        for row in rows:
            bucket = counts.setdefault(str(row["queue"]), {"pending": 0, "retry": 0, "leased": 0, "completed": 0, "dead": 0, "delayed": 0})
            state = str(row["state"])
            if state in bucket:
                bucket[state] = int(row["total"])
            if state == "retry":
                bucket["delayed"] = int(row["delayed"] or 0)
        return counts

    async def inspect_queue(self, queue: str, limit: int) -> list[dict[str, object]]:
        """Return the oldest jobs on a queue, including payloads."""
        gate, db = self._require()
        async with gate:
            cursor = await db.execute(
                """
                SELECT * FROM queue_jobs
                WHERE queue = ?
                ORDER BY created_at ASC, rowid ASC
                LIMIT ?
                """,
                (queue, limit),
            )
            rows = list(await cursor.fetchall())
            await cursor.close()
        return [_job_row(row) for row in rows]

    async def find_record(self, message_id: str) -> dict[str, object]:
        """Return a stored event, a job, or both when an id exists in either table."""
        gate, db = self._require()
        async with gate:
            event_cursor = await db.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            event = await event_cursor.fetchone()
            await event_cursor.close()
            job_cursor = await db.execute("SELECT * FROM queue_jobs WHERE id = ?", (message_id,))
            job = await job_cursor.fetchone()
            await job_cursor.close()
            deliveries = await self._delivery_rows(db, message_id)
        if event is None and job is None:
            raise ProtocolError("NOT_FOUND", "No event or job has that id.")
        return {
            "event": None if event is None else _envelope(event),
            "job": None if job is None else _job_row(job),
            "deliveries": [_delivery_row(row) for row in deliveries],
        }

    async def retry_job(self, job_id: str) -> dict[str, object]:
        """Queue a dead-lettered job again. The id, payload, and failure history stay."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute("SELECT * FROM queue_jobs WHERE id = ?", (job_id,))
                row = await cursor.fetchone()
                await cursor.close()
                if row is None:
                    raise ProtocolError("NOT_FOUND", "No job has that id.")
                state = str(row["state"])
                original = str(row["original_queue"])
                current = str(row["queue"])
                dead_lettered = state == "dead" or current != original
                if state == "leased" or not dead_lettered:
                    raise ProtocolError("JOB_NOT_RETRYABLE", "Only a dead-lettered job can be retried.")
                now = self._now()
                updated = await db.execute(
                    """
                    UPDATE queue_jobs SET
                        queue = ?,
                        state = 'pending',
                        attempts = 0,
                        available_at = ?,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        delivery_id = NULL
                    WHERE id = ?
                    """,
                    (original, now, job_id),
                )
                await updated.close()
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        fresh = await self.find_record(job_id)
        job = fresh["job"]
        if not isinstance(job, dict):
            raise ProtocolError("NOT_FOUND", "No job has that id.")
        return job

    async def purge_queue(self, queue: str, *, dry_run: bool = False) -> int:
        """Delete jobs that are not currently leased. Returns the number removed."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute("SELECT id FROM queue_jobs WHERE queue = ? AND state != 'leased'", (queue,))
                rows = list(await cursor.fetchall())
                await cursor.close()
                if dry_run:
                    await db.rollback()
                    return len(rows)
                for row in rows:
                    job_id = str(row["id"])
                    removed = await db.execute("DELETE FROM queue_jobs WHERE id = ?", (job_id,))
                    await removed.close()
                    keys = await db.execute("DELETE FROM idempotency_keys WHERE message_id = ?", (job_id,))
                    await keys.close()
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return len(rows)

    async def runtime_queues(self) -> list[dict[str, object]]:
        """Return queues created through the administration commands."""
        gate, db = self._require()
        async with gate:
            cursor = await db.execute("SELECT * FROM runtime_queues ORDER BY name ASC")
            rows = list(await cursor.fetchall())
            await cursor.close()
        return [_runtime_queue(row) for row in rows]

    async def create_runtime_queue(self, *, name: str, max_attempts: int | None, visibility_timeout_seconds: int, dead_letter_queue: str | None) -> None:
        """Persist a queue created while the broker is running."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute("SELECT name FROM runtime_queues WHERE name = ?", (name,))
                existing = await cursor.fetchone()
                await cursor.close()
                if existing is not None:
                    raise ProtocolError("QUEUE_EXISTS", f"Queue already exists: {name}")
                inserted = await db.execute(
                    """
                    INSERT INTO runtime_queues (
                        name, max_attempts, visibility_timeout_seconds,
                        dead_letter_queue, created_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (name, max_attempts, visibility_timeout_seconds, dead_letter_queue, self._now()),
                )
                await inserted.close()
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def list_subscriptions(self) -> list[Subscription]:
        """Return every durable subscription."""
        gate, db = self._require()
        async with gate:
            cursor = await db.execute("SELECT * FROM subscriptions ORDER BY name ASC")
            rows = list(await cursor.fetchall())
            await cursor.close()
        return [_subscription(row) for row in rows]

    async def subscription(self, name: str) -> Subscription:
        """Return one durable subscription."""
        gate, db = self._require()
        async with gate:
            found = await self._subscription_by_name(db, name)
        if found is None:
            raise ProtocolError("NOT_FOUND", f"No subscription is named {name}.")
        return found

    async def delete_subscription(self, name: str) -> None:
        """Remove a durable subscription."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute("DELETE FROM subscriptions WHERE name = ?", (name,))
                deleted = cursor.rowcount
                await cursor.close()
                if not deleted:
                    raise ProtocolError("NOT_FOUND", f"No subscription is named {name}.")
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def reset_subscription(self, name: str, start: str) -> Subscription:
        """Move a subscription's cursor and drop its outstanding delivery."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                found = await self._subscription_by_name(db, name)
                if found is None:
                    raise ProtocolError("NOT_FOUND", f"No subscription is named {name}.")
                cursor_sequence = await self._start_cursor(db, start, None)
                updated = await db.execute(
                    """
                    UPDATE subscriptions SET
                        cursor_sequence = ?,
                        pending_delivery_id = NULL,
                        pending_message_id = NULL,
                        pending_sequence = NULL,
                        gap_from = NULL,
                        gap_to = NULL
                    WHERE name = ?
                    """,
                    (cursor_sequence, name),
                )
                await updated.close()
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return await self.subscription(name)

    async def storage_report(self) -> dict[str, object]:
        """Return file size, row counts, and the SQLite quick check result."""
        gate, db = self._require()
        async with gate:
            messages = await db.execute("SELECT COUNT(*) FROM messages")
            message_row = await messages.fetchone()
            await messages.close()
            jobs = await db.execute("SELECT COUNT(*) FROM queue_jobs")
            job_row = await jobs.fetchone()
            await jobs.close()
            check = await db.execute("PRAGMA quick_check")
            check_row = await check.fetchone()
            await check.close()
            version_cursor = await db.execute("SELECT MAX(version) FROM schema_migrations")
            version_row = await version_cursor.fetchone()
            await version_cursor.close()
        healthy = check_row is not None and str(check_row[0]) == "ok"
        version = 0 if version_row is None or version_row[0] is None else int(version_row[0])
        return {
            "bytes": _storage_bytes(self.path),
            "messages": 0 if message_row is None else int(message_row[0]),
            "jobs": 0 if job_row is None else int(job_row[0]),
            "healthy": healthy,
            "schema_version": version,
            "schema_compatible": version == SCHEMA_VERSION,
        }

    async def backup_to(self, destination: Path) -> None:
        """Copy the live database to destination with the SQLite backup API."""
        if destination.resolve() == self.path.resolve():
            raise UsageError("The backup path must not be the live database.")
        destination.parent.mkdir(parents=True, exist_ok=True)
        gate, db = self._require()
        target = sqlite3.connect(destination)
        try:
            async with gate:
                await db.backup(target)
        finally:
            target.close()

    async def open_subscription(self, *, name: str, pattern: str, start: str, sequence: int | None) -> Subscription:
        """Create a durable subscription, or return the one that already exists."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                current = await self._subscription_by_name(db, name)
                if current is not None:
                    if current.pattern != pattern:
                        raise ProtocolError("SUBSCRIPTION_CONFLICT", "This durable subscription already uses a different pattern.")
                    await db.rollback()
                    return current
                cursor_sequence = await self._start_cursor(db, start, sequence)
                subscription_id = uuid7()
                created_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                await db.execute(
                    """
                    INSERT INTO subscriptions (
                        id, name, pattern, cursor_sequence, pending_delivery_id,
                        pending_message_id, pending_sequence, last_delivery_id,
                        gap_from, gap_to, created_at
                    ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?)
                    """,
                    (subscription_id, name, pattern, cursor_sequence, created_at),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        created = await self.get_subscription(name)
        if created is None:
            raise UsageError("The subscription was not stored.")
        return created

    async def get_subscription(self, name: str) -> Subscription | None:
        gate, db = self._require()
        async with gate:
            return await self._subscription_by_name(db, name)

    async def claim_next(self, name: str, *, resend: bool) -> PendingDelivery | None:
        """Claim the next matching event, or return the one already outstanding."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                current = await self._subscription_by_name(db, name)
                if current is None:
                    await db.rollback()
                    return None
                if current.pending_delivery_id is not None:
                    delivery = await self._pending_delivery(db, current)
                    if delivery is not None:
                        if resend:
                            await db.rollback()
                            return delivery
                        await db.rollback()
                        return None
                    await self._clear_pending(db, current)
                    current = await self._subscription_by_name(db, name)
                    if current is None:
                        await db.rollback()
                        return None
                delivery = await self._claim_new(db, current)
                if delivery is None:
                    await db.rollback()
                    return None
                await self._bump(db, "delivered_messages")
                await self._note(db, message_id=delivery.message_id, kind="event", action="deliver", subscription=current.name)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return delivery

    async def acknowledge(self, delivery_id: str) -> tuple[str, bool]:
        """Ack a delivery. The bool is false when the ack was already recorded."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute(
                    """
                    SELECT * FROM subscriptions
                    WHERE pending_delivery_id = ? OR last_delivery_id = ?
                    """,
                    (delivery_id, delivery_id),
                )
                rows = list(await cursor.fetchall())
                if not rows:
                    raise ProtocolError("DELIVERY_NOT_FOUND", "That delivery is not outstanding.")
                pending = [row for row in rows if row["pending_delivery_id"] == delivery_id]
                chosen = pending[0] if pending else rows[0]
                name = str(chosen["name"])
                if chosen["pending_delivery_id"] != delivery_id:
                    await db.rollback()
                    return name, False
                pending_sequence = chosen["pending_sequence"]
                if isinstance(pending_sequence, bool) or not isinstance(pending_sequence, int):
                    raise UsageError("The outstanding delivery has no sequence.")
                gap_from = _optional_int(chosen["gap_from"])
                gap_to = _optional_int(chosen["gap_to"])
                if gap_to is not None and pending_sequence >= gap_to:
                    gap_from = None
                    gap_to = None
                await db.execute(
                    """
                    UPDATE subscriptions SET
                        cursor_sequence = ?,
                        pending_delivery_id = NULL,
                        pending_message_id = NULL,
                        pending_sequence = NULL,
                        last_delivery_id = ?,
                        gap_from = ?,
                        gap_to = ?
                    WHERE name = ?
                    """,
                    (pending_sequence, delivery_id, gap_from, gap_to, name),
                )
                pending_message = chosen["pending_message_id"]
                await self._bump(db, "acknowledged_deliveries")
                await self._note(db, message_id="" if pending_message is None else str(pending_message), kind="event", action="ack", subscription=name)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return name, True

    async def apply_retention(self, *, max_messages: int, max_age_days: int) -> list[str]:
        """Delete a bounded batch of old events and record durable gaps."""
        gate, db = self._require()
        cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
        cutoff_text = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                counted = await db.execute("SELECT COUNT(*) FROM messages WHERE kind = 'event'")
                count_row = await counted.fetchone()
                total = 0 if count_row is None or count_row[0] is None else int(count_row[0])
                overflow = total - max_messages
                found = await db.execute(
                    """
                    SELECT id, sequence, destination, timestamp
                    FROM messages
                    WHERE kind = 'event'
                    ORDER BY sequence ASC
                    LIMIT ?
                    """,
                    (_RETENTION_BATCH,),
                )
                rows = list(await found.fetchall())
                doomed = [row for index, row in enumerate(rows) if index < overflow or str(row["timestamp"]) < cutoff_text]
                if not doomed:
                    await db.rollback()
                    return []
                subs = await self._subscription_rows(db)
                changed: set[str] = set()
                for message in doomed:
                    sequence = int(message["sequence"])
                    destination = str(message["destination"])
                    message_id = str(message["id"])
                    for sub in subs:
                        cursor_sequence = sub["cursor_sequence"]
                        if isinstance(cursor_sequence, bool) or not isinstance(cursor_sequence, int):
                            continue
                        if sequence <= cursor_sequence:
                            continue
                        if not matches(str(sub["pattern"]), destination):
                            continue
                        _extend_gap(sub, sequence)
                        if sub["pending_message_id"] == message_id:
                            sub["last_delivery_id"] = sub["pending_delivery_id"]
                            sub["pending_delivery_id"] = None
                            sub["pending_message_id"] = None
                            sub["pending_sequence"] = None
                        sub["dirty"] = True
                        changed.add(str(sub["name"]))
                    await db.execute("DELETE FROM messages WHERE id = ?", (message_id,))
                    await db.execute("DELETE FROM idempotency_keys WHERE message_id = ?", (message_id,))
                for sub in subs:
                    if sub.get("dirty") is True:
                        await self._update_subscription(db, sub)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return sorted(changed)

    async def enqueue_job(
        self, *, queue: str, data: object, headers: dict[str, object], content_type: str, idempotency_key: str | None, window_seconds: int, max_depth: int | None = None
    ) -> tuple[str, bool]:
        """Store a job. The bool is false when an idempotency key is replayed."""
        gate, db = self._require()
        digest = _job_fingerprint(queue, content_type, headers, data)
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                if idempotency_key:
                    existing = await self._replay(db, "enqueue", idempotency_key, digest, window_seconds)
                    if existing is not None:
                        await db.rollback()
                        return existing, False
                if max_depth is not None and await self._depth(db, queue) >= max_depth:
                    raise ProtocolError("CAPACITY", f"Queue {queue} is full.")
                job_id = uuid7()
                now = self._now()
                inserted = await db.execute(
                    """
                    INSERT INTO queue_jobs (
                        id, queue, original_queue, state, payload, headers, content_type,
                        attempts, lifetime_attempts, available_at, created_at
                    ) VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, 0, ?, ?)
                    """,
                    (job_id, queue, queue, json.dumps(data, separators=(",", ":"), sort_keys=True), json.dumps(headers, separators=(",", ":"), sort_keys=True), content_type, now, now),
                )
                await inserted.close()
                await self._bump(db, "enqueued_jobs")
                if idempotency_key:
                    await db.execute(
                        """
                        INSERT INTO idempotency_keys (
                            operation, key, fingerprint, message_id, created_at
                        ) VALUES ('enqueue', ?, ?, ?, ?)
                        """,
                        (idempotency_key, digest, job_id, now),
                    )
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return job_id, True

    async def reap_queue(self, queue: str, policy: AttemptPolicy) -> list[str]:
        """Release expired leases. Returns the delivery ids that were released."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                released = await self._reap_locked(db, queue, policy)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return released

    async def claim_job(self, queue: str, *, worker_id: str, policy: AttemptPolicy) -> JobLease | None:
        """Lease the next eligible job, after releasing expired leases on this queue."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await self._reap_locked(db, queue, policy)
                now = self._now()
                cursor = await db.execute(
                    """
                    SELECT * FROM queue_jobs
                    WHERE queue = ?
                      AND state IN ('pending', 'retry')
                      AND available_at <= ?
                    ORDER BY available_at ASC, created_at ASC, rowid ASC
                    LIMIT 1
                    """,
                    (queue, now),
                )
                row = await cursor.fetchone()
                await cursor.close()
                if row is None:
                    await db.rollback()
                    return None
                attempts = int(row["attempts"]) + 1
                lifetime = int(row["lifetime_attempts"]) + 1
                delivery_id = uuid7()
                lease_token = uuid7()
                expires = now + policy.visibility_timeout_seconds
                await db.execute(
                    """
                    UPDATE queue_jobs SET
                        state = 'leased',
                        attempts = ?,
                        lifetime_attempts = ?,
                        delivery_id = ?,
                        lease_token = ?,
                        lease_expires_at = ?,
                        worker_id = ?
                    WHERE id = ?
                    """,
                    (attempts, lifetime, delivery_id, lease_token, expires, worker_id, str(row["id"])),
                )
                await self._bump(db, "delivered_messages")
                await self._note(db, message_id=str(row["id"]), kind="job", action="deliver", queue=str(row["queue"]), worker_id=worker_id, attempt=attempts)
                leased = _job_lease(row, delivery_id, lease_token, attempts, lifetime)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return leased

    async def complete_job(self, delivery_id: str, lease_token: str, *, completed_max_age_days: int, dead_max_age_days: int) -> None:
        """Mark a leased job complete. A repeated ack of that lease is accepted."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                row = await self._job_by_delivery(db, delivery_id)
                self._require_current_lease(row, lease_token, allow_completed=True)
                if row is not None and str(row["state"]) != "completed":
                    await db.execute(
                        """
                        UPDATE queue_jobs SET state = 'completed', completed_at = ?
                        WHERE id = ?
                        """,
                        (self._now(), str(row["id"])),
                    )
                    await self._bump(db, "acknowledged_deliveries")
                    await self._note(db, message_id=str(row["id"]), kind="job", action="ack", queue=str(row["queue"]), worker_id=_optional_str(row["worker_id"]), attempt=int(row["attempts"]))
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        await self.retain_jobs(completed_max_age_days=completed_max_age_days, dead_max_age_days=dead_max_age_days)

    async def extend_job(self, delivery_id: str, lease_token: str, seconds: int) -> float:
        """Push a live lease forward. Returns the new expiry time."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                row = await self._job_by_delivery(db, delivery_id)
                self._require_current_lease(row, lease_token, allow_completed=False)
                if row is None:
                    raise ProtocolError("LEASE_EXPIRED", "The delivery lease is no longer valid.")
                expires = self._now() + seconds
                await db.execute("UPDATE queue_jobs SET lease_expires_at = ? WHERE id = ?", (expires, str(row["id"])))
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return expires

    async def fail_delivery(self, delivery_id: str, lease_token: str, *, reason: str, requeue: bool, policy: AttemptPolicy) -> str:
        """Retry or dead-letter a live lease. Returns 'retried' or 'dead'."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                row = await self._job_by_delivery(db, delivery_id)
                self._require_current_lease(row, lease_token, allow_completed=False)
                if row is None:
                    raise ProtocolError("LEASE_EXPIRED", "The delivery lease is no longer valid.")
                outcome = await self._apply_failure(db, row, reason=reason, requeue=requeue, policy=policy)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        return outcome

    async def release_lease(self, *, job_id: str, lease_token: str, policy: AttemptPolicy, reason: str) -> None:
        """Give up a lease because its worker disconnected."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cursor = await db.execute("SELECT * FROM queue_jobs WHERE id = ?", (job_id,))
                row = await cursor.fetchone()
                await cursor.close()
                if row is None or str(row["state"]) != "leased":
                    await db.rollback()
                    return
                if str(row["lease_token"]) != lease_token:
                    await db.rollback()
                    return
                await self._apply_failure(db, row, reason=reason, requeue=True, policy=policy)
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def retain_jobs(self, *, completed_max_age_days: int, dead_max_age_days: int) -> None:
        """Delete old completed and terminally dead jobs. Pending jobs stay."""
        gate, db = self._require()
        now = self._now()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await self._delete_jobs(db, "completed", "completed_at", now - completed_max_age_days * 86_400)
                await self._delete_jobs(db, "dead", "failed_at", now - dead_max_age_days * 86_400)
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def _delete_jobs(self, db: aiosqlite.Connection, state: str, column: str, cutoff: float) -> None:
        cursor = await db.execute(
            f"""
            SELECT id FROM queue_jobs
            WHERE state = ? AND {column} IS NOT NULL AND {column} <= ?
            ORDER BY {column} ASC
            LIMIT ?
            """,
            (state, cutoff, _RETENTION_BATCH),
        )
        rows = list(await cursor.fetchall())
        await cursor.close()
        for row in rows:
            job_id = str(row["id"])
            await db.execute("DELETE FROM queue_jobs WHERE id = ?", (job_id,))
            await db.execute("DELETE FROM idempotency_keys WHERE message_id = ?", (job_id,))

    async def _reap_locked(self, db: aiosqlite.Connection, queue: str, policy: AttemptPolicy) -> list[str]:
        now = self._now()
        cursor = await db.execute(
            """
            SELECT * FROM queue_jobs
            WHERE queue = ? AND state = 'leased' AND lease_expires_at <= ?
            """,
            (queue, now),
        )
        rows = list(await cursor.fetchall())
        await cursor.close()
        released: list[str] = []
        for row in rows:
            delivery_id = _optional_str(row["delivery_id"])
            if delivery_id is not None:
                released.append(delivery_id)
            await self._apply_failure(db, row, reason="The lease expired.", requeue=True, policy=policy)
        return released

    async def _apply_failure(self, db: aiosqlite.Connection, row: aiosqlite.Row, *, reason: str, requeue: bool, policy: AttemptPolicy) -> str:
        attempts = int(row["attempts"])
        exhausted = policy.max_attempts is not None and attempts >= policy.max_attempts
        now = self._now()
        job_id = str(row["id"])
        await self._bump(db, "negative_acknowledgements")
        await self._note(db, message_id=job_id, kind="job", action="nack", queue=str(row["queue"]), worker_id=_optional_str(row["worker_id"]), attempt=attempts, detail=reason)
        if requeue and not exhausted:
            delay = retry_delay(policy, attempts)
            await db.execute(
                """
                UPDATE queue_jobs SET
                    state = 'retry',
                    available_at = ?,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    delivery_id = NULL,
                    failure_reason = ?
                WHERE id = ?
                """,
                (now + delay, reason, job_id),
            )
            await self._bump(db, "retries")
            return "retried"
        if policy.dead_letter_queue:
            await db.execute(
                """
                UPDATE queue_jobs SET
                    queue = ?,
                    state = 'pending',
                    attempts = 0,
                    available_at = ?,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    delivery_id = NULL,
                    failure_reason = ?,
                    failed_at = ?
                WHERE id = ?
                """,
                (policy.dead_letter_queue, now, reason, now, job_id),
            )
            await self._bump(db, "dead_lettered_jobs")
            await self._note(db, message_id=job_id, kind="job", action="dead_letter", queue=policy.dead_letter_queue, attempt=attempts, detail=reason)
            return "dead"
        await db.execute(
            """
            UPDATE queue_jobs SET
                state = 'dead',
                lease_token = NULL,
                lease_expires_at = NULL,
                delivery_id = NULL,
                failure_reason = ?,
                failed_at = ?
            WHERE id = ?
            """,
            (reason, now, job_id),
        )
        await self._bump(db, "dead_lettered_jobs")
        await self._note(db, message_id=job_id, kind="job", action="dead_letter", queue=str(row["queue"]), attempt=attempts, detail=reason)
        return "dead"

    async def _job_by_delivery(self, db: aiosqlite.Connection, delivery_id: str) -> aiosqlite.Row | None:
        cursor = await db.execute("SELECT * FROM queue_jobs WHERE delivery_id = ?", (delivery_id,))
        row = await cursor.fetchone()
        await cursor.close()
        return row

    def _require_current_lease(self, row: aiosqlite.Row | None, lease_token: str, *, allow_completed: bool) -> None:
        if row is None or str(row["lease_token"]) != lease_token:
            raise ProtocolError("LEASE_EXPIRED", "The delivery lease is no longer valid.")
        state = str(row["state"])
        if state == "completed" and allow_completed:
            return
        if state != "leased":
            raise ProtocolError("LEASE_EXPIRED", "The delivery lease is no longer valid.")
        expires = row["lease_expires_at"]
        if expires is not None and float(expires) <= self._now():
            raise ProtocolError("LEASE_EXPIRED", "The delivery lease is no longer valid.")

    async def counters(self) -> dict[str, int]:
        """Return lifetime totals. Missing names are zero."""
        gate, db = self._require()
        found = dict.fromkeys(_COUNTER_NAMES, 0)
        async with gate:
            cursor = await db.execute("SELECT name, value FROM counters")
            rows = list(await cursor.fetchall())
            await cursor.close()
        for row in rows:
            found[str(row["name"])] = int(row["value"])
        return found

    async def record_delivery(
        self,
        *,
        message_id: str,
        kind: str,
        action: str,
        subscription: str | None = None,
        queue: str | None = None,
        worker_id: str | None = None,
        attempt: int | None = None,
        detail: str | None = None,
        counter: str | None = None,
    ) -> None:
        """Record one delivery outside an existing transaction."""
        gate, db = self._require()
        async with gate:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await self._note(db, message_id=message_id, kind=kind, action=action, subscription=subscription, queue=queue, worker_id=worker_id, attempt=attempt, detail=detail)
                if counter is not None:
                    await self._bump(db, counter)
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def _history_pattern(self, db: aiosqlite.Connection, pattern: str, limit: int) -> list[aiosqlite.Row]:
        collected: list[aiosqlite.Row] = []
        last: int | None = None
        while len(collected) < limit:
            if last is None:
                cursor = await db.execute(
                    """
                    SELECT * FROM messages
                    WHERE kind = 'event'
                    ORDER BY sequence DESC
                    LIMIT 200
                    """
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT * FROM messages
                    WHERE kind = 'event' AND sequence < ?
                    ORDER BY sequence DESC
                    LIMIT 200
                    """,
                    (last,),
                )
            rows = list(await cursor.fetchall())
            await cursor.close()
            if not rows:
                break
            for row in rows:
                last = int(row["sequence"])
                if matches(pattern, str(row["destination"])):
                    collected.append(row)
                    if len(collected) >= limit:
                        break
        return collected

    async def _depth(self, db: aiosqlite.Connection, queue: str) -> int:
        cursor = await db.execute(
            """
            SELECT COUNT(*) FROM queue_jobs
            WHERE queue = ? AND state IN ('pending', 'retry', 'leased')
            """,
            (queue,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    async def _bump(self, db: aiosqlite.Connection, name: str, amount: int = 1) -> None:
        cursor = await db.execute(
            """
            INSERT INTO counters (name, value) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET value = value + excluded.value
            """,
            (name, amount),
        )
        await cursor.close()

    async def _note(
        self,
        db: aiosqlite.Connection,
        *,
        message_id: str,
        kind: str,
        action: str,
        subscription: str | None = None,
        queue: str | None = None,
        worker_id: str | None = None,
        attempt: int | None = None,
        detail: str | None = None,
    ) -> None:
        cursor = await db.execute(
            """
            INSERT INTO deliveries (
                message_id, kind, action, subscription, queue, worker_id,
                attempt, detail, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (message_id, kind, action, subscription, queue, worker_id, attempt, detail, self._now()),
        )
        await cursor.close()

    async def _delivery_rows(self, db: aiosqlite.Connection, message_id: str) -> list[aiosqlite.Row]:
        cursor = await db.execute("SELECT * FROM deliveries WHERE message_id = ? ORDER BY id ASC", (message_id,))
        rows = list(await cursor.fetchall())
        await cursor.close()
        return rows

    async def _migrate(self) -> None:
        db = self._connection()
        await db.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL)")
        cursor = await db.execute("SELECT MAX(version) FROM schema_migrations")
        row = await cursor.fetchone()
        current = None if row is None or row[0] is None else int(row[0])
        if current is not None and current > SCHEMA_VERSION:
            raise UsageError(f"This database schema is version {current}, which is newer than this Relay.")
        if current is not None and current < SCHEMA_VERSION:
            if current < 2:
                await db.executescript(_CREATE_SUBSCRIPTIONS)
            if current < 3:
                await db.executescript(_CREATE_JOBS)
            if current < 4:
                await db.executescript(_CREATE_RUNTIME)
            if current < 5:
                await db.executescript(_CREATE_OBSERVABILITY)
            await db.execute("UPDATE schema_migrations SET version = ?", (SCHEMA_VERSION,))
            await db.commit()
            return
        if current is None:
            await db.executescript(_CREATE)
            await db.execute("INSERT INTO schema_migrations (version) VALUES (?)", (SCHEMA_VERSION,))
            await db.commit()

    async def _replay(self, db: aiosqlite.Connection, operation: str, key: str, digest: str, window_seconds: int) -> str | None:
        cursor = await db.execute(
            """
            SELECT fingerprint, message_id, created_at
            FROM idempotency_keys
            WHERE operation = ? AND key = ?
            """,
            (operation, key),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        age = self._now() - float(row["created_at"])
        if age > window_seconds:
            await db.execute("DELETE FROM idempotency_keys WHERE operation = ? AND key = ?", (operation, key))
            return None
        if str(row["fingerprint"]) != digest:
            raise ProtocolError("IDEMPOTENCY_CONFLICT", "This idempotency key was already used for a different message.")
        return str(row["message_id"])

    async def _subscription_by_name(self, db: aiosqlite.Connection, name: str) -> Subscription | None:
        cursor = await db.execute("SELECT * FROM subscriptions WHERE name = ?", (name,))
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return _subscription(row)

    async def _subscription_rows(self, db: aiosqlite.Connection) -> list[dict[str, object]]:
        cursor = await db.execute("SELECT * FROM subscriptions")
        rows = list(await cursor.fetchall())
        loaded: list[dict[str, object]] = []
        for row in rows:
            loaded.append(
                {
                    "name": str(row["name"]),
                    "cursor_sequence": int(row["cursor_sequence"]),
                    "pattern": str(row["pattern"]),
                    "pending_delivery_id": _optional_str(row["pending_delivery_id"]),
                    "pending_message_id": _optional_str(row["pending_message_id"]),
                    "pending_sequence": _optional_int(row["pending_sequence"]),
                    "last_delivery_id": _optional_str(row["last_delivery_id"]),
                    "gap_from": _optional_int(row["gap_from"]),
                    "gap_to": _optional_int(row["gap_to"]),
                    "dirty": False,
                }
            )
        return loaded

    async def _update_subscription(self, db: aiosqlite.Connection, sub: dict[str, object]) -> None:
        await db.execute(
            """
            UPDATE subscriptions SET
                cursor_sequence = ?,
                pending_delivery_id = ?,
                pending_message_id = ?,
                pending_sequence = ?,
                last_delivery_id = ?,
                gap_from = ?,
                gap_to = ?
            WHERE name = ?
            """,
            (sub["cursor_sequence"], sub["pending_delivery_id"], sub["pending_message_id"], sub["pending_sequence"], sub["last_delivery_id"], sub["gap_from"], sub["gap_to"], sub["name"]),
        )

    async def _pending_delivery(self, db: aiosqlite.Connection, current: Subscription) -> PendingDelivery | None:
        if current.pending_delivery_id is None or current.pending_message_id is None:
            return None
        found = await db.execute("SELECT * FROM messages WHERE id = ?", (current.pending_message_id,))
        row = await found.fetchone()
        if row is None:
            return None
        return _pending(row, current.pending_delivery_id)

    async def _clear_pending(self, db: aiosqlite.Connection, current: Subscription) -> None:
        await db.execute(
            """
            UPDATE subscriptions SET
                last_delivery_id = COALESCE(?, last_delivery_id),
                pending_delivery_id = NULL,
                pending_message_id = NULL,
                pending_sequence = NULL
            WHERE name = ?
            """,
            (current.pending_delivery_id, current.name),
        )

    async def _claim_new(self, db: aiosqlite.Connection, current: Subscription) -> PendingDelivery | None:
        message = await self._next_match(db, current.pattern, current.cursor_sequence)
        if message is None:
            return None
        delivery_id = uuid7()
        await db.execute(
            """
            UPDATE subscriptions SET
                pending_delivery_id = ?,
                pending_message_id = ?,
                pending_sequence = ?
            WHERE name = ?
            """,
            (delivery_id, str(message["id"]), int(message["sequence"]), current.name),
        )
        return _pending(message, delivery_id)

    async def _next_match(self, db: aiosqlite.Connection, pattern: str, cursor_sequence: int) -> aiosqlite.Row | None:
        last = cursor_sequence
        while True:
            found = await db.execute(
                """
                SELECT * FROM messages
                WHERE sequence > ? AND kind = 'event'
                ORDER BY sequence ASC
                LIMIT 50
                """,
                (last,),
            )
            rows = list(await found.fetchall())
            if not rows:
                return None
            for row in rows:
                last = int(row["sequence"])
                if matches(pattern, str(row["destination"])):
                    return row

    async def _start_cursor(self, db: aiosqlite.Connection, start: str, sequence: int | None) -> int:
        if start == "earliest":
            return 0
        if start == "sequence":
            if sequence is None or sequence < 1:
                raise ProtocolError("INVALID_MESSAGE", "sequence must be a positive integer.")
            return sequence - 1
        if start != "latest":
            raise ProtocolError("INVALID_MESSAGE", "start must be latest, earliest, or sequence.")
        cursor = await db.execute("SELECT COALESCE(MAX(sequence), 0) FROM messages")
        row = await cursor.fetchone()
        await cursor.close()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    def _require(self) -> tuple[asyncio.Lock, aiosqlite.Connection]:
        if self._gate is None or self._db is None:
            raise UsageError("The database is not open.")
        return self._gate, self._db

    def _connection(self) -> aiosqlite.Connection:
        if self._db is None:
            raise UsageError("The database is not open.")
        return self._db


def _extend_gap(sub: dict[str, object], sequence: int) -> None:
    gap_from = _optional_int(sub.get("gap_from"))
    gap_to = _optional_int(sub.get("gap_to"))
    sub["gap_from"] = sequence if gap_from is None else min(gap_from, sequence)
    sub["gap_to"] = sequence if gap_to is None else max(gap_to, sequence)


def _subscription(row: aiosqlite.Row) -> Subscription:
    return Subscription(
        id=str(row["id"]),
        name=str(row["name"]),
        pattern=str(row["pattern"]),
        cursor_sequence=int(row["cursor_sequence"]),
        pending_delivery_id=_optional_str(row["pending_delivery_id"]),
        pending_message_id=_optional_str(row["pending_message_id"]),
        pending_sequence=_optional_int(row["pending_sequence"]),
        last_delivery_id=_optional_str(row["last_delivery_id"]),
        gap_from=_optional_int(row["gap_from"]),
        gap_to=_optional_int(row["gap_to"]),
    )


def _pending(row: aiosqlite.Row, delivery_id: str) -> PendingDelivery:
    headers = json.loads(str(row["headers"]))
    data = json.loads(str(row["data"]))
    return PendingDelivery(
        delivery_id=delivery_id, message_id=str(row["id"]), sequence=int(row["sequence"]), destination=str(row["destination"]), data=data, headers=headers, content_type=str(row["content_type"])
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise UsageError("The subscription record is corrupt.")
    return value


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _job_fingerprint(queue: str, content_type: str, headers: dict[str, object], data: object) -> str:
    blob = json.dumps(
        {
            "content_type": content_type,
            "data": data,
            "headers": headers,
            "queue": queue,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode()).hexdigest()


def _job_lease(row: aiosqlite.Row, delivery_id: str, lease_token: str, attempts: int, lifetime: int) -> JobLease:
    return JobLease(
        id=str(row["id"]),
        queue=str(row["queue"]),
        original_queue=str(row["original_queue"]),
        delivery_id=delivery_id,
        lease_token=lease_token,
        attempts=attempts,
        lifetime_attempts=lifetime,
        data=json.loads(str(row["payload"])),
        headers=json.loads(str(row["headers"])),
        content_type=str(row["content_type"]),
        failure_reason=_optional_str(row["failure_reason"]),
    )


def _fingerprint(topic: str, content_type: str, headers: dict[str, object], data: object) -> str:
    blob = json.dumps(
        {
            "content_type": content_type,
            "data": data,
            "headers": headers,
            "topic": topic,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode()).hexdigest()


def _storage_bytes(path: Path) -> int:
    total = 0
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix) if suffix else path
        if candidate.is_file():
            total += candidate.stat().st_size
    return total


def _runtime_queue(row: aiosqlite.Row) -> dict[str, object]:
    attempts = row["max_attempts"]
    return {
        "name": str(row["name"]),
        "max_attempts": None if attempts is None else int(attempts),
        "visibility_timeout_seconds": int(row["visibility_timeout_seconds"]),
        "dead_letter_queue": _optional_str(row["dead_letter_queue"]),
    }


def _delivery_row(row: aiosqlite.Row) -> dict[str, object]:
    attempt = row["attempt"]
    return {
        "action": str(row["action"]),
        "kind": str(row["kind"]),
        "subscription": _optional_str(row["subscription"]),
        "queue": _optional_str(row["queue"]),
        "worker_id": _optional_str(row["worker_id"]),
        "attempt": None if attempt is None else int(attempt),
        "detail": _optional_str(row["detail"]),
        "created_at": float(row["created_at"]),
    }


def _job_row(row: aiosqlite.Row) -> dict[str, object]:
    failed = row["failed_at"]
    return {
        "id": str(row["id"]),
        "queue": str(row["queue"]),
        "original_queue": str(row["original_queue"]),
        "state": str(row["state"]),
        "attempts": int(row["attempts"]),
        "lifetime_attempts": int(row["lifetime_attempts"]),
        "failure_reason": _optional_str(row["failure_reason"]),
        "failed_at": None if failed is None else float(failed),
        "worker_id": _optional_str(row["worker_id"]),
        "content_type": str(row["content_type"]),
        "headers": json.loads(str(row["headers"])),
        "data": json.loads(str(row["payload"])),
    }


def _envelope(row: aiosqlite.Row) -> dict[str, object]:
    return {
        "id": str(row["id"]),
        "kind": str(row["kind"]),
        "destination": str(row["destination"]),
        "sequence": int(row["sequence"]),
        "timestamp": str(row["timestamp"]),
        "headers": json.loads(str(row["headers"])),
        "content_type": str(row["content_type"]),
        "data": json.loads(str(row["data"])),
    }
