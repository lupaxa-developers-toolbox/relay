"""Optional text interface. It reads the broker through the public protocol."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lupaxa.relay.client import RelayClient
from lupaxa.relay.exceptions import ConnectionFailed, ProtocolError, RelayError


def run(socket: Path) -> None:
    """Open the management interface against a running broker."""
    try:
        app: Any = build_app(socket)
    except ImportError as exc:
        raise RelayError(
            "Install the tui extra to use the text interface: pip install lupaxa-relay[tui]",
            exit_code=2,
        ) from exc
    app.run()


def build_app(socket: Path) -> Any:
    """Return the text interface for this socket. Imports Textual."""
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical
    from textual.widgets import DataTable, Footer, Header, Static

    class Overview(App[None]):
        """Broker overview, events, subscriptions, and dead-letter actions."""

        BINDINGS = [
            Binding("q", "quit", "Quit"),
            Binding("r", "refresh", "Refresh"),
            Binding("y", "retry", "Retry"),
            Binding("p", "purge", "Purge"),
            Binding("n", "cancel", "Cancel"),
            Binding("d", "details", "Details"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self.socket_path = socket
            self.title = "Relay"
            self.pending: tuple[str, str] | None = None
            self.event_ids: list[str] = []
            self.dead_jobs: list[tuple[str, str]] = []

        def compose(self) -> ComposeResult:
            yield Header()
            with Vertical():
                yield Static("Connecting", id="summary")
                yield Static("", id="confirm")
                yield Static("", id="detail")
                yield DataTable(id="queues")
                yield DataTable(id="topics")
                yield DataTable(id="events")
                yield DataTable(id="subscriptions")
                yield DataTable(id="dead")
            yield Footer()

        def on_mount(self) -> None:
            self.query_one("#queues", DataTable).add_columns(
                "Queue",
                "Pending",
                "Leased",
                "Retry",
                "Dead",
                "Workers",
            )
            self.query_one("#topics", DataTable).add_columns("Topic", "Messages")
            self.query_one("#events", DataTable).add_columns("Topic", "Id")
            self.query_one("#subscriptions", DataTable).add_columns("Name", "Pattern", "Cursor")
            self.query_one("#dead", DataTable).add_columns("Queue", "Id", "Reason")
            self.set_interval(1, self.action_refresh)
            self.action_refresh()

        def action_refresh(self) -> None:
            summary = self.query_one("#summary", Static)
            confirm = self.query_one("#confirm", Static)
            detail = self.query_one("#detail", Static)
            queues = self.query_one("#queues", DataTable)
            topics = self.query_one("#topics", DataTable)
            events = self.query_one("#events", DataTable)
            subscriptions = self.query_one("#subscriptions", DataTable)
            dead = self.query_one("#dead", DataTable)
            for table in (queues, topics, events, subscriptions, dead):
                table.clear()
            self.event_ids = []
            self.dead_jobs = []
            try:
                with RelayClient(socket=self.socket_path, timeout=1) as client:
                    status = client.status()
                    history = client.history(limit=20)
                    dead_rows = _dead_jobs(client, status)
            except (ConnectionFailed, ProtocolError, OSError):
                summary.update("Disconnected")
                confirm.update("")
                detail.update("")
                self.pending = None
                return
            messages = status.get("messages", 0)
            jobs = status.get("jobs", 0)
            connections = status.get("connections", 0)
            summary.update(f"Connected  messages {messages}  jobs {jobs}  clients {connections}")
            if self.pending is None:
                confirm.update("")
            _fill_queues(queues, status)
            _fill_topics(topics, status)
            for item in history:
                event_id = str(item.get("id", ""))
                self.event_ids.append(event_id)
                events.add_row(str(item.get("destination", "")), event_id)
            listed = status.get("subscriptions")
            if isinstance(listed, list):
                for item in listed:
                    if not isinstance(item, dict):
                        continue
                    subscriptions.add_row(
                        str(item.get("name", "")),
                        str(item.get("pattern", "")),
                        str(item.get("cursor_sequence", "")),
                    )
            for queue, job_id, reason in dead_rows:
                self.dead_jobs.append((queue, job_id))
                dead.add_row(queue, job_id, reason)

        def action_details(self) -> None:
            if not self.event_ids:
                return
            events = self.query_one("#events", DataTable)
            index = events.cursor_row
            if index < 0 or index >= len(self.event_ids):
                index = 0
            message_id = self.event_ids[index]
            detail = self.query_one("#detail", Static)
            try:
                with RelayClient(socket=self.socket_path, timeout=1) as client:
                    payload = client.message(message_id)
            except (ConnectionFailed, ProtocolError, OSError):
                self.action_refresh()
                return
            detail.update(json.dumps(payload))

        def action_cancel(self) -> None:
            self.pending = None
            self.query_one("#confirm", Static).update("")

        def action_retry(self) -> None:
            self._confirm_action("retry")

        def action_purge(self) -> None:
            self._confirm_action("purge")

        def _confirm_action(self, kind: str) -> None:
            confirm = self.query_one("#confirm", Static)
            if self.pending is not None and self.pending[0] == kind:
                target = self.pending[1]
                self.pending = None
                try:
                    with RelayClient(socket=self.socket_path, timeout=1) as client:
                        if kind == "retry":
                            client.retry_job(target)
                        else:
                            client.purge(target)
                except (ConnectionFailed, ProtocolError, OSError):
                    self.action_refresh()
                    return
                confirm.update("")
                self.action_refresh()
                return
            if kind == "retry":
                if not self.dead_jobs:
                    confirm.update("There is no dead-lettered job.")
                    return
                _queue, job_id = self.dead_jobs[0]
                self.pending = ("retry", job_id)
                confirm.update(f"Retry {job_id}? Press y to confirm or n to cancel.")
                return
            if not self.dead_jobs:
                confirm.update("There is no dead-letter queue to purge.")
                return
            queue = self.dead_jobs[0][0]
            self.pending = ("purge", queue)
            confirm.update(f"Purge {queue}? Press p to confirm or n to cancel.")

    return Overview()


def _fill_queues(table: Any, status: dict[str, object]) -> None:
    listed = status.get("queues")
    if not isinstance(listed, list):
        return
    for item in listed:
        if not isinstance(item, dict):
            continue
        table.add_row(
            str(item.get("name", "")),
            str(item.get("pending", 0)),
            str(item.get("leased", 0)),
            str(item.get("retry", 0)),
            str(item.get("dead", 0)),
            str(item.get("workers", 0)),
        )


def _fill_topics(table: Any, status: dict[str, object]) -> None:
    listed = status.get("topics")
    if not isinstance(listed, list):
        return
    for item in listed:
        if not isinstance(item, dict):
            continue
        table.add_row(str(item.get("name", "")), str(item.get("messages", 0)))


def _dead_jobs(
    client: RelayClient,
    status: dict[str, object],
) -> list[tuple[str, str, str]]:
    names = status.get("queues")
    if not isinstance(names, list):
        return []
    rows: list[tuple[str, str, str]] = []
    for item in names:
        if not isinstance(item, dict):
            continue
        queue = item.get("name")
        if not isinstance(queue, str):
            continue
        for job in client.inspect(queue, limit=50):
            state = job.get("state")
            original = job.get("original_queue")
            if state == "dead" or (isinstance(original, str) and original != queue):
                reason = job.get("failure_reason")
                rows.append((queue, str(job.get("id", "")), "" if reason is None else str(reason)))
    return rows
