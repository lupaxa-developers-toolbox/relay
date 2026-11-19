"""The text interface reads the broker and clears itself when the connection drops."""

from __future__ import annotations

from pathlib import Path

import pytest

from lupaxa.relay.client import RelayClient
from tests.conftest import RunningBroker
from tests.test_admin import _config

pytest.importorskip("textual")
from textual.widgets import DataTable, Static  # noqa: E402

from lupaxa.relay.tui import build_app  # noqa: E402


@pytest.mark.asyncio
async def test_overview_shows_a_queue_and_clears_it_when_disconnected(workspace: Path) -> None:
    running = RunningBroker(_config(workspace))
    try:
        with RelayClient(socket=running.socket) as client:
            client.enqueue("image.resize", {"n": 1})
        app = build_app(running.socket)
        async with app.run_test() as pilot:
            await pilot.pause()
            summary = app.query_one("#summary", Static)
            assert "Connected" in str(summary.render())
            queues = app.query_one("#queues", DataTable)
            assert queues.row_count >= 1
            running.stop()
            await pilot.press("r")
            await pilot.pause()
            assert "Disconnected" in str(summary.render())
            assert queues.row_count == 0
            assert app.query_one("#events", DataTable).row_count == 0
            assert app.query_one("#dead", DataTable).row_count == 0
    finally:
        running.stop()


@pytest.mark.asyncio
async def test_dead_letter_actions_wait_for_confirmation(workspace: Path) -> None:
    from tests.test_queues import _worker

    running = RunningBroker(_config(workspace))
    try:
        with RelayClient(socket=running.socket) as client:
            client.publish("build.completed", {"n": 1})
            client.enqueue("once", {"n": 1})
        with _worker(running.socket, "once") as worker:
            worker.take().nack()
        app = build_app(running.socket)
        async with app.run_test() as pilot:
            await pilot.pause()
            assert app.query_one("#events", DataTable).row_count >= 1
            assert app.query_one("#subscriptions", DataTable).row_count == 0
            dead = app.query_one("#dead", DataTable)
            assert dead.row_count >= 1
            await pilot.press("d")
            await pilot.pause()
            assert "deliveries" in str(app.query_one("#detail", Static).render())
            await pilot.press("y")
            assert "Retry" in str(app.query_one("#confirm", Static).render())
            await pilot.press("n")
            assert app.pending is None
            await pilot.press("y")
            await pilot.press("y")
            await pilot.pause()
            assert app.query_one("#dead", DataTable).row_count == 0
    finally:
        running.stop()
