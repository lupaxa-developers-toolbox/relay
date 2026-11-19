"""Start and stop a background broker process."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from lupaxa.relay.client import RelayClient
from lupaxa.relay.config import RelayConfig
from lupaxa.relay.exceptions import ConnectionFailed, RelayError


def pid_file(config: RelayConfig) -> Path:
    """Return the pid file that sits beside the database."""
    return Path(str(config.database_path()) + ".pid")


def start(config: RelayConfig, *, config_path: Path | None, socket: Path) -> int:
    """Start `relay serve` in a new session. Returns the process id."""
    path = pid_file(config)
    if _alive(path):
        raise RelayError("Relay is already running.", exit_code=4)
    path.unlink(missing_ok=True)
    command = [sys.executable, "-m", "lupaxa.relay"]
    if config_path is not None:
        command.extend(["--config", str(config_path)])
    command.extend(["--socket", str(socket), "serve"])
    log_path = Path(str(config.database_path()) + ".log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        process = subprocess.Popen(
            command,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RelayError("Relay exited during start.", exit_code=1)
        try:
            with RelayClient(socket=socket, timeout=0.3) as client:
                client.status()
            break
        except ConnectionFailed:
            time.sleep(0.05)
    else:
        process.terminate()
        raise RelayError("Relay did not start.", exit_code=1)
    path.write_text(f"{process.pid}\n")
    return process.pid


def stop(config: RelayConfig) -> None:
    """Ask the background broker to shut down."""
    path = pid_file(config)
    pid = _read_pid(path)
    if pid is None or not _process_alive(pid):
        path.unlink(missing_ok=True)
        raise RelayError("Relay is not running.", exit_code=1)
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not _process_alive(pid):
            path.unlink(missing_ok=True)
            return
        time.sleep(0.05)
    raise RelayError("Relay did not stop.", exit_code=1)


def describe(
    config: RelayConfig,
    *,
    socket: Path | None,
    timeout: float = 0.5,
) -> dict[str, object]:
    """Report whether the background process and the broker socket are up."""
    pid = _read_pid(pid_file(config))
    alive = pid is not None and _process_alive(pid)
    payload: dict[str, object] = {"pid": pid, "process": alive, "reachable": False}
    if socket is None:
        return payload
    try:
        with RelayClient(socket=socket, timeout=timeout) as client:
            status = client.status()
    except ConnectionFailed:
        return payload
    payload["reachable"] = True
    for key in ("messages", "jobs", "connections", "uptime_seconds", "healthy"):
        if key in status:
            payload[key] = status[key]
    return payload


def _alive(path: Path) -> bool:
    pid = _read_pid(path)
    return pid is not None and _process_alive(pid)


def _read_pid(path: Path) -> int | None:
    if not path.is_file():
        return None
    text = path.read_text().strip()
    if not text.isdigit():
        return None
    return int(text)


def _process_alive(pid: int) -> bool:
    try:
        reaped, _status = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        reaped = 0
    except OSError:
        return False
    if reaped:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True
