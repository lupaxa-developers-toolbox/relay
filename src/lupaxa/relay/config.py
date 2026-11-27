"""YAML configuration for a local Relay broker."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import platformdirs
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from lupaxa.relay.exceptions import UsageError
from lupaxa.relay.names import validate_destination

_MISSING = object()
_LOOPBACK = {"127.0.0.1", "::1", "localhost"}


class StrictModel(BaseModel):
    """Reject keys the schema does not define."""

    model_config = ConfigDict(extra="forbid")


class BrokerSpec(StrictModel):
    name: str = "local-relay"


class TcpSpec(StrictModel):
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=8765, ge=0, le=65535)


class ServerSpec(StrictModel):
    transport: Literal["unix", "tcp"] = "unix"
    socket: str = "auto"
    tcp: TcpSpec = TcpSpec()
    max_connections: int = Field(default=100, gt=0)
    max_frame_kb: int = Field(default=512, gt=0)
    idle_timeout_seconds: int = Field(default=60, gt=0)


class EventRetention(StrictModel):
    max_age_days: int = Field(default=14, ge=0)
    max_messages: int = Field(default=100_000, ge=0)


class AgeRetention(StrictModel):
    max_age_days: int = Field(default=7, ge=0)


class DeadLetterRetention(StrictModel):
    max_age_days: int = Field(default=30, ge=0)


class RetentionSpec(StrictModel):
    events: EventRetention = EventRetention()
    completed_jobs: AgeRetention = AgeRetention()
    dead_letters: DeadLetterRetention = DeadLetterRetention()


class StorageSpec(StrictModel):
    backend: Literal["sqlite"] = "sqlite"
    path: str = "auto"
    durability: Literal["full", "normal"] = "full"
    retention: RetentionSpec = RetentionSpec()


class PubSubSpec(StrictModel):
    default_start_position: Literal["latest", "earliest", "sequence"] = "latest"
    max_unacknowledged: int = Field(default=1, gt=0)


class ConsumerSpec(StrictModel):
    default_prefetch: int = Field(default=1, gt=0)
    max_prefetch: int = Field(default=100, gt=0)


class MessagingSpec(StrictModel):
    max_payload_kb: int = Field(default=256, gt=0)
    max_headers_kb: int = Field(default=16, gt=0)
    idempotency_window_seconds: int = Field(default=86_400, ge=0)
    pubsub: PubSubSpec = PubSubSpec()


class RetrySpec(StrictModel):
    strategy: Literal["immediate", "fixed", "linear", "exponential"] = "immediate"
    initial_delay_seconds: float = Field(default=0, ge=0)
    multiplier: float = Field(default=2, gt=0)
    max_delay_seconds: float = Field(default=300, ge=0)
    increment_seconds: float = Field(default=0, ge=0)


class QueueSpec(StrictModel):
    name: str
    durable: bool = True
    max_attempts: int | None = 5
    visibility_timeout_seconds: int = Field(default=60, gt=0)
    retry: RetrySpec = RetrySpec()
    dead_letter_queue: str | None = None
    max_depth: int | None = Field(default=None, gt=0)


class BindingSpec(StrictModel):
    id: str
    topic: str
    queue: str


class LoggingSpec(StrictModel):
    level: Literal["debug", "info", "warning", "error", "critical"] = "info"
    format: Literal["text", "json"] = "text"


class SecuritySpec(StrictModel):
    allow_remote: bool = False
    token: str | None = None


class RelayConfig(StrictModel):
    """The resolved broker configuration."""

    version: Literal[1] = 1
    broker: BrokerSpec = BrokerSpec()
    server: ServerSpec = ServerSpec()
    storage: StorageSpec = StorageSpec()
    messaging: MessagingSpec = MessagingSpec()
    consumers: ConsumerSpec = ConsumerSpec()
    queues: list[QueueSpec] = []
    bindings: list[BindingSpec] = []
    logging: LoggingSpec = LoggingSpec()
    security: SecuritySpec = SecuritySpec()

    @model_validator(mode="after")
    def _check(self) -> RelayConfig:
        if self.security.allow_remote:
            raise ValueError("Relay does not allow remote access.")
        if self.security.token is not None and not self.security.token:
            raise ValueError("The authentication token must not be empty.")
        tcp = self.server.tcp
        if tcp.enabled and tcp.host not in _LOOPBACK:
            raise ValueError("TCP must bind to a loopback address.")
        if self.server.transport == "tcp" and not tcp.enabled:
            raise ValueError("TCP transport requires server.tcp.enabled.")
        if self.consumers.default_prefetch > self.consumers.max_prefetch:
            raise ValueError("default_prefetch cannot exceed max_prefetch.")
        if self.server.socket != "auto" and not self.server.socket.strip():
            raise ValueError("The socket path must not be empty.")
        if self.storage.path != "auto" and not self.storage.path.strip():
            raise ValueError("The database path must not be empty.")
        names: list[str] = []
        for queue in self.queues:
            names.append(validate_destination(queue.name))
            if queue.max_attempts is not None and queue.max_attempts < 1:
                raise ValueError("max_attempts must be at least 1, or null when unlimited.")
            if queue.retry.strategy != "immediate" and queue.retry.initial_delay_seconds <= 0:
                raise ValueError("A delayed retry needs an initial delay greater than zero.")
            if queue.dead_letter_queue is not None:
                validate_destination(queue.dead_letter_queue)
                if queue.dead_letter_queue == queue.name:
                    raise ValueError("A queue cannot be its own dead-letter queue.")
        if len(names) != len(set(names)):
            raise ValueError("Queue names must be unique.")
        known = set(names)
        for queue in self.queues:
            if queue.dead_letter_queue is not None and queue.dead_letter_queue not in known:
                raise ValueError(f"Dead-letter queue is not defined: {queue.dead_letter_queue}")
        seen_ids: set[str] = set()
        seen_routes: set[tuple[str, str]] = set()
        for binding in self.bindings:
            if not binding.id or binding.id in seen_ids:
                raise ValueError("Binding ids must be unique.")
            seen_ids.add(binding.id)
            topic = validate_destination(binding.topic)
            queue_name = validate_destination(binding.queue)
            if queue_name not in known:
                raise ValueError(f"Binding queue is not defined: {queue_name}")
            route = (topic, queue_name)
            if route in seen_routes:
                raise ValueError(f"Duplicate binding for {topic} and {queue_name}.")
            seen_routes.add(route)
        return self

    def database_path(self) -> Path:
        if self.storage.path == "auto":
            return default_data_dir() / "relay.sqlite3"
        return Path(self.storage.path)

    def socket_path(self) -> Path | None:
        if self.server.transport == "tcp":
            return None
        if self.server.socket == "auto":
            return default_socket_path()
        return Path(self.server.socket)


def default_config_path() -> Path:
    """Return the user configuration file, honouring ``LUPAXA_RELAY_CONFIG``."""
    override = os.environ.get("LUPAXA_RELAY_CONFIG")
    if override:
        return Path(override)
    return Path(platformdirs.user_config_dir("lupaxa-relay", appauthor=False)) / "relay.yml"


def default_data_dir() -> Path:
    """Return the data directory, honouring ``LUPAXA_RELAY_DATA``."""
    override = os.environ.get("LUPAXA_RELAY_DATA")
    if override:
        return Path(override)
    return Path(platformdirs.user_data_dir("lupaxa-relay", appauthor=False))


def default_socket_path() -> Path:
    """Return the default Unix socket, honouring ``LUPAXA_RELAY_SOCKET``."""
    override = os.environ.get("LUPAXA_RELAY_SOCKET")
    if override:
        return Path(override)
    runtime = Path(platformdirs.user_runtime_dir("lupaxa-relay", appauthor=False))
    return runtime / "relay.sock"


def load_config(path: Path | None = None, *, user_path: Path | None | object = _MISSING, max_payload_kb: int | None = None, socket: str | None = None) -> RelayConfig:
    """Load defaults, then the user file, then an explicit file, then CLI overrides."""
    merged: dict[str, object] = {"version": 1}
    chosen_user = default_config_path() if user_path is _MISSING else user_path
    if isinstance(chosen_user, Path) and chosen_user.is_file():
        merged = _merge(merged, _read_yaml(chosen_user))
    if path is not None:
        merged = _merge(merged, _read_yaml(path))
    if max_payload_kb is not None or socket is not None:
        messaging = _as_dict(merged.get("messaging"))
        server = _as_dict(merged.get("server"))
        if max_payload_kb is not None:
            messaging["max_payload_kb"] = max_payload_kb
            merged["messaging"] = messaging
        if socket is not None:
            server["socket"] = socket
            merged["server"] = server
    try:
        return RelayConfig.model_validate(merged)
    except ValidationError as exc:
        raise UsageError(_format_validation(exc)) from exc
    except UsageError:
        raise
    except ValueError as exc:
        raise UsageError(str(exc)) from exc


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _read_yaml(path: Path) -> dict[str, object]:
    try:
        loaded = yaml.safe_load(path.read_text())
    except OSError as exc:
        raise UsageError(f"Could not read configuration: {path}") from exc
    except yaml.YAMLError as exc:
        raise UsageError(f"Invalid YAML in {path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise UsageError(f"Configuration must be a mapping: {path}")
    return loaded


def _merge(base: dict[str, object], overlay: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _merge(current, value)
        else:
            merged[key] = value
    return merged


def _format_validation(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        loc = ".".join(str(item) for item in error["loc"])
        message = error["msg"]
        parts.append(f"{loc}: {message}" if loc else message)
    return "Invalid configuration: " + "; ".join(parts)
