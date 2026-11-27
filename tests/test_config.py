"""Configuration is validated before a broker can start."""

from __future__ import annotations

from pathlib import Path

import pytest

from lupaxa.relay.config import load_config
from lupaxa.relay.exceptions import UsageError


def test_missing_file_uses_defaults(tmp_path: Path) -> None:
    config = load_config(None, user_path=tmp_path / "absent.yml")
    assert config.version == 1
    assert config.server.transport == "unix"
    assert config.server.tcp.enabled is False
    assert config.messaging.max_payload_kb == 256
    assert config.storage.durability == "full"


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text("version: 1\nbroker:\n  nickname: local\n")
    with pytest.raises(UsageError, match="nickname"):
        load_config(path, user_path=None)


def test_remote_tcp_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text("version: 1\nserver:\n  tcp:\n    enabled: true\n    host: 0.0.0.0\n    port: 8765\n")
    with pytest.raises(UsageError, match="loopback"):
        load_config(path, user_path=None)


def test_allow_remote_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text("version: 1\nsecurity:\n  allow_remote: true\n")
    with pytest.raises(UsageError, match="remote"):
        load_config(path, user_path=None)


def test_binding_queue_must_exist(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text("version: 1\nqueues:\n  - name: image.resize\nbindings:\n  - id: deploy\n    topic: build.completed\n    queue: deployment.jobs\n")
    with pytest.raises(UsageError, match="deployment.jobs"):
        load_config(path, user_path=None)


def test_duplicate_binding_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text(
        "version: 1\nqueues:\n  - name: deployment.jobs\n"
        "bindings:\n"
        "  - id: one\n    topic: build.completed\n    queue: deployment.jobs\n"
        "  - id: two\n    topic: build.completed\n    queue: deployment.jobs\n"
    )
    with pytest.raises(UsageError, match="Duplicate binding"):
        load_config(path, user_path=None)


def test_dead_letter_queue_must_exist(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text("version: 1\nqueues:\n  - name: image.resize\n    dead_letter_queue: image.resize.failed\n")
    with pytest.raises(UsageError, match="image.resize.failed"):
        load_config(path, user_path=None)


def test_explicit_file_overrides_user_file(tmp_path: Path) -> None:
    user = tmp_path / "user.yml"
    explicit = tmp_path / "explicit.yml"
    user.write_text("version: 1\nmessaging:\n  max_payload_kb: 10\n  max_headers_kb: 4\n")
    explicit.write_text("version: 1\nmessaging:\n  max_payload_kb: 2\n")
    config = load_config(explicit, user_path=user)
    assert config.messaging.max_payload_kb == 2
    assert config.messaging.max_headers_kb == 4


def test_cli_override_wins(tmp_path: Path) -> None:
    path = tmp_path / "relay.yml"
    path.write_text("version: 1\nmessaging:\n  max_payload_kb: 10\n")
    config = load_config(path, user_path=None, max_payload_kb=3)
    assert config.messaging.max_payload_kb == 3
