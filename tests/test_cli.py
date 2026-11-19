"""The CLI reports its version and rejects unknown flags."""

from __future__ import annotations

import json

from pytest import CaptureFixture

from lupaxa.relay import __version__
from lupaxa.relay.cli import main


def test_version_flag(capsys: CaptureFixture[str]) -> None:
    assert main(["--version"]) == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == __version__
    assert "Traceback" not in captured.err


def test_unknown_flag_is_a_usage_error(capsys: CaptureFixture[str]) -> None:
    assert main(["--not-a-command"]) == 2
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert "Traceback" not in captured.out


def test_publish_command_prints_the_assigned_id(running, capsys: CaptureFixture[str]) -> None:
    code = main(
        [
            "--socket",
            str(running.socket),
            "publish",
            "build.completed",
            "--json",
            '{"version":"1.2.0"}',
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["sequence"] == 1
    assert payload["message_id"]
