"""Destination names are exact, case-sensitive, and restricted."""

from __future__ import annotations

import pytest

from lupaxa.relay.exceptions import UsageError
from lupaxa.relay.names import validate_destination


@pytest.mark.parametrize("name", ["build.completed", "repository.updated", "image.resize", "A", "job_1.part-2"])
def test_ordinary_names_are_accepted(name: str) -> None:
    assert validate_destination(name) == name


@pytest.mark.parametrize("name", ["", "build.", ".build", "build..completed", "build.*", "build.**", "has space", "a/b"])
def test_malformed_names_are_rejected(name: str) -> None:
    with pytest.raises(UsageError):
        validate_destination(name)


def test_names_are_case_sensitive() -> None:
    assert validate_destination("Build.Completed") == "Build.Completed"


def test_system_prefix_is_reserved() -> None:
    with pytest.raises(UsageError, match="_relay"):
        validate_destination("_relay.internal")


def test_name_length_is_bounded() -> None:
    with pytest.raises(UsageError):
        validate_destination("a" * 129)
