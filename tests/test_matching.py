"""Topic patterns match independently of the broker."""

from __future__ import annotations

import pytest

from lupaxa.relay.exceptions import UsageError
from lupaxa.relay.matching import matches, validate_pattern


@pytest.mark.parametrize(
    ("pattern", "topic"),
    [
        ("build.completed", "build.completed"),
        ("build.*", "build.completed"),
        ("build.*", "build.started"),
        ("build.**", "build"),
        ("build.**", "build.completed"),
        ("build.**", "build.completed.now"),
        ("**", "build.completed"),
        ("**", "image"),
        ("a.*.c", "a.b.c"),
        ("*", "image"),
    ],
)
def test_pattern_matches(pattern: str, topic: str) -> None:
    assert matches(pattern, topic)


@pytest.mark.parametrize(
    ("pattern", "topic"),
    [
        ("build.completed", "build.started"),
        ("build.*", "build"),
        ("build.*", "build.completed.now"),
        ("build.**", "image.resize"),
        ("a.*.c", "a.b.d"),
        ("a.*.c", "a.c"),
        ("*", "build.completed"),
    ],
)
def test_pattern_rejects_unrelated_topics(pattern: str, topic: str) -> None:
    assert not matches(pattern, topic)


def test_patterns_are_case_sensitive() -> None:
    assert not matches("build.*", "Build.completed")


@pytest.mark.parametrize("pattern", ["*", "**", "build.*", "build.**", "a.*.c"])
def test_valid_patterns_are_accepted(pattern: str) -> None:
    assert validate_pattern(pattern) == pattern


@pytest.mark.parametrize("pattern", ["", "build.", ".build", "**.build", "build.**.now", "a b", "_relay.*"])
def test_invalid_patterns_are_rejected(pattern: str) -> None:
    with pytest.raises(UsageError):
        validate_pattern(pattern)
