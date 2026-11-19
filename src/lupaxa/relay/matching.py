"""Deterministic topic patterns.

A pattern is a dot-separated list of segments. ``*`` matches one segment.
``**`` matches the rest of the topic, including nothing, and is only valid
as the final segment. Every other segment uses the destination name rules.
"""

from __future__ import annotations

from lupaxa.relay.exceptions import UsageError
from lupaxa.relay.names import valid_segment


def validate_pattern(pattern: str) -> str:
    """Return the pattern when it is legal."""
    if not isinstance(pattern, str) or not pattern or len(pattern) > 128:
        raise UsageError("Patterns must be 1 to 128 characters.")
    if pattern == "_relay" or pattern.startswith("_relay."):
        raise UsageError("Patterns starting with _relay are reserved.")
    parts = pattern.split(".")
    if any(part == "" for part in parts):
        raise UsageError(f"Invalid pattern: {pattern}")
    for index, part in enumerate(parts):
        if part == "**":
            if index != len(parts) - 1:
                raise UsageError("** is only valid as the last pattern segment.")
            continue
        if part == "*":
            continue
        if not valid_segment(part):
            raise UsageError(f"Invalid pattern: {pattern}")
    return pattern


def matches(pattern: str, topic: str) -> bool:
    """Return whether pattern matches topic. Both must already be valid."""
    return _match(pattern.split("."), topic.split("."))


def _match(pattern: list[str], topic: list[str]) -> bool:
    if not pattern:
        return not topic
    head = pattern[0]
    if head == "**":
        return True
    if not topic:
        return False
    if head in {"*", topic[0]}:
        return _match(pattern[1:], topic[1:])
    return False
