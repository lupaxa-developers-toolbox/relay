"""Destination name rules.

Names are case-sensitive and 1 to 128 characters. Segments are separated by
a period. A segment starts with a letter, digit, or underscore, and then
may contain letters, digits, underscores, and hyphens. Names beginning
with ``_relay`` are reserved for the broker.
"""

from __future__ import annotations

import re

from lupaxa.relay.exceptions import UsageError

_SEGMENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,62}")


def valid_segment(segment: str) -> bool:
    """Return whether segment is one legal name segment."""
    return _SEGMENT.fullmatch(segment) is not None


def validate_destination(name: str) -> str:
    """Return the name when it is a legal topic or queue."""
    if not isinstance(name, str) or not name or len(name) > 128:
        raise UsageError("Destination names must be 1 to 128 characters.")
    if name == "_relay" or name.startswith("_relay."):
        raise UsageError("Destination names starting with _relay are reserved.")
    if any(not valid_segment(part) for part in name.split(".")):
        raise UsageError(f"Invalid destination name: {name}")
    return name
