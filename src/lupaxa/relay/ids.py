"""Sortable message identifiers.

UUIDv7 keeps a millisecond timestamp in the high bits and random bits after
it, so uniqueness does not depend on the clock alone.
"""

from __future__ import annotations

import secrets
import time


def uuid7() -> str:
    """Return a UUIDv7 string."""
    unix_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    value = (unix_ms << 80) | (0x7 << 76) | (secrets.randbits(12) << 64) | (0b10 << 62) | secrets.randbits(62)
    hexed = f"{value:032x}"
    return f"{hexed[0:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:32]}"
