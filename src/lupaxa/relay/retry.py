"""Deterministic retry delays. There is no jitter."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AttemptPolicy:
    """The retry and dead-letter rules for one queue."""

    max_attempts: int | None
    strategy: str
    initial_delay_seconds: float
    multiplier: float
    max_delay_seconds: float
    increment_seconds: float
    dead_letter_queue: str | None
    visibility_timeout_seconds: int


def retry_delay(policy: AttemptPolicy, attempts: int) -> float:
    """Return the delay after attempt `attempts` failed. Attempts start at 1."""
    if attempts < 1:
        return 0.0
    if policy.strategy == "immediate":
        delay = 0.0
    elif policy.strategy == "fixed":
        delay = policy.initial_delay_seconds
    elif policy.strategy == "linear":
        delay = policy.initial_delay_seconds + policy.increment_seconds * (attempts - 1)
    elif policy.strategy == "exponential":
        delay = policy.initial_delay_seconds * (policy.multiplier ** (attempts - 1))
    else:
        delay = 0.0
    if delay < 0:
        delay = 0.0
    return min(delay, policy.max_delay_seconds)
