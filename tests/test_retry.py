"""Retry delays are deterministic."""

from __future__ import annotations

from lupaxa.relay.retry import AttemptPolicy, retry_delay


def _policy(strategy: str) -> AttemptPolicy:
    return AttemptPolicy(
        max_attempts=5,
        strategy=strategy,
        initial_delay_seconds=5,
        multiplier=2,
        max_delay_seconds=12,
        increment_seconds=3,
        dead_letter_queue=None,
        visibility_timeout_seconds=10,
    )


def test_immediate_retry_is_ready_now() -> None:
    assert retry_delay(_policy("immediate"), 3) == 0


def test_fixed_retry_uses_the_initial_delay() -> None:
    assert retry_delay(_policy("fixed"), 1) == 5
    assert retry_delay(_policy("fixed"), 4) == 5


def test_linear_retry_grows_by_the_increment() -> None:
    policy = _policy("linear")
    assert retry_delay(policy, 1) == 5
    assert retry_delay(policy, 2) == 8
    assert retry_delay(policy, 3) == 11


def test_exponential_retry_caps_at_the_maximum() -> None:
    policy = _policy("exponential")
    assert retry_delay(policy, 1) == 5
    assert retry_delay(policy, 2) == 10
    assert retry_delay(policy, 3) == 12
