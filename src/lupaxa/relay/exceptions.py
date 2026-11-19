"""Errors raised by Relay."""

from __future__ import annotations


class RelayError(Exception):
    """A Relay failure with a process exit code."""

    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class UsageError(RelayError):
    """The request or configuration is invalid."""

    def __init__(self, message: str) -> None:
        super().__init__(message, exit_code=2)


class BrokerBusy(RelayError):
    """Another process already owns this database."""

    def __init__(self, message: str = "Another broker is using this database.") -> None:
        super().__init__(message, exit_code=4)


class ConnectionFailed(RelayError):
    """The client could not reach a broker."""

    def __init__(self, message: str) -> None:
        super().__init__(message, exit_code=3)


class ProtocolError(RelayError):
    """A protocol response with a stable error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message, exit_code=1)
        self.code = code
