"""Broker logging. Payloads are never written here."""

from __future__ import annotations

import json
import logging

from lupaxa.relay.config import RelayConfig


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(config: RelayConfig, *, verbose: bool = False, quiet: bool = False) -> None:
    """Attach one handler to the relay logger."""
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.WARNING
    else:
        level = getattr(logging, config.logging.level.upper(), logging.INFO)
    log = logging.getLogger("lupaxa.relay")
    log.handlers.clear()
    log.setLevel(level)
    log.propagate = False
    handler = logging.StreamHandler()
    if config.logging.format == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    log.addHandler(handler)
