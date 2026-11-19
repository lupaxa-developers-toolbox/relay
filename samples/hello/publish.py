"""Publish one event to the hello sample broker."""

from __future__ import annotations

import json
import os
from pathlib import Path

from lupaxa.relay.client import RelayClient
from lupaxa.relay.config import load_config


def main() -> None:
    root = Path(__file__).resolve().parent
    os.chdir(root)
    config = load_config(root / "relay.yml", user_path=None)
    socket = config.socket_path()
    if socket is None:
        raise SystemExit("This sample uses a Unix socket.")
    with RelayClient(socket=socket) as client:
        result = client.publish("build.completed", {"version": "1.2.0"})
        stored = client.history("build.completed", limit=1)
    print(
        json.dumps(
            {
                "message_id": result.message_id,
                "sequence": result.sequence,
                "stored": len(stored),
            }
        )
    )


if __name__ == "__main__":
    main()
