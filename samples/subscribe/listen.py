"""Read one matching event for the auditor subscription, then acknowledge it."""

from __future__ import annotations

import json
import os
from pathlib import Path

from lupaxa.relay.client import Delivery, RelayClient
from lupaxa.relay.config import load_config


def main() -> None:
    root = Path(__file__).resolve().parent
    os.chdir(root)
    config = load_config(root / "relay.yml", user_path=None)
    socket = config.socket_path()
    if socket is None:
        raise SystemExit("This sample uses a Unix socket.")
    with RelayClient(socket=socket) as client:
        for item in client.subscribe("build.*", durable="auditor", start="earliest"):
            if not isinstance(item, Delivery):
                continue
            print(
                json.dumps(
                    {
                        "message_id": item.message_id,
                        "destination": item.destination,
                        "data": item.data,
                    }
                )
            )
            item.ack()
            return


if __name__ == "__main__":
    main()
