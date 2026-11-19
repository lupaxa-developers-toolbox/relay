"""Take one job from image.resize, print it, and acknowledge it."""

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
        for job in client.consume("image.resize", prefetch=1):
            print(json.dumps({"message_id": job.message_id, "data": job.data}))
            job.ack()
            return


if __name__ == "__main__":
    main()
