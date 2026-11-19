# Relay

Relay stores a message before it tells the sender the send succeeded.
Other programs can subscribe to a topic, or take jobs from a queue one
worker at a time. Nothing else has to be installed. There is no Redis
and no RabbitMQ.

An event stays an event. A job stays a job. A binding can copy one
published event onto a queue in the same commit. The event is still in
the history after that copy.

The package name is `lupaxa-relay`. The import path is `lupaxa.relay`.
The command is `relay`.

> **Note:** The broker is local. The default listener is a Unix socket
> owned by your user. Loopback TCP is optional and refuses any other
> address. Payloads are not written to the log.

## What it Does

- Accepts a publish only after the event is stored
- Delivers a topic pattern to ephemeral and durable subscribers
- Leases each job to one worker, then retries or dead-letters it
- Copies a published event onto each matching queue in that same commit
- Remembers idempotency keys, subscription cursors, and unexpired leases
- Serves a command line, a Python client, and an optional text interface

## Samples

The repository includes three directories you can run as they stand.
Each one has its own `relay.yml`. Start the broker from that directory
so the socket and database stay next to the sample.

| Sample              | What it Shows                                        |
| ------------------- | ---------------------------------------------------- |
| `samples/hello`     | Publish one event and read it back                   |
| `samples/queue`     | Enqueue a job and acknowledge it                     |
| `samples/subscribe` | Keep a subscription and copy the event onto a queue  |

## The Guide

[Getting Started](getting-started.md) installs the package and runs
`samples/hello`. [Usage](usage.md) covers events, queues, bindings, and
the text interface. [Reference](reference.md) lists commands, exit
codes, and configuration. [Examples](examples.md) walks through all
three samples with commands you can paste.
