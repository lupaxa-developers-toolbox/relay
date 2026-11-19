# Reference

Commands print JSON when the result is data. Human sentences, such as
the line from `relay doctor`, are omitted when you pass `--quiet`.

## Commands

```text
relay
├── serve
├── daemon
│   ├── start
│   ├── stop
│   ├── restart
│   └── status
├── publish
├── enqueue
├── subscribe
├── topic
│   ├── list
│   ├── show
│   └── history
├── queue
│   ├── create
│   ├── list
│   ├── show
│   ├── stats
│   ├── inspect
│   ├── retry
│   └── purge
├── subscription
│   ├── list
│   ├── show
│   ├── create
│   ├── reset
│   └── delete
├── worker
│   └── consume
├── history
├── storage
│   ├── stats
│   ├── check
│   └── backup
├── config
│   ├── validate
│   ├── show
│   └── path
├── doctor
├── status
└── tui
```

Global options are `--config`, `--socket`, `--quiet`, `--verbose`,
`--timeout`, `--dry-run`, `--version`, and `--help`.

`relay config show` masks a configured token as `***`. `relay config
path` prints the user configuration file. `LUPAXA_RELAY_CONFIG`,
`LUPAXA_RELAY_DATA`, and `LUPAXA_RELAY_SOCKET` override those default
locations.

## Exit Codes

| Code | Meaning                                                     |
| ---- | ----------------------------------------------------------- |
| 0    | The command finished                                        |
| 1    | The broker rejected the request, or a check failed          |
| 2    | The command line or the configuration is invalid            |
| 3    | The client could not reach a broker                         |
| 4    | Another broker already holds this database                  |

An unknown flag returns 2 and does not print a traceback.

## Names

A destination is 1 to 128 characters. Segments are separated by periods.
Each segment starts with a letter, digit, or underscore, and then may
include hyphens. Names are case-sensitive. `_relay` and `_relay.` are
reserved. A pattern uses the same segments, plus `*` for one segment and
`**` only as the last segment. A pattern is not a valid topic or queue
name.

## Configuration

Files are merged in this order: built-in defaults, the user file, the
`--config` file, then `--socket`. Unknown keys are rejected. This is a
valid file:

```yaml
version: 1
server:
  socket: ./r.sock
  tcp:
    enabled: false
    host: 127.0.0.1
    port: 8765
  max_connections: 100
storage:
  path: ./relay.sqlite3
  durability: full
  retention:
    events:
      max_age_days: 14
      max_messages: 100000
    completed_jobs:
      max_age_days: 7
    dead_letters:
      max_age_days: 30
messaging:
  max_payload_kb: 256
  max_headers_kb: 16
consumers:
  default_prefetch: 1
  max_prefetch: 100
queues:
  - name: image.resize
    max_attempts: 5
    visibility_timeout_seconds: 60
    max_depth: 1000
    retry:
      strategy: exponential
      initial_delay_seconds: 5
      multiplier: 2
      max_delay_seconds: 300
    dead_letter_queue: image.resize.failed
  - name: image.resize.failed
    max_attempts: 1
bindings:
  - id: deployment-events
    topic: build.completed
    queue: image.resize
logging:
  level: info
  format: text
security:
  allow_remote: false
```

`server.socket` and `storage.path` may be `auto`. `durability` is `full`
or `normal`. Retry strategies are `immediate`, `fixed`, `linear`, and
`exponential`. A strategy other than `immediate` needs
`initial_delay_seconds` greater than zero. `max_attempts` may be `null`
for no limit. A queue cannot name itself as its dead-letter queue, and
the dead-letter queue must be defined. Binding topics are exact names.
Duplicate binding ids and duplicate topic-and-queue pairs are rejected.

`security.allow_remote: true` is a configuration error. TCP, when
enabled, must bind to `127.0.0.1`, `::1`, or `localhost`. An optional
token is compared in constant time and is never logged.

`default_prefetch` cannot be greater than `max_prefetch`.

## Status

`relay status` prints message count, connection count, and uptime. The
protocol snapshot also includes job counts, storage size, a quick-check
flag, per-queue depths, topics, subscriptions, and lifetime counters:

| Counter                     | What it Counts                            |
| --------------------------- | ----------------------------------------- |
| `published_events`          | Events committed                          |
| `enqueued_jobs`             | Jobs inserted, including bound copies     |
| `delivered_messages`        | Deliveries handed to a consumer           |
| `acknowledged_deliveries`   | Acknowledgements committed                |
| `negative_acknowledgements` | Failed attempts, including expired leases |
| `retries`                   | Attempts scheduled for later              |
| `dead_lettered_jobs`        | Moves into a dead-letter state            |

Depths describe what is stored now. The counters survive a restart
because they are updated in the same transaction as the change they
describe.

## Python Client

```python
from pathlib import Path

from lupaxa.relay.client import RelayClient

with RelayClient(socket=Path("r.sock")) as client:
    result = client.publish("build.completed", {"version": "1.2.0"})
    print(result.message_id, result.sequence)
```

`RelayClient` also speaks loopback TCP when you pass `host` and `port`
instead of a socket. The same process offers `AsyncRelayClient` for
`async` code. Both use the public protocol. They do not open the
database.

## What is Stored

The database is SQLite in WAL mode. `durability: full` uses synchronous
`FULL`. A publish, an enqueue, a lease, an acknowledgement, a retry, and
a dead-letter move each commit before the client is told they succeeded.
Retention deletes old events and old completed or dead jobs. It does not
delete a pending, leased, or retrying job. A durable subscriber is told
when retention removes events it had not yet consumed.

Killing the process can leave a publish that the client never saw. Send
the same idempotency key to learn whether it committed. The database
stays readable. A later open recovers the committed rows.
