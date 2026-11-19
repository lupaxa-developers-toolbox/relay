# Usage

Run one broker for a database. A second process that opens the same
database exits 4. The lock file sits beside the database. SQLite's own
lock is not the broker lock.

## Serve

```bash
relay serve
relay daemon start
relay daemon status
relay daemon stop
relay daemon restart
```

`relay serve` stays in the foreground and stops on Ctrl-C or `SIGTERM`.
`relay daemon start` runs that same command in the background and writes
a pid file beside the database. `relay daemon stop` sends `SIGTERM`.

`--config` selects a YAML file. `--socket` overrides the socket for
that invocation. `--timeout` is how long a client command waits, in
seconds. `--quiet` hides informational text and still prints JSON
results. `--verbose` turns broker logging up to debug.

## Events

```bash
relay publish build.completed --json '{"version":"1.2.0"}'
relay subscribe "build.*"
relay subscribe "build.*" --durable auditor --from earliest --limit 1
relay history --topic "build.*"
relay history --message <id>
```

A publish is acknowledged only after the database commit. The printed
`sequence` is the order of stored events.

`relay subscribe` without `--durable` lives only as long as that
connection. A durable name keeps its cursor. `--from` is `latest`,
`earliest`, or `sequence`. A new durable subscription defaults to
`latest`, so it does not replay events that are already stored. Pass
`--from earliest` when you want those.

A durable subscriber must acknowledge each event before the next one is
sent. The command does that unless you pass `--manual-ack`. `--limit 1`
prints one event and exits. `0` keeps streaming.

`*` matches one segment of a topic. `**` matches the rest, and only as
the last segment. `build.*` matches `build.completed` and does not match
`build.completed.v2`.

History with a pattern returns stored events. It does not create a
subscription. `--message` prints the event or job, plus each recorded
delivery, acknowledgement, negative acknowledgement, and dead-letter
transition.

Publish the same `--idempotency-key` with the same payload and Relay
returns the original id. A different payload for that key is rejected.

## Queues

```bash
relay enqueue image.resize --json '{"image":"a.jpg"}'
relay worker consume image.resize --limit 1
relay queue list
relay queue show image.resize
relay queue stats image.resize
relay queue inspect image.resize.failed
```

A job is stored before `relay enqueue` prints its id. `relay worker
consume` prints each job and acknowledges it. The worker does not run
the payload. `--limit` stops after that many acknowledgements.

One valid lease exists for a job. The visibility timeout returns an
unacknowledged job to the queue. `nack` schedules a retry. `reject`
moves the job toward the dead-letter queue. When attempts are exhausted,
the job moves to `dead_letter_queue` if one is set, or stays on the
original queue with state `dead`.

```bash
relay queue retry image.resize.failed --message <id>
relay queue purge image.resize --dry-run
relay queue purge image.resize --yes
```

Retry puts a dead-lettered job back on its original queue and keeps the
id, the payload, and the failure reason. Purge deletes jobs that are
not leased. A leased job stays. Without `--yes`, purge asks for
confirmation. `--dry-run` reports how many jobs would be deleted and
deletes nothing. The same flag previews subscription reset and delete.

`max_depth` on a queue counts pending, retrying, and leased jobs. The
next enqueue past that count is refused and is not stored.
`consumers.default_prefetch` is how many jobs a worker holds when it
does not ask for a number. `consumers.max_prefetch` is the ceiling.

## Bindings

A binding names an exact topic and a queue that already exists. A
matching publish inserts one job per binding in the same transaction as
the event. If a bound queue cannot take the job, the publish is not
acknowledged. Publishing the same idempotency key again does not insert
another job.

The event remains available to subscribers. The job has its own id.

## Subscriptions and Storage

```bash
relay subscription list
relay subscription create auditor "build.*"
relay subscription show auditor
relay subscription reset auditor --from earliest --dry-run
relay subscription delete auditor --yes
relay storage stats
relay storage check
relay storage backup ./relay-backup.sqlite3
```

Create fails when the name already exists. Reset moves the cursor to
`latest` or `earliest` and clears the outstanding delivery. Delete
removes the subscription. Backup uses the SQLite backup API and refuses
to write onto the live database.

```bash
relay queue create reports.generate --dead-letter reports.failed
```

A queue created this way is stored in the database and loaded the next
time the broker starts. Editing the YAML file does not reload a broker
that is already running.

## Topics

Topics are not created as their own records. `relay topic list` is the
set of destinations that have stored events. `relay topic show` and
`relay topic history` print recent events for one name.

## Text Interface

```bash
relay tui
```

The interface reads the broker through the public protocol. It shows
queue counts, topics, recent events, subscriptions, and dead-lettered
jobs. `r` refreshes. `d` prints the selected event, including its
delivery history. `y` asks before retrying the first dead-lettered job.
`p` asks before purging that job's queue. `n` cancels. If the broker
goes away, the tables clear and the summary says Disconnected.

## Logs and Diagnostics

```yaml
logging:
  level: info
  format: json
```

`format: json` writes one JSON object per log line. The line has the
level, the logger name, and the message. Payloads and tokens are not
included. `level` is `debug`, `info`, `warning`, `error`, or `critical`.

`relay doctor` reports whether the schema matches this version of Relay,
whether the quick check passed, whether the socket is restricted to your
user, and whether a broker answers on that socket.
