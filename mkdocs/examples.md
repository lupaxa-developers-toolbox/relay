# Examples

These three directories are in `samples/`. Each one has a `relay.yml`
and a short Python script. Start the broker from the sample directory
before you run the script. The script changes to its own directory, so
`python samples/hello/publish.py` from the repository root still uses
that sample's socket.

Stop the broker with Ctrl-C when you are finished. Delete `relay.sqlite3`
in the sample directory when you want an empty database. A second
`relay serve` on the same file exits 4 while the first one is running.

## Hello

`samples/hello` publishes `build.completed` and then reads that topic
back.

```yaml
--8<-- "samples/hello/relay.yml"
```

```python
--8<-- "samples/hello/publish.py"
```

```bash
cd samples/hello
relay --config relay.yml serve
```

From another terminal, still in `samples/hello`:

```bash
python publish.py
relay --config relay.yml history --topic build.completed
relay --config relay.yml doctor
```

`publish.py` prints JSON. `stored` is `1` when the event just written is
the latest event on that topic. `relay doctor` should report a
compatible schema, a restrictive socket, and a listening broker.

The same publish without the script:

```bash
relay --config relay.yml publish build.completed \
  --json '{"version":"1.2.0"}' \
  --idempotency-key hello
relay --config relay.yml publish build.completed \
  --json '{"version":"1.2.0"}' \
  --idempotency-key hello
```

The second command prints the same `message_id` as the first.

## Queue

`samples/queue` defines `image.resize` and its dead-letter queue.

```yaml
--8<-- "samples/queue/relay.yml"
```

```python
--8<-- "samples/queue/worker.py"
```

```bash
cd samples/queue
relay --config relay.yml serve
```

From another terminal:

```bash
cd samples/queue
relay --config relay.yml enqueue image.resize --json '{"image":"a.jpg"}'
python worker.py
relay --config relay.yml queue stats image.resize
```

`worker.py` prints the job and acknowledges it, then exits. The stats
show the job as completed. It is no longer pending. `relay queue inspect
image.resize` prints the same job, including its payload and state.

## Subscribe

`samples/subscribe` keeps a durable subscription named `auditor` and
copies `build.completed` onto `deployment.jobs`.

```yaml
--8<-- "samples/subscribe/relay.yml"
```

```python
--8<-- "samples/subscribe/listen.py"
```

```bash
cd samples/subscribe
relay --config relay.yml serve
```

Publish first, then listen. `listen.py` starts at the earliest stored
event, prints one match, acknowledges it, and exits:

```bash
cd samples/subscribe
relay --config relay.yml publish build.completed --json '{"version":"1.2.0"}'
python listen.py
relay --config relay.yml queue inspect deployment.jobs
relay --config relay.yml subscription show auditor
```

Inspect shows a job whose payload is the event payload and whose id is
not the event id. The event is still returned by `relay history --topic
build.completed`. The subscription cursor has moved past the event you
acknowledged.

Run `python listen.py` before the publish when you want to watch the
event arrive. It waits until one matching event is stored. Publish again
with `--idempotency-key demo` and the same payload. Relay returns the
original event and does not append another job.
