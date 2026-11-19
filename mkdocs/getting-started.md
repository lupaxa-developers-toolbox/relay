# Getting Started

Relay needs Python 3.11 or newer.

## Install

```bash
pip install lupaxa-relay
```

The text interface is separate:

```bash
pip install "lupaxa-relay[tui]"
```

A normal install does not include that extra. `relay tui` without it
prints a usage error and exits 2.

Check the configuration and the database:

```bash
relay doctor
relay --version
```

`relay doctor` loads the configuration. When a database already exists
it reports the schema and the quick check. It also says whether a
broker is listening on the configured socket.

## Run a Sample

From a clone of this repository, in one terminal:

```bash
cd samples/hello
relay --config relay.yml serve
```

In another terminal, from the same directory:

```bash
python publish.py
relay --config relay.yml history --topic build.completed
```

`publish.py` sends `build.completed` and prints the id Relay assigned
after the write committed. `relay history` prints that stored event.
Stop the broker with Ctrl-C. The database file remains, so the event is
still there the next time you serve this sample.

The socket path in the sample is `./r.sock`. Run the commands from
`samples/hello`, or pass the same `--config` from elsewhere only after
you change those paths to absolute paths. A relative socket is resolved
from the current directory.

## One Publish Without the Script

From `samples/hello`, with the broker still running:

```bash
relay --config relay.yml publish build.completed --json '{"version":"1.2.0"}'
relay --config relay.yml status
```

`relay publish` prints JSON with `message_id` and `sequence`. Pass
`--json` or `--text`, not both. `--idempotency-key` repeats the same
committed result when you send the same payload again.

## Read the Docs Locally

The guide in this site is the `mkdocs/` directory.

```bash
pip install -r requirements.txt
mkdocs serve
```

Open the URL MkDocs prints. The published site is
<https://relay.thelupaxaproject.org/>.
