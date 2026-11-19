<p align="center">
  <a href="https://github.com/lupaxa-developers-toolbox">
    <img src="https://raw.githubusercontent.com/the-lupaxa-project/brand-assets/master/logos/organisations/developers-toolbox/readme-logo.png" alt="Developers Toolbox" />
  </a>
</p>

<h1 align="center">Relay</h1>

Relay is a local message broker. A program publishes an event, and Relay
stores it before acknowledging the publish. Other programs can subscribe
to a topic pattern and receive those events. A queue leases each job to
one worker until that worker acknowledges it, retries it, or dead-letters
it. Programs do not need Redis, RabbitMQ, or another external service.

> **Note:** A durable subscription keeps its place across restarts and
> waits for an acknowledgement before the next event. A queue keeps a
> pending job, and an unexpired lease, across a broker restart. A binding
> copies a published event onto a queue in that same commit. `relay history`
> can filter with a topic pattern, and `--message` includes deliveries and
> acknowledgements. `relay tui` shows queues, events, subscriptions, and
> dead letters when the `tui` extra is installed. Destructive commands
> accept `--dry-run`.

The PyPI name is `lupaxa-relay`. The import path is `lupaxa.relay`. The
console script is `relay`. `lupaxa` is a namespace package — there is no
`lupaxa/__init__.py`.

## Install

```bash
pip install lupaxa-relay
```

Python 3.11 or newer is required.

## Run

```bash
relay serve
relay publish build.completed --json '{"version":"1.2.0"}'
relay subscribe "build.*"
relay subscribe "build.*" --durable auditor
relay enqueue image.resize --json '{"image":"a.jpg"}'
relay worker consume image.resize
relay status
relay queue list
relay history --topic "build.*"
relay history --message <id>
relay doctor
relay queue purge image.resize --dry-run
relay daemon start
relay tui
relay --help
relay --version
```

`relay serve` listens on a Unix socket. `relay publish` prints the message
id and sequence after the event is stored. Stop the broker with Ctrl-C.
Events already stored are still there the next time it starts.

## Samples

The repository has three ready-to-run directories under `samples/`.
Start the broker from the sample directory. This is the smallest:

```bash
cd samples/hello
relay --config relay.yml serve
```

From another terminal, in that same directory:

```bash
python publish.py
relay --config relay.yml history --topic build.completed
```

`samples/queue` enqueues a job and acknowledges it. `samples/subscribe`
keeps a durable subscription and copies the event onto a queue.

## Documentation

The guide lives in `mkdocs/` and publishes to
<https://relay.thelupaxaproject.org/>.
It has install steps, events and queues, a command reference, and a
walkthrough of each sample with commands you can paste.

```bash
pip install -r requirements.txt
mkdocs serve
```

<a href="https://github.com/the-lupaxa-project">
    <img src="https://raw.githubusercontent.com/the-lupaxa-project/brand-assets/master/logos/components/footer-for-child-orgs.svg" alt="The Lupaxa Project Footer" width="100%" />
</a>
