"""Command-line entry point."""

from __future__ import annotations

import asyncio
import contextlib
import json
import signal
import sys
from pathlib import Path
from typing import Annotated

import typer
import yaml
from typer._click.exceptions import ClickException

from lupaxa.relay.broker import Broker
from lupaxa.relay.client import Delivery, GapNotice, JobDelivery, RelayClient
from lupaxa.relay.config import default_config_path, load_config
from lupaxa.relay.daemon import describe, pid_file, start, stop
from lupaxa.relay.exceptions import ConnectionFailed, RelayError
from lupaxa.relay.observe import configure_logging
from lupaxa.relay.storage import Storage
from lupaxa.relay.tui import run as run_tui
from lupaxa.relay.version import __version__

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Exchange messages between independent programs.")
config_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Inspect configuration.")
worker_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Run a queue worker.")
queue_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Inspect and manage queues.")
topic_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Inspect topics.")
subscription_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Manage durable subscriptions.")
storage_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Inspect stored data.")
daemon_app = typer.Typer(add_completion=False, no_args_is_help=True, help="Run the broker in the background.")
app.add_typer(config_app, name="config")
app.add_typer(worker_app, name="worker")
app.add_typer(queue_app, name="queue")
app.add_typer(topic_app, name="topic")
app.add_typer(subscription_app, name="subscription")
app.add_typer(storage_app, name="storage")
app.add_typer(daemon_app, name="daemon")


def _show_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit


@app.callback()
def _root(
    ctx: typer.Context,
    _version: Annotated[
        bool,
        typer.Option("--version", help="Show the package version and exit.", callback=_show_version, is_eager=True),
    ] = False,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Explicit configuration file."),
    ] = None,
    socket: Annotated[
        Path | None,
        typer.Option("--socket", help="Unix socket for this invocation."),
    ] = None,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", help="Hide informational text."),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Log broker detail."),
    ] = False,
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Seconds to wait for the broker."),
    ] = 5.0,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show destructive changes without applying them."),
    ] = False,
) -> None:
    """Exchange messages between independent programs."""
    if quiet and verbose:
        raise RelayError("Pass either --quiet or --verbose.", exit_code=2)
    if timeout <= 0:
        raise RelayError("Timeout must be greater than zero.", exit_code=2)
    ctx.obj = {
        "config": config,
        "socket": socket,
        "quiet": quiet,
        "verbose": verbose,
        "timeout": timeout,
        "dry_run": dry_run,
    }
    try:
        settings = load_config(config, socket=str(socket) if socket is not None else None)
    except RelayError:
        settings = None
    if settings is not None:
        configure_logging(settings, verbose=verbose, quiet=quiet)


@app.command()
def serve(ctx: typer.Context) -> None:
    """Run the broker in the foreground."""
    broker = Broker(_settings(ctx))
    asyncio.run(_serve(broker, quiet=_flag(ctx, "quiet")))


@app.command()
def publish(
    ctx: typer.Context,
    topic: Annotated[str, typer.Argument(help="Topic name.")],
    body: Annotated[
        str | None,
        typer.Option("--json", help="JSON payload."),
    ] = None,
    text: Annotated[
        str | None,
        typer.Option("--text", help="Plain-text payload."),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        typer.Option("--idempotency-key", help="Reuse the result of a committed publish."),
    ] = None,
) -> None:
    """Publish one event and print its id and sequence."""
    if (body is None) == (text is None):
        raise RelayError("Pass either --json or --text.", exit_code=2)
    if body is not None:
        try:
            data: object = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RelayError(f"Invalid JSON payload: {exc}", exit_code=2) from exc
        content_type = "application/json"
    else:
        data = text
        content_type = "text/plain"
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        result = client.publish(topic, data, content_type=content_type, idempotency_key=idempotency_key)
    typer.echo(json.dumps({"message_id": result.message_id, "sequence": result.sequence}))


@app.command()
def enqueue(ctx: typer.Context, queue: Annotated[str, typer.Argument(help="Queue name.")], body: Annotated[str, typer.Option("--json", help="JSON payload.")]) -> None:
    """Enqueue one job and print its id."""
    try:
        data: object = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RelayError(f"Invalid JSON payload: {exc}", exit_code=2) from exc
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        message_id = client.enqueue(queue, data)
    typer.echo(json.dumps({"message_id": message_id}))


@worker_app.command("consume")
def worker_consume(
    ctx: typer.Context,
    queue: Annotated[str, typer.Argument(help="Queue name.")],
    limit: Annotated[
        int,
        typer.Option("--limit", help="Stop after this many jobs. 0 keeps streaming."),
    ] = 0,
) -> None:
    """Print jobs as JSON lines and acknowledge each one."""
    shown = 0
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        for item in client.consume(queue):
            typer.echo(_job_json(item))
            item.ack()
            shown += 1
            if limit and shown >= limit:
                return


@app.command()
def subscribe(
    ctx: typer.Context,
    pattern: Annotated[str, typer.Argument(help="Topic pattern.")],
    durable: Annotated[
        str | None,
        typer.Option("--durable", help="Durable subscription name."),
    ] = None,
    start: Annotated[
        str,
        typer.Option("--from", help="Where a new durable subscription starts."),
    ] = "latest",
    sequence: Annotated[
        int | None,
        typer.Option("--sequence", help="First sequence when --from is sequence."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Stop after this many events. 0 keeps streaming."),
    ] = 0,
    manual_ack: Annotated[
        bool,
        typer.Option("--manual-ack", help="Print one durable event and leave it unacked."),
    ] = False,
) -> None:
    """Print matching events as JSON lines."""
    if start not in {"latest", "earliest", "sequence"}:
        raise RelayError("Pass --from latest, earliest, or sequence.", exit_code=2)
    if start == "sequence" and sequence is None:
        raise RelayError("Pass --sequence with --from sequence.", exit_code=2)
    shown = 0
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        for item in client.subscribe(pattern, durable=durable, start=start, sequence=sequence):
            if isinstance(item, GapNotice):
                typer.echo(
                    json.dumps(
                        {
                            "gap": {
                                "from_sequence": item.from_sequence,
                                "to_sequence": item.to_sequence,
                            }
                        }
                    )
                )
                continue
            typer.echo(_delivery_json(item))
            shown += 1
            if durable is not None and not manual_ack:
                item.ack()
            if manual_ack or (limit and shown >= limit):
                return


@app.command()
def history(
    ctx: typer.Context,
    topic: Annotated[
        str | None,
        typer.Option("--topic", help="Only events for this topic."),
    ] = None,
    message: Annotated[
        str | None,
        typer.Option("--message", help="Show one event or job by id."),
    ] = None,
    limit: Annotated[int, typer.Option("--limit", help="Maximum events to print.")] = 50,
) -> None:
    """Print stored events, or one event or job."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        if message is not None:
            typer.echo(json.dumps(client.message(message)))
            return
        typer.echo(json.dumps(client.history(topic, limit)))


@app.command()
def tui(ctx: typer.Context) -> None:
    """Open the text interface."""
    run_tui(_client_socket(ctx))


@queue_app.command("list")
def queue_list(ctx: typer.Context) -> None:
    """Print the configured queues and their job counts."""
    typer.echo(json.dumps(_status(ctx).get("queues", [])))


@queue_app.command("show")
def queue_show(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Queue name.")]) -> None:
    """Print one queue and its counts."""
    typer.echo(json.dumps(_queue_record(ctx, name)))


@queue_app.command("stats")
def queue_stats(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Queue name.")]) -> None:
    """Print pending, leased, retry, and dead counts for one queue."""
    record = _queue_record(ctx, name)
    typer.echo(
        json.dumps(
            {
                "name": record["name"],
                "pending": record["pending"],
                "leased": record["leased"],
                "retry": record["retry"],
                "dead": record["dead"],
                "delayed": record["delayed"],
                "workers": record["workers"],
            }
        )
    )


@queue_app.command("inspect")
def queue_inspect(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Queue name.")], limit: Annotated[int, typer.Option("--limit", help="Maximum jobs to print.")] = 50) -> None:
    """Print jobs on a queue, including failure details."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        typer.echo(json.dumps(client.inspect(name, limit)))


@queue_app.command("retry")
def queue_retry(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Queue the job is on now.")], message: Annotated[str, typer.Option("--message", help="Job id to queue again.")]) -> None:
    """Put a dead-lettered job back on its original queue."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        found = client.message(message).get("job")
        if not isinstance(found, dict) or found.get("queue") != name:
            raise RelayError(f"Job {message} is not on {name}.", exit_code=1)
        job = client.retry_job(message)
    typer.echo(json.dumps(job))


@queue_app.command("purge")
def queue_purge(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Queue name.")], yes: Annotated[bool, typer.Option("--yes", help="Delete without prompting.")] = False) -> None:
    """Delete jobs on a queue that are not currently leased."""
    preview = _flag(ctx, "dry_run")
    if not preview:
        _confirm(yes, f"Delete jobs that are not leased from {name}?")
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        deleted = client.purge(name, dry_run=preview)
    payload: dict[str, object] = {"queue": name, "deleted": deleted}
    if preview:
        payload["dry_run"] = True
    typer.echo(json.dumps(payload))


@queue_app.command("create")
def queue_create(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Queue name.")],
    dead_letter_queue: Annotated[
        str | None,
        typer.Option("--dead-letter", help="Queue that receives exhausted jobs."),
    ] = None,
) -> None:
    """Create a queue and keep it for the next broker start."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        client.create_queue(name, dead_letter_queue=dead_letter_queue)
    typer.echo(json.dumps({"queue": name}))


@topic_app.command("list")
def topic_list(ctx: typer.Context) -> None:
    """Print topics that have stored events."""
    typer.echo(json.dumps(_status(ctx).get("topics", [])))


@topic_app.command("show")
def topic_show(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Topic name.")]) -> None:
    """Print recent events for one topic."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        typer.echo(json.dumps(client.history(name, 50)))


@topic_app.command("history")
def topic_history(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Topic name.")], limit: Annotated[int, typer.Option("--limit", help="Maximum events to print.")] = 50) -> None:
    """Print stored events for one topic."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        typer.echo(json.dumps(client.history(name, limit)))


@subscription_app.command("list")
def subscription_list(ctx: typer.Context) -> None:
    """Print durable subscriptions."""
    typer.echo(json.dumps(_status(ctx).get("subscriptions", [])))


@subscription_app.command("show")
def subscription_show(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Subscription name.")]) -> None:
    """Print one durable subscription."""
    rows = _status(ctx).get("subscriptions", [])
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict) and item.get("name") == name:
                typer.echo(json.dumps(item))
                return
    raise RelayError(f"No subscription is named {name}.", exit_code=1)


@subscription_app.command("create")
def subscription_create(ctx: typer.Context, name: Annotated[str, typer.Argument(help="Subscription name.")], pattern: Annotated[str, typer.Argument(help="Topic pattern.")]) -> None:
    """Create a durable subscription at the latest event."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        typer.echo(json.dumps(client.create_subscription(name, pattern)))


@subscription_app.command("reset")
def subscription_reset(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Subscription name.")],
    start: Annotated[str, typer.Option("--from", help="latest or earliest.")] = "earliest",
    yes: Annotated[bool, typer.Option("--yes", help="Reset without prompting.")] = False,
) -> None:
    """Move a subscription back and drop its outstanding delivery."""
    if start not in {"latest", "earliest"}:
        raise RelayError("Pass --from latest or earliest.", exit_code=2)
    if _flag(ctx, "dry_run"):
        current = _subscription_record(ctx, name)
        current["dry_run"] = True
        current["start"] = start
        typer.echo(json.dumps(current))
        return
    _confirm(yes, f"Reset subscription {name} to {start}?")
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        typer.echo(json.dumps(client.reset_subscription(name, start)))


@subscription_app.command("delete")
def subscription_delete(
    ctx: typer.Context, name: Annotated[str, typer.Argument(help="Subscription name.")], yes: Annotated[bool, typer.Option("--yes", help="Delete without prompting.")] = False
) -> None:
    """Delete a durable subscription."""
    if _flag(ctx, "dry_run"):
        current = _subscription_record(ctx, name)
        typer.echo(json.dumps({"name": current["name"], "deleted": False, "dry_run": True}))
        return
    _confirm(yes, f"Delete subscription {name}?")
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        client.delete_subscription(name)
    typer.echo(json.dumps({"name": name, "deleted": True}))


@storage_app.command("stats")
def storage_stats(ctx: typer.Context) -> None:
    """Print database size and row counts."""
    payload = _status(ctx)
    typer.echo(
        json.dumps(
            {
                "bytes": payload.get("storage_bytes"),
                "messages": payload.get("messages"),
                "jobs": payload.get("jobs"),
                "healthy": payload.get("healthy"),
            }
        )
    )


@storage_app.command("check")
def storage_check(ctx: typer.Context) -> None:
    """Run the SQLite quick check."""
    payload = _status(ctx)
    if payload.get("healthy") is not True:
        raise RelayError("The database failed its quick check.", exit_code=1)
    _note(ctx, "Database is healthy.")


@storage_app.command("backup")
def storage_backup(ctx: typer.Context, destination: Annotated[Path, typer.Argument(help="Where to write the copy.")]) -> None:
    """Copy the live database."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        client.backup(destination)
    typer.echo(json.dumps({"path": str(destination.resolve())}))


@daemon_app.command("start")
def daemon_start(ctx: typer.Context) -> None:
    """Start the broker in the background."""
    config = _settings(ctx)
    socket = _client_socket(ctx)
    config_path = _obj(ctx).get("config")
    pid = start(config, config_path=config_path if isinstance(config_path, Path) else None, socket=socket)
    typer.echo(json.dumps({"pid": pid, "pid_file": str(pid_file(config))}))


@daemon_app.command("stop")
def daemon_stop(ctx: typer.Context) -> None:
    """Stop the background broker."""
    stop(_settings(ctx))
    typer.echo("Relay stopped.")


@daemon_app.command("restart")
def daemon_restart(ctx: typer.Context) -> None:
    """Stop the background broker, then start it again."""
    config = _settings(ctx)
    with contextlib.suppress(RelayError):
        stop(config)
    config_path = _obj(ctx).get("config")
    pid = start(config, config_path=config_path if isinstance(config_path, Path) else None, socket=_client_socket(ctx))
    typer.echo(json.dumps({"pid": pid}))


@daemon_app.command("status")
def daemon_status(ctx: typer.Context) -> None:
    """Show whether the background broker is running."""
    socket = _obj(ctx).get("socket")
    chosen = socket if isinstance(socket, Path) else _settings(ctx).socket_path()
    typer.echo(json.dumps(describe(_settings(ctx), socket=chosen, timeout=_timeout(ctx))))


@app.command()
def status(ctx: typer.Context) -> None:
    """Show whether the broker is reachable and how many messages it holds."""
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        payload = client.status()
    typer.echo(
        json.dumps(
            {
                "messages": payload.get("messages"),
                "connections": payload.get("connections"),
                "uptime_seconds": payload.get("uptime_seconds"),
            }
        )
    )


@config_app.command("validate")
def config_validate(ctx: typer.Context) -> None:
    """Load the configuration and report errors."""
    _settings(ctx)
    _note(ctx, "Configuration is valid.")


@config_app.command("show")
def config_show(ctx: typer.Context) -> None:
    """Print the resolved configuration."""
    dumped = _settings(ctx).model_dump(mode="json")
    token = dumped.get("security", {}).get("token")
    if token:
        dumped["security"]["token"] = "***"
    typer.echo(yaml.safe_dump(dumped, sort_keys=False).rstrip())


@config_app.command("path")
def config_path() -> None:
    """Print the user configuration path."""
    typer.echo(str(default_config_path()))


@app.command()
def doctor(ctx: typer.Context) -> None:
    """Check configuration, schema, socket permissions, and whether a broker is listening."""
    config = _settings(ctx)
    _note(ctx, "Configuration is valid.")
    path = config.database_path()
    if not path.is_file():
        _note(ctx, "Database has not been created.")
    else:
        try:
            report = asyncio.run(_inspect_database(config))
        except RelayError as exc:
            _note(ctx, str(exc))
            raise
        version = report.get("schema_version")
        if report.get("schema_compatible") is True:
            _note(ctx, f"Schema {version} is compatible.")
        else:
            _note(ctx, f"Schema {version} does not match this Relay.")
        if report.get("healthy") is True:
            _note(ctx, "Database is healthy.")
        else:
            _note(ctx, "Database is not healthy.")
        _note(ctx, f"Messages: {report.get('messages', 0)}")
        if report.get("schema_compatible") is not True or report.get("healthy") is not True:
            raise RelayError("The database failed its diagnostic check.", exit_code=1)
    socket_path = config.socket_path()
    if socket_path is None:
        _note(ctx, "Transport is TCP.")
    elif not socket_path.exists():
        _note(ctx, "Socket is not present.")
    else:
        mode = socket_path.stat().st_mode & 0o777
        if mode & 0o077:
            _note(ctx, f"Socket permissions are {mode:o}.")
        else:
            _note(ctx, "Socket permissions are restrictive.")
        try:
            with RelayClient(socket=socket_path, timeout=_timeout(ctx)) as client:
                client.status()
        except (ConnectionFailed, OSError):
            _note(ctx, "Broker is not listening.")
        else:
            _note(ctx, "Broker is listening.")


def main(argv: list[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""
    try:
        result = app(args=argv, standalone_mode=False)
    except typer.Exit as exc:
        return int(exc.exit_code or 0)
    except ClickException as exc:
        exc.show()
        code = getattr(exc, "exit_code", 2)
        return int(code) if isinstance(code, int) else 2
    except RelayError as exc:
        typer.echo(str(exc), err=True)
        return exc.exit_code
    if isinstance(result, int):
        return result
    return 0


def _job_json(item: JobDelivery) -> str:
    payload: dict[str, object] = {
        "delivery_id": item.delivery_id,
        "message_id": item.message_id,
        "queue": item.queue,
        "attempt": item.attempt,
        "data": item.data,
    }
    if item.original_queue != item.queue:
        payload["original_queue"] = item.original_queue
    if item.failure_reason is not None:
        payload["failure_reason"] = item.failure_reason
    return json.dumps(payload)


def _delivery_json(item: Delivery) -> str:
    return json.dumps(
        {
            "delivery_id": item.delivery_id,
            "message_id": item.message_id,
            "sequence": item.sequence,
            "destination": item.destination,
            "data": item.data,
        }
    )


def _flag(ctx: typer.Context, name: str) -> bool:
    return _obj(ctx).get(name) is True


def _timeout(ctx: typer.Context) -> float:
    value = _obj(ctx).get("timeout", 5.0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 5.0
    return float(value)


def _note(ctx: typer.Context, message: str) -> None:
    if not _flag(ctx, "quiet"):
        typer.echo(message)


def _subscription_record(ctx: typer.Context, name: str) -> dict[str, object]:
    rows = _status(ctx).get("subscriptions")
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict) and item.get("name") == name:
                return item
    raise RelayError(f"No subscription is named {name}.", exit_code=1)


async def _inspect_database(config: object) -> dict[str, object]:
    from lupaxa.relay.config import RelayConfig
    from lupaxa.relay.exceptions import UsageError

    if not isinstance(config, RelayConfig):
        raise RelayError("Configuration is not available.", exit_code=1)
    storage = Storage(config.database_path(), durability=config.storage.durability)
    try:
        await storage.open()
        return await storage.storage_report()
    except UsageError as exc:
        raise RelayError(str(exc), exit_code=1) from exc
    finally:
        await storage.close()


def _confirm(yes: bool, prompt: str) -> None:
    if yes:
        return
    if not sys.stdin.isatty():
        raise RelayError(f"{prompt} Pass --yes to confirm.", exit_code=2)
    if not typer.confirm(prompt):
        raise RelayError("Cancelled.", exit_code=2)


def _status(ctx: typer.Context) -> dict[str, object]:
    with RelayClient(socket=_client_socket(ctx), timeout=_timeout(ctx)) as client:
        return client.status()


def _queue_record(ctx: typer.Context, name: str) -> dict[str, object]:
    rows = _status(ctx).get("queues", [])
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict) and item.get("name") == name:
                return item
    raise RelayError(f"Queue is not configured: {name}", exit_code=1)


def _obj(ctx: typer.Context) -> dict[str, object]:
    current: object = ctx
    while current is not None:
        obj = getattr(current, "obj", None)
        if isinstance(obj, dict) and "socket" in obj:
            return {str(key): value for key, value in obj.items()}
        current = getattr(current, "parent", None)
    return {}


def _settings(ctx: typer.Context):
    obj = _obj(ctx)
    socket = obj.get("socket")
    config_path = obj.get("config")
    return load_config(config_path if isinstance(config_path, Path) else None, socket=str(socket) if isinstance(socket, Path) else None)


def _client_socket(ctx: typer.Context) -> Path:
    override = _obj(ctx).get("socket")
    if isinstance(override, Path):
        return override
    path = _settings(ctx).socket_path()
    if path is None:
        raise RelayError("Pass --socket or configure a Unix socket.", exit_code=2)
    return path


async def _serve(broker: Broker, *, quiet: bool) -> None:
    await broker.start()
    if not quiet:
        typer.echo(f"Relay listening on {broker.socket_path or broker.tcp_port}")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await broker.stop()
