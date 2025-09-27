import pathlib
import uuid

import confluent_kafka
import typer

from kafkaux.config.config import Configuration
from kafkaux.config.config import load_configuration
from kafkaux.kafka import KafkaService

app = typer.Typer()  # Todo: Update args


@app.command()
def produce(ctx: typer.Context):
    """subcommand for producing workflows"""
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"producing -> cfg: {ctx.obj.get('config')}")


@app.command()
def consume(
    ctx: typer.Context,
    topic: str = typer.Argument(..., help="Kafka topic to consume from"),
    partitions: tuple[int] | None = typer.Option(
        None, help="Optional subset of partitions to consume from"
    ),
    tail: bool = typer.Option(
        False, "--tail", "-t", help="Consume only new messages, started at `latest`"
    ),
    filter_strategy: str | None = typer.Option(
        None, "--filter", "-f", help="Filter messages based on complex strategies"
    ),
    output: pathlib.Path | None = typer.Option(
        None, "--output", "-o", help="Write filtered messages to a file"
    ),
):
    """subcommand for consuming workflows

    TODO: Handle --output, some sort of io.writer style, default to sys.stdout
    TODO: Filter strategies, what kind of API should that be exposed as, string literal?
    TODO: This api is a horrible mess right now, hacking to make something functional,
    abstract it, scattered if tail blocks etc is unwieldy and will get worse as more
    functionality is added, perhaps a strategy that takes the config and returns a
    consumer, likewise for a producer and admin client.
    """
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"consuming -> cfg: {ctx.obj.get('config')}, locals: {locals()}")
    ctx.obj["config"] = cfg
    group_id = f"kafkaux-{uuid.uuid4()}"
    consumer_overrides = {}
    if tail:
        consumer_overrides = {
            "auto.offset.reset": "latest",  # do not play the offsets from the beginning of the stream
            "group.id": group_id,  # identify the consumer uniquely per run
            "enable.auto.commit": False,  # do not commit offsets for the group to avoid potential side effects
            "log_level": 1,  # only fatal/alert logging
        }
    cfg.librdkafka.update(**consumer_overrides)
    consumer = confluent_kafka.Consumer(cfg.librdkafka)
    with KafkaService(consumer=consumer) as kafka_service:
        if tail:
            kafka_service.tail(topic=topic, partitions=partitions)


@app.command()
def meta(ctx: typer.Context):
    """subcommand for managing the cluster and querying metadata"""
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"querying -> cfg: {ctx.obj.get('config')}")
    ctx.obj["config"] = cfg


@app.callback(add_help_option=True)
def core(ctx: typer.Context, config: pathlib.Path | None = None):
    """
    Custom path to a kafkaux.ini file.  This should be an absolute path.
    By default kafkaux will look for an `KAFKAUX_CONFIG` environment variable
    if this is not specified and if the env var is not found, will fallback
    to looking for the config in `~/.config/kafkaux.ini`
    """
    cfg: Configuration = load_configuration(config)
    ctx.ensure_object(dict)
    ctx.obj["config"] = cfg


def main() -> int:
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
