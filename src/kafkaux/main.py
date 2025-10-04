import pathlib

import typer

from kafkaux.config.config import Configuration
from kafkaux.config.config import load_configuration
from kafkaux.kafka.consumer import ConsumerMode
from kafkaux.kafka.consumer import KafkauxConsumer

app = typer.Typer()  # Todo: Update args

# TODO: Come up with a solution for signal handling.

@app.command()
def produce(ctx: typer.Context):
    """subcommand for producing workflows"""
    ...


@app.command()
def consume(
    ctx: typer.Context,
    topics: list[str] = typer.Option(..., help="List of topics to subscribe to"),
    tail: bool = typer.Option(
        False, "--tail", "-t", help="Consume only new messages, started at `latest`"
    ),
    filters: list[str] = typer.Argument(
        default=(), help="Arbitrary filters to apply to messages"
    ),
):
    """subcommand for consuming workflows

    TODO: Handle --output, some sort of io.writer style, default to sys.stdout
    TODO: Filter strategies, what kind of API should that be exposed as, string literal?
    TODO: This api is a horrible mess right now, hacking to make something functional,
    TODO: Multi topics
    abstract it, scattered if tail blocks etc is unwieldy and will get worse as more
    functionality is added, perhaps a strategy that takes the config and returns a
    consumer, likewise for a producer and admin client.
    """
    cfg: Configuration = ctx.obj.get("config")
    ctx.obj["config"] = cfg
    c = KafkauxConsumer(
        kconfig=cfg,
        mode=ConsumerMode.TAIL,
        topics=topics,
        filters=None,
    ) # TODO: Multiple topics
    c.tail()


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
