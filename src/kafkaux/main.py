import typer

from kafkaux.config.config import Configuration
from kafkaux.config.config import load_configuration
from kafkaux.kafka.consumer import ConsumerMode
from kafkaux.kafka.consumer import KafkauxConsumer
from kafkaux.kafka.filter import FILTER_REGISTRY

app = typer.Typer()  # Todo: Update args

# TODO: Come up with a solution for signal handling.


def parse_filter_mapping(filters: list[str]) -> dict[str, str]:
    """parse_filter_mapping handles the --filter flag which can be
    provided multiple times on the command line.  The format for
    passing filters is 'key=value' where key is the strategy function
    and the value is the strategy function to call and the value is
    the expected value to match on.

    Example:

    # To match all messages where the message key ended with test:
    kafkaux consume --topics my-topic --filter 'key_matched=^.*test$'

    """
    result = {}
    for f in filters:
        if "=" not in f:
            raise typer.BadParameter("--filter must contain a 'key=value' format")
        k, v = f.split("=", 1)
        if k not in FILTER_REGISTRY:
            raise typer.BadParameter(
                f"unsupported --filter key: {k}, must be in: {FILTER_REGISTRY.keys()}"
            )
        result[k] = v
    return result


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
    filters: list[str] = typer.Option(
        default_factory=list, help="Arbitrary filters to apply to messages"
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
    parsed_filters = parse_filter_mapping(filters)
    cfg: Configuration = ctx.obj.get("config")
    ctx.obj["config"] = cfg
    mode = ConsumerMode.TAIL if tail else ConsumerMode.TAIL  # Bug: Plumb it in
    c = KafkauxConsumer(
        kconfig=cfg,
        mode=mode,
        topics=topics,
        filters=parsed_filters,
    )  # TODO: Multiple topics
    c.start()


@app.command()
def meta(ctx: typer.Context):
    """subcommand for managing the cluster and querying metadata"""
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"querying -> cfg: {ctx.obj.get('config')}")
    ctx.obj["config"] = cfg


@app.callback(add_help_option=True)
def core(ctx: typer.Context, config: str | None = None):
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
