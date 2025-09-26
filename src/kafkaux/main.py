import pathlib

import typer

from kafkaux.config.parser import Configuration
from kafkaux.config.parser import load_configuration
from kafkaux.util import enforce_config

app = typer.Typer()  # Todo: Update args


@app.command()
def produce(ctx: typer.Context):
    """subcommand for producing workflows"""
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"producing -> cfg: {ctx.obj.get('config')}")
    enforce_config(cfg)


@app.command()
def consume(ctx: typer.Context,
            topic: str = typer.Argument(..., help="Kafka topic to consume from"),
            partitions: tuple[int] | None = typer.Option(None, help="Optional subset of partitions to consume from"),
            tail: bool = typer.Option(False, "--tail", "-t", help="Consume only new messages, started at `latest`"),
            filter_strategy: str | None = typer.Option(None, "--filter", "-f", help="Filter messages based on complex strategies"),
            output: pathlib.Path | None = typer.Option(None, "--output", "-o", help="Write filtered messages to a file")):
    """subcommand for consuming workflows"""
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"consuming -> cfg: {ctx.obj.get('config')}, locals: {locals()}")
    enforce_config(cfg)


@app.command()
def meta(ctx: typer.Context):
    """subcommand for managing the cluster and querying metadata"""
    cfg: Configuration = ctx.obj.get("config")
    typer.echo(f"querying -> cfg: {ctx.obj.get('config')}")
    enforce_config(cfg)


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
