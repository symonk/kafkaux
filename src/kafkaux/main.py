import pathlib

import typer

from kafkaux.config.parser import Configuration
from kafkaux.config.parser import load_configuration

app = typer.Typer()  # Todo: Update args


@app.command()
def produce(ctx: typer.Context):
    """subcommand for producing workflows"""
    typer.echo(f"producing -> cfg: {ctx.obj.get('config')}")


@app.command()
def consume(ctx: typer.Context):
    """subcommand for consuming workflows"""
    typer.echo(f"consuming -> cfg: {ctx.obj.get('config')}")


@app.command()
def meta(ctx: typer.Context):
    """subcommand for managing the cluster and querying metadata"""
    typer.echo(f"querying -> cfg: {ctx.obj.get('config')}")


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
