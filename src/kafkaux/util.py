import typer

from kafkaux.config import Configuration


def enforce_config(cfg: Configuration) -> None:
    """enforce_config ensures that the parsed config is fit for purpose
    and contains the bare minimum requirements to interact with a
    kafka cluster."""
    if cfg.librdkafka is None:
        typer.echo("parsed config must contain a [librdkafka] section", err=True)
        raise typer.Exit(code=2)
    must_have = ("bootstrap.servers", "metadata.broker.list")
    if not any(key in cfg.librdkafka for key in must_have):
        typer.echo(
            f"missing required keys in [librdkafka] section, should contain one of: {must_have}",
            err=True,
        )
        raise typer.Exit(code=2)
