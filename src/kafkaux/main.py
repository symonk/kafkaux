import typer

app = typer.Typer()  # Todo: Update args


@app.command()
def produce(): ...


@app.command()
def consume(): ...


@app.command()
def meta(): ...


def main() -> int:
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
