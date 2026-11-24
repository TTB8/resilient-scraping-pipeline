"""Command-line entry point (``pipeline``). The stage commands are added as they are built."""

import typer

from scraping_pipeline import __version__

app = typer.Typer(help="Polite, resumable web data pipeline.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Polite, resumable web data pipeline (keeps ``pipeline`` a command group)."""


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)
