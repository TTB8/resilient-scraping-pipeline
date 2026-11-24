"""``python -m scraping_pipeline``: the same CLI as the ``pipeline`` command."""

from scraping_pipeline.cli import app

app(prog_name="pipeline")
