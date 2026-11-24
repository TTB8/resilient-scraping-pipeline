from typer.testing import CliRunner

from scraping_pipeline import __version__
from scraping_pipeline.cli import app


def test_version_command_prints_package_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__
