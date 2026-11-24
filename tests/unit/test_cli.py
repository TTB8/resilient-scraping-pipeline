"""The CLI is thin: arguments in, a stage called, its result mapped to an exit code.

The fetch stage is swapped for one driven by a scripted fetcher in virtual time (the CLI's own
``run_fetch`` reference is patched), so these tests exercise the real commands with no socket.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner, Result

from scraping_pipeline import __version__, cli
from scraping_pipeline.config import Settings
from scraping_pipeline.core.types import Site
from scraping_pipeline.fetch.fake import Step, block_page
from scraping_pipeline.stages.fetch import FetchStageResult, run_fetch
from scraping_pipeline.storage.state import StateStore
from stage_support import saved_site_fetcher, virtual_runner

pytestmark = pytest.mark.usefixtures("no_real_fetchers", "restore_logging")

FAST_ENV = {
    "PIPELINE_REQUESTS_PER_SECOND": "5",
    "PIPELINE_BURST": "10",
    "PIPELINE_BACKOFF_BASE_S": "0.01",
    "PIPELINE_BACKOFF_CAP_S": "0.05",
    "PIPELINE_LOG_FORMAT": "json",
}


@pytest.fixture
def invoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., Result]:
    """Run ``pipeline <args> --data-dir <tmp>`` with a clean environment and no ``.env``."""
    for key in list(os.environ):
        if key.startswith("PIPELINE_"):
            monkeypatch.delenv(key)
    monkeypatch.chdir(tmp_path)  # no .env file here

    def run(*args: str, data_dir: bool = True) -> Result:
        extra = ["--data-dir", str(tmp_path / "data")] if data_dir else []
        return CliRunner().invoke(cli.app, [*args, *extra], env=FAST_ENV)

    return run


def _ctrl_c(coro: Any) -> Any:
    coro.close()
    raise KeyboardInterrupt  # what asyncio.run raises after Ctrl-C


def scripted_fetch(
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Step | list[Step]] | None = None,
    *,
    interrupt: bool = False,
) -> None:
    """Make the CLI's fetch use the saved sites as a scripted fetcher, in virtual time."""

    def fake_run_fetch(settings: Settings, site: Site, **kwargs: Any) -> FetchStageResult:
        fixtures = "books" if site is Site.BOOKS else "quotes"
        factory, _ = saved_site_fetcher(fixtures, overrides)
        runner, timing = virtual_runner()
        return run_fetch(
            settings,
            site,
            fetcher_factory=factory,
            timing=timing,
            runner=_ctrl_c if interrupt else runner,
            **kwargs,
        )

    monkeypatch.setattr(cli, "run_fetch", fake_run_fetch)


def test_version(invoke: Callable[..., Result]) -> None:
    result = invoke("version", data_dir=False)
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


@pytest.mark.slow
def test_run_prints_the_report_and_exits_0_when_the_data_passes(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted_fetch(monkeypatch)
    result = invoke("run", "--site", "books", "--max-pages", "30")
    assert result.exit_code == 0, result.stdout
    assert result.stdout.startswith("# Run report: books")
    assert "**PASS**" in result.stdout
    assert '"stage": "export"' in result.stderr  # logs go to stderr, as JSON lines
    assert "stage_started" not in result.stdout


@pytest.mark.slow
def test_run_exits_1_when_validation_fails(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted_fetch(monkeypatch)
    result = invoke("run", "--site", "quotes")
    assert result.exit_code == 1
    assert "**FAIL**" in result.stdout


def test_a_stopped_domain_exits_3_and_the_other_stages_still_run(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    scripted_fetch(monkeypatch, {"https://quotes.toscrape.com/page/2/": block_page()})
    result = invoke("run", "--site", "quotes")
    assert result.exit_code == 3
    assert "domain stopped: quotes.toscrape.com" in result.stderr
    assert "| quotes.toscrape.com | blocked |" in result.stdout
    assert (tmp_path / "data" / "output" / "quotes" / "quotes.parquet").is_file()

    status = invoke("status", "--site", "quotes")
    assert status.exit_code == 0
    assert "quotes.toscrape.com: stopped (" in status.stdout
    fetched = invoke("fetch", "--site", "quotes")  # still stopped: the data is incomplete
    assert fetched.exit_code == 3


@pytest.mark.slow
def test_each_stage_runs_on_its_own(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted_fetch(monkeypatch)
    assert invoke("discover", "--site", "quotes").stdout.startswith("discover: 1 seeds")
    fetched = invoke("fetch", "--site", "quotes")
    assert fetched.exit_code == 0
    assert "done 4" in fetched.stdout
    parsed = invoke("parse", "--site", "quotes")
    assert parsed.exit_code == 0
    assert "22 records" in parsed.stdout
    validated = invoke("validate", "--site", "quotes")
    assert validated.exit_code == 1
    assert validated.stdout.startswith("validate: FAIL")
    exported = invoke("export", "--site", "quotes")
    assert exported.exit_code == 1

    status = invoke("status", "--site", "quotes")
    assert "done             4" in status.stdout
    assert "quotes.toscrape.com: open" in status.stdout
    assert status.stdout.count("quotes-") == 0  # no other site's runs
    assert [line.split()[1] for line in status.stdout.split("last runs:\n")[1].splitlines()][
        :5
    ] == ["export", "validate", "parse", "fetch", "discover"]


def test_ctrl_c_exits_130_and_records_the_interruption(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    scripted_fetch(monkeypatch, interrupt=True)
    result = invoke("run", "--site", "quotes")
    assert result.exit_code == 130
    assert "interrupted" in result.stderr
    settings = Settings(_env_file=None, data_dir=tmp_path / "data")
    with StateStore.open(settings.state_db) as store:
        assert store.runs(Site.QUOTES)[-1].status.value == "interrupted"
    assert invoke("fetch", "--site", "quotes").exit_code == 130


def test_ctrl_c_in_an_offline_stage_exits_130(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupted(*_: object, **__: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run_parse", interrupted)
    assert invoke("parse", "--site", "quotes").exit_code == 130


def test_usage_errors_exit_2(invoke: Callable[..., Result], tmp_path: Path) -> None:
    assert invoke("run", "--site", "films").exit_code == 2
    bad = invoke("fetch", "--site", "quotes", "--max-pages", "0")
    assert bad.exit_code == 2
    assert "invalid settings" in bad.stderr
    missing = invoke("validate", "--site", "quotes")
    assert missing.exit_code == 2
    assert "run the previous stage first" in missing.stderr

    settings = Settings(_env_file=None, data_dir=tmp_path / "data")
    with StateStore.open(settings.state_db):  # another pipeline holds the state database
        locked = invoke("status", "--site", "quotes")
    assert locked.exit_code == 2
    assert "another pipeline process" in locked.stderr


def test_status_of_a_fresh_data_dir(invoke: Callable[..., Result]) -> None:
    result = invoke("status", "--site", "books")
    assert result.exit_code == 0
    assert "pending          0" in result.stdout
    assert result.stdout.count("none yet") == 2


def test_cli_options_override_the_environment(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[Settings] = []

    def spy(settings: Settings, site: Site) -> Any:
        seen.append(settings)
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run_discover", spy)
    monkeypatch.setenv("PIPELINE_MAX_DEPTH", "7")
    invoke("discover", "--site", "books", "--max-pages", "9", "--max-depth", "2")
    assert (seen[0].max_pages, seen[0].max_depth) == (9, 2)
