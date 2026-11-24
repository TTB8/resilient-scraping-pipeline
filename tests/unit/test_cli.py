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
from playwright.async_api import Error as PlaywrightError
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
    # The valid rows are still delivered, and the report says in words that the batch failed.
    assert "**This batch failed validation.**" in result.stdout
    assert "- `quotes.parquet`" in result.stdout


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
    assert "already open" in locked.stderr or "another pipeline process" in locked.stderr


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


# --- exit codes for crashes and a broken environment (R2-06) --------------------------------------


def test_an_unexpected_exception_exits_4_with_its_traceback_logged(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_: object, **__: object) -> None:
        raise KeyError("failures")  # e.g. a malformed staging file nobody anticipated

    monkeypatch.setattr(cli, "run_parse", broken)
    result = invoke("parse", "--site", "quotes")
    assert result.exit_code == 4  # never 1: a crash must not read as "validation failed"
    assert "internal error (KeyError" in result.stderr
    assert '"event": "internal_error"' in result.stderr
    assert "Traceback (most recent call last)" in result.stderr


def test_a_crash_inside_run_exits_4_not_1(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    scripted_fetch(monkeypatch)

    def broken(*_: object, **__: object) -> None:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(cli, "run_export", broken)
    assert invoke("run", "--site", "quotes").exit_code == 4


def test_a_browser_that_is_not_installed_exits_2_and_says_make_browsers(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_chromium(*_: object, **__: object) -> None:
        raise PlaywrightError(
            "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium/chrome\n"
            "Looks like Playwright was just installed or updated. Please run: playwright install"
        )

    monkeypatch.setattr(cli, "run_fetch", no_chromium)
    result = invoke("fetch", "--site", "quotes-js")
    assert result.exit_code == 2
    assert "run `make browsers`" in result.stderr
    assert "Executable doesn't exist" in result.stderr


def test_any_other_playwright_error_is_an_internal_error(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_: object, **__: object) -> None:
        raise PlaywrightError("Target page, context or browser has been closed")

    monkeypatch.setattr(cli, "run_fetch", broken)
    assert invoke("fetch", "--site", "quotes-js").exit_code == 4


def test_a_data_dir_that_is_a_file_exits_2_with_the_path(
    invoke: Callable[..., Result], tmp_path: Path
) -> None:
    (tmp_path / "data").write_text("not a directory")
    for command in ("discover", "fetch", "parse", "validate", "export", "run"):
        result = invoke(command, "--site", "books")
        assert result.exit_code == 2, (command, result.stderr)
        assert str(tmp_path / "data") in result.stderr
        assert "cannot be written or opened" in result.stderr


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
def test_an_unwritable_data_dir_exits_2_and_names_the_docker_fix(
    invoke: Callable[..., Result], tmp_path: Path
) -> None:
    data = tmp_path / "data"
    data.mkdir()
    data.chmod(0o500)
    try:
        result = invoke("discover", "--site", "books")
    finally:
        data.chmod(0o700)
    assert result.exit_code == 2
    assert str(data) in result.stderr
    assert '--user "$(id -u):$(id -g)"' in result.stderr
    assert not (data / "state.duckdb").exists()


def test_an_os_error_on_a_data_file_exits_2_with_its_path(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "data" / "quotes" / "parsed" / "records.jsonl"

    def disk_full(*_: object, **__: object) -> None:
        raise OSError(28, "No space left on device", str(target))

    monkeypatch.setattr(cli, "run_parse", disk_full)
    result = invoke("parse", "--site", "quotes")
    assert result.exit_code == 2
    assert str(target) in result.stderr
    assert "No space left on device" in result.stderr


def test_an_os_error_outside_the_data_dir_is_an_internal_error(
    invoke: Callable[..., Result], monkeypatch: pytest.MonkeyPatch
) -> None:
    def elsewhere(*_: object, **__: object) -> None:
        raise OSError(2, "No such file or directory", "/nonexistent/somewhere/else")

    monkeypatch.setattr(cli, "run_parse", elsewhere)
    assert invoke("parse", "--site", "quotes").exit_code == 4


def test_an_unopenable_state_database_exits_2_with_its_path(
    invoke: Callable[..., Result], tmp_path: Path
) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "state.duckdb").write_bytes(b"this is not a duckdb file" * 100)
    result = invoke("discover", "--site", "books")
    assert result.exit_code == 2, result.stderr
    assert str(data / "state.duckdb") in result.stderr


def test_status_does_not_create_a_state_database(
    invoke: Callable[..., Result], tmp_path: Path
) -> None:
    data = tmp_path / "data"
    data.mkdir()
    result = invoke("status", "--site", "books")
    assert result.exit_code == 0
    assert "pending          0" in result.stdout
    assert list(data.iterdir()) == []
    assert not (tmp_path / "data2").exists()
    elsewhere = CliRunner().invoke(
        cli.app, ["status", "--site", "books", "--data-dir", str(tmp_path / "data2")], env=FAST_ENV
    )
    assert elsewhere.exit_code == 0
    assert not (tmp_path / "data2").exists()
