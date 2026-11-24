"""Command-line entry point (``pipeline``): parse arguments, call a stage, map its result to an exit code.

Every command is a thin shell over a ``stages.run_<stage>`` function, which is where the work and
the tests are. The CLI builds :class:`~scraping_pipeline.config.Settings` (environment and ``.env``
first, then the command-line overrides), configures logging (JSON lines to stderr, so stdout stays
for command output such as the run report), and turns the result into an exit code.

Exit codes are part of the interface, because a scheduler (cron, CI, an orchestrator) reacts to
them without reading logs:

====  =========================================================================================
0     everything ran and the data passed validation
1     validation failed (the files are still written, so the reason can be read). Only ever this:
      a crash is 4, never 1
2     usage or environment error: a bad option or setting, a stage's input missing (run the
      previous stage first), another pipeline process holding the state database, a data
      directory that cannot be written or opened (the message names the path), or Chromium not
      installed for the JavaScript site (run ``make browsers``)
3     a domain was stopped (blocked, a challenge, rate limited beyond our limits). The other
      stages still ran on what was fetched; the report says why. Distinct from 1 because the cure
      differs: wait, look at the site, then ``fetch --retry-failed``; not fix a parser
4     internal error: an unexpected exception, which is a bug. It is logged with its traceback
      (``internal_error`` on stderr); nothing about the data should be inferred from the run
130   interrupted (Ctrl-C). Everything completed so far is kept; re-run to resume
====  =========================================================================================

When several apply to ``pipeline run``, the most actionable wins: 130, then 3 (a stopped domain
usually explains a failed validation too), then 1.

Every command body runs under :func:`_command`, which turns any exception that is not one of the
mapped ones above into exit 4, so "the program crashed" can never be read as "the data failed".
"""

from __future__ import annotations

import functools
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from enum import IntEnum
from pathlib import Path
from typing import Annotated, Any

import duckdb
import typer
from playwright.async_api import Error as PlaywrightError
from pydantic import ValidationError

from scraping_pipeline import __version__
from scraping_pipeline.config import Settings
from scraping_pipeline.core.types import FrontierStatus, Site
from scraping_pipeline.core.urls import domain as domain_of
from scraping_pipeline.logging import configure_logging, get_logger
from scraping_pipeline.stages import (
    run_discover,
    run_export,
    run_fetch,
    run_parse,
    run_validate,
)
from scraping_pipeline.stages.common import StageInputError, stopped_domains, utc_now_iso
from scraping_pipeline.stages.fetch import FetchStageResult
from scraping_pipeline.storage.state import (
    DomainEventKind,
    RunRecord,
    StateLockedError,
    StateStore,
)

app = typer.Typer(help="Polite, resumable web data pipeline.", no_args_is_help=True)
log = get_logger(__name__)

#: How many recent runs ``pipeline status`` lists.
STATUS_RUNS = 10


class ExitCode(IntEnum):
    OK = 0
    VALIDATION_FAILED = 1
    USAGE = 2
    DOMAIN_STOPPED = 3
    INTERNAL_ERROR = 4
    INTERRUPTED = 130


SiteOption = Annotated[Site, typer.Option("--site", help="Which site to run.", show_choices=True)]
DataDirOption = Annotated[
    Path | None, typer.Option("--data-dir", help="Cache, state and outputs (PIPELINE_DATA_DIR).")
]
MaxPagesOption = Annotated[
    int | None, typer.Option("--max-pages", help="Frontier size cap per site (PIPELINE_MAX_PAGES).")
]
MaxDepthOption = Annotated[
    int | None,
    typer.Option("--max-depth", help="Link depth cap from the seeds (PIPELINE_MAX_DEPTH)."),
]


def _fail(message: str, code: ExitCode) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(int(code))


def _settings(
    data_dir: Path | None,
    max_pages: int | None,
    max_depth: int | None,
    *,
    writes: bool = True,
) -> Settings:
    overrides: dict[str, Any] = {
        name: value
        for name, value in (
            ("data_dir", data_dir),
            ("max_pages", max_pages),
            ("max_depth", max_depth),
        )
        if value is not None
    }
    try:
        settings = Settings(**overrides)
    except ValidationError as exc:
        raise _fail(f"invalid settings:\n{exc}", ExitCode.USAGE) from None
    configure_logging(settings.log_level, settings.log_format)
    if writes:
        _require_writable(settings.data_dir)
    return settings


def _data_dir_message(path: Path, detail: object) -> str:
    return (
        f"the data directory {path} cannot be written or opened ({detail}). Check that it exists "
        "and that this user may write to it (PIPELINE_DATA_DIR or --data-dir to use another). "
        'In Docker, run as your own user: docker run --user "$(id -u):$(id -g)" ...'
    )


def _require_writable(path: Path) -> None:
    """Fail with exit 2 and the path, before any stage starts, if ``path`` cannot be written."""
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=path):
            pass
    except OSError as exc:
        raise _fail(_data_dir_message(path, exc.strerror or exc), ExitCode.USAGE) from None


def _browser_missing(exc: BaseException) -> bool:
    """Playwright's launch failure for a browser that is not installed (or cannot start)."""
    text = str(exc)
    return isinstance(exc, PlaywrightError) and (
        "Executable doesn't exist" in text or "playwright install" in text
    )


def _is_under(filename: object, root: Path) -> bool:
    if not isinstance(filename, str | Path):
        return False
    try:
        return Path(filename).resolve().is_relative_to(root.resolve())
    except OSError:
        return False


@contextmanager
def _guarded(settings: Settings) -> Iterator[None]:
    """Map the exceptions a user or the environment can cause to exit codes.

    Anything else propagates to :func:`_command`, which reports it as an internal error (4).
    """
    try:
        yield
    except KeyboardInterrupt:
        raise _fail(
            "interrupted; progress is saved, re-run to resume", ExitCode.INTERRUPTED
        ) from None
    except (StageInputError, StateLockedError) as exc:
        raise _fail(str(exc), ExitCode.USAGE) from None
    except PlaywrightError as exc:
        if not _browser_missing(exc):
            raise
        first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        raise _fail(
            "the browser for the JavaScript-rendered site is not installed or cannot start: "
            "run `make browsers` (uv run playwright install chromium; in Docker, build with "
            f"--build-arg WITH_BROWSER=1). Playwright said: {first_line}",
            ExitCode.USAGE,
        ) from None
    except duckdb.IOException as exc:
        raise _fail(_data_dir_message(settings.state_db, exc), ExitCode.USAGE) from None
    except OSError as exc:
        if not _is_under(exc.filename, settings.data_dir):
            raise
        raise _fail(
            _data_dir_message(Path(str(exc.filename)), exc.strerror or exc), ExitCode.USAGE
        ) from None


def _command[**P, R](body: Callable[P, R]) -> Callable[P, R]:
    """Run a command body; an exception nobody mapped is a bug: log it with its traceback, exit 4."""

    @functools.wraps(body)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return body(*args, **kwargs)
        except (typer.Exit, typer.Abort, typer.TyperException):
            raise
        except Exception as exc:
            log.exception("internal_error", error=f"{type(exc).__name__}: {exc}")
            raise _fail(
                f"internal error ({type(exc).__name__}: {exc}). This is a bug, not a fact about "
                "the data; the traceback is logged above as internal_error.",
                ExitCode.INTERNAL_ERROR,
            ) from None

    return wrapper


def _call[T](settings: Settings, stage: Callable[[], T]) -> T:
    with _guarded(settings):
        return stage()


def _fetch_code(result: FetchStageResult) -> ExitCode:
    if result.interrupted:
        return ExitCode.INTERRUPTED
    if result.stopped_domains:
        for name, reason in result.stopped_domains.items():
            typer.echo(f"domain stopped: {name}: {reason}", err=True)
        return ExitCode.DOMAIN_STOPPED
    return ExitCode.OK


@app.callback()
def main() -> None:
    """Polite, resumable web data pipeline (keeps ``pipeline`` a command group)."""


@app.command()
@_command
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command()
@_command
def discover(
    site: SiteOption,
    data_dir: DataDirOption = None,
    max_pages: MaxPagesOption = None,
    max_depth: MaxDepthOption = None,
) -> None:
    """Add seed URLs and plan links from pages already cached (offline)."""
    settings = _settings(data_dir, max_pages, max_depth)
    result = _call(settings, lambda: run_discover(settings, site))
    typer.echo(
        f"discover: {result.seeds_added} seeds and {result.links_added} links added, "
        f"frontier {result.frontier_size}"
    )


@app.command()
@_command
def fetch(
    site: SiteOption,
    data_dir: DataDirOption = None,
    max_pages: MaxPagesOption = None,
    max_depth: MaxDepthOption = None,
    retry_failed: Annotated[
        bool,
        typer.Option(
            "--retry-failed",
            help="Re-queue failed_exhausted and stopped entries first (one more attempt each).",
        ),
    ] = False,
) -> None:
    """Fetch every pending URL politely into the cache (the only stage that uses the network)."""
    settings = _settings(data_dir, max_pages, max_depth)
    result = _call(settings, lambda: run_fetch(settings, site, retry_failed=retry_failed))
    typer.echo(_fetch_line(result))
    raise typer.Exit(int(_fetch_code(result)))


def _fetch_line(result: FetchStageResult) -> str:
    statuses = ", ".join(f"{k.value} {v}" for k, v in result.statuses.items() if v)
    return (
        f"fetch {result.run_id}: {result.attempts} attempts"
        + (f" ({result.requests} requests)" if result.requests is not None else "")
        + f", {result.recovered} recovered from "
        f"cache, {result.links_added} links added; frontier: {statuses or 'empty'}"
        + (" (interrupted)" if result.interrupted else "")
    )


@app.command()
@_command
def parse(
    site: SiteOption,
    data_dir: DataDirOption = None,
    max_pages: MaxPagesOption = None,
    max_depth: MaxDepthOption = None,
) -> None:
    """Parse the cached pages into draft records with lineage (offline, deterministic)."""
    settings = _settings(data_dir, max_pages, max_depth)
    result = _call(settings, lambda: run_parse(settings, site))
    typer.echo(
        f"parse {result.parse_run_id}: {result.records} records from {result.pages_parsed} "
        f"pages, {result.warnings} warnings"
    )


@app.command()
@_command
def validate(
    site: SiteOption,
    data_dir: DataDirOption = None,
    max_pages: MaxPagesOption = None,
    max_depth: MaxDepthOption = None,
) -> None:
    """Check every draft against the contract; quarantine what fails. Exit 1 on a failing verdict."""
    settings = _settings(data_dir, max_pages, max_depth)
    result = _call(settings, lambda: run_validate(settings, site))
    report = result.report
    typer.echo(f"validate: {report.verdict.value.upper()}: {report.verdict_reason}")
    raise typer.Exit(int(ExitCode.OK if result.passed else ExitCode.VALIDATION_FAILED))


@app.command()
@_command
def export(
    site: SiteOption,
    data_dir: DataDirOption = None,
    max_pages: MaxPagesOption = None,
    max_depth: MaxDepthOption = None,
) -> None:
    """Write Parquet and CSV deliverables and the run report. Exit 1 if validation failed."""
    settings = _settings(data_dir, max_pages, max_depth)
    result = _call(settings, lambda: run_export(settings, site))
    typer.echo(f"export: {result.paths.output}")
    raise typer.Exit(int(ExitCode.OK if result.passed else ExitCode.VALIDATION_FAILED))


@app.command()
@_command
def run(
    site: SiteOption,
    data_dir: DataDirOption = None,
    max_pages: MaxPagesOption = None,
    max_depth: MaxDepthOption = None,
    retry_failed: Annotated[
        bool, typer.Option("--retry-failed", help="Passed to the fetch stage.")
    ] = False,
) -> None:
    """Run all five stages in order and print the run report (Markdown) to stdout."""
    settings = _settings(data_dir, max_pages, max_depth)
    started_at = utc_now_iso()
    _call(settings, lambda: run_discover(settings, site))
    fetched = _call(settings, lambda: run_fetch(settings, site, retry_failed=retry_failed))
    fetch_code = _fetch_code(fetched)
    if fetch_code is ExitCode.INTERRUPTED:
        raise _fail("interrupted; progress is saved, re-run to resume", ExitCode.INTERRUPTED)
    _call(settings, lambda: run_parse(settings, site))
    validated = _call(settings, lambda: run_validate(settings, site))
    exported = _call(settings, lambda: run_export(settings, site, started_at=started_at))
    typer.echo(exported.markdown, nl=False)
    if fetch_code is ExitCode.DOMAIN_STOPPED:
        raise typer.Exit(int(ExitCode.DOMAIN_STOPPED))
    raise typer.Exit(int(ExitCode.OK if validated.passed else ExitCode.VALIDATION_FAILED))


@app.command()
@_command
def status(
    site: SiteOption,
    data_dir: DataDirOption = None,
) -> None:
    """Show frontier counts by status, open and stopped domains, and the latest runs.

    Read-only: in a data directory with no state database yet it reports an empty frontier and
    creates nothing.
    """
    settings = _settings(data_dir, None, None, writes=False)
    counts: dict[FrontierStatus, int] = dict.fromkeys(FrontierStatus, 0)
    stopped: dict[str, str] = {}
    seen: set[str] = set()
    runs: list[RunRecord] = []
    if settings.state_db.exists():
        with _guarded(settings), StateStore.open(settings.state_db) as store:
            counts = store.counts(site)
            stopped = stopped_domains(store, site)
            seen = {
                e.domain for e in store.domain_events(site) if e.event is DomainEventKind.ROBOTS
            }
            seen |= {domain_of(e.url) for e in store.entries(site)}
            runs = store.runs(site)[-STATUS_RUNS:]
    lines = [f"site: {site.value}", "", "frontier:"]
    lines += [f"  {status.value:<17}{count}" for status, count in counts.items()]
    lines += ["", "domains:"]
    lines += [f"  {name}: open" for name in sorted(seen - set(stopped))]
    lines += [f"  {name}: stopped ({reason})" for name, reason in stopped.items()]
    if not seen and not stopped:
        lines.append("  none yet")
    lines += ["", "last runs:"]
    lines += [
        f"  {r.started_at}  {r.stage.value:<9}{r.status.value:<12}{r.run_id}"
        for r in reversed(runs)
    ]
    if not runs:
        lines.append("  none yet")
    typer.echo("\n".join(lines))
