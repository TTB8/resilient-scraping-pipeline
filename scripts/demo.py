"""Offline demo: the whole pipeline against the local fixture server, no internet needed.

Act 1 runs the books site cleanly and prints the run report (expected exit code 0).

Act 2 runs the quotes site while the server pushes back, page by page:

1. the first request for ``/page/2/`` answers ``429`` with ``Retry-After: 1``. The pipeline slows
   the domain down, waits at least that second, and asks again; the second answer is the page;
2. the first author page linked from page 2 (``/author/Allen-Saunders``) answers ``403`` with a
   block page, so the pipeline stops the domain: every URL still pending is marked ``stopped``
   and nothing more is sent;
3. parse, validate and export still run on what was fetched. The valid rows (the quotes whose
   authors were fetched, and those authors) are delivered; the rest are in ``quarantine.csv``
   with their reasons, and the report names the block. Expected exit code 3.

The server's log is printed as a timeline, so the 429, the wait of at least one second and the
block are visible. The demo checks its own story and exits non-zero if any of it does not hold:
the exit codes (0, then 3), the honoured ``Retry-After``, the block in the report, and valid rows
in the delivered files. CI runs it (``make demo``), so a regression in any of those fails the build.

Run it with ``make demo`` (or ``uv run python scripts/demo.py``). Everything goes to a temporary
directory; pass ``--keep DIR`` to keep the outputs there instead.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from http import HTTPStatus
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))

from fixture_server import FixtureServer, Hit, Phase  # noqa: E402 - test helper, path set above

# Short waits so the demo finishes in seconds; every other politeness setting keeps its default.
FAST = {
    "PIPELINE_REQUESTS_PER_SECOND": "5",
    "PIPELINE_BURST": "5",
    "PIPELINE_BACKOFF_BASE_S": "0.2",
    "PIPELINE_BACKOFF_CAP_S": "1",
    "PIPELINE_LOG_LEVEL": "WARNING",
    "PIPELINE_LOG_FORMAT": "console",
}

RATE_LIMITED_PATH = "/page/2/"
RETRY_AFTER_S = 1
BLOCKED_PATH = "/author/Allen-Saunders"  # linked only from page 2, so asked for after its retry

# The exit codes each act must end with (cli.ExitCode; the demo runs the CLI as a subprocess).
EXIT_OK = 0
EXIT_DOMAIN_STOPPED = 3


class DemoError(Exception):
    """The run did not tell the story the README promises."""


def _run(site: str, server: FixtureServer, data_dir: Path) -> int:
    env = {k: v for k, v in os.environ.items() if not k.startswith("PIPELINE_")}
    env |= FAST | {
        "PIPELINE_DATA_DIR": str(data_dir),
        "PIPELINE_BOOKS_BASE_URL": server.base_url + "/",
        "PIPELINE_QUOTES_BASE_URL": server.base_url + "/",
    }
    command = [sys.executable, "-m", "scraping_pipeline", "run", "--site", site]
    return subprocess.run(command, env=env, cwd=ROOT, check=False).returncode  # noqa: S603


def _banner(text: str) -> None:
    print(f"\n{'=' * 78}\n{text}\n{'=' * 78}\n", flush=True)


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise DemoError(message)
    print(f"  ok: {message}")


def _report(data_dir: Path, site: str) -> dict[str, Any]:
    path = data_dir / "output" / site / "run_report.json"
    if not path.is_file():
        raise DemoError(f"no run report at {path}")
    report: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return report


def _csv_rows(data_dir: Path, site: str, table: str) -> int:
    with (data_dir / "output" / site / f"{table}.csv").open(encoding="utf-8", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def _timeline(log: list[Hit]) -> None:
    start = log[0].at if log else 0.0
    print("what the server saw (seconds from the first request):")
    for hit in log:
        note = ""
        if hit.status == HTTPStatus.TOO_MANY_REQUESTS:
            note = f"  <- rate limited, Retry-After: {RETRY_AFTER_S}"
        elif hit.status == HTTPStatus.FORBIDDEN:
            note = "  <- block page: the domain is stopped"
        elif hit.path == RATE_LIMITED_PATH and hit.status == HTTPStatus.OK:
            note = "  <- the retry, after the wait"
        print(f"  +{hit.at - start:5.2f}s  {hit.status}  {hit.path}{note}")


def _act_1(base: Path) -> None:
    _banner("Act 1: the books site, served locally from the saved pages")
    books = FixtureServer("books").start()
    try:
        code = _run("books", books, base / "books")
    finally:
        books.stop()
    print(f"\nexit code {code}\n")
    _check(code == EXIT_OK, "exit code 0: the data passed validation")
    validation = _report(base / "books", "books")["validation"]
    _check(validation["verdict"] == "pass", "the report's verdict is PASS")
    _check(_csv_rows(base / "books", "books", "books") > 0, "books.csv holds the books")


def _act_2(base: Path) -> None:
    _banner("Act 2: the quotes site rate-limits one page, then serves a block page")
    quotes = FixtureServer("quotes").start()
    quotes.path_scenario(RATE_LIMITED_PATH, Phase.rate_limited(str(RETRY_AFTER_S), times=1))
    quotes.path_scenario(BLOCKED_PATH, Phase.block())
    try:
        code = _run("quotes", quotes, base / "quotes")
    finally:
        quotes.stop()
    log = quotes.log()
    print(f"\nexit code {code}\n")
    _timeline(log)
    print()

    _check(
        code == EXIT_DOMAIN_STOPPED,
        "exit code 3: a domain was stopped, and the other stages still ran",
    )
    page2 = [hit for hit in log if hit.path == RATE_LIMITED_PATH]
    _check(
        [hit.status for hit in page2] == [429, 200],
        f"{RATE_LIMITED_PATH} was rate limited once, then fetched on the retry",
    )
    waited = page2[1].at - page2[0].at
    _check(waited >= RETRY_AFTER_S, f"the retry waited {waited:.2f}s, at least the Retry-After")
    blocked = [i for i, hit in enumerate(log) if hit.status == HTTPStatus.FORBIDDEN]
    _check(bool(blocked), f"{BLOCKED_PATH} answered with a block page")
    report = _report(base / "quotes", "quotes")
    stops = report["domains_stopped"]
    _check(
        [s["response_class"] for s in stops] == ["blocked"],
        "the report names the stopped domain and why (blocked)",
    )
    validation = report["validation"]
    delivered = {t: _csv_rows(base / "quotes", "quotes", t) for t in ("quotes", "authors")}
    _check(
        int(validation["valid_rows"]) > 0 and all(n > 0 for n in delivered.values()),
        f"valid rows were still delivered: {delivered['quotes']} quotes, "
        f"{delivered['authors']} authors (the rest are in quarantine.csv with reasons)",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep", type=Path, help="keep the outputs in this directory")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as scratch:
        base = args.keep or Path(scratch)
        try:
            _act_1(base)
            _act_2(base)
        except DemoError as failure:
            print(f"\ndemo FAILED: {failure}", file=sys.stderr)
            return 1
    print("\ndemo passed: both acts told the story above.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
