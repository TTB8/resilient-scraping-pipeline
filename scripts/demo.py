"""Offline demo: the whole pipeline against the local fixture server, no internet needed.

Act 1 runs the books site cleanly and prints the run report. Act 2 serves the quotes site, starts
answering 429 with ``Retry-After`` and then a 403 block page: the pipeline backs off, records the
block, stops the domain and still delivers what it fetched (exit code 3).

Run it with ``make demo`` (or ``uv run python scripts/demo.py``). Everything goes to a temporary
directory; pass ``--keep DIR`` to keep the outputs there instead.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))

from fixture_server import FixtureServer, Phase  # noqa: E402 - test helper, path set above

# Short waits so the demo finishes in seconds; every other politeness setting keeps its default.
FAST = {
    "PIPELINE_REQUESTS_PER_SECOND": "5",
    "PIPELINE_BURST": "5",
    "PIPELINE_BACKOFF_BASE_S": "0.2",
    "PIPELINE_BACKOFF_CAP_S": "1",
    "PIPELINE_LOG_LEVEL": "WARNING",
    "PIPELINE_LOG_FORMAT": "console",
}


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep", type=Path, help="keep the outputs in this directory")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as scratch:
        base = args.keep or Path(scratch)

        _banner("Act 1: the books site, served locally from the saved pages")
        books = FixtureServer("books").start()
        try:
            code = _run("books", books, base / "books")
        finally:
            books.stop()
        print(f"\nexit code {code} (0: data passed validation)")

        _banner("Act 2: the quotes site starts rate-limiting, then serves a block page")
        quotes = FixtureServer("quotes").start()
        quotes.scenario(Phase.serve(2), Phase.rate_limited("1", times=1), Phase.block())
        try:
            code = _run("quotes", quotes, base / "quotes")
        finally:
            quotes.stop()
        print(f"\nexit code {code} (3: a domain was stopped; the report says why)")
        print(f"\nrequests the server saw per path: {dict(sorted(quotes.hit_counts().items()))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
