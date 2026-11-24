"""``pipeline run`` end to end: the real CLI, real HTTP, against the local fixture server.

The fixture server holds a handful of saved pages per site. Every other page the crawl discovers
answers 404, which the pipeline records as ``not_found`` (permanent, never retried): exactly what
the run report should say, and realistic (pages do disappear). So the row counts below are the
saved fixtures' row counts, not the live sites'.

Also here: the "429s, then a block page" scenario through the real CLI (the domain is stopped,
exit code 3, the report says why, the other stages still run), and the JavaScript variant through
a real browser (marked ``browser``: it skips where Chromium cannot launch, and CI requires it).
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner, Result

from fixture_server import FixtureServer, Phase
from scraping_pipeline import cli
from scraping_pipeline.config import Settings
from scraping_pipeline.core.types import FrontierStatus, Site
from scraping_pipeline.storage.state import StateStore

pytestmark = pytest.mark.usefixtures("restore_logging")

FAST_ENV = {
    "PIPELINE_REQUESTS_PER_SECOND": "5",
    "PIPELINE_BURST": "10",
    "PIPELINE_MAX_CONCURRENCY_PER_DOMAIN": "4",
    "PIPELINE_BACKOFF_BASE_S": "0.01",
    "PIPELINE_BACKOFF_CAP_S": "0.05",
    "PIPELINE_MAX_RETRY_AFTER_S": "5",
    "PIPELINE_REQUEST_TIMEOUT_S": "10",
}


@pytest.fixture
def pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., Result]:
    """``pipeline <args> --data-dir <tmp>`` with both sites pointed at ``server``."""
    for key in list(os.environ):
        if key.startswith("PIPELINE_"):
            monkeypatch.delenv(key)
    monkeypatch.chdir(tmp_path)

    def run(server: FixtureServer, *args: str) -> Result:
        env = {
            **FAST_ENV,
            "PIPELINE_BOOKS_BASE_URL": server.base_url + "/",
            "PIPELINE_QUOTES_BASE_URL": server.base_url + "/",
        }
        return CliRunner().invoke(cli.app, [*args, "--data-dir", str(tmp_path / "data")], env=env)

    return run


def _output(tmp_path: Path, site: str) -> Path:
    return tmp_path / "data" / "output" / site


@pytest.mark.slow
def test_books_end_to_end(
    pipeline: Callable[..., Result],
    make_fixture_server: Callable[..., FixtureServer],
    tmp_path: Path,
) -> None:
    server = make_fixture_server("books")
    result = pipeline(server, "run", "--site", "books")
    assert result.exit_code == 0, result.stdout + result.stderr

    out = _output(tmp_path, "books")
    books = pq.read_table(out / "books.parquet").to_pylist()
    # The three saved product pages, sorted by book_id (the UPC).
    assert {b["title"] for b in books} == {
        "A Light in the Attic",
        "Sharp Objects",
        "It's Only the Himalayas",
    }
    assert [b["book_id"] for b in books] == sorted(b["book_id"] for b in books)
    assert all(b["source_url"].startswith(server.base_url) for b in books)
    assert (out / "books.csv").read_text().count("\n") == 4  # header + 3 rows

    report = json.loads((out / "run_report.json").read_text())
    assert report["validation"]["verdict"] == "pass"
    assert report["validation"]["valid_rows"] == 3
    assert report["parse"]["item_pages_fetched"] == 3
    # Home, catalogue page 2, the two saved categories (+ mystery page 2) and the 3 products.
    assert report["pages"]["fetched"] == 8
    assert set(report["pages"]["failed_by_class"]) == {"not_found"}
    assert report["domains_stopped"] == []
    # Requests are what the politeness bound counts: every page plus robots.txt (no redirects here).
    assert report["pages"]["http_requests"] == sum(server.hit_counts().values())
    assert report["deliverables"] == {
        "published": ["books.parquet", "books.csv"],
        "verdict_failed": False,
    }
    assert result.stdout == (out / "run_report.md").read_text()
    assert max(server.hit_counts().values()) == 1  # every URL asked for once


@pytest.mark.slow
def test_quotes_end_to_end(
    pipeline: Callable[..., Result],
    make_fixture_server: Callable[..., FixtureServer],
    tmp_path: Path,
) -> None:
    server = make_fixture_server("quotes")
    result = pipeline(server, "run", "--site", "quotes")
    # Most authors' pages were never saved (404), so their quotes fail unknown_author: exit 1.
    assert result.exit_code == 1, result.stderr

    out = _output(tmp_path, "quotes")
    authors = pq.read_table(out / "authors.parquet").to_pylist()
    quotes = pq.read_table(out / "quotes.parquet").to_pylist()
    assert [a["author_slug"] for a in authors] == ["Albert-Einstein", "J-K-Rowling"]
    assert {q["author_slug"] for q in quotes} == {"Albert-Einstein", "J-K-Rowling"}
    assert len(quotes) == 6

    report = json.loads((out / "run_report.json").read_text())
    assert report["validation"]["total_rows"] == 22  # 2 listing pages x 10 quotes + 2 authors
    assert report["validation"]["rule_counts"] == {"unknown_author": 14}
    assert report["pages"]["fetched"] == 4
    assert report["pages"]["failed_by_class"] == {"not_found": 14}
    assert "| unknown_author | 14 |" in result.stdout
    quarantine = (out / "quarantine.csv").read_text().splitlines()
    assert len(quarantine) == 15  # header + one line per failure


def test_rate_limited_then_blocked_stops_the_domain_and_exits_3(
    pipeline: Callable[..., Result],
    make_fixture_server: Callable[..., FixtureServer],
    tmp_path: Path,
) -> None:
    server = make_fixture_server("quotes")
    server.scenario(Phase.serve(2), Phase.rate_limited("1", times=1), Phase.block())
    result = pipeline(server, "run", "--site", "quotes")
    assert result.exit_code == 3, result.stderr
    assert "domain stopped: 127.0.0.1" in result.stderr

    out = _output(tmp_path, "quotes")
    report = json.loads((out / "run_report.json").read_text())
    [stop] = report["domains_stopped"]
    assert stop["domain"] == "127.0.0.1"
    assert stop["response_class"] == "blocked"
    assert "403" in stop["reason"]
    assert "| 127.0.0.1 | blocked |" in result.stdout
    # The other stages still ran on what was fetched.
    assert report["parse"]["records_parsed"] == 10
    assert (out / "quotes.parquet").is_file()
    assert (out / "quarantine.csv").is_file()

    settings = Settings(_env_file=None, data_dir=tmp_path / "data")
    with StateStore.open(settings.state_db) as store:
        counts = store.counts(Site.QUOTES)
    assert counts[FrontierStatus.PENDING] == 0
    assert counts[FrontierStatus.STOPPED] > 0
    statuses = [hit.status for hit in server.log() if hit.path != "/robots.txt"]
    assert statuses.count(403) >= 1
    assert 429 in statuses
    # Nothing is sent after the first block page (requests already in flight may still land).
    first_block = statuses.index(403)
    assert len(statuses) - first_block <= int(FAST_ENV["PIPELINE_MAX_CONCURRENCY_PER_DOMAIN"])


@pytest.mark.browser
@pytest.mark.usefixtures("chromium_available")
def test_quotes_js_end_to_end_in_a_real_browser(
    pipeline: Callable[..., Result],
    make_fixture_server: Callable[..., FixtureServer],
    tmp_path: Path,
) -> None:
    server = make_fixture_server("quotes")
    result = pipeline(server, "run", "--site", "quotes-js")
    assert result.exit_code == 0, result.stdout + result.stderr

    out = _output(tmp_path, "quotes-js")
    quotes = pq.read_table(out / "quotes.parquet").to_pylist()
    assert len(quotes) == 20  # /js/ and /js/page/2/, rendered by the page's own script
    assert all(q["author_slug"] is None for q in quotes)  # the JS variant renders no author link
    assert not (out / "authors.parquet").exists()
    report = json.loads((out / "run_report.json").read_text())
    assert report["validation"]["verdict"] == "pass"
    assert report["pages"]["fetched"] == 2
    assert server.hits("/static/jquery.js") >= 1  # the browser really ran the page
