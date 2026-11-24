"""Shared pytest fixtures: the local fixture server, the Chromium probe, and fetched data dirs.

The stage fixtures fetch the saved quotes site (and the books site, capped) through a scripted
fetcher in virtual time once per session; each test gets its own copy of the data dir, so it can
mutate state freely. ``no_real_fetchers`` makes any real fetcher fail the test.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
import structlog

from fixture_server import FixtureServer
from scraping_pipeline.config import Settings
from scraping_pipeline.core.types import Site
from scraping_pipeline.fetch.httpx_fetcher import HttpxFetcher
from scraping_pipeline.fetch.playwright_fetcher import PlaywrightFetcher
from scraping_pipeline.stages import run_discover
from stage_support import fast_fetch, saved_site_fetcher, stage_settings


@pytest.fixture
def make_fixture_server() -> Iterator[Callable[..., FixtureServer]]:
    """Factory: ``make_fixture_server("books", robots_txt="...", gzip=True)``. Every server is
    stopped after."""
    servers: list[FixtureServer] = []

    def factory(
        site: str = "quotes", *, robots_txt: str | None = None, gzip: bool = False
    ) -> FixtureServer:
        server = FixtureServer(site, robots_txt=robots_txt, gzip=gzip).start()
        servers.append(server)
        return server

    yield factory
    for server in servers:
        server.stop()


@pytest.fixture
def fixture_server(make_fixture_server: Callable[..., FixtureServer]) -> FixtureServer:
    """The quotes site on 127.0.0.1, robots.txt 404, no scenario."""
    return make_fixture_server("quotes")


async def _probe_chromium() -> str | None:
    """None when headless Chromium launches here, else the reason it does not."""
    try:
        from playwright.async_api import async_playwright  # noqa: PLC0415 - probe only

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            await browser.close()
    except Exception as exc:
        first_line = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
        return f"{type(exc).__name__}: {first_line[:300]}"
    return None


@pytest.fixture(scope="session")
def chromium_available() -> None:
    """Skip browser tests when Chromium cannot launch (probed once per session).

    Set ``REQUIRE_BROWSER_TESTS=1`` (as CI should, where Chromium is installed) to turn the skip
    into a failure, so a broken browser install cannot silently skip the browser suite.
    """
    reason = asyncio.run(_probe_chromium())
    if reason is None:
        return
    if os.environ.get("REQUIRE_BROWSER_TESTS") == "1":
        pytest.fail(f"Chromium cannot launch and REQUIRE_BROWSER_TESTS=1: {reason}")
    pytest.skip(f"Chromium cannot launch on this machine: {reason}")


# --- stage fixtures ------------------------------------------------------------------------------
#: Small enough to be fast, large enough to reach one saved product page.
BOOKS_MAX_PAGES = 30


@pytest.fixture
def no_real_fetchers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stage unit tests use scripted fetchers only; a real one here is a test bug, not a crawl."""

    async def refuse(self: object, url: str) -> None:
        raise AssertionError(f"a real fetcher was used in a unit test: {url}")

    monkeypatch.setattr(HttpxFetcher, "fetch", refuse)
    monkeypatch.setattr(PlaywrightFetcher, "fetch", refuse)


def _fetched(root: Path, site: Site, fixtures: str, **overrides: object) -> Path:
    settings = stage_settings(root, **overrides)
    run_discover(settings, site)
    factory, _ = saved_site_fetcher(fixtures)
    fast_fetch(settings, site, factory)
    return root


@pytest.fixture(scope="session")
def _fetched_quotes(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _fetched(tmp_path_factory.mktemp("quotes-fetched"), Site.QUOTES, "quotes")


@pytest.fixture(scope="session")
def _fetched_books(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _fetched(
        tmp_path_factory.mktemp("books-fetched"),
        Site.BOOKS,
        "books",
        max_pages=BOOKS_MAX_PAGES,
    )


def _copy(source: Path, target: Path) -> Path:
    shutil.copytree(source, target)
    return target


@pytest.fixture
def quotes_settings(_fetched_quotes: Path, tmp_path: Path) -> Settings:
    """Settings over a private copy of a data dir holding a finished quotes fetch."""
    return stage_settings(_copy(_fetched_quotes, tmp_path / "data"))


@pytest.fixture
def books_settings(_fetched_books: Path, tmp_path: Path) -> Settings:
    """Settings over a private copy of a finished books fetch (capped at BOOKS_MAX_PAGES)."""
    return stage_settings(_copy(_fetched_books, tmp_path / "data"), max_pages=BOOKS_MAX_PAGES)


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Undo ``configure_logging`` after a CLI test: it binds the stream the CLI ran with, and
    ``CliRunner`` closes that stream when the command returns."""
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
