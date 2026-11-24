"""Shared pytest fixtures: the local fixture server and the Chromium availability probe."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Iterator

import pytest

from fixture_server import FixtureServer


@pytest.fixture
def make_fixture_server() -> Iterator[Callable[..., FixtureServer]]:
    """Factory: ``make_fixture_server("books", robots_txt="...")``. Every server is stopped after."""
    servers: list[FixtureServer] = []

    def factory(site: str = "quotes", *, robots_txt: str | None = None) -> FixtureServer:
        server = FixtureServer(site, robots_txt=robots_txt).start()
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
