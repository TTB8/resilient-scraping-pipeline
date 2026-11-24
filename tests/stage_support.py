"""Helpers for stage tests: settings in a temp dir, the saved sites as a scripted fetcher, virtual time.

``saved_site_fetcher`` turns ``tests/fixtures/sites/<site>/`` into a :class:`FakeFetcher` script at
the sites' real URLs (``https://books.toscrape.com/...``), so a whole crawl runs with no socket at
all. Like the real quotes site, a saved directory requested without its trailing slash answers 301
to the slashed path; the scheduler follows the hop and reports it as ``final_url``. Unknown URLs are 404.

``fast_fetch`` runs ``run_fetch`` under :class:`fetch_support.VirtualTime`, so politeness waits and
backoffs cost no real time.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fetch_support import VirtualTime, no_env_settings
from fixture_server import SITES
from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FINAL_STATES
from scraping_pipeline.core.types import Site
from scraping_pipeline.fetch.fake import FakeFetcher, Respond, Step, ok, slow
from scraping_pipeline.stages.fetch import FetchStageResult, FetchTiming, run_fetch
from scraping_pipeline.storage.cache import PageCache
from scraping_pipeline.storage.state import StateStore

BASE = {"books": "https://books.toscrape.com", "quotes": "https://quotes.toscrape.com"}
FAST = {
    "requests_per_second": 5.0,
    "burst": 10,
    "max_concurrency_per_domain": 4,
    "backoff_base_s": 0.01,
    "backoff_cap_s": 0.05,
    "request_timeout_s": 5.0,
}


def stage_settings(data_dir: Path, **overrides: Any) -> Settings:
    """Settings for a stage test: no environment, no ``.env``, fast politeness, ``data_dir``."""
    values = {**FAST, **overrides}
    with no_env_settings():
        return Settings(_env_file=None, data_dir=data_dir, **values)


def server_settings(data_dir: Path, base_url: str, **overrides: Any) -> Settings:
    """Settings whose books and quotes origins both point at a local fixture server."""
    return stage_settings(
        data_dir, books_base_url=base_url + "/", quotes_base_url=base_url + "/", **overrides
    )


def saved_pages(fixtures: str) -> dict[str, Step]:
    """URL -> scripted answer for every saved page of ``fixtures`` (``books`` or ``quotes``)."""
    root = SITES / fixtures
    base = BASE[fixtures]
    scripts: dict[str, Step] = {}
    for path in sorted(root.rglob("*.html")):
        rel = path.relative_to(root).as_posix()
        body = path.read_bytes()
        if rel == "index.html" or rel.endswith("/index.html"):
            folder = rel.removesuffix("index.html")
            slashed = f"{base}/{folder}"
            scripts[slashed] = ok(body)
            scripts[f"{base}/{rel}"] = ok(body)  # books links its pages as .../index.html
            if folder:
                bare = slashed.rstrip("/")
                # A real redirect, as the site sends: the scheduler follows it itself.
                scripts[bare] = Respond(301, b"", {"location": slashed})
        else:
            scripts[f"{base}/{rel}"] = ok(body)
    return scripts


@dataclass
class FetcherLog:
    """Every FakeFetcher a factory built, so tests can count calls across runs."""

    fetchers: list[FakeFetcher] = field(default_factory=list)

    def urls(self) -> list[str]:
        return [
            url for f in self.fetchers for url in f.urls_called() if not url.endswith("/robots.txt")
        ]


def saved_site_fetcher(
    fixtures: str,
    overrides: dict[str, Step | list[Step]] | None = None,
    *,
    delay_s: float = 0.0,
    sleep: Callable[[float], Coroutine[Any, Any, None]] | None = None,
) -> tuple[Callable[[], FakeFetcher], FetcherLog]:
    """A factory of fresh FakeFetchers serving the saved site (plus ``overrides``), and its log.

    With ``delay_s`` every saved page answers after that much (injected) ``sleep``, so a fetch
    yields to the event loop like real I/O does; without it a fetch completes synchronously.
    """
    log = FetcherLog()

    def factory() -> FakeFetcher:
        pages = {url: slow(step, delay_s) for url, step in saved_pages(fixtures).items()}
        fake = FakeFetcher(pages, sleep=sleep) if sleep is not None else FakeFetcher(pages)
        for url, steps in (overrides or {}).items():
            fake.script(url, *(steps if isinstance(steps, list) else [steps]))
        log.fetchers.append(fake)
        return fake

    return factory, log


def virtual_runner() -> tuple[Callable[[Coroutine[Any, Any, Any]], Any], FetchTiming]:
    vt = VirtualTime()

    def runner(coro: Coroutine[Any, Any, Any]) -> Any:
        return asyncio.run(vt.run(coro))

    return runner, FetchTiming(clock=vt.clock, wall_clock=vt.wall, sleep=vt.sleep)


def fast_fetch(
    settings: Settings,
    site: Site,
    factory: Callable[[], FakeFetcher],
    *,
    retry_failed: bool = False,
) -> FetchStageResult:
    runner, timing = virtual_runner()
    return run_fetch(
        settings,
        site,
        retry_failed=retry_failed,
        fetcher_factory=factory,
        timing=timing,
        runner=runner,
    )


def settled_urls(settings: Settings, site: Site) -> set[str]:
    """URLs a resumed run must never request again: every final frontier entry, plus where each
    cached page's redirects ended. (An attempt abandoned mid-flight, between a redirect and its
    hop say, stays pending and is rightly fetched again.)"""

    with StateStore.open(settings.state_db) as store:
        final = [e.url for e in store.entries(site) if e.status in FINAL_STATES]
    cache = PageCache(settings.cache_dir)
    hops = {entry.final_url for url in final if (entry := cache.get_entry(url)) is not None}
    return set(final) | hops
