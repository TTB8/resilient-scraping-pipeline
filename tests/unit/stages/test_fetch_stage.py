"""The fetch stage with scripted fetchers in virtual time: expansion, recovery, stops, retries, Ctrl-C."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pytest

from fetch_support import VirtualTime
from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, PageKind, ResponseClass, Site
from scraping_pipeline.fetch.fake import FakeFetcher, block_page, ok, rate_limited, server_error
from scraping_pipeline.stages import run_discover
from scraping_pipeline.stages.fetch import FetchTiming, _Feeder, run_fetch
from scraping_pipeline.storage.cache import PageCache
from scraping_pipeline.storage.state import (
    DomainEventKind,
    RunStatus,
    Stage,
    StateStore,
)
from stage_support import fast_fetch, saved_site_fetcher, settled_urls, stage_settings

HOME = "https://quotes.toscrape.com/"
PAGE_2 = "https://quotes.toscrape.com/page/2/"

pytestmark = pytest.mark.usefixtures("no_real_fetchers")


class SimulatedCrashError(RuntimeError):
    """Stands in for the process dying at an exact point."""


def _snapshot(settings: Settings, site: Site = Site.QUOTES) -> list[tuple[object, ...]]:
    with StateStore.open(settings.state_db) as store:
        return [(e.url, e.kind, e.depth, e.status, e.last_class) for e in store.entries(site)]


def _discovered(tmp_path: Path, **overrides: Any) -> Settings:
    settings = stage_settings(tmp_path, **overrides)
    run_discover(settings, Site.QUOTES)
    return settings


def test_fetch_expands_links_as_pages_land_until_nothing_is_left(tmp_path: Path) -> None:
    settings = _discovered(tmp_path)
    factory, log = saved_site_fetcher("quotes")
    result = fast_fetch(settings, Site.QUOTES, factory)

    assert result.statuses[FrontierStatus.PENDING] == 0  # the feeder did not end early
    assert result.statuses[FrontierStatus.DONE] == 4  # /, /page/2/ and the two saved authors
    assert result.links_added == 17
    assert result.attempts == 18
    # The site saw every attempt, the two redirect hops (/author/X -> /author/X/) and robots.txt.
    assert result.requests == sum(len(f.calls) for f in log.fetchers) == 18 + 2 + 1
    assert max(Counter(log.urls()).values()) == 1  # nothing fetched twice
    assert result.by_class == {ResponseClass.NOT_FOUND: 14, ResponseClass.OK: 4}
    assert not result.interrupted
    assert not result.stopped_domains

    cache = PageCache(settings.cache_dir)
    with StateStore.open(settings.state_db) as store:
        done = store.entries(Site.QUOTES, {FrontierStatus.DONE})
        for entry in done:  # cache before state: every done page has its body
            cached = cache.get_entry(entry.url)
            assert cached is not None
            assert cached.fetch_run_id == result.run_id
            attempts = store.attempts(entry.url)
            assert [a.record.content_sha256 for a in attempts] == [cached.content_sha256]
        run = store.get_run(result.run_id)
        events = store.domain_events(Site.QUOTES)
    assert run is not None
    assert run.status is RunStatus.SUCCEEDED
    assert run.stage is Stage.FETCH
    assert [e.event for e in events] == [DomainEventKind.ROBOTS]
    # The redirected author page is cached under its frontier URL, with where it ended up.
    author = cache.get_entry("https://quotes.toscrape.com/author/Albert-Einstein")
    assert author is not None
    assert author.final_url.endswith("/author/Albert-Einstein/")


def test_nothing_pending_means_no_fetcher_and_no_request(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)  # no discover: empty frontier
    built: list[object] = []

    def factory() -> FakeFetcher:
        built.append(object())
        return FakeFetcher()

    result = fast_fetch(settings, Site.QUOTES, factory)
    assert built == []
    assert result.attempts == 0
    assert result.statuses[FrontierStatus.PENDING] == 0


@pytest.mark.slow
def test_a_crash_between_cache_and_state_is_recovered_without_a_refetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _discovered(tmp_path)
    factory, log = saved_site_fetcher("quotes")
    real = StateStore.record_result

    def crash_on_page_2(self: StateStore, run_id: str, before: FrontierEntry, *rest: Any) -> None:
        if before.url == PAGE_2:
            raise SimulatedCrashError("killed after cache.put, before record_result")
        real(self, run_id, before, *rest)

    monkeypatch.setattr(StateStore, "record_result", crash_on_page_2)
    with pytest.raises(SimulatedCrashError):
        fast_fetch(settings, Site.QUOTES, factory)
    monkeypatch.setattr(StateStore, "record_result", real)

    # The page reached the cache; the state never heard of it.
    assert PageCache(settings.cache_dir).has(PAGE_2)
    with StateStore.open(settings.state_db) as store:
        entry = store.get(PAGE_2)
        assert entry is not None
        assert entry.status is FrontierStatus.PENDING
        assert store.runs(Site.QUOTES)[-1].status is RunStatus.FAILED

    before = len(log.urls())
    settled = settled_urls(settings, Site.QUOTES)
    result = fast_fetch(settings, Site.QUOTES, factory)
    assert result.recovered == 1
    assert PAGE_2 not in log.urls()[before:]  # recovered, not refetched
    assert not settled & set(log.urls()[before:])  # nothing settled is asked for again
    assert result.statuses[FrontierStatus.PENDING] == 0
    # Its links (page 3, its authors) were planned offline at the start of the second run.
    assert ("https://quotes.toscrape.com/page/3/") in log.urls()[before:]

    reference = _discovered(tmp_path / "reference")
    fast_fetch(reference, Site.QUOTES, saved_site_fetcher("quotes")[0])
    assert _snapshot(settings) == _snapshot(reference)


def _cancelling_runner(
    after_results: int, monkeypatch: pytest.MonkeyPatch
) -> tuple[Callable[[Coroutine[Any, Any, Any]], Any], FetchTiming, VirtualTime]:
    """Run in virtual time and cancel the crawl (what Ctrl-C does) after N recorded results."""
    vt = VirtualTime()
    main: list[asyncio.Task[Any]] = []
    real = StateStore.record_result
    seen = [0]

    def counting(self: StateStore, *args: Any) -> None:
        real(self, *args)
        seen[0] += 1
        if seen[0] == after_results:
            main[0].cancel()

    monkeypatch.setattr(StateStore, "record_result", counting)

    def runner(coro: Coroutine[Any, Any, Any]) -> Any:
        async def crawl() -> Any:  # the task asyncio.run would cancel on Ctrl-C
            task = asyncio.current_task()
            assert task is not None
            main.append(task)
            return await coro

        return asyncio.run(vt.run(crawl()))

    return runner, FetchTiming(clock=vt.clock, wall_clock=vt.wall, sleep=vt.sleep), vt


@pytest.mark.slow
def test_ctrl_c_keeps_every_recorded_result_and_the_rerun_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _discovered(tmp_path, max_concurrency_per_domain=1)
    runner, timing, vt = _cancelling_runner(3, monkeypatch)
    # Pages answer after a (virtual) moment, so a fetch yields like real I/O and the cancellation
    # reaches the crawl between pages, as Ctrl-C does with a real fetcher.
    factory, _ = saved_site_fetcher("quotes", delay_s=0.01, sleep=vt.sleep)
    first = run_fetch(settings, Site.QUOTES, fetcher_factory=factory, timing=timing, runner=runner)
    monkeypatch.undo()

    assert first.interrupted
    assert first.attempts == 3
    assert first.statuses[FrontierStatus.PENDING] > 0
    with StateStore.open(settings.state_db) as store:
        run = store.get_run(first.run_id)
    assert run is not None
    assert run.status is RunStatus.INTERRUPTED

    settled = settled_urls(settings, Site.QUOTES)
    assert len(settled) >= 3
    resumed, resumed_log = saved_site_fetcher("quotes")  # a new process: new fetcher, new clock
    second = fast_fetch(settings, Site.QUOTES, resumed)
    assert not second.interrupted
    assert second.statuses[FrontierStatus.PENDING] == 0
    # Every recorded result survived the kill: none of those pages is asked for again. (An
    # attempt abandoned mid-flight stays pending and is fetched again, as it should be.)
    assert not settled & set(resumed_log.urls())
    assert max(Counter(resumed_log.urls()).values()) == 1
    reference = _discovered(tmp_path / "reference")
    fast_fetch(reference, Site.QUOTES, saved_site_fetcher("quotes")[0])
    assert _snapshot(settings) == _snapshot(reference)


def test_a_keyboard_interrupt_from_the_runner_marks_the_run_interrupted(tmp_path: Path) -> None:
    settings = _discovered(tmp_path)

    def runner(coro: Coroutine[Any, Any, Any]) -> Any:
        coro.close()
        raise KeyboardInterrupt  # what asyncio.run raises after Ctrl-C

    result = run_fetch(
        settings, Site.QUOTES, fetcher_factory=FakeFetcher, runner=runner, timing=FetchTiming()
    )
    assert result.interrupted
    with StateStore.open(settings.state_db) as store:
        assert store.runs(Site.QUOTES)[-1].status is RunStatus.INTERRUPTED


def test_a_block_page_stops_the_domain_and_names_the_class(tmp_path: Path) -> None:
    settings = _discovered(tmp_path, max_concurrency_per_domain=1)
    factory, log = saved_site_fetcher("quotes", {PAGE_2: block_page()})
    result = fast_fetch(settings, Site.QUOTES, factory)

    assert list(result.domains_stopped_now) == ["quotes.toscrape.com"]
    assert "blocked" in result.stopped_domains["quotes.toscrape.com"]
    assert result.statuses[FrontierStatus.STOPPED] > 0
    assert result.statuses[FrontierStatus.PENDING] == 0
    assert log.urls()[-1] == PAGE_2  # nothing was sent after the block page
    with StateStore.open(settings.state_db) as store:
        stops = [
            e for e in store.domain_events(Site.QUOTES) if e.event is DomainEventKind.DOMAIN_STOPPED
        ]
    assert [(s.domain, s.response_class) for s in stops] == [
        ("quotes.toscrape.com", ResponseClass.BLOCKED)
    ]


@pytest.mark.slow
def test_a_robots_crawl_delay_stop_carries_no_page_class(tmp_path: Path) -> None:
    settings = _discovered(tmp_path)
    factory, log = saved_site_fetcher(
        "quotes",
        {"https://quotes.toscrape.com/robots.txt": ok("User-agent: *\nCrawl-delay: 9999\n")},
    )
    result = fast_fetch(settings, Site.QUOTES, factory)
    assert log.urls() == []
    assert result.statuses[FrontierStatus.STOPPED] == 1
    with StateStore.open(settings.state_db) as store:
        stops = [
            e for e in store.domain_events(Site.QUOTES) if e.event is DomainEventKind.DOMAIN_STOPPED
        ]
    assert len(stops) == 1
    assert stops[0].response_class is None
    assert "Crawl-delay" in stops[0].reason


@pytest.mark.slow
def test_a_rate_limited_page_is_retried_within_the_run(tmp_path: Path) -> None:
    settings = _discovered(tmp_path, max_depth=0)
    factory, log = saved_site_fetcher("quotes", {HOME: [rate_limited("2"), ok("<html></html>")]})
    result = fast_fetch(settings, Site.QUOTES, factory)
    assert log.urls() == [HOME, HOME]
    assert result.by_class == {ResponseClass.RATE_LIMITED: 1, ResponseClass.OK: 1}
    with StateStore.open(settings.state_db) as store:
        entry = store.get(HOME)
    assert entry is not None
    assert entry.status is FrontierStatus.DONE
    assert entry.attempts == 2


@pytest.mark.slow
def test_retry_failed_is_opt_in_and_grants_one_more_attempt(tmp_path: Path) -> None:
    settings = _discovered(tmp_path, max_attempts=2, max_depth=0)
    factory, log = saved_site_fetcher("quotes", {HOME: server_error()})
    first = fast_fetch(settings, Site.QUOTES, factory)
    assert first.statuses[FrontierStatus.FAILED_EXHAUSTED] == 1
    assert log.urls() == [HOME, HOME]

    assert fast_fetch(settings, Site.QUOTES, factory).attempts == 0  # final stays final

    again = fast_fetch(settings, Site.QUOTES, factory, retry_failed=True)
    assert again.reset == 1
    assert log.urls() == [HOME, HOME, HOME]  # one more attempt: the budget is lifetime
    with StateStore.open(settings.state_db) as store:
        entry = store.get(HOME)
    assert entry is not None
    assert entry.status is FrontierStatus.FAILED_EXHAUSTED
    assert entry.attempts == 3

    healed, _ = saved_site_fetcher("quotes", {HOME: ok("<html></html>")})
    final = fast_fetch(settings, Site.QUOTES, healed, retry_failed=True)
    assert final.statuses[FrontierStatus.DONE] == 1


# --- the feeder ----------------------------------------------------------------------------------


def _entry(url: str) -> FrontierEntry:
    return FrontierEntry(url, Site.QUOTES, PageKind.LISTING, 0)


async def test_feeder_ends_at_once_when_given_nothing() -> None:
    assert [e async for e in _Feeder().entries()] == []


async def test_feeder_waits_for_outstanding_entries_and_feeds_their_links() -> None:
    feeder = _Feeder()
    feeder.add([_entry("https://h/a"), _entry("https://h/a")])  # duplicates are fed once
    stream = feeder.entries()
    first = await anext(stream)
    assert first.url == "https://h/a"

    waiting = asyncio.ensure_future(anext(stream))
    await asyncio.sleep(0)
    assert not waiting.done()  # queue empty, but /a is outstanding: not the end
    feeder.add([_entry("https://h/b"), _entry("https://h/a")])  # links of /a, queued first ...
    feeder.settle("https://h/a")  # ... then /a settles
    assert (await waiting).url == "https://h/b"

    ending = asyncio.ensure_future(anext(stream))
    await asyncio.sleep(0)
    assert not ending.done()
    feeder.settle("https://h/b")
    with pytest.raises(StopAsyncIteration):
        await ending
