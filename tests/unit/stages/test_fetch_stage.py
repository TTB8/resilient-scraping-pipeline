"""The fetch stage with scripted fetchers in virtual time: expansion, recovery, stops, retries, Ctrl-C."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pytest

from fetch_support import VirtualTime
from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, PageKind, ResponseClass, Site
from scraping_pipeline.fetch.fake import (
    FakeFetcher,
    Respond,
    block_page,
    ok,
    rate_limited,
    server_error,
)
from scraping_pipeline.sites import SITES, SiteSpec
from scraping_pipeline.stages import run_discover
from scraping_pipeline.stages.fetch import FetchTiming, _Feeder, run_fetch
from scraping_pipeline.storage.cache import PageCache
from scraping_pipeline.storage.state import (
    DomainEventKind,
    RunStatus,
    Stage,
    StateStore,
)
from stage_support import (
    fast_fetch,
    saved_site_fetcher,
    settled_urls,
    stage_settings,
    virtual_runner,
)

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


# --- round 2: links on redirected pages, downgrades, breaker events, robots fetcher ---------------

_NEXT_ONLY = '<html><body><ul class="pager"><li class="next"><a href="next.html">Next</a></li></ul></body></html>'


def _planned(settings: Settings) -> set[str]:
    with StateStore.open(settings.state_db) as store:
        return {e.url for e in store.entries(Site.QUOTES)}


def test_links_on_a_redirected_listing_resolve_against_where_it_ended(tmp_path: Path) -> None:
    # S14: / redirects to /dir/new/, whose relative link next.html means /dir/new/next.html. Against
    # the entry URL it would be /next.html, a page the site never linked.
    settings = _discovered(tmp_path, max_depth=1)
    factory, _ = saved_site_fetcher(
        "quotes",
        {
            HOME: Respond(301, b"", {"location": "/dir/new/"}),
            "https://quotes.toscrape.com/dir/new/": ok(_NEXT_ONLY),
        },
    )
    result = fast_fetch(settings, Site.QUOTES, factory)
    planned = _planned(settings)
    assert "https://quotes.toscrape.com/dir/new/next.html" in planned
    assert "https://quotes.toscrape.com/next.html" not in planned
    assert result.scheme_downgrades == 0


def test_links_on_a_scheme_downgraded_page_are_kept_on_the_entry_origin(tmp_path: Path) -> None:
    # R2-08 / ADR 0010: the hop went to http on the same host. Resolved against the http URL, every
    # link would be off-origin for the scheme-strict link rule and silently dropped.
    settings = _discovered(tmp_path, max_depth=1)
    factory, log = saved_site_fetcher(
        "quotes",
        {
            HOME: Respond(308, b"", {"location": "http://quotes.toscrape.com/dir/new/"}),
            "http://quotes.toscrape.com/dir/new/": ok(_NEXT_ONLY),
        },
    )
    result = fast_fetch(settings, Site.QUOTES, factory)
    planned = _planned(settings)
    assert "https://quotes.toscrape.com/dir/new/next.html" in planned
    assert not any(url.startswith("http://") for url in planned)
    assert "https://quotes.toscrape.com/dir/new/next.html" in log.urls()  # and it was crawled
    assert result.scheme_downgrades == 1
    with StateStore.open(settings.state_db) as store:
        run = store.get_run(result.run_id)
    assert run is not None
    assert run.summary_json is not None
    assert json.loads(run.summary_json)["scheme_downgrades"] == 1


def test_offline_expansion_keeps_links_of_a_downgraded_page(tmp_path: Path) -> None:
    # The same rule in expand_from_cache: a frontier rebuilt next to the cache plans the same links.
    settings = _discovered(tmp_path, max_depth=1)
    factory, _ = saved_site_fetcher(
        "quotes",
        {
            HOME: Respond(308, b"", {"location": "http://quotes.toscrape.com/dir/new/"}),
            "http://quotes.toscrape.com/dir/new/": ok(_NEXT_ONLY),
        },
    )
    fast_fetch(settings, Site.QUOTES, factory)
    settings.state_db.unlink()
    run_discover(settings, Site.QUOTES)  # seeds, then offline expansion from the cache
    assert "https://quotes.toscrape.com/dir/new/next.html" in _planned(settings)


def test_a_breaker_opening_is_recorded_as_a_domain_event(tmp_path: Path) -> None:
    # I2-1: ADR 0008 lists the breaker among domain_events; the event must actually be written.
    settings = _discovered(
        tmp_path,
        max_depth=0,
        breaker_failure_threshold=2,
        max_concurrency_per_domain=1,
        max_attempts=5,
    )
    factory, _ = saved_site_fetcher("quotes", {HOME: [server_error(), server_error(), ok()]})
    result = fast_fetch(settings, Site.QUOTES, factory)
    assert result.statuses[FrontierStatus.DONE] == 1
    with StateStore.open(settings.state_db) as store:
        events = store.domain_events(Site.QUOTES)
    assert [e.event for e in events] == [DomainEventKind.ROBOTS, DomainEventKind.BREAKER_OPENED]
    opened = events[1]
    assert (opened.domain, opened.run_id) == ("quotes.toscrape.com", result.run_id)
    assert opened.response_class is ResponseClass.SERVER_ERROR
    assert opened.reason


_JS_HOME = "https://quotes.toscrape.com/js/"
_JS_ROBOTS = "https://quotes.toscrape.com/robots.txt"
_JS_RULES = ok("User-agent: *\nDisallow: /js/\n", content_type="text/plain")


def test_a_browser_site_reads_robots_with_the_robots_fetcher(tmp_path: Path) -> None:
    # R2-04: the page fetcher (the browser) is never asked for robots.txt. Had it been, its 404
    # would allow /js/; the raw fetcher's 200 disallows it.
    settings = stage_settings(tmp_path)
    run_discover(settings, Site.QUOTES_JS)
    page = FakeFetcher({_JS_HOME: ok()})
    raw = FakeFetcher({_JS_ROBOTS: _JS_RULES})
    runner, timing = virtual_runner()
    result = run_fetch(
        settings,
        Site.QUOTES_JS,
        fetcher_factory=lambda: page,
        robots_fetcher_factory=lambda: raw,
        timing=timing,
        runner=runner,
    )
    assert raw.urls_called() == [_JS_ROBOTS]
    assert page.urls_called() == []
    assert result.statuses[FrontierStatus.SKIPPED_ROBOTS] == 1
    assert result.requests == 1


def test_by_default_a_site_gets_its_robots_fetcher_from_its_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = stage_settings(tmp_path)
    run_discover(settings, Site.QUOTES_JS)
    page = FakeFetcher({_JS_HOME: ok()})
    raw = FakeFetcher({_JS_ROBOTS: _JS_RULES})
    asked: list[Site] = []

    def make_robots(spec: SiteSpec, _settings: Settings) -> FakeFetcher:
        asked.append(spec.site)
        return raw

    monkeypatch.setattr(SiteSpec, "make_fetcher", lambda _spec, _settings: page)
    monkeypatch.setattr(SiteSpec, "make_robots_fetcher", make_robots)
    runner, timing = virtual_runner()
    result = run_fetch(settings, Site.QUOTES_JS, timing=timing, runner=runner)
    assert asked == [Site.QUOTES_JS]
    assert raw.urls_called() == [_JS_ROBOTS]
    assert page.urls_called() == []
    assert result.statuses[FrontierStatus.SKIPPED_ROBOTS] == 1


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


# --- round 3: the https to http hop is a per-site opt-in (R3-06, ADR 0010) -----------------------


def test_only_the_quotes_sites_opt_in_to_scheme_downgrades() -> None:
    assert {s for s, spec in SITES.items() if spec.allow_scheme_downgrade} == {
        Site.QUOTES,
        Site.QUOTES_JS,
    }


def test_books_refuses_a_same_host_downgrade_as_off_site(tmp_path: Path) -> None:
    books_home = "https://books.toscrape.com/"
    settings = stage_settings(tmp_path)
    run_discover(settings, Site.BOOKS)
    factory, log = saved_site_fetcher(
        "books",
        {
            books_home: Respond(308, b"", {"location": "http://books.toscrape.com/"}),
            "http://books.toscrape.com/": ok(_NEXT_ONLY),
        },
    )
    result = fast_fetch(settings, Site.BOOKS, factory)
    assert "http://books.toscrape.com/" not in log.urls()  # never sent in clear text
    assert result.scheme_downgrades == 0
    with StateStore.open(settings.state_db) as store:
        entry = store.get(books_home)
    assert entry is not None
    assert entry.status is FrontierStatus.FAILED_PERMANENT
    assert "https to http downgrade not allowed for this site" in (entry.last_reason or "")
