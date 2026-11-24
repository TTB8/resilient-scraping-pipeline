"""Scheduler + HttpxFetcher + fixture server, in real asyncio time with tiny settings."""

import itertools
import time
from collections.abc import Callable

from fetch_support import RecordingSink, make_settings
from fixture_server import FixtureServer, Phase
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, PageKind, ResponseClass, Site
from scraping_pipeline.fetch.httpx_fetcher import HttpxFetcher
from scraping_pipeline.fetch.scheduler import PoliteScheduler

PAGES = ["/", "/page/2/", "/page/10/", "/tag/love/", "/author/Albert-Einstein/", "/js/"]


def entries(server: FixtureServer) -> list[FrontierEntry]:
    return [
        FrontierEntry(url=server.url(p), site=Site.QUOTES, kind=PageKind.LISTING, depth=0)
        for p in PAGES
    ]


async def test_rate_limit_then_block_page_stops_the_domain(fixture_server: FixtureServer) -> None:
    # Two good pages, one 429 asking for a 1 s pause, then a block page for everything after.
    fixture_server.scenario(Phase.serve(2), Phase.rate_limited("1", times=1), Phase.block())
    settings = make_settings(
        requests_per_second=5.0,
        burst=1,
        max_concurrency_per_domain=1,
        request_timeout_s=5.0,
        backoff_base_s=0.01,
        backoff_cap_s=0.05,
        max_retry_after_s=5.0,
    )
    sink = RecordingSink()
    started = time.monotonic()
    async with HttpxFetcher(settings) as fetcher:
        summary = await PoliteScheduler(fetcher, settings, sink).run(entries(fixture_server))
    took = time.monotonic() - started

    log = fixture_server.log()
    assert log[0].path == "/robots.txt"
    pages = [hit for hit in log if hit.path != "/robots.txt"]
    # Nothing reaches the server after the block page.
    assert [hit.status for hit in pages] == [200, 200, 429, 403]
    assert pages[0].path == PAGES[0]

    finals = sink.finals()
    assert set(finals) == {fixture_server.url(p) for p in PAGES}
    statuses = [finals[fixture_server.url(p)].status for p in PAGES]
    assert statuses[:2] == [FrontierStatus.DONE, FrontierStatus.DONE]
    assert set(statuses[2:]) == {FrontierStatus.STOPPED}
    rate_limited = finals[fixture_server.url(PAGES[2])]
    assert rate_limited.attempts == 1  # its retry never happened: the domain stopped first
    assert rate_limited.last_class is ResponseClass.RATE_LIMITED

    assert summary.by_class == {
        ResponseClass.OK: 2,
        ResponseClass.RATE_LIMITED: 1,
        ResponseClass.BLOCKED: 1,
    }
    assert list(summary.domains_stopped) == ["127.0.0.1"]
    assert len(sink.of("stopped")) == 1
    # Generous on purpose (a loaded CI runner): it only rules out a run that hangs or sleeps out
    # backoffs it should not. That the stop wakes an entry waiting out Retry-After is pinned in
    # virtual time by test_stop_wakes_entries_waiting_on_a_long_retry, and above by attempts == 1.
    assert took < 15.0


async def test_clean_crawl_of_saved_pages(fixture_server: FixtureServer) -> None:
    fixture_server.robots_txt = "User-agent: *\nDisallow: /js/\n"
    settings = make_settings(requests_per_second=5.0, burst=5, max_concurrency_per_domain=2)
    sink = RecordingSink()
    async with HttpxFetcher(settings) as fetcher:
        summary = await PoliteScheduler(fetcher, settings, sink).run(entries(fixture_server))

    assert summary.final_statuses == {FrontierStatus.DONE: 5, FrontierStatus.SKIPPED_ROBOTS: 1}
    assert fixture_server.hits("/js/") == 0
    assert fixture_server.hits("/robots.txt") == 1
    done = [e for e in sink.of("result") if e.result is not None]
    assert all(b"quote" in e.result.body for e in done if e.result is not None)


async def test_redirect_chain_stays_on_site(
    make_fixture_server: Callable[..., FixtureServer],
) -> None:
    # The reviewer's R1-01 repro, through the scheduler: four same-site hops, then a hop to another
    # server. The same-site hops are followed (each with its own token); the off-site one is not.
    site = make_fixture_server("quotes")
    other = make_fixture_server("quotes")
    for n in range(4):
        site.redirect(f"/hop{n}", f"/hop{n + 1}")
    site.redirect("/hop4", f"http://localhost:{other.port}/secret")
    site.redirect("/moved", "/page/2/", status=301)
    settings = make_settings(requests_per_second=5.0, burst=1, max_concurrency_per_domain=1)
    sink = RecordingSink()
    entries_ = [
        FrontierEntry(url=site.url(p), site=Site.QUOTES, kind=PageKind.LISTING, depth=0)
        for p in ("/hop0", "/moved")
    ]
    async with HttpxFetcher(settings) as fetcher:
        summary = await PoliteScheduler(fetcher, settings, sink).run(entries_)

    assert other.hits() == 0  # the other host never saw a request
    finals = sink.finals()
    refused = finals[site.url("/hop0")]
    assert refused.status is FrontierStatus.FAILED_PERMANENT
    assert "off-site redirect to localhost refused" in (refused.last_reason or "")
    assert site.hits("/hop0") == 1  # permanent: not retried
    moved = finals[site.url("/moved")]
    assert moved.status is FrontierStatus.DONE
    result = next(e.result for e in sink.of("result") if e.url == site.url("/moved"))
    assert result is not None
    assert result.final_url == site.url("/page/2/")
    assert b'class="quote"' in result.body
    # robots + 5 hops for /hop0 + 2 for /moved, all at <= 5 req/s with burst 1.
    assert summary.requests == 1 + 5 + 2
    times = [hit.at for hit in site.log()]
    gaps = [b - a for a, b in itertools.pairwise(times)]
    # Spacing measured at the server. One gap can shrink when the earlier hit is logged late (a
    # server thread scheduled after the next request was sent), so assert the whole span, which
    # still catches a burst, plus a loose floor per gap.
    assert times[-1] - times[0] >= (len(times) - 1) * 0.2 - 0.05
    assert min(gaps) >= 0.1
