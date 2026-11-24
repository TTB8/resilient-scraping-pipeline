"""PoliteScheduler in virtual time, driven by FakeFetcher.

Every test runs the real scheduler and the real core rules; only the network (FakeFetcher) and time
(VirtualTime) are simulated. Assertions are about what a site operator would see: which requests
arrived, when, how many at once, and what the pipeline recorded about each URL.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from email.utils import format_datetime
from typing import Any

import pytest

from fetch_support import EPOCH, RecordingSink, VirtualTime, entry, make_settings
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, ResponseClass
from scraping_pipeline.fetch.fake import (
    FakeFetcher,
    Respond,
    Step,
    block_page,
    challenge_page,
    connect_error,
    not_found,
    ok,
    rate_limited,
    server_error,
    slow,
    timeout,
)
from scraping_pipeline.fetch.scheduler import CrawlSummary, PoliteScheduler, origin_of

HOST = "https://quotes.example.test"
OTHER = "https://books.example.test"
ROBOTS = HOST + "/robots.txt"


def u(path: str, host: str = HOST) -> str:
    return host + path


def redirect(location: str, status: int = 302) -> Respond:
    return Respond(status, b"", {"location": location})


class Crawl:
    """One scheduler run in virtual time, with everything a test wants to inspect."""

    def __init__(
        self,
        scripts: Mapping[str, Step | list[Step]] | None = None,
        *,
        robots: Mapping[str, Step | list[Step]] | None = None,
        allow_downgrade: bool = False,
        **settings: Any,
    ):
        """``robots``: give the scheduler a separate robots.txt fetcher with these scripts.

        ``allow_downgrade``: the site opts in to same-host https to http hops (ADR 0010)."""
        self.vt = VirtualTime()
        self.fake = FakeFetcher(scripts, sleep=self.vt.sleep, clock=self.vt.clock)
        self.robots_fake = (
            FakeFetcher(robots, sleep=self.vt.sleep, clock=self.vt.clock)
            if robots is not None
            else None
        )
        self.sink = RecordingSink(clock=self.vt.clock)
        self.settings = make_settings(**settings)
        self.scheduler = PoliteScheduler(
            self.fake,
            self.settings,
            self.sink,
            clock=self.vt.clock,
            wall_clock=self.vt.wall,
            sleep=self.vt.sleep,
            rand=lambda: 0.5,
            robots_fetcher=self.robots_fake,
            allow_scheme_downgrade=allow_downgrade,
        )

    async def run(self, urls: list[str]) -> CrawlSummary:
        return await self.vt.run(self.scheduler.run([entry(x) for x in urls]))

    def starts(self, url: str) -> list[float]:
        return [c.started_at for c in self.fake.calls if c.url == url]

    def page_calls(self) -> list[str]:
        return [c.url for c in self.fake.calls if not c.url.endswith("/robots.txt")]


def assert_rate_respected(times: list[float], rate: float, burst: float) -> None:
    """Over every window [t_i, t_j], at most burst + rate * (t_j - t_i) requests started."""
    ordered = sorted(times)
    for i in range(len(ordered)):
        for j in range(i, len(ordered)):
            allowed = burst + rate * (ordered[j] - ordered[i]) + 1e-6
            assert j - i + 1 <= allowed, (ordered[i], ordered[j], j - i + 1, allowed)


# --- the happy path ---------------------------------------------------------------------------


async def test_every_url_fetched_once_after_robots() -> None:
    urls = [u(f"/page/{n}/") for n in range(1, 6)]
    crawl = Crawl({x: ok() for x in urls})
    summary = await crawl.run(urls)

    assert crawl.fake.calls[0].url == ROBOTS  # robots first, before any page
    assert crawl.fake.call_counts()[ROBOTS] == 1
    assert all(crawl.fake.call_counts()[x] == 1 for x in urls)
    finals = crawl.sink.finals()
    assert set(finals) == set(urls)
    assert {e.status for e in finals.values()} == {FrontierStatus.DONE}
    assert summary.attempts == 5
    assert summary.by_class == {ResponseClass.OK: 5}
    assert summary.final_statuses == {FrontierStatus.DONE: 5}
    assert summary.domains_stopped == {}
    assert "404" in summary.robots["quotes.example.test"]
    assert crawl.sink.of("robots")[0].domain == "quotes.example.test"


async def test_duplicate_urls_fetched_once() -> None:
    crawl = Crawl({u("/a"): ok()})
    summary = await crawl.vt.run(crawl.scheduler.run([entry(u("/a")), entry(u("/a"))]))
    assert crawl.fake.call_counts()[u("/a")] == 1
    assert summary.duplicates_ignored == 1


async def test_async_source_is_consumed() -> None:
    urls = [u("/a"), u("/b")]

    async def source() -> AsyncIterator[FrontierEntry]:
        for x in urls:
            await asyncio.sleep(0)
            yield entry(x)

    crawl = Crawl({x: ok() for x in urls})
    summary = await crawl.vt.run(crawl.scheduler.run(source()))
    assert summary.final_statuses == {FrontierStatus.DONE: 2}


async def test_non_pending_entry_is_a_caller_bug() -> None:
    crawl = Crawl()
    done = FrontierEntry(
        url=u("/a"), site=entry(u("/a")).site, kind=entry(u("/a")).kind, depth=0,
        status=FrontierStatus.DONE,
    )  # fmt: skip
    with pytest.raises(ValueError, match="only pending"):
        await crawl.vt.run(crawl.scheduler.run([done]))


async def test_scheduler_runs_once() -> None:
    crawl = Crawl()
    await crawl.run([])
    with pytest.raises(RuntimeError, match="runs once"):
        await crawl.run([])


async def test_future_next_attempt_is_waited_for() -> None:
    crawl = Crawl({u("/a"): ok()})
    later = FrontierEntry(
        url=u("/a"), site=entry(u("/a")).site, kind=entry(u("/a")).kind, depth=0,
        next_attempt_at=EPOCH + 40,
    )  # fmt: skip
    await crawl.vt.run(crawl.scheduler.run([later]))
    assert crawl.starts(u("/a")) == [40.0]


# --- retries ----------------------------------------------------------------------------------


async def test_retry_after_seconds_is_honoured() -> None:
    crawl = Crawl({u("/a"): [rate_limited("7"), ok()]})
    await crawl.run([u("/a")])

    first, second = crawl.fake.calls[1], crawl.fake.calls[2]
    assert first.ended_at is not None
    assert second.started_at == pytest.approx(first.ended_at + 7)  # jitter (0.5s) < Retry-After
    final = crawl.sink.finals()[u("/a")]
    assert final.status is FrontierStatus.DONE
    assert final.attempts == 2


async def test_retry_after_http_date_is_honoured() -> None:
    when = format_datetime(datetime.fromtimestamp(EPOCH + 30, UTC), usegmt=True)
    crawl = Crawl({u("/a"): [rate_limited(when), ok()]})
    await crawl.run([u("/a")])

    first_try = crawl.sink.of("result")[0]
    assert first_try.classification is not None
    assert first_try.classification.retry_after_s == pytest.approx(29.0)  # sent at t=1
    assert first_try.new_entry is not None
    assert first_try.new_entry.next_attempt_at == pytest.approx(EPOCH + 30)
    assert crawl.starts(u("/a")) == [pytest.approx(1.0), pytest.approx(30.0)]


async def test_retry_after_longer_than_limit_stops_domain() -> None:
    crawl = Crawl({u("/a"): rate_limited("3600"), u("/b"): ok()}, max_concurrency_per_domain=1)
    summary = await crawl.run([u("/a"), u("/b")])

    assert crawl.page_calls() == [u("/a")]
    assert "3600" in summary.domains_stopped["quotes.example.test"]
    assert crawl.sink.finals()[u("/b")].status is FrontierStatus.STOPPED


async def test_404_is_not_retried() -> None:
    crawl = Crawl({u("/gone"): not_found()})
    summary = await crawl.run([u("/gone")])
    assert crawl.fake.call_counts()[u("/gone")] == 1
    assert crawl.sink.finals()[u("/gone")].status is FrontierStatus.FAILED_PERMANENT
    assert summary.by_class == {ResponseClass.NOT_FOUND: 1}


async def test_5xx_is_retried_until_exhausted_with_backoff() -> None:
    crawl = Crawl(
        {u("/flaky"): server_error(503)},
        max_attempts=3,
        backoff_base_s=2.0,
        breaker_failure_threshold=10,
    )
    await crawl.run([u("/flaky")])

    starts = crawl.starts(u("/flaky"))
    assert len(starts) == 3
    # Full jitter with rand=0.5: 0.5 * 2 * 2**(n-1) -> 1s then 2s, floored by the 1 req/s bucket.
    assert starts[1] - starts[0] == pytest.approx(1.0)
    assert starts[2] - starts[1] == pytest.approx(2.0)
    final = crawl.sink.finals()[u("/flaky")]
    assert final.status is FrontierStatus.FAILED_EXHAUSTED
    assert final.attempts == 3


async def test_5xx_then_recovery_is_done() -> None:
    crawl = Crawl({u("/a"): [server_error(), connect_error(), ok()]})
    summary = await crawl.run([u("/a")])
    assert crawl.sink.finals()[u("/a")].status is FrontierStatus.DONE
    assert summary.by_class == {
        ResponseClass.SERVER_ERROR: 1,
        ResponseClass.NETWORK_ERROR: 1,
        ResponseClass.OK: 1,
    }


# --- stopping a domain ------------------------------------------------------------------------


async def _assert_stopped_after(crawl: Crawl, urls: list[str], stopper: str) -> None:
    summary = await crawl.run(urls)
    before = urls[: urls.index(stopper)]
    after = urls[urls.index(stopper) + 1 :]

    assert crawl.page_calls() == [*before, stopper]  # nothing sent after the refusal
    finals = crawl.sink.finals()
    assert all(finals[x].status is FrontierStatus.DONE for x in before)
    assert finals[stopper].status is FrontierStatus.STOPPED
    assert all(finals[x].status is FrontierStatus.STOPPED for x in after)
    stops = crawl.sink.of("stopped")
    assert len(stops) == 1
    assert "quotes.example.test" in summary.domains_stopped
    # The stop is announced before the entries it explains.
    first_marked = next(i for i, e in enumerate(crawl.sink.events) if e.kind == "marked")
    assert crawl.sink.events.index(stops[0]) < first_marked


async def test_block_page_stops_domain_and_marks_the_rest() -> None:
    urls = [u(f"/p{n}") for n in range(5)]
    crawl = Crawl({**{x: ok() for x in urls}, urls[1]: block_page()}, max_concurrency_per_domain=1)
    await _assert_stopped_after(crawl, urls, urls[1])
    assert "blocked" in (crawl.sink.of("stopped")[0].reason or "")


async def test_challenge_served_with_200_stops_domain() -> None:
    urls = [u(f"/p{n}") for n in range(4)]
    crawl = Crawl(
        {**{x: ok() for x in urls}, urls[2]: challenge_page()}, max_concurrency_per_domain=1
    )
    await _assert_stopped_after(crawl, urls, urls[2])
    assert "challenge" in (crawl.sink.of("stopped")[0].reason or "")


async def test_stop_wakes_entries_waiting_on_a_long_retry() -> None:
    crawl = Crawl(
        {u("/slow"): rate_limited("200"), u("/b"): block_page()}, max_concurrency_per_domain=1
    )
    await crawl.run([u("/slow"), u("/b")])
    finals = crawl.sink.finals()
    assert finals[u("/slow")].status is FrontierStatus.STOPPED
    assert crawl.vt.now < 200  # it did not sit out the Retry-After first
    assert crawl.fake.call_counts()[u("/slow")] == 1


async def test_other_domains_continue_when_one_is_stopped() -> None:
    blocked = [u(f"/p{n}") for n in range(3)]
    fine = [u(f"/p{n}", OTHER) for n in range(3)]
    crawl = Crawl({**{x: block_page() for x in blocked}, **{x: ok() for x in fine}})
    summary = await crawl.run(blocked + fine)

    finals = crawl.sink.finals()
    assert all(finals[x].status is FrontierStatus.DONE for x in fine)
    assert all(finals[x].status is FrontierStatus.STOPPED for x in blocked)
    assert set(summary.domains_stopped) == {"quotes.example.test"}
    assert len(summary.robots) == 2


# --- circuit breaker --------------------------------------------------------------------------


async def test_breaker_opens_after_consecutive_timeouts_then_trips() -> None:
    urls = [u(f"/t{n}") for n in range(3)]
    crawl = Crawl(
        {x: timeout() for x in urls},
        breaker_failure_threshold=2,
        breaker_cooldown_s=30.0,
        breaker_max_open_cycles=1,
        max_attempts=10,
        max_concurrency_per_domain=1,
    )
    summary = await crawl.run(urls)

    starts = sorted(c.started_at for c in crawl.fake.calls if c.url != ROBOTS)
    # Two failures open the breaker; nothing is sent for the 30 s cooldown; the probe fails and,
    # with one open cycle allowed, the breaker trips and the domain stops.
    assert len(starts) == 3
    assert starts[2] - starts[1] >= 30.0
    reason = summary.domains_stopped["quotes.example.test"]
    assert "tripped" in reason
    finals = crawl.sink.finals()
    assert set(finals) == set(urls)
    assert {e.status for e in finals.values()} == {FrontierStatus.STOPPED}
    assert len(crawl.sink.of("stopped")) == 1


async def test_breaker_closes_again_after_a_good_probe() -> None:
    crawl = Crawl(
        {u("/a"): [timeout(), timeout(), ok()]},
        breaker_failure_threshold=2,
        breaker_cooldown_s=30.0,
        max_concurrency_per_domain=1,
    )
    summary = await crawl.run([u("/a")])
    assert summary.domains_stopped == {}
    assert summary.final_statuses == {FrontierStatus.DONE: 1}
    assert crawl.starts(u("/a"))[2] - crawl.starts(u("/a"))[1] >= 30.0


# --- politeness: rate and concurrency ---------------------------------------------------------


@pytest.mark.parametrize(("rate", "burst"), [(1.0, 1), (2.0, 3), (5.0, 10)])
async def test_rate_limit_is_never_exceeded(rate: float, burst: int) -> None:
    urls = [u(f"/r{n}") for n in range(25)]
    crawl = Crawl(
        {x: slow(ok(), 0.05) for x in urls},
        requests_per_second=rate,
        burst=burst,
        max_concurrency_per_domain=8,
    )
    await crawl.run(urls)
    times = [c.started_at for c in crawl.fake.calls]  # robots.txt included: it is a request too
    assert len(times) == 26
    assert_rate_respected(times, rate, burst)
    # And the bucket is actually used, not just a slow loop: the run takes about (n - burst) / rate.
    assert max(times) == pytest.approx((26 - burst) / rate, abs=1e-6)


async def test_rate_limited_answer_slows_the_domain_down() -> None:
    urls = [u(f"/r{n}") for n in range(4)]
    crawl = Crawl(
        {**{x: ok() for x in urls}, urls[0]: [rate_limited(None), ok()]},
        requests_per_second=2.0,
        max_concurrency_per_domain=1,
        backoff_base_s=0.1,
    )
    await crawl.run(urls)
    starts = sorted(c.started_at for c in crawl.fake.calls)
    gaps = [b - a for a, b in itertools.pairwise(starts)]
    # Before the 429 the domain ran at 2/s (robots -> first page: 0.5 s). The 429 halves the rate to
    # 1/s and empties the bucket; each success then adds 0.05/s back (AIMD), never above 2/s.
    assert gaps[0] == pytest.approx(0.5)
    assert gaps[1:] == pytest.approx([1 / 1.0, 1 / 1.05, 1 / 1.10, 1 / 1.15])


@pytest.mark.parametrize("cap", [1, 2, 3])
async def test_concurrency_cap_is_never_exceeded(cap: int) -> None:
    urls = [u(f"/c{n}") for n in range(12)]
    crawl = Crawl(
        {x: slow(ok(), 5.0) for x in urls},
        requests_per_second=5.0,
        burst=10,
        max_concurrency_per_domain=cap,
    )
    await crawl.run(urls)
    assert crawl.fake.max_in_flight == cap  # reached, never passed


# --- robots.txt -------------------------------------------------------------------------------


async def test_robots_disallow_marks_skipped_without_a_request() -> None:
    robots = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: robots, u("/private/x"): ok(), u("/public"): ok()})
    summary = await crawl.run([u("/private/x"), u("/public")])

    assert u("/private/x") not in crawl.fake.urls_called()
    finals = crawl.sink.finals()
    assert finals[u("/private/x")].status is FrontierStatus.SKIPPED_ROBOTS
    assert finals[u("/private/x")].attempts == 0
    assert finals[u("/public")].status is FrontierStatus.DONE
    assert summary.final_statuses == {FrontierStatus.SKIPPED_ROBOTS: 1, FrontierStatus.DONE: 1}


async def test_robots_5xx_is_retried_then_stops_the_domain() -> None:
    # R1-04: "we could not read the rules" is not "the rules forbid it". After max_attempts the
    # domain is stopped (reset and retried by the next run), never finalised as skipped_robots.
    urls = [u("/a"), u("/b")]
    crawl = Crawl({ROBOTS: server_error(503), **{x: ok() for x in urls}}, max_attempts=3)
    summary = await crawl.run(urls)
    assert crawl.fake.urls_called() == [ROBOTS] * 3
    assert {e.status for e in crawl.sink.finals().values()} == {FrontierStatus.STOPPED}
    reason = summary.domains_stopped["quotes.example.test"]
    assert reason.startswith("robots.txt unreachable: 503 server error")
    assert len(crawl.sink.of("stopped")) == 1
    assert len(crawl.sink.of("robots")) == 1


async def test_robots_blip_recovers_on_retry() -> None:
    urls = [u("/a"), u("/b")]
    crawl = Crawl({ROBOTS: [server_error(503), ok("")], **{x: ok() for x in urls}})
    summary = await crawl.run(urls)
    assert crawl.fake.call_counts()[ROBOTS] == 2
    assert summary.final_statuses == {FrontierStatus.DONE: 2}
    assert summary.domains_stopped == {}


async def test_robots_retry_honours_retry_after() -> None:
    crawl = Crawl({ROBOTS: [rate_limited("30"), ok("")], u("/a"): ok()})
    await crawl.run([u("/a")])
    assert crawl.starts(ROBOTS) == [0.0, pytest.approx(30.0)]
    assert crawl.sink.finals()[u("/a")].status is FrontierStatus.DONE


async def test_robots_unreachable_stops_the_whole_domain() -> None:
    crawl = Crawl({ROBOTS: timeout(), u("/a"): ok()}, max_attempts=2)
    summary = await crawl.run([u("/a")])
    assert crawl.fake.urls_called() == [ROBOTS, ROBOTS]
    assert crawl.sink.finals()[u("/a")].status is FrontierStatus.STOPPED
    assert "robots.txt unreachable: timed out" in summary.domains_stopped["quotes.example.test"]


async def test_robots_4xx_still_allows_everything() -> None:
    crawl = Crawl({ROBOTS: Respond(410, b""), u("/a"): ok()})
    summary = await crawl.run([u("/a")])
    assert crawl.fake.call_counts()[ROBOTS] == 1
    assert summary.final_statuses == {FrontierStatus.DONE: 1}


async def test_robots_same_host_redirect_is_followed_with_a_token_per_hop() -> None:
    moved = u("/rules/robots.txt")
    rules = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: redirect(moved), moved: rules, u("/private/x"): ok(), u("/a"): ok()})
    await crawl.run([u("/private/x"), u("/a")])
    assert crawl.fake.urls_called()[:2] == [ROBOTS, moved]
    assert crawl.starts(moved) == [1.0]  # the hop waited for its own token
    finals = crawl.sink.finals()
    assert finals[u("/private/x")].status is FrontierStatus.SKIPPED_ROBOTS
    assert finals[u("/a")].status is FrontierStatus.DONE


async def test_robots_off_host_redirect_stops_the_domain_without_following() -> None:
    crawl = Crawl({ROBOTS: redirect("https://elsewhere.test/robots.txt"), u("/a"): ok()})
    summary = await crawl.run([u("/a")])
    assert crawl.fake.urls_called() == [ROBOTS]
    reason = summary.domains_stopped["quotes.example.test"]
    assert reason == "robots.txt unreachable: redirect to elsewhere.test refused"


async def test_robots_crawl_delay_caps_the_rate() -> None:
    urls = [u(f"/d{n}") for n in range(4)]
    robots = ok("User-agent: *\nCrawl-delay: 3\n", content_type="text/plain")
    crawl = Crawl(
        {ROBOTS: robots, **{x: ok() for x in urls}},
        requests_per_second=5.0,
        burst=5,
        max_concurrency_per_domain=4,
    )
    await crawl.run(urls)
    starts = sorted(c.started_at for c in crawl.fake.calls if c.url != ROBOTS)
    gaps = [b - a for a, b in itertools.pairwise(starts)]
    assert min(gaps) >= 3.0 - 1e-9


# --- cancellation -----------------------------------------------------------------------------


async def test_cancellation_keeps_emitted_results_and_drops_in_flight() -> None:
    urls = [u(f"/k{n}") for n in range(10)]
    crawl = Crawl(
        {x: slow(ok(), 0.5) for x in urls},
        requests_per_second=1.0,
        max_concurrency_per_domain=2,
    )
    run = asyncio.ensure_future(crawl.scheduler.run([entry(x) for x in urls]))
    emitted_at_cancel: list[int] = []

    async def cancel_later() -> None:
        await crawl.vt.sleep(4.2)  # mid-flight: the request sent at t=4 is still out
        emitted_at_cancel.append(len(crawl.sink.events))
        run.cancel()

    async def drive() -> None:
        canceller = asyncio.ensure_future(cancel_later())
        with pytest.raises(asyncio.CancelledError):
            await run
        await canceller

    await crawl.vt.run(drive())

    assert len(crawl.sink.events) == emitted_at_cancel[0]  # nothing emitted after the cancel
    reported = [e.url for e in crawl.sink.of("result")]
    assert 0 < len(reported) < len(urls)
    assert len(reported) == len(set(reported))  # no URL emitted twice
    crawl.sink.finals()  # and no URL settled twice
    in_flight = [c.url for c in crawl.fake.calls if c.ended_at is None]
    assert in_flight  # the scenario really cancelled a request mid-flight
    assert not set(in_flight) & set(reported)


# --- programming errors -----------------------------------------------------------------------


async def test_sink_error_propagates_unwrapped() -> None:
    class Boom(RecordingSink):
        async def on_marked(self, entry: FrontierEntry, new_entry: FrontierEntry) -> None:
            del entry, new_entry
            raise LookupError("sink is broken")

    crawl = Crawl({ROBOTS: ok("User-agent: *\nDisallow: /\n", content_type="text/plain")})
    crawl.scheduler = PoliteScheduler(
        crawl.fake, crawl.settings, Boom(), clock=crawl.vt.clock, wall_clock=crawl.vt.wall,
        sleep=crawl.vt.sleep, rand=lambda: 0.5,
    )  # fmt: skip
    with pytest.raises(LookupError, match="sink is broken"):
        await crawl.run([u("/a")])


async def test_multiple_errors_stay_grouped() -> None:
    class Boom(RecordingSink):
        async def on_marked(self, entry: FrontierEntry, new_entry: FrontierEntry) -> None:
            del new_entry
            raise LookupError(entry.url)

    # Both robots.txt files disallow everything, so both on_marked calls fail in the same instant.
    closed = ok("User-agent: *\nDisallow: /\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: closed, u("/robots.txt", OTHER): closed})
    crawl.scheduler = PoliteScheduler(
        crawl.fake, crawl.settings, Boom(), clock=crawl.vt.clock, wall_clock=crawl.vt.wall,
        sleep=crawl.vt.sleep, rand=lambda: 0.5,
    )  # fmt: skip
    with pytest.raises(ExceptionGroup):
        await crawl.run([u("/a"), u("/a", OTHER)])


# --- sinks that really await (a database does) --------------------------------------------------


class YieldingSink(RecordingSink):
    """Hands control back to the loop inside every call, like a real async database sink."""

    async def on_robots(self, domain: str, policy: Any, result: Any) -> None:
        await asyncio.sleep(0)
        await super().on_robots(domain, policy, result)

    async def on_result(self, entry: Any, result: Any, classification: Any, new_entry: Any) -> None:
        await asyncio.sleep(0)
        await super().on_result(entry, result, classification, new_entry)

    async def on_marked(self, entry: Any, new_entry: Any) -> None:
        await asyncio.sleep(0)
        await super().on_marked(entry, new_entry)

    async def on_domain_stopped(self, domain: str, reason: str) -> None:
        await asyncio.sleep(0)
        await super().on_domain_stopped(domain, reason)


async def test_stop_is_ordered_and_explained_even_when_the_sink_yields() -> None:
    urls = [u(f"/y{n}") for n in range(6)]
    crawl = Crawl(
        {**{x: ok() for x in urls}, urls[1]: block_page()},
        max_concurrency_per_domain=2,
        requests_per_second=5.0,
        burst=5,  # tokens to spare: a waiting task is admitted while the sink is still writing
    )
    crawl.sink = YieldingSink(clock=crawl.vt.clock)
    crawl.scheduler = PoliteScheduler(
        crawl.fake, crawl.settings, crawl.sink, clock=crawl.vt.clock, wall_clock=crawl.vt.wall,
        sleep=crawl.vt.sleep, rand=lambda: 0.5,
    )  # fmt: skip
    summary = await crawl.run(urls)

    kinds = [(e.kind, e.url) for e in crawl.sink.events]
    stop_at = kinds.index(("stopped", None))
    assert kinds.index(("result", urls[1])) < stop_at  # the refusal is recorded before the stop
    assert all(kind != "marked" for kind, _ in kinds[:stop_at])
    reason = summary.domains_stopped["quotes.example.test"]
    assert "the site refused us" in reason  # the retry decision's reason, not a side effect
    assert len(crawl.sink.of("stopped")) == 1
    assert crawl.sink.finals()[urls[1]].status is FrontierStatus.STOPPED


async def test_entries_arriving_after_a_stop_are_marked_without_a_request() -> None:
    crawl = Crawl({u("/first"): block_page(), u("/late"): ok()})

    async def source() -> AsyncIterator[FrontierEntry]:
        yield entry(u("/first"))
        await crawl.vt.sleep(60)  # the stage streams more due entries later
        yield entry(u("/late"))

    await crawl.vt.run(crawl.scheduler.run(source()))
    assert u("/late") not in crawl.fake.urls_called()
    assert crawl.sink.finals()[u("/late")].status is FrontierStatus.STOPPED


async def test_only_the_probe_is_in_flight_while_half_open() -> None:
    crawl = Crawl(
        {u("/a"): [timeout(), slow(ok(), 5.0)], u("/b"): slow(ok(), 5.0)},
        breaker_failure_threshold=1,
        breaker_cooldown_s=10.0,
        max_concurrency_per_domain=2,
    )
    summary = await crawl.run([u("/a"), u("/b")])

    pages = sorted((c for c in crawl.fake.calls if c.url != ROBOTS), key=lambda c: c.started_at)
    failed, probe, after = pages
    assert failed.url == u("/a")
    assert probe.started_at >= 1.0 + 10.0  # nothing during the cooldown
    assert probe.ended_at is not None
    assert after.started_at >= probe.ended_at  # the other request waited for the probe's verdict
    assert summary.final_statuses == {FrontierStatus.DONE: 2}


async def test_two_refusals_in_flight_stop_the_domain_once() -> None:
    urls = [u("/x"), u("/y"), u("/z")]
    crawl = Crawl(
        {u("/x"): slow(block_page(), 1.0), u("/y"): slow(block_page(), 1.0), u("/z"): ok()},
        requests_per_second=5.0,
        burst=5,
        max_concurrency_per_domain=2,
    )
    summary = await crawl.run(urls)
    assert len(crawl.sink.of("stopped")) == 1
    assert len(crawl.sink.of("result")) == 2  # both in-flight answers are recorded
    assert u("/z") not in crawl.fake.urls_called()
    assert summary.final_statuses == {FrontierStatus.STOPPED: 3}


# --- redirects are followed by the scheduler, inside the site (R1-01) ----------------------------


async def test_same_origin_redirect_is_followed_as_a_new_request() -> None:
    crawl = Crawl({u("/old"): redirect("/new", 301), u("/new"): ok("<html>moved</html>")})
    summary = await crawl.run([u("/old")])

    assert crawl.fake.urls_called() == [ROBOTS, u("/old"), u("/new")]
    assert crawl.starts(u("/new")) == [2.0]  # the hop took its own token (1 req/s)
    done = crawl.sink.of("result")[0]
    assert done.result is not None
    assert done.result.requested_url == u("/old")
    assert done.result.final_url == u("/new")
    assert done.result.body == b"<html>moved</html>"
    assert crawl.sink.finals()[u("/old")].status is FrontierStatus.DONE
    assert (summary.attempts, summary.requests) == (1, 3)


@pytest.mark.parametrize(
    ("location", "host"),
    [
        ("http://169.254.169.254/latest/meta-data/", "169.254.169.254"),
        ("http://127.0.0.1:8080/admin", "127.0.0.1"),
        ("https://books.example.test/x", "books.example.test"),
        ("http://quotes.example.test:8080/a", "quotes.example.test"),  # scheme switch, odd port
        ("https://quotes.example.test:8443/a", "quotes.example.test"),  # other port
    ],
)
async def test_off_site_redirect_is_refused_permanently(location: str, host: str) -> None:
    crawl = Crawl({u("/go"): redirect(location)})
    await crawl.run([u("/go")])

    assert crawl.fake.urls_called() == [ROBOTS, u("/go")]  # never followed, never retried
    final = crawl.sink.finals()[u("/go")]
    assert final.status is FrontierStatus.FAILED_PERMANENT
    assert final.last_class is ResponseClass.CLIENT_ERROR
    assert f"off-site redirect to {host} refused" in (final.last_reason or "")


async def test_same_host_scheme_switch_on_default_ports_is_followed() -> None:
    # The live quotes site answers https://.../author/X with 308 to http://.../author/X/ (seen on
    # the first real run; the saved fixtures already had the slash, so only a live run showed it).
    target = "http://quotes.example.test/author/X/"
    crawl = Crawl(
        {u("/author/X"): redirect(target, status=308), target: ok("<html>author</html>")},
        allow_downgrade=True,  # the quotes sites opt in (SiteSpec.allow_scheme_downgrade)
    )
    summary = await crawl.run([u("/author/X")])

    assert crawl.fake.urls_called() == [ROBOTS, u("/author/X"), target]
    final = crawl.sink.finals()[u("/author/X")]
    assert final.status is FrontierStatus.DONE
    assert (summary.attempts, summary.requests) == (1, 3)


async def test_fetcher_that_followed_off_site_is_refused() -> None:
    # Defence in depth: a fetcher reporting a final_url on another origin (the reviewer's repro).
    page = Respond(200, b"<html>x</html>", final_url="http://169.254.169.254/latest/meta-data/")
    crawl = Crawl({u("/a"): page})
    await crawl.run([u("/a")])
    final = crawl.sink.finals()[u("/a")]
    assert final.status is FrontierStatus.FAILED_PERMANENT
    assert "off-site redirect to 169.254.169.254 refused" in (final.last_reason or "")


async def test_redirect_loop_is_permanent_after_five_hops() -> None:
    chain = {u(f"/l{n}"): redirect(f"/l{n + 1}") for n in range(10)}
    crawl = Crawl(chain)
    await crawl.run([u("/l0")])

    pages = [x for x in crawl.fake.urls_called() if x != ROBOTS]
    assert pages == [u(f"/l{n}") for n in range(6)]  # the request plus five hops, then stop
    final = crawl.sink.finals()[u("/l0")]
    assert final.status is FrontierStatus.FAILED_PERMANENT  # not 5 attempts x 6 requests
    assert "redirect loop" in (final.last_reason or "")


async def test_redirect_into_a_robots_disallowed_path_is_refused() -> None:
    robots = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: robots, u("/a"): redirect("/private/x"), u("/private/x"): ok()})
    await crawl.run([u("/a")])
    assert u("/private/x") not in crawl.fake.urls_called()
    assert "robots.txt disallows" in (crawl.sink.finals()[u("/a")].last_reason or "")


async def test_redirect_to_a_non_http_scheme_is_refused() -> None:
    crawl = Crawl({u("/a"): redirect("ftp://quotes.example.test/file")})
    await crawl.run([u("/a")])
    final = crawl.sink.finals()[u("/a")]
    assert final.status is FrontierStatus.FAILED_PERMANENT
    assert "not an http(s) URL" in (final.last_reason or "")


async def test_rate_bound_counts_every_redirect_hop() -> None:
    urls = [u(f"/r{n}") for n in range(6)]
    scripts: dict[str, Step | list[Step]] = {}
    for n in range(6):
        scripts[u(f"/r{n}")] = redirect(f"/r{n}/h1")
        scripts[u(f"/r{n}/h1")] = redirect(f"/r{n}/h2")
        scripts[u(f"/r{n}/h2")] = ok()
    crawl = Crawl(scripts, requests_per_second=2.0, burst=2, max_concurrency_per_domain=4)
    summary = await crawl.run(urls)
    times = [c.started_at for c in crawl.fake.calls]
    assert len(times) == summary.requests == 1 + 6 * 3
    assert_rate_respected(times, 2.0, 2)
    assert summary.final_statuses == {FrontierStatus.DONE: 6}


# --- breaker: a stale result is not the probe's answer (R1-10) -----------------------------------


async def test_stale_ok_does_not_close_the_half_open_breaker() -> None:
    late = FrontierEntry(
        url=u("/c"), site=entry(u("/c")).site, kind=entry(u("/c")).kind, depth=0,
        next_attempt_at=EPOCH + 10,
    )  # fmt: skip
    crawl = Crawl(
        {
            u("/slow"): slow(ok(), 20.0),  # sent at t=0, before the breaker opens; back at t=20
            u("/fail"): [timeout(), slow(ok(), 30.0)],  # opens it; then the probe, out 5..35
            u("/c"): ok(),
        },
        requests_per_second=5.0,
        burst=5,
        max_concurrency_per_domain=3,
        breaker_failure_threshold=1,
        breaker_cooldown_s=5.0,
    )
    await crawl.vt.run(crawl.scheduler.run([entry(u("/slow")), entry(u("/fail")), late]))

    probe = [c for c in crawl.fake.calls if c.url == u("/fail")][1]
    assert probe.started_at == pytest.approx(5.0)
    assert probe.ended_at == pytest.approx(35.0)
    # /c is due at t=10 while the probe is out. The stale ok from /slow at t=20 must not let it go.
    assert crawl.starts(u("/c")) == [pytest.approx(35.0)]
    assert {e.status for e in crawl.sink.finals().values()} == {FrontierStatus.DONE}


# --- bucket: a 429 on a slow request still costs the next request a full interval (R1-11) --------


async def test_next_request_after_a_slow_429_waits_a_full_interval() -> None:
    crawl = Crawl(
        {u("/a"): [slow(rate_limited(None), 3.0), ok()], u("/b"): ok()},
        requests_per_second=1.0,
        max_concurrency_per_domain=1,
    )
    await crawl.run([u("/a"), u("/b")])
    first = crawl.fake.calls[1]
    assert first.url == u("/a")
    assert first.ended_at == pytest.approx(4.0)  # sent at t=1, the 429 seen at t=4
    after = [c.started_at for c in crawl.fake.calls[2:]]
    # Rate halved to 0.5/s: the next request goes 2 s after the 429, not at once.
    assert min(after) == pytest.approx(4.0 + 2.0)


async def test_robots_redirect_loop_stops_the_domain() -> None:
    loop = {u("/robots.txt"): redirect("/r1")} | {
        u(f"/r{n}"): redirect(f"/r{n + 1}") for n in range(9)
    }
    crawl = Crawl({**loop, u("/a"): ok()})
    summary = await crawl.run([u("/a")])
    assert len(crawl.fake.calls) == 6  # robots.txt plus five hops, then give up
    reason = summary.domains_stopped["quotes.example.test"]
    assert reason == "robots.txt unreachable: redirect loop: more than 5 hops"


async def test_robots_redirect_to_a_non_http_scheme_stops_the_domain() -> None:
    crawl = Crawl({ROBOTS: redirect("ftp://quotes.example.test/robots.txt"), u("/a"): ok()})
    summary = await crawl.run([u("/a")])
    assert crawl.fake.urls_called() == [ROBOTS]
    assert "not an http(s) URL" in summary.domains_stopped["quotes.example.test"]


async def test_crawl_delay_over_the_limit_stops_the_domain() -> None:
    robots = ok("User-agent: *\nCrawl-delay: 600\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: robots, u("/a"): ok()}, max_crawl_delay_s=60.0)
    summary = await crawl.run([u("/a")])
    assert crawl.fake.urls_called() == [ROBOTS]
    assert (
        "Crawl-delay 600s exceeds the 60s limit" in summary.domains_stopped["quotes.example.test"]
    )
    assert crawl.sink.finals()[u("/a")].status is FrontierStatus.STOPPED


# --- round 2: robots.txt is never read through the browser (R2-04) -------------------------------


async def test_a_browser_site_reads_robots_through_the_raw_fetcher_and_obeys_it() -> None:
    # The page fetcher stands in for the browser: it has no robots.txt script, so had it been asked
    # it would answer 404 (allow all) and /private/x would be fetched. The raw fetcher has the rules.
    rules = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl(
        {u("/private/x"): ok(), u("/a"): ok()},
        robots={ROBOTS: rules},
        requests_per_second=1.0,
    )
    summary = await crawl.run([u("/private/x"), u("/a")])

    assert crawl.robots_fake is not None
    assert crawl.robots_fake.urls_called() == [ROBOTS]
    assert crawl.fake.urls_called() == [u("/a")]  # no robots.txt and no disallowed page
    finals = crawl.sink.finals()
    assert finals[u("/private/x")].status is FrontierStatus.SKIPPED_ROBOTS
    assert finals[u("/a")].status is FrontierStatus.DONE
    # Same politeness budget: robots.txt took the first token, the page waited a full interval.
    assert crawl.robots_fake.calls[0].started_at == 0.0
    assert crawl.starts(u("/a")) == [1.0]
    assert summary.requests == 2


async def test_robots_redirects_through_the_raw_fetcher_are_followed_by_it() -> None:
    moved = u("/rules/robots.txt")
    rules = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl({u("/private/x"): ok()}, robots={ROBOTS: redirect(moved), moved: rules})
    await crawl.run([u("/private/x")])
    assert crawl.robots_fake is not None
    assert crawl.robots_fake.urls_called() == [ROBOTS, moved]
    assert crawl.fake.urls_called() == []
    assert crawl.sink.finals()[u("/private/x")].status is FrontierStatus.SKIPPED_ROBOTS


# --- round 2: robots.txt redirects are confined like page redirects (R2-09) ----------------------


@pytest.mark.parametrize(
    ("location", "authority"),
    [
        ("http://quotes.example.test:6379/robots.txt", "quotes.example.test:6379"),
        ("https://quotes.example.test:8443/robots.txt", "quotes.example.test:8443"),
    ],
)
async def test_robots_redirect_to_another_port_on_the_same_host_is_refused(
    location: str, authority: str
) -> None:
    rules = ok("User-agent: *\nDisallow: /secret\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: redirect(location), location: rules, u("/a"): ok()})
    summary = await crawl.run([u("/a")])
    assert crawl.fake.urls_called() == [ROBOTS]  # the other server is never asked
    reason = summary.domains_stopped["quotes.example.test"]
    assert reason == f"robots.txt unreachable: redirect to {authority} refused"
    assert crawl.sink.finals()[u("/a")].status is FrontierStatus.STOPPED


async def test_robots_same_host_scheme_switch_is_followed_and_counted() -> None:
    target = "http://quotes.example.test/robots.txt"
    rules = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl(
        {ROBOTS: redirect(target, 308), target: rules, u("/private/x"): ok()},
        allow_downgrade=True,
    )
    summary = await crawl.run([u("/private/x")])
    assert crawl.fake.urls_called() == [ROBOTS, target]
    assert crawl.sink.finals()[u("/private/x")].status is FrontierStatus.SKIPPED_ROBOTS
    assert summary.scheme_downgrades == 1


# --- round 2: Location is resolved like a link, empty segments kept (R2-10) ----------------------


async def test_redirect_location_keeps_empty_path_segments() -> None:
    # urljoin would turn ../c//d into /c/d, a different resource than the one the server named.
    crawl = Crawl({u("/a/b"): redirect("../c//d"), u("/c//d"): ok("<html>here</html>")})
    await crawl.run([u("/a/b")])
    assert crawl.fake.urls_called() == [ROBOTS, u("/a/b"), u("/c//d")]
    done = crawl.sink.of("result")[0]
    assert done.result is not None
    assert done.result.final_url == u("/c//d")


async def test_robots_redirect_location_keeps_empty_path_segments() -> None:
    rules = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: redirect("../c//d"), u("/c//d"): rules, u("/private/x"): ok()})
    await crawl.run([u("/private/x")])
    assert crawl.fake.urls_called() == [ROBOTS, u("/c//d")]
    assert crawl.sink.finals()[u("/private/x")].status is FrontierStatus.SKIPPED_ROBOTS


# --- round 2: downgrades are counted (R2-08, ADR 0010) -------------------------------------------


async def test_only_https_to_http_hops_count_as_downgrades() -> None:
    down = "http://quotes.example.test/author/X/"
    up_home = "http://quotes.example.test"
    crawl = Crawl(
        {
            u("/author/X"): redirect(down, 308),
            down: ok(),
            u("/a"): redirect("/b"),  # same origin: not a downgrade
            u("/b"): ok(),
            u("/up", up_home): redirect(u("/up/")),  # http -> https: an upgrade
            u("/up/"): ok(),
        },
        allow_downgrade=True,
    )
    summary = await crawl.vt.run(
        crawl.scheduler.run([entry(u("/author/X")), entry(u("/a")), entry(u("/up", up_home))])
    )
    assert summary.scheme_downgrades == 1
    assert summary.final_statuses == {FrontierStatus.DONE: 3}


# --- round 2: a robots.txt 429 slows the domain's rate, not just the retry (S17) ----------------


async def test_robots_rate_limited_answer_slows_the_domain_down() -> None:
    urls = [u(f"/r{n}") for n in range(4)]
    crawl = Crawl(
        {ROBOTS: [rate_limited(None), ok("")], **{x: ok() for x in urls}},
        requests_per_second=2.0,
        max_concurrency_per_domain=1,
        backoff_base_s=0.1,
    )
    await crawl.run(urls)
    starts = sorted(c.started_at for c in crawl.fake.calls if c.url != ROBOTS)
    gaps = [b - a for a, b in itertools.pairwise(starts)]
    # The robots.txt 429 halved the rate from 2/s to 1/s; each page success then adds 0.05/s back
    # (AIMD). Without the slow-down the pages would go out every 0.5 s.
    assert gaps == pytest.approx([1 / 1.05, 1 / 1.10, 1 / 1.15])


# --- round 2: the breaker's openings reach the sink (I2-1) ---------------------------------------


async def test_breaker_opening_is_reported_once_per_opening() -> None:
    crawl = Crawl(
        {u("/a"): [timeout(), timeout(), timeout(), ok()]},
        breaker_failure_threshold=2,
        breaker_cooldown_s=30.0,
        max_concurrency_per_domain=1,
        max_attempts=10,
    )
    summary = await crawl.run([u("/a")])
    opened = crawl.sink.of("breaker")
    # Two timeouts open it; the probe times out and it opens again; the next probe closes it.
    assert [e.domain for e in opened] == ["quotes.example.test"] * 2
    assert all(e.reason for e in opened)
    assert summary.final_statuses == {FrontierStatus.DONE: 1}
    # Each opening is reported before the attempt that caused it is recorded.
    kinds = [e.kind for e in crawl.sink.events if e.kind in {"breaker", "result"}]
    assert kinds == ["result", "breaker", "result", "breaker", "result", "result"]


@pytest.mark.parametrize(
    ("url", "origin"),
    [
        ("https://Quotes.Example.test/a", ("https", "quotes.example.test", 443)),
        ("http://quotes.example.test:8080/", ("http", "quotes.example.test", 8080)),
        ("ftp://quotes.example.test/", None),
        ("https:///no-host", None),
        ("https://quotes.example.test:99999/", None),  # invalid port
    ],
)
def test_origin_of(url: str, origin: tuple[str, str, int] | None) -> None:
    assert origin_of(url) == origin


# --- round 3: a downgrade is counted once its hop is admitted (R3-03) ----------------------------


async def test_a_downgrade_hop_the_domain_stops_before_sending_is_not_counted() -> None:
    # t=0 robots.txt; t=1 /slow-block goes out and answers 403 at t=2.5; t=2 /author/X answers 308
    # to http. Its hop waits for the t=3 token, but the domain stopped at t=2.5: never sent.
    down = "http://quotes.example.test/author/X/"
    crawl = Crawl(
        {
            u("/slow-block"): slow(block_page(), 1.5),
            u("/author/X"): redirect(down, 308),
            down: ok(),
        },
        allow_downgrade=True,
        max_concurrency_per_domain=2,
    )
    summary = await crawl.run([u("/slow-block"), u("/author/X")])
    assert crawl.fake.urls_called() == [ROBOTS, u("/slow-block"), u("/author/X")]
    assert "quotes.example.test" in summary.domains_stopped
    assert summary.scheme_downgrades == 0  # the clear-text request was never made


# --- round 3: the https to http hop is a per-site opt-in (R3-06, ADR 0010) -----------------------


async def test_without_the_opt_in_a_same_host_downgrade_is_refused_as_off_site() -> None:
    down = "http://quotes.example.test/author/X/"
    crawl = Crawl({u("/author/X"): redirect(down, 308), down: ok()})
    summary = await crawl.run([u("/author/X")])
    assert crawl.fake.urls_called() == [ROBOTS, u("/author/X")]  # never sent in clear text
    final = crawl.sink.finals()[u("/author/X")]
    assert final.status is FrontierStatus.FAILED_PERMANENT
    assert final.last_class is ResponseClass.CLIENT_ERROR
    assert final.last_reason is not None
    assert "off-site redirect to quotes.example.test refused" in final.last_reason
    assert "https to http downgrade not allowed for this site" in final.last_reason
    assert summary.scheme_downgrades == 0


async def test_with_the_opt_in_the_same_downgrade_is_followed_and_counted() -> None:
    down = "http://quotes.example.test/author/X/"
    crawl = Crawl({u("/author/X"): redirect(down, 308), down: ok()}, allow_downgrade=True)
    summary = await crawl.run([u("/author/X")])
    assert crawl.fake.urls_called() == [ROBOTS, u("/author/X"), down]
    assert crawl.sink.finals()[u("/author/X")].status is FrontierStatus.DONE
    assert summary.scheme_downgrades == 1


async def test_without_the_opt_in_an_upgrade_is_still_followed() -> None:
    home = "http://quotes.example.test"
    crawl = Crawl({u("/up", home): redirect(u("/up/")), u("/up/"): ok()})
    summary = await crawl.vt.run(crawl.scheduler.run([entry(u("/up", home))]))
    assert crawl.sink.finals()[u("/up", home)].status is FrontierStatus.DONE
    assert summary.scheme_downgrades == 0


async def test_without_the_opt_in_a_fetcher_that_followed_a_downgrade_is_refused() -> None:
    page = Respond(200, b"<html>x</html>", final_url="http://quotes.example.test/a")
    crawl = Crawl({u("/a"): page})
    await crawl.run([u("/a")])
    final = crawl.sink.finals()[u("/a")]
    assert final.status is FrontierStatus.FAILED_PERMANENT
    assert "off-site redirect to quotes.example.test refused" in (final.last_reason or "")


async def test_without_the_opt_in_a_robots_downgrade_stops_the_domain() -> None:
    target = "http://quotes.example.test/robots.txt"
    rules = ok("User-agent: *\nDisallow: /private/\n", content_type="text/plain")
    crawl = Crawl({ROBOTS: redirect(target, 308), target: rules, u("/a"): ok()})
    summary = await crawl.run([u("/a")])
    assert crawl.fake.urls_called() == [ROBOTS]
    assert summary.domains_stopped["quotes.example.test"] == (
        "robots.txt unreachable: redirect to quotes.example.test refused: "
        "https to http downgrade not allowed for this site"
    )
    assert summary.scheme_downgrades == 0
