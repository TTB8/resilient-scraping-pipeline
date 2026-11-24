"""HttpxFetcher against the local fixture server: real sockets, loopback only."""

import socket
import time

from fetch_support import make_settings
from fixture_server import FixtureServer, Phase
from scraping_pipeline.core.classify import TransportError
from scraping_pipeline.fetch.httpx_fetcher import HttpxFetcher


async def test_200_body_and_headers(fixture_server: FixtureServer) -> None:
    async with HttpxFetcher(make_settings()) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/page/2/"))
    assert result.status == 200
    assert result.error is None
    assert result.headers["content-type"] == "text/html; charset=utf-8"
    assert b'class="quote"' in result.body
    assert result.final_url == fixture_server.url("/page/2/")
    assert result.elapsed_s >= 0


async def test_404_for_unknown_path(fixture_server: FixtureServer) -> None:
    async with HttpxFetcher(make_settings()) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/no/such/page/"))
    assert result.status == 404
    assert result.error is None


async def test_path_traversal_is_refused(fixture_server: FixtureServer) -> None:
    async with HttpxFetcher(make_settings()) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/..%2F..%2Fconftest.py"))
    assert result.status == 404


async def test_redirect_is_returned_for_the_scheduler(fixture_server: FixtureServer) -> None:
    fixture_server.redirect("/old", "/page/2/", status=301)
    async with HttpxFetcher(make_settings()) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/old"))
    assert result.status == 301
    assert result.headers["location"] == "/page/2/"
    assert result.final_url == fixture_server.url("/old")
    assert fixture_server.hit_counts() == {"/old": 1}  # the hop is the scheduler's decision


async def test_dripping_body_hits_the_overall_deadline(fixture_server: FixtureServer) -> None:
    # 40 bytes, one every 0.1 s: every read is far inside httpx's per-read timeout, the whole
    # body takes 4 s. Without the overall deadline this returned 200 after 4 s (R1-03).
    fixture_server.scenario(Phase.drip(0.1))
    async with HttpxFetcher(make_settings(request_timeout_s=0.5)) as fetcher:
        started = time.monotonic()
        result = await fetcher.fetch(fixture_server.url("/"))
        took = time.monotonic() - started
    assert result.error is TransportError.TIMEOUT
    # What the deadline proves: it returned well before the 4 s the whole body takes. The margin
    # is wide so a loaded runner cannot fail it.
    assert took < 3.0


async def test_slow_response_maps_to_timeout(fixture_server: FixtureServer) -> None:
    fixture_server.scenario(Phase.slow(5.0))
    async with HttpxFetcher(make_settings(request_timeout_s=0.2)) as fetcher:
        result = await fetcher.fetch(fixture_server.url("/"))
    assert result.error is TransportError.TIMEOUT
    assert result.status is None
    assert result.elapsed_s < 2.5  # it gave up long before the server's 5 s answer


async def test_closed_port_maps_to_connect_error() -> None:
    with socket.socket() as probe:  # grab a free port, then close it so nothing listens there
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    async with HttpxFetcher(make_settings(request_timeout_s=2.0)) as fetcher:
        result = await fetcher.fetch(f"http://127.0.0.1:{port}/")
    assert result.error is TransportError.CONNECT
    assert result.status is None


async def test_server_receives_the_configured_user_agent(fixture_server: FixtureServer) -> None:
    settings = make_settings(user_agent="fixture-check/9.9 (+https://example.org/contact)")
    async with HttpxFetcher(settings) as fetcher:
        await fetcher.fetch(fixture_server.url("/"))
        await fetcher.fetch(fixture_server.url("/robots.txt"))
    assert {hit.user_agent for hit in fixture_server.log()} == {settings.user_agent}


async def test_body_cap_against_a_real_server(fixture_server: FixtureServer) -> None:
    fixture_server.route("/big", Phase(200, b"z" * 50_000, {"Content-Type": "text/plain"}))
    async with HttpxFetcher(make_settings(), max_body_bytes=20_000) as fetcher:
        big = await fetcher.fetch(fixture_server.url("/big"))
        fine = await fetcher.fetch(fixture_server.url("/"))
    assert big.error is TransportError.PROTOCOL
    assert big.status == 200
    assert fine.error is None


async def test_scenarios_play_in_order(fixture_server: FixtureServer) -> None:
    fixture_server.robots_txt = "User-agent: *\nDisallow:\n"
    fixture_server.scenario(Phase.serve(1), Phase.error(503, times=2), Phase.challenge())
    async with HttpxFetcher(make_settings()) as fetcher:
        robots = await fetcher.fetch(fixture_server.url("/robots.txt"))
        statuses = [(await fetcher.fetch(fixture_server.url("/"))).status for _ in range(5)]
    assert robots.status == 200  # robots.txt never consumes scenario steps
    assert statuses == [200, 503, 503, 200, 200]
    log = fixture_server.log()
    assert [hit.status for hit in log] == [200, 200, 503, 503, 200, 200]


async def test_path_scenarios_count_only_their_own_path(fixture_server: FixtureServer) -> None:
    fixture_server.path_scenario("/page/2/", Phase.rate_limited("1", times=1))
    fixture_server.path_scenario("/author/Albert-Einstein/", Phase.block())
    async with HttpxFetcher(make_settings()) as fetcher:
        home = await fetcher.fetch(fixture_server.url("/"))
        limited = await fetcher.fetch(fixture_server.url("/page/2/"))
        again = await fetcher.fetch(fixture_server.url("/page/2/"))
        blocked = [
            (await fetcher.fetch(fixture_server.url("/author/Albert-Einstein/"))).status
            for _ in range(2)
        ]
    assert (home.status, limited.status, again.status) == (200, 429, 200)
    assert limited.headers["retry-after"] == "1"
    assert b'class="quote"' in again.body
    assert blocked == [403, 403]
