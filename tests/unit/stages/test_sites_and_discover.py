"""The site registry, and the discover stage (seeds plus offline link planning)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pyarrow as pa
import pytest

from fixture_server import SITES
from scraping_pipeline.core.contracts import Author, Book, Quote
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, PageKind, Site
from scraping_pipeline.fetch.httpx_fetcher import HttpxFetcher
from scraping_pipeline.fetch.playwright_fetcher import PlaywrightFetcher
from scraping_pipeline.sites import SITES as REGISTRY
from scraping_pipeline.sites import get_site
from scraping_pipeline.stages import run_discover
from scraping_pipeline.stages.common import expand_from_cache
from scraping_pipeline.stages.export import arrow_schema
from scraping_pipeline.storage.cache import FetchMeta, PageCache
from scraping_pipeline.storage.state import Stage, StateStore
from stage_support import stage_settings

QUOTES_HOME = "https://quotes.toscrape.com/"

pytestmark = pytest.mark.usefixtures("no_real_fetchers")


def test_every_site_is_registered_with_coherent_tables() -> None:
    assert set(REGISTRY) == set(Site)
    for site, spec in REGISTRY.items():
        assert spec.site is site
        assert spec.item_kinds
        assert spec.tables
        for table in spec.tables:
            assert set(table.sort_key) <= set(table.model.model_fields)


def test_seeds_and_origin_follow_the_configured_base_urls(tmp_path: Path) -> None:
    settings = stage_settings(
        tmp_path, books_base_url="http://127.0.0.1:8000/", quotes_base_url="http://127.0.0.1:9/"
    )
    assert get_site(Site.BOOKS).seed_urls(settings) == ("http://127.0.0.1:8000/",)
    assert get_site(Site.QUOTES).seed_urls(settings) == ("http://127.0.0.1:9/",)
    assert get_site(Site.QUOTES_JS).seed_urls(settings) == ("http://127.0.0.1:9/js/",)
    assert get_site(Site.QUOTES_JS).origin(settings) == "http://127.0.0.1:9/"


def test_each_site_gets_its_fetcher(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)
    httpx_fetcher = get_site(Site.BOOKS).make_fetcher(settings)
    assert isinstance(httpx_fetcher, HttpxFetcher)
    asyncio.run(httpx_fetcher.aclose())
    browser = get_site(Site.QUOTES_JS).make_fetcher(settings)  # launches nothing until used
    assert isinstance(browser, PlaywrightFetcher)


def test_a_browser_site_reads_robots_txt_with_a_raw_http_fetcher(tmp_path: Path) -> None:
    # R2-04: robots.txt through Chromium either times out on the selector wait or parses the text
    # viewer's HTML as "no rules". A browser site gets an HttpxFetcher for it; others reuse theirs.
    settings = stage_settings(tmp_path)
    raw = get_site(Site.QUOTES_JS).make_robots_fetcher(settings)
    assert isinstance(raw, HttpxFetcher)
    asyncio.run(raw.aclose())
    assert get_site(Site.BOOKS).make_robots_fetcher(settings) is None
    assert get_site(Site.QUOTES).make_robots_fetcher(settings) is None


def test_books_context_is_the_home_page_category_set() -> None:
    home = (SITES / "books" / "index.html").read_text(encoding="utf-8")
    context = get_site(Site.BOOKS).build_context([home])
    assert len(context["categories"]) == 50
    assert context["categories"] == sorted(context["categories"])
    assert {"Poetry", "Travel", "Mystery"} <= set(context["categories"])
    # Without the home page the set is empty: every book then fails unknown_category.
    assert get_site(Site.BOOKS).build_context([None]) == {"categories": []}
    assert get_site(Site.QUOTES).build_context([None]) == {}


def test_only_the_static_quotes_site_requires_author_links() -> None:
    quotes = get_site(Site.QUOTES).build_dataset([], {})
    js = get_site(Site.QUOTES_JS).build_dataset([], {})
    assert getattr(quotes, "require_author_links") is True  # noqa: B009
    assert getattr(js, "require_author_links") is False  # noqa: B009
    assert [t.name for t in get_site(Site.QUOTES_JS).tables] == ["quotes"]


def test_table_for_refuses_a_record_of_another_site() -> None:
    with pytest.raises(ValueError, match="no table"):
        get_site(Site.BOOKS).table_for(Author.model_construct())  # type: ignore[call-arg]
    quote = Quote.model_construct()  # type: ignore[call-arg]
    assert get_site(Site.QUOTES).table_for(quote).name == "quotes"


def test_arrow_schemas_follow_the_contracts() -> None:
    books = arrow_schema(Book)
    assert books.names == list(Book.model_fields)
    assert books.field("price_gbp").type == pa.decimal128(18, 2)
    assert books.field("in_stock").type == pa.bool_()
    assert books.field("stock_count").type == pa.int64()
    assert not any(f.nullable for f in books)
    quotes = arrow_schema(Quote)
    assert quotes.field("tags").type == pa.list_(pa.string())
    assert quotes.field("author_slug").nullable
    assert arrow_schema(Author).field("born_date").type == pa.date32()


# --- discover ------------------------------------------------------------------------------------


def _cache_page(settings_dir: Path, url: str, fixture: Path) -> None:
    PageCache(settings_dir / "cache").put(
        url,
        fixture.read_bytes(),
        FetchMeta(url, 200, "2026-11-24T00:00:00+00:00", "fetch-test", "httpx"),
    )


def test_discover_adds_seeds_once(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)
    first = run_discover(settings, Site.QUOTES)
    second = run_discover(settings, Site.QUOTES)
    assert (first.seeds_added, first.links_added, first.frontier_size) == (1, 0, 1)
    assert (second.seeds_added, second.links_added, second.frontier_size) == (0, 0, 1)
    with StateStore.open(settings.state_db) as store:
        assert [e.url for e in store.entries(Site.QUOTES)] == [QUOTES_HOME]
        assert [r.stage for r in store.runs(Site.QUOTES)] == [Stage.DISCOVER] * 2


def test_discover_plans_links_from_the_cache_offline_and_idempotently(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)
    _cache_page(tmp_path, QUOTES_HOME, SITES / "quotes" / "index.html")
    result = run_discover(settings, Site.QUOTES)
    with StateStore.open(settings.state_db) as store:
        entries = {e.url: e for e in store.entries(Site.QUOTES)}
    # The home page links 8 distinct authors and the next listing page.
    assert result.links_added == 9
    assert entries["https://quotes.toscrape.com/page/2/"].kind is PageKind.LISTING
    assert entries["https://quotes.toscrape.com/author/Albert-Einstein"].kind is PageKind.AUTHOR
    assert all(e.depth == 1 for url, e in entries.items() if url != QUOTES_HOME)
    assert all(e.status is FrontierStatus.PENDING for e in entries.values())
    assert run_discover(settings, Site.QUOTES).links_added == 0


@pytest.mark.slow
def test_discover_respects_max_depth_and_max_pages(tmp_path: Path) -> None:
    _cache_page(tmp_path / "a", QUOTES_HOME, SITES / "quotes" / "index.html")
    assert run_discover(stage_settings(tmp_path / "a", max_depth=0), Site.QUOTES).links_added == 0
    _cache_page(tmp_path / "b", QUOTES_HOME, SITES / "quotes" / "index.html")
    capped = run_discover(stage_settings(tmp_path / "b", max_pages=4), Site.QUOTES)
    assert (capped.links_added, capped.frontier_size) == (3, 4)


@pytest.mark.slow
def test_offline_expansion_reaches_a_fixpoint_through_cached_children(tmp_path: Path) -> None:
    # A frontier rebuilt next to an old cache: page 2 is cached too, so its links are planned in
    # the same pass, without a request.
    settings = stage_settings(tmp_path)
    _cache_page(tmp_path, QUOTES_HOME, SITES / "quotes" / "index.html")
    _cache_page(
        tmp_path, "https://quotes.toscrape.com/page/2/", SITES / "quotes" / "page/2/index.html"
    )
    spec = get_site(Site.QUOTES)
    with StateStore.open(settings.state_db) as store:
        store.start_run("r", Site.QUOTES, Stage.DISCOVER, "2026-01-01T00:00:00+00:00", "{}")
        store.add_entries([FrontierEntry(QUOTES_HOME, Site.QUOTES, PageKind.LISTING, 0)], "r")
        added = expand_from_cache(store, PageCache(settings.cache_dir), spec, settings, "r")
    urls = {e.url for e in added}
    assert "https://quotes.toscrape.com/page/3/" in urls  # planned from cached page 2
    assert {e.depth for e in added if e.url.endswith("/page/3/")} == {2}
