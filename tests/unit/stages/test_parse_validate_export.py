"""parse, validate and export over a cache fetched from the saved sites (no network anywhere)."""

from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel, Field

from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FrontierEntry, mark_skipped_robots, mark_stopped
from scraping_pipeline.core.lineage import derive_parse_run_id
from scraping_pipeline.core.parsers import books as books_parser
from scraping_pipeline.core.types import FrontierStatus, PageKind, ResponseClass, Site
from scraping_pipeline.core.validate import Verdict
from scraping_pipeline.fetch.fake import FakeFetcher
from scraping_pipeline.stages import run_export, run_parse, run_validate
from scraping_pipeline.stages.common import SitePaths, StageInputError, read_jsonl
from scraping_pipeline.stages.export import QUARANTINE_COLUMNS, _page_counts, arrow_schema
from scraping_pipeline.storage.cache import FetchMeta, PageCache
from scraping_pipeline.storage.state import (
    DomainEvent,
    DomainEventKind,
    RunStatus,
    Stage,
    StateStore,
)
from stage_support import stage_settings

pytestmark = pytest.mark.usefixtures("no_real_fetchers")


def _files(paths: SitePaths) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes()
        for root in (paths.parsed, paths.validated, paths.output)
        for p in sorted(root.rglob("*"))
        if p.is_file() and not p.name.startswith("run_report")
    }


# --- parse ---------------------------------------------------------------------------------------


def test_parse_reads_only_the_cache(
    quotes_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_network(self: object, url: str) -> None:
        raise AssertionError(f"parse fetched {url}")

    monkeypatch.setattr(FakeFetcher, "fetch", no_network)  # real fetchers are refused already
    result = run_parse(quotes_settings, Site.QUOTES)
    assert (result.pages_parsed, result.records, result.warnings) == (4, 22, 0)
    assert (result.item_pages_fetched, result.item_pages_with_records) == (4, 4)


def test_parse_run_id_is_derived_from_this_sites_cache(quotes_settings: Settings) -> None:
    result = run_parse(quotes_settings, Site.QUOTES)
    cache = PageCache(quotes_settings.cache_dir)
    with StateStore.open(quotes_settings.state_db) as store:
        urls = [e.url for e in store.entries(Site.QUOTES)]
    manifest = cache.manifest_sha256(urls, with_fetch_run_ids=True)
    assert result.manifest_sha256 == manifest
    assert result.parse_run_id == derive_parse_run_id(
        manifest, "quotes-parser/1.0.0", "quotes/1.0.0"
    )
    rows = read_jsonl(result.paths.records)
    assert {r["lineage"]["parse_run_id"] for r in rows} == {result.parse_run_id}
    # Rows are in (source_url, position) order and carry the page's own fetch run id.
    keys = [(r["lineage"]["source_url"], r["draft"].get("position", 0)) for r in rows]
    assert keys == sorted(keys)
    fetch_ids = {r["lineage"]["fetch_run_id"] for r in rows}
    assert len(fetch_ids) == 1
    assert next(iter(fetch_ids)).startswith("fetch-")


def test_same_bodies_from_another_fetch_run_get_another_parse_run_id(
    quotes_settings: Settings, tmp_path: Path
) -> None:
    first = run_parse(quotes_settings, Site.QUOTES)
    first_bytes = first.paths.records.read_bytes()
    again = run_parse(quotes_settings, Site.QUOTES)
    assert again.parse_run_id == first.parse_run_id
    assert again.paths.records.read_bytes() == first_bytes

    # Re-store every page with the same bytes under another fetch run id.
    cache = PageCache(quotes_settings.cache_dir)
    for entry in list(cache.iter_entries()):
        cache.put(
            entry.url,
            cache.read_body(entry),
            FetchMeta(entry.final_url, entry.status, entry.fetched_at, "fetch-other", "httpx"),
        )
    other = run_parse(quotes_settings, Site.QUOTES)
    assert other.parse_run_id != first.parse_run_id
    assert other.paths.records.read_bytes() != first_bytes


@pytest.mark.slow
def test_another_sites_pages_do_not_change_the_parse_run_id(quotes_settings: Settings) -> None:
    before = run_parse(quotes_settings, Site.QUOTES).parse_run_id

    PageCache(quotes_settings.cache_dir).put(
        "https://books.toscrape.com/",
        b"<html>another site</html>",
        FetchMeta("https://books.toscrape.com/", 200, "2026-11-24T00:00:00+00:00", "f", "httpx"),
    )
    assert run_parse(quotes_settings, Site.QUOTES).parse_run_id == before


def test_a_corrupt_cache_object_is_reported_not_parsed(quotes_settings: Settings) -> None:
    cache = PageCache(quotes_settings.cache_dir)
    home = cache.get_entry("https://quotes.toscrape.com/")
    assert home is not None
    obj = cache.root / "objects" / home.content_sha256[:2] / f"{home.content_sha256}.gz"
    obj.write_bytes(b"not gzip")
    result = run_parse(quotes_settings, Site.QUOTES)
    stats = json.loads(result.paths.parse_stats.read_text())
    assert stats["unreadable"] == ["https://quotes.toscrape.com/"]
    assert result.pages_parsed == 3


# --- validate ------------------------------------------------------------------------------------


def test_validate_quarantines_instead_of_dropping(quotes_settings: Settings) -> None:
    run_parse(quotes_settings, Site.QUOTES)
    result = run_validate(quotes_settings, Site.QUOTES)
    report = result.report
    # Most authors were never saved (404), so their quotes fail unknown_author: kept, explained.
    assert report.verdict is Verdict.FAIL
    assert not result.passed
    assert report.total_rows == 22
    assert report.valid_rows + report.quarantined_rows == report.total_rows
    assert result.rows_by_table == {"authors": 2, "quotes": 6}
    quarantine = read_jsonl(result.paths.quarantine)
    assert len(quarantine) == report.quarantined_rows == 14
    assert {f["rule"] for q in quarantine for f in q["failures"]} == {"unknown_author"}
    assert json.loads(result.paths.validation_report.read_text())["verdict"] == "fail"
    authors = read_jsonl(result.paths.valid_rows("authors"))
    assert [a["author_slug"] for a in authors] == ["Albert-Einstein", "J-K-Rowling"]
    assert authors[0]["born_date"] == "1879-03-14"
    with StateStore.open(quotes_settings.state_db) as store:
        run = store.get_run(result.run_id)
    assert run is not None
    assert run.status is RunStatus.FAILED  # the data failed, not the code


@pytest.mark.slow
def test_validate_passes_within_the_tolerated_rate(quotes_settings: Settings) -> None:
    settings = stage_settings(quotes_settings.data_dir, max_failure_rate=0.7)
    run_parse(settings, Site.QUOTES)
    assert run_validate(settings, Site.QUOTES).passed


def test_validate_needs_parse_first(tmp_path: Path) -> None:
    with pytest.raises(StageInputError, match="run the previous stage"):
        run_validate(stage_settings(tmp_path), Site.QUOTES)


@pytest.mark.slow
def test_validate_refuses_malformed_parse_output(quotes_settings: Settings) -> None:
    result = run_parse(quotes_settings, Site.QUOTES)
    result.paths.records.write_text('{"record_type": "quote"}\n')
    with pytest.raises(StageInputError, match="malformed row"):
        run_validate(quotes_settings, Site.QUOTES)
    result.paths.records.write_text("")
    result.paths.context.write_text("[]\n")
    with pytest.raises(StageInputError, match="expected an object"):
        run_validate(quotes_settings, Site.QUOTES)


@pytest.mark.slow
def test_books_categories_come_from_the_cached_home_page(books_settings: Settings) -> None:
    run_parse(books_settings, Site.BOOKS)
    paths = SitePaths.of(books_settings, Site.BOOKS)
    context = json.loads(paths.context.read_text())
    home = PageCache(books_settings.cache_dir).get_entry("https://books.toscrape.com/")
    assert home is not None
    body = PageCache(books_settings.cache_dir).read_body(home).decode()
    assert context["categories"] == sorted(books_parser.category_names(body))
    assert run_validate(books_settings, Site.BOOKS).passed


# --- export --------------------------------------------------------------------------------------


@pytest.mark.slow
def test_export_publishes_typed_sorted_tables_and_a_report(books_settings: Settings) -> None:
    run_parse(books_settings, Site.BOOKS)
    run_validate(books_settings, Site.BOOKS)
    result = run_export(books_settings, Site.BOOKS, started_at="2026-11-24T00:00:00+00:00")
    out = result.paths.output
    assert result.passed
    assert result.rows_by_table == {"books": 1}
    table = pq.read_table(out / "books.parquet")
    assert table.num_rows == 1
    assert str(table.column("price_gbp")[0].as_py()) == "51.77"
    with (out / "books.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["title"] == "A Light in the Attic"
    assert rows[0]["price_gbp"] == "51.77"
    assert (out / "quarantine.csv").read_text() == ",".join(QUARANTINE_COLUMNS) + "\n"

    report = json.loads((out / "run_report.json").read_text())
    assert report["started_at"] == "2026-11-24T00:00:00+00:00"
    assert report["validation"]["verdict"] == "pass"
    assert report["pages"]["fetched"] == 3
    assert report["pages"]["failed_by_class"] == {"not_found": 27}
    assert (out / "run_report.md").read_text() == result.markdown
    assert "**PASS**" in result.markdown


@pytest.mark.slow
def test_export_writes_empty_tables_and_quarantine_lines(quotes_settings: Settings) -> None:
    run_parse(quotes_settings, Site.QUOTES)
    run_validate(quotes_settings, Site.QUOTES)
    result = run_export(quotes_settings, Site.QUOTES)
    assert not result.passed
    out = result.paths.output
    with (out / "quarantine.csv").open(encoding="utf-8", newline="") as handle:
        lines = list(csv.DictReader(handle))
    assert len(lines) == 14
    assert {line["rule"] for line in lines} == {"unknown_author"}
    assert [int(line["row"]) for line in lines] == list(range(14))
    quotes = pq.read_table(out / "quotes.parquet")
    assert quotes.column("tags").type.value_type == "string"
    ids = quotes.column("quote_id").to_pylist()
    assert len(ids) == 6
    assert ids == sorted(ids)  # sorted by the site's key, not by page or source
    with (out / "quotes.csv").open(encoding="utf-8", newline="") as handle:
        assert [row["quote_id"] for row in csv.DictReader(handle)] == ids
    with StateStore.open(quotes_settings.state_db) as store:
        fetch_run = [r for r in store.runs(Site.QUOTES) if r.stage is Stage.FETCH][-1]
    assert result.report.fetch_run_id == fetch_run.run_id
    assert result.report.started_at == fetch_run.started_at


@pytest.mark.slow
def test_export_refuses_stale_or_tampered_staging(quotes_settings: Settings) -> None:
    run_parse(quotes_settings, Site.QUOTES)
    validated = run_validate(quotes_settings, Site.QUOTES)
    staged = validated.paths.valid_rows("authors")
    original = staged.read_text()

    staged.write_text(original.replace('"Albert Einstein"', '""', 1))
    with pytest.raises(StageInputError, match="breaks the contract"):
        run_export(quotes_settings, Site.QUOTES)

    staged.write_text(original)
    stats = validated.paths.parse_stats
    data = json.loads(stats.read_text())
    stats.write_text(json.dumps({**data, "parse_run_id": "ffffffffffffffff"}))
    with pytest.raises(StageInputError, match="re-run validate"):
        run_export(quotes_settings, Site.QUOTES)


@pytest.mark.slow
def test_export_counts_never_attempted_stopped_pages_under_the_stop_class(
    quotes_settings: Settings,
) -> None:

    url = "https://quotes.toscrape.com/page/9/"
    with StateStore.open(quotes_settings.state_db) as store:
        store.start_run("manual", Site.QUOTES, Stage.FETCH, "2026-11-24T00:00:00+00:00", "{}")
        entry = FrontierEntry(url, Site.QUOTES, PageKind.LISTING, 1)
        store.add_entries([entry], "manual")
        store.record_domain_event(
            DomainEvent(
                "manual",
                Site.QUOTES,
                "quotes.toscrape.com",
                DomainEventKind.DOMAIN_STOPPED,
                "challenge page",
                1.0,
                ResponseClass.CHALLENGE,
            )
        )
        store.record_result("manual", entry, mark_stopped(entry, "domain stopped"), None)
        assert store.counts(Site.QUOTES)[FrontierStatus.STOPPED] == 1
    run_parse(quotes_settings, Site.QUOTES)
    run_validate(quotes_settings, Site.QUOTES)
    report = run_export(quotes_settings, Site.QUOTES).report
    assert report.pages.failed_by_class[ResponseClass.CHALLENGE] == 1
    assert [(s.domain, s.response_class) for s in report.domains_stopped] == [
        ("quotes.toscrape.com", ResponseClass.CHALLENGE)
    ]


def test_parse_validate_export_twice_gives_the_same_bytes(quotes_settings: Settings) -> None:
    def once() -> dict[str, bytes]:
        run_parse(quotes_settings, Site.QUOTES)
        run_validate(quotes_settings, Site.QUOTES)
        run_export(quotes_settings, Site.QUOTES)
        return _files(SitePaths.of(quotes_settings, Site.QUOTES))

    first = once()
    assert len(first) == 12  # 3 parsed, 4 validated, 5 deliverables
    assert once() == first


class _Floaty(BaseModel):
    ratio: float


class _Mixed(BaseModel):
    value: int | str


class _Unscaled(BaseModel):
    price: Decimal


class _Counts(BaseModel):
    counts: tuple[int, ...]


class _Whole(BaseModel):
    pence: Annotated[Decimal, Field(decimal_places=0)]


@pytest.mark.parametrize(
    ("model", "message"),
    [
        (_Floaty, "no Arrow mapping"),
        (_Mixed, "unsupported union"),
        (_Unscaled, "needs decimal_places"),
        (_Counts, "no Arrow mapping"),
    ],
)
def test_arrow_schema_refuses_types_it_would_have_to_guess(
    model: type[BaseModel], message: str
) -> None:
    with pytest.raises(TypeError, match=message):
        arrow_schema(model)


def test_arrow_schema_keeps_a_zero_decimal_scale() -> None:
    assert arrow_schema(_Whole).field("pence").type == pa.decimal128(18, 0)


def test_page_counts_leave_out_robots_skips_and_unexplained_stops(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)
    skipped = FrontierEntry("https://quotes.toscrape.com/a", Site.QUOTES, PageKind.LISTING, 0)
    stopped = FrontierEntry("https://quotes.toscrape.com/b", Site.QUOTES, PageKind.LISTING, 0)
    with StateStore.open(settings.state_db) as store:
        store.start_run("r", Site.QUOTES, Stage.FETCH, "2026-11-24T00:00:00+00:00", "{}")
        store.add_entries([skipped, stopped], "r")
        store.record_result("r", skipped, mark_skipped_robots(skipped, "robots"), None)
        store.record_result("r", stopped, mark_stopped(stopped, "stopped"), None)
        counts = _page_counts(store, Site.QUOTES)
    assert (counts.discovered, counts.fetched, counts.failed_by_class) == (2, 0, {})
