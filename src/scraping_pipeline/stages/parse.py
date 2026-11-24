"""Stage 3, parse: turn cached pages into draft records with lineage. Offline, deterministic.

Why parse is a stage of its own, and offline
--------------------------------------------

A parser is the part of a scraper that changes most often (a selector breaks, a field is added),
and re-crawling a site to apply a parser fix is slow, impolite and not even reproducible: the site
may have changed since. So parse reads **only the page cache**, plus the frontier for each URL's
page kind, and never a fetcher or the network. The same cache always yields the same bytes, and a
parser change is applied by re-running this stage alone.

What makes the output byte-identical
------------------------------------

* ``parse_run_id`` is **derived**, not random: ``core.lineage.derive_parse_run_id`` over the
  manifest of *this site's* cache entries, the parser version and the contract version. The
  manifest hashes ``(url, content_sha256, fetch_run_id)`` per page
  (``PageCache.manifest_sha256(urls, with_fetch_run_ids=True)``): the ``fetch_run_id`` is part of
  every row's lineage, so it is part of what determines the output bytes. Same pages from the same
  fetches + same code = same id and same bytes; any change to any of them = a new id.
* Pages are read in URL order (the frontier's order), drafts in document order, so the row order
  is ``(source_url, position on the page)``. No set or dict order reaches a file.
* Files are canonical JSON (``stages.common``), written atomically. Nothing here reads a clock;
  run timestamps live in the state database, never in these files.

Outputs, under ``<data_dir>/<site>/parsed/``:

* ``records.jsonl``: one ``{"record_type", "draft", "lineage"}`` per draft, decimals and dates
  tagged so validation sees exactly the types the parser produced;
* ``context.json``: what dataset-level validation needs from the seed pages (the books category set);
* ``parse_stats.json``: ids, counts, coverage inputs and every parser warning with its URL.

A page whose cache object is corrupt is skipped and listed under ``unreadable`` in the stats (the
manifest still names it, so the run id stays honest about which cache it read).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scraping_pipeline.config import Settings
from scraping_pipeline.core.lineage import Lineage, derive_parse_run_id
from scraping_pipeline.core.types import Site
from scraping_pipeline.sites import get_site
from scraping_pipeline.stages.common import (
    SitePaths,
    encode_draft,
    encode_lineage,
    read_cached_html,
    record_type_of,
    stage_run,
    write_json,
    write_jsonl,
)
from scraping_pipeline.storage.cache import PageCache
from scraping_pipeline.storage.state import Stage, StateStore


@dataclass(frozen=True, slots=True)
class ParseStageResult:
    run_id: str
    parse_run_id: str
    manifest_sha256: str
    pages_parsed: int
    records: int
    warnings: int
    item_pages_fetched: int
    item_pages_with_records: int
    paths: SitePaths


def run_parse(settings: Settings, site: Site) -> ParseStageResult:
    """Parse every cached page of ``site`` into ``<data_dir>/<site>/parsed/``. No network."""
    spec = get_site(site)
    cache = PageCache(settings.cache_dir)
    paths = SitePaths.of(settings, site)
    with (
        StateStore.open(settings.state_db) as store,
        stage_run(store, settings, site, Stage.PARSE) as run,
    ):
        frontier = store.entries(site)  # ordered by URL
        manifest = cache.manifest_sha256((e.url for e in frontier), with_fetch_run_ids=True)
        parse_run_id = derive_parse_run_id(manifest, spec.parser_version, spec.contract_version)

        rows: list[dict[str, Any]] = []
        warnings: list[dict[str, str]] = []
        unreadable: list[str] = []
        records_by_type: dict[str, int] = {}
        pages = item_pages = item_pages_with_records = 0
        for entry in frontier:
            if not cache.has(entry.url):
                continue
            cached = read_cached_html(cache, entry.url)
            if cached is None:
                unreadable.append(entry.url)
                continue
            cache_entry, html = cached
            pages += 1
            result = spec.parse(entry.url, html, entry.kind)
            lineage = Lineage(
                source_url=entry.url,
                content_sha256=cache_entry.content_sha256,
                fetch_run_id=cache_entry.fetch_run_id,
                parse_run_id=parse_run_id,
                parser_version=spec.parser_version,
                contract_version=spec.contract_version,
            )
            for draft in result.drafts:
                record_type = record_type_of(draft)
                records_by_type[record_type] = records_by_type.get(record_type, 0) + 1
                rows.append(
                    {
                        "record_type": record_type,
                        "draft": encode_draft(draft),
                        "lineage": encode_lineage(lineage),
                    }
                )
            warnings += [{"url": entry.url, "message": message} for message in result.warnings]
            if entry.kind in spec.item_kinds:
                item_pages += 1
                item_pages_with_records += bool(result.drafts)

        seed_html = []
        for url in spec.seed_urls(settings):
            seed = read_cached_html(cache, url)
            seed_html.append(seed[1] if seed is not None else None)
        context = spec.build_context(seed_html)

        stats = {
            "site": site.value,
            "parse_run_id": parse_run_id,
            "manifest_sha256": manifest,
            "parser_version": spec.parser_version,
            "contract_version": spec.contract_version,
            "pages_parsed": pages,
            "item_pages_fetched": item_pages,
            "item_pages_with_records": item_pages_with_records,
            "records_parsed": len(rows),
            "records_by_type": dict(sorted(records_by_type.items())),
            "warnings_count": len(warnings),
            "warnings": warnings,
            "unreadable": unreadable,
        }
        write_jsonl(paths.records, rows)
        write_json(paths.context, context)
        write_json(paths.parse_stats, stats)
        run.summary = {
            "parse_run_id": parse_run_id,
            "pages_parsed": pages,
            "records": len(rows),
            "warnings": len(warnings),
            "unreadable": len(unreadable),
        }
    return ParseStageResult(
        run_id=run.run_id,
        parse_run_id=parse_run_id,
        manifest_sha256=manifest,
        pages_parsed=pages,
        records=len(rows),
        warnings=len(warnings),
        item_pages_fetched=item_pages,
        item_pages_with_records=item_pages_with_records,
        paths=paths,
    )
