"""Stage 1, discover: put the seeds in the frontier and plan links from pages already cached. Offline.

Why discover never touches the network: its job is to say *what* to fetch, not to fetch it. Seeds
come from settings, and every further link comes from a page the cache already holds, parsed with
the site's own parser. So discover can run any number of times, before or after any fetch, and
the frontier only ever grows by what the cache justifies (``add_entries`` is insert-only: a page
that is ``done`` or ``failed`` stays so).

On a first run that means just the seeds; ``fetch`` then expands links as pages land. On a later
run (say, after ``--max-pages`` was raised, or the state file was lost but the cache kept) it
re-plans everything the cache allows without a single request. Depth and page caps
(``--max-depth``, ``--max-pages``) are applied by ``core.frontier.plan_new_links``.
"""

from __future__ import annotations

from dataclasses import dataclass

from scraping_pipeline.config import Settings
from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import PageKind, Site
from scraping_pipeline.sites import get_site
from scraping_pipeline.stages.common import expand_from_cache, stage_run
from scraping_pipeline.storage.cache import PageCache
from scraping_pipeline.storage.state import Stage, StateStore


@dataclass(frozen=True, slots=True)
class DiscoverResult:
    run_id: str
    seeds_added: int
    links_added: int
    frontier_size: int


def run_discover(settings: Settings, site: Site) -> DiscoverResult:
    """Add the site's seeds (idempotently) and every link the cached pages justify."""
    spec = get_site(site)
    cache = PageCache(settings.cache_dir)
    with (
        StateStore.open(settings.state_db) as store,
        stage_run(store, settings, site, Stage.DISCOVER) as run,
    ):
        seeds = [
            FrontierEntry(url=url, site=spec.site, kind=PageKind.LISTING, depth=0)
            for url in spec.seed_urls(settings)
        ]
        seeds_added = store.add_entries(seeds, run.run_id)
        links = expand_from_cache(store, cache, spec, settings, run.run_id)
        size = len(store.entries(site))
        run.summary = {
            "seeds_added": seeds_added,
            "links_added": len(links),
            "frontier_size": size,
        }
    return DiscoverResult(run.run_id, seeds_added, len(links), size)
