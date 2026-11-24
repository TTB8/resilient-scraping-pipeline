"""Properties of the run-state store: discovery is idempotent whatever order batches arrive in."""

import itertools
import tempfile
from dataclasses import replace
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from scraping_pipeline.core.frontier import FrontierEntry
from scraping_pipeline.core.types import FrontierStatus, PageKind, Site
from scraping_pipeline.storage.state import Stage, StateStore

_segment = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=1, max_size=6)
_urls = st.lists(_segment, min_size=1, max_size=3).map(
    lambda parts: "https://books.toscrape.com/" + "/".join(parts)
)
_entries = st.builds(
    FrontierEntry,
    url=_urls,
    site=st.just(Site.BOOKS),
    kind=st.sampled_from(PageKind),
    depth=st.integers(0, 5),
)
# One consistent entry per URL, as plan_new_links would produce.
_frontiers = st.dictionaries(_urls, _entries, max_size=15).map(
    lambda by_url: {url: replace(entry, url=url) for url, entry in by_url.items()}
)


def _batches(data: st.DataObject, frontier: dict[str, FrontierEntry]) -> list[list[FrontierEntry]]:
    """Split the frontier into batches, with repeats, in a drawn order."""
    entries = list(frontier.values())
    repeated = (
        entries + data.draw(st.lists(st.sampled_from(entries), max_size=10)) if entries else []
    )
    shuffled = data.draw(st.permutations(repeated))
    cuts = sorted(data.draw(st.lists(st.integers(0, len(shuffled)), max_size=4)))
    bounds = [0, *cuts, len(shuffled)]
    return [shuffled[a:b] for a, b in itertools.pairwise(bounds)]


# Each example opens two databases (~0.15s each): 20 examples keep the fast checks in budget.
@settings(max_examples=20, deadline=None)
@given(data=st.data(), frontier=_frontiers, done_count=st.integers(0, 3))
def test_any_interleaving_of_add_batches_yields_the_same_frontier(
    data: st.DataObject, frontier: dict[str, FrontierEntry], done_count: int
) -> None:
    # Some URLs were already fetched by an earlier run: re-discovery must never reset them.
    already_done = {
        url: replace(entry, status=FrontierStatus.DONE, attempts=1, last_reason="ok")
        for url, entry in sorted(frontier.items())[:done_count]
    }
    expected = {**frontier, **already_done}
    results = []
    for _ in range(2):
        with tempfile.TemporaryDirectory() as tmp, StateStore.open(Path(tmp) / "s.duckdb") as store:
            store.start_run("r", Site.BOOKS, Stage.DISCOVER, "2026-11-24T10:00:00+00:00", "{}")
            store.add_entries(already_done.values(), "r")
            inserted = sum(store.add_entries(batch, "r") for batch in _batches(data, frontier))
            assert inserted == len(frontier) - len(already_done)
            stored = store.entries(Site.BOOKS)
            assert {entry.url: entry for entry in stored} == expected
            results.append(stored)
    assert results[0] == results[1]
