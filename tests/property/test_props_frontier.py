"""plan_new_links invariants for arbitrary link batches."""

import random

from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.frontier import plan_new_links
from scraping_pipeline.core.types import Link, PageKind, Site
from scraping_pipeline.core.urls import canonicalize, same_site

ORIGIN = "https://books.toscrape.com/"

_hosts = st.sampled_from(
    ["books.toscrape.com", "BOOKS.toscrape.com", "books.toscrape.com:443", "quotes.toscrape.com"]
)
_schemes = st.sampled_from(["https", "HTTPS", "http"])
_paths = st.lists(st.sampled_from(["a", "b", "c", "..", ".", "catalogue", "x.html"]), max_size=4)
_suffix = st.sampled_from(["", "/", "#frag", "?b=1&a=2", "?a=2&b=1"])

links = st.builds(
    lambda scheme, host, parts, suffix, kind: Link(
        f"{scheme}://{host}/{'/'.join(parts)}{suffix}", kind
    ),
    _schemes,
    _hosts,
    _paths,
    _suffix,
    st.sampled_from(list(PageKind)),
)


@given(
    batch=st.lists(links, max_size=40),
    known=st.lists(links, max_size=10),
    depth=st.integers(min_value=0, max_value=4),
    max_depth=st.integers(min_value=0, max_value=4),
    max_pages=st.integers(min_value=0, max_value=30),
    current=st.integers(min_value=0, max_value=30),
    order=st.randoms(use_true_random=False),
)
def test_plan_invariants(
    *,
    batch: list[Link],
    known: list[Link],
    depth: int,
    max_depth: int,
    max_pages: int,
    current: int,
    order: random.Random,
) -> None:
    known_urls = frozenset(canonicalize(k.url) for k in known)

    def run(items: list[Link]) -> list[tuple[str, PageKind, int]]:
        result = plan_new_links(
            known_urls,
            items,
            origin=ORIGIN,
            site=Site.BOOKS,
            depth=depth,
            max_depth=max_depth,
            max_pages=max_pages,
            current_count=current,
        )
        return [(e.url, e.kind, e.depth) for e in result]

    planned = run(batch)
    urls = [u for u, _, _ in planned]
    assert len(urls) == len(set(urls))  # deduplicated
    assert not set(urls) & known_urls  # nothing already known
    assert all(same_site(u, ORIGIN) and canonicalize(u) == u for u in urls)
    assert len(urls) <= max(0, max_pages - current)  # page limit
    assert all(d == depth + 1 <= max_depth for _, _, d in planned)  # depth limit

    shuffled = list(batch)
    order.shuffle(shuffled)
    assert run(shuffled) == planned  # input order never matters (not even for the page cap)
