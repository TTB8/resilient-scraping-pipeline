"""The site registry: everything that differs between targets, in one table. Edit here to add a site.

The five stages are generic. They never branch on a site's name; they ask its :class:`SiteSpec`.
That is what keeps a new target a small, reviewable change instead of a hunt through ``if site ==``
checks spread over the code.

Adapting the pipeline to a new site, in miniature
-------------------------------------------------

1. **Check you may.** Read the site's terms and robots.txt. The pipeline honours robots.txt and
   stops on blocks, but whether to crawl at all is a human decision. No evasion, ever (ADR 0005).
2. **Add a** :class:`~scraping_pipeline.core.types.Site` **member** and its base URL in
   :class:`~scraping_pipeline.config.Settings` (overridable, so tests can point it at the local
   fixture server). The state store's CHECK constraints list sites, so that needs a migration too.
3. **Write a parser** in ``core/parsers/<site>.py``: ``parse(url, html, kind) -> ParseResult`` plus a
   ``PARSER_VERSION``. Lenient: extract what is there, warn about the rest, never raise. Pure: no
   I/O, no clock, no locale. Test it against saved pages in ``tests/fixtures/sites/<site>/``.
4. **Write a contract** in ``core/contracts/<site>_v1.py`` with a ``CONTRACT_VERSION``, and teach
   ``core.validate`` the dataset-level rules (uniqueness key, referential checks).
5. **Register a** :class:`SiteSpec` **below**: seed paths, the fetcher (``httpx`` unless the content
   only exists after JavaScript runs), which page kinds carry links and which carry records, how
   to build the validation dataset, and the output tables with their sort keys.
6. **Run it** against the fixture server first (``tests/integration/``), then for real with
   ``--max-pages`` small.

Why the sort key lives here: the deliverables are sorted by it (byte-identical outputs need a
total order), and only someone who knows the site knows which column is the stable unique id.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urljoin

from pydantic import BaseModel

from scraping_pipeline.config import Settings
from scraping_pipeline.core.contracts import Author, Book, Quote, books_v1, quotes_v1
from scraping_pipeline.core.parsers import books as books_parser
from scraping_pipeline.core.parsers import quotes as quotes_parser
from scraping_pipeline.core.parsers.base import AuthorDraft, BookDraft, ParseResult, QuoteDraft
from scraping_pipeline.core.types import PageKind, Site
from scraping_pipeline.core.urls import canonicalize
from scraping_pipeline.core.validate import BooksDataset, Candidate, Dataset, QuotesDataset
from scraping_pipeline.fetch.httpx_fetcher import HttpxFetcher
from scraping_pipeline.fetch.playwright_fetcher import PlaywrightFetcher
from scraping_pipeline.fetch.protocol import Fetcher

__all__ = ["SITES", "FetcherKind", "SiteSpec", "TableSpec", "get_site"]

FetcherKind = Literal["httpx", "playwright"]
ParseFn = Callable[[str, str, PageKind], ParseResult[Any]]


@dataclass(frozen=True, slots=True)
class TableSpec:
    """One deliverable table: its file name, the contract model of its rows, and its sort key.

    ``record_type`` is the name ``core.validate`` gives the rows (``book``, ``quote``, ``author``).
    ``sort_key`` must be unique among valid rows; validation quarantines duplicates, so the
    contract's uniqueness key is the natural choice.
    """

    name: str
    record_type: str
    model: type[BaseModel]
    sort_key: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SiteSpec:
    """Everything the stages need to know about one site. See the module docstring."""

    site: Site
    seed_paths: tuple[str, ...]
    """Joined onto ``Settings.base_url(site)``; each becomes a depth-0 ``LISTING`` entry."""
    fetcher: FetcherKind
    wait_for_selector: str | None
    """Playwright only: the selector that proves the page rendered what we came for."""
    parse: ParseFn
    parser_version: str
    contract_version: str
    link_kinds: frozenset[PageKind]
    """Page kinds whose links are followed (parsed during fetch, and offline by discover)."""
    item_kinds: frozenset[PageKind]
    """Page kinds expected to hold records: the denominator of parse coverage."""
    tables: tuple[TableSpec, ...]
    build_context: Callable[[Sequence[str | None]], dict[str, Any]]
    """Seed pages' HTML (``None`` when not cached), in seed order -> JSON-ready validation context.

    Computed by parse (which may read the cache) and stored next to the drafts, so validate needs
    nothing but the parse output."""
    build_dataset: Callable[[Sequence[Candidate[Any]], dict[str, Any]], Dataset]

    def origin(self, settings: Settings) -> str:
        """The site's origin: links that leave it are never followed."""
        return canonicalize(settings.base_url(self.site))

    def seed_urls(self, settings: Settings) -> tuple[str, ...]:
        base = settings.base_url(self.site)
        return tuple(canonicalize(urljoin(base, path)) for path in self.seed_paths)

    def make_fetcher(self, settings: Settings) -> Fetcher:
        if self.fetcher == "playwright":
            return PlaywrightFetcher(settings, self.wait_for_selector)
        return HttpxFetcher(settings)

    def table_for(self, record: BaseModel) -> TableSpec:
        for table in self.tables:
            if isinstance(record, table.model):
                return table
        raise ValueError(f"{self.site}: no table for {type(record).__name__}")


# --- books ---------------------------------------------------------------------------------------


def _books_context(seed_html: Sequence[str | None]) -> dict[str, Any]:
    # The home page side nav is the category set every book's category must belong to. Without it
    # the set is empty and every book fails ``unknown_category``: fail closed, and loudly.
    home = seed_html[0] if seed_html else None
    names = books_parser.category_names(home) if home is not None else ()
    return {"categories": sorted(names)}


def _books_dataset(candidates: Sequence[Candidate[Any]], context: dict[str, Any]) -> Dataset:
    books = tuple(c for c in candidates if isinstance(c.draft, BookDraft))
    return BooksDataset(books=books, categories=frozenset(context.get("categories", ())))


# --- quotes --------------------------------------------------------------------------------------


def _no_context(_seed_html: Sequence[str | None]) -> dict[str, Any]:
    return {}


def _quotes_dataset_factory(
    require_author_links: bool,
) -> Callable[[Sequence[Candidate[Any]], dict[str, Any]], Dataset]:
    def build(candidates: Sequence[Candidate[Any]], _context: dict[str, Any]) -> Dataset:
        return QuotesDataset(
            quotes=tuple(c for c in candidates if isinstance(c.draft, QuoteDraft)),
            authors=tuple(c for c in candidates if isinstance(c.draft, AuthorDraft)),
            require_author_links=require_author_links,
        )

    return build


_QUOTES_TABLE = TableSpec("quotes", "quote", Quote, ("quote_id",))
_AUTHORS_TABLE = TableSpec("authors", "author", Author, ("author_slug",))


SITES: dict[Site, SiteSpec] = {
    Site.BOOKS: SiteSpec(
        site=Site.BOOKS,
        seed_paths=("",),  # the home page: first listing page plus the category side nav
        fetcher="httpx",
        wait_for_selector=None,
        parse=books_parser.parse,
        parser_version=books_parser.PARSER_VERSION,
        contract_version=books_v1.CONTRACT_VERSION,
        link_kinds=frozenset({PageKind.LISTING}),
        item_kinds=frozenset({PageKind.DETAIL}),
        tables=(TableSpec("books", "book", Book, ("book_id",)),),
        build_context=_books_context,
        build_dataset=_books_dataset,
    ),
    Site.QUOTES: SiteSpec(
        site=Site.QUOTES,
        seed_paths=("/",),
        fetcher="httpx",
        wait_for_selector=None,
        parse=quotes_parser.parse,
        parser_version=quotes_parser.PARSER_VERSION,
        contract_version=quotes_v1.CONTRACT_VERSION,
        link_kinds=frozenset({PageKind.LISTING, PageKind.AUTHOR}),
        item_kinds=frozenset({PageKind.LISTING, PageKind.AUTHOR}),
        tables=(_AUTHORS_TABLE, _QUOTES_TABLE),
        build_context=_no_context,
        build_dataset=_quotes_dataset_factory(require_author_links=True),
    ),
    # Same origin and parser as the static quotes site; the quotes only exist after the page's
    # script runs, so a real browser renders them and the parser reads the rendered DOM. The
    # rendered quotes carry no author link, hence no authors table and no author-link rule.
    Site.QUOTES_JS: SiteSpec(
        site=Site.QUOTES_JS,
        seed_paths=("/js/",),
        fetcher="playwright",
        wait_for_selector=".quote",
        parse=quotes_parser.parse,
        parser_version=quotes_parser.PARSER_VERSION,
        contract_version=quotes_v1.CONTRACT_VERSION,
        link_kinds=frozenset({PageKind.LISTING}),
        item_kinds=frozenset({PageKind.LISTING}),
        tables=(_QUOTES_TABLE,),
        build_context=_no_context,
        build_dataset=_quotes_dataset_factory(require_author_links=False),
    ),
}


def get_site(site: Site) -> SiteSpec:
    """The registered spec for ``site``. Every :class:`Site` member has one (a test checks)."""
    return SITES[Site(site)]
