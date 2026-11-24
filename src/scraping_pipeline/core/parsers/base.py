"""What a parser returns, and the small typed HTML helpers every parser shares.

Parsers are *lenient*: they extract what they find and never raise on odd HTML. A field the page does
not have is ``None``; a field the page has but the parser could not interpret keeps the raw text (a
``str`` where a ``Decimal``, ``int`` or ``date`` was expected), and either case adds a warning. The
strict versioned contract (``core.contracts``) then rejects such a draft with a precise failure that
shows the offending value, and the row goes to quarantine instead of disappearing. Splitting "extract"
from "judge" is what lets the pipeline report *why* a row is bad rather than silently dropping it.

Every container here is an immutable tuple in document order, so the same input bytes always produce
an equal ``ParseResult``: no set or dict iteration order can leak into the output.

selectolax ships its own type stubs (``py.typed`` + ``lexbor.pyi``), so no wrapper is needed for mypy
``--strict``; the helpers below exist to keep the "text or None" and "resolve or warn" rules in one
place.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from selectolax.lexbor import LexborHTMLParser, LexborNode

from scraping_pipeline.core.text import normalize_text
from scraping_pipeline.core.types import Link, PageKind
from scraping_pipeline.core.urls import canonicalize, resolve


@dataclass(frozen=True, slots=True)
class BookDraft:
    """One book as found on a books.toscrape.com product page, before the contract judges it."""

    upc: str | None
    title: str | None
    price_gbp: Decimal | str | None
    stock_count: int | str | None
    in_stock: bool | None
    rating: int | str | None
    category: str | None
    description: str
    """``""`` when the page has no description section, which is a legitimate state on the real site."""
    product_url: str
    image_url: str | None


@dataclass(frozen=True, slots=True)
class QuoteDraft:
    """One quote from a quotes listing page (static, or the browser-rendered JavaScript variant)."""

    quote_id: str | None
    """sha256 of the normalized text and author (``core.text.quote_id``); ``None`` if either is missing."""
    text: str | None
    author_name: str | None
    author_slug: str | None
    """From the "(about)" link; ``None`` when the page has none (the JavaScript variant renders none)."""
    tags: tuple[str, ...]
    page_url: str
    position: int
    """0-based index of the quote on its page: a stable key for a row that has no ``quote_id``."""


@dataclass(frozen=True, slots=True)
class AuthorDraft:
    """One author page of the quotes site."""

    author_slug: str | None
    name: str | None
    born_date: date | str | None
    born_location: str | None
    bio: str | None


type Draft = BookDraft | QuoteDraft | AuthorDraft


@dataclass(frozen=True, slots=True)
class ParseResult[D: Draft]:
    """The output of parsing one page: records, outgoing links and warnings, all in document order."""

    drafts: tuple[D, ...]
    links: tuple[Link, ...]
    """Deduplicated by URL (first occurrence wins), canonicalized with ``core.urls.resolve``."""
    warnings: tuple[str, ...]


class PageContext:
    """Collects links and warnings while one page is parsed, then freezes them into a result.

    It owns the "never raise" rules for URLs: a link that does not resolve to an absolute http(s) URL
    becomes a warning, not an exception.
    """

    def __init__(self, url: str, html: str) -> None:
        self.url = url
        # Lone surrogates (possible in a Python str, impossible in real UTF-8) would make the C parser
        # raise. Replace them so garbled input degrades to warnings, never to an exception.
        clean = html.encode("utf-8", "replace").decode("utf-8")
        self.tree = LexborHTMLParser(clean)
        self._links: dict[str, Link] = {}
        self._warnings: list[str] = []

    def warn(self, message: str) -> None:
        self._warnings.append(message)

    def resolve(self, href: str | None, what: str) -> str | None:
        """Resolve ``href`` against the page URL, or warn and return ``None``."""
        if href is None or not href.strip():
            self.warn(f"{what}: link without href")
            return None
        try:
            return resolve(self.url, href)
        except ValueError:
            self.warn(f"{what}: unresolvable href {href!r}")
            return None

    def add_link(self, href: str | None, kind: PageKind, what: str) -> str | None:
        resolved = self.resolve(href, what)
        if resolved is not None and resolved not in self._links:
            self._links[resolved] = Link(url=resolved, kind=kind)
        return resolved

    def page_url(self) -> str:
        """The canonical page URL, or the raw one (with a warning) if it is not canonicalizable."""
        try:
            return canonicalize(self.url)
        except ValueError:
            self.warn(f"page: not an absolute http(s) URL {self.url!r}")
            return self.url

    def result[D: Draft](self, drafts: tuple[D, ...]) -> ParseResult[D]:
        return ParseResult(
            drafts=drafts, links=tuple(self._links.values()), warnings=tuple(self._warnings)
        )


def text_of(node: LexborNode | None) -> str | None:
    """Normalized text of a node; ``None`` if the node is absent or its text is empty."""
    if node is None:
        return None
    value = normalize_text(node.text())
    return value or None


def attr(node: LexborNode | None, name: str) -> str | None:
    if node is None:
        return None
    return node.attributes.get(name)
