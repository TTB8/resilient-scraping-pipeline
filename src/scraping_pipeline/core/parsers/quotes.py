"""Parser for quotes.toscrape.com: listing pages give quotes and links, author pages give one author.

Crawl shape. ``/``, ``/page/N/`` list ten quotes each and link the next page; page 10 is the last. A
listing yields its quotes, one ``AUTHOR`` link per distinct "(about)" link, and the "next" link.

Tag pages (``/tag/<tag>/page/N/``) are **not followed**. They paginate the same way, but every quote
they show is also on the main listing, so following them would only fetch duplicates and load the site
for nothing. A tag page passed in as ``LISTING`` still parses correctly (same markup).

The JavaScript variant (``/js/``) builds its quotes in the browser from an inline ``var data = [...]``.
That script is deliberately **not** parsed: the point of the variant is to exercise a real browser, so
the fetch stage renders the page and this parser reads the *rendered DOM* with the same selectors as
the static site. The rendering template writes ``<div class='quote'><span class='text'>``, ``<small
class='author'>`` and ``<a class='tag'>`` but no "(about)" link, so rendered quotes come out with
``author_slug=None`` and the variant discovers no author pages. Its ``quote_id`` is still identical to
the static site's, because the id hashes text and author name only.

Dates on author pages ("March 14, 1879") are parsed with a fixed English month table, not
``strptime("%B")``, which depends on the process locale and would break determinism.
"""

import re
from datetime import date
from urllib.parse import unquote, urlsplit

from scraping_pipeline.core.parsers.base import (
    AuthorDraft,
    PageContext,
    ParseResult,
    QuoteDraft,
    attr,
    text_of,
)
from scraping_pipeline.core.text import quote_id
from scraping_pipeline.core.types import PageKind

PARSER_VERSION = "quotes-parser/1.0.0"

MONTHS = {
    name: number
    for number, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}

_BORN_DATE = re.compile(r"([A-Z][a-z]+) (\d{1,2}), (\d{4})")
_AUTHOR_PATH = re.compile(r"/author/([^/]+)/?")
_LEADING_IN = re.compile(r"^in(?: |$)")
_OPEN_QUOTE = "“"
_CLOSE_QUOTE = "”"

type QuotesDraft = QuoteDraft | AuthorDraft


def parse(url: str, html: str, kind: PageKind) -> ParseResult[QuotesDraft]:
    """Parse one cached (or browser-rendered) quotes page. Never raises on odd HTML."""
    ctx = PageContext(url, html)
    if kind is PageKind.LISTING:
        return ctx.result(_parse_listing(ctx))
    if kind is PageKind.AUTHOR:
        return ctx.result((_parse_author(ctx),))
    ctx.warn(f"page: the quotes site has no {kind.value!r} pages")
    return ctx.result(())


def author_slug_from_url(url: str) -> str | None:
    """``https://quotes.toscrape.com/author/Albert-Einstein`` to ``"Albert-Einstein"``."""
    try:
        path = urlsplit(url).path
    except ValueError:
        return None
    match = _AUTHOR_PATH.fullmatch(path)
    return unquote(match.group(1)) if match else None


def parse_born_date(raw: str) -> date | str:
    """``"March 14, 1879"`` to ``date(1879, 3, 14)``; anything unparseable stays raw."""
    match = _BORN_DATE.fullmatch(raw)
    if match is None or match.group(1) not in MONTHS:
        return raw
    try:
        return date(int(match.group(3)), MONTHS[match.group(1)], int(match.group(2)))
    except ValueError:  # e.g. "February 30, 1900"
        return raw


def strip_quote_marks(text: str) -> str:
    """Remove the curly quotes the site wraps every quote in (one at each end, only if present)."""
    text = text.removeprefix(_OPEN_QUOTE).removesuffix(_CLOSE_QUOTE)
    return text.strip()


def _parse_listing(ctx: PageContext) -> tuple[QuoteDraft, ...]:
    page_url = ctx.page_url()
    drafts: list[QuoteDraft] = []
    for position, node in enumerate(ctx.tree.css("div.quote")):
        where = f"quote[{position}]"
        raw_text = text_of(node.css_first("span.text"))
        text = strip_quote_marks(raw_text) if raw_text is not None else None
        if not text:
            text = None
            ctx.warn(f"{where}.text: missing")
        author_name = text_of(node.css_first("small.author"))
        if author_name is None:
            ctx.warn(f"{where}.author_name: missing")

        author_slug = None
        about = node.css_first('a[href*="/author/"]')
        if about is not None:
            author_url = ctx.add_link(attr(about, "href"), PageKind.AUTHOR, f"{where}.author link")
            if author_url is not None:
                author_slug = author_slug_from_url(author_url)

        tags = {tag for tag in (text_of(a) for a in node.css("div.tags a.tag")) if tag is not None}
        drafts.append(
            QuoteDraft(
                quote_id=quote_id(text, author_name) if text and author_name else None,
                text=text,
                author_name=author_name,
                author_slug=author_slug,
                tags=tuple(sorted(tags)),
                page_url=page_url,
                position=position,
            )
        )
    if not drafts:
        ctx.warn("page: no quotes found")
    next_anchor = ctx.tree.css_first("ul.pager li.next a")
    if next_anchor is not None:
        ctx.add_link(attr(next_anchor, "href"), PageKind.LISTING, "next link")
    return tuple(drafts)


def _parse_author(ctx: PageContext) -> AuthorDraft:
    tree = ctx.tree
    slug = author_slug_from_url(ctx.page_url())
    if slug is None:
        ctx.warn(f"author_slug: not an author URL {ctx.url!r}")
    name = text_of(tree.css_first("h3.author-title"))
    if name is None:
        ctx.warn("name: missing")

    born_date: date | str | None = None
    born_text = text_of(tree.css_first("span.author-born-date"))
    if born_text is None:
        ctx.warn("born_date: missing")
    else:
        born_date = parse_born_date(born_text)
        if isinstance(born_date, str):
            ctx.warn(f"born_date: unrecognised date {born_text!r}")

    location = text_of(tree.css_first("span.author-born-location"))
    if location is None:
        ctx.warn("born_location: missing")
    else:
        location = _LEADING_IN.sub("", location) or None
        if location is None:
            ctx.warn("born_location: empty after removing 'in'")

    bio = text_of(tree.css_first("div.author-description"))
    if bio is None:
        ctx.warn("bio: missing")
    return AuthorDraft(
        author_slug=slug, name=name, born_date=born_date, born_location=location, bio=bio
    )
