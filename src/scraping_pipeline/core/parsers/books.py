"""Parser for books.toscrape.com: listing and category pages give links, product pages give one book.

Crawl shape. The home page and ``catalogue/page-N.html`` list 20 products each and link the next page.
Category pages (``catalogue/category/books/<name>_<id>/``) paginate the same way. So a listing page
yields its product links (``DETAIL``) and its "next" link (``LISTING``); the home page also yields the
category links of the side navigation (``LISTING``), which is how every category page, and with it the
category set used by the referential check, gets discovered. The side nav repeats on every listing
page; it is read on the home page only, since one copy is enough and the frontier would discard the
rest anyway. The top "Books" entry is the all-books category, a second copy of the main listing, so it
is skipped.

Product pages also show "recently viewed" products. Those are not followed: every product is reachable
from the main listing, and following them would only add duplicates to the frontier.

Every selector on a product page is scoped to ``.product_main`` or a page-unique id, because the
"recently viewed" cards repeat the same ``price_color`` and ``star-rating`` classes.

Price text is ``£51.77``. Some captures of this site decode the pound sign as ``Â£`` (UTF-8 bytes read
as Latin-1); that mojibake prefix is accepted. A price in any other currency is kept raw so the
contract rejects it rather than the parser guessing an exchange.
"""

import re
from decimal import Decimal
from urllib.parse import urlsplit

from scraping_pipeline.core.parsers.base import BookDraft, PageContext, ParseResult, attr, text_of
from scraping_pipeline.core.text import normalize_text
from scraping_pipeline.core.types import PageKind

PARSER_VERSION = "books-parser/1.0.0"

RATINGS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
"""The rating is encoded as a class word: ``<p class="star-rating Three">``."""

_PRICE = re.compile(r"Â?£\s?(\d+(?:\.\d+)?)")
_IN_STOCK_COUNT = re.compile(r"In stock \((\d+) available\)")
_HOME_PATHS = frozenset({"", "/", "/index.html"})
_CATEGORY_LINKS = "div.side_categories ul.nav-list > li > ul > li > a"


def parse(url: str, html: str, kind: PageKind) -> ParseResult[BookDraft]:
    """Parse one cached books.toscrape.com page. Never raises on odd HTML; see the module docstring."""
    ctx = PageContext(url, html)
    if kind is PageKind.LISTING:
        _parse_listing(ctx)
        return ctx.result(())
    if kind is PageKind.DETAIL:
        return ctx.result((_parse_detail(ctx),))
    ctx.warn(f"page: the books site has no {kind.value!r} pages")
    return ctx.result(())


def category_names(html: str) -> tuple[str, ...]:
    """Category names from a listing page's side nav, in document order, without duplicates.

    The validate stage uses this (from the cached home page) as the set every ``book.category`` must
    belong to.
    """
    ctx = PageContext("https://books.toscrape.com/", html)
    names: dict[str, None] = {}
    for node in ctx.tree.css(_CATEGORY_LINKS):
        name = text_of(node)
        if name is not None:
            names[name] = None
    return tuple(names)


def parse_price(raw: str) -> Decimal | str:
    """``"£51.77"`` (or the mojibake ``"Â£51.77"``) to ``Decimal("51.77")``; anything else stays raw."""
    match = _PRICE.fullmatch(normalize_text(raw))
    return Decimal(match.group(1)) if match else raw


def parse_availability(raw: str) -> tuple[int | str | None, bool | None]:
    """``"In stock (22 available)"`` to ``(22, True)``; ``"Out of stock"`` to ``(0, False)``.

    A bare ``"In stock"`` (the form listing cards use) has no count, so it gives ``(None, True)``.
    Unknown wording is kept raw with ``in_stock=None`` for the contract to reject.
    """
    text = normalize_text(raw)
    match = _IN_STOCK_COUNT.fullmatch(text)
    if match:
        return int(match.group(1)), True
    if text == "Out of stock":
        return 0, False
    if text == "In stock":
        return None, True
    return raw, None


def _parse_listing(ctx: PageContext) -> None:
    for anchor in ctx.tree.css("article.product_pod h3 a"):
        ctx.add_link(attr(anchor, "href"), PageKind.DETAIL, "product link")
    next_anchor = ctx.tree.css_first("ul.pager li.next a")
    if next_anchor is not None:
        ctx.add_link(attr(next_anchor, "href"), PageKind.LISTING, "next link")
    if _is_home(ctx.url):
        for anchor in ctx.tree.css(_CATEGORY_LINKS):
            ctx.add_link(attr(anchor, "href"), PageKind.LISTING, "category link")


def _is_home(url: str) -> bool:
    try:
        path = urlsplit(url).path
    except ValueError:  # e.g. an unbalanced IPv6 bracket in a garbled URL
        return False
    return path in _HOME_PATHS


def _parse_detail(ctx: PageContext) -> BookDraft:
    tree = ctx.tree
    title = text_of(tree.css_first(".product_main h1"))
    if title is None:
        ctx.warn("title: missing")

    price: Decimal | str | None = None
    price_text = text_of(tree.css_first(".product_main p.price_color"))
    if price_text is None:
        ctx.warn("price_gbp: missing")
    else:
        price = parse_price(price_text)
        if isinstance(price, str):
            ctx.warn(f"price_gbp: unrecognised price {price_text!r}")

    stock_count: int | str | None = None
    in_stock: bool | None = None
    availability = text_of(tree.css_first(".product_main p.availability"))
    if availability is None:
        ctx.warn("stock_count: availability missing")
    else:
        stock_count, in_stock = parse_availability(availability)
        if not isinstance(stock_count, int):
            ctx.warn(f"stock_count: no count in {availability!r}")

    return BookDraft(
        upc=_upc(ctx),
        title=title,
        price_gbp=price,
        stock_count=stock_count,
        in_stock=in_stock,
        rating=_rating(ctx),
        category=_category(ctx),
        description=_description(ctx),
        product_url=ctx.page_url(),
        image_url=_image_url(ctx),
    )


def _upc(ctx: PageContext) -> str | None:
    for row in ctx.tree.css("article.product_page table tr"):
        if text_of(row.css_first("th")) == "UPC":
            upc = text_of(row.css_first("td"))
            if upc is not None:
                return upc
    ctx.warn("upc: missing from the product table")
    return None


def _rating(ctx: PageContext) -> int | str | None:
    node = ctx.tree.css_first(".product_main p.star-rating")
    if node is None:
        ctx.warn("rating: missing")
        return None
    words = [word for word in (attr(node, "class") or "").split() if word != "star-rating"]
    for word in words:
        if word in RATINGS:
            return RATINGS[word]
    raw = " ".join(words)
    ctx.warn(f"rating: unknown rating class {raw!r}")
    return raw


def _category(ctx: PageContext) -> str | None:
    # Breadcrumb: Home > Books > <Category> > <Title (active, not a link)>.
    anchors = ctx.tree.css("ul.breadcrumb li a")
    if len(anchors) < 3:  # noqa: PLR2004 - the three links of the breadcrumb above
        ctx.warn("category: breadcrumb has no category link")
        return None
    category = text_of(anchors[-1])
    if category is None:
        ctx.warn("category: empty breadcrumb link")
    return category


def _description(ctx: PageContext) -> str:
    header = ctx.tree.css_first("#product_description")
    if header is None:
        return ""  # some real product pages have no description; that is not an error
    paragraph = ctx.tree.css_first("#product_description + p")
    text = text_of(paragraph)
    if text is None:
        ctx.warn("description: header present but no text")
        return ""
    return text


def _image_url(ctx: PageContext) -> str | None:
    img = ctx.tree.css_first("#product_gallery img")
    if img is None:
        ctx.warn("image_url: missing")
        return None
    return ctx.resolve(attr(img, "src"), "image_url")
