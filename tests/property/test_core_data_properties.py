"""Properties of the data core: parsers never raise, round-trip, are deterministic; validation loses nothing."""

import hashlib
import html as html_lib
from datetime import date
from decimal import Decimal
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from scraping_pipeline.core.contracts import Book, books_v1, quotes_v1
from scraping_pipeline.core.lineage import Lineage, derive_parse_run_id
from scraping_pipeline.core.parsers import books, quotes
from scraping_pipeline.core.parsers.base import AuthorDraft, BookDraft, QuoteDraft
from scraping_pipeline.core.text import normalize_text, quote_id
from scraping_pipeline.core.types import PageKind
from scraping_pipeline.core.validate import BooksDataset, Candidate, QuotesDataset, validate

SITES = Path(__file__).parents[1] / "fixtures" / "sites"
PARSERS = (books.parse, quotes.parse)

_TAGS = ["div", "p", "a", "span", "ul", "li", "h1", "h3", "table", "tr", "th", "td", "img", "small"]
_CLASSES = [
    "product_main",
    "price_color",
    "instock availability",
    "star-rating Three",
    "star-rating",
    "breadcrumb",
    "quote",
    "text",
    "author",
    "tags",
    "tag",
    "pager",
    "next",
    "product_pod",
    "side_categories",
    "author-born-date",
    "author-born-location",
    "author-description",
    "author-title",
]


@st.composite
def garbled_html(draw: st.DrawFn) -> str:
    """Fragments of the sites' own vocabulary, shuffled and unbalanced: worse than any real page."""
    parts = []
    for _ in range(draw(st.integers(0, 25))):
        choice = draw(st.integers(0, 4))
        if choice == 0:
            tag, cls = draw(st.sampled_from(_TAGS)), draw(st.sampled_from(_CLASSES))
            href = draw(
                st.sampled_from(
                    [
                        "",
                        ' href="x"',
                        ' href="/author/A"',
                        ' href="javascript:x"',
                        " href",
                        ' src="//"',
                        ' href="http://[x"',
                    ]
                )
            )
            parts.append(f'<{tag} class="{cls}" id="product_description"{href}>')
        elif choice == 1:
            parts.append(f"</{draw(st.sampled_from(_TAGS))}>")
        elif choice == 2:
            parts.append(
                draw(
                    st.sampled_from(
                        [
                            "£",
                            "Â£12.5",
                            "In stock (3 available)",
                            "“",
                            "March 14, 1879",
                            "UPC",
                            "in ",
                        ]
                    )
                )
            )
        else:
            parts.append(draw(st.text(max_size=20)))
    return "".join(parts)


_urls = st.sampled_from(
    [
        "https://books.toscrape.com/",
        "https://books.toscrape.com/catalogue/x_1/index.html",
        "https://quotes.toscrape.com/author/A",
        "https://quotes.toscrape.com/page/2/",
        "not a url",
        "",
        "http://[broken",
    ]
)


@settings(max_examples=300)
@given(url=_urls, page=st.one_of(st.text(), garbled_html()), kind=st.sampled_from(PageKind))
def test_parsers_never_raise_and_are_deterministic(url: str, page: str, kind: PageKind) -> None:
    for parse in PARSERS:
        first = parse(url, page, kind)
        assert parse(url, page, kind) == first
        assert all(isinstance(w, str) for w in first.warnings)
        assert len({link.url for link in first.links}) == len(first.links)


def test_parsing_every_saved_page_twice_gives_equal_results() -> None:
    for path in sorted(SITES.rglob("*.html")):
        page = path.read_text(encoding="utf-8")
        for parse in PARSERS:
            for kind in PageKind:
                assert parse("https://x.test/", page, kind) == parse("https://x.test/", page, kind)


_title = st.text(
    alphabet=st.characters(categories=["L", "N", "Zs"], exclude_characters="\x1f"),
    min_size=1,
    max_size=60,
).filter(lambda t: normalize_text(t) != "")


@given(
    title=_title,
    cents=st.integers(1, 999_999),
    rating=st.integers(1, 5),
    stock=st.integers(0, 500),
    mojibake=st.booleans(),
)
def test_generated_product_page_round_trips(
    title: str, cents: int, rating: int, stock: int, mojibake: bool
) -> None:
    price = Decimal(cents).scaleb(-2)
    symbol = "Â£" if mojibake else "£"
    availability = f"In stock ({stock} available)" if stock else "Out of stock"
    word = {v: k for k, v in books.RATINGS.items()}[rating]
    page = (
        '<ul class="breadcrumb"><li><a href="/">Home</a></li><li><a href="b">Books</a></li>'
        '<li><a href="c">Poetry</a></li></ul><article class="product_page">'
        '<div id="product_gallery"><img src="../../media/i.jpg"></div>'
        f'<div class="product_main"><h1>{html_lib.escape(title)}</h1>'
        f'<p class="price_color">{symbol}{price}</p>'
        f'<p class="instock availability">\n    {availability}\n</p>'
        f'<p class="star-rating {word}"></p></div>'
        "<table><tr><th>UPC</th><td>0123456789abcdef</td></tr></table></article>"
    )
    url = "https://books.toscrape.com/catalogue/x_1/index.html"
    result = books.parse(url, page, PageKind.DETAIL)
    assert result.warnings == ()
    (draft,) = result.drafts
    assert draft.title == normalize_text(title)
    assert (draft.price_gbp, draft.rating, draft.stock_count, draft.in_stock) == (
        price,
        rating,
        stock,
        stock > 0,
    )
    lineage = _lineage(url, books_v1.CONTRACT_VERSION)
    valid = validate(
        BooksDataset(books=(Candidate(draft, lineage),), categories=frozenset({"Poetry"}))
    )
    (book,) = valid.books
    assert isinstance(book, Book)
    assert (book.title, book.price_gbp, book.rating, book.stock_count) == (
        normalize_text(title),
        price,
        rating,
        stock,
    )


def _lineage(url: str, contract: str) -> Lineage:
    return Lineage(
        source_url=url,
        content_sha256=hashlib.sha256(url.encode()).hexdigest(),
        fetch_run_id="fetch",
        parse_run_id=derive_parse_run_id("0" * 64, "p", "c"),
        parser_version="p",
        contract_version=contract,
    )


_maybe_str = st.one_of(
    st.none(), st.sampled_from(["", " x", "Poetry", "A", "abc"]), st.text(max_size=5)
)
_upc = st.one_of(st.none(), st.sampled_from(["0123456789abcdef", "fedcba9876543210", "bad"]))


@st.composite
def book_candidates(draw: st.DrawFn) -> Candidate[BookDraft]:
    url = draw(
        st.sampled_from(
            ["https://books.toscrape.com/a", "https://books.toscrape.com/b", "http://x/"]
        )
    )
    draft = BookDraft(
        upc=draw(_upc),
        title=draw(_maybe_str),
        price_gbp=draw(st.one_of(st.none(), st.just("£x"), st.decimals(-5, 100, places=3))),
        stock_count=draw(st.one_of(st.none(), st.integers(-2, 5), st.just("n/a"))),
        in_stock=draw(st.one_of(st.none(), st.booleans())),
        rating=draw(st.one_of(st.none(), st.integers(0, 6), st.just("Three"))),
        category=draw(_maybe_str),
        description=draw(st.text(max_size=5)),
        product_url=url,
        image_url=draw(st.one_of(st.none(), st.just(url))),
    )
    contract = draw(st.sampled_from([books_v1.CONTRACT_VERSION, "books/9.9.9"]))
    return Candidate(draft, _lineage(url, contract))


@settings(max_examples=400)
@given(rows=st.lists(book_candidates(), max_size=12))
def test_book_validation_never_loses_a_row(rows: list[Candidate[BookDraft]]) -> None:
    result = validate(BooksDataset(books=tuple(rows), categories=frozenset({"Poetry", "A"})))
    assert len(result.valid) + len(result.quarantined) == len(rows) == result.report.total_rows
    assert all(q.failures for q in result.quarantined)
    assert result == validate(
        BooksDataset(books=tuple(reversed(rows)), categories=frozenset({"Poetry", "A"}))
    )


@st.composite
def quotes_dataset(draw: st.DrawFn) -> QuotesDataset:
    slugs = st.one_of(st.none(), st.sampled_from(["A", "B", "a/b"]))
    quote_rows = []
    for position in range(draw(st.integers(0, 8))):
        text = draw(st.one_of(st.none(), st.sampled_from(["One.", "Two.", ""])))
        name = draw(st.one_of(st.none(), st.just("N")))
        good_id = quote_id(text, name) if text and name else None
        draft = QuoteDraft(
            quote_id=draw(st.sampled_from([good_id, None, "0" * 64])),
            text=text,
            author_name=name,
            author_slug=draw(slugs),
            tags=draw(st.sampled_from([(), ("a", "b"), ("b", "a"), ("A",)])),
            page_url="https://quotes.toscrape.com/",
            position=position,
        )
        quote_rows.append(Candidate(draft, _lineage(draft.page_url, quotes_v1.CONTRACT_VERSION)))
    author_rows = []
    for _ in range(draw(st.integers(0, 4))):
        slug = draw(slugs)
        draft_a = AuthorDraft(
            author_slug=slug,
            name=draw(_maybe_str),
            born_date=draw(st.sampled_from([date(1900, 1, 1), "soon", None])),
            born_location=draw(_maybe_str),
            bio=draw(_maybe_str),
        )
        url = f"https://quotes.toscrape.com/author/{slug}"
        author_rows.append(Candidate(draft_a, _lineage(url, quotes_v1.CONTRACT_VERSION)))
    return QuotesDataset(
        quotes=tuple(quote_rows),
        authors=tuple(author_rows),
        require_author_links=draw(st.booleans()),
    )


@settings(max_examples=400)
@given(dataset=quotes_dataset())
def test_quote_validation_never_loses_a_row(dataset: QuotesDataset) -> None:
    result = validate(dataset)
    total = len(dataset.quotes) + len(dataset.authors)
    assert len(result.valid) + len(result.quarantined) == total == result.report.total_rows
    assert len(result.quotes) + len(result.authors) == len(result.valid)
