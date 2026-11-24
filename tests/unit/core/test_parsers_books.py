"""The books parser against every saved books.toscrape.com page, plus synthetic edge cases."""

from decimal import Decimal
from pathlib import Path

import pytest

from scraping_pipeline.core.parsers import books
from scraping_pipeline.core.parsers.base import BookDraft
from scraping_pipeline.core.types import Link, PageKind

SITES = Path(__file__).parents[2] / "fixtures" / "sites"
BASE = "https://books.toscrape.com/"


def page(rel: str) -> tuple[str, str]:
    """(real URL, saved HTML) of a fixture page; the folder layout mirrors the site's paths."""
    return BASE + rel, (SITES / "books" / rel).read_text(encoding="utf-8")


def detail(rel: str) -> tuple[BookDraft, tuple[str, ...]]:
    url, html = page(rel)
    result = books.parse(url, html, PageKind.DETAIL)
    assert result.links == ()
    assert len(result.drafts) == 1
    return result.drafts[0], result.warnings


def links_of(rel: str) -> tuple[Link, ...]:
    url, html = page(rel)
    result = books.parse(url, html, PageKind.LISTING)
    assert result.drafts == ()
    assert result.warnings == ()
    return result.links


def test_a_light_in_the_attic() -> None:
    draft, warnings = detail("catalogue/a-light-in-the-attic_1000/index.html")
    assert warnings == ()
    assert draft.upc == "a897fe39b1053632"
    assert draft.title == "A Light in the Attic"
    assert draft.price_gbp == Decimal("51.77")
    assert draft.stock_count == 22
    assert draft.in_stock is True
    assert draft.rating == 3
    assert draft.category == "Poetry"
    assert draft.description.startswith(
        "It's hard to imagine a world without A Light in the Attic."
    )
    assert draft.description.endswith("Shel, you never sounded so good. ...more")
    assert draft.product_url == BASE + "catalogue/a-light-in-the-attic_1000/index.html"
    assert draft.image_url == BASE + "media/cache/fe/72/fe72f0532301ec28892ae79a629a293c.jpg"


@pytest.mark.parametrize(
    ("rel", "expected"),
    [
        (
            "catalogue/its-only-the-himalayas_981/index.html",
            (
                "a22124811bfa8350",
                "It's Only the Himalayas",
                Decimal("45.17"),
                19,
                True,
                2,
                "Travel",
            ),
        ),
        (
            "catalogue/sharp-objects_997/index.html",
            ("e00eb4fd7b871a48", "Sharp Objects", Decimal("47.82"), 20, True, 4, "Mystery"),
        ),
    ],
)
def test_other_detail_pages_ignore_recently_viewed_cards(
    rel: str, expected: tuple[object, ...]
) -> None:
    # These pages also show "recently viewed" cards with their own price and star-rating.
    draft, warnings = detail(rel)
    assert warnings == ()
    got = (draft.upc, draft.title, draft.price_gbp, draft.stock_count, draft.in_stock)
    assert (*got, draft.rating, draft.category) == expected
    assert draft.description


def test_home_page_links_products_next_and_categories() -> None:
    links = links_of("index.html")
    kinds = [link.kind for link in links]
    assert kinds.count(PageKind.DETAIL) == 20
    assert kinds.count(PageKind.LISTING) == 1 + 50  # next + 50 categories (not the "Books" root)
    assert links[0] == Link(
        BASE + "catalogue/a-light-in-the-attic_1000/index.html", PageKind.DETAIL
    )
    assert links[20] == Link(BASE + "catalogue/page-2.html", PageKind.LISTING)
    assert links[21] == Link(
        BASE + "catalogue/category/books/travel_2/index.html", PageKind.LISTING
    )
    assert BASE + "catalogue/category/books_1/index.html" not in {link.url for link in links}
    assert len({link.url for link in links}) == len(links)


def test_home_page_also_reached_as_root_url() -> None:
    _, html = page("index.html")
    assert books.parse(BASE, html, PageKind.LISTING).links == links_of("index.html")


def test_category_names_from_home_page() -> None:
    names = books.category_names(page("index.html")[1])
    assert len(names) == 50
    assert names[:3] == ("Travel", "Mystery", "Historical Fiction")
    assert "Poetry" in names
    assert "Books" not in names


@pytest.mark.parametrize(
    ("rel", "products", "next_url"),
    [
        ("catalogue/page-2.html", 20, "catalogue/page-3.html"),
        ("catalogue/page-50.html", 20, None),
        (
            "catalogue/category/books/mystery_3/index.html",
            20,
            "catalogue/category/books/mystery_3/page-2.html",
        ),
        ("catalogue/category/books/mystery_3/page-2.html", 12, None),
        ("catalogue/category/books/travel_2/index.html", 11, None),
    ],
)
def test_listing_and_category_pages(rel: str, products: int, next_url: str | None) -> None:
    links = links_of(rel)  # the side nav on non-home pages is not read again
    details = [link for link in links if link.kind is PageKind.DETAIL]
    listings = [link for link in links if link.kind is PageKind.LISTING]
    assert len(details) == products
    assert listings == ([Link(BASE + next_url, PageKind.LISTING)] if next_url else [])
    assert all(link.url.startswith(BASE + "catalogue/") for link in details)


def test_listing_first_and_last_products_resolve_from_catalogue_dir() -> None:
    assert links_of("catalogue/page-2.html")[0].url == BASE + "catalogue/in-her-wake_980/index.html"
    assert links_of("catalogue/page-50.html")[-1].url == (
        BASE + "catalogue/1000-places-to-see-before-you-die_1/index.html"
    )
    travel = links_of("catalogue/category/books/travel_2/index.html")
    assert travel[0].url == BASE + "catalogue/its-only-the-himalayas_981/index.html"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("£51.77", Decimal("51.77")),
        ("Â£51.77", Decimal("51.77")),
        (" £ 7.5 ", Decimal("7.5")),
        ("£10", Decimal("10")),
        ("$51.77", "$51.77"),
        ("£", "£"),
        ("£1,000.00", "£1,000.00"),
    ],
)
def test_parse_price(raw: str, expected: Decimal | str) -> None:
    assert books.parse_price(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("In stock (22 available)", (22, True)),
        ("\n In  stock (0 available)\n", (0, True)),
        ("Out of stock", (0, False)),
        ("In stock", (None, True)),
        ("Backordered", ("Backordered", None)),
    ],
)
def test_parse_availability(raw: str, expected: tuple[object, object]) -> None:
    assert books.parse_availability(raw) == expected


def _product(
    *,
    price: str = "£12.00",
    availability: str = "In stock (3 available)",
    rating_class: str | None = "star-rating Five",
    upc: str | None = "0123456789abcdef",
    breadcrumb: str = '<li><a href="/">Home</a></li><li><a href="b">Books</a></li>'
    '<li><a href="c">Poetry</a></li>',
    description: str | None = "<div id='product_description'></div><p>Nice.</p>",
    image: str | None = '<img src="../../media/x.jpg">',
) -> str:
    rating = f'<p class="{rating_class}"></p>' if rating_class is not None else ""
    table = f"<table><tr><th>UPC</th><td>{upc}</td></tr></table>" if upc is not None else ""
    gallery = f'<div id="product_gallery">{image}</div>' if image is not None else ""
    return (
        f'<ul class="breadcrumb">{breadcrumb}</ul><article class="product_page">{gallery}'
        f'<div class="product_main"><h1>Title</h1><p class="price_color">{price}</p>'
        f'<p class="instock availability">{availability}</p>{rating}</div>'
        f"{description or ''}{table}</article>"
    )


URL = BASE + "catalogue/x_1/index.html"


def test_synthetic_product_round_trip() -> None:
    result = books.parse(URL, _product(), PageKind.DETAIL)
    assert result.warnings == ()
    draft = result.drafts[0]
    assert (draft.upc, draft.price_gbp, draft.stock_count, draft.rating) == (
        "0123456789abcdef",
        Decimal("12.00"),
        3,
        5,
    )
    assert draft.description == "Nice."
    assert draft.image_url == BASE + "media/x.jpg"


def test_missing_description_section_is_empty_and_silent() -> None:
    result = books.parse(URL, _product(description=None), PageKind.DETAIL)
    assert result.drafts[0].description == ""
    assert result.warnings == ()


def test_description_header_without_text_warns() -> None:
    html = _product(description="<div id='product_description'></div>")
    result = books.parse(URL, html, PageKind.DETAIL)
    assert result.drafts[0].description == ""
    assert result.warnings == ("description: header present but no text",)


@pytest.mark.parametrize(
    ("kwargs", "field", "value", "warning"),
    [
        ({"price": "$3.00"}, "price_gbp", "$3.00", "price_gbp: unrecognised price '$3.00'"),
        ({"price": ""}, "price_gbp", None, "price_gbp: missing"),
        ({"availability": "In stock"}, "stock_count", None, "stock_count: no count in 'In stock'"),
        ({"availability": ""}, "stock_count", None, "stock_count: availability missing"),
        (
            {"rating_class": "star-rating Zero"},
            "rating",
            "Zero",
            "rating: unknown rating class 'Zero'",
        ),
        ({"rating_class": None}, "rating", None, "rating: missing"),
        ({"upc": None}, "upc", None, "upc: missing from the product table"),
        ({"upc": ""}, "upc", None, "upc: missing from the product table"),
        (
            {"breadcrumb": '<li><a href="/">Home</a></li>'},
            "category",
            None,
            "category: breadcrumb has no category link",
        ),
        (
            {"breadcrumb": "<li><a>Home</a></li><li><a>Books</a></li><li><a> </a></li>"},
            "category",
            None,
            "category: empty breadcrumb link",
        ),
        ({"image": None}, "image_url", None, "image_url: missing"),
        ({"image": "<img>"}, "image_url", None, "image_url: link without href"),
        (
            {"image": '<img src="javascript:void(0)">'},
            "image_url",
            None,
            "image_url: unresolvable href 'javascript:void(0)'",
        ),
    ],
)
def test_odd_product_pages_degrade_to_warnings(
    kwargs: dict[str, str | None], field: str, value: object, warning: str
) -> None:
    result = books.parse(URL, _product(**kwargs), PageKind.DETAIL)  # type: ignore[arg-type]
    assert getattr(result.drafts[0], field) == value
    assert warning in result.warnings


def test_missing_title_warns() -> None:
    result = books.parse(URL, _product().replace("<h1>Title</h1>", ""), PageKind.DETAIL)
    assert result.drafts[0].title is None
    assert "title: missing" in result.warnings


def test_bad_page_url_is_kept_raw_with_warning() -> None:
    result = books.parse("not a url", _product(image=None), PageKind.DETAIL)
    assert result.drafts[0].product_url == "not a url"
    assert "page: not an absolute http(s) URL 'not a url'" in result.warnings


def test_listing_links_are_deduplicated_in_document_order() -> None:
    html = (
        '<article class="product_pod"><h3><a href="b_2/index.html">B</a></h3></article>'
        '<article class="product_pod"><h3><a href="a_1/index.html">A</a></h3></article>'
        '<article class="product_pod"><h3><a href="b_2/index.html#x">B</a></h3></article>'
        '<article class="product_pod"><h3><a>no href</a></h3></article>'
    )
    result = books.parse(BASE + "catalogue/page-3.html", html, PageKind.LISTING)
    assert [link.url for link in result.links] == [
        BASE + "catalogue/b_2/index.html",
        BASE + "catalogue/a_1/index.html",
    ]
    assert result.warnings == ("product link: link without href",)


def test_home_detection_survives_a_garbled_url() -> None:
    result = books.parse("http://[broken", "<p></p>", PageKind.LISTING)
    assert result == books.parse("http://[broken", "<p></p>", PageKind.LISTING)
    assert result.links == ()


def test_author_kind_is_not_a_books_page() -> None:
    result = books.parse(BASE, "<html></html>", PageKind.AUTHOR)
    assert result.drafts == ()
    assert result.warnings == ("page: the books site has no 'author' pages",)


def test_parser_version_is_set() -> None:
    assert books.PARSER_VERSION == "books-parser/1.0.0"
