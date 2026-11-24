"""Contract ``books/1.0.0``: what a published book row is guaranteed to look like.

Versioning (semver on the published shape, not on the code):

- **major** (``books/2.0.0``): a consumer could break. A column removed or renamed, a type changed, a
  rule tightened so rows that used to pass now fail, a rule name (error ``type``) renamed. Lives in a
  new module, ``books_v2.py``, next to this one, so old outputs stay readable.
- **minor** (``books/1.1.0``): additive. A new optional column, a rule relaxed.
- **patch** (``books/1.0.1``): wording of messages or docs only; every row validates the same.

Changelog:

- ``1.0.0``: first version.

Decisions recorded here because they are part of the contract:

- ``book_id`` is the site's UPC (16 lowercase hex chars), the only id the site guarantees unique.
- ``price_gbp`` is a ``Decimal`` > 0 with at most 2 decimal places, stored quantized to exactly 2
  (``51.7`` is published as ``51.70``), so money is never a float and always prints the same.
- ``in_stock`` must agree with ``stock_count`` (``in_stock == stock_count > 0``).
- ``description`` may be ``""``: some real product pages have no description section, and that is
  the site's truth, not a parse failure. ``""`` rather than ``None`` keeps the column non-nullable.
- ``category`` must be non-empty here; that it is a *known* category is a dataset-level rule in
  ``core.validate`` (it needs the category set discovered on the home page).
"""

from decimal import Decimal
from typing import Annotated, Self

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from scraping_pipeline.core.contracts.base import STRICT, HttpsUrl, LineageColumns, NonEmptyStr

CONTRACT_VERSION = "books/1.0.0"

_CENT = Decimal("0.01")


class _BookColumns(BaseModel):
    model_config = STRICT

    book_id: Annotated[str, Field(pattern=r"^[0-9a-f]{16}$")]
    title: NonEmptyStr
    price_gbp: Annotated[Decimal, Field(gt=0, decimal_places=2)]
    stock_count: Annotated[int, Field(ge=0)]
    in_stock: bool
    rating: Annotated[int, Field(ge=1, le=5)]
    category: NonEmptyStr
    description: str
    product_url: HttpsUrl
    image_url: HttpsUrl

    @field_validator("price_gbp")
    @classmethod
    def _two_places(cls, value: Decimal) -> Decimal:
        return value.quantize(_CENT)

    @model_validator(mode="after")
    def _stock_consistent(self) -> Self:
        if self.in_stock != (self.stock_count > 0):
            raise PydanticCustomError(
                "in_stock_mismatch",
                "in_stock={in_stock} contradicts stock_count={stock_count}",
                {
                    "field": "in_stock",
                    "in_stock": str(self.in_stock),
                    "stock_count": str(self.stock_count),
                },
            )
        return self


class Book(LineageColumns, _BookColumns):
    """A validated book row: the book columns first, then the six lineage columns."""

    CONTRACT_VERSION = CONTRACT_VERSION
