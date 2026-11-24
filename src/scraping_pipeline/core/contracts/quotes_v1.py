"""Contract ``quotes/1.0.0``: published quote and author rows (one version covers both, they ship together).

Versioning follows the same rules as ``books_v1``: **major** for anything a consumer could trip on
(column removed/renamed/retyped, a rule tightened or renamed), **minor** for additive changes (new
optional column, relaxed rule), **patch** for wording only. A major bump is a new module
(``quotes_v2.py``).

Changelog:

- ``1.0.0``: first version.

Decisions recorded here because they are part of the contract:

- ``quote_id`` must equal ``core.text.quote_id(text, author_name)``. The row proves its own key, so a
  hand-edited or mis-joined row cannot keep a stale id.
- ``tags`` is a tuple that is already sorted, unique and lowercase. The contract checks, it never
  repairs: a site that starts serving ``"Love"`` should show up in the quarantine, not be hidden.
- ``author_slug`` is nullable. The JavaScript variant renders no author link, so it has no slug to
  give. Whether a slug is *required* and must resolve to an author row is a dataset-level rule in
  ``core.validate`` (on for the static site, off for the JavaScript variant).
- ``born_date`` is a real ``date`` (ISO in every export); ``born_location`` drops the site's "in ".
"""

from datetime import date
from typing import Annotated, Self

from pydantic import AfterValidator, BaseModel, Field, model_validator
from pydantic_core import PydanticCustomError

from scraping_pipeline.core.contracts.base import (
    STRICT,
    LineageColumns,
    NonEmptyStr,
    Sha256Hex,
    WebUrl,
)
from scraping_pipeline.core.text import quote_id as derive_quote_id

CONTRACT_VERSION = "quotes/1.0.0"


def _canonical_tags(tags: tuple[str, ...]) -> tuple[str, ...]:
    if any(not tag or tag != tag.strip() for tag in tags):
        raise PydanticCustomError("tag_blank", "tags must be non-empty and trimmed")
    if any(tag != tag.lower() for tag in tags):
        raise PydanticCustomError("tag_not_lowercase", "tags must be lowercase")
    if list(tags) != sorted(set(tags)):
        raise PydanticCustomError("tags_not_sorted_unique", "tags must be sorted and unique")
    return tags


Slug = Annotated[str, Field(pattern=r"^[^/\s]+$")]
"""The site's author slug as it appears in ``/author/<slug>``: non-empty, no slash or whitespace."""


class _QuoteColumns(BaseModel):
    model_config = STRICT

    quote_id: Sha256Hex
    text: NonEmptyStr
    author_name: NonEmptyStr
    author_slug: Slug | None = None
    tags: Annotated[tuple[str, ...], AfterValidator(_canonical_tags)]
    page_url: WebUrl

    @model_validator(mode="after")
    def _id_matches_content(self) -> Self:
        expected = derive_quote_id(self.text, self.author_name)
        if self.quote_id != expected:
            raise PydanticCustomError(
                "quote_id_mismatch",
                "quote_id does not match sha256(text, author_name) = {expected}",
                {"field": "quote_id", "expected": expected},
            )
        return self


class Quote(LineageColumns, _QuoteColumns):
    """A validated quote row: the quote columns first, then the six lineage columns."""

    CONTRACT_VERSION = CONTRACT_VERSION


class _AuthorColumns(BaseModel):
    model_config = STRICT

    author_slug: Slug
    name: NonEmptyStr
    born_date: date
    born_location: NonEmptyStr
    bio: NonEmptyStr


class Author(LineageColumns, _AuthorColumns):
    """A validated author row: the author columns first, then the six lineage columns."""

    CONTRACT_VERSION = CONTRACT_VERSION
