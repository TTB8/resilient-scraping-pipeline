"""Building blocks shared by every contract: strict model config, constrained types, lineage columns.

Contracts are strict on purpose. ``strict=True`` means no coercion: a price must already be a
``Decimal`` and a rating an ``int``, so a raw string a parser could not interpret is reported with its
value instead of being half-converted. ``extra="forbid"`` makes a renamed or added column a loud error,
and ``frozen=True`` makes a validated record impossible to alter afterwards.

Custom rules raise ``PydanticCustomError`` with a stable snake_case ``type``. That type is the *rule
name* in validation reports, so it is part of the contract: renaming one is a breaking change. A rule
that spans fields (a model validator) names the field it reports against in its error context as
``field``, because pydantic gives model-level errors an empty location.
"""

import re
from typing import Annotated, ClassVar
from urllib.parse import urlsplit

from pydantic import AfterValidator, BaseModel, ConfigDict, StringConstraints, field_validator
from pydantic_core import PydanticCustomError

from scraping_pipeline.core.urls import canonicalize

STRICT = ConfigDict(strict=True, frozen=True, extra="forbid")

NonEmptyStr = Annotated[str, StringConstraints(min_length=1, pattern=r"^\S(?:.*\S)?$")]
"""Non-empty, no leading or trailing whitespace (parsers emit normalized text)."""

Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_PARSE_RUN_ID = re.compile(r"[0-9a-f]{16}")
_WEB_SCHEMES = frozenset({"http", "https"})


def _web_url(value: str) -> str:
    try:
        canonical = canonicalize(value)  # rejects relative URLs and any scheme but http(s)
    except ValueError:
        canonical = None
    if canonical is None or urlsplit(value).scheme not in _WEB_SCHEMES:
        raise PydanticCustomError("url_not_http", "must be an absolute http(s) URL")
    if canonical != value:
        raise PydanticCustomError(
            "url_not_canonical", "must be canonical: {canonical}", {"canonical": canonical}
        )
    return value


WebUrl = Annotated[str, AfterValidator(_web_url)]
"""An absolute http(s) URL already in ``core.urls.canonicalize`` form, so joins on URLs are exact.

http is allowed as well as https because the integration tests crawl a local fixture server at
``http://127.0.0.1:<port>/``; any other scheme, a relative URL or a non-canonical spelling is rejected.
"""


class LineageColumns(BaseModel):
    """The six lineage columns, flattened into every record (friendlier for Parquet and CSV).

    Subclasses set ``CONTRACT_VERSION``; a row stamped with any other version is rejected, so a file
    can never mix rows validated against different contracts without saying so.
    """

    model_config = STRICT
    CONTRACT_VERSION: ClassVar[str] = ""

    source_url: WebUrl
    content_sha256: Sha256Hex
    fetch_run_id: NonEmptyStr
    parse_run_id: str
    parser_version: NonEmptyStr
    contract_version: str

    @field_validator("parse_run_id")
    @classmethod
    def _parse_run_id_shape(cls, value: str) -> str:
        if not _PARSE_RUN_ID.fullmatch(value):
            raise PydanticCustomError(
                "parse_run_id_shape", "must be 16 lowercase hex chars (derive_parse_run_id)"
            )
        return value

    @field_validator("contract_version")
    @classmethod
    def _contract_version_matches(cls, value: str) -> str:
        if value != cls.CONTRACT_VERSION:
            raise PydanticCustomError(
                "contract_version_mismatch",
                "expected {expected}",
                {"expected": cls.CONTRACT_VERSION},
            )
        return value
