"""Lineage: where every published row came from, and the deterministic id of the parse that made it.

Each final record carries six lineage columns (``source_url``, ``content_sha256``, ``fetch_run_id``,
``parse_run_id``, ``parser_version``, ``contract_version``) so that any value in an export can be
traced back to the exact cached bytes, the fetch that stored them and the code that read them.

Parsers do not know run ids; the parse stage builds a ``Lineage`` per cached page and attaches it.

Why ``parse_run_id`` is *derived* rather than random or time-based: parse must be byte-identical for
the same cache. A UUID or timestamp in every row would make two parses of the same cache differ. So the
id is a hash of exactly the things that determine the output (the cache manifest, the parser version
and the contract version): the same inputs give the same id, and changing any of them gives a new one.
"""

import hashlib
import re
from dataclasses import dataclass

from scraping_pipeline.core.text import KEY_SEPARATOR

_SHA256_HEX = re.compile(r"[0-9a-f]{64}")

PARSE_RUN_ID_LENGTH = 16
"""Hex characters kept from the digest: 64 bits, ample to tell parse runs of one project apart."""


@dataclass(frozen=True, slots=True)
class Lineage:
    """The lineage columns the parse stage attaches to every draft record of one cached page."""

    source_url: str
    content_sha256: str
    fetch_run_id: str
    parse_run_id: str
    parser_version: str
    contract_version: str


def derive_parse_run_id(manifest_sha256: str, parser_version: str, contract_version: str) -> str:
    """Return a short stable id for a parse run: the first 16 hex chars of a sha256.

    The digest is taken over ``manifest_sha256 + U+001F + parser_version + U+001F + contract_version``
    (UTF-8). It fails closed: a manifest hash that is not 64 lowercase hex characters, an empty version
    or a version containing the separator raises ``ValueError`` instead of producing an id that could
    collide or hide a wiring bug.

    >>> derive_parse_run_id("0" * 64, "books-parser/1.0.0", "books/1.0.0")
    '9cf173d472b1ea7b'
    """
    if not _SHA256_HEX.fullmatch(manifest_sha256):
        raise ValueError(f"manifest_sha256 must be 64 lowercase hex chars, got {manifest_sha256!r}")
    for name, value in (("parser_version", parser_version), ("contract_version", contract_version)):
        if not value or KEY_SEPARATOR in value:
            raise ValueError(f"{name} must be non-empty and free of U+001F, got {value!r}")
    key = KEY_SEPARATOR.join((manifest_sha256, parser_version, contract_version))
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:PARSE_RUN_ID_LENGTH]
