"""Site parsers: cached HTML in, draft records + links + warnings out. Offline and deterministic.

Each site module exposes ``parse(url, html, kind) -> ParseResult`` and a ``PARSER_VERSION`` string
that goes into every record's lineage. Bump ``PARSER_VERSION`` whenever a change could alter the
output for the same input bytes: it is part of the derived ``parse_run_id``, so a re-parse with new
code is never mistaken for the old one.

Purity rules: no network, no filesystem, no clock, no locale, no randomness. The same ``(url, html,
kind)`` always produces an equal ``ParseResult``.
"""

from scraping_pipeline.core.parsers.base import (
    AuthorDraft,
    BookDraft,
    Draft,
    ParseResult,
    QuoteDraft,
)

__all__ = ["AuthorDraft", "BookDraft", "Draft", "ParseResult", "QuoteDraft"]
