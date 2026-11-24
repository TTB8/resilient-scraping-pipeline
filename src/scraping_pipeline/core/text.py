"""Text normalization and content-derived identifiers.

Scraped text arrives with whatever whitespace the template produced (indentation, newlines, runs of
spaces) and, in principle, in either Unicode composition form. Two pages that *mean* the same text must
produce the same string, or a content-derived id would change when a template is re-indented. So every
text field the parsers emit goes through ``normalize_text``: NFC composition, every run of Unicode
whitespace collapsed to one ASCII space, ends stripped. It is locale-free and has no state.

``quote_id`` lives here rather than in a parser or a contract because both need it: the parser derives
the id, and the contract re-derives it to check the row is self-consistent.
"""

import hashlib
import re
import unicodedata

_WHITESPACE = re.compile(r"\s+")

# Separates the parts of a hashed key. U+001F (UNIT SEPARATOR) cannot appear in normalized text, so
# ("ab", "c") and ("a", "bc") can never hash to the same input.
KEY_SEPARATOR = "\x1f"


def normalize_text(value: str) -> str:
    """NFC-compose, collapse whitespace runs to a single space and strip the ends.

    >>> normalize_text("  Caf\\u0065\\u0301\\n\\t au   lait ")
    'Café au lait'
    """
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFC", value)).strip()


def quote_id(text: str, author_name: str) -> str:
    """The stable id of a quote: sha256 over ``normalize(text) + U+001F + normalize(author)``.

    The quotes site has no ids of its own, and the same quote is served by the static listing, the
    JavaScript variant and the tag pages. Hashing the content gives all of them one key, which is what
    makes cross-variant deduplication and run-to-run comparison possible. The hex digest is lowercase.
    """
    key = normalize_text(text) + KEY_SEPARATOR + normalize_text(author_name)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
