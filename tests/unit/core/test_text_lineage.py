import doctest

import pytest

from scraping_pipeline.core import lineage, text
from scraping_pipeline.core.lineage import derive_parse_run_id
from scraping_pipeline.core.text import normalize_text, quote_id

SHA = "ab" * 32


def test_docstring_examples_hold() -> None:
    for module in (text, lineage):
        assert doctest.testmod(module).failed == 0


def test_normalize_text_composes_and_collapses() -> None:
    assert normalize_text("e\u0301") == "\u00e9"
    assert normalize_text("\n  a \u00a0\t b  ") == "a b"
    assert normalize_text("   ") == ""


def test_quote_id_ignores_whitespace_and_composition_but_not_content() -> None:
    base = quote_id("To be or not", "Someone")
    assert base == quote_id("  To  be\nor not ", " Someone\t")
    assert quote_id("Caf\u00e9", "A") == quote_id("Cafe\u0301", "A")
    assert base != quote_id("To be or not", "Someone else")
    # The separator keeps the boundary: ("ab", "c") is not ("a", "bc").
    assert quote_id("ab", "c") != quote_id("a", "bc")
    assert len(base) == 64
    assert base == base.lower()


def test_parse_run_id_is_stable_and_sensitive_to_every_input() -> None:
    a = derive_parse_run_id(SHA, "p/1", "c/1")
    assert a == derive_parse_run_id(SHA, "p/1", "c/1")
    assert len(a) == 16
    assert int(a, 16) >= 0
    assert a != derive_parse_run_id("cd" * 32, "p/1", "c/1")
    assert a != derive_parse_run_id(SHA, "p/2", "c/1")
    assert a != derive_parse_run_id(SHA, "p/1", "c/2")


@pytest.mark.parametrize(
    ("manifest", "parser", "contract"),
    [
        ("AB" * 32, "p", "c"),  # upper-case hex
        ("ab" * 31, "p", "c"),  # too short
        (SHA, "", "c"),
        (SHA, "p", ""),
        (SHA, "p\x1fx", "c"),
    ],
)
def test_parse_run_id_fails_closed(manifest: str, parser: str, contract: str) -> None:
    with pytest.raises(ValueError, match="must be"):
        derive_parse_run_id(manifest, parser, contract)
