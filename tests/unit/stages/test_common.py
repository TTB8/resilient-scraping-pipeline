"""The shared stage plumbing: the draft codec, canonical JSON, run records, stopped domains."""

from __future__ import annotations

import dataclasses
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from scraping_pipeline.core.frontier import FrontierEntry, mark_stopped
from scraping_pipeline.core.lineage import Lineage
from scraping_pipeline.core.parsers.base import AuthorDraft, BookDraft, QuoteDraft
from scraping_pipeline.core.types import PageKind, ResponseClass, Site
from scraping_pipeline.stages.common import (
    StageInputError,
    canonical_json,
    decode_draft,
    decode_lineage,
    decode_value,
    encode_draft,
    encode_lineage,
    encode_value,
    new_run_id,
    read_json,
    read_jsonl,
    record_type_of,
    stage_run,
    stopped_domains,
)
from scraping_pipeline.storage.state import (
    DomainEvent,
    DomainEventKind,
    RunStatus,
    Stage,
    StateStore,
)
from stage_support import stage_settings

BOOK = BookDraft(
    upc="a897fe39b1053632",
    title="A Light in the Attic",
    price_gbp=Decimal("51.77"),
    stock_count=22,
    in_stock=True,
    rating=3,
    category="Poetry",
    description="",
    product_url="https://books.toscrape.com/catalogue/a/index.html",
    image_url=None,
)
LINEAGE = Lineage("https://h/x", "0" * 64, "fetch-1", "0123456789abcdef", "p/1", "c/1")

pytestmark = pytest.mark.usefixtures("no_real_fetchers")


def test_codec_keeps_the_types_the_parser_produced() -> None:
    raw = dataclasses.replace(BOOK, price_gbp="£51.77x")
    for draft in (
        BOOK,
        raw,
        QuoteDraft("q", "t", "a", None, ("b", "a"), "https://h/", 3),
        AuthorDraft("A", "A", date(1879, 3, 14), "Ulm", None),
        AuthorDraft("A", "A", "March 99, 1879", "Ulm", "bio"),
    ):
        encoded = json.loads(canonical_json(encode_draft(draft)))
        assert decode_draft(record_type_of(draft), encoded) == draft
    # A Decimal and the same digits as a raw string are told apart.
    assert encode_value(Decimal("1.5")) != encode_value("1.5")


@pytest.mark.slow
@given(
    st.recursive(
        st.none()
        | st.booleans()
        | st.integers()
        | st.text()
        | st.decimals(allow_nan=False, allow_infinity=False)
        | st.dates(),
        lambda inner: st.lists(inner, max_size=4).map(tuple),
        max_leaves=8,
    )
)
def test_codec_round_trips_every_draft_value(value: object) -> None:
    assert decode_value(json.loads(canonical_json(encode_value(value)))) == value


def test_codec_fails_closed() -> None:
    with pytest.raises(TypeError, match="cannot encode"):
        encode_value(1.5)
    with pytest.raises(StageInputError, match="unknown tagged"):
        decode_value({"money": "1"})
    with pytest.raises(StageInputError, match="unknown record type"):
        decode_draft("film", {})
    with pytest.raises(StageInputError, match="fields"):
        decode_draft("book", {"upc": "x"})
    assert decode_lineage(encode_lineage(LINEAGE)) == LINEAGE
    with pytest.raises(StageInputError, match="malformed lineage"):
        decode_lineage({**encode_lineage(LINEAGE), "source_url": 1})


def test_canonical_json_is_order_free_and_compact() -> None:
    assert canonical_json({"b": 1, "a": ["é", None]}) == '{"a":["é",null],"b":1}'


def test_reading_missing_or_broken_inputs_says_what_to_do(tmp_path: Path) -> None:
    with pytest.raises(StageInputError, match="run the previous stage"):
        read_json(tmp_path / "nope.json")
    with pytest.raises(StageInputError, match="run the previous stage"):
        read_jsonl(tmp_path / "nope.jsonl")
    (tmp_path / "bad.json").write_text("{")
    with pytest.raises(StageInputError, match="not valid JSON"):
        read_json(tmp_path / "bad.json")
    (tmp_path / "bad.jsonl").write_text('{"a":1}\n{\n')
    with pytest.raises(StageInputError, match=r"bad.jsonl:2"):
        read_jsonl(tmp_path / "bad.jsonl")


def test_run_ids_are_unique_and_name_the_stage() -> None:
    ids = {new_run_id(Stage.FETCH) for _ in range(50)}
    assert len(ids) == 50
    assert all(i.startswith("fetch-") for i in ids)


def test_stage_run_records_how_the_stage_ended(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)
    with StateStore.open(settings.state_db) as store:
        with stage_run(store, settings, Site.QUOTES, Stage.PARSE, run_id="ok") as run:
            run.summary = {"records": 3}
        with (
            pytest.raises(ValueError, match="boom"),
            stage_run(store, settings, Site.QUOTES, Stage.PARSE, run_id="bad"),
        ):
            raise ValueError("boom")
        with (
            pytest.raises(KeyboardInterrupt),
            stage_run(store, settings, Site.QUOTES, Stage.PARSE, run_id="int"),
        ):
            raise KeyboardInterrupt
        runs = {r.run_id: r for r in store.runs(Site.QUOTES)}
    assert runs["ok"].status is RunStatus.SUCCEEDED
    assert json.loads(runs["ok"].summary_json or "") == {"records": 3}
    assert json.loads(runs["ok"].settings_json)["data_dir"] == str(tmp_path)
    assert runs["bad"].status is RunStatus.FAILED
    assert "boom" in (runs["bad"].summary_json or "")
    assert runs["int"].status is RunStatus.INTERRUPTED


def test_stopped_domains_come_from_the_frontier_with_the_latest_reason(tmp_path: Path) -> None:
    settings = stage_settings(tmp_path)
    url = "https://quotes.toscrape.com/page/2/"
    other = "https://other.example/page/"
    with StateStore.open(settings.state_db) as store:
        store.start_run("r", Site.QUOTES, Stage.FETCH, "2026-01-01T00:00:00+00:00", "{}")
        entries = [FrontierEntry(u, Site.QUOTES, PageKind.LISTING, 0) for u in (url, other)]
        store.add_entries(entries, "r")
        assert stopped_domains(store, Site.QUOTES) == {}
        for at, reason in ((1.0, "first"), (2.0, "403 blocked")):
            store.record_domain_event(
                DomainEvent(
                    "r",
                    Site.QUOTES,
                    "quotes.toscrape.com",
                    DomainEventKind.DOMAIN_STOPPED,
                    reason,
                    at,
                    ResponseClass.BLOCKED,
                )
            )
        for entry in entries:
            store.record_result("r", entry, mark_stopped(entry, "domain stopped: x"), None)
        assert stopped_domains(store, Site.QUOTES) == {
            "other.example": "domain stopped: x",  # no event: the entry's own reason
            "quotes.toscrape.com": "403 blocked",
        }
