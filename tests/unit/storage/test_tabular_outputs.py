import os
import subprocess
import sys
import textwrap
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum, StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scraping_pipeline.storage.outputs import (
    PARQUET_OPTIONS,
    SchemaMismatchError,
    render_csv_value,
    write_csv,
    write_parquet,
)

SCHEMA = pa.schema(
    [
        pa.field("upc", pa.string(), nullable=False),
        pa.field("title", pa.string()),
        pa.field("price", pa.decimal128(10, 2)),
        pa.field("in_stock", pa.int64()),
        pa.field("scraped_on", pa.date32()),
        pa.field("tags", pa.list_(pa.string())),
    ]
)
COLUMNS = ["upc", "title", "price", "in_stock", "scraped_on", "tags"]


def rows() -> list[dict[str, object]]:
    return [
        {
            "upc": "c",
            "title": "Sharp Objects",
            "price": Decimal("47.82"),
            "in_stock": 20,
            "scraped_on": date(2026, 11, 24),
            "tags": ["mystery"],
        },
        {
            "upc": "a",
            "title": 'A Light in the "Attic"',
            "price": Decimal("51.77"),
            "in_stock": 22,
            "scraped_on": date(2026, 11, 24),
            "tags": ["poetry", "classic"],
        },
        {
            # keys deliberately in another insertion order
            "tags": [],
            "in_stock": None,
            "scraped_on": None,
            "price": None,
            "title": "line\nbreak, comma",
            "upc": "b",
        },
    ]


# --- Parquet -----------------------------------------------------------------------------------


def test_parquet_roundtrip_is_sorted(tmp_path: Path) -> None:
    path = tmp_path / "books.parquet"
    write_parquet(path, rows(), SCHEMA, sort_key=["upc"])
    table = pq.read_table(path)
    assert table.schema.equals(SCHEMA)
    assert table.column("upc").to_pylist() == ["a", "b", "c"]
    assert table.column("price").to_pylist() == [Decimal("51.77"), None, Decimal("47.82")]


def test_parquet_is_byte_identical_whatever_the_input_order(tmp_path: Path) -> None:
    first, second = tmp_path / "1.parquet", tmp_path / "2.parquet"
    write_parquet(first, rows(), SCHEMA, sort_key=["upc"])
    write_parquet(second, list(reversed(rows())), SCHEMA, sort_key=["upc"])
    assert first.read_bytes() == second.read_bytes()


def test_parquet_metadata_has_no_timestamp_and_records_settings(tmp_path: Path) -> None:
    path = tmp_path / "books.parquet"
    write_parquet(path, rows(), SCHEMA, sort_key=["upc"])
    meta = pq.read_metadata(path)
    assert meta.created_by == f"parquet-cpp-arrow version {pa.__version__}"
    assert meta.metadata is not None
    assert set(meta.metadata) == {b"ARROW:schema"}  # no pandas blob, no clock
    assert meta.format_version == PARQUET_OPTIONS["version"]
    group = meta.row_group(0)
    assert group.column(0).compression == "ZSTD"
    assert group.column(0).statistics is not None
    assert [c.column_index for c in group.sorting_columns] == [0]


def test_parquet_schema_mismatches_raise_clearly(tmp_path: Path) -> None:
    path = tmp_path / "x.parquet"
    missing = [{k: v for k, v in rows()[0].items() if k != "title"}]
    with pytest.raises(SchemaMismatchError, match=r"missing columns \['title'\]"):
        write_parquet(path, missing, SCHEMA, sort_key=["upc"])
    extra = [{**rows()[0], "isbn": "x"}]
    with pytest.raises(SchemaMismatchError, match=r"extra \['isbn'\]"):
        write_parquet(path, extra, SCHEMA, sort_key=["upc"])
    wrong_type = [{**rows()[0], "in_stock": "twenty"}]
    with pytest.raises(SchemaMismatchError, match="do not fit the schema"):
        write_parquet(path, wrong_type, SCHEMA, sort_key=["upc"])
    null_key = [{**rows()[0], "upc": None}]
    with pytest.raises(SchemaMismatchError, match="non-nullable column 'upc'"):
        write_parquet(path, null_key, SCHEMA, sort_key=["title"])
    assert not path.exists()


def test_parquet_type_error_is_wrapped(tmp_path: Path) -> None:
    bad = [{**rows()[0], "tags": 5}]
    with pytest.raises(SchemaMismatchError, match="do not fit the schema"):
        write_parquet(tmp_path / "x.parquet", bad, SCHEMA, sort_key=["upc"])


def test_failed_parquet_write_keeps_previous_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "books.parquet"
    write_parquet(path, rows(), SCHEMA, sort_key=["upc"])
    before = path.read_bytes()

    def crash(*args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(pq, "write_table", crash)
    with pytest.raises(OSError, match="disk full"):
        write_parquet(path, rows()[:1], SCHEMA, sort_key=["upc"])
    assert path.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["books.parquet"]


def test_empty_parquet_is_valid(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"
    write_parquet(path, [], SCHEMA, sort_key=["upc"])
    assert pq.read_table(path).num_rows == 0


# --- sort key -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sort_key", "message"),
    [
        ([], "at least one column"),
        (["nope"], "not in"),
        (["scraped_on"], "not unique"),
    ],
)
def test_bad_sort_keys_raise(tmp_path: Path, sort_key: list[str], message: str) -> None:
    data = [r for r in rows() if r["scraped_on"] is not None]
    with pytest.raises(SchemaMismatchError, match=message):
        write_csv(tmp_path / "x.csv", data, COLUMNS, sort_key=sort_key)


def test_none_sorts_first_and_composite_keys_work(tmp_path: Path) -> None:
    path = tmp_path / "x.csv"
    data: list[dict[str, object]] = [
        {"k": 2, "j": "b"},
        {"k": None, "j": "z"},
        {"k": 2, "j": "a"},
        {"k": 1, "j": "q"},
    ]
    write_csv(path, data, ["k", "j"], sort_key=["k", "j"])
    assert path.read_text() == "k,j\n,z\n1,q\n2,a\n2,b\n"


def test_incomparable_sort_values_raise(tmp_path: Path) -> None:
    mixed: list[dict[str, object]] = [{"k": 1}, {"k": "a"}]
    with pytest.raises(SchemaMismatchError, match="not comparable"):
        write_csv(tmp_path / "x.csv", mixed, ["k"], sort_key=["k"])


# --- CSV ----------------------------------------------------------------------------------------


def test_csv_exact_bytes(tmp_path: Path) -> None:
    path = tmp_path / "books.csv"
    write_csv(path, rows(), COLUMNS, sort_key=["upc"])
    assert path.read_bytes() == (
        b"upc,title,price,in_stock,scraped_on,tags\n"
        b'a,"A Light in the ""Attic""",51.77,22,2026-11-24,poetry|classic\n'
        b'b,"line\nbreak, comma",,,,\n'
        b"c,Sharp Objects,47.82,20,2026-11-24,mystery\n"
    )


def test_csv_is_byte_identical_whatever_the_input_order(tmp_path: Path) -> None:
    first, second = tmp_path / "1.csv", tmp_path / "2.csv"
    write_csv(first, rows(), COLUMNS, sort_key=["upc"])
    write_csv(second, list(reversed(rows())), COLUMNS, sort_key=["upc"])
    assert first.read_bytes() == second.read_bytes()


def test_csv_is_utf8_without_bom(tmp_path: Path) -> None:
    path = tmp_path / "q.csv"
    write_csv(path, [{"quote": "“Ñandú” — ok"}], ["quote"], sort_key=["quote"])
    assert path.read_bytes() == "quote\n“Ñandú” — ok\n".encode()


def test_csv_schema_mismatch_and_duplicate_columns(tmp_path: Path) -> None:
    with pytest.raises(SchemaMismatchError, match=r"missing columns \['b'\]"):
        write_csv(tmp_path / "x.csv", [{"a": 1}], ["a", "b"], sort_key=["a"])
    with pytest.raises(SchemaMismatchError, match="duplicate column"):
        write_csv(tmp_path / "x.csv", [{"a": 1}], ["a", "a"], sort_key=["a"])


class Colour(StrEnum):
    RED = "red"


class Level(Enum):
    HIGH = 3


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (None, ""),
        (True, "true"),
        (False, "false"),
        (0, "0"),
        (-12, "-12"),
        ("plain", "plain"),
        (0.1, "0.1"),
        (1e20, "1e+20"),
        (Decimal("1E+3"), "1000"),
        (Decimal("0.00000001"), "0.00000001"),
        (Decimal("-47.820"), "-47.820"),
        (date(2026, 1, 2), "2026-01-02"),
        (datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), "2026-01-02T03:04:05+00:00"),
        (time(9, 30), "09:30:00"),
        (Colour.RED, "red"),
        (Level.HIGH, "3"),
        (["a", 1, Decimal("2.5"), None, True], "a|1|2.5||true"),
        (("x",), "x"),
        ([], ""),
    ],
)
def test_csv_value_rendering(value: object, text: str) -> None:
    assert render_csv_value(value) == text


def test_csv_rendering_rejects_ambiguous_or_unknown_values() -> None:
    with pytest.raises(ValueError, match="separator"):
        render_csv_value(["a|b"])
    with pytest.raises(TypeError, match="cannot render"):
        render_csv_value({"a": 1})
    with pytest.raises(TypeError, match="cannot render"):
        render_csv_value([["nested"]])
    with pytest.raises(TypeError, match="cannot render"):
        render_csv_value(b"bytes")


# --- determinism across interpreters -------------------------------------------------------------

_CHILD = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, sys.argv[1])
    import test_tabular_outputs as t
    from scraping_pipeline.storage.outputs import write_csv, write_parquet
    out = Path(sys.argv[2])
    write_parquet(out / "books.parquet", t.rows(), t.SCHEMA, sort_key=["upc"])
    write_csv(out / "books.csv", t.rows(), t.COLUMNS, sort_key=["upc"])
    """
)


def _write_in_fresh_interpreter(out: Path, hash_seed: str) -> Mapping[str, bytes]:
    out.mkdir()
    env = {**os.environ, "PYTHONHASHSEED": hash_seed}
    subprocess.run(  # noqa: S603 - fixed arguments, our own interpreter
        [sys.executable, "-c", _CHILD, str(Path(__file__).parent), str(out)],
        check=True,
        env=env,
        timeout=120,
    )
    return {name: (out / name).read_bytes() for name in ("books.parquet", "books.csv")}


def test_outputs_are_byte_identical_across_processes_and_hash_seeds(tmp_path: Path) -> None:
    here = tmp_path / "here"
    here.mkdir()
    write_parquet(here / "books.parquet", rows(), SCHEMA, sort_key=["upc"])
    write_csv(here / "books.csv", rows(), COLUMNS, sort_key=["upc"])
    local = {name: (here / name).read_bytes() for name in ("books.parquet", "books.csv")}

    assert _write_in_fresh_interpreter(tmp_path / "seed1", "1") == local
    assert _write_in_fresh_interpreter(tmp_path / "seed2", "2024") == local
