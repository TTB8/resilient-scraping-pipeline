"""Deterministic Parquet and CSV writers: same rows in, byte-identical file out.

A deliverable that changes bytes when its data did not is a deliverable nobody can diff, checksum or
cache. Reproducibility is the pipeline's headline promise (same cache, same output), so these
writers remove every source of variation that the usual one-liners let in:

- **Row order.** Rows are sorted by caller-named ``sort_key`` columns before writing, so the order
  in which the parse stage happened to produce them never leaks into the file. The key must be
  unique: with ties, a stable sort would fall back to the input order, which is exactly the
  variation we are removing, so duplicates raise :class:`SchemaMismatchError`. ``None`` sorts first.
- **Columns.** Every row must have exactly the declared columns (Parquet: the schema's fields; CSV:
  ``columns``). Missing or extra keys raise instead of being silently nulled or dropped, and
  key insertion order in the row mappings does not matter.
- **Parquet writer settings.** Everything that affects bytes is pinned in :data:`PARQUET_OPTIONS`
  rather than left to library defaults that can move between releases. Parquet footers carry no
  creation timestamp. They do carry ``created_by`` (``"parquet-cpp-arrow version X.Y.Z"``) and the
  Arrow schema (``ARROW:schema``), both of which are stable for a given pyarrow version; the
  ``uv.lock`` pins that version, so a locked environment reproduces files exactly, and a pyarrow
  upgrade is an intentional, reviewable change to the output bytes.
- **CSV rendering.** UTF-8 without BOM, ``\\n`` line endings, minimal quoting, a fixed column order
  and one canonical text form per type (see :func:`render_csv_value`).

All writes go through :mod:`scraping_pipeline.storage.atomic`, so a killed export leaves the
previous deliverable intact rather than a truncated one.
"""

import csv
import io
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from itertools import pairwise
from pathlib import Path
from typing import Any, Final

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from scraping_pipeline.storage.atomic import atomic_path, atomic_write_text

__all__ = [
    "LIST_SEPARATOR",
    "PARQUET_OPTIONS",
    "SchemaMismatchError",
    "render_csv_value",
    "write_csv",
    "write_parquet",
]

Row = Mapping[str, object]

#: Separator for list/tuple cells in CSV. Items containing it are rejected, so the cell can always be
#: split back unambiguously.
LIST_SEPARATOR: Final = "|"

#: Every ``pyarrow.parquet.write_table`` option that affects the output bytes, pinned explicitly.
PARQUET_OPTIONS: Final[Mapping[str, Any]] = {
    "version": "2.6",
    "data_page_version": "1.0",
    "compression": "zstd",
    "compression_level": 3,
    "use_dictionary": True,
    "write_statistics": True,
    "row_group_size": 128 * 1024,
    "data_page_size": 1024 * 1024,
    "write_batch_size": 1024,
    "dictionary_pagesize_limit": 1024 * 1024,
    "use_byte_stream_split": False,
    "store_schema": True,
    "write_page_index": False,
    "write_page_checksum": False,
    "use_compliant_nested_type": True,
}


class SchemaMismatchError(ValueError):
    """Rows do not match the declared schema or columns, or the sort key is not unique."""


def write_parquet(
    path: Path, rows: Sequence[Row], schema: pa.Schema, *, sort_key: Sequence[str]
) -> None:
    """Write ``rows`` to ``path`` as Parquet with ``schema``, sorted by ``sort_key``.

    Raises :class:`SchemaMismatchError` when a row's keys differ from the schema's field names, a
    value cannot be converted to its field's type, a non-nullable field holds ``None``, or the sort
    key is missing from the schema or not unique.
    """
    names = list(schema.names)
    ordered = _sorted_rows(rows, names, sort_key)
    try:
        table = pa.Table.from_pylist(ordered, schema=schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise SchemaMismatchError(f"rows do not fit the schema: {exc}") from exc
    for schema_field in schema:
        # pyarrow accepts None in a non-nullable field without complaint; a contract must not.
        if not schema_field.nullable and table.column(schema_field.name).null_count:
            raise SchemaMismatchError(f"non-nullable column {schema_field.name!r} contains nulls")
    sorting = [pq.SortingColumn(names.index(key), nulls_first=True) for key in sort_key]
    with atomic_path(path) as temp:
        pq.write_table(table, temp, sorting_columns=sorting, **PARQUET_OPTIONS)


def write_csv(
    path: Path, rows: Sequence[Row], columns: Sequence[str], *, sort_key: Sequence[str]
) -> None:
    """Write ``rows`` to ``path`` as CSV with a ``columns`` header, sorted by ``sort_key``.

    Cells are rendered by :func:`render_csv_value`. Raises :class:`SchemaMismatchError` on missing
    or extra keys or a missing or non-unique sort key, and ``TypeError``/``ValueError`` from
    rendering an unsupported value.
    """
    names = list(columns)
    if len(set(names)) != len(names):
        raise SchemaMismatchError(f"duplicate column names in {names}")
    ordered = _sorted_rows(rows, names, sort_key)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(names)
    for row in ordered:
        writer.writerow([render_csv_value(row[name]) for name in names])
    atomic_write_text(path, buffer.getvalue(), encoding="utf-8")


def render_csv_value(value: object) -> str:
    """The canonical CSV text of one cell value.

    ======================  =============================================================
    ``None``                empty string
    ``bool``                ``true`` / ``false``
    ``int``, ``str``        ``str(value)`` (a ``StrEnum`` renders as its value)
    ``float``               ``repr`` (shortest round-trip form, stable across platforms)
    ``Decimal``             plain notation, never an exponent: ``Decimal("1E+3")`` -> ``1000``
    ``datetime``            ``isoformat()``, e.g. ``2026-01-02T03:04:05+00:00``
    ``date``, ``time``      ``isoformat()``
    ``Enum``                its value, rendered by these rules
    ``list``, ``tuple``     items rendered by these rules and joined with ``|``
    ======================  =============================================================

    List items must be scalars and must not contain ``|`` (``ValueError``); any other type raises
    ``TypeError`` rather than falling back to ``str()``, whose output is not a contract.
    """
    if isinstance(value, list | tuple):
        parts = [_render_scalar(item) for item in value]
        if any(LIST_SEPARATOR in part for part in parts):
            raise ValueError(f"list item contains the separator {LIST_SEPARATOR!r}: {value!r}")
        return LIST_SEPARATOR.join(parts)
    return _render_scalar(value)


def _render_scalar(value: object) -> str:  # noqa: PLR0911 - one return per type reads as a table
    if value is None:
        return ""
    if isinstance(value, bool):  # before int: bool is an int subclass
        return "true" if value else "false"
    if isinstance(value, str):  # before Enum: a StrEnum member is a str
        return str(value)
    if isinstance(value, Enum):
        return _render_scalar(value.value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime | date | time):  # datetime is a date subclass; same rule
        return value.isoformat()
    raise TypeError(f"cannot render {type(value).__name__} as a CSV cell: {value!r}")


def _sorted_rows(rows: Sequence[Row], names: Sequence[str], sort_key: Sequence[str]) -> list[Row]:
    """Check every row has exactly ``names`` and return them sorted by a unique ``sort_key``."""
    if not sort_key:
        raise SchemaMismatchError("sort_key must name at least one column")
    unknown = [key for key in sort_key if key not in names]
    if unknown:
        raise SchemaMismatchError(f"sort_key columns {unknown} are not in {list(names)}")
    expected = set(names)
    for index, row in enumerate(rows):
        keys = set(row)
        if keys != expected:
            missing, extra = sorted(expected - keys), sorted(keys - expected)
            raise SchemaMismatchError(f"row {index}: missing columns {missing}, extra {extra}")

    def key_of(row: Row) -> tuple[tuple[bool, object], ...]:
        # (False, None) sorts before (True, value): None first, and None is never compared.
        return tuple((row[key] is not None, row[key]) for key in sort_key)

    try:
        ordered = sorted(rows, key=key_of)
    except TypeError as exc:
        raise SchemaMismatchError(f"sort_key {list(sort_key)} values are not comparable") from exc
    for previous, current in pairwise(ordered):
        if key_of(previous) == key_of(current):
            raise SchemaMismatchError(
                f"sort_key {list(sort_key)} is not unique: {key_of(current)!r} repeats"
            )
    return ordered
