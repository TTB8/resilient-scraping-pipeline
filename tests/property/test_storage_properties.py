"""Properties of the storage layer that must hold for any input, not just the examples we thought of."""

import gzip
import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from scraping_pipeline.storage.atomic import atomic_write_bytes, is_temp_name
from scraping_pipeline.storage.cache import FetchMeta, PageCache, gzip_bytes
from scraping_pipeline.storage.outputs import write_csv

META = FetchMeta(
    final_url="https://quotes.toscrape.com/",
    status=200,
    fetched_at="2026-11-24T10:00:00+00:00",
    fetch_run_id="prop",
    fetcher="httpx",
)

# Canonical URLs on one practice site: lower-case path segments, optional sorted query.
_segment = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-_", min_size=1, max_size=8)
urls = st.lists(_segment, min_size=1, max_size=3).map(
    lambda parts: "https://quotes.toscrape.com/" + "/".join(parts)
)
pages = st.dictionaries(urls, st.binary(max_size=256), min_size=0, max_size=12)


@settings(max_examples=60, deadline=None)
@given(data=st.data(), contents=pages)
def test_manifest_is_independent_of_insertion_order(
    data: st.DataObject, contents: dict[str, bytes]
) -> None:
    items = list(contents.items())
    order_a = data.draw(st.permutations(items))
    order_b = data.draw(st.permutations(items))
    with tempfile.TemporaryDirectory() as tmp:
        a, b = PageCache(Path(tmp) / "a"), PageCache(Path(tmp) / "b")
        for url, body in order_a:
            a.put(url, body, META)
        for url, body in order_b:
            b.put(url, body, META)
        assert a.manifest_sha256() == b.manifest_sha256()
        assert [e.url for e in a.iter_entries()] == sorted(contents)
        assert list(a.iter_entries()) == list(b.iter_entries())
        for entry in a.iter_entries():
            assert a.read_body(entry) == contents[entry.url]


@settings(max_examples=40, deadline=None)
@given(contents=pages, extra_url=urls, extra_body=st.binary(max_size=64))
def test_manifest_changes_when_any_page_changes(
    contents: dict[str, bytes], extra_url: str, extra_body: bytes
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cache = PageCache(Path(tmp))
        for url, body in contents.items():
            cache.put(url, body, META)
        before = cache.manifest_sha256()
        changed = contents.get(extra_url) != extra_body
        cache.put(extra_url, extra_body, META)
        assert (cache.manifest_sha256() != before) is changed


@given(body=st.binary(max_size=4096))
def test_gzip_objects_are_deterministic_and_lossless(body: bytes) -> None:
    packed = gzip_bytes(body)
    assert packed == gzip_bytes(body)
    assert packed[4:8] == b"\x00\x00\x00\x00"  # mtime
    assert gzip.decompress(packed) == body


@settings(max_examples=40, deadline=None)
@given(versions=st.lists(st.binary(max_size=512), min_size=1, max_size=5))
def test_atomic_write_leaves_last_version_and_no_temp_files(versions: list[bytes]) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "f"
        for version in versions:
            atomic_write_bytes(target, version)
        assert target.read_bytes() == versions[-1]
        assert not [p for p in Path(tmp).iterdir() if is_temp_name(p.name)]


_cell = st.one_of(
    st.none(),
    st.integers(),
    st.text(alphabet=st.characters(exclude_characters="|\r", exclude_categories=["Cs"])),
)


@settings(max_examples=40, deadline=None)
@given(data=st.data(), values=st.dictionaries(st.integers(), _cell, max_size=20))
def test_csv_bytes_do_not_depend_on_row_order(
    data: st.DataObject, values: dict[int, object]
) -> None:
    rows = [{"id": key, "value": value} for key, value in values.items()]
    shuffled = data.draw(st.permutations(rows))
    with tempfile.TemporaryDirectory() as tmp:
        a, b = Path(tmp) / "a.csv", Path(tmp) / "b.csv"
        write_csv(a, rows, ["id", "value"], sort_key=["id"])
        write_csv(b, shuffled, ["id", "value"], sort_key=["id"])
        assert a.read_bytes() == b.read_bytes()
