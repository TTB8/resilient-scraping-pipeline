import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scraping_pipeline.storage import cache as cache_module
from scraping_pipeline.storage.atomic import atomic_write_bytes
from scraping_pipeline.storage.cache import (
    CacheEntry,
    CorruptCacheError,
    FetchMeta,
    PageCache,
    gzip_bytes,
)

URL_A = "https://books.toscrape.com/catalogue/page-2.html"
URL_B = "https://books.toscrape.com/catalogue/page-3.html"
BODY = b"<html><body>a light in the attic</body></html>"


def meta(**overrides: object) -> FetchMeta:
    values: dict[str, object] = {
        "final_url": URL_A,
        "status": 200,
        "fetched_at": "2026-11-24T10:00:00+00:00",
        "fetch_run_id": "run-1",
        "fetcher": "httpx",
        "headers": {
            "Content-Type": "text/html; charset=utf-8",
            "ETag": '"abc"',
            "Set-Cookie": "session=secret",
            "Server": "nginx",
        },
    }
    values.update(overrides)
    return FetchMeta(**values)  # type: ignore[arg-type]


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def cache(tmp_path: Path) -> PageCache:
    return PageCache(tmp_path / "cache")


def object_path(cache: PageCache, digest: str) -> Path:
    return cache.root / "objects" / digest[:2] / f"{digest}.gz"


def entry_path(cache: PageCache, url: str) -> Path:
    key = sha(url.encode())
    return cache.root / "entries" / key[:2] / f"{key}.json"


def test_put_get_roundtrip(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta())
    assert entry.url == URL_A
    assert entry.status == 200
    assert entry.content_sha256 == sha(BODY)
    assert entry.content_type == "text/html; charset=utf-8"
    assert entry.fetcher == "httpx"
    assert cache.get_entry(URL_A) == entry
    assert cache.has(URL_A)
    assert cache.read_body(entry) == BODY


def test_layout_matches_the_documented_paths(cache: PageCache) -> None:
    cache.put(URL_A, BODY, meta())
    obj = object_path(cache, sha(BODY))
    assert gzip.decompress(obj.read_bytes()) == BODY
    record = json.loads(entry_path(cache, URL_A).read_text())
    assert record["content_sha256"] == sha(BODY)
    assert record["fetched_at"] == "2026-11-24T10:00:00+00:00"


def test_only_allow_listed_headers_are_kept_lower_cased_and_sorted(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta(headers={"Retry-After": "5", "etag": "x", "X-A": "y"}))
    assert entry.headers == (("etag", "x"), ("retry-after", "5"))
    assert entry.content_type is None


def test_repeated_header_names_are_merged(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta(headers={"ETag": "a", "etag": "b"}))
    assert entry.headers == (("etag", "a, b"),)


def test_missing_url_is_absent(cache: PageCache) -> None:
    assert cache.get_entry(URL_A) is None
    assert not cache.has(URL_A)
    assert list(cache.iter_entries()) == []


def test_non_canonical_url_is_rejected(cache: PageCache) -> None:
    with pytest.raises(ValueError, match="canonical"):
        cache.put("HTTPS://Books.ToScrape.com/x#frag", BODY, meta())
    with pytest.raises(ValueError, match="canonical"):
        cache.put("not a url", BODY, meta())


@pytest.mark.parametrize(
    "overrides",
    [
        {"fetched_at": "yesterday"},
        {"fetched_at": "2026-11-24T10:00:00"},  # naive: no offset
        {"status": 42},
        {"fetcher": "curl"},
        {"fetch_run_id": ""},
    ],
)
def test_invalid_meta_is_rejected_before_anything_is_written(
    cache: PageCache, overrides: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match="validation error"):
        cache.put(URL_A, BODY, meta(**overrides))
    assert not cache.root.exists()


def test_identical_bodies_share_one_object(cache: PageCache) -> None:
    a = cache.put(URL_A, BODY, meta())
    b = cache.put(URL_B, BODY, meta(final_url=URL_B))
    assert a.content_sha256 == b.content_sha256
    objects = list((cache.root / "objects").rglob("*.gz"))
    assert objects == [object_path(cache, sha(BODY))]
    scan = cache.scan()
    assert (scan.entries, scan.objects) == (2, 1)


def test_dedup_skips_rewriting_an_intact_object(
    cache: PageCache, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache.put(URL_A, BODY, meta())
    written: list[Path] = []
    real = atomic_write_bytes

    def spy(path: Path, data: bytes) -> None:
        written.append(path)
        real(path, data)

    monkeypatch.setattr(f"{cache_module.__name__}.atomic_write_bytes", spy)
    cache.put(URL_B, BODY, meta())
    assert written == [entry_path(cache, URL_B)]


def test_object_is_written_before_entry(cache: PageCache, monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    real = atomic_write_bytes

    def spy(path: Path, data: bytes) -> None:
        order.append(path.parent.parent.name)
        real(path, data)

    monkeypatch.setattr(f"{cache_module.__name__}.atomic_write_bytes", spy)
    cache.put(URL_A, BODY, meta())
    assert order == ["objects", "entries"]


def test_crash_between_object_and_entry_leaves_only_a_harmless_orphan(
    cache: PageCache, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = atomic_write_bytes

    def crash_on_entry(path: Path, data: bytes) -> None:
        if path.suffix == ".json":
            raise KeyboardInterrupt  # the process dies between the two writes
        real(path, data)

    monkeypatch.setattr(f"{cache_module.__name__}.atomic_write_bytes", crash_on_entry)
    with pytest.raises(KeyboardInterrupt):
        cache.put(URL_A, BODY, meta())
    monkeypatch.undo()

    assert cache.get_entry(URL_A) is None  # the URL simply looks unfetched
    scan = cache.scan()
    assert scan.healthy
    assert scan.dangling_entries == ()
    assert scan.orphan_objects == (object_path(cache, sha(BODY)),)


def test_refetch_replaces_entry_and_old_body_becomes_orphan(cache: PageCache) -> None:
    cache.put(URL_A, BODY, meta())
    newer = cache.put(URL_A, b"<html>v2</html>", meta(fetch_run_id="run-2"))
    assert cache.get_entry(URL_A) == newer
    assert cache.read_body(newer) == b"<html>v2</html>"
    assert cache.scan().orphan_objects == (object_path(cache, sha(BODY)),)


def test_corrupt_object_is_detected_on_read(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta())
    object_path(cache, entry.content_sha256).write_bytes(gzip_bytes(b"tampered"))
    with pytest.raises(CorruptCacheError, match="hashes to"):
        cache.read_body(entry)


def test_garbage_object_is_detected_on_read(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta())
    obj = object_path(cache, entry.content_sha256)
    obj.write_bytes(obj.read_bytes()[:10])  # truncated gzip
    with pytest.raises(CorruptCacheError, match="unreadable"):
        cache.read_body(entry)
    obj.write_bytes(b"not gzip at all")
    with pytest.raises(CorruptCacheError, match="unreadable"):
        cache.read_body(entry)
    flipped = bytearray(gzip_bytes(BODY))
    flipped[10] ^= 0xFF  # first byte of the deflate stream: zlib.error, not an OSError
    obj.write_bytes(bytes(flipped))
    with pytest.raises(CorruptCacheError, match="unreadable"):
        cache.read_body(entry)
    assert cache.scan(verify_objects=True).corrupt_objects == (obj,)


def test_missing_object_is_reported_as_corrupt_and_dangling(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta())
    object_path(cache, entry.content_sha256).unlink()
    with pytest.raises(CorruptCacheError, match="missing"):
        cache.read_body(entry)
    scan = cache.scan()
    assert scan.dangling_entries == (URL_A,)
    assert not scan.healthy


def test_put_heals_a_damaged_object(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta())
    object_path(cache, entry.content_sha256).write_bytes(b"rot")
    cache.put(URL_A, BODY, meta())
    assert cache.read_body(entry) == BODY


@pytest.mark.parametrize(
    "damage",
    [
        lambda raw: raw[: len(raw) // 2],  # truncated
        lambda _raw: b"\x00\xff garbage",  # not JSON, not UTF-8
        lambda _raw: b"{}",  # JSON, wrong shape
        lambda raw: raw.replace(b'"status":200', b'"status":"200"'),  # strict: no coercion
        lambda raw: raw.replace(b"}\n", b',"extra":1}\n'),  # unknown field
    ],
)
def test_damaged_entry_is_absent_and_reported_by_scan(cache: PageCache, damage: object) -> None:
    cache.put(URL_A, BODY, meta())
    cache.put(URL_B, b"other", meta())
    path = entry_path(cache, URL_A)
    path.write_bytes(damage(path.read_bytes()))  # type: ignore[operator]

    assert cache.get_entry(URL_A) is None
    assert not cache.has(URL_A)
    assert [e.url for e in cache.iter_entries()] == [URL_B]
    scan = cache.scan()
    assert scan.corrupt_entries == (path,)
    assert scan.entries == 1
    assert not scan.healthy
    # The damaged entry's body is now unreferenced, so it is reported as an orphan.
    assert scan.orphan_objects == (object_path(cache, sha(BODY)),)


def test_entry_misfiled_under_another_urls_name_is_rejected(cache: PageCache) -> None:
    cache.put(URL_A, BODY, meta())
    copied = entry_path(cache, URL_B)
    copied.parent.mkdir(parents=True, exist_ok=True)
    copied.write_bytes(entry_path(cache, URL_A).read_bytes())
    assert cache.get_entry(URL_B) is None
    assert cache.scan().corrupt_entries == (copied,)


def test_unreadable_entry_path_is_absent(cache: PageCache) -> None:
    path = entry_path(cache, URL_A)
    path.mkdir(parents=True)  # a directory where a file should be
    assert cache.get_entry(URL_A) is None


@pytest.mark.parametrize(
    "field_value",
    [
        {"content_sha256": "ABC"},
        {"headers": [["server", "nginx"]]},
        {"headers": [["etag", "b"], ["content-type", "a"]]},
    ],
)
def test_entry_model_validation(field_value: dict[str, object]) -> None:
    record: dict[str, object] = {
        "url": URL_A,
        "final_url": URL_A,
        "status": 200,
        "content_type": None,
        "content_sha256": sha(BODY),
        "fetched_at": "2026-11-24T10:00:00Z",
        "fetch_run_id": "r",
        "fetcher": "playwright",
        "headers": [],
    }
    assert CacheEntry.model_validate_json(json.dumps(record)).fetcher == "playwright"
    record.update(field_value)
    with pytest.raises(ValueError, match="validation error"):
        CacheEntry.model_validate_json(json.dumps(record))


def test_entry_json_is_byte_stable(cache: PageCache, tmp_path: Path) -> None:
    other = PageCache(tmp_path / "other")
    cache.put(URL_A, BODY, meta(headers={"ETag": "1", "Content-Type": "text/html"}))
    other.put(URL_A, BODY, meta(headers={"Content-Type": "text/html", "ETag": "1"}))
    assert entry_path(cache, URL_A).read_bytes() == entry_path(other, URL_A).read_bytes()


def test_iter_entries_is_sorted_by_url(cache: PageCache) -> None:
    urls = [f"https://quotes.toscrape.com/page/{n}/" for n in (9, 1, 5, 10, 3)]
    for n, url in enumerate(urls):
        cache.put(url, f"body {n}".encode(), meta(final_url=url))
    assert [e.url for e in cache.iter_entries()] == sorted(urls)


def test_manifest_depends_on_content_only(cache: PageCache, tmp_path: Path) -> None:
    empty = cache.manifest_sha256()
    cache.put(URL_A, BODY, meta())
    one = cache.manifest_sha256()
    assert one != empty
    # Same content under different metadata (run id, time, fetcher): same manifest.
    other = PageCache(tmp_path / "other")
    other.put(URL_A, BODY, meta(fetch_run_id="x", fetched_at="2020-01-01T00:00:00Z"))
    assert other.manifest_sha256() == one
    # A changed body changes it.
    cache.put(URL_A, b"changed", meta())
    assert cache.manifest_sha256() != one


def test_manifest_encoding_is_documented_and_unambiguous(cache: PageCache) -> None:
    cache.put(URL_B, b"b", meta())
    cache.put(URL_A, BODY, meta())
    expected = hashlib.sha256(
        b"page-cache-manifest/v1\n"
        + json.dumps([URL_A, sha(BODY)], separators=(",", ":")).encode()
        + b"\n"
        + json.dumps([URL_B, sha(b"b")], separators=(",", ":")).encode()
        + b"\n"
    ).hexdigest()
    assert cache.manifest_sha256() == expected


def test_manifest_does_not_depend_on_listing_order(
    cache: PageCache, monkeypatch: pytest.MonkeyPatch
) -> None:
    for n in range(20):
        url = f"https://quotes.toscrape.com/page/{n}/"
        cache.put(url, f"body {n}".encode(), meta())
    expected = cache.manifest_sha256()
    real_files = cache_module._files

    def reversed_files(root: Path) -> list[Path]:
        return list(reversed(real_files(root)))

    monkeypatch.setattr(cache_module, "_files", reversed_files)
    assert cache.manifest_sha256() == expected


def test_gzip_objects_are_deterministic() -> None:
    first = gzip_bytes(BODY)
    assert first == gzip_bytes(BODY)
    # RFC 1952 header: magic, deflate, no flags (no file name), mtime 0, XFL 2 (max), OS 255.
    assert first[:10] == b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff"
    assert gzip.decompress(first) == BODY


def test_scan_reports_temp_files_and_gc_removes_waste_only(cache: PageCache) -> None:
    keep = cache.put(URL_A, BODY, meta())
    cache.put(URL_B, b"old body", meta())
    cache.put(URL_B, b"new body", meta())  # "old body" becomes an orphan
    stale_obj = object_path(cache, sha(BODY)).parent / ".x.gz.0123456789abcdef.tmp"
    stale_obj.write_bytes(b"partial")
    stale_entry = entry_path(cache, URL_A).parent / ".y.json.fedcba9876543210.tmp"
    stale_entry.write_bytes(b"partial")
    unrelated = cache.root / "objects" / "zz" / "README"
    unrelated.parent.mkdir()
    unrelated.write_text("not an object")

    report = cache.gc()
    assert report.orphan_objects == (object_path(cache, sha(b"old body")),)
    assert set(report.temp_files) == {stale_obj, stale_entry}
    assert report.healthy

    after = cache.scan()
    assert after.orphan_objects == ()
    assert after.temp_files == ()
    assert (after.entries, after.objects) == (2, 2)
    assert cache.read_body(keep) == BODY
    assert unrelated.exists()  # gc only deletes what it recognises


def test_object_in_the_wrong_shard_is_not_counted(cache: PageCache) -> None:
    digest = sha(BODY)
    wrong = cache.root / "objects" / "00" / f"{digest}.gz"
    wrong.parent.mkdir(parents=True)
    wrong.write_bytes(gzip_bytes(BODY))
    assert cache.scan().objects == 0


def test_scan_can_verify_object_integrity(cache: PageCache) -> None:
    entry = cache.put(URL_A, BODY, meta())
    cache.put(URL_B, b"fine", meta())
    assert cache.scan(verify_objects=True).corrupt_objects == ()
    bad = object_path(cache, entry.content_sha256)
    bad.write_bytes(gzip_bytes(b"rot"))
    assert cache.scan().corrupt_objects == ()  # not checked by default
    scan = cache.scan(verify_objects=True)
    assert scan.corrupt_objects == (bad,)
    assert not scan.healthy


def test_scan_of_a_missing_cache_is_empty(cache: PageCache) -> None:
    scan = cache.scan()
    assert (scan.entries, scan.objects, scan.healthy) == (0, 0, True)
