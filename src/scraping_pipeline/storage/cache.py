"""Content-addressed cache of fetched pages: the only input the offline parse stage ever reads.

Layout under the cache root::

    objects/<sha[:2]>/<sha>.gz              gzip of the raw body, sha = sha256(raw body)
    entries/<h[:2]>/<h>.json                one record per URL, h = sha256(url)

**Why content addressing.** Many URLs serve the same bytes (a listing page reached by two paths,
an unchanged page fetched by two runs), and keying the body by its own hash stores it once. It
also makes integrity checkable for free: :meth:`PageCache.read_body` re-hashes what it reads and
raises :class:`CorruptCacheError` when the bytes no longer match their name, so bit rot or a
hand-edited file can never flow silently into a deliverable.

**Why this write order.** :meth:`PageCache.put` writes the object first and the entry second, each
with :func:`~scraping_pipeline.storage.atomic.atomic_write_bytes`. A crash between the two leaves an
*orphan object* (bytes nobody points at), which is harmless: :meth:`PageCache.scan` reports it and
:meth:`PageCache.gc` deletes it. The reverse order could leave an entry pointing at a missing body,
and the URL would look fetched while its content is gone. So the invariant is: **an entry never
points at a missing object** (unless something outside this module deletes objects).

**Why the manifest.** :meth:`PageCache.manifest_sha256` hashes the sorted ``(url, content_sha256)``
pairs. The parse stage derives its run id from it, so two parses of the same cache share an id
whatever the directory listing order, file mtimes or insertion history, and any change to any page
changes the id.

**Determinism of objects.** Gzip normally stamps the current time and the source file name into its
header. Objects are written with ``mtime=0``, no file name and a fixed compression level, so the
same body always produces the same object bytes for a given zlib build. (Identity never depends on
that: the object's *name* is the hash of the decompressed body.)

**Damage tolerance.** An entry file that is truncated, not JSON, or fails validation is treated as
absent (the URL will simply be fetched again) and reported by :meth:`PageCache.scan`; it never
crashes a run. Clocks are not read here: ``fetched_at`` is supplied by the caller.

``gc`` must not run concurrently with ``put``: it could delete an object whose entry is about to be
written. Run it as a maintenance step between runs.
"""

import gzip
import hashlib
import io
import json
import re
import zlib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from scraping_pipeline.core.urls import InvalidUrlError, canonicalize
from scraping_pipeline.storage.atomic import atomic_write_bytes, is_temp_name

__all__ = ["CacheEntry", "CacheError", "CacheScan", "CorruptCacheError", "FetchMeta", "PageCache"]

#: Response headers worth keeping: enough to revalidate (etag, last-modified), to explain a
#: back-off (retry-after) and to pick a decoder (content-type). Everything else is noise or PII.
HEADER_ALLOWLIST: frozenset[str] = frozenset(
    {"content-type", "etag", "last-modified", "retry-after"}
)

#: Fixed gzip level. Changing it changes object bytes (not object names); treat it as a format
#: constant.
GZIP_LEVEL = 9

_MANIFEST_VERSION = b"page-cache-manifest/v1\n"
_LINEAGE_MANIFEST_VERSION = b"page-cache-manifest/v1+fetch-run-id\n"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OBJECT_NAME = re.compile(r"^[0-9a-f]{64}\.gz$")
_ENTRY_NAME = re.compile(r"^[0-9a-f]{64}\.json$")

Fetcher = Literal["httpx", "playwright"]


class CacheError(Exception):
    """Base class for page-cache errors."""


class CorruptCacheError(CacheError):
    """A cached body is missing, unreadable, or does not hash to the sha256 its entry records."""


@dataclass(frozen=True, slots=True)
class FetchMeta:
    """What the fetcher knows about a response, passed to :meth:`PageCache.put`.

    ``headers`` may be the full response header map; only :data:`HEADER_ALLOWLIST` is kept.
    ``fetched_at`` is an ISO 8601 timestamp with a UTC offset, taken from the caller's clock.
    """

    final_url: str
    status: int
    fetched_at: str
    fetch_run_id: str
    fetcher: Fetcher
    headers: Mapping[str, str] = field(default_factory=dict)


class CacheEntry(BaseModel):
    """The per-URL record. Validated strictly on read: a damaged file is rejected, not guessed at.

    ``headers`` is a sorted tuple of ``(lower-case name, value)`` pairs, which keeps the model
    hashable and its JSON byte-stable.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    url: str = Field(min_length=1)
    final_url: str = Field(min_length=1)
    status: int = Field(ge=100, le=599)
    content_type: str | None
    content_sha256: str
    fetched_at: str
    fetch_run_id: str = Field(min_length=1)
    fetcher: Fetcher
    headers: tuple[tuple[str, str], ...]

    @field_validator("content_sha256")
    @classmethod
    def _hex_digest(cls, value: str) -> str:
        if not _SHA256.match(value):
            raise ValueError("content_sha256 must be 64 lower-case hex characters")
        return value

    @field_validator("fetched_at")
    @classmethod
    def _aware_iso_timestamp(cls, value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("fetched_at must be an ISO 8601 timestamp") from exc
        if parsed.tzinfo is None:
            raise ValueError("fetched_at must carry a UTC offset")
        return value

    @field_validator("headers")
    @classmethod
    def _allowed_sorted_headers(
        cls, value: tuple[tuple[str, str], ...]
    ) -> tuple[tuple[str, str], ...]:
        names = [name for name, _ in value]
        if names != sorted(set(names)) or not set(names) <= HEADER_ALLOWLIST:
            raise ValueError("headers must be unique, sorted, lower-case allow-listed names")
        return value


@dataclass(frozen=True, slots=True)
class CacheScan:
    """A health report of the cache. Paths are absolute; lists are sorted for stable output."""

    entries: int
    objects: int
    corrupt_entries: tuple[Path, ...] = ()
    dangling_entries: tuple[str, ...] = ()  # URLs whose object is missing
    orphan_objects: tuple[Path, ...] = ()
    corrupt_objects: tuple[Path, ...] = ()  # only filled by scan(verify_objects=True)
    temp_files: tuple[Path, ...] = ()  # left by a writer that was killed mid-write

    @property
    def healthy(self) -> bool:
        """No damage that loses data. Orphans and temp files are waste, not damage."""
        return not (self.corrupt_entries or self.dangling_entries or self.corrupt_objects)


class PageCache:
    """The page cache rooted at ``root`` (``Settings.cache_dir`` in a run)."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._objects = self.root / "objects"
        self._entries = self.root / "entries"

    # --- writes ------------------------------------------------------------------------------

    def put(self, url: str, body: bytes, meta: FetchMeta) -> CacheEntry:
        """Store ``body`` as the current content of ``url`` and return the entry written.

        ``url`` must already be canonical (:func:`scraping_pipeline.core.urls.canonicalize`), so one
        page has one entry. The object is written before the entry: see the module docstring.
        """
        if _canonical_or_none(url) != url:
            raise ValueError(f"url must be canonical: {url!r}")
        digest = hashlib.sha256(body).hexdigest()
        entry = CacheEntry(
            url=url,
            final_url=meta.final_url,
            status=meta.status,
            content_type=_header_subset(meta.headers).get("content-type"),
            content_sha256=digest,
            fetched_at=meta.fetched_at,
            fetch_run_id=meta.fetch_run_id,
            fetcher=meta.fetcher,
            headers=tuple(sorted(_header_subset(meta.headers).items())),
        )
        # 1. The object. Skip the write when an intact copy is already there (deduplication);
        #    rewrite it when the existing copy is damaged, so a re-fetch heals the cache.
        object_path = self._object_path(digest)
        if not self._object_intact(object_path, digest):
            atomic_write_bytes(object_path, gzip_bytes(body))
        # 2. The entry, only once its object is durable.
        atomic_write_bytes(self._entry_path(url), _entry_json(entry))
        return entry

    def gc(self) -> CacheScan:
        """Delete orphan objects and stale temp files; return the scan taken before deleting.

        Never run concurrently with :meth:`put` (see the module docstring).
        """
        report = self.scan()
        for path in (*report.orphan_objects, *report.temp_files):
            path.unlink(missing_ok=True)
        return report

    # --- reads -------------------------------------------------------------------------------

    def get_entry(self, url: str) -> CacheEntry | None:
        """The entry for ``url``, or ``None`` when absent or damaged."""
        return self._load_entry(self._entry_path(url))

    def has(self, url: str) -> bool:
        """True when ``url`` has a valid entry."""
        return self.get_entry(url) is not None

    def read_body(self, entry: CacheEntry) -> bytes:
        """The raw body of ``entry``, verified against its sha256.

        Raises :class:`CorruptCacheError` when the object is missing, not valid gzip, or hashes to
        anything other than ``entry.content_sha256``.
        """
        path = self._object_path(entry.content_sha256)
        try:
            body = gzip.decompress(path.read_bytes())
        except FileNotFoundError as exc:
            raise CorruptCacheError(f"object missing for {entry.url}: {path}") from exc
        except (OSError, EOFError, zlib.error) as exc:  # bad header / truncated / bad deflate
            raise CorruptCacheError(f"object unreadable for {entry.url}: {path}: {exc}") from exc
        actual = hashlib.sha256(body).hexdigest()
        if actual != entry.content_sha256:
            raise CorruptCacheError(
                f"object for {entry.url} hashes to {actual}, expected {entry.content_sha256}"
            )
        return body

    def iter_entries(self) -> Iterator[CacheEntry]:
        """Every valid entry, sorted by URL (independent of directory listing order)."""
        entries = [entry for _, entry in self._walk_entries() if entry is not None]
        return iter(sorted(entries, key=lambda entry: entry.url))

    def manifest_sha256(
        self, urls: Iterable[str] | None = None, *, with_fetch_run_ids: bool = False
    ) -> str:
        """sha256 over the sorted ``(url, content_sha256)`` pairs of all valid entries.

        Each pair is encoded as a JSON array on its own line after a version header, so the
        encoding is unambiguous whatever characters a URL contains.

        ``urls`` restricts the manifest to those URLs' valid entries (URLs without one are simply
        absent), so one site's parse run id does not change when another site's pages are fetched
        into the same cache. The encoding is the same either way: the manifest of a subset equals
        the full manifest of a cache that holds only that subset. Order and repeats in ``urls`` do
        not matter.

        ``with_fetch_run_ids=True`` hashes ``(url, content_sha256, fetch_run_id)`` triples under a
        different version header instead. That is the identity the parse stage needs: every row it
        writes carries the ``fetch_run_id`` of its page, so two caches with the same bodies from
        different fetch runs produce different bytes and must not share a parse run id. The
        default (content only) is unchanged.
        """
        if with_fetch_run_ids:
            digest = hashlib.sha256(_LINEAGE_MANIFEST_VERSION)
        else:
            digest = hashlib.sha256(_MANIFEST_VERSION)
        if urls is None:
            selected: Iterable[CacheEntry] = self.iter_entries()
        else:
            selected = (e for url in set(urls) if (e := self.get_entry(url)) is not None)
        rows = sorted(
            [entry.url, entry.content_sha256, *([entry.fetch_run_id] if with_fetch_run_ids else [])]
            for entry in selected
        )
        for row in rows:
            line = json.dumps(row, ensure_ascii=True, separators=(",", ":"))
            digest.update(line.encode("ascii") + b"\n")
        return digest.hexdigest()

    def scan(self, *, verify_objects: bool = False) -> CacheScan:
        """Report entries, objects and damage without changing anything.

        ``verify_objects=True`` also decompresses and re-hashes every object (slow on big caches).
        """
        valid: list[CacheEntry] = []
        corrupt_entries: list[Path] = []
        for path, entry in self._walk_entries():
            if entry is None:
                corrupt_entries.append(path)
            else:
                valid.append(entry)

        objects: dict[str, Path] = {}
        temp_files = list(self._walk_temp_files(self._entries))
        for path in _files(self._objects):
            if is_temp_name(path.name):
                temp_files.append(path)
            elif _OBJECT_NAME.match(path.name) and path.parent.name == path.name[:2]:
                objects[path.name.removesuffix(".gz")] = path

        referenced = {entry.content_sha256 for entry in valid}
        corrupt_objects = []
        if verify_objects:
            corrupt_objects = [
                path for sha, path in objects.items() if not self._object_intact(path, sha)
            ]
        return CacheScan(
            entries=len(valid),
            objects=len(objects),
            corrupt_entries=tuple(sorted(corrupt_entries)),
            dangling_entries=tuple(
                sorted(entry.url for entry in valid if entry.content_sha256 not in objects)
            ),
            orphan_objects=tuple(
                sorted(path for sha, path in objects.items() if sha not in referenced)
            ),
            corrupt_objects=tuple(sorted(corrupt_objects)),
            temp_files=tuple(sorted(temp_files)),
        )

    # --- internals ---------------------------------------------------------------------------

    def _object_path(self, digest: str) -> Path:
        return self._objects / digest[:2] / f"{digest}.gz"

    def _entry_path(self, url: str) -> Path:
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self._entries / key[:2] / f"{key}.json"

    def _object_intact(self, path: Path, digest: str) -> bool:
        try:
            body = gzip.decompress(path.read_bytes())
        except (OSError, EOFError, zlib.error):
            return False
        return hashlib.sha256(body).hexdigest() == digest

    def _load_entry(self, path: Path) -> CacheEntry | None:
        try:
            raw = path.read_bytes()
        except OSError:
            return None
        try:
            entry = CacheEntry.model_validate_json(raw)
        except ValidationError:
            return None
        # An entry must live at the path its own URL hashes to: a misfiled or copied record is
        # rejected, so get_entry(url) can only ever return an entry *for* that url.
        if self._entry_path(entry.url) != path:
            return None
        return entry

    def _walk_entries(self) -> Iterator[tuple[Path, CacheEntry | None]]:
        """Each entry-shaped file with its parsed entry, or ``None`` when it is damaged."""
        for path in _files(self._entries):
            if _ENTRY_NAME.match(path.name):
                yield path, self._load_entry(path)

    def _walk_temp_files(self, root: Path) -> Iterator[Path]:
        return (path for path in _files(root) if is_temp_name(path.name))


def gzip_bytes(body: bytes) -> bytes:
    """Deterministic gzip: ``mtime=0``, no file name, level :data:`GZIP_LEVEL`."""
    buffer = io.BytesIO()
    with gzip.GzipFile(
        filename="", mode="wb", compresslevel=GZIP_LEVEL, fileobj=buffer, mtime=0
    ) as stream:
        stream.write(body)
    return buffer.getvalue()


def _files(root: Path) -> list[Path]:
    """Regular files one shard level below ``root``, sorted. Missing root means no files."""
    if not root.is_dir():
        return []
    return sorted(path for path in root.glob("*/*") if path.is_file())


def _header_subset(headers: Mapping[str, str]) -> dict[str, str]:
    kept: dict[str, str] = {}
    for name, value in headers.items():
        key = name.lower()
        if key in HEADER_ALLOWLIST:
            kept[key] = f"{kept[key]}, {value}" if key in kept else value
    return kept


def _entry_json(entry: CacheEntry) -> bytes:
    return entry.model_dump_json().encode("utf-8") + b"\n"


def _canonical_or_none(url: str) -> str | None:
    try:
        return canonicalize(url)
    except InvalidUrlError:
        return None
