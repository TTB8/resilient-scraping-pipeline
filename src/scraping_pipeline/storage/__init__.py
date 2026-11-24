"""Filesystem storage: crash-safe writes, the content-addressed page cache and deterministic outputs.

This package owns every byte the pipeline puts on disk:

- :mod:`.atomic` makes each write all-or-nothing, so a killed run never leaves a torn file;
- :mod:`.cache` stores fetched pages by the hash of their body, so the parse stage reads a cache
  whose identity (:meth:`PageCache.manifest_sha256`) depends only on its contents;
- :mod:`.outputs` writes Parquet and CSV deliverables that are byte-identical for identical rows;
- :mod:`.state` is the run-state database (frontier, attempts, domain events, runs) that makes
  ``fetch`` resumable.

Together they carry the pipeline's reproducibility promise: same cache, same output bytes.
"""

from scraping_pipeline.storage.atomic import atomic_path, atomic_write_bytes, atomic_write_text
from scraping_pipeline.storage.cache import (
    CacheEntry,
    CacheError,
    CacheScan,
    CorruptCacheError,
    FetchMeta,
    PageCache,
)
from scraping_pipeline.storage.outputs import SchemaMismatchError, write_csv, write_parquet
from scraping_pipeline.storage.state import (
    AttemptRecord,
    DomainEvent,
    DomainEventKind,
    RunError,
    RunRecord,
    RunStatus,
    SchemaVersionError,
    Stage,
    StaleEntryError,
    StateError,
    StateLockedError,
    StateStore,
    StoredAttempt,
)

__all__ = [
    "AttemptRecord",
    "CacheEntry",
    "CacheError",
    "CacheScan",
    "CorruptCacheError",
    "DomainEvent",
    "DomainEventKind",
    "FetchMeta",
    "PageCache",
    "RunError",
    "RunRecord",
    "RunStatus",
    "SchemaMismatchError",
    "SchemaVersionError",
    "Stage",
    "StaleEntryError",
    "StateError",
    "StateLockedError",
    "StateStore",
    "StoredAttempt",
    "atomic_path",
    "atomic_write_bytes",
    "atomic_write_text",
    "write_csv",
    "write_parquet",
]
