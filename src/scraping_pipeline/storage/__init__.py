"""Filesystem storage: crash-safe writes, the content-addressed page cache and deterministic outputs.

This package owns every byte the pipeline puts on disk outside the run-state database:

- :mod:`.atomic` makes each write all-or-nothing, so a killed run never leaves a torn file;
- :mod:`.cache` stores fetched pages by the hash of their body, so the parse stage reads a cache
  whose identity (:meth:`PageCache.manifest_sha256`) depends only on its contents;
- :mod:`.outputs` writes Parquet and CSV deliverables that are byte-identical for identical rows.

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

__all__ = [
    "CacheEntry",
    "CacheError",
    "CacheScan",
    "CorruptCacheError",
    "FetchMeta",
    "PageCache",
    "SchemaMismatchError",
    "atomic_path",
    "atomic_write_bytes",
    "atomic_write_text",
    "write_csv",
    "write_parquet",
]
