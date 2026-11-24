# 0008. DuckDB holds the run state; the frontier is the source of truth for progress

## Context

"Resumable" has to mean: after a crash, a `kill -9` or a Ctrl-C at any moment, nothing that was
completed is lost, and re-running fetches only what is missing. An in-memory queue loses
everything. A log file is hard to query and easy to corrupt.

## Decision

- One DuckDB file per data directory holds four tables: `runs`, `frontier` (one row per URL with
  status, attempts, next attempt time, last response class and reason), `fetch_attempts` (every
  attempt, never overwritten) and `domain_events` (robots.txt, breaker, stopped domains). The
  schema is versioned with forward-only migrations, and a newer schema is refused.
- **Discovery is insert-only.** Adding a URL that already exists never resets it, so re-running
  `discover` is idempotent.
- **Each fetch result is one transaction:** the attempt row and the frontier update commit
  together. The update is optimistic (`WHERE url = ? AND attempts = ? AND status = ?`), so a stale
  copy cannot overwrite newer state. Status transitions are checked against the same table the
  core uses.
- **Cache before state.** A successful page is written to the cache (ADR 0002) *before* its
  frontier row is updated. A crash between the two leaves a page the state doesn't know about.
  At the start of each fetch, `reconcile_with_cache` marks such pages done, without a request,
  when their cached status is 2xx and the body verifies. It records a "recovered from cache"
  attempt so the lineage stays honest. Anything else is fetched again.
- DuckDB allows one writing process per file. A second process gets a clear
  "already in use" error instead of a corrupted run.

## Consequences

- The resumability test can kill the fetch stage midway, re-run it, and assert from the server's
  request log that no completed page was requested twice.
- The run report is built in Python from a few `StateStore` reads (the frontier, the runs and the
  domain events) plus the parse and validation files; no ad-hoc SQL outside the store.
- Re-queueing failed or stopped URLs is an explicit operator action (`--retry-failed`). A default
  run never re-fetches a URL that reached a final state.

## Alternatives considered

- **SQLite.** Equally valid for the state. DuckDB was chosen because the same engine reads the
  Parquet deliverables and answers ad-hoc questions about a run in SQL. It has one fewer dependency than a
  SQLite-plus-pandas setup.
- **Files only (JSON per URL).** No transactions, so the two-write problem would be everywhere.
