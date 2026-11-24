# 0002. Raw pages live in a content-addressed cache

## Context

The cache is the input to every later stage (ADR 0001), so it has to survive crashes, detect
corruption, and say precisely what it contains.

## Decision

- Bodies are stored once per distinct content, named by the sha256 of the raw bytes:
  `cache/objects/<sha[:2]>/<sha>.gz`. Two URLs serving the same bytes share one object.
- Each URL has a small JSON entry (`cache/entries/...`) with the final URL, status, an allow-list of
  headers, the content hash, the fetch run id and the fetcher used.
- **Write order is the crash-safety argument:** object first, then entry, each with an atomic
  write (temp file in the same directory, fsync, rename, fsync the directory). An entry therefore
  never points at a missing object. A crash between the two writes leaves an orphan object, which
  is harmless and reported by `scan()`.
- Every read re-hashes the body. A mismatch, a truncated gzip stream or a missing object raises
  `CorruptCacheError`; the page is treated as absent and fetched again.
- gzip is written deterministically (`mtime=0`, no file name, fixed level), so the same body always
  produces the same object bytes.
- `manifest_sha256()` hashes the sorted `(url, content hash)` pairs. It depends only on the cache's
  content, never on file times or directory order, which is what lets parse derive a stable run id
  (ADR 0003).

## Consequences

- Corruption is detected rather than parsed. A flipped byte on disk cannot become a wrong row.
- Resumability gets a cheap, trustworthy test: "is this URL's entry present and does its object
  verify?" (ADR 0008).
- Garbage collection of orphans must not run concurrently with a fetch. It is a maintenance step
  between runs.

## Alternatives considered

- **Files named by URL.** No dedup, no integrity check, and URL-to-path escaping is its own
  bug source.
- **Bodies inside the database.** One file to back up, but a large blob store in DuckDB is the
  wrong tool, and a database crash would take the pages with it.
