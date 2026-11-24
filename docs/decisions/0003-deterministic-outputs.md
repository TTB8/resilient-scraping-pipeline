# 0003. Same cache, byte-identical outputs; the parse run id is derived, not random

## Context

An analyst who receives a file needs to be able to ask "is this the same data as last week's?" and
get an answer from a checksum, not from a diff tool. A pipeline whose output changes when nothing
upstream changed hides real changes in noise and makes bugs impossible to bisect.

The brief also asks for a run id on every record, for lineage. A random run id on every row would
make every parse produce different bytes, which contradicts determinism.

## Decision

- `parse`, `validate` and `export` are pure functions of the cache. The same cache gives
  byte-identical parsed files, valid-row files, quarantine files, Parquet and CSV, in any process
  and under any hash seed.
- The run id written on each record is **derived from its inputs**:
  `parse_run_id = sha256(manifest of that site's cache entries, parser version, contract version)`,
  truncated to 16 hex characters. The same inputs give the same id and the same bytes. A changed
  page, parser or contract gives a new id. The id still answers the lineage question ("which inputs
  and which code produced this row?"), and each row also carries the `fetch_run_id` and the
  content hash of the page it came from.
- Everything that varies by nature (start and finish times, durations, the fetch run's own random
  id) lives in the **run report**, which is explicitly outside the determinism guarantee.
- The writers remove each source of drift explicitly: rows are sorted by a unique key before
  writing (ties are an error, not an accident); Parquet writer settings are pinned (compression,
  format version, page version, dictionary, statistics, row-group size); CSV has a fixed column
  order, `\n` line endings, Decimal written without an exponent and ISO dates.

## Consequences

- The determinism test (parse twice, also in a fresh interpreter, and compare bytes) is meaningful
  and runs in CI.
- Byte identity holds within one locked environment (`uv.lock`). A different pyarrow or zlib build
  may write different bytes for the same data. That is why the lock file is committed.
- A run id is not a wall-clock fact: two runs on the same cache share it. That is the point.

## Alternatives considered

- **A random run id per parse.** Easy, and it breaks the guarantee above.
- **Determinism "modulo metadata", checked by comparing data frames.** Weaker. A frame comparison
  hides the column order, encoding and file-metadata drift that breaks consumers.
