# Architecture decision records

Each record states the context, the decision, what it costs, and the alternatives we turned down.
They are short on purpose: a reader should be able to disagree with one in five minutes.

| # | Decision | Status |
|---|---|---|
| [0001](0001-separate-fetch-from-parse.md) | Fetch and parse are separate stages; parse is offline | Accepted |
| [0002](0002-content-addressed-cache.md) | Raw pages live in a content-addressed cache | Accepted |
| [0003](0003-deterministic-outputs.md) | Same cache, byte-identical outputs; the parse run id is derived, not random | Accepted |
| [0004](0004-contracts-and-quarantine.md) | Versioned contracts; failing rows are quarantined and reported, never dropped | Accepted |
| [0005](0005-detection-not-evasion.md) | Detect blocks and rate limits, then stop. Never evade them | Accepted |
| [0006](0006-politeness-defaults.md) | Polite defaults with hard ceilings; robots.txt cannot be switched off | Accepted |
| [0007](0007-pure-core-injected-time.md) | A pure core: clocks, randomness and I/O are injected | Accepted |
| [0008](0008-duckdb-run-state.md) | DuckDB holds the run state; the frontier is the source of truth for progress | Accepted |
| [0009](0009-site-confinement.md) | The crawl is confined to its site, redirects included | Accepted; superseded in part by 0010 |
| [0010](0010-same-host-scheme-switch.md) | A same-host http<->https redirect is followed, and every downgrade is counted | Accepted |

New record: copy the shape of an existing one, take the next number, and link it here. A record is
never edited to reverse itself; a later record supersedes it.
