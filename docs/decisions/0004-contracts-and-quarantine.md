# 0004. Versioned contracts; failing rows are quarantined and reported, never dropped

## Context

The most expensive scraper bug is the silent one. A selector stops matching, a price comes back as
`None`, the code skips the row or writes a blank, and three weeks later a dashboard is wrong.
Silently dropping rows that "look bad" turns a visible failure into an invisible one.

## Decision

- **Parsers are lenient.** They extract what they find into draft records whose fields may be
  missing or raw strings, add warnings, and never raise on odd HTML.
- **Contracts are strict.** Each dataset has a versioned contract (`books/1.0.0`,
  `quotes/1.0.0`): pydantic models with `extra="forbid"` and no silent coercion, plus dataset
  rules: uniqueness of keys, referential checks (every quote's author exists; every book's
  category was discovered), and URL, range and consistency rules (`in_stock` agrees with
  `stock_count`).
- **Every row ends up somewhere.** `validate` writes valid rows, and writes every failing row to a
  quarantine file with each failure: row key, field, rule, message and the offending value.
  "Valid plus quarantined equals input" is a property test.
- **The verdict is explicit.** A configurable `max_failure_rate`, default 0, turns the share of
  quarantined rows into PASS or FAIL. FAIL sets exit code 1, and every report is still written.
- **Versions follow semver.** Adding an optional field is a minor bump. Renaming, removing or
  tightening a rule is a major bump. The version is written on every row.

## Consequences

- A broken selector shows up as a failing run with a precise report, the same day.
- Consumers can rely on the deliverable's schema; anything that doesn't meet it is in a file they
  can inspect, not lost.
- Duplicates keep the first row in a canonical order and quarantine the rest, pointing at the kept
  row. The order is total, so the choice never depends on input order. A Hypothesis property
  caught an early version that did depend on it.

## Alternatives considered

- **Drop invalid rows and log a warning.** This is what most scrapers do. Logs are not read, so the
  problem stays invisible.
- **Fail the run on the first invalid row.** Safe but useless at scale: one odd page blocks the
  whole delivery, and the report covers only that first problem.
