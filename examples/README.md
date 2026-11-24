# Example output

Real deliverables from two small, polite runs against the scraping practice sites on 2026-11-24.
Both ran at the default one request per second with the project's own User-Agent.

| Folder | Command | Pages | Requests | Rows |
|---|---|---|---|---|
| [`books/`](books/) | `pipeline run --site books --max-depth 1` | 72 | 73 | 20 books, all valid |
| [`quotes/`](quotes/) | `pipeline run --site quotes` (the whole site) | 60 | 111 | 100 quotes and 50 authors, all valid |

Each folder holds what `export` writes:

- `<table>.parquet`: the typed deliverable (decimal prices, ISO dates, list-typed tags).
- `<table>.csv`: the same rows as plain text. It is a data-interchange file, so cells are written
  as is: open it in a spreadsheet only through a text import.
- `quarantine.csv`: every row that failed its contract, with the reason. It is empty here, header
  only.
- `run_report.md` and `run_report.json`: pages, requests, failures by class, stopped domains,
  parse coverage and the validation verdict.

Every row carries its lineage: `source_url`, `content_sha256` of the page it came from,
`fetch_run_id`, `parse_run_id`, `parser_version` and `contract_version`.

Two details worth noticing in `quotes/run_report.md`:

- **111 requests for 60 pages.** Every author link on that site redirects (`308`, to the same page
  with a trailing slash). Each hop is a real request, and each one waits for the rate limiter.
- **The run that produced this folder was the second one.** The first one quarantined all 100
  quotes, because the redirect also switches from `https` to `http` and the pipeline refused it as
  off-site. The contract caught it as `unknown_author` instead of shipping quotes without authors.
  The fix and its test are in the history (`fix(fetch): follow a same-host http<->https
  redirect...`).

The data files (`.parquet`, `.csv`, `quarantine.csv`) are deterministic: re-running `parse`,
`validate` and `export` on the same cache reproduces them byte for byte. The run reports carry
times, so they differ between runs.
