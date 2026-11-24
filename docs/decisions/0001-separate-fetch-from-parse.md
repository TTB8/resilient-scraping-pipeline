# 0001. Fetch and parse are separate stages; parse is offline

## Context

The usual scraper downloads a page and extracts fields in the same function. When the extraction
changes (a new field, a fixed selector) the only way to apply it is to download everything again.
That is slow, it is unkind to the site, and the second download is not the same data: prices,
stock and wording have moved on, so a "fix" silently mixes two snapshots.

## Decision

`fetch` stores the raw response and nothing else. `parse` reads only the cache. It has no fetcher,
no network client and no settings that could reach one. Adding a field means editing a parser and
re-running `parse`; the pages are not touched again.

## Consequences

- A parser change is cheap and repeatable, and it applies to exactly the snapshot that was fetched.
- Parsing can run anywhere the cache is: on a laptop, in CI, on another machine. It needs no
  network access and cannot be blocked.
- Disk is spent on raw HTML. On these sites a full crawl is a few tens of megabytes compressed. For
  bigger crawls, a retention policy is required. It is out of scope here.
- A JavaScript-rendered page is cached as the DOM the browser produced. Parse never runs a browser.

## Alternatives considered

- **Parse while fetching, store only records.** Smaller, simpler, and it gives up every property
  above.
- **Store records and raw pages both, parse inline.** Doubles the write path and still couples a
  parser bug to a crawl.
