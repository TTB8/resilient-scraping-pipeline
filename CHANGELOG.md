# Changelog

All notable changes to this project. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Data contracts have their own versions
(`books/1.0.0`, `quotes/1.0.0`), written on every row.

## [0.1.0] - 2026-11-24

### Added

- Five re-runnable stages, `discover`, `fetch`, `parse`, `validate` and `export`, plus `run` and
  `status` commands.
- Sites: books.toscrape.com, quotes.toscrape.com and its JavaScript-rendered variant
  (Playwright).
- A pure core:
  - URL canonicalization (RFC 3986);
  - frontier rules;
  - response classification;
  - retry policy with full jitter and `Retry-After`;
  - a per-domain circuit breaker and token-bucket rate limiter;
  - an RFC 9309 robots.txt policy;
  - lenient parsers, versioned contracts, and validation with quarantine.
- A polite async scheduler:
  - per-domain rate and concurrency limits;
  - same-site redirect following, with every hop checked;
  - one overall deadline per request;
  - domain stop on blocks, challenges and long `Retry-After` or `Crawl-delay`.
- Storage:
  - a content-addressed page cache with integrity checks;
  - a DuckDB run state with crash recovery;
  - deterministic Parquet and CSV writers.
- A run report with pages, requests, failures by class, stopped domains, parse coverage, the
  validation verdict and the deliverables.
- An offline demo (`make demo`), a Dockerfile, and example output from two live runs.
- Architecture decision records 0001-0009.
