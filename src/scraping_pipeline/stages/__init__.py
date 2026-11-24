"""The five pipeline stages, each a typed, re-runnable Python function: the CLI is a thin shell over them.

``discover -> fetch -> parse -> validate -> export``. Each stage reads what the previous one left on
disk (the state database, the page cache, JSONL files) and writes its own outputs atomically, so any
stage can be re-run on its own, after a crash, or with new code, without re-running the ones before.

Why five stages and not one crawl-and-save loop:

* **fetch is the only stage that touches the network**, and the only one that is inherently
  non-deterministic (servers answer differently from one minute to the next). Keeping it apart
  means everything downstream can be re-run for free, offline, as often as a parser changes.
* **parse reads only the cache**, so the same cache always produces byte-identical output, and a
  parser fix is applied by re-parsing, never by re-crawling a site that has already served us.
* **validate quarantines instead of dropping**, so a bad row shows up with its reason rather than
  silently shrinking the dataset; and **export** only ever publishes rows that passed a contract.

Every stage records a run (``start_run`` / ``finish_run``) in the state database with the settings
it ran with and a summary, which is what ``pipeline status`` and the run report read.
"""

from scraping_pipeline.stages.discover import DiscoverResult, run_discover
from scraping_pipeline.stages.export import ExportResult, run_export
from scraping_pipeline.stages.fetch import FetchStageResult, run_fetch
from scraping_pipeline.stages.parse import ParseStageResult, run_parse
from scraping_pipeline.stages.validate import ValidateStageResult, run_validate

__all__ = [
    "DiscoverResult",
    "ExportResult",
    "FetchStageResult",
    "ParseStageResult",
    "ValidateStageResult",
    "run_discover",
    "run_export",
    "run_fetch",
    "run_parse",
    "run_validate",
]
