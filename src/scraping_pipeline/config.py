"""Validated runtime settings, read from ``PIPELINE_*`` environment variables or a ``.env`` file.

Politeness lives here as *defaults that are safe without reading the docs*: one request per second
per domain, two in flight at most, an honest User-Agent (our own name/version, a contact point,
no borrowed browser or crawler identity). They can be tuned, but
not past hard ceilings, and robots.txt cannot be switched off at all: a pipeline that can be
configured into being rude eventually will be. See ``docs/decisions/0006-politeness-defaults.md``.

Validation happens once, at start-up, so a bad value fails the run before the first request
instead of halfway through a crawl.
"""

import re
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from scraping_pipeline.core.types import Site

DEFAULT_USER_AGENT = (
    "resilient-scraping-pipeline/0.1 (+https://github.com/TTB8/resilient-scraping-pipeline)"
)

#: Hard ceilings. Raising them is a code change with a review, not an environment variable.
MAX_REQUESTS_PER_SECOND = 5.0
MAX_CONCURRENCY_PER_DOMAIN = 8

# A contact point: an http(s) URL or an e-mail address, anywhere in the User-Agent.
_CONTACT = re.compile(r"(https?://\S+|[\w.+-]+@[\w-]+\.[\w.-]+)")

# "Honest" means it names this crawler and how to reach its operator, so it must lead with its own
# product token (``name/version``) ...
_PRODUCT_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*(\s|$)")
# ... and must not borrow the identity of a browser or of another crawler. A site that allow-lists
# a search engine, or serves browsers differently, would otherwise treat us as something we're not.
_IMPERSONATION = re.compile(
    r"mozilla/|applewebkit|chrome/|chromium/|safari/|firefox/|gecko/|edg/|opr/|"
    r"googlebot|bingbot|yandex|baiduspider|duckduckbot|slurp|applebot|facebookexternalhit|"
    r"python-requests|python-httpx|curl/|wget/|go-http-client|okhttp|headlesschrome",
    re.IGNORECASE,
)


class Settings(BaseSettings):
    """Every knob of a run. Field descriptions double as the ``.env.example`` documentation."""

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_", env_file=".env", extra="forbid", frozen=True
    )

    data_dir: Path = Field(Path("data"), description="Cache, state DB and outputs live here.")

    # --- Politeness -------------------------------------------------------------------------
    user_agent: str = Field(
        DEFAULT_USER_AGENT, description="Sent on every request. Must name a contact URL or e-mail."
    )
    requests_per_second: float = Field(
        1.0, gt=0, le=MAX_REQUESTS_PER_SECOND, description="Per-domain sustained request rate."
    )
    burst: int = Field(
        1, ge=1, le=10, description="Requests allowed back to back before the rate applies."
    )
    max_concurrency_per_domain: int = Field(
        2, ge=1, le=MAX_CONCURRENCY_PER_DOMAIN, description="Requests in flight per domain."
    )
    request_timeout_s: float = Field(20.0, gt=0, le=120, description="Per-request timeout.")

    # --- Retries and circuit breaker -----------------------------------------------------------
    max_attempts: int = Field(5, ge=1, le=10, description="Attempts per URL, first try included.")
    backoff_base_s: float = Field(1.0, gt=0, description="First retry delay before jitter.")
    backoff_cap_s: float = Field(60.0, gt=0, description="Longest delay between two attempts.")
    max_retry_after_s: float = Field(
        300.0,
        gt=0,
        description="A Retry-After longer than this stops the domain instead of waiting.",
    )
    max_crawl_delay_s: float = Field(
        60.0,
        gt=0,
        description="A robots.txt Crawl-delay longer than this stops the domain instead of crawling.",
    )
    breaker_failure_threshold: int = Field(
        5, ge=1, description="Consecutive transient failures that open a domain's breaker."
    )
    breaker_cooldown_s: float = Field(30.0, gt=0, description="Open time before one probe.")
    breaker_max_open_cycles: int = Field(
        3, ge=1, description="Openings in one run after which the domain is stopped."
    )

    # --- Crawl scope ------------------------------------------------------------------------
    max_pages: int | None = Field(None, ge=1, description="Frontier size cap per site.")
    max_depth: int | None = Field(None, ge=0, description="Link depth cap from the seeds.")

    # --- Validation -------------------------------------------------------------------------
    max_failure_rate: float = Field(
        0.0,
        ge=0,
        le=1,
        description="Share of rows allowed to fail the contract before the run fails.",
    )

    # --- Targets (overridable so tests can point at the local fixture server) ---------------
    books_base_url: HttpUrl = Field(HttpUrl("https://books.toscrape.com/"))
    quotes_base_url: HttpUrl = Field(HttpUrl("https://quotes.toscrape.com/"))

    # --- Logging ----------------------------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    @field_validator("user_agent")
    @classmethod
    def _honest_user_agent(cls, value: str) -> str:
        value = value.strip()
        if not _PRODUCT_TOKEN.match(value):
            raise ValueError("user_agent must start with this crawler's own name/version token")
        if match := _IMPERSONATION.search(value):
            raise ValueError(
                f"user_agent must not impersonate a browser or another crawler ({match.group(0)!r})"
            )
        if not _CONTACT.search(value):
            raise ValueError("user_agent must include a contact URL or e-mail address")
        return value

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if self.backoff_base_s > self.backoff_cap_s:
            raise ValueError("backoff_base_s must not exceed backoff_cap_s")
        return self

    def base_url(self, site: Site) -> str:
        """The seed origin for a site (the JS variant lives under the quotes origin)."""
        if site is Site.BOOKS:
            return str(self.books_base_url)
        return str(self.quotes_base_url)

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def state_db(self) -> Path:
        return self.data_dir / "state.duckdb"

    @property
    def output_dir(self) -> Path:
        return self.data_dir / "output"
