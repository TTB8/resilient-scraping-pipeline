import io
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from scraping_pipeline.config import MAX_REQUESTS_PER_SECOND, Settings
from scraping_pipeline.core.types import Site
from scraping_pipeline.logging import bind_run, configure_logging, get_logger


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)  # no stray .env from the developer's checkout
    for key in list(__import__("os").environ):
        if key.startswith("PIPELINE_"):
            monkeypatch.delenv(key)


def test_defaults_are_polite() -> None:
    s = Settings()
    assert s.requests_per_second == 1.0
    assert s.max_concurrency_per_domain == 2
    assert "https://" in s.user_agent
    assert s.state_db == Path("data/state.duckdb")
    assert s.base_url(Site.BOOKS) == "https://books.toscrape.com/"
    assert s.base_url(Site.QUOTES_JS) == "https://quotes.toscrape.com/"


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_REQUESTS_PER_SECOND", "2.5")
    monkeypatch.setenv("PIPELINE_BOOKS_BASE_URL", "http://127.0.0.1:8765/")
    s = Settings()
    assert s.requests_per_second == 2.5
    assert s.base_url(Site.BOOKS) == "http://127.0.0.1:8765/"


@pytest.mark.parametrize(
    "overrides",
    [
        {"user_agent": "python-requests/2.0"},  # no contact point
        {"requests_per_second": MAX_REQUESTS_PER_SECOND + 1},
        {"requests_per_second": 0},
        {"max_concurrency_per_domain": 0},
        {"backoff_base_s": 90, "backoff_cap_s": 60},
        {"max_failure_rate": 1.5},
        {"respect_robots": False},  # robots cannot be switched off: unknown keys are rejected
    ],
)
def test_rejects_rude_or_incoherent_settings(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Settings(**overrides)  # type: ignore[arg-type]


def test_user_agent_accepts_an_email_contact() -> None:
    assert Settings(user_agent="my-bot/1.0 (ops@example.org)").user_agent.endswith("org)")


def test_json_logs_carry_run_lineage() -> None:
    buf = io.StringIO()
    configure_logging("INFO", "json", stream=buf)
    bind_run(run_id="r-1", site="books", stage="fetch")
    get_logger("t").info("page_fetched", url="https://x.test/")
    get_logger("t").debug("hidden")
    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert len(lines) == 1
    assert lines[0]["event"] == "page_fetched"
    assert {lines[0][k] for k in ("run_id", "site", "stage")} == {"r-1", "books", "fetch"}
    assert lines[0]["level"] == "info"


def test_console_format_renders() -> None:
    buf = io.StringIO()
    configure_logging("DEBUG", "console", stream=buf)
    get_logger("t").debug("hello")
    assert "hello" in buf.getvalue()
