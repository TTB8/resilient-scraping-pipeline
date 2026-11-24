from datetime import UTC, datetime
from pathlib import Path

import pytest

from scraping_pipeline.core.classify import (
    DEFAULT_MARKERS,
    BlockMarkers,
    TransportError,
    classify,
)
from scraping_pipeline.core.types import ResponseClass

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "sites"

CHALLENGE_PAGE = """<!doctype html><html><head><title>Just a moment...</title>
<script>var x = "verify you are human";</script></head>
<body><h1>Checking your browser before accessing the site.</h1></body></html>"""

BLOCK_PAGE = "<html><head><title>Access Denied</title></head><body>Request blocked.</body></html>"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, ResponseClass.OK),
        (204, ResponseClass.OK),
        (299, ResponseClass.OK),
        (404, ResponseClass.NOT_FOUND),
        (410, ResponseClass.NOT_FOUND),
        (401, ResponseClass.BLOCKED),
        (403, ResponseClass.BLOCKED),
        (429, ResponseClass.RATE_LIMITED),
        (503, ResponseClass.SERVER_ERROR),  # no Retry-After: just a server error
        (500, ResponseClass.SERVER_ERROR),
        (502, ResponseClass.SERVER_ERROR),
        (504, ResponseClass.SERVER_ERROR),
        (599, ResponseClass.SERVER_ERROR),
        (400, ResponseClass.CLIENT_ERROR),
        (405, ResponseClass.CLIENT_ERROR),
        (418, ResponseClass.CLIENT_ERROR),
        (451, ResponseClass.CLIENT_ERROR),
        (301, ResponseClass.CLIENT_ERROR),  # redirects are the fetcher's job
        (304, ResponseClass.CLIENT_ERROR),
        (100, ResponseClass.CLIENT_ERROR),
        (99, ResponseClass.NETWORK_ERROR),
        (600, ResponseClass.NETWORK_ERROR),
        (0, ResponseClass.NETWORK_ERROR),
    ],
)
def test_status_table(status: int, expected: ResponseClass) -> None:
    result = classify(status, {}, "", None)
    assert result.response_class is expected
    assert result.reason
    assert result.retry_after_s is None


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (TransportError.TIMEOUT, ResponseClass.TIMEOUT),
        (TransportError.CONNECT, ResponseClass.NETWORK_ERROR),
        (TransportError.DNS, ResponseClass.NETWORK_ERROR),
        (TransportError.RESET, ResponseClass.NETWORK_ERROR),
        (TransportError.PROTOCOL, ResponseClass.NETWORK_ERROR),
        (TransportError.TLS, ResponseClass.NETWORK_ERROR),
    ],
)
def test_transport_errors(error: TransportError, expected: ResponseClass) -> None:
    assert classify(None, {}, None, error).response_class is expected


def test_transport_error_wins_over_status() -> None:
    assert (
        classify(200, {}, "ok", TransportError.RESET).response_class is ResponseClass.NETWORK_ERROR
    )


def test_needs_status_or_error() -> None:
    with pytest.raises(ValueError, match="status or a transport error"):
        classify(None, {}, None, None)


# --- Retry-After -----------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["Retry-After", "retry-after", "RETRY-AFTER", "rEtRy-AfTeR"])
def test_retry_after_header_is_case_insensitive(name: str) -> None:
    result = classify(429, {name: "30"}, None, None)
    assert result.response_class is ResponseClass.RATE_LIMITED
    assert result.retry_after_s == 30.0
    assert "30s" in result.reason


def test_503_with_retry_after_is_rate_limited() -> None:
    result = classify(503, {"Retry-After": "120"}, None, None)
    assert result.response_class is ResponseClass.RATE_LIMITED
    assert result.retry_after_s == 120.0


def test_503_with_garbage_retry_after_is_server_error() -> None:
    result = classify(503, {"Retry-After": "later"}, None, None)
    assert result.response_class is ResponseClass.SERVER_ERROR
    assert result.retry_after_s is None


def test_503_with_http_date_and_now() -> None:
    now = datetime(2015, 10, 21, 7, 27, tzinfo=UTC).timestamp()
    result = classify(
        503, {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}, None, None, now_epoch=now
    )
    assert result.response_class is ResponseClass.RATE_LIMITED
    assert result.retry_after_s == 60.0


def test_503_with_http_date_without_now_is_rate_limited_without_delay() -> None:
    result = classify(503, {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}, None, None)
    assert result.response_class is ResponseClass.RATE_LIMITED
    assert result.retry_after_s is None
    assert result.reason == "503 rate limited"


def test_429_without_retry_after() -> None:
    result = classify(429, {"Content-Type": "text/html"}, None, None)
    assert result.response_class is ResponseClass.RATE_LIMITED
    assert result.retry_after_s is None


# --- markers ---------------------------------------------------------------------------------


def test_200_challenge_page_is_challenge() -> None:
    result = classify(200, {}, CHALLENGE_PAGE, None)
    assert result.response_class is ResponseClass.CHALLENGE
    assert "checking your browser" in result.reason


def test_marker_inside_script_is_ignored() -> None:
    page = "<html><title>Hi</title><script>'verify you are human'</script><p>hello</p></html>"
    assert classify(200, {}, page, None).response_class is ResponseClass.OK


def test_marker_inside_comment_is_ignored() -> None:
    page = "<html><title>Hi</title><!-- access denied --><p>hello</p></html>"
    assert classify(200, {}, page, None).response_class is ResponseClass.OK


def test_200_block_page_is_blocked() -> None:
    assert classify(200, {}, BLOCK_PAGE, None).response_class is ResponseClass.BLOCKED


def test_markers_are_case_and_whitespace_insensitive() -> None:
    page = "<p>VERIFY   you\n are&nbsp;HUMAN</p>"
    assert classify(200, {}, page, None).response_class is ResponseClass.CHALLENGE


def test_403_with_challenge_marker_is_challenge() -> None:
    assert classify(403, {}, CHALLENGE_PAGE, None).response_class is ResponseClass.CHALLENGE


def test_403_with_block_marker_stays_blocked() -> None:
    result = classify(403, {}, BLOCK_PAGE, None)
    assert result.response_class is ResponseClass.BLOCKED
    assert result.reason == "403 access refused"


def test_403_long_page_still_scanned_for_challenge() -> None:
    page = "<p>" + "filler text " * 1000 + "</p><p>Verify you are human</p>"
    assert classify(403, {}, page, None).response_class is ResponseClass.CHALLENGE


def test_single_word_robot_never_triggers() -> None:
    page = "<title>The Robot</title><p>A robot learns to love. Human, robot, verify.</p>"
    assert classify(200, {}, page, None).response_class is ResponseClass.OK


def test_long_2xx_page_with_marker_phrase_is_ok() -> None:
    # A real content page that happens to say "access denied" in its text or title.
    page = "<title>Access Denied | A Novel</title><p>" + "story " * 1000 + "</p>"
    assert classify(200, {}, page, None).response_class is ResponseClass.OK


def test_scan_limit_bounds_the_search() -> None:
    markers = BlockMarkers(challenge=("verify you are human",), blocked=(), scan_limit_chars=50)
    page = "<p>" + "x" * 100 + " verify you are human</p>"
    assert classify(200, {}, page, None, markers=markers).response_class is ResponseClass.OK
    assert classify(200, {}, page, None).response_class is ResponseClass.CHALLENGE


def test_custom_markers() -> None:
    markers = BlockMarkers(challenge=("Solve The Puzzle",), blocked=("go away now",))
    assert markers.challenge == ("solve the puzzle",)
    assert classify(200, {}, "<p>go away now</p>", None, markers=markers).response_class is (
        ResponseClass.BLOCKED
    )
    assert classify(200, {}, BLOCK_PAGE, None, markers=markers).response_class is ResponseClass.OK


@pytest.mark.parametrize("phrase", ["robot", "  denied  ", ""])
def test_single_word_markers_are_rejected(phrase: str) -> None:
    with pytest.raises(ValueError, match="at least two words"):
        BlockMarkers(challenge=(phrase,), blocked=())


@pytest.mark.parametrize(
    "kwargs", [{"scan_limit_chars": 0}, {"short_page_chars": -1}], ids=["scan", "short"]
)
def test_marker_limits_are_validated(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="must be"):
        BlockMarkers(challenge=(), blocked=(), **kwargs)


def test_default_markers_are_all_multi_word() -> None:
    for phrase in DEFAULT_MARKERS.challenge + DEFAULT_MARKERS.blocked:
        assert len(phrase.split()) >= 2


def test_empty_body_is_ok() -> None:
    assert classify(200, {}, None, None).response_class is ResponseClass.OK
    assert classify(200, {}, "", None).response_class is ResponseClass.OK


@pytest.mark.parametrize(
    "fixture",
    sorted(p.relative_to(FIXTURES).as_posix() for p in FIXTURES.rglob("*.html")),
)
def test_saved_practice_pages_classify_ok(fixture: str) -> None:
    # Every real page in the fixture set must be ok: a false positive would stop the domain.
    body = (FIXTURES / fixture).read_text(encoding="utf-8")
    result = classify(200, {"Content-Type": "text/html"}, body, None)
    assert result.response_class is ResponseClass.OK, result.reason
