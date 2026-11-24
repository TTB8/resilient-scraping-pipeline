"""The CLI as a real process: what the shell sees when stdout is closed early (R3-02).

``pipeline run --site books | head -20`` closes the pipe before the report is printed. That is the
reader's choice, not a bug: the command must not print "internal error", and it must exit with the
code its data earned (``CliRunner`` cannot show this: its stdout is a buffer, not a pipe).
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from fixture_server import FixtureServer


def _env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("PIPELINE_")}
    return env | {"PIPELINE_LOG_LEVEL": "ERROR", **extra}


def _closed_stdout(args: list[str], env: dict[str, str], cwd: Path) -> tuple[int, str]:
    """Run ``python -m scraping_pipeline <args>`` with a stdout pipe whose reader is already gone."""
    read_end, write_end = os.pipe()
    os.close(read_end)  # every write to stdout now fails with EPIPE, deterministically
    try:
        done = subprocess.run(  # noqa: S603 - our own interpreter and module
            [sys.executable, "-m", "scraping_pipeline", *args],
            stdout=write_end,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd,
            timeout=120,
            check=False,
        )
    finally:
        os.close(write_end)
    return done.returncode, done.stderr.decode("utf-8", "replace")


def test_a_closed_stdout_is_not_an_internal_error(tmp_path: Path) -> None:
    code, stderr = _closed_stdout(["version"], _env(), tmp_path)
    assert "internal error" not in stderr
    assert "BrokenPipeError" not in stderr
    assert code == 0  # what `version` earned; not 4 (a bug) and not 120 (the failed final flush)


@pytest.mark.slow
def test_run_into_a_closed_pipe_exits_with_the_code_its_data_earned(
    make_fixture_server: Callable[..., FixtureServer], tmp_path: Path
) -> None:
    server = make_fixture_server("books")
    data = tmp_path / "data"
    env = _env(
        PIPELINE_DATA_DIR=str(data),
        PIPELINE_MAX_PAGES="10",
        PIPELINE_REQUESTS_PER_SECOND="5",
        PIPELINE_BURST="5",
        PIPELINE_BOOKS_BASE_URL=server.base_url + "/",
        PIPELINE_QUOTES_BASE_URL=server.base_url + "/",
    )
    code, stderr = _closed_stdout(["run", "--site", "books"], env, tmp_path)
    assert "internal error" not in stderr
    assert (data / "output" / "books").is_dir()  # the deliverables were written all the same
    # The same verdict, read back with stdout open: the exit code is the data's, not the pipe's.
    validated = subprocess.run(
        [sys.executable, "-m", "scraping_pipeline", "validate", "--site", "books"],
        capture_output=True,
        env=env,
        cwd=tmp_path,
        timeout=120,
        check=False,
    )
    assert validated.returncode in {0, 1}
    assert code == validated.returncode
