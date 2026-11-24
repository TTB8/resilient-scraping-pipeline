"""Same cache, same bytes: parse, validate and export reproduce every data file exactly.

After one fetch, the offline stages run twice in this process, once in a fresh interpreter with a
different hash seed (so set and dict iteration order would differ if any leaked into a file), and
once more after the outputs were deleted. Every data file must be byte-identical each time: parsed
JSONL, valid rows, quarantine, Parquet and CSV. The run report says when the pipeline ran, so it
is the documented exception and is left out.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scraping_pipeline.config import Settings
from scraping_pipeline.core.types import Site
from scraping_pipeline.stages import run_export, run_parse, run_validate
from scraping_pipeline.stages.common import SitePaths

pytestmark = [pytest.mark.slow, pytest.mark.usefixtures("no_real_fetchers")]


def _offline_stages(settings: Settings, site: Site) -> None:
    run_parse(settings, site)
    run_validate(settings, site)
    run_export(settings, site)


def _data_files(settings: Settings, site: Site) -> dict[str, bytes]:
    paths = SitePaths.of(settings, site)
    files = {
        str(path.relative_to(settings.data_dir)): path.read_bytes()
        for root in (paths.parsed, paths.validated, paths.output)
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.name.startswith("run_report.")
    }
    suffixes = {Path(name).suffix for name in files}
    assert suffixes == {".jsonl", ".json", ".parquet", ".csv"}
    return files


def _delete_outputs(settings: Settings, site: Site) -> None:
    paths = SitePaths.of(settings, site)
    for root in (paths.parsed, paths.validated, paths.output):
        shutil.rmtree(root)


@pytest.mark.parametrize("site_fixture", ["quotes_settings", "books_settings"])
def test_offline_stages_are_byte_identical_in_process(
    site_fixture: str, request: pytest.FixtureRequest
) -> None:
    settings: Settings = request.getfixturevalue(site_fixture)
    site = Site.QUOTES if site_fixture == "quotes_settings" else Site.BOOKS
    _offline_stages(settings, site)
    first = _data_files(settings, site)
    _offline_stages(settings, site)
    assert _data_files(settings, site) == first
    _delete_outputs(settings, site)
    _offline_stages(settings, site)
    assert _data_files(settings, site) == first


def test_offline_stages_are_byte_identical_in_a_fresh_process(quotes_settings: Settings) -> None:
    site = Site.QUOTES
    _offline_stages(quotes_settings, site)
    first = _data_files(quotes_settings, site)
    _delete_outputs(quotes_settings, site)

    env = {k: v for k, v in os.environ.items() if not k.startswith("PIPELINE_")}
    env["PYTHONHASHSEED"] = "12345"
    # The saved quotes data fails validation (most authors were never saved), so validate and
    # export exit 1. Exactly 1: a crash exits 4 and would fail here.
    for stage, expected in (("parse", 0), ("validate", 1), ("export", 1)):
        done = subprocess.run(  # noqa: S603 - our own interpreter and CLI, fixed arguments
            [
                sys.executable,
                "-c",
                "from scraping_pipeline.cli import app; app()",
                stage,
                "--site",
                site.value,
                "--data-dir",
                str(quotes_settings.data_dir),
            ],
            cwd=quotes_settings.data_dir,  # no .env there
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=50,
        )
        assert done.returncode == expected, (stage, done.stderr)
    assert _data_files(quotes_settings, site) == first
