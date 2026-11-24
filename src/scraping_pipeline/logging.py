"""Structured logging: one JSON object per line, so a run can be grepped, shipped and queried.

Every event carries ``run_id``, ``site`` and ``stage`` once they are bound (``bind_run``), which is
what lets an analyst trace a bad row back to the fetch that produced it. Logs go to stderr; stdout is
kept for command output such as the run report.
"""

import logging
import sys
from typing import Literal, TextIO

import structlog
from structlog.typing import Processor


def configure_logging(
    level: str = "INFO", fmt: Literal["json", "console"] = "json", *, stream: TextIO | None = None
) -> None:
    """Configure structlog once per process. ``stream`` defaults to stderr (tests pass a buffer)."""
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer: Processor = (
        structlog.processors.JSONRenderer(sort_keys=True)
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[*processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(file=stream or sys.stderr),
        cache_logger_on_first_use=False,
    )


def bind_run(*, run_id: str, site: str, stage: str) -> None:
    """Attach lineage fields to every log event emitted from now on in this context."""
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(run_id=run_id, site=site, stage=stage)


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    logger: structlog.typing.FilteringBoundLogger = structlog.get_logger(name)
    return logger
