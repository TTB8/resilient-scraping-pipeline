# Developer shortcuts. Everything runs through uv from the locked environment.
.PHONY: help install demo test test-fast lint typecheck check browsers docker clean-data

help:          ## list the targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:       ## create the environment from uv.lock
	uv sync --frozen

demo:          ## offline end-to-end demo against the local fixture server; prints the run reports
	uv run --frozen python scripts/demo.py

test:          ## the whole suite, in parallel, with coverage
	uv run --frozen pytest -n auto --cov --cov-report=term

test-fast:     ## the quick subset (no browser, no slow property tests)
	uv run --frozen pytest -q -n 4 -m 'not browser and not slow'

lint:          ## ruff check + format check
	uv run --frozen ruff check .
	uv run --frozen ruff format --check .

typecheck:     ## mypy --strict
	uv run --frozen mypy

check: lint typecheck test  ## everything CI runs

browsers:      ## install Chromium for the JS-rendered site (quotes-js) and its tests
	uv run --frozen playwright install chromium

docker:        ## build the CLI image
	docker build -t resilient-scraping-pipeline .

clean-data:    ## delete the local cache, state DB and outputs (./data)
	rm -rf data
