# syntax=docker/dockerfile:1
# The `pipeline` CLI in a small image. Chromium is opt-in, because only the JS-rendered site needs it:
#   docker build -t resilient-scraping-pipeline .
#   docker build --build-arg WITH_BROWSER=1 -t resilient-scraping-pipeline:browser .
#   docker run --rm -v "$PWD/data:/data" resilient-scraping-pipeline run --site books --max-depth 1

ARG PYTHON_IMAGE=python:3.12-slim-bookworm

FROM ghcr.io/astral-sh/uv:0.13.9 AS uv

# --- build: resolve the locked dependencies into /app/.venv -------------------------------------
FROM ${PYTHON_IMAGE} AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Dependencies first, so a code change does not re-download them.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

# --- runtime: the venv, a non-root user, nothing else ---------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime
ARG WITH_BROWSER=0
RUN useradd --create-home --uid 10001 pipeline
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PIPELINE_DATA_DIR=/data \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN if [ "$WITH_BROWSER" = "1" ]; then \
        playwright install --with-deps chromium && chown -R pipeline /ms-playwright; \
    fi \
    && mkdir -p /data && chown pipeline /data
USER pipeline
WORKDIR /data
VOLUME ["/data"]
ENTRYPOINT ["pipeline"]
CMD ["--help"]
