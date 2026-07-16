FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58

ENV UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

# .git is not part of the build context, so hatch-vcs cannot derive the package
# version; release builds pass it in explicitly (defaults to the dev fallback).
ARG VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}

WORKDIR /app

# Copy lockfiles first for better Docker layer caching; the project itself
# needs README.md and src/, so install it only after the full copy.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy only package inputs. Local registries, tests, and workspace-only strategy
# files never enter the runtime image even if the build context is misconfigured.
COPY README.md ./
COPY src/ ./src/
RUN uv sync --frozen --no-dev

EXPOSE 8765

CMD ["uv", "run", "--no-sync", "airflow-mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8765"]
