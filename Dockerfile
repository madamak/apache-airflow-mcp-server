FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy lockfiles first for better Docker layer caching; the project itself
# needs README.md and src/, so install it only after the full copy.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Now copy the remainder of the source tree and install the project
COPY . .
RUN uv sync --frozen

EXPOSE 8765

CMD ["uv", "run", "--no-sync", "airflow-mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8765"]

