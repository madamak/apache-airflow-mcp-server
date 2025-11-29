FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy lockfiles first for better Docker layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Now copy the remainder of the source tree
COPY . .

EXPOSE 8765

CMD ["uv", "run", "airflow-mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8765"]

