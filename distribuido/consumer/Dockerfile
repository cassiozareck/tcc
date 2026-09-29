FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY processing-engine.py ./

CMD ["uv", "run", "python", "processing-engine.py"]
