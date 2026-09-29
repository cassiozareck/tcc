FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY fleet-simulator.py ./

CMD ["uv", "run", "python", "fleet-simulator.py"]
