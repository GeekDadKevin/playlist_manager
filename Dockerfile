FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --frozen --no-dev

COPY app ./app
COPY .env.example ./.env.example

EXPOSE 8000

CMD ["uv", "run", "waitress-serve", "--host=0.0.0.0", "--port=8000", "app:app"]
