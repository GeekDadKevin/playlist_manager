FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libchromaprint-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --frozen --no-dev

COPY env_config.py /app/env_config.py
COPY env_config.py /env_config.py
COPY validate_env.py /app/validate_env.py
COPY validate_env.py /validate_env.py
COPY app ./app
COPY scripts ./scripts
COPY .env.example ./.env.example
COPY config.json.example ./config.json.example

EXPOSE 3000

CMD ["uv", "run", "waitress-serve", "--host=0.0.0.0", "--port=3000", "app:app"]
