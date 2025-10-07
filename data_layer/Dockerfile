
FROM python:3.11-slim AS builder
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY .. /app

WORKDIR /app/data_layer


RUN --mount=type=cache,target=/root/.cache/uv uv lock
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked



FROM python:3.11-slim
WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip libfreetype6-dev libpng-dev fontconfig && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=builder /app /app


ENV PATH="/app/data_layer/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1


RUN pip install --no-cache-dir matplotlib pytest pytest-benchmark aiohttp


RUN python -c "import matplotlib, aiohttp; print(' matplotlib', matplotlib.__version__, '| aiohttp OK')"

RUN mkdir -p /app/datalake /app/datamarts /app/control

CMD ["uv", "run", "data_layer/pipeline.py"]
