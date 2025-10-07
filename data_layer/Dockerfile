# FROM python:3.11-slim
#
# WORKDIR /app
#
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
# COPY pyproject.toml uv.lock ./
#
# ENV UV_NO_CACHE=1
# ENV UV_LINK_MODE=copy
# ENV UV_SYNC_ALWAYS=1
#
# RUN rm -rf /root/.cache/uv && uv pip install --system -r <(uv pip compile pyproject.toml)
#
# COPY . .
#
# CMD ["uv", "run", "pipeline.py"]
#
FROM python:3.11-slim AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./

ENV UV_NO_CACHE=1
ENV UV_LINK_MODE=copy

RUN rm -rf /root/.cache/uv \
    && uv lock \
    && uv pip compile pyproject.toml -o requirements.txt \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uv", "run", "pipeline.py"]

