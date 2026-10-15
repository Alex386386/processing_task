FROM cgr.dev/chainguard/wolfi-base:latest AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON=/usr/src/app/.venv/bin/python \
    UV_CACHE_DIR=/tmp/uv-cache
WORKDIR /usr/src/app

RUN apk add --no-cache python-3.13 py3.13-pip
COPY pyproject.toml uv.lock README.md ./
RUN ["python", "-m", "venv", "--without-pip", "/usr/src/app/.venv"]
RUN --mount=type=cache,target=/tmp/uv-cache ["uv", "sync", "--frozen", "--no-install-project", "--no-dev"]

COPY src /usr/src/app/src
COPY config /usr/src/app/config
COPY migrations /usr/src/app/migrations
COPY alembic.ini /usr/src/app/alembic.ini
RUN --mount=type=cache,target=/tmp/uv-cache ["uv", "sync", "--frozen", "--no-dev"]

FROM cgr.dev/chainguard/wolfi-base:latest AS runtime
RUN apk add --no-cache python-3.13
WORKDIR /usr/src/app
ENV PATH="/usr/src/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=builder --chown=65532:65532 /usr/src/app /usr/src/app

USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["python", "-m", "uvicorn"]
CMD ["entrypoints.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
