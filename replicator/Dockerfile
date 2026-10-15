# syntax=docker/dockerfile:1.7

# -------- Builder: produce wheels for the project + runtime deps. --------
FROM python:3.12-slim AS builder
WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --wheel-dir /wheels .

# -------- Runtime: slim image, non-root, exec-form entrypoint. --------
FROM python:3.12-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 replicator
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
USER replicator
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 LOG_LEVEL=INFO

# tini as PID 1 forwards SIGTERM to dsql-replicator so Fargate stops cleanly.
# ConsumerPipeline installs SIGTERM/SIGINT handlers that drain the apply runner
# before exiting.
ENTRYPOINT ["/usr/bin/tini", "--", "dsql-replicator"]
CMD ["consume"]

# ECS task definition should set healthCheck to run `dsql-replicator healthz`;
# we expose a HEALTHCHECK here too so `docker run` works for local verification.
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD dsql-replicator healthz || exit 1
