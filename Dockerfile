# Multi-stage Dockerfile for Webhook Delivery Lab
# Uses a shared base image, then separate targets per service.

# ---------------------------------------------------------------------------
# Base stage — shared dependencies
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS base

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[dev]"

# Copy application code
COPY app/ ./app/
COPY receiver/ ./receiver/
COPY scripts/ ./scripts/
COPY migrations/ ./migrations/
COPY alembic.ini ./

# ---------------------------------------------------------------------------
# API target
# ---------------------------------------------------------------------------
FROM base AS api
ENV PYTHONPATH=/app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# ---------------------------------------------------------------------------
# Worker target
# ---------------------------------------------------------------------------
FROM base AS worker
ENV PYTHONPATH=/app
CMD ["python", "scripts/run_worker.py"]

# ---------------------------------------------------------------------------
# Outbox publisher target
# ---------------------------------------------------------------------------
FROM base AS outbox
ENV PYTHONPATH=/app
CMD ["python", "scripts/run_outbox.py"]

# ---------------------------------------------------------------------------
# Receiver target
# ---------------------------------------------------------------------------
FROM base AS receiver
ENV PYTHONPATH=/app
EXPOSE 8001
CMD ["uvicorn", "receiver.main:app", "--host", "0.0.0.0", "--port", "8001"]

# ---------------------------------------------------------------------------
# Migrations target
# ---------------------------------------------------------------------------
FROM base AS migrations
ENV PYTHONPATH=/app
CMD ["alembic", "upgrade", "head"]

