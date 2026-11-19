# Troubleshooting & Operations Guide

This guide covers common issues encountered when running, deploying, or testing the Webhook Delivery Lab.

---

## 1. API Returns 503 Service Unavailable on `/ready`

### Cause:
The API service cannot establish a connection to PostgreSQL or RabbitMQ.

### Diagnostic Steps:
```bash
# Check container statuses:
docker compose ps

# Inspect logs of PostgreSQL and RabbitMQ:
docker compose logs postgres
docker compose logs rabbitmq

# Test PostgreSQL ping manually:
docker compose exec postgres pg_isready -U webhook -d webhookdb

# Test RabbitMQ management endpoint:
curl -s -u webhook:webhook http://localhost:15672/api/overview
```

---

## 2. Webhook Deliveries Remain in `pending` Status

### Cause:
The Outbox Publisher is not running, or RabbitMQ is unreachable from the publisher.

### Diagnostic Steps:
```bash
# Check outbox events table in Postgres:
make db-outbox

# Check outbox publisher logs:
make logs-outbox

# Verify RabbitMQ exchange declarations:
curl -s -u webhook:webhook http://localhost:15672/api/exchanges/%2F/webhook.direct
```

---

## 3. Messages Stuck in `webhook.delivery` Queue

### Cause:
The delivery worker is crashed, disconnected, or has exhausted its concurrency semaphore without releasing it.

### Diagnostic Steps:
```bash
# Check worker logs:
make logs-worker

# Inspect RabbitMQ queue details:
make rabbitmq-queues

# Restart delivery worker container:
docker compose restart delivery-worker
```

---

## 4. Alembic Migration Errors

### Error: `relation already exists` or `relation does not exist`
If manual tables were created:
```bash
# Reset database volumes (destroys data in local lab):
make clean-volumes
make up
```

---

## 5. Traces Not Visible in Grafana Tempo

### Cause:
OTel Collector is not forwarding spans, or Tempo has not flushed in-memory blocks to disk yet.

### Diagnostic Steps:
- Open `http://localhost:3000/explore` -> Select Tempo.
- Wait ~10-15 seconds for Tempo block processor to flush.
- Inspect OTel Collector container logs:
  ```bash
  docker compose logs otel-collector
  ```

---

## 6. Port Conflicts on Local Machine

If ports `5432`, `5672`, `8000`, `9090`, or `3000` are already bound by existing local services (e.g. local PostgreSQL, local Prometheus):
- Edit `.env` or `docker-compose.yml` to change the host port bindings (e.g. `"5433:5432"`, `"8005:8000"`).
