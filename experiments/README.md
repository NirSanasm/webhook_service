# Distributed Systems Laboratory Experiments

This directory contains runnable hands-on experiments for testing distributed systems reliability, asynchronous queue semantics, failure recovery, and observability.

---

## Experiment Index

| # | Name | Script / Command | Primary Concept |
|---|------|------------------|-----------------|
| **1** | Consumer Crash | `python experiments/01_consumer_crash.py` | RabbitMQ manual acks, unacked message requeue, `redelivered=True` flag |
| **2** | Duplicate Messages | `python experiments/02_idempotency.py` | At-least-once delivery, receiver-side deduplication using delivery ID |
| **3** | Retry Behaviour | `python experiments/03_retry_behaviour.py` | Exponential backoff (10s, 60s, 300s TTL retry queues), eventual DLQ routing |
| **4** | Queue Backlog | `python experiments/04_queue_backlog.py` | High-throughput publishing, queue depth monitoring, worker drain rates |
| **5** | Backpressure | `python experiments/05_backpressure.py` | Worker concurrency limit, prefetch count, broker buffering under slow receiver |
| **6** | Outbox Recovery | `python experiments/06_outbox_recovery.py` | Dual-write problem resolution, PostgreSQL persistence during broker outage |
| **7** | Distributed Tracing | `python experiments/07_distributed_tracing.py` | W3C trace context propagation across HTTP & RabbitMQ boundaries into Tempo |
| **8** | Observability Under Failure | `python experiments/08_observability_failure.py` | Correlating 500/400/429 errors across Prometheus metrics, Loki logs, and Tempo |
| **9** | Graceful Shutdown | `python experiments/09_graceful_shutdown.py` | SIGTERM handling, in-flight task safety, unacked redelivery |
| **10** | Load Testing | `python experiments/load_test.py` | Sustained throughput (RPS), p95 latency, queue backlog |
| **Bonus**| Exchange Routing Demo | `python experiments/exchange_demo.py` | Isolated demo of Direct, Fanout, and Topic exchange routing |

---

## Detailed Walkthroughs

### Experiment 1: Consumer Crash
Simulates a worker dying after receiving a message but before acknowledging it.
```bash
# Terminal 1: Publish a test delivery job
python experiments/01_consumer_crash.py --publish

# Terminal 1: Start consumer and interrupt with Ctrl+C after receipt
python experiments/01_consumer_crash.py

# Terminal 1: Restart consumer and observe redelivered=True
python experiments/01_consumer_crash.py --recover
```

### Experiment 2: Duplicate Messages & Idempotency
Submits an event, waits for delivery, then forces a replay of the same `delivery_id`.
```bash
python experiments/02_idempotency.py
```
Observe that the mock receiver detects the duplicate `delivery_id` and returns `200 OK` without triggering side-effects twice.

### Experiment 3: Retry Policy & Dead-Letter Queue
Sends a webhook to `/webhooks/flaky` (which fails 3 times with HTTP 500 before succeeding) or `/webhooks/fail` (which always returns 500).
```bash
python experiments/03_retry_behaviour.py
```
Observe messages moving through:
`webhook.delivery` -> `webhook.retry.10s` -> `webhook.delivery` -> `webhook.retry.60s` -> `webhook.delivery` -> delivered (or `webhook.dead_letter`).

### Experiment 4: Queue Backlog
Submits a burst of events and charts the drain rate:
```bash
python experiments/04_queue_backlog.py --count 500 --concurrency 25
```

### Experiment 5: Backpressure with Slow Consumers
Sends requests to `/webhooks/slow` which takes 10s to respond:
```bash
python experiments/05_backpressure.py --count 25
```
Watch how `messages_unacknowledged` caps at `RABBITMQ_PREFETCH_COUNT` while remaining messages are safely held in `messages_ready`.

### Experiment 6: Outbox Recovery
```bash
python experiments/06_outbox_recovery.py
```
Stops RabbitMQ, submits events to FastAPI (which commit safely to PostgreSQL), verifies they remain unpublished in `outbox_events`, restarts RabbitMQ, and verifies the outbox worker publishes them upon reconnection.

### Experiment 7: Distributed Tracing
```bash
python experiments/07_distributed_tracing.py
```
Follow the trace in Grafana Tempo (`http://localhost:3000/explore` -> select Tempo).

### Experiment 8: Observability Under Failure
```bash
python experiments/08_observability_failure.py
```
Examine how 500 (retryable), 400 (permanent), and 429 (rate-limited) appear across Grafana dashboards, Loki logs, and Prometheus metrics.

### Experiment 9: Graceful Shutdown
```bash
python experiments/09_graceful_shutdown.py
```
Sends SIGTERM to the delivery worker during in-flight deliveries and verifies zero message loss.

### Experiment 10: Load Testing
```bash
python experiments/load_test.py --count 1000 --concurrency 20
```
Measures API requests/second, delivery success rates, and latency percentiles.
