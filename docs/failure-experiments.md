# Failure Injection & Chaos Engineering Guide

This document describes how to execute and verify the 10 experiments provided in the `experiments/` directory.

---

## Experiment 1: Consumer Crash & Redelivery
- **Objective**: Observe RabbitMQ redelivering messages when a consumer drops connection without sending an ACK.
- **Run**:
  ```bash
  python experiments/01_consumer_crash.py --publish
  python experiments/01_consumer_crash.py          # press Ctrl+C to kill
  python experiments/01_consumer_crash.py --recover
  ```
- **What to look for**: The recovered message has `message.redelivered == True`.

---

## Experiment 2: Duplicate Delivery & Receiver Idempotency
- **Objective**: Prove that even when messages are duplicated, receiver-side deduplication guarantees safety.
- **Run**:
  ```bash
  python experiments/02_idempotency.py
  ```
- **What to look for**: Second delivery attempt returns `HTTP 200 {"status": "already_processed"}` without executing duplicate side-effects.

---

## Experiment 3: Exponential Backoff & DLQ Routing
- **Objective**: Observe retry queues with message TTLs: 10s -> 60s -> 300s -> DLQ.
- **Run**:
  ```bash
  python experiments/03_retry_behaviour.py
  ```
- **What to look for**: Polling output shows `attempts` incrementing and queue hops. Verify with:
  ```bash
  make dlq-info
  ```

---

## Experiment 4: Queue Backlog & Worker Concurrency
- **Objective**: Observe queue depth rise and measure consumption rate under load.
- **Run**:
  ```bash
  python experiments/04_queue_backlog.py --count 500
  ```
- **What to look for**: Real-time output of ready vs unacked messages.

---

## Experiment 5: Backpressure with Slow Receivers
- **Objective**: Verify that worker concurrency bounds and RabbitMQ prefetch prevent worker OOM.
- **Run**:
  ```bash
  python experiments/05_backpressure.py --count 30
  ```
- **What to look for**: Unacked count stays bounded at prefetch limit while ready count holds the remainder.

---

## Experiment 6: Outbox Pattern Broker Recovery
- **Objective**: Demonstrate that when RabbitMQ is completely offline, events remain safe in PostgreSQL and publish automatically upon broker return.
- **Run**:
  ```bash
  python experiments/06_outbox_recovery.py
  ```
- **What to look for**: PostgreSQL table `outbox_events` retains pending rows until RabbitMQ restarts.

---

## Experiment 7: Distributed Tracing Through RabbitMQ
- **Objective**: Validate end-to-end W3C trace propagation from FastAPI request through RabbitMQ to worker and receiver.
- **Run**:
  ```bash
  python experiments/07_distributed_tracing.py
  ```
- **What to look for**: Trace ID in Tempo showing linked spans across services.

---

## Experiment 8: Observability Under Failure
- **Objective**: Generate 500 (transient), 400 (permanent), and 429 (rate-limited) errors and inspect dashboards.
- **Run**:
  ```bash
  python experiments/08_observability_failure.py
  ```
- **What to look for**: Spikes in Grafana dashboard metrics, error logs in Loki, and error status in Tempo.

---

## Experiment 9: Graceful Shutdown
- **Objective**: Send SIGTERM to the delivery worker during active HTTP calls and verify zero lost messages.
- **Run**:
  ```bash
  python experiments/09_graceful_shutdown.py
  ```
- **What to look for**: All jobs eventually complete with `status: delivered`.

---

## Experiment 10: Load Testing
- **Objective**: Benchmark API and worker throughput.
- **Run**:
  ```bash
  python experiments/load_test.py --count 500 --concurrency 25
  ```
- **What to look for**: Requests per second (RPS) and latency percentiles.
