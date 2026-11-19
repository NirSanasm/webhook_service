# Observability in Distributed Systems

Observability is the ability to infer the internal state of a system based on its external outputs. This project implements the **Three Pillars of Observability**: Structured Metrics, Centralized Logs, and Distributed Tracing.

---

## 1. Metrics (Prometheus)

Metrics represent aggregated, numerical telemetry over time. They answer: *"Is the system healthy, and how fast is it degrading?"*

### Core Metrics Table:

| Metric Name | Type | Labels | Meaning |
|---|---|---|---|
| `webhook_events_received_total` | Counter | `service`, `event_type` | Total events received by API |
| `webhook_deliveries_total` | Counter | `service` | Delivery attempts started |
| `webhook_deliveries_succeeded_total` | Counter | `service`, `endpoint_category` | Deliveries that returned HTTP 2xx |
| `webhook_deliveries_failed_total` | Counter | `service`, `endpoint_category`, `failure_type` | Failed attempts (transient or fatal) |
| `webhook_retries_total` | Counter | `service`, `attempt_number` | Number of retries queued |
| `webhook_dead_lettered_total` | Counter | `service` | Terminal failures moved to DLQ |
| `outbox_published_total` | Counter | `service` | Records published to RabbitMQ |
| `outbox_publish_failures_total`| Counter | `service` | Failures during broker publish |
| `webhook_delivery_duration_seconds` | Histogram | `service`, `endpoint_category`, `status` | HTTP round-trip latency |
| `webhook_processing_duration_seconds`| Histogram | `service`, `status` | Total worker job duration |
| `api_request_duration_seconds` | Histogram | `service`, `method`, `endpoint`, `status_code` | API response latency |
| `outbox_publish_duration_seconds` | Histogram | `service` | Time to publish & confirm |
| `worker_active_jobs` | Gauge | `service`, `worker_id` | Current concurrency count |
| `outbox_pending_records` | Gauge | `service` | Backlog in `outbox_events` |

### Low-Cardinality Rule:
Never place high-cardinality values (such as `user_id`, `delivery_id`, `trace_id`, or raw URLs with query parameters) into Prometheus labels. High cardinality causes memory explosion in time-series databases. Categorize URLs into low-cardinality buckets (e.g. `success`, `fail`, `flaky`, `slow`).

---

## 2. Structured JSON Logging (Grafana Loki & Alloy)

Logs provide rich event context answering: *"Why did a specific event fail?"*

### Standard JSON Schema:
Every log message is serialized as JSON with consistent schema:
```json
{
  "asctime": "2026-11-01T22:15:30",
  "levelname": "INFO",
  "name": "app.workers.delivery_worker",
  "message": "Processing delivery",
  "service_name": "webhook-worker",
  "environment": "development",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "delivery_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "attempt": 1,
  "endpoint_url": "http://receiver:8001/webhooks/success"
}
```

### Log Collection Pipeline:
1. Docker container stdout/stderr -> Docker Engine
2. Grafana Alloy discovers container sockets via `discovery.docker`
3. Alloy extracts JSON attributes (`level`, `service_name`, `trace_id`)
4. Alloy pushes logs to Grafana Loki (`:3100`)
5. In Grafana, Loki logs with `trace_id` automatically render clickable links into Grafana Tempo!

---

## 3. Distributed Tracing (OpenTelemetry & Grafana Tempo)

Distributed tracing tracks the journey of a request across thread and network boundaries. It answers: *"Where did the latency occur in this request?"*

### Trace Propagation across RabbitMQ:
HTTP tracing is standard (via `traceparent` headers). But RabbitMQ is an asynchronous queue. Without explicit propagation, the trace breaks at the message broker.

In Webhook Delivery Lab:
1. **Outbox Publisher**:
   ```python
   # Injects W3C traceparent into message headers:
   headers = {}
   inject_trace_context(headers)
   message = aio_pika.Message(body=..., headers=headers)
   ```
2. **Delivery Worker**:
   ```python
   # Extracts parent context from RabbitMQ message headers:
   parent_ctx = extract_trace_context(message.headers)
   with tracer.start_as_current_span("webhook.delivery.process", context=parent_ctx):
       # Continues the original trace across the queue boundary!
   ```
3. Spans are exported via OTLP gRPC (`:4317`) to the **OpenTelemetry Collector**, which batches and exports them to **Grafana Tempo**.
