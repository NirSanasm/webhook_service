"""
Prometheus metrics definitions.

Rules:
- No high-cardinality labels (no IDs, URLs, correlation IDs).
- Labels: service, status, event_type, endpoint_category.
- Every metric must map to a real observable event.
"""
from prometheus_client import Counter, Gauge, Histogram, Info

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

webhook_events_received_total = Counter(
    "webhook_events_received_total",
    "Total number of webhook events received by the API",
    ["service", "event_type"],
)

webhook_deliveries_total = Counter(
    "webhook_deliveries_total",
    "Total number of delivery attempts initiated",
    ["service"],
)

webhook_deliveries_succeeded_total = Counter(
    "webhook_deliveries_succeeded_total",
    "Total number of successful webhook deliveries",
    ["service", "endpoint_category"],
)

webhook_deliveries_failed_total = Counter(
    "webhook_deliveries_failed_total",
    "Total number of failed webhook delivery attempts",
    ["service", "endpoint_category", "failure_type"],
)

webhook_retries_total = Counter(
    "webhook_retries_total",
    "Total number of delivery retries scheduled",
    ["service", "attempt_number"],
)

webhook_dead_lettered_total = Counter(
    "webhook_dead_lettered_total",
    "Total number of deliveries moved to the dead-letter queue",
    ["service"],
)

outbox_published_total = Counter(
    "outbox_published_total",
    "Total number of outbox events successfully published to RabbitMQ",
    ["service"],
)

outbox_publish_failures_total = Counter(
    "outbox_publish_failures_total",
    "Total number of outbox publish failures",
    ["service"],
)

# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

webhook_delivery_duration_seconds = Histogram(
    "webhook_delivery_duration_seconds",
    "Time spent performing the HTTP webhook delivery request",
    ["service", "endpoint_category", "status"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

webhook_processing_duration_seconds = Histogram(
    "webhook_processing_duration_seconds",
    "Total processing time per delivery job in the worker (DB read + HTTP + DB write)",
    ["service", "status"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

api_request_duration_seconds = Histogram(
    "api_request_duration_seconds",
    "HTTP request duration for API endpoints",
    ["service", "method", "endpoint", "status_code"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

outbox_publish_duration_seconds = Histogram(
    "outbox_publish_duration_seconds",
    "Time spent publishing a single outbox record to RabbitMQ",
    ["service"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# ---------------------------------------------------------------------------
# Gauges
# ---------------------------------------------------------------------------

worker_active_jobs = Gauge(
    "worker_active_jobs",
    "Number of delivery jobs currently being processed by this worker",
    ["service", "worker_id"],
)

outbox_pending_records = Gauge(
    "outbox_pending_records",
    "Number of outbox records not yet published",
    ["service"],
)

# Note: queue_messages_ready and queue_messages_unacked are exported by
# the RabbitMQ Prometheus plugin at :15692/metrics — do not duplicate here.

# ---------------------------------------------------------------------------
# Info
# ---------------------------------------------------------------------------

app_info = Info(
    "webhook_delivery_lab",
    "Build and runtime information",
)

