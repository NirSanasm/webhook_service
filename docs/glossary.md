# Distributed Systems & Messaging Glossary

A reference for key concepts and terminology used throughout this laboratory.

---

### At-Least-Once Delivery
A delivery guarantee where every message is guaranteed to reach its consumer at least one time, with the trade-off that network partitions or worker crashes may cause messages to be delivered more than once.

### Backpressure
A mechanism that limits the rate at which an upstream system sends data to a downstream system when the downstream system is running at or near capacity. In RabbitMQ, backpressure is achieved using `prefetch_count`.

### Dead-Letter Exchange (DLX) & Queue (DLQ)
A designated exchange/queue where messages are automatically routed when they are rejected (`basic_reject` or `basic_nack` with `requeue=false`), when their TTL expires, or when queue length limits are reached.

### Dual-Write Problem
An architectural anti-pattern where an application writes to two independent distributed data stores (e.g. database and message broker) without distributed transactions, leading to inconsistency if one write succeeds and the second fails.

### Exponential Backoff
A retry algorithm that exponentially increases the delay between consecutive retry attempts (e.g. 10s -> 60s -> 300s) to avoid overwhelming failing dependencies.

### Idempotency
The property of an operation whereby performing it multiple times yields the exact same state and result as performing it once. In webhooks, idempotency keys ensure duplicated requests are not executed twice.

### Manual Acknowledgement (`ack`)
A protocol mechanism where the message broker does not consider a message processed until the consumer explicitly sends an acknowledgement frame back to the broker.

### Outbox Pattern
A reliable integration pattern where domain changes and outbound message events are saved in the same database within a single local ACID transaction, and an outbox worker asynchronously reads and publishes them to the message broker.

### Poison Pill Message
A message that cannot be processed by a consumer regardless of how many times it is retried (e.g. invalid syntax, logic bug). Without a max retry limit or DLQ, it causes an infinite redelivery loop.

### Prefetch Count (`basic_qos`)
The maximum number of unacknowledged messages that RabbitMQ will deliver to a specific consumer channel before waiting for acknowledgements.

### Publisher Confirms
An AMQP extension where the RabbitMQ broker asynchronously acknowledges to the publisher that a message has been safely received, routed, and persisted to disk.

### `SELECT FOR UPDATE SKIP LOCKED`
A PostgreSQL SQL construct that locks matching rows for updating while skipping any rows already locked by other concurrent transactions, enabling lock-free horizontal scaling of workers.

### Server-Side Request Forgery (SSRF)
A vulnerability where an attacker manipulates a server into making unauthorized HTTP requests to internal, private, or prohibited resources. In webhooks, prevented using strict destination allowlists.

### TTL (Time-To-Live)
A timer attached to a message or queue determining how long a message can remain unconsumed before expiring and being dropped or dead-lettered.

### W3C Trace Context (`traceparent`)
A standardized HTTP and message header format (`version-trace_id-parent_id-trace_flags`) used to correlate distributed spans across microservice boundaries.
