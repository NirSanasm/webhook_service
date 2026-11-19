# Distributed Systems Reliability Patterns

This document details the core reliability patterns used in the Webhook Delivery Lab:
1. At-Least-Once Delivery vs Exactly-Once Myth
2. Idempotency Key Design
3. The Transactional Outbox Pattern and Dual-Write Problem
4. Failure Handling & Circuit Breaker / Dead-Letter Queue

---

## 1. At-Least-Once Delivery: Why "Exactly-Once" is Impossible in Distributed Networks

In any network communicating over non-shared memory (TCP/IP), network partitions, worker crashes, and timeouts are inevitable (the Two Generals' Problem).

### The Worker Crash Window:
Consider what happens during a webhook call:
1. Worker receives a delivery job message.
2. Worker performs HTTP POST to external receiver.
3. External receiver successfully processes the request and commits changes to its database.
4. External receiver returns `HTTP 200 OK`.
5. **The worker crashes right here (power loss, OOM, network disruption) before it can send `basic_ack` to RabbitMQ.**
6. RabbitMQ notices the TCP connection dropped while the message was unacknowledged.
7. RabbitMQ places the message back in the queue (`redelivered=True`).
8. Another worker picks up the message and sends the HTTP POST again!

Because step 5 can always occur, **exactly-once execution across a distributed network is theoretically impossible**. The only viable architecture is:
- **At-Least-Once Delivery** at the transport layer.
- **Idempotent Processing** at the receiving application layer.

---

## 2. Idempotency Pattern

Idempotence means that performing an operation once has the exact same effect as performing it multiple times with the same inputs:
$$f(f(x)) = f(x)$$

### Implementation in Webhook Delivery Lab:
- Each delivery is assigned a unique, stable UUID (`delivery_id`).
- When sending the HTTP request, the worker passes this identifier in both the JSON payload and the `X-Delivery-ID` HTTP header.
- The receiver maintains an idempotency log (`receiver_idempotency_records` table):
  ```sql
  SELECT status_returned FROM receiver_idempotency_records WHERE delivery_id = :id;
  ```
- If found, the receiver returns the previously computed response immediately (`HTTP 200 {"status": "already_processed"}`) without re-executing business operations (such as charging a credit card or creating a duplicate order).
- The worker also checks PostgreSQL before executing: if `delivery.status == 'delivered'`, it simply acknowledges the RabbitMQ message and skips the HTTP request.

---

## 3. The Transactional Outbox Pattern

### The Dual-Write Problem
When an API must both persist a record to a database and publish an event to a message broker, doing both independently creates an unrecoverable failure mode:

```python
# BROKEN PATTERN:
async def handle_event():
    await db.commit()          # Database committed
    # If the process crashes here, the message is NEVER published!
    await broker.publish()     # Broker publish
```
If you reverse the order (publish first, then commit DB), a database rollback causes downstream consumers to process an event that never existed in the database!

### The Outbox Solution
Instead of publishing to RabbitMQ during the HTTP request:
1. The API creates `webhook_events`, `webhook_deliveries`, and `outbox_events` inside a **single ACID PostgreSQL transaction**. Either all 3 are committed, or none are.
2. A separate background worker (`outbox_worker.py`) queries unpublished records:
   ```sql
   SELECT * FROM outbox_events 
   WHERE published = false AND attempts < 10
   ORDER BY created_at ASC
   LIMIT 50
   FOR UPDATE SKIP LOCKED;
   ```
3. `SKIP LOCKED` ensures multiple publisher instances can run concurrently without race conditions or deadlock.
4. The outbox worker publishes to RabbitMQ using **Publisher Confirms**.
5. Only when RabbitMQ sends an ACK does the worker update `published = true, published_at = now()`.

If the outbox worker crashes between publish and marking the row published, it will simply re-publish the event on restart. Downstream idempotency handles the duplicate safely.

---

## 4. Dead-Letter Queue (DLQ) & Poison Pill Isolation

When a delivery cannot succeed (e.g. invalid endpoint URL, client returns 400 Bad Request, or maximum attempts reached):
- We must not crash the consumer.
- We must not requeue indefinitely (which causes a poison pill loop).
- We route the message to `webhook.dead_letter`.
- Operations teams can inspect dead-lettered messages, fix underlying bugs or endpoints, and replay deliveries via `POST /api/v1/deliveries/{id}/replay`.
