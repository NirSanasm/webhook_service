# RabbitMQ Engineering & Learning Guide

This document explains the core RabbitMQ concepts implemented throughout the Webhook Delivery Lab.

---

## 1. Exchanges and Routing Models

RabbitMQ decouples publishers from queues using **Exchanges**. Publishers never write directly to a queue; they publish to an exchange with a **routing key**.

### Direct Exchange (`webhook.direct`)
- **Routing Mechanism**: Exact match between message `routing_key` and queue binding key.
- **Used In Project**: Main webhook delivery workflow.
  - `webhook.deliver` -> bound to `webhook.delivery`
  - `webhook.retry.10s` -> bound to `webhook.retry.10s`
  - `webhook.dead` -> bound to `webhook.dead_letter`

### Fanout Exchange (Demo in `experiments/exchange_demo.py`)
- **Routing Mechanism**: Broadcasts message to **all** queues bound to it, completely ignoring the routing key.
- **Use Cases**: System-wide notifications, cache invalidation fanout.

### Topic Exchange (Demo in `experiments/exchange_demo.py`)
- **Routing Mechanism**: Pattern matching with dot-delimited tokens:
  - `*` (asterisk): matches exactly one word (e.g. `order.*` matches `order.created`, `order.cancelled`).
  - `#` (hash): matches zero or more words (e.g. `#.critical` matches `payment.critical`, `audit.system.critical`).
- **Use Cases**: Multi-tenant event routing, log filtering.

---

## 2. Acknowledgment Semantics: Positive, Negative, and Rejection

When RabbitMQ delivers a message to a consumer, it considers the message unacknowledged (`unacked`).

### Basic Ack (`basic_ack` / `message.ack()`)
- Informs the broker that the consumer has successfully processed the message.
- The broker deletes the message from the queue.
- **Critical rule**: Never acknowledge a message before its processing outcome (or scheduled retry) is durably recorded!

### Basic Nack (`basic_nack` / `nack(requeue=True)`)
- Indicates failure to process.
- If `requeue=True`: The broker places the message back in the queue for redelivery.
- **Caution**: Indiscriminate `requeue=True` will cause an infinite loop if the failure is deterministic (e.g. invalid payload or crashing code), burning CPU and spamming downstream services.

### Basic Reject / Nack with `requeue=False`
- Informs the broker that the message was rejected and must **not** be requeued to this queue.
- If the queue has `x-dead-letter-exchange` configured, the broker routes the message to the dead-letter exchange; otherwise it is dropped.

---

## 3. Publisher Confirms

In standard AMQP 0-9-1, publishing is asynchronous. A publisher sends a message over the TCP socket, but if the broker crashes before writing it to disk, the message is permanently lost without the publisher knowing.

### How Publisher Confirms Work:
1. The publisher turns on confirms: `await channel.confirm_delivery()`.
2. The broker assigns an integer sequence number to each published message.
3. Once the broker has persisted the message (or delivered it to all matching queues), it sends an `Ack` back to the publisher.
4. If an internal broker error occurs, it sends a `Nack`.
5. In `aio_pika`, awaiting `exchange.publish(...)` on a confirm-enabled channel awaits this broker confirmation automatically and raises an exception if rejected.

---

## 4. Consumer Prefetch (`basic_qos`)

RabbitMQ pushes messages to consumers as fast as the network allows unless bounded.
- Without prefetch, if 10,000 messages arrive, RabbitMQ dumps all 10,000 into one connected worker's memory buffer.
- `await channel.set_qos(prefetch_count=10)` ensures the broker sends at most 10 unacknowledged messages to that consumer channel.
- Once the worker acknowledges 1 message, RabbitMQ pushes the next. This provides natural **backpressure** and evenly distributes work across multiple worker instances.

---

## 5. TTL-Based Retries vs Plugins

In distributed systems, retrying immediately upon failure often exacerbates transient outages (thundering herd). We use exponential backoff: 10s, 60s, 300s.

Instead of requiring external delay plugins, we use RabbitMQ's built-in **Message TTL + Dead-Letter Routing**:
1. Worker fails delivery attempt 1.
2. Worker publishes to `webhook.retry.10s` with TTL = 10,000ms.
3. Queue configuration:
   ```python
   "x-message-ttl": 10000,
   "x-dead-letter-exchange": "webhook.direct",
   "x-dead-letter-routing-key": "webhook.deliver"
   ```
4. While in `webhook.retry.10s`, no worker is listening to that queue.
5. After 10,000ms, RabbitMQ expires the message and dead-letters it to `webhook.direct` with key `webhook.deliver`.
6. The message reappears in `webhook.delivery`, where the delivery worker consumes it again.
