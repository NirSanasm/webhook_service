"""
RabbitMQ exchange and queue topology declarations.

This module documents and implements the full broker topology for the lab.

Exchange Types (educational reference):
  - direct:  Routes messages where routingKey == bindingKey exactly.
             Used by the webhook delivery pipeline.
  - fanout:  Broadcasts to ALL bound queues regardless of routing key.
             Used by the learning demo.
  - topic:   Routes using wildcard patterns (* = one word, # = zero or more words).
             Used by the learning demo. Enables flexible pub/sub.
  - headers: Routes based on message header attributes (not used here).

Topology used by the delivery pipeline:
  Exchange:   webhook.direct   (direct, durable)
  Queues:
    webhook.delivery           — main delivery queue
    webhook.retry.10s          — TTL 10s, dead-letters back to webhook.direct
    webhook.retry.60s          — TTL 60s, dead-letters back to webhook.direct
    webhook.retry.300s         — TTL 300s, dead-letters back to webhook.direct
    webhook.dead_letter        — terminal queue for exhausted deliveries
  Routing keys:
    webhook.deliver            — normal delivery
    webhook.retry.10s          — retry after 10s
    webhook.retry.60s          — retry after 60s
    webhook.retry.300s         — retry after 300s
    webhook.dead               — dead-letter destination

Retry mechanism:
  Instead of using a RabbitMQ delay plugin, we use TTL + dead-letter routing.
  A message published to webhook.retry.10s expires after 10,000 ms and is
  automatically routed to webhook.direct with routing key webhook.deliver.
  The worker then re-processes it as a normal delivery.

  This is a standard pattern when the RabbitMQ delayed message plugin is
  not available. It has the trade-off that messages in a TTL queue expire
  in FIFO order — a message behind a very long message must wait.
"""
import logging

import aio_pika
from aio_pika.abc import AbstractChannel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exchange names
# ---------------------------------------------------------------------------
EXCHANGE_WEBHOOK_DIRECT = "webhook.direct"
EXCHANGE_FANOUT_DEMO = "demo.fanout"
EXCHANGE_TOPIC_DEMO = "demo.topic"

# ---------------------------------------------------------------------------
# Queue names
# ---------------------------------------------------------------------------
QUEUE_DELIVERY = "webhook.delivery"
QUEUE_RETRY_10S = "webhook.retry.10s"
QUEUE_RETRY_60S = "webhook.retry.60s"
QUEUE_RETRY_300S = "webhook.retry.300s"
QUEUE_DEAD_LETTER = "webhook.dead_letter"

# Demo queues
QUEUE_DEMO_A = "demo.queue.a"
QUEUE_DEMO_B = "demo.queue.b"
QUEUE_DEMO_C = "demo.queue.c"

# ---------------------------------------------------------------------------
# Routing keys
# ---------------------------------------------------------------------------
ROUTING_KEY_DELIVER = "webhook.deliver"
ROUTING_KEY_RETRY_10S = "webhook.retry.10s"
ROUTING_KEY_RETRY_60S = "webhook.retry.60s"
ROUTING_KEY_RETRY_300S = "webhook.retry.300s"
ROUTING_KEY_DEAD = "webhook.dead"

# TTL values in milliseconds
TTL_10S = 10_000
TTL_60S = 60_000
TTL_300S = 300_000


async def declare_topology(channel: AbstractChannel) -> dict[str, object]:
    """
    Declare all exchanges, queues, and bindings required by the delivery pipeline.

    Idempotent: safe to call multiple times. If queues/exchanges already exist
    with matching parameters, RabbitMQ accepts the declaration without error.
    If parameters differ (e.g., different durability), RabbitMQ raises a 406 error.

    Returns a dict of named exchange objects for use by publishers.
    """
    logger.info("Declaring RabbitMQ topology")

    # -----------------------------------------------------------------------
    # Main exchange: direct, durable
    # -----------------------------------------------------------------------
    webhook_exchange = await channel.declare_exchange(
        EXCHANGE_WEBHOOK_DIRECT,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )

    # -----------------------------------------------------------------------
    # Main delivery queue: durable, persistent
    # -----------------------------------------------------------------------
    delivery_queue = await channel.declare_queue(
        QUEUE_DELIVERY,
        durable=True,
        arguments={
            # Dead-letter exchange for this queue (used if nack with requeue=False)
            "x-dead-letter-exchange": EXCHANGE_WEBHOOK_DIRECT,
            "x-dead-letter-routing-key": ROUTING_KEY_DEAD,
        },
    )
    await delivery_queue.bind(webhook_exchange, routing_key=ROUTING_KEY_DELIVER)

    # -----------------------------------------------------------------------
    # Retry queues with TTL and dead-letter routing back to the main exchange
    #
    # When a message in a retry queue expires (TTL), RabbitMQ routes it to
    # the dead-letter exchange (webhook.direct) with the dead-letter routing
    # key (webhook.deliver). The delivery queue receives it and the worker
    # processes it again.
    # -----------------------------------------------------------------------
    retry_10s_queue = await channel.declare_queue(
        QUEUE_RETRY_10S,
        durable=True,
        arguments={
            "x-message-ttl": TTL_10S,
            "x-dead-letter-exchange": EXCHANGE_WEBHOOK_DIRECT,
            "x-dead-letter-routing-key": ROUTING_KEY_DELIVER,
        },
    )
    await retry_10s_queue.bind(webhook_exchange, routing_key=ROUTING_KEY_RETRY_10S)

    retry_60s_queue = await channel.declare_queue(
        QUEUE_RETRY_60S,
        durable=True,
        arguments={
            "x-message-ttl": TTL_60S,
            "x-dead-letter-exchange": EXCHANGE_WEBHOOK_DIRECT,
            "x-dead-letter-routing-key": ROUTING_KEY_DELIVER,
        },
    )
    await retry_60s_queue.bind(webhook_exchange, routing_key=ROUTING_KEY_RETRY_60S)

    retry_300s_queue = await channel.declare_queue(
        QUEUE_RETRY_300S,
        durable=True,
        arguments={
            "x-message-ttl": TTL_300S,
            "x-dead-letter-exchange": EXCHANGE_WEBHOOK_DIRECT,
            "x-dead-letter-routing-key": ROUTING_KEY_DELIVER,
        },
    )
    await retry_300s_queue.bind(webhook_exchange, routing_key=ROUTING_KEY_RETRY_300S)

    # -----------------------------------------------------------------------
    # Dead-letter queue: terminal destination for exhausted deliveries
    # -----------------------------------------------------------------------
    dlq = await channel.declare_queue(QUEUE_DEAD_LETTER, durable=True)
    await dlq.bind(webhook_exchange, routing_key=ROUTING_KEY_DEAD)

    logger.info(
        "Webhook delivery topology declared",
        extra={
            "exchange": EXCHANGE_WEBHOOK_DIRECT,
            "queues": [
                QUEUE_DELIVERY,
                QUEUE_RETRY_10S,
                QUEUE_RETRY_60S,
                QUEUE_RETRY_300S,
                QUEUE_DEAD_LETTER,
            ],
        },
    )

    return {
        "webhook_exchange": webhook_exchange,
        "delivery_queue": delivery_queue,
        "retry_10s_queue": retry_10s_queue,
        "retry_60s_queue": retry_60s_queue,
        "retry_300s_queue": retry_300s_queue,
        "dead_letter_queue": dlq,
    }


async def declare_demo_topology(channel: AbstractChannel) -> dict[str, object]:
    """
    Declare demo exchanges and queues for learning exchange types.

    Used by experiments/exchange_demo.py to demonstrate:
    - Direct exchange routing
    - Fanout exchange broadcasting
    - Topic exchange pattern matching
    """
    # Fanout exchange
    fanout_exchange = await channel.declare_exchange(
        EXCHANGE_FANOUT_DEMO,
        aio_pika.ExchangeType.FANOUT,
        durable=False,  # Not durable — demo only
    )

    # Topic exchange
    topic_exchange = await channel.declare_exchange(
        EXCHANGE_TOPIC_DEMO,
        aio_pika.ExchangeType.TOPIC,
        durable=False,
    )

    # Demo queues
    queue_a = await channel.declare_queue(QUEUE_DEMO_A, durable=False)
    queue_b = await channel.declare_queue(QUEUE_DEMO_B, durable=False)
    queue_c = await channel.declare_queue(QUEUE_DEMO_C, durable=False)

    # Fanout: all queues receive all messages
    await queue_a.bind(fanout_exchange)
    await queue_b.bind(fanout_exchange)

    # Topic: queue_b gets "order.*", queue_c gets "#.critical"
    await queue_b.bind(topic_exchange, routing_key="order.*")
    await queue_c.bind(topic_exchange, routing_key="#.critical")

    return {
        "fanout_exchange": fanout_exchange,
        "topic_exchange": topic_exchange,
        "queue_a": queue_a,
        "queue_b": queue_b,
        "queue_c": queue_c,
    }

