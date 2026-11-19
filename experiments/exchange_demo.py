#!/usr/bin/env python3
"""
Exchange type demonstration.

Shows how direct, fanout, and topic exchanges route messages differently.
Useful for understanding RabbitMQ routing independent of the delivery pipeline.

Requirements: RabbitMQ running on localhost:5672

Run: python experiments/exchange_demo.py
"""
import asyncio
import json

import aio_pika

RABBITMQ_URL = "amqp://webhook:webhook@localhost:5672/"


async def demo_direct_exchange() -> None:
    """
    Direct exchange: routes message only to queues where routingKey == bindingKey.
    Think of it as a postal address — exact match only.
    """
    print("\n--- Direct Exchange Demo ---")
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        exchange = await channel.declare_exchange("demo.direct", aio_pika.ExchangeType.DIRECT, durable=False)
        q_a = await channel.declare_queue("demo.direct.a", durable=False)
        q_b = await channel.declare_queue("demo.direct.b", durable=False)

        await q_a.bind(exchange, routing_key="key.a")
        await q_b.bind(exchange, routing_key="key.b")

        await exchange.publish(aio_pika.Message(b"Hello queue A"), routing_key="key.a")
        await exchange.publish(aio_pika.Message(b"Hello queue B"), routing_key="key.b")
        await exchange.publish(aio_pika.Message(b"Goes nowhere"), routing_key="key.c")  # dropped

        msg_a = await q_a.get(fail=False)
        msg_b = await q_b.get(fail=False)

        print(f"  queue A received: {msg_a.body if msg_a else 'nothing'}")
        print(f"  queue B received: {msg_b.body if msg_b else 'nothing'}")
        print("  key.c message: silently dropped (no binding for key.c)")

        if msg_a:
            await msg_a.ack()
        if msg_b:
            await msg_b.ack()

        # Cleanup
        await q_a.delete()
        await q_b.delete()


async def demo_fanout_exchange() -> None:
    """
    Fanout exchange: broadcasts to ALL bound queues, ignoring routing key.
    Think of it as a broadcast announcement.
    """
    print("\n--- Fanout Exchange Demo ---")
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        exchange = await channel.declare_exchange("demo.fanout.ex", aio_pika.ExchangeType.FANOUT, durable=False)
        q_a = await channel.declare_queue("demo.fanout.a", durable=False)
        q_b = await channel.declare_queue("demo.fanout.b", durable=False)

        # For fanout, routing_key is ignored
        await q_a.bind(exchange)
        await q_b.bind(exchange)

        await exchange.publish(
            aio_pika.Message("Broadcast message -- goes everywhere".encode()),
            routing_key="ignored",
        )

        msg_a = await q_a.get(fail=False)
        msg_b = await q_b.get(fail=False)

        print(f"  queue A received: {msg_a.body if msg_a else 'nothing'}")
        print(f"  queue B received: {msg_b.body if msg_b else 'nothing'}")
        print("  Both queues received the same message (broadcast)!")

        if msg_a:
            await msg_a.ack()
        if msg_b:
            await msg_b.ack()

        await q_a.delete()
        await q_b.delete()


async def demo_topic_exchange() -> None:
    """
    Topic exchange: routes using wildcard patterns.
    * = exactly one word
    # = zero or more words

    Example bindings:
      queue_orders:  "order.*"    → matches order.created, order.shipped, NOT order.item.added
      queue_alerts:  "#.critical" → matches order.critical, ANY.OTHER.critical
    """
    print("\n--- Topic Exchange Demo ---")
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        exchange = await channel.declare_exchange("demo.topic.ex", aio_pika.ExchangeType.TOPIC, durable=False)
        q_orders = await channel.declare_queue("demo.topic.orders", durable=False)
        q_alerts = await channel.declare_queue("demo.topic.alerts", durable=False)

        await q_orders.bind(exchange, routing_key="order.*")       # one word after "order."
        await q_alerts.bind(exchange, routing_key="#.critical")    # any prefix, ends with .critical

        test_messages = [
            ("order.created", "New order event"),
            ("order.shipped", "Order shipped event"),
            ("order.item.added", "Multi-word — won't match order.*"),
            ("payment.critical", "Payment critical alert"),
            ("order.critical", "Goes to BOTH queues!"),
            ("user.login", "Random event — goes nowhere"),
        ]

        for routing_key, desc in test_messages:
            await exchange.publish(
                aio_pika.Message(json.dumps({"key": routing_key, "desc": desc}).encode()),
                routing_key=routing_key,
            )

        print("\n  Routing results:")
        # Drain queues
        while True:
            msg = await q_orders.get(fail=False)
            if not msg:
                break
            body = json.loads(msg.body)
            print(f"  [orders queue] key={body['key']}: {body['desc']}")
            await msg.ack()

        while True:
            msg = await q_alerts.get(fail=False)
            if not msg:
                break
            body = json.loads(msg.body)
            print(f"  [alerts queue] key={body['key']}: {body['desc']}")
            await msg.ack()

        await q_orders.delete()
        await q_alerts.delete()


async def main() -> None:
    print("=== RabbitMQ Exchange Type Demonstration ===")
    await demo_direct_exchange()
    await demo_fanout_exchange()
    await demo_topic_exchange()
    print("\n=== Demo complete ===")


if __name__ == "__main__":
    asyncio.run(main())

