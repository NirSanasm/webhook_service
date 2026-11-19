#!/usr/bin/env python3
"""
Experiment 1: Consumer Crash & Message Redelivery

Demonstrates at-least-once delivery in RabbitMQ.

What happens:
  1. Publish a message to the delivery queue.
  2. Start consuming — receive the message.
  3. Kill the consumer (simulate crash) BEFORE acking.
  4. Restart the consumer — observe the message is redelivered.
  5. On redelivery: message.redelivered == True.

RabbitMQ guarantees:
  If a consumer receives a message but does NOT send an ack before
  disconnecting, the message becomes ready again (after consumer timeout)
  and is redelivered to the next consumer.

Run:
  python experiments/01_consumer_crash.py

Expected output:
  [1] Message received, delivery_id=... redelivered=False
  [1] Simulating crash — NOT acking...
  ^C
  [Restart]
  [2] Message received, delivery_id=... redelivered=True
  [2] Processing complete, acking.
"""
import asyncio
import json
import signal
import sys
import uuid

import aio_pika

RABBITMQ_URL = "amqp://webhook:webhook@localhost:5672/"
QUEUE_NAME = "webhook.delivery"
EXCHANGE_NAME = "webhook.direct"


async def publish_test_message(delivery_id: str) -> None:
    """Publish a test delivery message."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
        )
        body = json.dumps({
            "delivery_id": delivery_id,
            "event_id": str(uuid.uuid4()),
            "endpoint_url": "http://localhost:8001/webhooks/success",
            "event_type": "experiment.crash",
            "max_attempts": 5,
        }).encode()
        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await exchange.publish(message, routing_key="webhook.deliver")
        print(f"Published test message, delivery_id={delivery_id}")


async def consume_one_and_crash(delivery_id: str) -> None:
    """Consume one message, then crash without acking."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(QUEUE_NAME, durable=True, passive=True)

    print(f"\n[Crash experiment] Waiting for message (delivery_id={delivery_id})...")
    print("Press Ctrl+C to simulate a worker crash AFTER receiving the message.\n")

    async with queue.iterator() as q:
        async for message in q:
            body = json.loads(message.body)
            msg_delivery_id = body.get("delivery_id")
            print(f"[1] Message received, delivery_id={msg_delivery_id} redelivered={message.redelivered}")
            print("[1] Simulating crash — NOT acking. Press Ctrl+C now...")

            # Hold the message without acking — this simulates the worker crashing
            try:
                await asyncio.sleep(3600)  # Hold until killed
            except (asyncio.CancelledError, KeyboardInterrupt):
                print("\n[Crash!] Connection closed without ack.")
                await connection.close()
                print("[Crash!] Reconnect and run this script again to see redelivery.")
                return


async def consume_and_ack() -> None:
    """Consume the redelivered message and acknowledge it."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(QUEUE_NAME, durable=True, passive=True)

    print("\n[Recovery] Waiting for redelivered message...")
    async with queue.iterator() as q:
        async for message in q:
            body = json.loads(message.body)
            delivery_id = body.get("delivery_id")
            print(f"[2] Message received, delivery_id={delivery_id} redelivered={message.redelivered}")

            if message.redelivered:
                print("[2] ✅ Confirmed: this is a redelivered message (at-least-once delivery)")
            else:
                print("[2] ⚠️  Not marked as redelivered — was this a fresh message?")

            await message.ack()
            print("[2] Message acknowledged. Experiment complete.")
            await connection.close()
            return


async def main() -> None:
    delivery_id = str(uuid.uuid4())

    if "--recover" in sys.argv:
        await consume_and_ack()
    elif "--publish" in sys.argv:
        await publish_test_message(delivery_id)
    else:
        print("Usage:")
        print("  Step 1: python experiments/01_consumer_crash.py --publish")
        print("  Step 2: python experiments/01_consumer_crash.py  (then Ctrl+C)")
        print("  Step 3: python experiments/01_consumer_crash.py --recover")


if __name__ == "__main__":
    asyncio.run(main())

