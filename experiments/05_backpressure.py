#!/usr/bin/env python3
"""
Experiment 5: Backpressure & Slow Consumers

Demonstrates how slow consumers and prefetch count create backpressure in RabbitMQ.

Concepts:
  1. Consumer Prefetch:
     By setting `channel.set_qos(prefetch_count=N)`, RabbitMQ will not deliver
     more than N unacknowledged messages to a given worker.
  2. Slow Downstream:
     When the webhook destination (or worker) is slow, the unacknowledged queue
     fills up to the prefetch limit.
  3. Upstream Buffering:
     RabbitMQ buffers remaining messages safely in the durable queue (`messages_ready`),
     preventing the worker process from running out of memory.

Workflow:
  1. Submit 50 webhook events targeted at the slow receiver (`/webhooks/slow`).
  2. Observe the worker processing only up to `WORKER_CONCURRENCY` / `RABBITMQ_PREFETCH_COUNT`
     jobs at any one time.
  3. Observe RabbitMQ queue statistics showing ready messages held safely in the broker.

Run:
  python experiments/05_backpressure.py --count 50
"""
import argparse
import asyncio
import time
import httpx

API_URL = "http://localhost:8000"
RABBITMQ_API = "http://localhost:15672/api"
RABBITMQ_AUTH = ("webhook", "webhook")
QUEUE_NAME = "webhook.delivery"


async def get_queue_stats() -> dict:
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{RABBITMQ_API}/queues/%2F/{QUEUE_NAME}",
                auth=RABBITMQ_AUTH,
                timeout=5.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "ready": data.get("messages_ready", 0),
                    "unacked": data.get("messages_unacknowledged", 0),
                    "consumers": data.get("consumers", 0),
                }
        except Exception as e:
            print(f"Warning: Could not contact RabbitMQ Management API: {e}")
    return {"ready": -1, "unacked": -1, "consumers": -1}


async def submit_slow_events(count: int) -> None:
    print(f"Submitting {count} events targeting the slow receiver (/webhooks/slow)...")
    async with httpx.AsyncClient() as client:
        for i in range(count):
            resp = await client.post(
                f"{API_URL}/api/v1/events",
                json={
                    "event_type": "experiment.backpressure",
                    "payload": {"batch_id": "slow_batch", "item": i},
                    "endpoint_url": "http://receiver:8001/webhooks/slow",
                },
                timeout=10.0,
            )
            if resp.status_code != 202:
                print(f"  Error submitting event {i}: HTTP {resp.status_code}")
    print(f"Submitted {count} slow events.")


async def monitor_backpressure(duration_seconds: int = 60, poll_interval: float = 3.0) -> None:
    print("\nObserving backpressure in RabbitMQ (Ready vs In-flight/Unacked):")
    print(f"{'Time':>6} | {'Ready in Broker':>17} | {'In-flight (Unacked)':>20} | {'Consumers':>10}")
    print("-" * 62)
    start = time.time()
    while time.time() - start < duration_seconds:
        elapsed = int(time.time() - start)
        stats = await get_queue_stats()
        print(
            f"{elapsed:>5}s | {stats['ready']:>17} | {stats['unacked']:>20} | {stats['consumers']:>10}"
        )
        if stats["ready"] == 0 and stats["unacked"] == 0 and elapsed > 5:
            print("\nAll slow requests completed!")
            break
        await asyncio.sleep(poll_interval)


async def main():
    parser = argparse.ArgumentParser(description="Backpressure Experiment")
    parser.add_argument("--count", type=int, default=25, help="Number of slow events to submit")
    args = parser.parse_args()

    print("=== Experiment 5: Backpressure & Slow Consumers ===")
    await submit_slow_events(args.count)
    await monitor_backpressure()


if __name__ == "__main__":
    asyncio.run(main())
