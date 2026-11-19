#!/usr/bin/env python3
"""
Experiment 4: Queue Backlog & Worker Concurrency Scaling

Demonstrates queue growth under load and how consumer concurrency impacts drain rate.

Workflow:
  1. Measure initial queue depth.
  2. Publish a batch of events (e.g. 500 or 10,000) rapidly to `webhook.delivery`.
  3. Observe queue backlog grow.
  4. Measure queue consumption rate with single vs scaled workers.

Run:
  python experiments/04_queue_backlog.py --count 500
"""
import argparse
import asyncio
import json
import time
import uuid

import aio_pika
import httpx

RABBITMQ_API = "http://localhost:15672/api"
RABBITMQ_AUTH = ("webhook", "webhook")
QUEUE_NAME = "webhook.delivery"
API_URL = "http://localhost:8000"


async def get_queue_depth() -> dict:
    """Fetch queue message count and consumer count from RabbitMQ Management API."""
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
                    "messages_ready": data.get("messages_ready", 0),
                    "messages_unacknowledged": data.get("messages_unacknowledged", 0),
                    "consumers": data.get("consumers", 0),
                }
        except Exception as e:
            print(f"Warning: Could not query RabbitMQ API: {e}")
    return {"messages_ready": -1, "messages_unacknowledged": -1, "consumers": -1}


async def submit_batch(count: int, concurrency: int = 25) -> None:
    print(f"Submitting {count} webhook events via API...")
    sem = asyncio.Semaphore(concurrency)
    success = 0
    failed = 0

    async with httpx.AsyncClient() as client:
        async def submit_one(i: int):
            nonlocal success, failed
            async with sem:
                try:
                    resp = await client.post(
                        f"{API_URL}/api/v1/events",
                        json={
                            "event_type": "experiment.backlog",
                            "payload": {"seq": i, "timestamp": time.time()},
                            "endpoint_url": "http://receiver:8001/webhooks/success",
                        },
                        timeout=10.0,
                    )
                    if resp.status_code == 202:
                        success += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

        tasks = [submit_one(i) for i in range(count)]
        await asyncio.gather(*tasks)

    print(f"Finished submission: {success} accepted, {failed} failed.")


async def monitor_drain(interval: float = 2.0, timeout: float = 120.0) -> None:
    print(f"\nMonitoring queue depth on '{QUEUE_NAME}' until drained...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        stats = await get_queue_depth()
        ready = stats["messages_ready"]
        unacked = stats["messages_unacknowledged"]
        consumers = stats["consumers"]
        elapsed = round(time.time() - start_time, 1)

        print(
            f"[{elapsed:>5}s] Ready: {ready:>5} | Unacked: {unacked:>4} | Active Consumers: {consumers:>2}"
        )

        if ready == 0 and unacked == 0 and elapsed > 5:
            print("\nQueue fully drained! Backlog cleared successfully.")
            return

        await asyncio.sleep(interval)

    print("\nMonitoring timed out before queue completely emptied.")


async def main():
    parser = argparse.ArgumentParser(description="Queue Backlog Experiment")
    parser.add_argument("--count", type=int, default=200, help="Number of messages to publish")
    parser.add_argument("--concurrency", type=int, default=25, help="API submission concurrency")
    args = parser.parse_args()

    print("=== Experiment 4: Queue Backlog & Worker Concurrency Scaling ===")
    initial_stats = await get_queue_depth()
    print(f"Initial State -> Ready: {initial_stats['messages_ready']}, Consumers: {initial_stats['consumers']}")

    await submit_batch(args.count, args.concurrency)
    await monitor_drain()


if __name__ == "__main__":
    asyncio.run(main())
