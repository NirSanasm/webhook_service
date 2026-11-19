#!/usr/bin/env python3
"""
Experiment 10: Load Test

Measures delivery throughput, latency, and system behaviour under load.

Usage:
  python experiments/load_test.py --count 1000 --concurrency 50

Metrics to observe:
  - Requests per second (API submission rate)
  - Successful deliveries per second
  - Queue backlog in RabbitMQ
  - Worker processing latency (P50, P95)
  - Failure rate

Check in Grafana (http://localhost:3000) or Prometheus (http://localhost:9090).

Example PromQL queries to run during the test:
  rate(webhook_deliveries_succeeded_total[1m])
  rate(webhook_deliveries_failed_total[1m])
  histogram_quantile(0.95, rate(webhook_delivery_duration_seconds_bucket[1m]))
"""
import argparse
import asyncio
import time
from dataclasses import dataclass, field

import httpx

API_URL = "http://localhost:8000"


@dataclass
class Stats:
    submitted: int = 0
    succeeded: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def rps(self) -> float:
        return self.submitted / self.elapsed if self.elapsed > 0 else 0


async def submit_event(client: httpx.AsyncClient, stats: Stats, endpoint: str) -> None:
    try:
        resp = await client.post(
            f"{API_URL}/api/v1/events",
            json={
                "event_type": "load.test",
                "payload": {"ts": time.time()},
                "endpoint_url": endpoint,
            },
            timeout=10,
        )
        if resp.status_code == 202:
            stats.submitted += 1
        else:
            stats.failed += 1
            stats.errors.append(f"HTTP {resp.status_code}")
    except Exception as e:
        stats.failed += 1
        stats.errors.append(str(e)[:50])


async def run_load_test(count: int, concurrency: int, endpoint: str) -> None:
    print(f"\n=== Load Test: {count} events, concurrency={concurrency} ===")
    print(f"Endpoint: {endpoint}\n")

    stats = Stats()
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded_submit(client: httpx.AsyncClient) -> None:
        async with semaphore:
            await submit_event(client, stats, endpoint)

    print_interval = max(1, count // 10)

    async with httpx.AsyncClient() as client:
        tasks = [asyncio.create_task(bounded_submit(client)) for _ in range(count)]

        for i, task in enumerate(asyncio.as_completed(tasks)):
            await task
            if (i + 1) % print_interval == 0:
                print(f"  Progress: {i+1}/{count} | RPS: {stats.rps:.1f} | "
                      f"Submitted: {stats.submitted} | Failed: {stats.failed}")

    print(f"\n=== Results ===")
    print(f"  Total submitted: {stats.submitted}/{count}")
    print(f"  Failed submissions: {stats.failed}")
    print(f"  Elapsed: {stats.elapsed:.1f}s")
    print(f"  Average RPS: {stats.rps:.1f}")

    if stats.errors:
        unique_errors = set(stats.errors[:10])
        print(f"  Sample errors: {', '.join(unique_errors)}")

    print("\nNow watch delivery processing in Grafana or:")
    print("  make deliveries")
    print("  make rabbitmq-queues")


def main() -> None:
    parser = argparse.ArgumentParser(description="Webhook Delivery Lab Load Test")
    parser.add_argument("--count", type=int, default=100, help="Number of events to submit")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent submissions")
    parser.add_argument(
        "--endpoint",
        default="http://receiver:8001/webhooks/success",
        help="Target endpoint URL",
    )
    args = parser.parse_args()

    asyncio.run(run_load_test(args.count, args.concurrency, args.endpoint))


if __name__ == "__main__":
    main()

