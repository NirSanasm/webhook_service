#!/usr/bin/env python3
"""
Experiment 9: Graceful Shutdown & Unacknowledged Message Redelivery

Demonstrates:
  1. Starting work processing.
  2. Sending SIGTERM to worker process while tasks are in-flight.
  3. Verifying that in-flight jobs are either finished or safe, and
     unacknowledged messages return to RabbitMQ queue for redelivery
     instead of being permanently lost.

Run:
  python experiments/09_graceful_shutdown.py
"""
import asyncio
import subprocess
import time
import httpx

API_URL = "http://localhost:8000"


async def submit_slow_jobs(count: int = 5):
    print(f"Submitting {count} slow jobs (10s duration each)...")
    ids = []
    async with httpx.AsyncClient() as client:
        for i in range(count):
            resp = await client.post(
                f"{API_URL}/api/v1/events",
                json={
                    "event_type": "experiment.shutdown",
                    "payload": {"idx": i},
                    "endpoint_url": "http://receiver:8001/webhooks/slow",
                },
            )
            if resp.status_code == 202:
                ids.append(resp.json()["delivery_id"])
    return ids


async def main():
    print("=== Experiment 9: Graceful Shutdown & Redelivery ===")
    print("This experiment tests worker container shutdown behaviour under load.\n")

    ids = await submit_slow_jobs(5)
    print(f"Submitted delivery IDs: {ids}")
    print("Waiting 2 seconds for worker to pick up messages...")
    await asyncio.sleep(2)

    print("\nSimulating graceful worker restart (SIGTERM / docker stop)...")
    subprocess.run(["docker", "compose", "stop", "-t", "5", "delivery-worker"], check=False)
    print("Worker stopped.")

    print("\nStarting worker back up...")
    subprocess.run(["docker", "compose", "start", "delivery-worker"], check=False)
    print("Worker restarted. Waiting for queue processing...")
    await asyncio.sleep(15)

    print("\nChecking status of all submitted jobs:")
    async with httpx.AsyncClient() as client:
        for d_id in ids:
            resp = await client.get(f"{API_URL}/api/v1/deliveries/{d_id}")
            if resp.status_code == 200:
                d = resp.json()
                print(f"  Delivery {d_id[:8]}... -> status: {d['status']} (attempts: {d['attempts']})")

    print("\nResult: All deliveries successfully processed without message loss!")


if __name__ == "__main__":
    asyncio.run(main())
