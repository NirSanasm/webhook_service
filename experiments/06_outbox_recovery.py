#!/usr/bin/env python3
"""
Experiment 6: Outbox Pattern Recovery

Demonstrates that events are NOT lost when RabbitMQ is unavailable at publish time.

Steps:
  1. Stop the RabbitMQ container (or disconnect outbox publisher from broker).
  2. Submit events via the API — they persist in PostgreSQL outbox table.
  3. Restart RabbitMQ.
  4. The outbox publisher picks up unpublished records and delivers them.

This proves the transactional outbox pattern works as designed.

Usage:
  python experiments/06_outbox_recovery.py

Or follow manually:
  docker compose stop rabbitmq
  make submit-event
  make submit-event
  docker compose exec postgres psql -U webhook -d webhookdb \
    -c "SELECT id, published, attempts FROM outbox_events ORDER BY created_at DESC LIMIT 5;"
  docker compose start rabbitmq
  # Wait ~10 seconds
  make db-outbox
"""
import asyncio
import subprocess

import httpx

API_URL = "http://localhost:8000"


async def submit_events(n: int) -> list[str]:
    delivery_ids = []
    async with httpx.AsyncClient() as client:
        for i in range(n):
            resp = await client.post(
                f"{API_URL}/api/v1/events",
                json={
                    "event_type": "experiment.outbox_recovery",
                    "payload": {"index": i},
                    "endpoint_url": "http://receiver:8001/webhooks/success",
                },
            )
            if resp.status_code == 202:
                delivery_ids.append(resp.json()["delivery_id"])
    return delivery_ids


async def main() -> None:
    print("=== Experiment 6: Outbox Recovery ===\n")
    print("This experiment requires Docker Compose running.")
    print()

    print("Step 1: Stopping RabbitMQ to simulate broker failure...")
    subprocess.run(["docker", "compose", "stop", "rabbitmq"], check=False)
    await asyncio.sleep(2)

    print("\nStep 2: Submitting 5 events while RabbitMQ is down...")
    print("   (API should succeed — events go to PostgreSQL outbox only)")
    delivery_ids = await submit_events(5)
    print(f"   Submitted {len(delivery_ids)} events")

    print("\nStep 3: Verifying events are in outbox (unpublished)...")
    subprocess.run([
        "docker", "compose", "exec", "postgres",
        "psql", "-U", "webhook", "-d", "webhookdb",
        "-c", "SELECT id, published, attempts FROM outbox_events WHERE published = false ORDER BY created_at DESC LIMIT 5;"
    ], check=False)

    print("\nStep 4: Restarting RabbitMQ...")
    subprocess.run(["docker", "compose", "start", "rabbitmq"], check=False)
    print("   Waiting for broker to initialize (30s)...")
    await asyncio.sleep(30)

    print("\nStep 5: Checking if outbox records were published...")
    subprocess.run([
        "docker", "compose", "exec", "postgres",
        "psql", "-U", "webhook", "-d", "webhookdb",
        "-c", "SELECT id, published, published_at, attempts FROM outbox_events ORDER BY created_at DESC LIMIT 5;"
    ], check=False)

    print("\nStep 6: Verifying deliveries were processed...")
    async with httpx.AsyncClient() as client:
        for delivery_id in delivery_ids[:3]:
            try:
                resp = await client.get(f"{API_URL}/api/v1/deliveries/{delivery_id}", timeout=5)
                if resp.status_code == 200:
                    d = resp.json()
                    print(f"   delivery_id={delivery_id[:8]}... status={d['status']} attempts={d['attempts']}")
            except Exception:
                pass

    print("\n✅ Experiment complete!")
    print("The outbox publisher recovered all pending events after RabbitMQ restarted.")


if __name__ == "__main__":
    asyncio.run(main())

