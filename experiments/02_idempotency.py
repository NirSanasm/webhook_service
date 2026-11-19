#!/usr/bin/env python3
"""
Experiment 2: Duplicate Messages & Idempotency

Demonstrates why idempotency is necessary even with RabbitMQ.

Scenario:
  1. Submit the same delivery_id to the worker multiple times.
  2. First delivery: receiver processes it and returns 200.
  3. Second delivery (same delivery_id): receiver detects duplicate and
     returns 200 without re-processing (idempotent behaviour).

Why duplicates happen even with manual acks and publisher confirms:
  - Publisher confirms guarantee the BROKER received the message.
  - Manual acks guarantee the CONSUMER processed the message.
  - But between the worker's HTTP success and the ack send, the worker
    can crash. RabbitMQ redelivers the message. The receiver sees it again.
  
  This is the fundamental at-least-once delivery trade-off.

Run:
  python experiments/02_idempotency.py
"""
import asyncio
import json
import uuid

import httpx

API_URL = "http://localhost:8000"
RECEIVER_URL = "http://localhost:8001"


async def main() -> None:
    print("=== Experiment 2: Duplicate Message Idempotency ===\n")

    async with httpx.AsyncClient() as client:
        # Step 1: Submit one event
        print("Step 1: Submitting webhook event...")
        response = await client.post(
            f"{API_URL}/api/v1/events",
            json={
                "event_type": "experiment.idempotency",
                "payload": {"test": "duplicate_detection"},
                "endpoint_url": "http://receiver:8001/webhooks/success",
            },
        )
        assert response.status_code == 202, f"Expected 202, got {response.status_code}"
        data = response.json()
        delivery_id = data["delivery_id"]
        print(f"  → delivery_id: {delivery_id}")
        print("  → Waiting 10 seconds for first delivery to complete...")
        await asyncio.sleep(10)

        # Step 2: Check first delivery succeeded
        delivery_resp = await client.get(f"{API_URL}/api/v1/deliveries/{delivery_id}")
        delivery = delivery_resp.json()
        print(f"  → Delivery status: {delivery['status']}")
        print(f"  → Attempts: {delivery['attempts']}")

        # Step 3: Check receiver received exactly one delivery
        received_resp = await client.get(f"{RECEIVER_URL}/webhooks/received")
        received = received_resp.json()
        print(f"\nStep 2: Receiver stats: {received['total']} deliveries received")

        # Step 4: Simulate duplicate by replaying the delivery
        print(f"\nStep 3: Replaying delivery {delivery_id} (simulates duplicate)...")
        replay_resp = await client.post(
            f"{API_URL}/api/v1/deliveries/{delivery_id}/replay",
            json={"force": True},
        )
        print(f"  → Replay response: {replay_resp.status_code}")
        print("  → Waiting 10 seconds for replay delivery...")
        await asyncio.sleep(10)

        # Step 5: Check receiver again — should still show idempotent handling
        received_resp2 = await client.get(f"{RECEIVER_URL}/webhooks/received")
        received2 = received_resp2.json()
        print(f"\nStep 4: Receiver stats after replay: {received2['total']} deliveries received")
        
        # Find our delivery
        ours = [d for d in received2["deliveries"] if delivery_id in str(d)]
        print(f"  → Our delivery appeared {len(ours)} time(s) in receiver log")
        
        if len(ours) == 1:
            print("\n✅ Idempotency working: receiver processed the delivery only ONCE")
        else:
            print(f"\n⚠️  Receiver processed the delivery {len(ours)} times")
            print("   This demonstrates why receiver-side idempotency is critical.")

    print("\nExperiment complete.")
    print("Observe the 'already_processed' responses in the receiver logs:")
    print("  docker compose logs receiver | grep idempotency")


if __name__ == "__main__":
    asyncio.run(main())

