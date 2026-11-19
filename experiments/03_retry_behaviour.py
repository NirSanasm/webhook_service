#!/usr/bin/env python3
"""
Experiment 3: Retry Behaviour with Flaky Receiver

Demonstrates the exponential backoff retry strategy.

The flaky receiver fails N times before succeeding.
You can observe:
  1. Initial delivery attempt fails (HTTP 500)
  2. Message routed to webhook.retry.10s queue
  3. After 10 seconds, message moves back to webhook.delivery queue
  4. Second attempt may fail again → webhook.retry.60s
  5. Eventually succeeds or reaches DLQ

Watch in real time:
  make logs-worker
  make logs-receiver

Configure flakiness:
  docker compose exec receiver env RECEIVER_FLAKY_FAIL_COUNT=4 \
    uvicorn receiver.main:app --host 0.0.0.0 --port 8001
"""
import asyncio

import httpx

API_URL = "http://localhost:8000"


async def main() -> None:
    print("=== Experiment 3: Retry Behaviour ===\n")
    print("Submitting to flaky endpoint (fails 3 times, then succeeds)...")
    print("Watch: docker compose logs -f delivery-worker receiver\n")

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_URL}/api/v1/events",
            json={
                "event_type": "order.flaky",
                "payload": {"experiment": "retry_behaviour"},
                "endpoint_url": "http://receiver:8001/webhooks/flaky",
            },
        )
        assert response.status_code == 202
        data = response.json()
        delivery_id = data["delivery_id"]
        print(f"Delivery ID: {delivery_id}")
        print("\nPolling delivery status every 15 seconds...\n")

        for i in range(30):  # Poll for up to 7.5 minutes
            await asyncio.sleep(15)
            dr = await client.get(f"{API_URL}/api/v1/deliveries/{delivery_id}")
            d = dr.json()
            print(f"  [{i*15:3d}s] status={d['status']:15s} attempts={d['attempts']} "
                  f"last_error={d.get('last_error', '-')[:50] if d.get('last_error') else '-'}")

            if d["status"] in ("delivered", "dead_lettered"):
                print(f"\n{'✅' if d['status'] == 'delivered' else '💀'} Final status: {d['status']}")
                print(f"   Total attempts: {d['attempts']}")
                if d["status"] == "delivered":
                    print("   Delivery succeeded after retries!")
                else:
                    print("   All retries exhausted — check DLQ:")
                    print("   make dlq-info")
                break


if __name__ == "__main__":
    asyncio.run(main())

