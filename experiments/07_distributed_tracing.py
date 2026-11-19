#!/usr/bin/env python3
"""
Experiment 7: Distributed Tracing Inspection

Submits a delivery and queries Tempo to verify end-to-end trace propagation:
  API request
    -> PostgreSQL transaction
    -> Outbox publisher
    -> RabbitMQ (W3C headers injection)
    -> Delivery worker (W3C headers extraction)
    -> HTTP request to receiver

Run:
  python experiments/07_distributed_tracing.py
"""
import asyncio
import uuid
import httpx

API_URL = "http://localhost:8000"
TEMPO_API = "http://localhost:3200/api"


async def main():
    print("=== Experiment 7: Distributed Tracing Across Async Boundaries ===\n")
    correlation_id = str(uuid.uuid4())
    print(f"Submitting event with correlation_id: {correlation_id}")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{API_URL}/api/v1/events",
            json={
                "event_type": "experiment.trace",
                "payload": {"test": "distributed_trace"},
                "endpoint_url": "http://receiver:8001/webhooks/success",
            },
            headers={"X-Correlation-ID": correlation_id},
        )
        assert resp.status_code == 202
        data = resp.json()
        delivery_id = data["delivery_id"]
        event_id = data["event_id"]
        print(f"Event ID:    {event_id}")
        print(f"Delivery ID: {delivery_id}")

        print("\nWaiting 6 seconds for outbox publisher and delivery worker to execute...")
        await asyncio.sleep(6)

        # Check delivery status
        d_resp = await client.get(f"{API_URL}/api/v1/deliveries/{delivery_id}")
        delivery = d_resp.json()
        print(f"Delivery Status: {delivery['status']}")

        # Query Tempo search API if accessible
        print("\nChecking Grafana Tempo for traces...")
        try:
            tempo_resp = await client.get(
                f"{TEMPO_API}/search",
                params={"tags": f'delivery.id="{delivery_id}"'},
                timeout=5.0,
            )
            if tempo_resp.status_code == 200:
                print("Tempo Search Response:")
                print(tempo_resp.json())
            else:
                print(f"Tempo search returned status {tempo_resp.status_code}")
        except Exception as e:
            print(f"Note: Could not query Tempo API directly: {e}")

        print("\nTo visualize the full trace in Grafana UI:")
        print("  1. Open http://localhost:3000/explore")
        print("  2. Select 'Tempo' datasource")
        print(f"  3. Search for tag: delivery.id = {delivery_id}")
        print(f"  4. Or search for logs in Loki matching: trace_id or delivery_id = {delivery_id}")


if __name__ == "__main__":
    asyncio.run(main())
