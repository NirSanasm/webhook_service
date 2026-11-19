#!/usr/bin/env python3
"""
Experiment 8: Observability Under Failure

Triggers real system failures and demonstrates how to correlate:
  - Error logs in Loki
  - Error metrics in Prometheus (webhook_deliveries_failed_total, webhook_dead_lettered_total)
  - Errored spans and exceptions in Tempo

Failure Scenarios demonstrated:
  1. HTTP 500 error from receiver -> retry scheduled.
  2. Permanent HTTP 400 error from receiver -> non-retryable, immediate DLQ.
  3. Rate limit HTTP 429 with Retry-After header.

Run:
  python experiments/08_observability_failure.py
"""
import asyncio
import uuid
import httpx

API_URL = "http://localhost:8000"


async def trigger_failure(client: httpx.AsyncClient, name: str, endpoint: str):
    print(f"\n--- Scenario: {name} ---")
    corr_id = str(uuid.uuid4())
    resp = await client.post(
        f"{API_URL}/api/v1/events",
        json={
            "event_type": f"fail.{name}",
            "payload": {"reason": name},
            "endpoint_url": endpoint,
        },
        headers={"X-Correlation-ID": corr_id},
    )
    if resp.status_code == 202:
        delivery_id = resp.json()["delivery_id"]
        print(f"Submitted delivery_id={delivery_id} (correlation_id={corr_id})")
        return delivery_id
    else:
        print(f"Failed to submit: {resp.status_code}")
        return None


async def main():
    print("=== Experiment 8: Observability Under Failure ===")
    async with httpx.AsyncClient() as client:
        # 1. Server error (500)
        d1 = await trigger_failure(client, "server_error", "http://receiver:8001/webhooks/fail")
        # 2. Client error (400 - permanent)
        d2 = await trigger_failure(client, "client_error", "http://receiver:8001/webhooks/invalid")
        # 3. Rate limit (429)
        d3 = await trigger_failure(client, "rate_limited", "http://receiver:8001/webhooks/rate-limit")

        print("\nWaiting 10 seconds for worker and outbox publisher processing...")
        await asyncio.sleep(10)

        for name, d_id in [("500 Server Error", d1), ("400 Client Error", d2), ("429 Rate Limit", d3)]:
            if d_id:
                resp = await client.get(f"{API_URL}/api/v1/deliveries/{d_id}")
                d = resp.json()
                print(f"[{name}] status={d['status']}, attempts={d['attempts']}, last_error={d.get('last_error')}")

    print("\nHow to inspect these failures across the Observability Pillars:")
    print("1. Metrics (Prometheus / Grafana):")
    print("   Query: rate(webhook_deliveries_failed_total[5m])")
    print("   Query: webhook_dead_lettered_total")
    print("2. Logs (Loki / Grafana):")
    print('   Query: {service="webhook-worker"} |= "error" or {service="webhook-worker"} |= "failed"')
    print("3. Traces (Tempo / Grafana):")
    print('   Search status: error=true, inspect HTTP status codes and exception stack traces.')


if __name__ == "__main__":
    asyncio.run(main())
