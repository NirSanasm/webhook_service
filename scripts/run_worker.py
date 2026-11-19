#!/usr/bin/env python3
"""
Entry point for the delivery worker process.
"""
import asyncio
import logging
import signal

from app.workers.delivery_worker import run_worker

logger = logging.getLogger(__name__)


async def main() -> None:
    loop = asyncio.get_running_loop()

    shutdown_event = asyncio.Event()

    def handle_shutdown(sig: signal.Signals) -> None:
        logger.info(f"Received {sig.name}, initiating graceful shutdown")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_shutdown, sig)

    worker_task = asyncio.create_task(run_worker())

    # Wait for shutdown signal
    await shutdown_event.wait()
    logger.info("Shutting down delivery worker...")

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    from app.messaging.connection import close_connection
    await close_connection()
    logger.info("Delivery worker stopped")


if __name__ == "__main__":
    asyncio.run(main())

