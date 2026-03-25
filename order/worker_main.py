import asyncio
import logging
import os
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker-main")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "4"))

#worker class, imports the worker loop functionality from the app
async def main():
    if SERVICE_NAME == "order":
        from app import worker, db, messaging
    elif SERVICE_NAME == "stock":
        from app import worker, db, messaging
    elif SERVICE_NAME == "payment":
        from app import worker, db, messaging
    else:
        logger.error(f"Unknown SERVICE_NAME: {SERVICE_NAME}")
        sys.exit(1)

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _signal_handler():
        logger.info("Shutdown signal received")
        messaging.request_shutdown()
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    tasks = []
    for i in range(WORKER_COUNT):
        t = asyncio.create_task(
            worker(f"{SERVICE_NAME}-worker-{os.getpid()}-{i}"),
            name=f"{SERVICE_NAME}-consumer-{i}",
        )
        tasks.append(t)
    logger.info(f"Started {WORKER_COUNT} consumer tasks for {SERVICE_NAME}")


    await stop.wait()

    logger.info("Draining workers...")
    _, pending = await asyncio.wait(tasks, timeout=10)
    for t in pending:
        t.cancel()

    await db.aclose()
    await messaging.close()
    logger.info("Worker process exited cleanly")


if __name__ == "__main__":
    asyncio.run(main())