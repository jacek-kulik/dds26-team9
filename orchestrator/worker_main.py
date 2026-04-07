import asyncio
import logging
import os
import signal

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker-main")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "orchestrator")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "6"))


async def main():
    from app import worker, db, messaging, PROTOCOL

    if PROTOCOL == "SAGA":
        from app import recover_incomplete_saga
    elif PROTOCOL == "2PC":
        from app import recover_incomplete_2pc

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

    if PROTOCOL == "SAGA":
        t = asyncio.create_task(recover_incomplete_saga(), name="saga-recovery")
        tasks.append(t)
        logger.info("Saga recovery task started")

    elif PROTOCOL == "2PC":
        t = asyncio.create_task(recover_incomplete_2pc(), name="2pc-recovery")
        tasks.append(t)
        logger.info("2PC recovery task started")

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