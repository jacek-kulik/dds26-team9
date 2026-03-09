import asyncio
import aiohttp
import subprocess
from verify_consistency import verify
from reset_state import reset

reset()

BASE = "http://localhost:8000"
NUM_WORKERS = 50
TEST_DURATION = 20  # seconds


async def checkout_loop(worker_id: int, stop_event: asyncio.Event):
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not stop_event.is_set():
            try:
                async with session.post(f"{BASE}/orders/create/0") as r:
                    if r.status != 200:
                        continue
                    order = (await r.json())["order_id"]

                await session.post(f"{BASE}/orders/addItem/{order}/0/1")
                await session.post(f"{BASE}/orders/checkout/{order}")

            except Exception:
                await asyncio.sleep(0.1)


async def crash_controller():
    await asyncio.sleep(5)

    print("Killing payment service")
    subprocess.run(
        ["docker", "kill", "dds26-team9-payment-service-1"],
        check=False,
    )

    await asyncio.sleep(5)

    print("Restarting payment service")
    subprocess.run(
        ["docker", "start", "dds26-team9-payment-service-1"],
        check=False,
    )


async def main():
    stop_event = asyncio.Event()

    workers = [
        asyncio.create_task(checkout_loop(i, stop_event))
        for i in range(NUM_WORKERS)
    ]
    controller = asyncio.create_task(crash_controller())

    await asyncio.sleep(TEST_DURATION)
    stop_event.set()

    await asyncio.gather(*workers, return_exceptions=True)
    await controller

    print("Crash test finished.")

    if verify():
        print("CRASH TEST PASS")
    else:
        print("CRASH TEST FAIL")


if __name__ == "__main__":
    asyncio.run(main())