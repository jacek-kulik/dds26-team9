import asyncio
import aiohttp
import time
from verify_consistency import verify
from reset_state import reset

reset()

BASE = "http://localhost:5000"
REQUESTS = 1000


async def checkout(session):

    async with session.post(f"{BASE}/orders/create/0") as r:
        order = (await r.json())["order_id"]

    await session.post(f"{BASE}/orders/addItem/{order}/0/1")
    await session.post(f"{BASE}/orders/checkout/{order}")


async def main():

    async with aiohttp.ClientSession() as session:

        start = time.time()

        tasks = [checkout(session) for _ in range(REQUESTS)]

        await asyncio.gather(*tasks)

        duration = time.time() - start

        print("Throughput:", REQUESTS / duration, "tx/sec")

    if verify():
        print("THROUGHPUT TEST PASS")
    else:
        print("THROUGHPUT TEST FAIL")


asyncio.run(main())