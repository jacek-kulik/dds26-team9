import asyncio
import aiohttp
from verify_consistency import verify
from reset_state import reset

reset()

BASE = "http://localhost:5000"
CONCURRENT = 500


async def checkout(session):

    async with session.post(f"{BASE}/orders/create/0") as r:
        order = (await r.json())["order_id"]

    await session.post(f"{BASE}/orders/addItem/{order}/0/1")

    async with session.post(f"{BASE}/orders/checkout/{order}") as r:
        return r.status


async def main():

    async with aiohttp.ClientSession() as session:

        tasks = [checkout(session) for _ in range(CONCURRENT)]

        results = await asyncio.gather(*tasks)

        success = results.count(200)
        fail = results.count(400)

        print("Successful orders:", success)
        print("Failed orders:", fail)

    if verify():
        print("CONTENTION TEST PASS")
    else:
        print("CONTENTION TEST FAIL")


asyncio.run(main())