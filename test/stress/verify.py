import asyncio
import os
import json
import logging
from typing import List

import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Atomicity Check")

# 🔥 MUST MATCH YOUR INIT
NUMBER_OF_ITEMS = 100_000
ITEM_STARTING_STOCK = 1_000_000
ITEM_PRICE = 1
NUMBER_OF_USERS = 100_000
USER_STARTING_CREDIT = 1_000_000

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URLS_PATH = os.path.join(BASE_DIR, "urls.json")

with open(URLS_PATH) as f:
    urls = json.load(f)

ORDER_URL = urls['ORDER_URL']
PAYMENT_URL = urls['PAYMENT_URL']
STOCK_URL = urls['STOCK_URL']

# 🔥 control concurrency (VERY IMPORTANT)
SEM = asyncio.Semaphore(200)


async def fetch_json(session, url):
    async with SEM:
        async with session.get(url) as resp:
            return await resp.json()


# 🔹 get ALL user credits (parallel)
async def get_user_credits(session, user_ids: List[int]):
    tasks = [
        fetch_json(session, f"{PAYMENT_URL}/payment/find_user/{uid}")
        for uid in user_ids
    ]
    results = await asyncio.gather(*tasks)
    return sum(r["credit"] for r in results)


# 🔹 get ALL stock
async def get_stock(session, item_ids: List[int]):
    tasks = [
        fetch_json(session, f"{STOCK_URL}/stock/find/{iid}")
        for iid in item_ids
    ]
    results = await asyncio.gather(*tasks)
    return sum(r["stock"] for r in results)


# 🔥 MAIN CHECK
async def verify_atomicity():
    user_ids = list(range(NUMBER_OF_USERS))
    item_ids = list(range(NUMBER_OF_ITEMS))

    async with aiohttp.ClientSession() as session:
        logger.info("Fetching user credits...")
        current_total_credit = await get_user_credits(session, user_ids)

        logger.info("Fetching stock...")
        current_total_stock = await get_stock(session, item_ids)

    # initial totals
    initial_total_credit = NUMBER_OF_USERS * USER_STARTING_CREDIT
    initial_total_stock = NUMBER_OF_ITEMS * ITEM_STARTING_STOCK

    # compute
    stock_sold = initial_total_stock - current_total_stock
    money_deducted = initial_total_credit - current_total_credit

    print("\n===== ATOMICITY CHECK =====")
    print(f"Stock sold: {stock_sold}")
    print(f"Money deducted: {money_deducted}")

    print("\n===== RESULT =====")
    if stock_sold * ITEM_PRICE != money_deducted:
        print("❌ ATOMICITY BROKEN!")
    else:
        print("✅ Atomicity holds.")


if __name__ == "__main__":
    asyncio.run(verify_atomicity())