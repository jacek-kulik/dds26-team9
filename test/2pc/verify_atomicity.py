import asyncio
import os
import json
import logging
from typing import List

import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("2PC Atomicity")

# Must match populate.py values
NUMBER_OF_ITEMS = 1
ITEM_STARTING_STOCK = 100
ITEM_PRICE = 1
NUMBER_OF_USERS = 1000
USER_STARTING_CREDIT = 1

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URLS_PATH = os.path.join(BASE_DIR, "urls.json")

with open(URLS_PATH) as f:
    urls = json.load(f)

ORDER_URL = urls['ORDER_URL']
PAYMENT_URL = urls['PAYMENT_URL']
STOCK_URL = urls['STOCK_URL']


async def get_user_credits(session, user_ids: List[str]):
    total = 0
    for uid in user_ids:
        async with session.get(f"{PAYMENT_URL}/payment/find_user/{uid}") as resp:
            jsn = await resp.json()
            total += jsn["credit"]
    return total


async def get_stock(session, item_ids: List[str]):
    total = 0
    for iid in item_ids:
        async with session.get(f"{STOCK_URL}/stock/find/{iid}") as resp:
            jsn = await resp.json()
            total += jsn["stock"]
    return total


async def verify_atomicity(item_ids, user_ids):
    async with aiohttp.ClientSession() as session:
        current_total_credit = await get_user_credits(session, user_ids)
        current_total_stock = await get_stock(session, item_ids)

    initial_total_credit = NUMBER_OF_USERS * USER_STARTING_CREDIT
    initial_total_stock = NUMBER_OF_ITEMS * ITEM_STARTING_STOCK

    stock_sold = initial_total_stock - current_total_stock
    money_deducted = initial_total_credit - current_total_credit

    logger.info(f"Stock sold: {stock_sold}")
    logger.info(f"Money deducted: {money_deducted}")

    if stock_sold * ITEM_PRICE != money_deducted:
        logger.error("ATOMICITY BROKEN!")
    else:
        logger.info("Atomicity holds.")