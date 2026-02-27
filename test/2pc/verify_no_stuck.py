import asyncio
import os
import json
import logging
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("2PC Stuck Check")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URLS_PATH = os.path.join(BASE_DIR, "urls.json")

with open(URLS_PATH) as f:
    urls = json.load(f)

ORDER_URL = urls['ORDER_URL']
PAYMENT_URL = urls['PAYMENT_URL']
STOCK_URL = urls['STOCK_URL']


async def verify_no_stuck(order_ids):
    async with aiohttp.ClientSession() as session:
        stuck = 0
        for oid in order_ids:
            async with session.get(f"{ORDER_URL}/orders/find/{oid}") as resp:
                jsn = await resp.json()
                status = jsn.get("status")

                if status in ("PREPARING", "COMMITTING", "ABORTING"):
                    stuck += 1

        if stuck > 0:
            logger.error(f"{stuck} orders stuck in intermediate state")
        else:
            logger.info("No stuck 2PC orders.")