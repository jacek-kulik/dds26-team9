import asyncio
import logging
from populate import populate_databases
from stress import stress
from verify_atomicity import verify_atomicity
from verify_no_stuck import verify_no_stuck

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("2PC Test Runner")

logger.info("Populating databases...")
item_ids, user_ids = asyncio.run(populate_databases())

logger.info("Running stress...")
order_ids = asyncio.run(stress(item_ids, user_ids))

logger.info("Verifying atomicity...")
asyncio.run(verify_atomicity(item_ids, user_ids))

logger.info("Verifying no stuck orders...")
asyncio.run(verify_no_stuck(order_ids))

logger.info("2PC Test Completed.")