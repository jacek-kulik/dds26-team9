"""
Contention Test (Hot Item)

Purpose
-------
This test simulates many users attempting to purchase the same item
simultaneously. This creates high contention on the stock service.

All users target the same item ID.

What this test reveals
----------------------
- Correctness of stock management under contention
- Effectiveness of transaction isolation
- Potential race conditions
- Differences between Saga and 2PC when resources are contested

Typical run configurations
--------------------------

Moderate contention
locust -f contention_test.py --users 200 --spawn-rate 50 --run-time 2m --headless

High contention
locust -f contention_test.py --users 400 --spawn-rate 100 --run-time 2m --headless

Why run this test
-----------------
Contention scenarios are common in e-commerce systems during flash
sales or product launches and can reveal concurrency issues.
"""
import json
import os
import random
from locust import HttpUser, task, between

with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']

ITEM_ID = 0
USER_ID = 0


class ContentionUser(HttpUser):

    wait_time = between(0.1, 0.5)

    @task
    def checkout(self):

        r = self.client.post(f"{ORDER_URL}/orders/create/{USER_ID}")
        order_id = r.json()["order_id"]

        self.client.post(f"{ORDER_URL}/orders/addItem/{order_id}/{ITEM_ID}/1")
        self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}")