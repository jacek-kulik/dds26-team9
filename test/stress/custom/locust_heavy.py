"""
Browse Heavy Workload Test

Purpose
-------
This test simulates a read-heavy workload where most users are browsing
products rather than purchasing them.

Task distribution
-----------------
browse_items  weight=10
checkout      weight=1

This reflects realistic online store traffic patterns where the majority
of requests are read operations.

What this test reveals
----------------------
- Performance of the stock service under heavy read load
- Gateway throughput for high request rates
- Redis read performance
- Impact of browsing traffic on checkout latency

Typical run configurations
--------------------------

Moderate browsing load
locust -f browse_heavy.py --users 400 --spawn-rate 80 --run-time 3m --headless

Heavy browsing load
locust -f browse_heavy.py --users 800 --spawn-rate 160 --run-time 3m --headless

Why run this test
-----------------
Most real-world systems experience far more read operations than
transactions. This test verifies that the system can handle high
read traffic without affecting checkout performance.
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

ITEM_COUNT = 100


class BrowseHeavyUser(HttpUser):

    wait_time = between(0.2, 1)

    @task(10)
    def browse(self):

        item = random.randint(0, ITEM_COUNT - 1)
        self.client.get(f"{STOCK_URL}/stock/find/{item}")

    @task(1)
    def checkout(self):

        r = self.client.post(f"{ORDER_URL}/orders/create/1")
        order_id = r.json()["order_id"]

        item = random.randint(0, ITEM_COUNT - 1)

        self.client.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item}/1")
        self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}")