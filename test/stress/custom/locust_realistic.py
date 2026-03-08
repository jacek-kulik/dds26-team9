"""
Realistic Shopping Workload Test

Purpose
-------
This test simulates realistic user behaviour in an online shopping system.
Users browse items, create orders, add items to orders, and occasionally
checkout.

Task distribution
-----------------
browse_items  weight=5
create_order  weight=3
checkout      weight=1

This approximates real-world behaviour where most users browse products
and only a smaller fraction actually completes purchases.

What this test reveals
----------------------
- Overall system performance under realistic workloads
- Interaction between multiple microservices
- Load on the gateway and stock service due to browsing
- Impact of distributed transactions in mixed workloads

This test stresses:
    Gateway (NGINX)
    Order Service
    Stock Service
    Payment Service
    Redis databases

Typical run configurations
--------------------------

Realistic load
locust -f realistic_shopping.py --users 200 --spawn-rate 40 --run-time 3m --headless

Heavier load
locust -f realistic_shopping.py --users 800 --spawn-rate 160 --run-time 3m --headless

Long stability run
locust -f realistic_shopping.py --users 200 --spawn-rate 40 --run-time 10m --headless

Why run this test
-----------------
This workload represents real system usage and helps evaluate how the
microservices architecture behaves when multiple endpoints are used
simultaneously.
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
USER_COUNT = 100


class ShoppingUser(HttpUser):

    wait_time = between(0.2, 1)

    @task(5)
    def browse_items(self):

        item = random.randint(0, ITEM_COUNT - 1)
        self.client.get(f"{STOCK_URL}/stock/find/{item}")

    @task(3)
    def create_order(self):

        user = random.randint(0, USER_COUNT - 1)

        r = self.client.post(f"{ORDER_URL}/orders/create/{user}")
        order_id = r.json()["order_id"]

        item = random.randint(0, ITEM_COUNT - 1)

        self.client.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item}/1")

    @task(1)
    def checkout(self):

        user = random.randint(0, USER_COUNT - 1)

        r = self.client.post(f"{ORDER_URL}/orders/create/{user}")
        order_id = r.json()["order_id"]

        item = random.randint(0, ITEM_COUNT - 1)

        self.client.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item}/1")
        self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}")