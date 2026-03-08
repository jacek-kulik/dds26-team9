"""
Heavy Checkout Load Test

Purpose
-------
This test stresses the distributed transaction system by generating
a large number of checkout operations with minimal delay.

What this test reveals
----------------------
- Maximum transaction throughput under sustained load
- Scalability of Saga vs 2PC
- Latency increase as user count grows
- Resource contention between services

Typical run configurations
--------------------------

High transaction throughput
locust -f heavy_checkout.py --users 200 --spawn-rate 50 --run-time 2m --headless

High stress
locust -f heavy_checkout.py --users 800 --spawn-rate 200 --run-time 2m --headless

Extreme load
locust -f heavy_checkout.py --users 1600 --spawn-rate 200 --run-time 2m --headless

Why run this test
-----------------
This scenario focuses on transaction-heavy workloads, which are the
most critical part of the distributed system.
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

ORDER_COUNT = 10000


class HeavyCheckoutUser(HttpUser):

    wait_time = between(0.1, 0.5)

    @task
    def checkout(self):

        order_id = random.randint(0, ORDER_COUNT - 1)

        self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}")