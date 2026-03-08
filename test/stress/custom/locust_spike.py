"""
Traffic Spike Test

Purpose
-------
This test evaluates how the system handles sudden traffic bursts.

Users are spawned almost instantly to simulate real-world spikes such as:
    flash sales
    ticket releases
    promotional campaigns

What this test reveals
----------------------
- System resilience under sudden load increases
- Gateway (NGINX) bottlenecks
- Service startup latency
- Failure rates during traffic spikes

Typical run configurations
--------------------------

Small spike
locust -f spike_test.py --users 200 --spawn-rate 200 --run-time 1m --headless

Large spike
locust -f spike_test.py --users 500 --spawn-rate 500 --run-time 1m --headless

Extreme spike
locust -f spike_test.py --users 1000 --spawn-rate 1000 --run-time 1m --headless

Why run this test
-----------------
Distributed systems often fail during sudden load spikes rather
than gradual increases. This test helps identify those weaknesses.
"""
import json
import os
import random
from locust import HttpUser, task, constant

with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']

ITEM_COUNT = 100
USER_COUNT = 100

class SpikeUser(HttpUser):

    wait_time = constant(0)

    @task
    def spike_checkout(self):

        user = random.randint(0, USER_COUNT - 1)

        r = self.client.post(f"{ORDER_URL}/orders/create/{user}")
        order_id = r.json()["order_id"]

        item = random.randint(0, ITEM_COUNT - 1)

        self.client.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item}/1")
        self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}")