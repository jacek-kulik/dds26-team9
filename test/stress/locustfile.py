"""
Checkout Only Load Test

Purpose
-------
This test measures the raw performance of the distributed transaction
implementation by executing only the checkout operation on existing orders.

Each Locust user repeatedly performs:
    POST /orders/checkout/{order_id}

This isolates the critical distributed transaction path involving:
    Order Service -> Stock Service -> Payment Service -> Redis

Because orders already exist, the test removes overhead from browsing
or order creation. This makes it ideal for comparing the efficiency of
transaction protocols such as Saga and Two-Phase Commit (2PC).

What this test reveals
----------------------
- Maximum checkout throughput
- Transaction latency
- Overhead introduced by distributed coordination
- Performance difference between Saga and 2PC

Typical run configurations
--------------------------

Moderate load
locust -f checkout_only.py --users 300 --spawn-rate 50 --run-time 2m --headless

Stress test
locust -f checkout_only.py --users 1500 --spawn-rate 200 --run-time 2m --headless

Why run this test
-----------------
This is the most direct comparison of Saga vs 2PC because the workload
consists only of distributed transactions without additional system noise.
"""

import os.path
import random
import json

from locust import HttpUser, SequentialTaskSet, constant, task

from init_orders import NUMBER_OF_ORDERS


# replace the example urls and ports with the appropriate ones
with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


class CreateAndCheckoutOrder(SequentialTaskSet):
    @task
    def user_checks_out_order(self):
        try:
            order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
            with self.client.post(f"{ORDER_URL}/orders/checkout/{order_id}", name="/orders/checkout/[order_id]",
                                  catch_response=True) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(response.text)
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="POST",
                name="/orders/checkout/[order_id]",
                response_time=0,
                response_length=0,
                exception=e,
                context={},
            )


class MicroservicesUser(HttpUser):
    # how much time a user waits (seconds) to run another TaskSequence (you could also use between (start, end))
    wait_time = constant(1)
    # [SequentialTaskSet]: [weight of the SequentialTaskSet]
    tasks = {
        CreateAndCheckoutOrder: 100
    }