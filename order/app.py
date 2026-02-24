import logging
import os
import atexit
import random
import time
import uuid
import threading
from collections import defaultdict
from enum import Enum

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import utils.messaging as messaging
from utils.atomic import atomic_update

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class Status(str, Enum):
    PENDING = "PENDING"
    SUBTRACTING = "SUBTRACTING"
    PAYING = "PAYING"
    ROLLING_BACK = "ROLLING_BACK"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    status: str = Status.PENDING
    items_pending: int = 0
    items_confirmed: list[tuple[str, int]] = []
    rollback_pending: int = 0
    error: str = ""


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

def get_order_status(order_id: str, timeout: float = 10.0, interval: float = 0.01) -> OrderValue | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        order = get_order_from_db(order_id)
        if order and (order.status == Status.COMPLETED or order.status == Status.FAILED):
            return order
        time.sleep(interval)
    return get_order_from_db(order_id)


def on_subtract_success(order_id: str, item_id: str, quantity: int):
    should_pay = False
    pay_user_id = ""
    pay_amount = 0

    def modifier(order: OrderValue):
        nonlocal should_pay, pay_user_id, pay_amount
        order.items_pending -= 1
        order.items_confirmed.append((item_id, quantity))
        if order.items_pending == 0:
            order.status = Status.PAYING
            should_pay = True
            pay_user_id = order.user_id
            pay_amount = order.total_cost
        return order, "ok"

    atomic_update(db, order_id, OrderValue, modifier)

    if should_pay:
        messaging.publish("payment", {
            "action": "pay",
            "order_id": order_id,
            "user_id": pay_user_id,
            "amount": str(pay_amount),
        })


def on_subtract_failed(order_id: str, error: str):
    rollback_items = []

    def modifier(order: OrderValue):
        nonlocal rollback_items
        if order.status not in (Status.SUBTRACTING, Status.PAYING):
            return None, "wrong_state"
        order.status = Status.ROLLING_BACK
        order.error = error
        rollback_items = list(order.items_confirmed)
        if not rollback_items:
            order.status = Status.FAILED
        else:
            order.rollback_pending = len(rollback_items)
        return order, "ok"

    atomic_update(db, order_id, OrderValue, modifier)

    for it_id, qty in rollback_items:
        messaging.publish("stock", {
            "action": "add",
            "order_id": order_id,
            "item_id": it_id,
            "amount": str(qty),
        })


def on_pay_success(order_id: str):
    def modifier(order: OrderValue):
        order.paid = True
        order.status = Status.COMPLETED
        return order, "ok"

    atomic_update(db, order_id, OrderValue, modifier)
    app.logger.info(f"Checkout COMPLETED: {order_id}")


def on_pay_failed(order_id: str, error: str):
    rollback_items = []

    def modifier(order: OrderValue):
        nonlocal rollback_items
        order.error = error
        order.status = Status.ROLLING_BACK
        rollback_items = list(order.items_confirmed)
        if not rollback_items:
            order.status = Status.FAILED
        else:
            order.rollback_pending = len(rollback_items)
        return order, "ok"

    atomic_update(db, order_id, OrderValue, modifier)

    for it_id, qty in rollback_items:
        messaging.publish("stock", {
            "action": "add",
            "order_id": order_id,
            "item_id": it_id,
            "amount": str(qty),
        })


def on_rollback_success(order_id: str):
    is_done = False

    def modifier(order: OrderValue):
        nonlocal is_done
        order.rollback_pending -= 1
        if order.rollback_pending <= 0:
            order.status = Status.FAILED
            is_done = True
        return order, "ok"

    atomic_update(db, order_id, OrderValue, modifier)

    if is_done:
        app.logger.info(f"Checkout Failed (rollback done): {order_id}")


def on_rollback_failed(order_id: str, error: str):
    app.logger.error(f"Rollback failed for {order_id}: {error}")
    on_rollback_success(order_id)

DISPATCH = {
    "subtract_success": on_subtract_success,
    "subtract_failed":  on_subtract_failed,
    "pay_success":      on_pay_success,
    "pay_failed":       on_pay_failed,
    "rollback_success": on_rollback_success,
    "rollback_failed":  on_rollback_failed,
}

def worker(worker_id: str):
    messaging.ensure_group("order")
    for msg_id, data in messaging.consume("order", worker_id):
        action = data.get(b"action", b"").decode()
        handler = DISPATCH.get(action)
        if handler:
            order_id = data[b"order_id"].decode()
            if action == "subtract_success":
                handler(order_id, data[b"item_id"].decode(), int(data.get(b"quantity", b"0")))
            elif action == "subtract_failed":
                handler(order_id, data.get(b"error", b"").decode())
            elif action in ("pay_success", "rollback_success"):
                handler(order_id)
            elif action in ("pay_failed", "rollback_failed"):
                handler(order_id, data.get(b"error", b"").decode())
        messaging.ack("order", msg_id)

def start_workers(n: int = 5):
    for i in range(n):
        t = threading.Thread(target=worker, args=(f"order-{os.getpid()}-{i}",), daemon=True)
        t.start()

start_workers()

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})

@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    item_reply = requests.get(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    price = item_json["price"]

    def modifier(order: OrderValue):
        order.items.append((item_id, int(quantity)))
        order.total_cost += int(quantity) * price
        return order, order.total_cost

    success, new_total = atomic_update(db, order_id, OrderValue, modifier)
    if not success:
        abort(400, f"Order failed to add item!")
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {new_total}",
                    status=200)


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    order_entry = get_order_from_db(order_id)
    if order_entry is None:
        abort(400, f"Order: {order_id} not found!")

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    items = list(items_quantities.items())

    def modifier(order: OrderValue):
        order.status = Status.SUBTRACTING
        order.items_pending = len(items)
        order.items_confirmed = []
        order.rollback_pending = 0
        order.error = ""
        return order, "ok"

    success, _ = atomic_update(db, order_id, OrderValue, modifier)
    if not success:
        abort(400, f"Order: {order_id} not found!")

    for item_id, quantity in items:
        messaging.publish("stock", {
            "action": "subtract",
            "order_id": order_id,
            "item_id": item_id,
            "amount": str(quantity),
        })

    order_entry = get_order_status(order_id, timeout=10.0)
    if order_entry is None:
        abort(400, DB_ERROR_STR)

    if order_entry.status == Status.COMPLETED:
        return Response("Checkout successful", status=200)
    elif order_entry.status == Status.FAILED:
        return Response("Checkout failed", 400)
    else:
        abort(400, "Checkout timed out")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)