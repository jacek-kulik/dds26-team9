import logging
import os
import atexit
import uuid
import threading
import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import utils.messaging as messaging
from utils.atomic import atomic_update

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry

def handle_subtract(order_id: str, item_id: str, amount: int):
    def modifier(it: StockValue):
        new_stock = it.stock - amount
        if new_stock < 0:
            return None, f"Item: {item_id} out of stock!"
        return StockValue(stock=new_stock, price=it.price), None

    ok, err = atomic_update(db, item_id, StockValue, modifier)

    if not ok:
        messaging.publish("order", {
            "action": "subtract_failed",
            "order_id": order_id,
            "item_id": item_id,
            "error": err or f"Item: {item_id} not found!",
        })
        return

    messaging.publish("order", {
        "action": "subtract_success",
        "order_id": order_id,
        "item_id": item_id,
        "quantity": str(amount),
    })


def handle_add(order_id: str, item_id: str, amount: int):
    def modifier(it: StockValue):
        return StockValue(stock=it.stock + amount, price=it.price), None

    ok, err = atomic_update(db, item_id, StockValue, modifier)

    if not ok:
        messaging.publish("order", {
            "action": "rollback_failed",
            "order_id": order_id,
            "item_id": item_id,
            "error": err or f"Item: {item_id} not found!",
        })
        return

    messaging.publish("order", {
        "action": "rollback_success",
        "order_id": order_id,
        "item_id": item_id,
    })

DISPATCH = {
    "subtract": handle_subtract,
    "add":      handle_add,
}


def worker(worker_id: str):
    messaging.ensure_group("stock")

    for msg_id, data in messaging.consume("stock", worker_id):
        action = data.get(b"action", b"").decode()
        handler = DISPATCH.get(action)

        if handler:
            order_id = data.get(b"order_id", b"").decode()
            item_id  = data.get(b"item_id", b"").decode()
            amount   = int(data.get(b"amount", b"0") or b"0")
            handler(order_id, item_id, amount)

        messaging.ack("stock", msg_id)


def start_workers(n: int = 5):
    for i in range(n):
        t = threading.Thread(
            target=worker,
            args=(f"stock-{os.getpid()}-{i}",),
            daemon=True,
        )
        t.start()


start_workers()

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)