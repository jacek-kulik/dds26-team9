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

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry

def handle_pay(order_id: str, user_id: str, amount: int):
    def modifier(u: UserValue):
        new_credit = u.credit - amount
        if new_credit < 0:
            return None, f"User: {user_id} does not have enough money!"
        return UserValue(credit=new_credit), None

    ok, err = atomic_update(db, user_id, UserValue, modifier)

    if not ok:
        messaging.publish("order", {
            "action": "pay_failed",
            "order_id": order_id,
            "error": err or f"User: {user_id} not found!",
        })
        return

    messaging.publish("order", {"action": "pay_success", "order_id": order_id})


def handle_refund(order_id: str, user_id: str, amount: int):
    def modifier(u: UserValue):
        return UserValue(credit=u.credit + amount), None

    ok, err = atomic_update(db, user_id, UserValue, modifier)

    if not ok:
        messaging.publish("order", {
            "action": "refund_failed",
            "order_id": order_id,
            "error": err or f"User: {user_id} not found!",
        })
        return
    messaging.publish("order", {"action": "refund_success", "order_id": order_id})

DISPATCH = {
    "pay":    handle_pay,
    "refund": handle_refund,
}

def worker(worker_id: str):
    messaging.ensure_group("payment")

    for msg_id, data in messaging.consume("payment", worker_id):
        action = data.get(b"action", b"").decode()
        handler = DISPATCH.get(action)

        if handler:
            order_id = data.get(b"order_id", b"").decode()
            user_id  = data.get(b"user_id", b"").decode()
            amount   = int(data.get(b"amount", b"0") or b"0")
            handler(order_id, user_id, amount)

        messaging.ack("payment", msg_id)


def start_workers(n: int = 5):
    for i in range(n):
        t = threading.Thread(
            target=worker,
            args=(f"payment-{os.getpid()}-{i}",),
            daemon=True,
        )
        t.start()


start_workers()

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)