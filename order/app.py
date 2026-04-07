import logging
import os
import random
import time
import uuid
import asyncio
from collections import defaultdict
from enum import Enum
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions
import httpx
from msgspec import msgpack, Struct, field
from quart import Quart, jsonify, abort, Response
import traceback

import utils.messaging as messaging
from utils.atomic import atomic_update

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Quart("order-service")

_http: httpx.AsyncClient | None = None


ORDER_TIMEOUT_SECONDS = float(os.getenv("ORDER_TIMEOUT_SECONDS", 10.0))

def _make_redis_client(host_var='REDIS_HOST', port_var='REDIS_PORT',
                       password_var='REDIS_PASSWORD', db_var='REDIS_DB',
                       sentinel_host_var='REDIS_SENTINEL_HOST',
                       sentinel_port_var='REDIS_SENTINEL_PORT',
                       sentinel_master_var='REDIS_SENTINEL_MASTER') -> aioredis.Redis:
    """Create a Redis client. Uses Sentinel if REDIS_SENTINEL_HOST is set,
    otherwise falls back to a direct connection."""
    sentinel_host = os.environ.get(sentinel_host_var)
    if sentinel_host:
        sentinel_port = int(os.environ.get(sentinel_port_var, '26379'))
        master_name = os.environ.get(sentinel_master_var, 'mymaster')
        password = os.environ.get(password_var, '')
        db_num = int(os.environ.get(db_var, '0'))
        sentinel = aioredis.Sentinel(
            [(sentinel_host, sentinel_port)],
            sentinel_kwargs={'password': password},
            password=password,
            db=db_num,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        return sentinel.master_for(master_name)
    else:
        return aioredis.Redis(
            host=os.environ[host_var],
            port=int(os.environ[port_var]),
            password=os.environ[password_var],
            db=int(os.environ[db_var]),
            socket_timeout=5,
            retry_on_timeout=True,
        )


db: aioredis.Redis = _make_redis_client()





class Status(str, Enum):
    PENDING = "PENDING"

    # Saga states
    CHECKOUT_PENDING = "CHECKOUT_PENDING"

    # 2PC states
    PREPARING = "PREPARING"
    COMMITTING = "COMMITTING"
    ABORTING = "ABORTING"

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    status: str = Status.PENDING
    error: str = ""

    # Timestamp (epoch) of last status change — used by recovery
    status_ts: float = 0.0


async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes | None = await db.get(order_id)
    except redis_exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

async def get_order_status(order_id: str, timeout: float = None, interval: float = 0.5) -> OrderValue | None:
    if timeout is None:
        timeout = ORDER_TIMEOUT_SECONDS
    deadline = time.time() + timeout
    while time.time() < deadline:
        order = await get_order_from_db(order_id)
        if order and order.status in (Status.COMPLETED, Status.FAILED):
            return order
        await asyncio.sleep(interval)
    return await get_order_from_db(order_id)


async def on_set_completed(order_id: str):
    def modifier(order: OrderValue):
        order.paid = True
        order.status = Status.COMPLETED
        order.status_ts = time.time()
        return order, "ok"
    await atomic_update(db, order_id, OrderValue, modifier)

async def on_set_failed(order_id: str, error: str = ""):
    def modifier(order: OrderValue):
        order.status = Status.FAILED
        order.status_ts = time.time()
        if error:
            order.error = error
        return order, "ok"
    await atomic_update(db, order_id, OrderValue, modifier)


async def worker(worker_id: str):
    await messaging.ensure_group("order")
    async for msg_id, data in messaging.consume("order", worker_id):
        try:
            action = data.get(b"action", b"").decode()

            if action == "set_completed":
                order_id = data[b"order_id"].decode()
                await on_set_completed(order_id)

            elif action == "set_failed":
                order_id = data[b"order_id"].decode()
                error = data.get(b"error", b"").decode()
                await on_set_failed(order_id, error)

            await messaging.ack("order", msg_id)
        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


_worker_tasks: list[asyncio.Task] = []

WORKER_COUNT = 6
WORKER_DRAIN_TIMEOUT = 10
RUN_MODE = os.environ.get("RUN_MODE", "web")

@app.before_serving
async def startup():
    global _http
    _http = httpx.AsyncClient(timeout=ORDER_TIMEOUT_SECONDS)

    if RUN_MODE != "web":
        # Only spawn consumers if its not a web pod
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(worker(f"order-{os.getpid()}-{i}"),name=f"order-worker-{i}")
            _worker_tasks.append(task)

@app.after_serving
async def shutdown():
    messaging.request_shutdown()

    if _worker_tasks:
        _, pending = await asyncio.wait(_worker_tasks, timeout=WORKER_DRAIN_TIMEOUT)
        for t in pending:
            t.cancel()

    if _http:
        await _http.aclose()
    await db.aclose()
    await messaging.close()

@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except redis_exceptions.RedisError:
        abort(401, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.get('/health')
async def health():
    try:
        await db.ping()
        return jsonify({"status": "ok"}), 200
    except redis_exceptions.RedisError:
        return jsonify({"status": "unhealthy"}), 503


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

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
        await db.mset(kv_pairs)
    except redis_exceptions.RedisError:
        abort(402, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})

@app.get('/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    item_reply = await _http.get(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(403, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    price = item_json["price"]

    def modifier(order: OrderValue):
        order.items.append((item_id, int(quantity)))
        order.total_cost += int(quantity) * price
        return order, order.total_cost

    success, new_total = await atomic_update(db, order_id, OrderValue, modifier)
    if not success:
        abort(403, f"Order failed to add item!")
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {new_total}",
                    status=200)

@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    return await orchestrated_checkout(order_id)


async def orchestrated_checkout(order_id: str):
    order_entry = await get_order_from_db(order_id)
    if order_entry is None:
        abort(404, f"Order: {order_id} not found!")

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    items = list(items_quantities.items())

    start_action = "checkout_start"
    cancel_action = "checkout_cancel"
    initial_status = Status.CHECKOUT_PENDING

    def modifier(order: OrderValue):
        order.status = initial_status
        order.status_ts = time.time()
        order.error = ""
        return order, "ok"

    success, _ = await atomic_update(db, order_id, OrderValue, modifier)
    if not success:
        abort(404, f"Order: {order_id} not found!")

    await messaging.publish("orchestrator", {
        "action": start_action,
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": str(order_entry.total_cost),
        "items": msgpack.encode(items),
    })

    order_entry = await get_order_status(order_id, timeout=ORDER_TIMEOUT_SECONDS)
    if order_entry is None:
        abort(404, DB_ERROR_STR)

    if order_entry.status == Status.COMPLETED:
        return Response("Checkout successful", status=200)
    elif order_entry.status == Status.FAILED:
        return Response("Checkout failed", 407)
    else:
        await messaging.publish("orchestrator", {
            "action": cancel_action,
            "order_id": order_id,
        })
        abort(408, "Checkout timed out")

@app.post("/reset")
async def reset_db():
    try:
        await db.flushdb()
        return jsonify({"status": "reset"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
