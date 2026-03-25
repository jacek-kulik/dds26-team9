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
import json

import utils.messaging as messaging
from utils.atomic import atomic_update

PROTOCOL = os.getenv("PROTOCOL", "SAGA")

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Quart("order-service")

_http: httpx.AsyncClient | None = None

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config.json')
try:
    with open(CONFIG_PATH, 'r') as f:
        _config = json.load(f)
except Exception:
    _config = {}

ORDER_TIMEOUT_SECONDS = float(_config.get('ORDER_TIMEOUT_SECONDS', 10.0))

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
    SUBTRACTING = "SUBTRACTING"
    PAYING = "PAYING"
    ROLLING_BACK = "ROLLING_BACK"

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

    # Saga fields
    items_pending: int = 0
    items_confirmed: list[tuple[str, int]] = field(default_factory=list)
    rollback_pending: int = 0
    error: str = ""

    # Timestamp (epoch) of last status change — used by recovery
    status_ts: float = 0.0

    # 2PC fields
    votes_yes: list[str] = field(default_factory=list)
    decision: str = ""   # COMMIT or ABORT
    acks: list[str] = field(default_factory=list)
    expected_participants: int = 2


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

async def get_order_status(order_id: str, timeout: float = None, interval: float = 0.1) -> OrderValue | None:
    if timeout is None:
        timeout = ORDER_TIMEOUT_SECONDS
    deadline = time.time() + timeout
    while time.time() < deadline:
        order = await get_order_from_db(order_id)
        if order and order.status in (Status.COMPLETED, Status.FAILED):
            return order
        await asyncio.sleep(interval)
    return await get_order_from_db(order_id)


async def on_subtract_success(order_id: str, item_id: str, quantity: int):
    should_pay = False
    should_rollback = False
    pay_user_id = ""
    pay_amount = 0

    def modifier(order: OrderValue):
        nonlocal should_pay, should_rollback, pay_user_id, pay_amount

        # If the order is already rolling back or failed, we need to undo this
        # successful subtract since another item's subtract failed first.
        if order.status in (Status.ROLLING_BACK, Status.FAILED):
            order.items_confirmed.append((item_id, quantity))
            order.rollback_pending += 1
            should_rollback = True
            # If it was already FAILED (no items to roll back before),
            # move it back to ROLLING_BACK
            if order.status == Status.FAILED:
                order.status = Status.ROLLING_BACK
                order.status_ts = time.time()
            return order, "ok"

        order.items_pending -= 1
        order.items_confirmed.append((item_id, quantity))
        if order.items_pending == 0:
            order.status = Status.PAYING
            order.status_ts = time.time()
            should_pay = True
            pay_user_id = order.user_id
            pay_amount = order.total_cost
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    if should_rollback:
        await messaging.publish("stock", {
            "action": "add",
            "order_id": order_id,
            "item_id": item_id,
            "amount": str(quantity),
        })
    elif should_pay:
        await messaging.publish("payment", {
            "action": "pay",
            "order_id": order_id,
            "user_id": pay_user_id,
            "amount": str(pay_amount),
        })


async def on_subtract_failed(order_id: str, error: str):
    rollback_items = []

    def modifier(order: OrderValue):
        nonlocal rollback_items
        if order.status in (Status.ROLLING_BACK, Status.FAILED):
            # Already handling a failure; decrement pending count
            order.items_pending -= 1
            return order, "ok"
        if order.status not in (Status.SUBTRACTING, Status.PAYING):
            return None, "wrong_state"
        order.status = Status.ROLLING_BACK
        order.status_ts = time.time()
        order.error = error
        order.items_pending -= 1
        rollback_items = list(order.items_confirmed)
        if not rollback_items and order.items_pending == 0:
            # No confirmed items and no more pending — we're done
            order.status = Status.FAILED
            order.status_ts = time.time()
        elif not rollback_items:
            # No confirmed items yet, but some subtracts are still pending.
            # Stay in ROLLING_BACK; on_subtract_success will handle them.
            order.rollback_pending = 0
        else:
            order.rollback_pending = len(rollback_items)
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    for it_id, qty in rollback_items:
        await messaging.publish("stock", {
            "action": "add",
            "order_id": order_id,
            "item_id": it_id,
            "amount": str(qty),
        })


async def on_pay_success(order_id: str):
    should_refund = False
    refund_user_id = ""
    refund_amount = 0

    def modifier(order: OrderValue):
        nonlocal should_refund, refund_user_id, refund_amount
        # If recovery already moved this order to ROLLING_BACK or FAILED,
        # don't mark it as COMPLETED — trigger a refund instead.
        if order.status in (Status.ROLLING_BACK, Status.FAILED):
            should_refund = True
            refund_user_id = order.user_id
            refund_amount = order.total_cost
            return order, "ok"
        order.paid = True
        order.status = Status.COMPLETED
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    if should_refund:
        await messaging.publish("payment", {
            "action": "refund",
            "order_id": order_id,
            "user_id": refund_user_id,
            "amount": str(refund_amount),
        })
        app.logger.info(f"Late pay_success for {order_id} during rollback — refund issued")
    else:
        app.logger.info(f"Checkout COMPLETED: {order_id}")


async def on_pay_failed(order_id: str, error: str):
    rollback_items = []

    def modifier(order: OrderValue):
        nonlocal rollback_items
        order.error = error
        order.status = Status.ROLLING_BACK
        order.status_ts = time.time()
        rollback_items = list(order.items_confirmed)
        if not rollback_items:
            order.status = Status.FAILED
            order.status_ts = time.time()
        else:
            order.rollback_pending = len(rollback_items)
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    for it_id, qty in rollback_items:
        await messaging.publish("stock", {
            "action": "add",
            "order_id": order_id,
            "item_id": it_id,
            "amount": str(qty),
        })


async def on_rollback_success(order_id: str):
    is_done = False

    def modifier(order: OrderValue):
        nonlocal is_done
        order.rollback_pending -= 1
        if order.rollback_pending <= 0:
            order.status = Status.FAILED
            is_done = True
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    if is_done:
        app.logger.info(f"Checkout Failed (rollback done): {order_id}")


async def on_rollback_failed(order_id: str, error: str):
    app.logger.error(f"Rollback failed for {order_id}: {error}")
    await on_rollback_success(order_id)

async def on_vote_yes(order_id: str, who: str):
    send_commit = False

    def modifier(order: OrderValue):
        if order.decision != "":
            return order, "ok"

        nonlocal send_commit
        if who not in order.votes_yes:
            order.votes_yes.append(who)

        if len(order.votes_yes) == order.expected_participants and order.decision == "":
            order.decision = "COMMIT"
            order.status = Status.COMMITTING
            send_commit = True
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    if send_commit:
        await messaging.publish("stock", {"action":"stock_commit", "order_id":order_id})
        await messaging.publish("payment", {"action":"payment_commit", "order_id":order_id})


async def on_vote_no(order_id: str, who: str):
    send_abort = False

    def modifier(order: OrderValue):
        if order.decision != "":
            return order, "ok"

        nonlocal send_abort
        order.decision = "ABORT"
        order.status = Status.ABORTING
        send_abort = True
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    if send_abort:
        await messaging.publish("stock", {"action":"stock_abort", "order_id":order_id})
        await messaging.publish("payment", {"action":"payment_abort", "order_id":order_id})


async def on_commit_ack(order_id: str, who: str):
    def modifier(order: OrderValue):
        if order.status in (Status.COMPLETED, Status.FAILED):
            return order, "ok"
        if who not in order.acks:
            order.acks.append(who)
        if len(order.acks) == order.expected_participants:
            order.status = Status.COMPLETED
            order.paid = True
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)


async def on_abort_ack(order_id: str, who: str):
    def modifier(order: OrderValue):
        if order.status in (Status.COMPLETED, Status.FAILED):
            return order, "ok"
        if who not in order.acks:
            order.acks.append(who)
        if len(order.acks) == order.expected_participants:
            order.status = Status.FAILED
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

DISPATCH = {
    "subtract_success": on_subtract_success,
    "subtract_failed":  on_subtract_failed,
    "pay_success":      on_pay_success,
    "pay_failed":       on_pay_failed,
    "rollback_success": on_rollback_success,
    "rollback_failed":  on_rollback_failed,
    "vote_yes": on_vote_yes,
    "vote_no": on_vote_no,
    "commit_ack": on_commit_ack,
    "abort_ack": on_abort_ack,
}

async def worker(worker_id: str):
    await messaging.ensure_group("order")
    async for msg_id, data in messaging.consume("order", worker_id):
        try:
            action = data.get(b"action", b"").decode()
            handler = DISPATCH.get(action)
            if handler:
                order_id = data[b"order_id"].decode()
                if action == "subtract_success":
                    await handler(order_id, data[b"item_id"].decode(), int(data.get(b"quantity", b"0")))
                elif action == "subtract_failed":
                    await handler(order_id, data.get(b"error", b"").decode())
                elif action in ("pay_success", "rollback_success"):
                    await handler(order_id)
                elif action in ("pay_failed", "rollback_failed"):
                    await handler(order_id, data.get(b"error", b"").decode())
                elif action in ("vote_yes", "vote_no", "commit_ack", "abort_ack"):
                    await handler(order_id, data[b"who"].decode())
            await messaging.ack("order", msg_id)
        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


_worker_tasks: list[asyncio.Task] = []


async def recover_incomplete_2pc():
    # Use a Redis lock so only one replica runs recovery at a time
    lock = db.lock("2pc_recovery_lock", timeout=30)
    if not await lock.acquire(blocking=False):
        app.logger.info("Another replica is running 2PC recovery, skipping.")
        return

    try:
        async for key in db.scan_iter():
            # Skip non-string keys (e.g. streams, hashes from other services)
            if await db.type(key) != b"string":
                continue

            raw = await db.get(key)
            if not raw:
                continue

            try:
                order = msgpack.decode(raw, type=OrderValue)
            except Exception:
                continue

            key_str = key.decode() if isinstance(key, bytes) else key

            # Recover COMMIT
            if order.decision == "COMMIT" and order.status != Status.COMPLETED:
                app.logger.info(f"Recovering COMMIT for {key_str}")
                await messaging.publish("stock", {
                    "action": "stock_commit",
                    "order_id": key_str
                })
                await messaging.publish("payment", {
                    "action": "payment_commit",
                    "order_id": key_str
                })

            # Recover ABORT
            if order.decision == "ABORT" and order.status != Status.FAILED:
                app.logger.info(f"Recovering ABORT for {key_str}")
                await messaging.publish("stock", {
                    "action": "stock_abort",
                    "order_id": key_str
                })
                await messaging.publish("payment", {
                    "action": "payment_abort",
                    "order_id": key_str
                })
    finally:
        try:
            await lock.release()
        except redis_exceptions.LockError:
            pass

SAGA_RECOVERY_THRESHOLD = 15  # seconds — only recover orders stuck longer than this

async def recover_incomplete_saga():
    """Periodically recover orders stuck in intermediate Saga states.

    Only acts on orders whose status_ts is older than SAGA_RECOVERY_THRESHOLD,
    ensuring in-flight messages have time to be processed first.
    """
    # Wait for workers to start up before first scan
    await asyncio.sleep(5)

    while not messaging.is_shutting_down():
        try:
            lock = db.lock("saga_recovery_lock", timeout=30)
            if not await lock.acquire(blocking=False):
                await asyncio.sleep(10)
                continue

            try:
                now = time.time()
                recovered = 0

                async for key in db.scan_iter():
                    if await db.type(key) != b"string":
                        continue

                    raw = await db.get(key)
                    if not raw:
                        continue

                    try:
                        order = msgpack.decode(raw, type=OrderValue)
                    except Exception:
                        continue

                    key_str = key.decode() if isinstance(key, bytes) else key

                    if order.status in (Status.COMPLETED, Status.FAILED, Status.PENDING):
                        continue

                    # Skip orders that haven't been stuck long enough —
                    # they may still be actively processing.
                    age = now - order.status_ts if order.status_ts > 0 else float('inf')
                    if age < SAGA_RECOVERY_THRESHOLD:
                        continue

                    # --- SUBTRACTING ---
                    if order.status == Status.SUBTRACTING:
                        app.logger.info(f"Saga recovery [{key_str}]: stuck in SUBTRACTING for {age:.0f}s "
                                        f"(pending={order.items_pending}, confirmed={len(order.items_confirmed)})")

                        if order.items_confirmed:
                            order.status = Status.ROLLING_BACK
                            order.rollback_pending = len(order.items_confirmed)
                            order.items_pending = 0
                            order.error = "Recovered after timeout during subtract phase"
                            order.status_ts = now
                            await db.set(key, msgpack.encode(order))

                            for item_id, qty in order.items_confirmed:
                                await messaging.publish("stock", {
                                    "action": "add",
                                    "order_id": key_str,
                                    "item_id": item_id,
                                    "amount": str(qty),
                                })
                        else:
                            order.status = Status.FAILED
                            order.items_pending = 0
                            order.error = "Recovered after timeout: no items were confirmed"
                            order.status_ts = now
                            await db.set(key, msgpack.encode(order))

                        recovered += 1

                    # --- PAYING ---
                    elif order.status == Status.PAYING:
                        app.logger.info(f"Saga recovery [{key_str}]: stuck in PAYING for {age:.0f}s, "
                                        f"rolling back {len(order.items_confirmed)} items")

                        if order.items_confirmed:
                            order.status = Status.ROLLING_BACK
                            order.rollback_pending = len(order.items_confirmed)
                            order.error = "Recovered after timeout during payment phase"
                            order.status_ts = now
                            await db.set(key, msgpack.encode(order))

                            for item_id, qty in order.items_confirmed:
                                await messaging.publish("stock", {
                                    "action": "add",
                                    "order_id": key_str,
                                    "item_id": item_id,
                                    "amount": str(qty),
                                })
                        else:
                            order.status = Status.FAILED
                            order.error = "Recovered after timeout during payment phase"
                            order.status_ts = now
                            await db.set(key, msgpack.encode(order))

                        recovered += 1

                    # --- ROLLING_BACK ---
                    elif order.status == Status.ROLLING_BACK:
                        app.logger.info(f"Saga recovery [{key_str}]: stuck in ROLLING_BACK for {age:.0f}s "
                                        f"(rollback_pending={order.rollback_pending}, "
                                        f"confirmed={len(order.items_confirmed)})")

                        if order.items_confirmed:
                            order.rollback_pending = len(order.items_confirmed)
                            order.status_ts = now
                            await db.set(key, msgpack.encode(order))

                            for item_id, qty in order.items_confirmed:
                                await messaging.publish("stock", {
                                    "action": "add",
                                    "order_id": key_str,
                                    "item_id": item_id,
                                    "amount": str(qty),
                                })
                        else:
                            order.status = Status.FAILED
                            order.error = "Recovered after timeout: rollback had nothing to undo"
                            order.status_ts = now
                            await db.set(key, msgpack.encode(order))

                        recovered += 1

                if recovered > 0:
                    app.logger.info(f"Saga recovery pass: {recovered} orders recovered")

            finally:
                try:
                    await lock.release()
                except redis_exceptions.LockError:
                    pass

        except Exception as e:
            app.logger.exception(f"Saga recovery error: {e}")

        # Run recovery every 10 seconds
        await asyncio.sleep(10)


WORKER_COUNT = 6
WORKER_DRAIN_TIMEOUT = 10

RUN_MODE = os.environ.get("RUN_MODE", "all")

@app.before_serving
async def startup():
    global _http
    _http = httpx.AsyncClient(timeout=ORDER_TIMEOUT_SECONDS)

    if RUN_MODE != "web":
        # Only spawn consumers if its not a web pod
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(worker(f"order-{os.getpid()}-{i}"),name=f"order-worker-{i}")
            _worker_tasks.append(task)

        if PROTOCOL == "2PC":
            await recover_incomplete_2pc()
        elif PROTOCOL == "SAGA":
            _worker_tasks.append(asyncio.create_task(recover_incomplete_saga(), name="saga-recovery"))

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
    if PROTOCOL == "SAGA":
        return await saga_checkout(order_id)
    else:
        return await two_pc_checkout(order_id)

async def saga_checkout(order_id: str):
    order_entry = await get_order_from_db(order_id)
    if order_entry is None:
        abort(499, f"Order: {order_id} not found!")

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
        order.status_ts = time.time()
        return order, "ok"

    success, _ = await atomic_update(db, order_id, OrderValue, modifier)
    if not success:
        abort(498, f"Order: {order_id} not found!")

    for item_id, quantity in items:
        await messaging.publish("stock", {
            "action": "subtract",
            "order_id": order_id,
            "item_id": item_id,
            "amount": str(quantity),
        })

    order_entry = await get_order_status(order_id, timeout=10.0)
    if order_entry is None:
        abort(497, DB_ERROR_STR)

    if order_entry.status == Status.COMPLETED:
        return Response("Checkout successful", status=200)
    elif order_entry.status == Status.FAILED:
        return Response("Checkout failed", 496)
    else:
        # Timed out — cancel the saga so it converges to FAILED,
        # preventing a silent late success after we return 4xx.
        await _cancel_saga(order_id)
        abort(495, "Checkout timed out")


async def _cancel_saga(order_id: str):
    """Force an in-flight saga into ROLLING_BACK / FAILED.

    Called when the HTTP poll times out.  The saga callbacks already handle
    the case where an order is in ROLLING_BACK or FAILED (they trigger
    compensating actions), so this is safe even if messages are still in
    flight.

    NOTE: we do NOT issue a refund directly here even if the order was in
    PAYING state.  If the payment was already deducted, the on_pay_success
    callback will see the ROLLING_BACK state and issue the refund.  If the
    payment failed, on_pay_failed handles rollback.  Issuing a refund here
    would risk a double-refund race.
    """
    rollback_items = []

    def modifier(order: OrderValue):
        nonlocal rollback_items

        # Already terminal — nothing to do
        if order.status in (Status.COMPLETED, Status.FAILED):
            return order, "ok"

        # Already rolling back — let it finish on its own
        if order.status == Status.ROLLING_BACK:
            return order, "ok"

        order.status = Status.ROLLING_BACK
        order.status_ts = time.time()
        order.error = "Cancelled: HTTP poll timed out"
        order.items_pending = 0
        rollback_items = list(order.items_confirmed)
        if not rollback_items:
            order.status = Status.FAILED
            order.status_ts = time.time()
        else:
            order.rollback_pending = len(rollback_items)
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    for it_id, qty in rollback_items:
        await messaging.publish("stock", {
            "action": "add",
            "order_id": order_id,
            "item_id": it_id,
            "amount": str(qty),
        })

async def two_pc_checkout(order_id: str):
    order_entry = await get_order_from_db(order_id)

    items_quantities = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items = list(items_quantities.items())

    def modifier(order: OrderValue):
        order.status = Status.PREPARING
        order.votes_yes.clear()
        order.decision = ""
        order.acks.clear()
        return order, "ok"

    await atomic_update(db, order_id, OrderValue, modifier)

    await messaging.publish("stock", {
        "action": "stock_prepare",
        "order_id": order_id,
        "items": msgpack.encode(items),
    })

    await messaging.publish("payment", {
        "action": "payment_prepare",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "amount": str(order_entry.total_cost)
    })

    order_entry = await get_order_status(order_id, timeout=10.0)

    if order_entry.status == Status.COMPLETED:
        return Response("2PC checkout successful", 200)
    else:
        return Response("2PC checkout failed", 444)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
