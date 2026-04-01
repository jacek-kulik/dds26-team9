import logging
import os
import time
import uuid
import asyncio
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

import utils.messaging as messaging
from utils.atomic import atomic_update

DB_ERROR_STR = "DB error"

app = Quart("stock-service")


def _make_redis_client(host_var='REDIS_HOST', port_var='REDIS_PORT',
                       password_var='REDIS_PASSWORD', db_var='REDIS_DB',
                       sentinel_host_var='REDIS_SENTINEL_HOST',
                       sentinel_port_var='REDIS_SENTINEL_PORT',
                       sentinel_master_var='REDIS_SENTINEL_MASTER') -> aioredis.Redis:
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


class StockValue(Struct):
    stock: int
    price: int

class StockTx(Struct):
    items: list[tuple[str, int]]
    state: str  # PREPARED | COMMITTED | ABORTED
    created_ts: float = 0.0

async def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = await db.get(item_id)
    except redis_exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(404, f"Item: {item_id} not found!")
    return entry



"""
                    ==== WARNING ====
saga ops are basically the exact same as 2pc, left them separate for extendibility
"""


async def saga_subtract(order_id: str, items: list[tuple[str, int]]):
    tx_key = f"tx:{order_id}"

    raw = await db.get(tx_key)
    if raw:
        tx = msgpack.decode(raw, type=StockTx)
        if tx.state == "PREPARED":
            # Duplicate prepare for the current in-flight attempt → idempotent shortcut
            await messaging.publish("orchestrator", {"action": "subtract_success", "order_id": order_id})
            return
        # COMMITTED or ABORTED are terminal states from a *previous* checkout
        # attempt for this order.  Delete the stale key and re-evaluate fresh.
        await db.delete(tx_key)

    # try subtract all; if partial success, undo
    subtracted: list[tuple[str, int]] = []

    for item_id, amount in items:
        def modifier(it: StockValue, _amt=int(amount)):
            new_stock = it.stock - _amt
            if new_stock < 0:
                return None, "out_of_stock"
            return StockValue(stock=new_stock, price=it.price), None

        ok, err = await atomic_update(db, item_id, StockValue, modifier)
        if not ok:
            # rollback already subtracted
            for rb_item, rb_amt in subtracted:
                def rb_mod(it: StockValue):
                    return StockValue(stock=it.stock + rb_amt, price=it.price), None
                await atomic_update(db, rb_item, StockValue, rb_mod)

            await db.set(tx_key, msgpack.encode(StockTx(items=items, state="ABORTED", created_ts=time.time())))
            await messaging.publish("orchestrator", {"action": "subtract_failed", "order_id": order_id})
            return

        subtracted.append((item_id, int(amount)))

    await db.set(tx_key, msgpack.encode(StockTx(items=items, state="PREPARED", created_ts=time.time())))
    await messaging.publish("orchestrator", {"action": "subtract_success", "order_id": order_id})


async def saga_rollback(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = await db.get(tx_key)
    if not raw:
        await messaging.publish("orchestrator", {"action": "rolled_back", "order_id": order_id})
        return

    tx = msgpack.decode(raw, type=StockTx)

    if tx.state == "ABORTED":
        await messaging.publish("orchestrator", {"action": "rolled_back", "order_id": order_id})
        return

    if tx.state != "PREPARED":
        return

    # undo the subtracts
    for item_id, amount in tx.items:
        def modifier(it: StockValue, _a=int(amount)):
            return StockValue(stock=it.stock + _a, price=it.price), None
        await atomic_update(db, item_id, StockValue, modifier)

    tx.state = "ABORTED"
    await db.set(tx_key, msgpack.encode(tx))

    await messaging.publish("orchestrator", {"action": "rolled_back", "order_id": order_id})

async def handle_stock_prepare(order_id: str, items: list[tuple[str, int]]):
    tx_key = f"tx:{order_id}"

    raw = await db.get(tx_key)
    if raw:
        tx = msgpack.decode(raw, type=StockTx)
        if tx.state == "PREPARED":
            # Duplicate prepare for the current in-flight attempt → idempotent shortcut
            await messaging.publish("orchestrator", {"action": "vote_yes", "order_id": order_id, "who": "stock"})
            return
        # COMMITTED or ABORTED are terminal states from a *previous* checkout
        # attempt for this order.  Delete the stale key and re-evaluate fresh.
        await db.delete(tx_key)

    # try subtract all; if partial success, undo
    subtracted: list[tuple[str, int]] = []

    for item_id, amount in items:
        def modifier(it: StockValue, _amt=int(amount)):
            new_stock = it.stock - _amt
            if new_stock < 0:
                return None, "out_of_stock"
            return StockValue(stock=new_stock, price=it.price), None

        ok, err = await atomic_update(db, item_id, StockValue, modifier)
        if not ok:
            # rollback already subtracted
            for rb_item, rb_amt in subtracted:
                def rb_mod(it: StockValue):
                    return StockValue(stock=it.stock + rb_amt, price=it.price), None
                await atomic_update(db, rb_item, StockValue, rb_mod)

            await db.set(tx_key, msgpack.encode(StockTx(items=items, state="ABORTED", created_ts=time.time())))
            await messaging.publish("orchestrator", {"action": "vote_no", "order_id": order_id, "who": "stock"})
            return

        subtracted.append((item_id, int(amount)))

    await db.set(tx_key, msgpack.encode(StockTx(items=items, state="PREPARED", created_ts=time.time())))
    await messaging.publish("orchestrator", {"action": "vote_yes", "order_id": order_id, "who": "stock"})


async def handle_stock_commit(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = await db.get(tx_key)
    if not raw:
        return

    tx = msgpack.decode(raw, type=StockTx)

    if tx.state == "COMMITTED":
        await messaging.publish("orchestrator", {"action": "commit_ack", "order_id": order_id, "who": "stock"})
        return

    if tx.state != "PREPARED":
        return

    tx.state = "COMMITTED"
    await db.set(tx_key, msgpack.encode(tx))

    await messaging.publish("orchestrator", {"action": "commit_ack", "order_id": order_id, "who": "stock"})


async def handle_stock_abort(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = await db.get(tx_key)
    if not raw:
        await messaging.publish("orchestrator", {"action": "abort_ack", "order_id": order_id, "who": "stock"})
        return

    tx = msgpack.decode(raw, type=StockTx)

    if tx.state == "ABORTED":
        await messaging.publish("orchestrator", {"action": "abort_ack", "order_id": order_id, "who": "stock"})
        return

    if tx.state != "PREPARED":
        return

    # undo the subtracts
    for item_id, amount in tx.items:
        def modifier(it: StockValue, _a=int(amount)):
            return StockValue(stock=it.stock + _a, price=it.price), None
        await atomic_update(db, item_id, StockValue, modifier)

    tx.state = "ABORTED"
    await db.set(tx_key, msgpack.encode(tx))

    await messaging.publish("orchestrator", {"action": "abort_ack", "order_id": order_id, "who": "stock"})

DISPATCH = {
    "saga_subtract": saga_subtract,
    "saga_rollback": saga_rollback,
    "stock_prepare": handle_stock_prepare,
    "stock_commit":  handle_stock_commit,
    "stock_abort":   handle_stock_abort,
}


async def worker(worker_id: str):
    await messaging.ensure_group("stock")

    async for msg_id, data in messaging.consume("stock", worker_id):
        try:
            action = data.get(b"action", b"").decode()
            handler = DISPATCH.get(action)

            if handler:
                if action == "stock_prepare":
                    order_id = data.get(b"order_id", b"").decode()
                    items_raw = data.get(b"items", b"")
                    items = msgpack.decode(items_raw)
                    await handler(order_id, items)

                elif action == "saga_subtract":
                    order_id = data.get(b"order_id", b"").decode()
                    items_raw = data.get(b"items", b"")
                    items = msgpack.decode(items_raw)
                    await handler(order_id, items)

                else:
                    # stock_commit and stock_abort only need order_id
                    order_id = data.get(b"order_id", b"").decode()
                    await handler(order_id)

            await messaging.ack("stock", msg_id)

        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


_worker_tasks: list[asyncio.Task] = []

WORKER_COUNT = 6
WORKER_DRAIN_TIMEOUT = 10

RUN_MODE = os.environ.get("RUN_MODE", "all")
@app.before_serving
async def startup():
    if RUN_MODE != "web":
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(worker(f"stock-{os.getpid()}-{i}"),name=f"stock-worker-{i}")
            _worker_tasks.append(task)

        _worker_tasks.append(asyncio.create_task(_cleanup_stale_tx(), name="stock-tx-cleanup"))
@app.after_serving
async def shutdown():
    messaging.request_shutdown()

    if _worker_tasks:
        _, pending = await asyncio.wait(_worker_tasks, timeout=WORKER_DRAIN_TIMEOUT)
        for t in pending:
            t.cancel()

    await db.aclose()
    await messaging.close()

TX_PREPARED_TIMEOUT = 60
TX_TERMINAL_TTL     = 300
TX_CLEANUP_INTERVAL = 15


async def _cleanup_stale_tx():
    """Periodically clean up stale 2PC transaction keys.

    • PREPARED keys older than TX_PREPARED_TIMEOUT: roll back the held stock
      and mark the tx ABORTED (prevents permanent resource locks).
    • COMMITTED / ABORTED keys older than TX_TERMINAL_TTL: delete them
      (no longer needed for idempotency).
    """
    await asyncio.sleep(10)

    while not messaging.is_shutting_down():
        try:
            lock = db.lock("stock_tx_cleanup_lock", timeout=30)
            if not await lock.acquire(blocking=False):
                await asyncio.sleep(TX_CLEANUP_INTERVAL)
                continue

            try:
                now = time.time()
                cleaned = 0

                async for key in db.scan_iter(match="tx:*"):
                    if await db.type(key) != b"string":
                        continue

                    raw = await db.get(key)
                    if not raw:
                        continue

                    try:
                        tx = msgpack.decode(raw, type=StockTx)
                    except Exception:
                        continue

                    age = now - tx.created_ts if tx.created_ts > 0 else float('inf')

                    if tx.state == "PREPARED" and age > TX_PREPARED_TIMEOUT:
                        for item_id, amount in tx.items:
                            def modifier(it: StockValue, _a=int(amount)):
                                return StockValue(stock=it.stock + _a, price=it.price), None
                            await atomic_update(db, item_id, StockValue, modifier)

                        tx.state = "ABORTED"
                        tx.created_ts = now
                        await db.set(key, msgpack.encode(tx))

                        app.logger.info(f"Stale tx cleanup: rolled back PREPARED tx {key} "
                                        f"(age {age:.0f}s, {len(tx.items)} items)")
                        cleaned += 1

                    elif tx.state in ("COMMITTED", "ABORTED") and age > TX_TERMINAL_TTL:
                        await db.delete(key)
                        app.logger.debug(f"Stale tx cleanup: deleted terminal tx {key} "
                                         f"(state={tx.state}, age {age:.0f}s)")
                        cleaned += 1

                if cleaned > 0:
                    app.logger.info(f"Stale tx cleanup pass: {cleaned} key(s) cleaned")

            finally:
                try:
                    await lock.release()
                except redis_exceptions.LockError:
                    pass

        except Exception as e:
            app.logger.exception(f"Stale tx cleanup error: {e}")

        await asyncio.sleep(TX_CLEANUP_INTERVAL)


@app.get('/health')
async def health():
    try:
        await db.ping()
        return jsonify({"status": "ok"}), 200
    except redis_exceptions.RedisError:
        return jsonify({"status": "unhealthy"}), 503


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except redis_exceptions.RedisError:
        return abort(500, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except redis_exceptions.RedisError:
        return abort(500, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    def modifier(item: StockValue):
        new_stock = item.stock + int(amount)
        return StockValue(stock=new_stock, price=item.price), new_stock

    success, new_stock = await atomic_update(db, item_id, StockValue, modifier)
    if not success:
        abort(404, f"Item: {item_id} not found!")
    return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    def modifier(item: StockValue):
        new_stock = item.stock - int(amount)
        if new_stock < 0:
            return None, f"Item: {item_id} stock cannot get reduced below zero!"
        return StockValue(stock=new_stock, price=item.price), new_stock

    success, result = await atomic_update(db, item_id, StockValue, modifier)
    if not success:
        if result:
            abort(404, result)
        abort(404, f"Item: {item_id} not found!")
    return Response(f"Item: {item_id} stock updated to: {result}", status=200)


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