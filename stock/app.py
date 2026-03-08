import logging
import os
import atexit
import signal
import time
import uuid
import threading
import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import utils.messaging as messaging
from utils.atomic import atomic_update

DB_ERROR_STR = "DB error"

app = Flask("stock-service")


def _make_redis_client(host_var='REDIS_HOST', port_var='REDIS_PORT',
                       password_var='REDIS_PASSWORD', db_var='REDIS_DB',
                       sentinel_host_var='REDIS_SENTINEL_HOST',
                       sentinel_port_var='REDIS_SENTINEL_PORT',
                       sentinel_master_var='REDIS_SENTINEL_MASTER') -> redis.Redis:
    sentinel_host = os.environ.get(sentinel_host_var)
    if sentinel_host:
        sentinel_port = int(os.environ.get(sentinel_port_var, '26379'))
        master_name = os.environ.get(sentinel_master_var, 'mymaster')
        password = os.environ.get(password_var, '')
        db_num = int(os.environ.get(db_var, '0'))
        sentinel = redis.Sentinel(
            [(sentinel_host, sentinel_port)],
            sentinel_kwargs={'password': password},
            password=password,
            db=db_num,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        return sentinel.master_for(master_name)
    else:
        return redis.Redis(
            host=os.environ[host_var],
            port=int(os.environ[port_var]),
            password=os.environ[password_var],
            db=int(os.environ[db_var]),
            socket_timeout=5,
            retry_on_timeout=True,
        )


db = _make_redis_client()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

class StockTx(Struct):
    items: list[tuple[str, int]]
    state: str  # PREPARED | COMMITTED | ABORTED
    created_ts: float = 0.0

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

def handle_stock_prepare(order_id: str, items: list[tuple[str, int]]):
    tx_key = f"tx:{order_id}"

    raw = db.get(tx_key)
    if raw:
        tx = msgpack.decode(raw, type=StockTx)
        if tx.state == "PREPARED":
            # Duplicate prepare for the current in-flight attempt → idempotent shortcut
            messaging.publish("order", {"action": "vote_yes", "order_id": order_id, "who": "stock"})
            return
        # COMMITTED or ABORTED are terminal states from a *previous* checkout
        # attempt for this order.  Delete the stale key and re-evaluate fresh.
        db.delete(tx_key)

    # try subtract all; if partial success, undo
    subtracted: list[tuple[str, int]] = []

    for item_id, amount in items:
        def modifier(it: StockValue):
            new_stock = it.stock - int(amount)
            if new_stock < 0:
                return None, "out_of_stock"
            return StockValue(stock=new_stock, price=it.price), None

        ok, err = atomic_update(db, item_id, StockValue, modifier)
        if not ok:
            # rollback already subtracted
            for rb_item, rb_amt in subtracted:
                def rb_mod(it: StockValue):
                    return StockValue(stock=it.stock + int(rb_amt), price=it.price), None
                atomic_update(db, rb_item, StockValue, rb_mod)

            db.set(tx_key, msgpack.encode(StockTx(items=items, state="ABORTED", created_ts=time.time())))
            messaging.publish("order", {"action": "vote_no", "order_id": order_id, "who": "stock"})
            return

        subtracted.append((item_id, int(amount)))

    # all good
    db.set(tx_key, msgpack.encode(StockTx(items=items, state="PREPARED", created_ts=time.time())))
    messaging.publish("order", {"action": "vote_yes", "order_id": order_id, "who": "stock"})


def handle_stock_commit(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = db.get(tx_key)
    if not raw:
        return

    tx = msgpack.decode(raw, type=StockTx)

    if tx.state == "COMMITTED":
        messaging.publish("order", {"action": "commit_ack", "order_id": order_id, "who": "stock"})
        return

    if tx.state != "PREPARED":
        return

    tx.state = "COMMITTED"
    db.set(tx_key, msgpack.encode(tx))

    messaging.publish("order", {"action": "commit_ack", "order_id": order_id, "who": "stock"})


def handle_stock_abort(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = db.get(tx_key)
    if not raw:
        # No tx record — we never prepared, or the key was already cleaned up.
        # Still acknowledge so the coordinator collects all acks.
        messaging.publish("order", {"action": "abort_ack", "order_id": order_id, "who": "stock"})
        return

    tx = msgpack.decode(raw, type=StockTx)

    if tx.state == "ABORTED":
        messaging.publish("order", {"action": "abort_ack", "order_id": order_id, "who": "stock"})
        return

    if tx.state != "PREPARED":
        return

    # undo the subtracts
    for item_id, amount in tx.items:
        def modifier(it: StockValue):
            return StockValue(stock=it.stock + int(amount), price=it.price), None
        atomic_update(db, item_id, StockValue, modifier)

    tx.state = "ABORTED"
    db.set(tx_key, msgpack.encode(tx))

    messaging.publish("order", {"action": "abort_ack", "order_id": order_id, "who": "stock"})


DISPATCH = {
    "subtract": handle_subtract,
    "add":      handle_add,
    "stock_prepare": handle_stock_prepare,
    "stock_commit":  handle_stock_commit,
    "stock_abort":   handle_stock_abort,
}


def worker(worker_id: str):
    messaging.ensure_group("stock")

    for msg_id, data in messaging.consume("stock", worker_id):
        try:
            action = data.get(b"action", b"").decode()
            handler = DISPATCH.get(action)

            if handler:
                if action == "stock_prepare":
                    order_id = data.get(b"order_id", b"").decode()
                    items_raw = data.get(b"items", b"")
                    items = msgpack.decode(items_raw)
                    handler(order_id, items)

                elif action in ("stock_commit", "stock_abort"):
                    order_id = data.get(b"order_id", b"").decode()
                    handler(order_id)

                else:
                    order_id = data.get(b"order_id", b"").decode()
                    item_id  = data.get(b"item_id", b"").decode()
                    amount   = int(data.get(b"amount", b"0") or b"0")
                    handler(order_id, item_id, amount)

            messaging.ack("stock", msg_id)

        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


_worker_threads: list[threading.Thread] = []

def start_workers(n: int = 2):
    for i in range(n):
        t = threading.Thread(
            target=worker,
            args=(f"stock-{os.getpid()}-{i}",),
            daemon=True,
        )
        t.start()
        _worker_threads.append(t)


WORKER_DRAIN_TIMEOUT = 10  # seconds to wait for workers to finish current message


def _graceful_shutdown(signum, frame):
    """Called on SIGTERM — drain message workers, then let gunicorn finish HTTP requests."""
    messaging.request_shutdown()
    for t in _worker_threads:
        t.join(timeout=WORKER_DRAIN_TIMEOUT)
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


signal.signal(signal.SIGTERM, _graceful_shutdown)


# --- Stale tx: key cleanup ---------------------------------------------------
TX_PREPARED_TIMEOUT = 60       # seconds before a PREPARED tx is considered stuck
TX_TERMINAL_TTL     = 300      # seconds before a COMMITTED/ABORTED tx key is deleted
TX_CLEANUP_INTERVAL = 15       # seconds between cleanup sweeps


def _cleanup_stale_tx():
    """Periodically clean up stale 2PC transaction keys.

    • PREPARED keys older than TX_PREPARED_TIMEOUT: roll back the held stock
      and mark the tx ABORTED (prevents permanent resource locks).
    • COMMITTED / ABORTED keys older than TX_TERMINAL_TTL: delete them
      (no longer needed for idempotency).
    """
    time.sleep(10)  # let workers boot first

    while not messaging.is_shutting_down():
        try:
            lock = db.lock("stock_tx_cleanup_lock", timeout=30)
            if not lock.acquire(blocking=False):
                time.sleep(TX_CLEANUP_INTERVAL)
                continue

            try:
                now = time.time()
                cleaned = 0

                for key in db.scan_iter(match="tx:*"):
                    if db.type(key) != b"string":
                        continue

                    raw = db.get(key)
                    if not raw:
                        continue

                    try:
                        tx = msgpack.decode(raw, type=StockTx)
                    except Exception:
                        continue

                    age = now - tx.created_ts if tx.created_ts > 0 else float('inf')

                    if tx.state == "PREPARED" and age > TX_PREPARED_TIMEOUT:
                        # Roll back the subtracted stock
                        order_id = key.decode().removeprefix("tx:") if isinstance(key, bytes) else key.removeprefix("tx:")
                        for item_id, amount in tx.items:
                            def modifier(it: StockValue):
                                return StockValue(stock=it.stock + int(amount), price=it.price), None
                            atomic_update(db, item_id, StockValue, modifier)

                        tx.state = "ABORTED"
                        tx.created_ts = now
                        db.set(key, msgpack.encode(tx))

                        app.logger.info(f"Stale tx cleanup: rolled back PREPARED tx {key} "
                                        f"(age {age:.0f}s, {len(tx.items)} items)")
                        cleaned += 1

                    elif tx.state in ("COMMITTED", "ABORTED") and age > TX_TERMINAL_TTL:
                        db.delete(key)
                        app.logger.debug(f"Stale tx cleanup: deleted terminal tx {key} "
                                         f"(state={tx.state}, age {age:.0f}s)")
                        cleaned += 1

                if cleaned > 0:
                    app.logger.info(f"Stale tx cleanup pass: {cleaned} key(s) cleaned")

            finally:
                try:
                    lock.release()
                except redis.exceptions.LockError:
                    pass

        except Exception as e:
            app.logger.exception(f"Stale tx cleanup error: {e}")

        time.sleep(TX_CLEANUP_INTERVAL)


threading.Thread(target=_cleanup_stale_tx, daemon=True).start()

start_workers()


@app.get('/health')
def health():
    try:
        db.ping()
        return jsonify({"status": "ok"}), 200
    except redis.exceptions.RedisError:
        return jsonify({"status": "unhealthy"}), 503


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
    def modifier(item: StockValue):
        new_stock = item.stock + int(amount)
        return StockValue(stock=new_stock, price=item.price), new_stock

    success, new_stock = atomic_update(db, item_id, StockValue, modifier)
    if not success:
        abort(400, f"Item: {item_id} not found!")
    return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    def modifier(item: StockValue):
        new_stock = item.stock - int(amount)
        if new_stock < 0:
            return None, f"Item: {item_id} stock cannot get reduced below zero!"
        return StockValue(stock=new_stock, price=item.price), new_stock

    success, result = atomic_update(db, item_id, StockValue, modifier)
    if not success:
        if result:
            abort(400, result)
        abort(400, f"Item: {item_id} not found!")
    return Response(f"Item: {item_id} stock updated to: {result}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)