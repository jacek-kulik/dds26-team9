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

app = Flask("payment-service")


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


class UserValue(Struct):
    credit: int

class PaymentTx(Struct):
    user_id: str
    amount: int
    state: str   # PREPARED | COMMITTED | ABORTED
    created_ts: float = 0.0


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

def handle_prepare(order_id: str, user_id: str, amount: int):
    tx_key = f"tx:{order_id}"

    # If tx already exists → idempotent behavior
    raw = db.get(tx_key)
    if raw:
        tx = msgpack.decode(raw, type=PaymentTx)

        if tx.state == "PREPARED":
            messaging.publish("order", {
                "action": "vote_yes",
                "order_id": order_id,
                "who": "payment"
            })
            return

        if tx.state == "COMMITTED":
            messaging.publish("order", {
                "action": "vote_yes",
                "order_id": order_id,
                "who": "payment"
            })
            return

        if tx.state == "ABORTED":
            messaging.publish("order", {
                "action": "vote_no",
                "order_id": order_id,
                "who": "payment"
            })
            return

    # Check credit WITHOUT modifying
    user = get_user_from_db(user_id)

    if user.credit < amount:
        messaging.publish("order", {
            "action": "vote_no",
            "order_id": order_id,
            "who": "payment"
        })
        return

    # Store PREPARED state
    db.set(tx_key, msgpack.encode(
        PaymentTx(user_id=user_id, amount=amount, state="PREPARED", created_ts=time.time())
    ))

    messaging.publish("order", {
        "action": "vote_yes",
        "order_id": order_id,
        "who": "payment"
    })

def handle_commit(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = db.get(tx_key)

    if not raw:
        return

    tx = msgpack.decode(raw, type=PaymentTx)

    if tx.state == "COMMITTED":
        messaging.publish("order", {
            "action": "commit_ack",
            "order_id": order_id,
            "who": "payment"
        })
        return

    if tx.state != "PREPARED":
        return

    def modifier(u: UserValue):
        return UserValue(credit=u.credit - tx.amount), None

    atomic_update(db, tx.user_id, UserValue, modifier)

    tx.state = "COMMITTED"
    db.set(tx_key, msgpack.encode(tx))

    messaging.publish("order", {
        "action": "commit_ack",
        "order_id": order_id,
        "who": "payment"
    })

def handle_abort(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = db.get(tx_key)

    if not raw:
        return

    tx = msgpack.decode(raw, type=PaymentTx)

    if tx.state == "ABORTED":
        messaging.publish("order", {
            "action": "abort_ack",
            "order_id": order_id,
            "who": "payment"
        })
        return

    tx.state = "ABORTED"
    db.set(tx_key, msgpack.encode(tx))

    messaging.publish("order", {
        "action": "abort_ack",
        "order_id": order_id,
        "who": "payment"
    })

DISPATCH = {
    "pay":    handle_pay,
    "refund": handle_refund,
    "payment_prepare": handle_prepare,
    "payment_commit": handle_commit,
    "payment_abort": handle_abort,
}

def worker(worker_id: str):
    messaging.ensure_group("payment")

    for msg_id, data in messaging.consume("payment", worker_id):
        try:
            action = data.get(b"action", b"").decode()
            handler = DISPATCH.get(action)

            if handler:
                order_id = data.get(b"order_id", b"").decode()
                user_id  = data.get(b"user_id", b"").decode()
                amount   = int(data.get(b"amount", b"0") or b"0")

                if action == "payment_prepare":
                    handler(order_id, user_id, amount)
                elif action == "payment_commit":
                    handler(order_id)
                elif action == "payment_abort":
                    handler(order_id)
                else:
                    handler(order_id, user_id, amount)

            messaging.ack("payment", msg_id)

        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


_worker_threads: list[threading.Thread] = []

def start_workers(n: int = 2):
    for i in range(n):
        t = threading.Thread(
            target=worker,
            args=(f"payment-{os.getpid()}-{i}",),
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

    • PREPARED keys older than TX_PREPARED_TIMEOUT: mark ABORTED.
      (Payment only checks credit at prepare time — nothing was deducted,
       so no rollback is needed.)
    • COMMITTED / ABORTED keys older than TX_TERMINAL_TTL: delete them
      (no longer needed for idempotency).
    """
    time.sleep(10)  # let workers boot first

    while not messaging.is_shutting_down():
        try:
            lock = db.lock("payment_tx_cleanup_lock", timeout=30)
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
                        tx = msgpack.decode(raw, type=PaymentTx)
                    except Exception:
                        continue

                    age = now - tx.created_ts if tx.created_ts > 0 else float('inf')

                    if tx.state == "PREPARED" and age > TX_PREPARED_TIMEOUT:
                        tx.state = "ABORTED"
                        tx.created_ts = now
                        db.set(key, msgpack.encode(tx))

                        app.logger.info(f"Stale tx cleanup: aborted PREPARED tx {key} "
                                        f"(age {age:.0f}s)")
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
    def modifier(user: UserValue):
        new_credit = user.credit + int(amount)
        return UserValue(credit=new_credit), new_credit

    success, new_credit = atomic_update(db, user_id, UserValue, modifier)
    if not success:
        abort(400, f"User: {user_id} not found!")
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")

    def modifier(user: UserValue):
        new_credit = user.credit - int(amount)
        if new_credit < 0:
            return None, f"User: {user_id} credit cannot get reduced below zero!"
        return UserValue(credit=new_credit), new_credit

    success, result = atomic_update(db, user_id, UserValue, modifier)
    if not success:
        if result:
            abort(400, result)
        abort(400, f"User: {user_id} not found!")
    return Response(f"User: {user_id} credit updated to: {result}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)