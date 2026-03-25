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

app = Quart("payment-service")


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


class UserValue(Struct):
    credit: int

class PaymentTx(Struct):
    user_id: str
    amount: int
    state: str   # PREPARED | COMMITTED | ABORTED
    created_ts: float = 0.0


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except redis_exceptions.RedisError:
        return abort(405, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(405, f"User: {user_id} not found!")
    return entry

async def handle_pay(order_id: str, user_id: str, amount: int):
    def modifier(u: UserValue):
        new_credit = u.credit - amount
        if new_credit < 0:
            return None, f"User: {user_id} does not have enough money!"
        return UserValue(credit=new_credit), None

    ok, err = await atomic_update(db, user_id, UserValue, modifier)

    if not ok:
        await messaging.publish("orchestrator", {
            "action": "pay_failed",
            "order_id": order_id,
            "error": err or f"User: {user_id} not found!",
        })
        return

    await messaging.publish("orchestrator", {"action": "pay_success", "order_id": order_id})


async def handle_refund(order_id: str, user_id: str, amount: int):
    def modifier(u: UserValue):
        return UserValue(credit=u.credit + amount), None

    ok, err = await atomic_update(db, user_id, UserValue, modifier)

    if not ok:
        await messaging.publish("orchestrator", {
            "action": "refund_failed",
            "order_id": order_id,
            "error": err or f"User: {user_id} not found!",
        })
        return
    await messaging.publish("orchestrator", {"action": "refund_success", "order_id": order_id})

async def handle_prepare(order_id: str, user_id: str, amount: int):
    tx_key = f"tx:{order_id}"

    raw = await db.get(tx_key)
    if raw:
        tx = msgpack.decode(raw, type=PaymentTx)

        if tx.state == "PREPARED":
            await messaging.publish("orchestrator", {
                "action": "vote_yes",
                "order_id": order_id,
                "who": "payment",
            })
            return

        # COMMITTED or ABORTED are terminal states from a *previous* checkout
        # attempt for this order.  Delete the stale key and re-evaluate fresh.
        await db.delete(tx_key)

    # Check credit WITHOUT modifying
    user = await get_user_from_db(user_id)

    if user.credit < amount:
        await messaging.publish("orchestrator", {
            "action": "vote_no",
            "order_id": order_id,
            "who": "payment",
        })
        return

    # Store PREPARED state
    await db.set(tx_key, msgpack.encode(
        PaymentTx(user_id=user_id, amount=amount, state="PREPARED", created_ts=time.time())
    ))

    await messaging.publish("orchestrator", {
        "action": "vote_yes",
        "order_id": order_id,
        "who": "payment",
    })

async def handle_commit(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = await db.get(tx_key)

    if not raw:
        return

    tx = msgpack.decode(raw, type=PaymentTx)

    if tx.state == "COMMITTED":
        await messaging.publish("orchestrator", {
            "action": "commit_ack",
            "order_id": order_id,
            "who": "payment",
        })
        return

    if tx.state != "PREPARED":
        return

    def modifier(u: UserValue):
        return UserValue(credit=u.credit - tx.amount), None

    await atomic_update(db, tx.user_id, UserValue, modifier)

    tx.state = "COMMITTED"
    await db.set(tx_key, msgpack.encode(tx))

    await messaging.publish("orchestrator", {
        "action": "commit_ack",
        "order_id": order_id,
        "who": "payment",
    })

async def handle_abort(order_id: str):
    tx_key = f"tx:{order_id}"
    raw = await db.get(tx_key)

    if not raw:
        # No tx record — we voted no before creating one, or it was already
        # cleaned up.  Still acknowledge so the coordinator collects all acks.
        await messaging.publish("orchestrator", {
            "action": "abort_ack",
            "order_id": order_id,
            "who": "payment",
        })
        return

    tx = msgpack.decode(raw, type=PaymentTx)

    if tx.state == "ABORTED":
        await messaging.publish("orchestrator", {
            "action": "abort_ack",
            "order_id": order_id,
            "who": "payment",
        })
        return

    tx.state = "ABORTED"
    await db.set(tx_key, msgpack.encode(tx))

    await messaging.publish("orchestrator", {
        "action": "abort_ack",
        "order_id": order_id,
        "who": "payment",
    })

DISPATCH = {
    "pay":    handle_pay,
    "refund": handle_refund,
    "payment_prepare": handle_prepare,
    "payment_commit": handle_commit,
    "payment_abort": handle_abort,
}

async def worker(worker_id: str):
    await messaging.ensure_group("payment")

    async for msg_id, data in messaging.consume("payment", worker_id):
        try:
            action = data.get(b"action", b"").decode()
            handler = DISPATCH.get(action)

            if handler:
                order_id = data.get(b"order_id", b"").decode()
                user_id  = data.get(b"user_id", b"").decode()
                amount   = int(data.get(b"amount", b"0") or b"0")

                if action == "payment_prepare":
                    await handler(order_id, user_id, amount)
                elif action in ("payment_commit", "payment_abort"):
                    await handler(order_id)
                else:
                    await handler(order_id, user_id, amount)

            await messaging.ack("payment", msg_id)

        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


_worker_tasks: list[asyncio.Task] = []




TX_PREPARED_TIMEOUT = 60
TX_TERMINAL_TTL     = 300
TX_CLEANUP_INTERVAL = 15


async def _cleanup_stale_tx():
    """Periodically clean up stale 2PC transaction keys.

    • PREPARED keys older than TX_PREPARED_TIMEOUT: mark ABORTED.
      (Payment only checks credit at prepare time — nothing was deducted,
       so no rollback is needed.)
    • COMMITTED / ABORTED keys older than TX_TERMINAL_TTL: delete them
      (no longer needed for idempotency).
    """
    await asyncio.sleep(10)  # let workers boot first

    while not messaging.is_shutting_down():
        try:
            lock = db.lock("payment_tx_cleanup_lock", timeout=30)
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
                        tx = msgpack.decode(raw, type=PaymentTx)
                    except Exception:
                        continue

                    age = now - tx.created_ts if tx.created_ts > 0 else float('inf')

                    if tx.state == "PREPARED" and age > TX_PREPARED_TIMEOUT:
                        tx.state = "ABORTED"
                        tx.created_ts = now
                        await db.set(key, msgpack.encode(tx))

                        app.logger.info(f"Stale tx cleanup: aborted PREPARED tx {key} "
                                        f"(age {age:.0f}s)")
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

WORKER_COUNT = 6
WORKER_DRAIN_TIMEOUT = 10

RUN_MODE = os.environ.get("RUN_MODE", "all")

@app.before_serving
async def startup():
    if RUN_MODE != "web":
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(worker(f"payment-{os.getpid()}-{i}"),name=f"payment-worker-{i}")
            _worker_tasks.append(task)

        _worker_tasks.append(asyncio.create_task(_cleanup_stale_tx(), name="payment-tx-cleanup"))

@app.after_serving
async def shutdown():
    messaging.request_shutdown()

    if _worker_tasks:
        _, pending = await asyncio.wait(_worker_tasks, timeout=WORKER_DRAIN_TIMEOUT)
        for t in pending:
            t.cancel()

    await db.aclose()
    await messaging.close()

@app.get('/health')
async def health():
    try:
        await db.ping()
        return jsonify({"status": "ok"}), 200
    except redis_exceptions.RedisError:
        return jsonify({"status": "unhealthy"}), 503


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except redis_exceptions.RedisError:
        return abort(406, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except redis_exceptions.RedisError:
        return abort(407, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    def modifier(user: UserValue):
        new_credit = user.credit + int(amount)
        return UserValue(credit=new_credit), new_credit

    success, new_credit = await atomic_update(db, user_id, UserValue, modifier)
    if not success:
        abort(408, f"User: {user_id} not found!")
    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")

    def modifier(user: UserValue):
        new_credit = user.credit - int(amount)
        if new_credit < 0:
            return None, f"User: {user_id} credit cannot get reduced below zero!"
        return UserValue(credit=new_credit), new_credit

    success, result = await atomic_update(db, user_id, UserValue, modifier)
    if not success:
        if result:
            abort(409, result)
        abort(409, f"User: {user_id} not found!")
    return Response(f"User: {user_id} credit updated to: {result}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)