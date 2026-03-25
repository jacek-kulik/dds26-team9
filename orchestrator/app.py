import logging
import os
import time
import asyncio
from enum import Enum
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions
from msgspec import msgpack, Struct, field
from quart import Quart, jsonify

import utils.messaging as messaging
from utils.atomic import atomic_update

PROTOCOL = os.getenv("PROTOCOL", "SAGA")

app = Quart("orchestrator-service")


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

class Status(str, Enum):
    WAITING = "WAITING"
    ROLLING_BACK = "ROLLING_BACK"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SagaState(Struct):
    order_id: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    Status: str = Status.WAITING

    stock_done: bool = False
    stock_ok: bool = False
    payment_done: bool = False
    payment_ok: bool = False

    error: str = ""
    created_ts: float = 0.0


async def handle_saga_start(order_id: str, user_id: str, total_cost: int,
                            items: list[tuple[str, int]]):
    state = SagaState(
        order_id=order_id, user_id=user_id, total_cost=total_cost,
        items=items, Status=Status.WAITING, created_ts=time.time(),
    )
    await db.set(order_id, msgpack.encode(state))

    await messaging.publish("stock", {
        "action": "saga_subtract",
        "order_id": order_id,
        "items": msgpack.encode(items),
    })

    await messaging.publish("payment", {
        "action": "pay",
        "order_id": order_id,
        "user_id": user_id,
        "amount": str(total_cost),
    })

# STOCK
async def on_subtract_success(order_id: str):
    next_action = None

    def modifier(state: SagaState):
        nonlocal next_action

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        state.stock_done = True
        state.stock_ok = True

        if not state.payment_done:
            return state, "Payment not done"

        if state.payment_ok:
            state.Status = Status.COMPLETED
            next_action = "complete"
            return state, "Transaction complete"

        state.Status = Status.ROLLING_BACK
        next_action = "rollback_stock"
        return state, "Rolling back, payment already failed"

    await atomic_update(db, order_id, SagaState, modifier)

    if next_action == "complete":
        await messaging.publish("order", {"action": "set_completed", "order_id": order_id})
    elif next_action == "rollback_stock":
        raw = await db.get(order_id)
        state = msgpack.decode(raw, type=SagaState)
        await messaging.publish("stock", {"action": "saga_rollback", "order_id": order_id})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": state.error})


async def on_subtract_failed(order_id: str):
    next_action = None

    def modifier(state: SagaState):
        nonlocal next_action

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        state.stock_done = True
        state.stock_ok = False
        state.error = "Stock subtract failed"

        if not state.payment_done:
            state.Status = Status.ROLLING_BACK
            return state, "Rolling back, payment not done yet"

        if state.payment_ok:
            state.Status = Status.ROLLING_BACK
            next_action = "rollback_payment"
            return state, "Rolling back, payment must be rolled back"

        else:
            state.Status = Status.FAILED
            next_action = "fail"
            return state, "Both services failed, admiting defeat"

    await atomic_update(db, order_id, SagaState, modifier)

    if next_action == "rollback_payment":
        raw = await db.get(order_id)
        state = msgpack.decode(raw, type=SagaState)
        await messaging.publish("payment", {"action": "refund", "order_id": order_id, "user_id": state.user_id, "amount": str(state.total_cost)})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": state.error})
    elif next_action == "fail":
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": "Stock subtract failed"})


#PAYMENT
async def on_pay_success(order_id: str):
    next_action = None

    def modifier(state: SagaState):
        nonlocal next_action

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        state.payment_done = True
        state.payment_ok = True

        if not state.stock_done:
            return state, "Stock not done"

        if state.stock_ok:
            state.Status = Status.COMPLETED
            next_action = "complete"
            return state, "Transaction complete"

        state.Status = Status.ROLLING_BACK
        next_action = "rollback_payment"
        return state, "Rolling back, stock already failed"

    await atomic_update(db, order_id, SagaState, modifier)

    if next_action == "complete":
        await messaging.publish("order", {"action": "set_completed", "order_id": order_id})
    elif next_action == "rollback_payment":
        raw = await db.get(order_id)
        state = msgpack.decode(raw, type=SagaState)
        await messaging.publish("payment", {"action": "refund", "order_id": order_id, "user_id": state.user_id, "amount": str(state.total_cost)})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": state.error})


async def on_pay_failed(order_id: str, error: str):
    next_action = None

    def modifier(state: SagaState):
        nonlocal next_action

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        state.payment_done = True
        state.payment_ok = False
        state.error = error

        if not state.stock_done:
            state.Status = Status.ROLLING_BACK
            return state, "Rolling back, stock not done yet"

        if state.stock_ok:
            state.Status = Status.ROLLING_BACK
            next_action = "rollback_stock"
            return state, "Rolling back, stock must be rolled back"

        else:
            state.Status = Status.FAILED
            next_action = "fail"
            return state, "Both services failed, admitting defeat"

    await atomic_update(db, order_id, SagaState, modifier)

    if next_action == "rollback_stock":
        await messaging.publish("stock", {"action": "saga_rollback", "order_id": order_id})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": error})
    elif next_action == "fail":
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": error})

async def handle_saga_cancel(order_id: str):
    resolve_actions = []

    def modifier(state: SagaState):
        nonlocal resolve_actions

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        state.Status = Status.FAILED
        state.error = "Cancelled, we failed"

        if state.stock_done and state.stock_ok:
            resolve_actions.append("rollback_stock")
        if state.payment_done and state.payment_ok:
            resolve_actions.append("rollback_payment")
        resolve_actions.append("fail")
        return state, "ok"

    await atomic_update(db, order_id, SagaState, modifier)

    raw = await db.get(order_id)
    state = msgpack.decode(raw, type=SagaState)

    for action in resolve_actions:
        if action == "rollback_stock":
            await messaging.publish("stock", {"action": "saga_rollback", "order_id": order_id})
        elif action == "rollback_payment":
            await messaging.publish("payment", {"action": "refund", "order_id": order_id,"user_id": state.user_id, "amount": str(state.total_cost)})
        elif action == "fail":
            await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": state.error})

async def worker(worker_id: str):
    await messaging.ensure_group("orchestrator")

    async for msg_id, data in messaging.consume("orchestrator", worker_id):
        try:
            action = data.get(b"action", b"").decode()
            order_id = data.get(b"order_id", b"").decode()

            if action == "saga_start":
                user_id = data.get(b"user_id", b"").decode()
                total_cost = int(data.get(b"total_cost", b"0"))
                items_raw = data.get(b"items", b"")
                items = msgpack.decode(items_raw)
                await handle_saga_start(order_id, user_id, total_cost, items)

            elif action == "saga_cancel":
                await handle_saga_cancel(order_id)

            elif action == "subtract_success":
                await on_subtract_success(order_id)

            elif action == "subtract_failed":
                await on_subtract_failed(order_id)

            elif action == "pay_success":
                await on_pay_success(order_id)

            elif action == "pay_failed":
                await on_pay_failed(order_id, data.get(b"error", b"").decode())


            await messaging.ack("orchestrator", msg_id)

        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


SAGA_RECOVERY_THRESHOLD = 15

async def recover_incomplete_saga():
    await asyncio.sleep(5)

    while not messaging.is_shutting_down():
        try:
            lock = db.lock("orch_saga_recovery_lock", timeout=30)
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
                        state = msgpack.decode(raw, type=SagaState)
                    except Exception:
                        continue

                    if state.Status in (Status.COMPLETED, Status.FAILED):
                        continue

                    age = now - state.created_ts if state.created_ts > 0 else float('inf')
                    if age < SAGA_RECOVERY_THRESHOLD:
                        continue

                    oid = state.order_id
                    app.logger.info(f"Recovery [{oid}]: stuck in {state.Status} for {age:.0f}s")

                    if state.stock_ok:
                        await messaging.publish("stock", {"action": "saga_rollback", "order_id": oid})
                    if state.payment_ok:
                        await messaging.publish("payment", {
                            "action": "refund", "order_id": oid,
                            "user_id": state.user_id,
                            "amount": str(state.total_cost),
                        })

                    state.Status = Status.FAILED
                    state.error = f"Recovered after timeout ({age:.0f}s)"
                    state.created_ts = now
                    await db.set(key, msgpack.encode(state))

                    await messaging.publish("order", {
                        "action": "set_failed", "order_id": oid,
                        "error": state.error,
                    })
                    recovered += 1

                if recovered > 0:
                    app.logger.info(f"Saga recovery: {recovered} workflows recovered")

            finally:
                try:
                    await lock.release()
                except redis_exceptions.LockError:
                    pass

        except Exception as e:
            app.logger.exception(f"Saga recovery error: {e}")

        await asyncio.sleep(10)


_worker_tasks: list[asyncio.Task] = []
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "6"))
WORKER_DRAIN_TIMEOUT = 10
RUN_MODE = os.environ.get("RUN_MODE", "all")

@app.before_serving
async def startup():
    if RUN_MODE != "web":
        for i in range(WORKER_COUNT):
            task = asyncio.create_task(worker(f"orchestrator-{os.getpid()}-{i}"),name=f"orchestrator-worker-{i}")
            _worker_tasks.append(task)
        if PROTOCOL == "SAGA":
            _worker_tasks.append(asyncio.create_task(recover_incomplete_saga(), name="saga-recovery"))

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

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)