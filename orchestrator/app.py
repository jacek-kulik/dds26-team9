import logging
import os
import time
import asyncio
from enum import Enum
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions
from msgspec import msgpack, Struct
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

class TwoPCStatus(str, Enum):
    PREPARING = "PREPARING"
    COMMITTING = "COMMITTING"
    ABORTING = "ABORTING"
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
    updated_ts: float = 0.0

class TwoPCState(Struct):
    order_id: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]

    status: str = TwoPCStatus.PREPARING

    # Votes
    stock_vote: str = ""       # "", "YES", "NO"
    payment_vote: str = ""     # "", "YES", "NO"

    # Commit acknowledgements
    stock_commit_ack: bool = False
    payment_commit_ack: bool = False

    # Abort acknowledgements
    stock_abort_ack: bool = False
    payment_abort_ack: bool = False

    # Metadata
    error: str = ""
    created_ts: float = 0.0
    decision_ts: float = 0.0
    updated_ts: float = 0.0

def get_2pc_key(order_id: str) -> str:
    return f"2pc:{order_id}"


SAGA_RECOVERY_INDEX = "saga:pending"
TWO_PC_RECOVERY_INDEX = "2pc:pending"


async def _track_saga(order_id: str, score: float):
    await db.zadd(SAGA_RECOVERY_INDEX, {order_id: score})


async def _untrack_saga(order_id: str):
    await db.zrem(SAGA_RECOVERY_INDEX, order_id)


async def _track_two_pc(order_id: str, score: float):
    await db.zadd(TWO_PC_RECOVERY_INDEX, {order_id: score})


async def _untrack_two_pc(order_id: str):
    await db.zrem(TWO_PC_RECOVERY_INDEX, order_id)


async def _get_due_recovery_ids(index_key: str, cutoff_ts: float, limit: int = 100) -> list[str]:
    due = await db.zrangebyscore(index_key, min="-inf", max=cutoff_ts, start=0, num=limit)
    return [member.decode() if isinstance(member, bytes) else member for member in due]

async def handle_saga_start(order_id: str, user_id: str, total_cost: int,
                            items: list[tuple[str, int]]):
    now = time.time()
    state = SagaState(
        order_id=order_id, user_id=user_id, total_cost=total_cost,
        items=items, Status=Status.WAITING, created_ts=now, updated_ts=now,
    )
    async with db.pipeline(transaction=True) as pipe:
        await pipe.set(order_id, msgpack.encode(state))
        await pipe.zadd(SAGA_RECOVERY_INDEX, {order_id: now})
        await pipe.execute()

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
    updated_ts = time.time()
    changed = False

    def modifier(state: SagaState):
        nonlocal next_action, changed

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        changed = True
        state.updated_ts = updated_ts
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

    if changed:
        if next_action == "complete":
            await _untrack_saga(order_id)
        else:
            await _track_saga(order_id, updated_ts)

    if next_action == "complete":
        await messaging.publish("order", {"action": "set_completed", "order_id": order_id})
    elif next_action == "rollback_stock":
        raw = await db.get(order_id)
        state = msgpack.decode(raw, type=SagaState)
        await messaging.publish("stock", {"action": "saga_rollback", "order_id": order_id})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": state.error})


async def on_subtract_failed(order_id: str):
    next_action = None
    updated_ts = time.time()
    changed = False

    def modifier(state: SagaState):
        nonlocal next_action, changed

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        changed = True
        state.updated_ts = updated_ts
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

    if changed:
        if next_action == "fail":
            await _untrack_saga(order_id)
        else:
            await _track_saga(order_id, updated_ts)

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
    updated_ts = time.time()
    changed = False

    def modifier(state: SagaState):
        nonlocal next_action, changed

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        changed = True
        state.updated_ts = updated_ts
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

    if changed:
        if next_action == "complete":
            await _untrack_saga(order_id)
        else:
            await _track_saga(order_id, updated_ts)

    if next_action == "complete":
        await messaging.publish("order", {"action": "set_completed", "order_id": order_id})
    elif next_action == "rollback_payment":
        raw = await db.get(order_id)
        state = msgpack.decode(raw, type=SagaState)
        await messaging.publish("payment", {"action": "refund", "order_id": order_id, "user_id": state.user_id, "amount": str(state.total_cost)})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": state.error})


async def on_pay_failed(order_id: str, error: str):
    next_action = None
    updated_ts = time.time()
    changed = False

    def modifier(state: SagaState):
        nonlocal next_action, changed

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        changed = True
        state.updated_ts = updated_ts
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

    if changed:
        if next_action == "fail":
            await _untrack_saga(order_id)
        else:
            await _track_saga(order_id, updated_ts)

    if next_action == "rollback_stock":
        await messaging.publish("stock", {"action": "saga_rollback", "order_id": order_id})
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": error})
    elif next_action == "fail":
        await messaging.publish("order", {"action": "set_failed", "order_id": order_id, "error": error})

async def handle_saga_cancel(order_id: str):
    resolve_actions = []
    updated_ts = time.time()
    changed = False

    def modifier(state: SagaState):
        nonlocal resolve_actions, changed

        if state.Status in (Status.COMPLETED, Status.FAILED):
            return state, "already resolved"

        changed = True
        state.updated_ts = updated_ts
        state.Status = Status.FAILED
        state.error = "Cancelled, we failed"

        if state.stock_done and state.stock_ok:
            resolve_actions.append("rollback_stock")
        if state.payment_done and state.payment_ok:
            resolve_actions.append("rollback_payment")
        resolve_actions.append("fail")
        return state, "ok"

    await atomic_update(db, order_id, SagaState, modifier)

    if changed:
        await _untrack_saga(order_id)

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

            if action == "checkout_start":
                order_id = data.get(b"order_id", b"").decode()
                user_id = data.get(b"user_id", b"").decode()
                total_cost = int(data.get(b"total_cost", b"0"))
                items = msgpack.decode(data.get(b"items", b""))

                if PROTOCOL == "2PC":
                    await handle_two_pc_start(order_id, user_id, total_cost, items)
                else:
                    await handle_saga_start(order_id, user_id, total_cost, items)

            elif action == "checkout_cancel":
                if PROTOCOL == "2PC":
                    order_id = data.get(b"order_id", b"").decode()
                    await handle_two_pc_cancel(order_id)
                else:
                    await handle_saga_cancel(order_id)

            elif action == "subtract_success":
                await on_subtract_success(order_id)

            elif action == "subtract_failed":
                await on_subtract_failed(order_id)

            elif action == "pay_success":
                await on_pay_success(order_id)

            elif action == "pay_failed":
                await on_pay_failed(order_id, data.get(b"error", b"").decode())

            #########
            #  2PC  #
            #########
            elif action == "vote_yes":
                order_id = data.get(b"order_id", b"").decode()
                who = data.get(b"who", b"").decode()
                await on_vote_yes(order_id, who)

            elif action == "vote_no":
                order_id = data.get(b"order_id", b"").decode()
                who = data.get(b"who", b"").decode()
                await on_vote_no(order_id, who)

            elif action == "commit_ack":
                order_id = data.get(b"order_id", b"").decode()
                who = data.get(b"who", b"").decode()
                await on_commit_ack(order_id, who)

            elif action == "abort_ack":
                order_id = data.get(b"order_id", b"").decode()
                who = data.get(b"who", b"").decode()
                await on_abort_ack(order_id, who)


            await messaging.ack("orchestrator", msg_id)

        except Exception as e:
            app.logger.exception(f"Worker error: {e}")


SAGA_RECOVERY_THRESHOLD = 15
TWO_PC_RECOVERY_THRESHOLD = 15

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

                due_order_ids = await _get_due_recovery_ids(
                    SAGA_RECOVERY_INDEX, now - SAGA_RECOVERY_THRESHOLD
                )

                for oid in due_order_ids:
                    raw = await db.get(oid)
                    if not raw:
                        await _untrack_saga(oid)
                        continue

                    try:
                        state = msgpack.decode(raw, type=SagaState)
                    except Exception:
                        await _untrack_saga(oid)
                        continue

                    if state.Status in (Status.COMPLETED, Status.FAILED):
                        await _untrack_saga(oid)
                        continue

                    age = now - (state.updated_ts or state.created_ts or now)
                    if age < SAGA_RECOVERY_THRESHOLD:
                        await _track_saga(oid, state.updated_ts or now)
                        continue

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
                    state.updated_ts = now
                    await db.set(oid, msgpack.encode(state))
                    await _untrack_saga(oid)

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


###########################################
#                   2PC                   #
###########################################

async def handle_two_pc_start(order_id: str, user_id: str, total_cost: int, items: list[tuple[str, int]]):
    key = get_2pc_key(order_id)
    now = time.time()

    state = TwoPCState(
        order_id=order_id,
        user_id=user_id,
        total_cost=total_cost,
        items=items,
        status=TwoPCStatus.PREPARING,
        stock_vote="",
        payment_vote="",
        stock_commit_ack=False,
        payment_commit_ack=False,
        stock_abort_ack=False,
        payment_abort_ack=False,
        error="",
        created_ts=now,
        decision_ts=0.0,
        updated_ts=now,
    )

    async with db.pipeline(transaction=True) as pipe:
        await pipe.set(key, msgpack.encode(state))
        await pipe.zadd(TWO_PC_RECOVERY_INDEX, {order_id: now})
        await pipe.execute()

    # Send prepare to stock
    await messaging.publish("stock", {
        "action": "stock_prepare",
        "order_id": order_id,
        "items": msgpack.encode(items),
    })

    # Send prepare to payment
    await messaging.publish("payment", {
        "action": "payment_prepare",
        "order_id": order_id,
        "user_id": user_id,
        "amount": str(total_cost),
    })



async def on_vote_yes(order_id: str, who: str):
    key = get_2pc_key(order_id)
    updated_ts = time.time()
    changed = False

    def modifier(state: TwoPCState):
        nonlocal changed

        if state is None:
            return None, "missing"

        # Ignore if already decided or finished
        if state.status in (TwoPCStatus.COMMITTING, TwoPCStatus.ABORTING,
                            TwoPCStatus.COMPLETED, TwoPCStatus.FAILED):
            return state, state

        changed = True
        state.updated_ts = updated_ts
        if who == "stock":
            state.stock_vote = "YES"
        elif who == "payment":
            state.payment_vote = "YES"

        # If both voted YES → COMMIT
        if state.stock_vote == "YES" and state.payment_vote == "YES":
            state.status = TwoPCStatus.COMMITTING
            state.decision_ts = time.time()

        return state, state

    success, updated_state = await atomic_update(db, key, TwoPCState, modifier)
    if not success or updated_state is None:
        return

    if changed:
        if updated_state.status in (TwoPCStatus.COMPLETED, TwoPCStatus.FAILED):
            await _untrack_two_pc(order_id)
        else:
            await _track_two_pc(order_id, updated_ts)

    # If we just moved to COMMITTING → send commit
    if updated_state.status == TwoPCStatus.COMMITTING:
        await messaging.publish("stock", {
            "action": "stock_commit",
            "order_id": order_id,
        })
        await messaging.publish("payment", {
            "action": "payment_commit",
            "order_id": order_id,
        })

async def on_vote_no(order_id: str, who: str):
    key = get_2pc_key(order_id)
    updated_ts = time.time()
    changed = False

    def modifier(state: TwoPCState):
        nonlocal changed

        if state is None:
            return None, "missing"

        # Ignore if already aborting or finished
        if state.status in (
                TwoPCStatus.COMMITTING,
                TwoPCStatus.ABORTING,
                TwoPCStatus.COMPLETED,
                TwoPCStatus.FAILED,
        ):
            return state, state

        changed = True
        state.updated_ts = updated_ts
        if who == "stock":
            state.stock_vote = "NO"
        elif who == "payment":
            state.payment_vote = "NO"

        # Move to ABORTING
        state.status = TwoPCStatus.ABORTING
        state.decision_ts = time.time()
        state.error = f"{who} voted NO"

        return state, state

    success, updated_state = await atomic_update(db, key, TwoPCState, modifier)
    if not success or updated_state is None:
        return

    if changed:
        if updated_state.status in (TwoPCStatus.COMPLETED, TwoPCStatus.FAILED):
            await _untrack_two_pc(order_id)
        else:
            await _track_two_pc(order_id, updated_ts)

    # Send abort to BOTH participants
    await messaging.publish("stock", {
        "action": "stock_abort",
        "order_id": order_id,
    })
    await messaging.publish("payment", {
        "action": "payment_abort",
        "order_id": order_id,
    })

async def on_commit_ack(order_id: str, who: str):
    key = get_2pc_key(order_id)
    updated_ts = time.time()
    changed = False

    def modifier(state: TwoPCState):
        nonlocal changed

        if state is None:
            return None, "missing"

        if state.status != TwoPCStatus.COMMITTING:
            return state, state

        changed = True
        state.updated_ts = updated_ts
        if who == "stock":
            state.stock_commit_ack = True
        elif who == "payment":
            state.payment_commit_ack = True

        if state.stock_commit_ack and state.payment_commit_ack:
            state.status = TwoPCStatus.COMPLETED

        return state, state

    success, updated_state = await atomic_update(db, key, TwoPCState, modifier)
    if not success or updated_state is None:
        return

    if changed:
        if updated_state.status == TwoPCStatus.COMPLETED:
            await _untrack_two_pc(order_id)
        else:
            await _track_two_pc(order_id, updated_ts)

    if updated_state.status == TwoPCStatus.COMPLETED:
        await messaging.publish("order", {
            "action": "set_completed",
            "order_id": order_id,
        })

async def on_abort_ack(order_id: str, who: str):
    key = get_2pc_key(order_id)
    updated_ts = time.time()
    changed = False

    def modifier(state: TwoPCState):
        nonlocal changed

        if state is None:
            return None, "missing"

        if state.status != TwoPCStatus.ABORTING:
            return state, state

        changed = True
        state.updated_ts = updated_ts
        if who == "stock":
            state.stock_abort_ack = True
        elif who == "payment":
            state.payment_abort_ack = True

        if state.stock_abort_ack and state.payment_abort_ack:
            state.status = TwoPCStatus.FAILED

        return state, state

    success, updated_state = await atomic_update(db, key, TwoPCState, modifier)
    if not success or updated_state is None:
        return

    if changed:
        if updated_state.status == TwoPCStatus.FAILED:
            await _untrack_two_pc(order_id)
        else:
            await _track_two_pc(order_id, updated_ts)

    if updated_state.status == TwoPCStatus.FAILED:
        await messaging.publish("order", {
            "action": "set_failed",
            "order_id": order_id,
            "error": updated_state.error or "2PC aborted",
        })

async def handle_two_pc_cancel(order_id: str):
    key = get_2pc_key(order_id)
    updated_ts = time.time()
    changed = False

    def modifier(state: TwoPCState):
        nonlocal changed

        if state is None:
            return None, "missing"

        # Already finished → ignore
        if state.status in (
                TwoPCStatus.COMPLETED,
                TwoPCStatus.FAILED,
        ):
            return state, state

        # Already aborting → ignore
        if state.status == TwoPCStatus.ABORTING:
            return state, state

        # If already committing → cannot cancel
        if state.status == TwoPCStatus.COMMITTING:
            return state, state

        # Only valid case: PREPARING → ABORT
        if state.status == TwoPCStatus.PREPARING:
            state.status = TwoPCStatus.ABORTING
            state.decision_ts = time.time()
            state.error = "Cancelled, we failed"
            state.updated_ts = updated_ts
            changed = True

        return state, state

    success, updated_state = await atomic_update(db, key, TwoPCState, modifier)
    if not success or updated_state is None:
        return

    if changed:
        await _track_two_pc(order_id, updated_ts)

    # If we moved to ABORTING → send aborts
    if updated_state.status == TwoPCStatus.ABORTING:
        await messaging.publish("stock", {
            "action": "stock_abort",
            "order_id": order_id,
        })
        await messaging.publish("payment", {
            "action": "payment_abort",
            "order_id": order_id,
        })

async def recover_incomplete_2pc():
    await asyncio.sleep(5)

    while not messaging.is_shutting_down():
        try:
            now = time.time()

            lock = db.lock("orch_2pc_recovery_lock", timeout=30)
            if not await lock.acquire(blocking=False):
                await asyncio.sleep(10)
                continue

            try:
                due_order_ids = await _get_due_recovery_ids(
                    TWO_PC_RECOVERY_INDEX, now - TWO_PC_RECOVERY_THRESHOLD
                )

                for order_id in due_order_ids:
                    key = get_2pc_key(order_id)
                    raw = await db.get(key)
                    if not raw:
                        await _untrack_two_pc(order_id)
                        continue

                    try:
                        state = msgpack.decode(raw, type=TwoPCState)
                    except Exception:
                        await _untrack_two_pc(order_id)
                        continue

                    if state.status in (TwoPCStatus.COMPLETED, TwoPCStatus.FAILED):
                        await _untrack_two_pc(order_id)
                        continue

                    age = now - (state.updated_ts or state.decision_ts or state.created_ts or now)

                    if age < TWO_PC_RECOVERY_THRESHOLD:
                        await _track_two_pc(order_id, state.updated_ts or now)
                        continue

                    # Case 1: stuck in PREPARING -> force ABORT
                    if state.status == TwoPCStatus.PREPARING:
                        def modifier(current: TwoPCState):
                            if current is None:
                                return None, "missing"

                            if current.status != TwoPCStatus.PREPARING:
                                return current, "ignore"

                            current.status = TwoPCStatus.ABORTING
                            current.decision_ts = time.time()
                            current.updated_ts = time.time()
                            current.error = f"Recovered after timeout ({age:.0f}s)"
                            return current, current

                        success, updated_state = await atomic_update(db, key, TwoPCState, modifier)
                        if success and updated_state is not None and updated_state.status == TwoPCStatus.ABORTING:
                            await _track_two_pc(order_id, updated_state.updated_ts or now)
                            await messaging.publish("stock", {
                                "action": "stock_abort",
                                "order_id": order_id,
                            })
                            await messaging.publish("payment", {
                                "action": "payment_abort",
                                "order_id": order_id,
                            })

                    # Case 2: stuck in ABORTING -> resend abort
                    elif state.status == TwoPCStatus.ABORTING:
                        await _track_two_pc(order_id, state.updated_ts or now)
                        await messaging.publish("stock", {
                            "action": "stock_abort",
                            "order_id": order_id,
                        })
                        await messaging.publish("payment", {
                            "action": "payment_abort",
                            "order_id": order_id,
                        })

                    # Case 3: stuck in COMMITTING -> resend commit
                    elif state.status == TwoPCStatus.COMMITTING:
                        await _track_two_pc(order_id, state.updated_ts or now)
                        await messaging.publish("stock", {
                            "action": "stock_commit",
                            "order_id": order_id,
                        })
                        await messaging.publish("payment", {
                            "action": "payment_commit",
                            "order_id": order_id,
                        })

            finally:
                try:
                    await lock.release()
                except redis_exceptions.LockError:
                    pass

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[orchestrator-recovery-2pc] error: {e}")

        await asyncio.sleep(5)

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
        if PROTOCOL == "2PC":
            _worker_tasks.append(asyncio.create_task(recover_incomplete_2pc(),  name="2pc-recovery"))

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