import os
import time
import logging
import random
import socket
import asyncio
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

_shutdown_event = asyncio.Event()


def request_shutdown():
    """Signal all consume() loops in this process to drain and exit."""
    logger.info("Shutdown requested — draining workers …")
    _shutdown_event.set()


def is_shutting_down() -> bool:
    return _shutdown_event.is_set()

_SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
_PRODUCER_ID = f"{_SERVICE_NAME}-{socket.gethostname()}-{os.getpid()}"

STREAMS = {
    "order":        "order.events",
    "stock":        "stock.events",
    "payment":      "payment.events",
    "orchestrator": "orchestrator.events",
}

GROUPS = {
    "order":        "order-workers",
    "stock":        "stock-workers",
    "payment":      "payment-workers",
    "orchestrator": "orchestrator-workers",
}

_SERVICE_REDIS_HOST_VARS = {
    "order":        "REDIS_ORDER_HOST",
    "stock":        "REDIS_STOCK_HOST",
    "payment":      "REDIS_PAYMENT_HOST",
    "orchestrator": "REDIS_ORCHESTRATOR_HOST",
}

_DEFAULT_PASSWORD = os.environ.get("BUS_REDIS_PASSWORD",
                    os.environ.get("REDIS_PASSWORD", "redis"))
_DEFAULT_PORT = int(os.environ.get("BUS_REDIS_PORT",
                    os.environ.get("REDIS_PORT", "6379")))

_bus_clients: dict[str, aioredis.Redis] = {}


def _get_bus_client(target_service: str) -> aioredis.Redis:
    if target_service in _bus_clients:
        return _bus_clients[target_service]

    shared_bus_host = os.environ.get("BUS_REDIS_HOST")
    host_var = _SERVICE_REDIS_HOST_VARS.get(target_service)
    target_host = os.environ.get(host_var) if host_var else None

    if target_host:
        host = target_host
        db_num = int(os.environ.get("BUS_REDIS_DB", "0"))
    elif shared_bus_host:
        host = shared_bus_host
        db_num = int(os.environ.get("BUS_REDIS_DB", "0"))
    else:
        host = os.environ.get("REDIS_HOST", "localhost")
        db_num = int(os.environ.get("BUS_REDIS_DB",
                     os.environ.get("REDIS_DB", "0")))

    sentinel_host = os.environ.get("BUS_REDIS_SENTINEL_HOST")
    if sentinel_host:
        sentinel_port = int(os.environ.get("BUS_REDIS_SENTINEL_PORT", "26379"))
        master_name = os.environ.get("BUS_REDIS_SENTINEL_MASTER", "mymaster")
        sentinel = aioredis.Sentinel(
            [(sentinel_host, sentinel_port)],
            sentinel_kwargs={"password": _DEFAULT_PASSWORD},
            password=_DEFAULT_PASSWORD,
            db=db_num,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        client = sentinel.master_for(master_name)
    else:
        client = aioredis.Redis(
            host=host,
            port=_DEFAULT_PORT,
            password=_DEFAULT_PASSWORD,
            db=db_num,
            socket_timeout=5,
            retry_on_timeout=True,
        )

    _bus_clients[target_service] = client
    logger.info(f"[{_SERVICE_NAME}] Bus connection to '{target_service}' → {host}:{_DEFAULT_PORT}/{db_num}")
    return client


STREAM_MAX_LEN = 50000
async def config_idmp(target_service: str):
    stream = STREAMS[target_service]
    client = _get_bus_client(target_service)
    try:
        await client.execute_command(
            "XCFGSET", stream,
            "IDMP-DURATION", str(120),
            "IDMP-MAXSIZE", str(1000),
        )
    except aioredis.ResponseError as e:
        logger.warning(f"Could not configure idempotency for {stream} -- {e}")
    except aioredis.ConnectionError:
        pass


async def ensure_group(service: str, retries: int = 30, delay: float = 1.0):
    client = _get_bus_client(service)
    stream = STREAMS[service]
    group = GROUPS[service]

    for attempt in range(retries):
        try:
            await client.xgroup_create(stream, group, id="0", mkstream=True)
            await config_idmp(service)
            return
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                await config_idmp(service)
                return
            raise
        except (aioredis.ConnectionError, aioredis.TimeoutError):
            logger.warning(f"[{service}] Bus Redis not ready.")
            await asyncio.sleep(delay)
    logger.error(f"[{service}] Could not connect to bus Redis after {retries} tries.")


async def publish(target_service: str, data: dict):
    client = _get_bus_client(target_service)
    stream = STREAMS[target_service]
    try:
        args = ["XADD", stream, "IDMPAUTO", _PRODUCER_ID, "*", "MAXLEN", "~", str(STREAM_MAX_LEN)]
        for k, v in data.items():
            args.append(k)
            args.append(v)
        await client.execute_command(*args)
    except aioredis.ResponseError:
        await client.xadd(stream, data, maxlen=STREAM_MAX_LEN, approximate=True)


PEL_TIMEOUT_SECONDS = float(os.getenv("PEL_TIMEOUT_SECONDS", 20.0))
PEL_SEARCH_INTERVAL_SECONDS = float(os.getenv("PEL_SEARCH_INTERVAL_SECONDS", 3.0))

PEL_IDLE_MS = int(PEL_TIMEOUT_SECONDS * 1000)  # reclaim messages idle longer than X s
PEL_CHECK_INTERVAL = PEL_SEARCH_INTERVAL_SECONDS  # seconds between reclaim sweeps


async def consume(service: str, worker_id: str, batch: int = 50):
    client = _get_bus_client(service)
    stream = STREAMS[service]
    group  = GROUPS[service]
    # Stagger the first reclaim so workers don't thundering-herd XAUTOCLAIM
    last_reclaim = time.time() - random.uniform(0, PEL_CHECK_INTERVAL)

    while not _shutdown_event.is_set():
        # --- Periodically reclaim orphaned PEL messages ---
        now = time.time()
        if now - last_reclaim >= PEL_CHECK_INTERVAL:
            last_reclaim = now
            try:
                # XAUTOCLAIM transfers messages idle > PEL_IDLE_MS
                # from any consumer in the group to *us*.
                # Returns (new_start_id, [(msg_id, data), ...], deleted_ids)
                _, claimed, _ = await client.xautoclaim(
                    name=stream,
                    groupname=group,
                    consumername=worker_id,
                    min_idle_time=PEL_IDLE_MS,
                    start_id="0-0",
                    count=batch,
                )
                if claimed:
                    logger.info(
                        f"[{service}] Reclaimed {len(claimed)} orphaned PEL message(s)"
                    )
                    for msg_id, data in claimed:
                        if data:          # deleted entries come back with data=None
                            yield msg_id, data
            except aioredis.RedisError as e:
                logger.warning(f"[{service}] PEL reclaim error: {e}")

        # --- Read new messages ---
        try:
            results = await client.xreadgroup(
                groupname=group,
                consumername=worker_id,
                streams={stream: ">"},
                count=batch,
                block=50,
            )
            if results:
                for _, messages in results:
                    for msg_id, data in messages:
                        yield msg_id, data
        except aioredis.RedisError as e:
            logger.error(f"[{service}] Redis error in consume: {e}")
            await asyncio.sleep(0.1)

    logger.info(f"[{service}] Worker {worker_id} exiting gracefully")


async def ack(service: str, msg_id):
    client = _get_bus_client(service)
    await client.xack(STREAMS[service], GROUPS[service], msg_id)

async def close():
    for name, client in _bus_clients.items():
        try:
            await client.aclose()
        except Exception:
            pass
    _bus_clients.clear()