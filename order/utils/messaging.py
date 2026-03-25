import json
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


def _make_bus_client() -> aioredis.Redis:
    sentinel_host = os.environ.get('BUS_REDIS_SENTINEL_HOST')
    if sentinel_host:
        sentinel_port = int(os.environ.get('BUS_REDIS_SENTINEL_PORT', '26379'))
        master_name = os.environ.get('BUS_REDIS_SENTINEL_MASTER', 'mymaster')
        password = os.environ.get('BUS_REDIS_PASSWORD', '')
        db_num = int(os.environ.get('BUS_REDIS_DB', '0'))
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
            host=os.environ['BUS_REDIS_HOST'],
            port=int(os.environ['BUS_REDIS_PORT']),
            password=os.environ['BUS_REDIS_PASSWORD'],
            db=int(os.environ['BUS_REDIS_DB']),
            socket_timeout=5,
            retry_on_timeout=True,
        )


bus_db = _make_bus_client()

_SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
_PRODUCER_ID = f"{_SERVICE_NAME}-{socket.gethostname()}-{os.getpid()}"

STREAMS = {
    "order":   "order.events",
    "stock":   "stock.events",
    "payment": "payment.events",
}

STREAM_MAX_LEN = 50000
async def config_idmp(stream: str):
    try:
        await bus_db.execute_command(
            "XCFGSET", stream,
            "IDMP-DURATION", str(120),
            "IDMP-MAXSIZE", str(1000),
        )
    except aioredis.ResponseError as e:
        logger.warning(f"Could not configure idempotency for {stream} -- {e}")
    except aioredis.ConnectionError:
        pass

GROUPS = {
    "order":   "order-workers",
    "stock":   "stock-workers",
    "payment": "payment-workers",
}

async def ensure_group(service: str, retries: int = 30, delay: float = 1.0):
    for attempt in range(retries):
        try:
            await bus_db.xgroup_create(STREAMS[service], GROUPS[service], id="0", mkstream=True)
            await config_idmp(STREAMS[service])
            return
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                await config_idmp(STREAMS[service])
                return
            raise
        except (aioredis.ConnectionError, aioredis.TimeoutError):
            logger.warning(f"[{service}] Bus Redis not ready.")
            await asyncio.sleep(delay)
    logger.error(f"[{service}] Could not connect to bus Redis after {retries} tries.")


async def publish(target_service: str, data: dict):
    stream = STREAMS[target_service]
    try:
        args = ["XADD", stream, "IDMPAUTO", _PRODUCER_ID, "*","MAXLEN", "~", str(STREAM_MAX_LEN),]
        for k, v in data.items():
            args.append(k)
            args.append(v)
        await bus_db.execute_command(*args)
    except aioredis.ResponseError:
        await bus_db.xadd(stream, data, maxlen=STREAM_MAX_LEN, approximate=True)

# Load PEL config from config.json
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'config.json')
try:
    with open(CONFIG_PATH, 'r') as f:
        _config = json.load(f)
except Exception:
    _config = {}
PEL_TIMEOUT_SECONDS = float(_config.get('PEL_TIMEOUT_SECONDS', 30.0))
PEL_SEARCH_INTERVAL_SECONDS = float(_config.get('PEL_SEARCH_INTERVAL_SECONDS', 2.0))

PEL_IDLE_MS = int(PEL_TIMEOUT_SECONDS * 1000)  # reclaim messages idle longer than X s
PEL_CHECK_INTERVAL = PEL_SEARCH_INTERVAL_SECONDS  # seconds between reclaim sweeps


async def consume(service: str, worker_id: str, batch: int = 50):
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
                _, claimed, _ = await bus_db.xautoclaim(
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
            results = await bus_db.xreadgroup(
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
    await bus_db.xack(STREAMS[service], GROUPS[service], msg_id)

async def close():
    global bus_db
    if bus_db is not None:
        await bus_db.aclose()
        bus_db = None